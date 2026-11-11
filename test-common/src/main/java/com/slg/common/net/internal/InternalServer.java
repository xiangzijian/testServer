package com.slg.common.net.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.dispatch.MessageDispatcher;
import com.slg.common.proto.HandshakeReq;
import com.slg.common.proto.HandshakeResp;
import com.slg.common.proto.InternalHeartbeat;
import com.slg.common.proto.SessionClosed;
import com.slg.common.util.NamedThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * 内部 TCP 服务端（world/game 共用）：接收 gateway（及将来其它服）的连接，
 * 处理握手/心跳等控制包，把 FORWARD 包交给 {@link MessageDispatcher}。
 */
public final class InternalServer {

    private static final Logger log = LoggerFactory.getLogger(InternalServer.class);
    public static final AttributeKey<String> PEER = AttributeKey.valueOf("slg.peer");

    private final ServiceInstance self;
    private final MessageDispatcher dispatcher;
    private final SessionListener sessionListener;
    private final int ioThreads;
    private final int readerIdleSeconds;
    private final ChannelGroup peers = new DefaultChannelGroup("internal-peers", GlobalEventExecutor.INSTANCE);

    private EventLoopGroup boss;
    private EventLoopGroup workers;
    private Channel serverChannel;

    public InternalServer(ServiceInstance self, MessageDispatcher dispatcher, SessionListener sessionListener,
                          int ioThreads, int readerIdleSeconds) {
        this.self = self;
        this.dispatcher = dispatcher;
        this.sessionListener = sessionListener == null ? SessionListener.NOOP : sessionListener;
        this.ioThreads = ioThreads;
        this.readerIdleSeconds = readerIdleSeconds;
    }

    public void start() throws InterruptedException {
        boss = new NioEventLoopGroup(1, new NamedThreadFactory(self.key() + "-boss", false));
        workers = new NioEventLoopGroup(ioThreads, new NamedThreadFactory(self.key() + "-io", false));
        ServerHandler handler = new ServerHandler();
        ServerBootstrap b = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("idle", new IdleStateHandler(readerIdleSeconds, 0, 0, TimeUnit.SECONDS));
                        InternalCodec.install(ch.pipeline(), InternalCodec.DEFAULT_MAX_FRAME);
                        ch.pipeline().addLast("handler", handler);
                    }
                });
        serverChannel = b.bind(self.host(), self.port()).sync().channel();
        log.info("[{}] internal server listening on {}", self.key(), serverChannel.localAddress());
    }

    /** 实际监听端口（配置端口为 0 时用于测试）。 */
    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public ChannelGroup peers() {
        return peers;
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        peers.close().awaitUninterruptibly();
        if (boss != null) {
            boss.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workers != null) {
            workers.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
        log.info("[{}] internal server stopped", self.key());
    }

    @ChannelHandler.Sharable
    private final class ServerHandler extends SimpleChannelInboundHandler<InternalPacket> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            peers.add(ctx.channel());
            log.info("[{}] peer connected {}", self.key(), ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("[{}] peer disconnected {} ({})", self.key(), ctx.channel().remoteAddress(),
                    ctx.channel().attr(PEER).get());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, InternalPacket p) throws InvalidProtocolBufferException {
            switch (p.kind()) {
                case FORWARD:
                    dispatcher.dispatch(ctx.channel(), p);
                    break;
                case CONTROL:
                    onControl(ctx, p);
                    break;
                default:
                    log.warn("[{}] unexpected packet {} from {}", self.key(), p, ctx.channel().remoteAddress());
            }
        }

        private void onControl(ChannelHandlerContext ctx, InternalPacket p) throws InvalidProtocolBufferException {
            switch (p.msgId()) {
                case ControlMsgIds.HANDSHAKE_REQ: {
                    HandshakeReq req = HandshakeReq.parseFrom(p.body());
                    String peer = req.getServiceType().toLowerCase() + "-" + req.getNodeId();
                    ctx.channel().attr(PEER).set(peer);
                    log.info("[{}] handshake from {} {}", self.key(), peer, ctx.channel().remoteAddress());
                    ctx.writeAndFlush(InternalPacket.control(ControlMsgIds.HANDSHAKE_RESP, HandshakeResp.newBuilder()
                            .setServiceType(self.type().name())
                            .setNodeId(self.nodeId())
                            .setServerTime(System.currentTimeMillis())
                            .build()));
                    break;
                }
                case ControlMsgIds.HEARTBEAT_REQ:
                    ctx.writeAndFlush(InternalPacket.control(ControlMsgIds.HEARTBEAT_RESP,
                            InternalHeartbeat.newBuilder().setTime(System.currentTimeMillis()).build()));
                    break;
                case ControlMsgIds.SESSION_CLOSED: {
                    SessionClosed sc = SessionClosed.parseFrom(p.body());
                    long key = sc.getPlayerId() != 0 ? sc.getPlayerId() : sc.getSessionId();
                    dispatcher.execute(key, () -> sessionListener.onSessionClosed(sc.getSessionId(), sc.getPlayerId()));
                    break;
                }
                default:
                    log.warn("[{}] unknown control msgId {}", self.key(), p.msgId());
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent e && e.state() == IdleState.READER_IDLE) {
                log.warn("[{}] peer {} idle {}s, closing", self.key(), ctx.channel().remoteAddress(), readerIdleSeconds);
                ctx.close();
                return;
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("[{}] peer {} error: {}", self.key(), ctx.channel().remoteAddress(), cause.toString());
            ctx.close();
        }
    }
}
