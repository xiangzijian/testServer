package com.slg.common.net.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.proto.HandshakeReq;
import com.slg.common.proto.HandshakeResp;
import com.slg.common.proto.InternalHeartbeat;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 到某个后端节点的内部长连接：连接成功后握手，握手完成才可发送；
 * 写空闲发心跳，读超时断开；断开或连接失败后指数退避自动重连。
 */
public final class InternalClient {

    private static final Logger log = LoggerFactory.getLogger(InternalClient.class);

    public interface Listener {
        /** 收到 RESPONSE / PUSH 包，在 IO 线程上回调。 */
        void onPacket(InternalClient client, InternalPacket packet);

        default void onReady(InternalClient client) {
        }

        default void onDisconnected(InternalClient client) {
        }
    }

    public static final class Options {
        public int heartbeatSeconds = 10;
        /** 读超时 = 心跳间隔 × 该倍数 */
        public int heartbeatTimeoutFactor = 3;
        public long reconnectMinMillis = 1_000;
        public long reconnectMaxMillis = 30_000;
        public int connectTimeoutMillis = 3_000;
    }

    private final ServiceInstance target;
    private final ServiceInstance self;
    private final EventLoopGroup group;
    private final Listener listener;
    private final Options options;

    private volatile Channel channel;
    private volatile boolean ready;
    private volatile boolean closed;
    private volatile long nextReconnectDelay;

    public InternalClient(ServiceInstance target, ServiceInstance self, EventLoopGroup group,
                          Listener listener, Options options) {
        this.target = target;
        this.self = self;
        this.group = group;
        this.listener = listener;
        this.options = options;
        this.nextReconnectDelay = options.reconnectMinMillis;
    }

    public ServiceInstance target() {
        return target;
    }

    public boolean isReady() {
        Channel ch = channel;
        return ready && ch != null && ch.isActive();
    }

    public void start() {
        connect();
    }

    /** @return false 表示当前链路不可用，调用方应自行处理（如回错误给客户端） */
    public boolean send(InternalPacket packet) {
        Channel ch = channel;
        if (!ready || ch == null || !ch.isActive()) {
            return false;
        }
        ch.writeAndFlush(packet);
        return true;
    }

    public void close() {
        closed = true;
        ready = false;
        Channel ch = channel;
        if (ch != null) {
            ch.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
        }
    }

    private void connect() {
        if (closed || group.isShuttingDown()) {
            return;
        }
        int readerIdle = options.heartbeatSeconds * options.heartbeatTimeoutFactor;
        Bootstrap b = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.connectTimeoutMillis)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("idle",
                                new IdleStateHandler(readerIdle, options.heartbeatSeconds, 0, TimeUnit.SECONDS));
                        InternalCodec.install(ch.pipeline(), InternalCodec.DEFAULT_MAX_FRAME);
                        ch.pipeline().addLast("handler", new ClientHandler());
                    }
                });
        ChannelFuture f = b.connect(target.host(), target.port());
        f.addListener((ChannelFuture cf) -> {
            if (cf.isSuccess()) {
                channel = cf.channel();
            } else {
                log.warn("[{}] connect {} failed: {}, retry in {}ms", self.key(), target, rootMessage(cf.cause()),
                        nextReconnectDelay);
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (closed || group.isShuttingDown()) {
            return;
        }
        long delay = nextReconnectDelay;
        nextReconnectDelay = Math.min(nextReconnectDelay * 2, options.reconnectMaxMillis);
        group.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.toString();
    }

    private final class ClientHandler extends SimpleChannelInboundHandler<InternalPacket> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            channel = ctx.channel();
            ctx.writeAndFlush(InternalPacket.control(ControlMsgIds.HANDSHAKE_REQ, HandshakeReq.newBuilder()
                    .setServiceType(self.type().name())
                    .setNodeId(self.nodeId())
                    .build()));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            boolean wasReady = ready;
            ready = false;
            if (wasReady) {
                listener.onDisconnected(InternalClient.this);
            }
            if (!closed) {
                log.warn("[{}] lost connection to {}, reconnect in {}ms", self.key(), target, nextReconnectDelay);
                scheduleReconnect();
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, InternalPacket p) throws InvalidProtocolBufferException {
            if (p.kind() == PacketKind.CONTROL) {
                switch (p.msgId()) {
                    case ControlMsgIds.HANDSHAKE_RESP: {
                        HandshakeResp resp = HandshakeResp.parseFrom(p.body());
                        ready = true;
                        nextReconnectDelay = options.reconnectMinMillis;
                        log.info("[{}] connected to {} ({}-{})", self.key(), target,
                                resp.getServiceType().toLowerCase(), resp.getNodeId());
                        listener.onReady(InternalClient.this);
                        break;
                    }
                    case ControlMsgIds.HEARTBEAT_RESP:
                        break;
                    default:
                        log.warn("[{}] unknown control msgId {} from {}", self.key(), p.msgId(), target);
                }
                return;
            }
            listener.onPacket(InternalClient.this, p);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent e) {
                if (e.state() == IdleState.WRITER_IDLE) {
                    ctx.writeAndFlush(InternalPacket.control(ControlMsgIds.HEARTBEAT_REQ,
                            InternalHeartbeat.newBuilder().setTime(System.currentTimeMillis()).build()));
                } else if (e.state() == IdleState.READER_IDLE) {
                    log.warn("[{}] {} heartbeat timeout, closing", self.key(), target);
                    ctx.close();
                }
                return;
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("[{}] connection {} error: {}", self.key(), target, cause.toString());
            ctx.close();
        }
    }
}
