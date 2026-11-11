package com.slg.robot;

import com.google.protobuf.MessageLite;
import com.slg.common.net.client.ClientCodec;
import com.slg.common.net.client.ClientMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 最小客户端：按 seq 匹配请求与回包，seq = 0 的下行视为推送。
 */
public final class RobotClient implements AutoCloseable {

    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final Map<Integer, CompletableFuture<ClientMessage>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger seq = new AtomicInteger();
    private volatile Consumer<ClientMessage> pushListener = m -> { };
    private Channel channel;

    public void connect(String host, int port) throws InterruptedException {
        Bootstrap b = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ClientCodec.install(ch.pipeline(), ClientCodec.DEFAULT_MAX_FRAME);
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ClientMessage>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ClientMessage msg) {
                                CompletableFuture<ClientMessage> f = msg.seq() == 0 ? null : pending.remove(msg.seq());
                                if (f != null) {
                                    f.complete(msg);
                                } else {
                                    pushListener.accept(msg);
                                }
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                IllegalStateException e = new IllegalStateException("connection closed");
                                pending.values().forEach(f -> f.completeExceptionally(e));
                                pending.clear();
                            }
                        });
                    }
                });
        channel = b.connect(host, port).sync().channel();
    }

    public void onPush(Consumer<ClientMessage> listener) {
        this.pushListener = listener;
    }

    public CompletableFuture<ClientMessage> request(int msgId, MessageLite body, long timeoutMillis) {
        int s = seq.incrementAndGet();
        CompletableFuture<ClientMessage> f = new CompletableFuture<>();
        pending.put(s, f);
        channel.writeAndFlush(ClientMessage.of(msgId, s, body));
        return f.orTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                .whenComplete((r, e) -> pending.remove(s));
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
        }
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }
}
