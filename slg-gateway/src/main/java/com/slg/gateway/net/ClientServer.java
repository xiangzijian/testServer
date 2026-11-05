package com.slg.gateway.net;

import com.slg.common.net.client.ClientCodec;
import com.slg.common.util.NamedThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/** 面向客户端的 TCP 监听。TODO WebSocket 接入（设计文档 A7 备选）。 */
public final class ClientServer {

    private static final Logger log = LoggerFactory.getLogger(ClientServer.class);

    private final String host;
    private final int port;
    private final int ioThreads;
    private final int readerIdleSeconds;
    private final int maxFrameLength;
    private final ClientHandler handler;

    private EventLoopGroup boss;
    private EventLoopGroup workers;
    private Channel serverChannel;

    public ClientServer(String host, int port, int ioThreads, int readerIdleSeconds, int maxFrameLength,
                        ClientHandler handler) {
        this.host = host;
        this.port = port;
        this.ioThreads = ioThreads;
        this.readerIdleSeconds = readerIdleSeconds;
        this.maxFrameLength = maxFrameLength;
        this.handler = handler;
    }

    public void start() throws InterruptedException {
        boss = new NioEventLoopGroup(1, new NamedThreadFactory("gw-client-boss", false));
        workers = new NioEventLoopGroup(ioThreads, new NamedThreadFactory("gw-client-io", false));
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
                        ClientCodec.install(ch.pipeline(), maxFrameLength);
                        ch.pipeline().addLast("handler", handler);
                    }
                });
        serverChannel = b.bind(host, port).sync().channel();
        log.info("gateway client listener on {}", serverChannel.localAddress());
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (boss != null) {
            boss.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workers != null) {
            workers.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
