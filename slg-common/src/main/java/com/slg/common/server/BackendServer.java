package com.slg.common.server;

import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceDiscovery;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.discovery.ServiceType;
import com.slg.common.dispatch.KeyedExecutor;
import com.slg.common.dispatch.MessageDispatcher;
import com.slg.common.net.internal.InternalServer;
import com.slg.common.net.internal.SessionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 有状态后端服（world/game）的通用启动骨架：
 * 配置 → 业务线程 → handler 注册表 → 内部 TCP 服务端 → 服务注册。
 * 具体服务只需要 {@link #dispatcher()} 注册 handler 后调用 {@link #start()}。
 *
 * <p>配置项：
 * <pre>
 * server.nodeId=1
 * server.host=0.0.0.0          # 监听地址
 * server.port=9101
 * server.ioThreads=0           # 0 = Netty 默认（CPU*2）
 * server.bizThreads=0          # 0 = CPU 核数
 * server.readerIdleSeconds=60  # 超过该时间未收到对端任何包则断开
 * </pre>
 */
public final class BackendServer {

    private static final Logger log = LoggerFactory.getLogger(BackendServer.class);

    private final ServiceInstance self;
    private final KeyedExecutor executor;
    private final MessageDispatcher dispatcher;
    private final InternalServer server;
    private final ServiceDiscovery discovery;

    public BackendServer(ServiceType type, ServerConfig config, ServiceDiscovery discovery,
                         SessionListener sessionListener) {
        int nodeId = config.getInt("server.nodeId", 1);
        String host = config.getString("server.host", "0.0.0.0");
        int port = config.getInt("server.port", 0);
        this.self = new ServiceInstance(type, nodeId, host, port);
        this.discovery = discovery;
        this.executor = new KeyedExecutor(self.key() + "-biz", config.getInt("server.bizThreads", 0));
        this.dispatcher = new MessageDispatcher(executor);
        this.server = new InternalServer(self, dispatcher, sessionListener,
                config.getInt("server.ioThreads", 0), config.getInt("server.readerIdleSeconds", 60));
    }

    public ServiceInstance self() {
        return self;
    }

    public MessageDispatcher dispatcher() {
        return dispatcher;
    }

    public int boundPort() {
        return server.boundPort();
    }

    public void start() throws InterruptedException {
        server.start();
        discovery.register(self);
        log.info("[{}] started, bizThreads={}", self.key(), executor.size());
    }

    public void stop() {
        server.stop();
        executor.shutdown();
        discovery.close();
    }
}
