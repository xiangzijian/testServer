package com.slg.common.server;

import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceDiscovery;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.discovery.ServiceType;

import com.slg.common.dispatch.MessageDispatcher;
import com.slg.common.net.internal.InternalServer;
import com.slg.common.net.internal.SessionListener;
import com.slg.common.thread.SerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
 * server.bizQueueCapacity=3000 # 单个玩家（或会话）的排队上限，满了回 SERVER_BUSY
 * server.readerIdleSeconds=60  # 超过该时间未收到对端任何包则断开
 * </pre>
 */
public final class BackendServer {

    private static final Logger log = LoggerFactory.getLogger(BackendServer.class);

    private final ServiceInstance self;
    private final SerialExecutor executor;
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
        int bizThreads = config.getInt("server.bizThreads", 0);
        if (bizThreads <= 0) {
            bizThreads = Runtime.getRuntime().availableProcessors();
        }
        this.executor = new SerialExecutor(self.key() + "-biz", bizThreads,
                config.getInt("server.bizQueueCapacity", 3000));
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
        executor.start();
        server.start();
        discovery.register(self);
        log.info("[{}] started, bizThreads={}", self.key(), executor.size());
    }

    /** 先从注册中心摘除，再停网络，最后等业务线程执行完已接收的消息。 */
    public void stop() {
        discovery.close();
        server.stop();
        if (!executor.shutdown(10, TimeUnit.SECONDS)) {
            log.warn("[{}] biz executor did not finish in 10s, pending={}", self.key(), executor.pendingTasks());
        }
    }
}
