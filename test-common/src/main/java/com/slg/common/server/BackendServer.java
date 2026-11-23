package com.slg.common.server;

import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceDiscovery;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.discovery.ServiceType;

import com.slg.common.dispatch.MessageDispatcher;
import com.slg.common.net.internal.InternalServer;
import com.slg.common.net.internal.SessionListener;
import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.SerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 有状态后端服（world/game）的网络入口：handler 注册表 + 内部 TCP 服务端 + 服务注册。
 * 不创建业务线程：业务执行器由 server 启动器创建并传入，和本对象一起注册到 ThreadManage，
 * 本对象注册在 INGRESS 阶段，最后对外开放、最先摘除。
 *
 * <p>配置项：
 * <pre>
 * server.nodeId=1
 * server.host=0.0.0.0          # 监听地址
 * server.port=9101
 * server.ioThreads=0           # 0 = Netty 默认（CPU*2）
 * server.bizThreads=0          # 0 = CPU 核数，见 {@link #newBizExecutor}
 * server.bizQueueCapacity=3000 # 单个玩家（或会话）的排队上限，满了回 SERVER_BUSY
 * server.readerIdleSeconds=60  # 超过该时间未收到对端任何包则断开
 * </pre>
 */
public final class BackendServer implements ManagedExecutor {

    private static final Logger log = LoggerFactory.getLogger(BackendServer.class);

    private final ServiceInstance self;
    private final MessageDispatcher dispatcher;
    private final InternalServer server;
    private final ServiceDiscovery discovery;

    public BackendServer(ServiceType type, ServerConfig config, ServiceDiscovery discovery,
                         SessionListener sessionListener, SerialExecutor bizExecutor) {
        this.self = selfOf(type, config);
        this.discovery = discovery;
        this.dispatcher = new MessageDispatcher(bizExecutor);
        this.server = new InternalServer(self, dispatcher, sessionListener,
                config.getInt("server.ioThreads", 0), config.getInt("server.readerIdleSeconds", 60));
    }

    /**
     * 按 server.bizThreads / server.bizQueueCapacity 创建业务执行器的默认做法；server 也可以自行创建。
     */
    public static SerialExecutor newBizExecutor(ServiceType type, ServerConfig config) {
        int bizThreads = config.getInt("server.bizThreads", 0);
        if (bizThreads <= 0) {
            bizThreads = Runtime.getRuntime().availableProcessors();
        }
        return new SerialExecutor(selfOf(type, config).key() + "-biz", bizThreads,
                config.getInt("server.bizQueueCapacity", 3000));
    }

    private static ServiceInstance selfOf(ServiceType type, ServerConfig config) {
        return new ServiceInstance(type, config.getInt("server.nodeId", 1),
                config.getString("server.host", "0.0.0.0"), config.getInt("server.port", 0));
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

    @Override
    public String name() {
        return self.key() + "-ingress";
    }

    /** 先打开端口，再注册到服务发现。 */
    @Override
    public void start() {
        try {
            server.start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while binding " + self, e);
        }
        discovery.register(self);
        log.info("[{}] ingress started", self.key());
    }

    /** 先从注册中心摘除，再停网络；业务线程由 ThreadManage 在之后的阶段关闭。 */
    @Override
    public boolean shutdown(long timeout, TimeUnit unit) {
        discovery.close();
        server.stop();
        return true;
    }
}
