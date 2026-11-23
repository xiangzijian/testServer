package com.slg.world;

import com.slg.common.config.ConfigLoader;
import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceType;
import com.slg.common.discovery.StaticServiceDiscovery;
import com.slg.common.msg.MsgIds;
import com.slg.common.proto.PingReq;
import com.slg.common.server.BackendServer;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.ThreadManage;
import com.slg.common.thread.ThreadManage.Phase;
import com.slg.world.handler.WorldPingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * World 世界服入口，也是 world 服所有线程和组件的组装点。只包含网络骨架；ChunkActor、视野订阅、行军等见设计文档，均为 TODO。
 */
public final class WorldServer {

    private static final Logger log = LoggerFactory.getLogger(WorldServer.class);

    private final ThreadManage threads = new ThreadManage("world");
    private final BackendServer backend;

    public WorldServer(ServerConfig config) {
        SerialExecutor biz = BackendServer.newBizExecutor(ServiceType.WORLD, config);
        // TODO 会话关闭时清理该会话的视野订阅
        this.backend = new BackendServer(ServiceType.WORLD, config, new StaticServiceDiscovery(config),
                (sessionId, playerId) -> log.info("session closed sessionId={} playerId={}", sessionId, playerId),
                biz);
        threads.register(Phase.BUSINESS, biz);
        threads.register(Phase.INGRESS, backend);
        registerHandlers();
    }

    private void registerHandlers() {
        String nodeName = backend.self().key();
        backend.dispatcher()
                .register(MsgIds.WORLD_PING_REQ, PingReq.parser(), new WorldPingHandler(nodeName));
        // 新消息在此注册：.register(MsgIds.XXX_REQ, XxxReq.parser(), new XxxHandler())
    }

    public BackendServer backend() {
        return backend;
    }

    public void start() {
        threads.startAll();
    }

    public void stop() {
        if (!threads.shutdownAll()) {
            log.warn("[{}] some components did not shut down cleanly", backend.self().key());
        }
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ConfigLoader.load("world.properties");
        log.info("loading config from {}", config.source());
        WorldServer server = new WorldServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "world-shutdown"));
        server.start();
    }
}
