package com.slg.game;

import com.slg.common.config.ConfigLoader;
import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceType;
import com.slg.common.discovery.StaticServiceDiscovery;
import com.slg.common.msg.MsgIds;
import com.slg.common.proto.PingReq;
import com.slg.common.server.BackendServer;
import com.slg.game.handler.GamePingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Game 主城服入口。只包含网络骨架；PlayerActor、时间轮、持久化等见设计文档，均为 TODO。
 */
public final class GameServer {

    private static final Logger log = LoggerFactory.getLogger(GameServer.class);

    private final BackendServer backend;

    public GameServer(ServerConfig config) {
        // TODO 会话关闭时通知对应 PlayerActor（下线/卸载流程）
        this.backend = new BackendServer(ServiceType.GAME, config, new StaticServiceDiscovery(config),
                (sessionId, playerId) -> log.info("session closed sessionId={} playerId={}", sessionId, playerId));
        registerHandlers();
    }

    private void registerHandlers() {
        String nodeName = backend.self().key();
        backend.dispatcher()
                .register(MsgIds.GAME_PING_REQ, PingReq.parser(), new GamePingHandler(nodeName));
        // 新消息在此注册：.register(MsgIds.XXX_REQ, XxxReq.parser(), new XxxHandler())
    }

    public BackendServer backend() {
        return backend;
    }

    public void start() throws InterruptedException {
        backend.start();
    }

    public void stop() {
        backend.stop();
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ConfigLoader.load("game.properties");
        log.info("loading config from {}", config.source());
        GameServer server = new GameServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "game-shutdown"));
        server.start();
    }
}
