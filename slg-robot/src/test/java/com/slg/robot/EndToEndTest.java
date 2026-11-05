package com.slg.robot;

import com.slg.common.config.ConfigLoader;
import com.slg.common.config.ServerConfig;
import com.slg.common.discovery.ServiceType;
import com.slg.common.msg.ErrorCodes;
import com.slg.common.msg.MsgIds;
import com.slg.common.net.client.ClientMessage;
import com.slg.common.proto.ErrorResp;
import com.slg.common.proto.HeartbeatReq;
import com.slg.common.proto.PingReq;
import com.slg.common.proto.PingResp;
import com.slg.game.GameServer;
import com.slg.gateway.GatewayServer;
import com.slg.world.WorldServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程内拉起 gateway / world / game，验证：
 * 后端未启动时报 BACKEND_UNAVAILABLE → 后端启动后 gateway 自动重连 → ping/pong 打通 → 后端重启后再次自动恢复。
 */
class EndToEndTest {

    private GatewayServer gateway;
    private WorldServer world;
    private GameServer game;

    @AfterEach
    void tearDown() {
        if (gateway != null) {
            gateway.stop();
        }
        if (world != null) {
            world.stop();
        }
        if (game != null) {
            game.stop();
        }
    }

    @Test
    void pingThroughGatewayWithReconnect() throws Exception {
        int worldPort = freePort();
        int gamePort = freePort();

        ServerConfig gwCfg = ConfigLoader.fromClasspath("gateway.properties")
                .with("client.port", "0")
                .with("backend.reconnectMinMillis", "100")
                .with("backend.reconnectMaxMillis", "300")
                .with("discovery.world.nodes", "1@127.0.0.1:" + worldPort)
                .with("discovery.game.nodes", "1@127.0.0.1:" + gamePort);
        gateway = new GatewayServer(gwCfg);
        gateway.start();

        try (RobotClient client = new RobotClient()) {
            client.connect("127.0.0.1", gateway.clientPort());

            ClientMessage hb = client.request(MsgIds.HEARTBEAT_REQ, HeartbeatReq.getDefaultInstance(), 3000).get();
            assertEquals(MsgIds.HEARTBEAT_RESP, hb.msgId());

            ClientMessage err = client.request(MsgIds.GAME_PING_REQ, ping("x"), 3000).get();
            assertEquals(MsgIds.ERROR_RESP, err.msgId());
            assertEquals(ErrorCodes.BACKEND_UNAVAILABLE, ErrorResp.parseFrom(err.body()).getCode());

            world = new WorldServer(ConfigLoader.fromClasspath("world.properties").with("server.port", "" + worldPort));
            world.start();
            game = new GameServer(ConfigLoader.fromClasspath("game.properties").with("server.port", "" + gamePort));
            game.start();
            awaitTrue(() -> gateway.backends().readyCount(ServiceType.WORLD) == 1
                    && gateway.backends().readyCount(ServiceType.GAME) == 1);

            assertPong(client, MsgIds.GAME_PING_REQ, MsgIds.GAME_PING_RESP, "hello-game", "game-1");
            assertPong(client, MsgIds.WORLD_PING_REQ, MsgIds.WORLD_PING_RESP, "hello-world", "world-1");

            game.stop();
            awaitTrue(() -> gateway.backends().readyCount(ServiceType.GAME) == 0);
            game = new GameServer(ConfigLoader.fromClasspath("game.properties").with("server.port", "" + gamePort));
            game.start();
            awaitTrue(() -> gateway.backends().readyCount(ServiceType.GAME) == 1);
            assertPong(client, MsgIds.GAME_PING_REQ, MsgIds.GAME_PING_RESP, "after-reconnect", "game-1");

            ClientMessage unknown = client.request(99999, PingReq.getDefaultInstance(), 3000).get();
            assertEquals(ErrorCodes.UNKNOWN_MSG, ErrorResp.parseFrom(unknown.body()).getCode());
        }
    }

    private static void assertPong(RobotClient client, int reqId, int respId, String content, String server)
            throws Exception {
        ClientMessage m = client.request(reqId, ping(content), 3000).get();
        assertEquals(respId, m.msgId());
        PingResp resp = PingResp.parseFrom(m.body());
        assertEquals(content, resp.getContent());
        assertEquals(server, resp.getServer());
    }

    private static PingReq ping(String content) {
        return PingReq.newBuilder().setContent(content).setClientTime(System.currentTimeMillis()).build();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void awaitTrue(BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!cond.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline, "condition not met within 10s");
            Thread.sleep(50);
        }
    }
}
