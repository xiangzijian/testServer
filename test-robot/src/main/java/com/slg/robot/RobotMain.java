package com.slg.robot;

import com.slg.common.msg.MsgIds;
import com.slg.common.net.client.ClientMessage;
import com.slg.common.proto.ErrorResp;
import com.slg.common.proto.HeartbeatReq;
import com.slg.common.proto.HeartbeatResp;
import com.slg.common.proto.PingReq;
import com.slg.common.proto.PingResp;

/**
 * 测试客户端：连接 gateway，发送心跳 + game ping + world ping，打印结果。
 * <pre>
 * java -jar slg-robot-all.jar [host] [port] [rounds]
 * </pre>
 * 全部成功退出码 0，否则 1。
 */
public final class RobotMain {

    private static final long TIMEOUT_MS = 3000;

    private RobotMain() {
    }

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9001;
        int rounds = args.length > 2 ? Integer.parseInt(args[2]) : 3;

        int ok = 0;
        int total = 0;
        try (RobotClient client = new RobotClient()) {
            client.connect(host, port);
            System.out.printf("connected to gateway %s:%d%n", host, port);

            total++;
            ClientMessage hb = client.request(MsgIds.HEARTBEAT_REQ,
                    HeartbeatReq.newBuilder().setClientTime(System.currentTimeMillis()).build(), TIMEOUT_MS).get();
            if (hb.msgId() == MsgIds.HEARTBEAT_RESP) {
                ok++;
                System.out.printf("[heartbeat] serverTime=%d%n", HeartbeatResp.parseFrom(hb.body()).getServerTime());
            } else {
                System.out.printf("[heartbeat] FAILED %s%n", describe(hb));
            }

            for (int i = 1; i <= rounds; i++) {
                total += 2;
                ok += ping(client, "game", MsgIds.GAME_PING_REQ, MsgIds.GAME_PING_RESP, i) ? 1 : 0;
                ok += ping(client, "world", MsgIds.WORLD_PING_REQ, MsgIds.WORLD_PING_RESP, i) ? 1 : 0;
            }
        }
        System.out.printf("RESULT: %d/%d ok -> %s%n", ok, total, ok == total ? "PASS" : "FAIL");
        System.exit(ok == total ? 0 : 1);
    }

    private static boolean ping(RobotClient client, String target, int reqId, int respId, int round) {
        String content = target + "-ping-" + round;
        long start = System.nanoTime();
        try {
            ClientMessage m = client.request(reqId, PingReq.newBuilder()
                    .setContent(content).setClientTime(System.currentTimeMillis()).build(), TIMEOUT_MS).get();
            double rttMs = (System.nanoTime() - start) / 1_000_000.0;
            if (m.msgId() != respId) {
                System.out.printf("[%s] round %d FAILED: %s%n", target, round, describe(m));
                return false;
            }
            PingResp resp = PingResp.parseFrom(m.body());
            boolean match = content.equals(resp.getContent());
            System.out.printf("[%s] round %d seq=%d pong from %s content=%s rtt=%.2fms%s%n", target, round, m.seq(),
                    resp.getServer(), resp.getContent(), rttMs, match ? "" : " (CONTENT MISMATCH)");
            return match;
        } catch (Exception e) {
            System.out.printf("[%s] round %d FAILED: %s%n", target, round, e);
            return false;
        }
    }

    private static String describe(ClientMessage m) {
        if (m.msgId() == MsgIds.ERROR_RESP) {
            try {
                ErrorResp e = ErrorResp.parseFrom(m.body());
                return "ErrorResp{code=" + e.getCode() + ", message=" + e.getMessage() + "}";
            } catch (Exception ignored) {
                // fall through
            }
        }
        return m.toString();
    }
}
