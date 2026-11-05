package com.slg.game.handler;

import com.slg.common.dispatch.HandlerContext;
import com.slg.common.dispatch.MessageHandler;
import com.slg.common.msg.MsgIds;
import com.slg.common.proto.PingReq;
import com.slg.common.proto.PingResp;

/** 示例 handler：原样回显 content，用于验证 client → gateway → game → gateway → client 链路。 */
public final class GamePingHandler implements MessageHandler<PingReq> {

    private final String nodeName;

    public GamePingHandler(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public void handle(HandlerContext ctx, PingReq req) {
        ctx.reply(MsgIds.GAME_PING_RESP, PingResp.newBuilder()
                .setContent(req.getContent())
                .setClientTime(req.getClientTime())
                .setServerTime(System.currentTimeMillis())
                .setServer(nodeName)
                .build());
    }
}
