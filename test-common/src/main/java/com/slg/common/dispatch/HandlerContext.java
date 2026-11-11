package com.slg.common.dispatch;

import com.google.protobuf.MessageLite;
import com.slg.common.msg.MsgIds;
import com.slg.common.net.internal.InternalPacket;
import com.slg.common.net.internal.PacketKind;
import com.slg.common.proto.ErrorResp;
import io.netty.channel.Channel;

/**
 * 一次请求的处理上下文：请求头信息 + 回包/推送能力。
 */
public final class HandlerContext {

    private final Channel channel;
    private final InternalPacket request;

    public HandlerContext(Channel channel, InternalPacket request) {
        this.channel = channel;
        this.request = request;
    }

    public long sessionId() {
        return request.sessionId();
    }

    public long playerId() {
        return request.playerId();
    }

    public int kingdomId() {
        return request.kingdomId();
    }

    public long traceId() {
        return request.traceId();
    }

    public int seq() {
        return request.seq();
    }

    public int msgId() {
        return request.msgId();
    }

    /** 与请求来自同一条连接（通常是某个 gateway）。 */
    public Channel channel() {
        return channel;
    }

    /** 回包给发起请求的客户端，seq 自动带回。 */
    public void reply(int respMsgId, MessageLite body) {
        channel.writeAndFlush(request.reply(respMsgId, body));
    }

    public void replyError(int code, String message) {
        reply(MsgIds.ERROR_RESP, ErrorResp.newBuilder()
                .setCode(code).setMessage(message == null ? "" : message).setReqMsgId(request.msgId()).build());
    }

    /** 向当前会话主动推送（seq = 0）。推送给其它会话需要通过会话所在 gateway 的连接，后续由路由层提供。 */
    public void push(int msgId, MessageLite body) {
        channel.writeAndFlush(new InternalPacket(PacketKind.PUSH, msgId, 0, request.sessionId(), request.playerId(),
                request.kingdomId(), request.traceId(), InternalPacket.bytes(body)));
    }
}
