package com.slg.common.net.internal;

import com.google.protobuf.MessageLite;

/**
 * 服务间内部协议包（已去掉长度字段）。
 * <pre>
 * | length(4B) | kind(1B) | msgId(4B) | seq(4B) | sessionId(8B) | playerId(8B) | kingdomId(4B) | traceId(8B) | body |
 * </pre>
 * length = 其后所有字节数，大端序。
 * TODO deadline：设计文档 5.2 要求携带，后续加入头部或扩展字段。
 */
public record InternalPacket(PacketKind kind, int msgId, int seq, long sessionId, long playerId,
                             int kingdomId, long traceId, byte[] body) {

    public static InternalPacket control(int msgId, MessageLite body) {
        return new InternalPacket(PacketKind.CONTROL, msgId, 0, 0, 0, 0, 0, bytes(body));
    }

    /** 基于当前（请求）包构造回包，保持会话、seq、trace 等上下文。 */
    public InternalPacket reply(int respMsgId, MessageLite body) {
        return new InternalPacket(PacketKind.RESPONSE, respMsgId, seq, sessionId, playerId, kingdomId, traceId, bytes(body));
    }

    public static byte[] bytes(MessageLite body) {
        return body == null ? new byte[0] : body.toByteArray();
    }

    @Override
    public String toString() {
        return "InternalPacket{" + kind + ", msgId=" + msgId + ", seq=" + seq + ", sessionId=" + sessionId
                + ", playerId=" + playerId + ", kingdomId=" + kingdomId + ", traceId=" + traceId
                + ", bodyLen=" + (body == null ? 0 : body.length) + "}";
    }
}
