package com.slg.common.net.client;

import com.google.protobuf.MessageLite;

/**
 * 客户端协议的一帧（已去掉长度字段）。
 * <pre>
 * | length(4B) | msgId(4B) | seq(4B) | flags(1B) | body(Protobuf) |
 * </pre>
 * length = 其后所有字节数（msgId + seq + flags + body），大端序。
 *
 * @param flags bit0 = 压缩，bit1 = 加密（当前骨架未实现，见 {@link #FLAG_COMPRESSED} / {@link #FLAG_ENCRYPTED}）
 */
public record ClientMessage(int msgId, int seq, byte flags, byte[] body) {

    public static final byte FLAG_COMPRESSED = 0x01;
    public static final byte FLAG_ENCRYPTED = 0x02;

    public static ClientMessage of(int msgId, int seq, MessageLite body) {
        return new ClientMessage(msgId, seq, (byte) 0, body == null ? new byte[0] : body.toByteArray());
    }

    @Override
    public String toString() {
        return "ClientMessage{msgId=" + msgId + ", seq=" + seq + ", flags=" + flags
                + ", bodyLen=" + (body == null ? 0 : body.length) + "}";
    }
}
