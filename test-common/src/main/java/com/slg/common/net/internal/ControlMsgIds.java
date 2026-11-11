package com.slg.common.net.internal;

/** {@link PacketKind#CONTROL} 包使用的消息 ID，与客户端消息 ID 空间独立。 */
public final class ControlMsgIds {

    private ControlMsgIds() {
    }

    public static final int HANDSHAKE_REQ = 1;
    public static final int HANDSHAKE_RESP = 2;
    public static final int HEARTBEAT_REQ = 3;
    public static final int HEARTBEAT_RESP = 4;
    public static final int SESSION_CLOSED = 5;
}
