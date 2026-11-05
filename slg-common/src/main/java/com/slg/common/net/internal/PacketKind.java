package com.slg.common.net.internal;

public enum PacketKind {
    /** gateway → 后端：转发客户端请求 */
    FORWARD(1),
    /** 后端 → gateway：对某次请求的回包（seq 与请求一致） */
    RESPONSE(2),
    /** 后端 → gateway：主动推送给某会话（seq = 0） */
    PUSH(3),
    /** 链路控制：握手、心跳、会话关闭通知等，msgId 见 {@link ControlMsgIds} */
    CONTROL(4);

    private final byte code;

    PacketKind(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static PacketKind of(byte code) {
        switch (code) {
            case 1: return FORWARD;
            case 2: return RESPONSE;
            case 3: return PUSH;
            case 4: return CONTROL;
            default: throw new IllegalArgumentException("unknown packet kind " + code);
        }
    }
}
