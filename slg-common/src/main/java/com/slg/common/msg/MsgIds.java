package com.slg.common.msg;

/**
 * 客户端协议消息 ID。按模块分段（设计文档 5.1）：
 * <ul>
 *     <li>1xxxx：Gateway 本地 / 登录</li>
 *     <li>2xxxx：Game 主城服</li>
 *     <li>3xxxx：World 世界服</li>
 *     <li>4xxxx：Alliance（预留）……</li>
 * </ul>
 * 约定：请求为奇数，对应回包 = 请求 + 1。
 */
public final class MsgIds {

    private MsgIds() {
    }

    // ---- 1xxxx gateway ----
    public static final int HEARTBEAT_REQ = 10001;
    public static final int HEARTBEAT_RESP = 10002;
    public static final int ERROR_RESP = 10099;

    // ---- 2xxxx game ----
    public static final int GAME_PING_REQ = 20001;
    public static final int GAME_PING_RESP = 20002;

    // ---- 3xxxx world ----
    public static final int WORLD_PING_REQ = 30001;
    public static final int WORLD_PING_RESP = 30002;
}
