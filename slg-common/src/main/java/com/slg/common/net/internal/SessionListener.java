package com.slg.common.net.internal;

/** 后端服监听 gateway 的会话事件。回调在该会话对应的业务线程上执行。 */
@FunctionalInterface
public interface SessionListener {

    SessionListener NOOP = (sessionId, playerId) -> {
    };

    void onSessionClosed(long sessionId, long playerId);
}
