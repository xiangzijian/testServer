package com.slg.common.dispatch;

import com.google.protobuf.Message;

/**
 * 业务 handler。在业务线程（{@link KeyedExecutor}）上执行，禁止阻塞调用。
 */
@FunctionalInterface
public interface MessageHandler<T extends Message> {

    void handle(HandlerContext ctx, T request) throws Exception;
}
