package com.slg.common.dispatch;

import com.google.protobuf.Message;

/**

 */
@FunctionalInterface
public interface MessageHandler<T extends Message> {

    void handle(HandlerContext ctx, T request) throws Exception;
}
