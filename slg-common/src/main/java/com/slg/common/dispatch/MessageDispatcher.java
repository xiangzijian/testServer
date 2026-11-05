package com.slg.common.dispatch;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.slg.common.msg.ErrorCodes;
import com.slg.common.net.internal.InternalPacket;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息 ID → handler 的注册与分发。
 * <pre>
 * dispatcher.register(MsgIds.GAME_PING_REQ, PingReq.parser(), new GamePingHandler());
 * </pre>
 * IO 线程调用 {@link #dispatch}，解析与 handler 执行都在 {@link KeyedExecutor} 上完成。
 */
public final class MessageDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MessageDispatcher.class);
    private static final long SLOW_HANDLER_NANOS = 20_000_000L;

    private final Map<Integer, Registration<?>> handlers = new ConcurrentHashMap<>();
    private final KeyedExecutor executor;

    private record Registration<T extends Message>(Parser<T> parser, MessageHandler<T> handler) {
        void invoke(HandlerContext ctx, byte[] body) throws Exception {
            T req;
            try {
                req = parser.parseFrom(body);
            } catch (InvalidProtocolBufferException e) {
                ctx.replyError(ErrorCodes.BAD_REQUEST, "bad body for msgId " + ctx.msgId());
                return;
            }
            handler.handle(ctx, req);
        }
    }

    public MessageDispatcher(KeyedExecutor executor) {
        this.executor = executor;
    }

    public <T extends Message> MessageDispatcher register(int msgId, Parser<T> parser, MessageHandler<T> handler) {
        Registration<?> old = handlers.putIfAbsent(msgId, new Registration<>(parser, handler));
        if (old != null) {
            throw new IllegalStateException("duplicate handler for msgId " + msgId);
        }
        return this;
    }

    public boolean hasHandler(int msgId) {
        return handlers.containsKey(msgId);
    }

    public void dispatch(Channel channel, InternalPacket packet) {
        long key = packet.playerId() != 0 ? packet.playerId() : packet.sessionId();
        executor.execute(key, () -> invoke(channel, packet));
    }

    /** 在与该 key 的消息相同的业务线程上执行任意任务（如会话关闭清理），保证顺序。 */
    public void execute(long key, Runnable task) {
        executor.execute(key, task);
    }

    private void invoke(Channel channel, InternalPacket packet) {
        HandlerContext ctx = new HandlerContext(channel, packet);
        Registration<?> reg = handlers.get(packet.msgId());
        if (reg == null) {
            log.warn("no handler for msgId={} session={}", packet.msgId(), packet.sessionId());
            ctx.replyError(ErrorCodes.NO_HANDLER, "no handler for msgId " + packet.msgId());
            return;
        }
        long start = System.nanoTime();
        try {
            reg.invoke(ctx, packet.body());
        } catch (Exception e) {
            log.error("handler error msgId={} session={}", packet.msgId(), packet.sessionId(), e);
            ctx.replyError(ErrorCodes.SERVER_ERROR, "server error");
        } finally {
            long cost = System.nanoTime() - start;
            if (cost > SLOW_HANDLER_NANOS) {
                log.warn("slow handler msgId={} cost={}ms", packet.msgId(), cost / 1_000_000);
            }
        }
    }
}
