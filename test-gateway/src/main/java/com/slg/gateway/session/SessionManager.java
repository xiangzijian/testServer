package com.slg.gateway.session;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** sessionId → 会话。sessionId 在单个 gateway 进程内唯一（高 16 位为 gateway nodeId）。 */
public final class SessionManager {

    public static final AttributeKey<GatewaySession> SESSION = AttributeKey.valueOf("slg.session");

    private final Map<Long, GatewaySession> sessions = new ConcurrentHashMap<>();
    private final AtomicLong idGen = new AtomicLong();
    private final long idPrefix;

    public SessionManager(int gatewayNodeId) {
        this.idPrefix = ((long) (gatewayNodeId & 0xFFFF)) << 48;
    }

    public GatewaySession create(Channel channel) {
        GatewaySession s = new GatewaySession(idPrefix | idGen.incrementAndGet(), channel);
        sessions.put(s.sessionId(), s);
        channel.attr(SESSION).set(s);
        return s;
    }

    public GatewaySession remove(Channel channel) {
        GatewaySession s = channel.attr(SESSION).getAndSet(null);
        if (s != null) {
            sessions.remove(s.sessionId());
        }
        return s;
    }

    public GatewaySession get(long sessionId) {
        return sessions.get(sessionId);
    }

    public static GatewaySession of(Channel channel) {
        return channel.attr(SESSION).get();
    }

    public int size() {
        return sessions.size();
    }

    public Collection<GatewaySession> all() {
        return sessions.values();
    }
}
