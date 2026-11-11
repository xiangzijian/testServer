package com.slg.gateway.session;

import com.slg.common.net.client.ClientMessage;
import io.netty.channel.Channel;

/**
 * 一个客户端连接对应的会话。除 playerId/kingdomId 外，其余字段只在该连接的 EventLoop 线程上访问。
 * TODO 断线重连会话保留 60s + 下行环形缓冲补发（设计文档 5.1）
 */
public final class GatewaySession {

    private final long sessionId;
    private final Channel channel;
    private final long createdAt = System.currentTimeMillis();

    /** 登录鉴权后设置；0 表示尚未登录。TODO 握手鉴权（校验 Login 签发的 Token） */
    private volatile long playerId;
    private volatile int kingdomId;
    private int lastSeq;

    GatewaySession(long sessionId, Channel channel) {
        this.sessionId = sessionId;
        this.channel = channel;
    }

    public long sessionId() {
        return sessionId;
    }

    public Channel channel() {
        return channel;
    }

    public long createdAt() {
        return createdAt;
    }

    public long playerId() {
        return playerId;
    }

    public void bindPlayer(long playerId, int kingdomId) {
        this.playerId = playerId;
        this.kingdomId = kingdomId;
    }

    public int kingdomId() {
        return kingdomId;
    }

    /**
     * 请求序号检查：seq 必须严格递增，重复/回退的请求视为重发并丢弃。seq = 0 表示不参与去重。
     *
     * @return true 表示接受该请求
     */
    public boolean acceptSeq(int seq) {
        if (seq == 0) {
            return true;
        }
        if (seq <= lastSeq) {
            return false;
        }
        lastSeq = seq;
        return true;
    }

    public void send(ClientMessage msg) {
        if (channel.isActive()) {
            channel.writeAndFlush(msg);
        }
    }

    @Override
    public String toString() {
        return "Session{" + sessionId + ", player=" + playerId + ", remote=" + channel.remoteAddress() + "}";
    }
}
