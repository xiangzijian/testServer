package com.slg.gateway.net;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.common.discovery.ServiceType;
import com.slg.common.msg.ErrorCodes;
import com.slg.common.msg.MsgIds;
import com.slg.common.msg.RouteTable;
import com.slg.common.net.client.ClientMessage;
import com.slg.common.proto.ErrorResp;
import com.slg.common.proto.HeartbeatReq;
import com.slg.common.proto.HeartbeatResp;
import com.slg.gateway.backend.BackendManager;
import com.slg.gateway.session.GatewaySession;
import com.slg.gateway.session.SessionManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端连接处理：会话生命周期、心跳、seq 去重、按消息 ID 路由。运行在 Netty IO 线程，不跑业务。
 */
@ChannelHandler.Sharable
public final class ClientHandler extends SimpleChannelInboundHandler<ClientMessage> {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    private final SessionManager sessions;
    private final BackendManager backends;
    private final RouteTable routes;
    private final int maxConnections;

    public ClientHandler(SessionManager sessions, BackendManager backends, RouteTable routes, int maxConnections) {
        this.sessions = sessions;
        this.backends = backends;
        this.routes = routes;
        this.maxConnections = maxConnections;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (sessions.size() >= maxConnections) {
            log.warn("connection limit {} reached, reject {}", maxConnections, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        GatewaySession s = sessions.create(ctx.channel());
        log.info("client connected {}", s);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        GatewaySession s = sessions.remove(ctx.channel());
        if (s != null) {
            log.info("client disconnected {}", s);
            backends.notifySessionClosed(s);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClientMessage msg) throws InvalidProtocolBufferException {
        GatewaySession s = SessionManager.of(ctx.channel());
        if (s == null) {
            return;
        }
        if (!s.acceptSeq(msg.seq())) {
            log.debug("{} duplicate/out-of-order seq {}, drop msgId={}", s, msg.seq(), msg.msgId());
            return;
        }
        // TODO 连接级 + 协议级令牌桶限流；未登录会话只允许登录/心跳协议；flags 解压/解密
        ServiceType target = routes.lookup(msg.msgId());
        if (target == null) {
            sendError(s, msg, ErrorCodes.UNKNOWN_MSG, "unknown msgId " + msg.msgId());
            return;
        }
        if (target == ServiceType.GATEWAY) {
            handleLocal(s, msg);
            return;
        }
        if (!backends.forward(target, s, msg)) {
            sendError(s, msg, ErrorCodes.BACKEND_UNAVAILABLE, target.lowerName() + " unavailable");
        }
    }

    private void handleLocal(GatewaySession s, ClientMessage msg) throws InvalidProtocolBufferException {
        switch (msg.msgId()) {
            case MsgIds.HEARTBEAT_REQ:
                HeartbeatReq.parseFrom(msg.body());
                s.send(ClientMessage.of(MsgIds.HEARTBEAT_RESP, msg.seq(),
                        HeartbeatResp.newBuilder().setServerTime(System.currentTimeMillis()).build()));
                break;
            default:
                sendError(s, msg, ErrorCodes.UNKNOWN_MSG, "unknown gateway msgId " + msg.msgId());
        }
    }

    private static void sendError(GatewaySession s, ClientMessage req, int code, String text) {
        s.send(ClientMessage.of(MsgIds.ERROR_RESP, req.seq(), ErrorResp.newBuilder()
                .setCode(code).setMessage(text).setReqMsgId(req.msgId()).build()));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent e && e.state() == IdleState.READER_IDLE) {
            log.info("client idle timeout {}", SessionManager.of(ctx.channel()));
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("client {} error: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
