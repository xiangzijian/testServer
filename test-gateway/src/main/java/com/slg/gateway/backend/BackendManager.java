package com.slg.gateway.backend;

import com.slg.common.discovery.ServiceDiscovery;
import com.slg.common.discovery.ServiceInstance;
import com.slg.common.discovery.ServiceType;
import com.slg.common.net.client.ClientMessage;
import com.slg.common.net.internal.ControlMsgIds;
import com.slg.common.net.internal.InternalClient;
import com.slg.common.net.internal.InternalPacket;
import com.slg.common.net.internal.PacketKind;
import com.slg.common.proto.SessionClosed;
import com.slg.common.util.NamedThreadFactory;
import com.slg.gateway.session.GatewaySession;
import com.slg.gateway.session.SessionManager;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * gateway 到后端（world/game）的内部连接管理：每个后端节点一条自动重连的长连接。
 * 负责请求转发、回包/推送回投到客户端会话、会话关闭通知。
 */
public final class BackendManager implements InternalClient.Listener {

    private static final Logger log = LoggerFactory.getLogger(BackendManager.class);
    private static final ServiceType[] BACKENDS = {ServiceType.GAME, ServiceType.WORLD};

    private final ServiceInstance self;
    private final ServiceDiscovery discovery;
    private final SessionManager sessions;
    private final InternalClient.Options options;
    private final EventLoopGroup group;
    private final Map<ServiceType, List<InternalClient>> clients = new EnumMap<>(ServiceType.class);
    private final AtomicLong traceSeq = new AtomicLong();

    public BackendManager(ServiceInstance self, ServiceDiscovery discovery, SessionManager sessions,
                          InternalClient.Options options, int ioThreads) {
        this.self = self;
        this.discovery = discovery;
        this.sessions = sessions;
        this.options = options;
        this.group = new NioEventLoopGroup(ioThreads, new NamedThreadFactory(self.key() + "-backend-io", false));
    }

    public void start() {
        for (ServiceType type : BACKENDS) {
            List<InternalClient> list = new ArrayList<>();
            for (ServiceInstance inst : discovery.getInstances(type)) {
                InternalClient c = new InternalClient(inst, self, group, this, options);
                list.add(c);
                c.start();
            }
            clients.put(type, List.copyOf(list));
            if (list.isEmpty()) {
                log.warn("[{}] no {} nodes configured (discovery.{}.nodes)", self.key(), type.lowerName(), type.lowerName());
            }
            // TODO 注册中心推送节点变化时增删连接
            discovery.subscribe(type, (t, instances) -> log.info("{} nodes changed: {}", t, instances));
        }
    }

    /**
     * 选择该会话在某类服务上的目标节点。
     * TODO 按设计文档 5.3：Game 按 playerId→kingdomId→节点组一致性哈希；World 按 kingdomId 定位。
     */
    InternalClient select(ServiceType type, GatewaySession session) {
        List<InternalClient> all = clients.getOrDefault(type, List.of());
        if (all.isEmpty()) {
            return null;
        }
        long key = session.playerId() != 0 ? session.playerId() : session.sessionId();
        int start = (int) ((key ^ (key >>> 32)) & 0x7fffffff) % all.size();
        for (int i = 0; i < all.size(); i++) {
            InternalClient c = all.get((start + i) % all.size());
            if (c.isReady()) {
                return c;
            }
        }
        return null;
    }

    /** @return false 表示该类后端当前无可用连接 */
    public boolean forward(ServiceType type, GatewaySession session, ClientMessage msg) {
        InternalClient c = select(type, session);
        if (c == null) {
            return false;
        }
        long traceId = ((long) (self.nodeId() & 0xFFFF) << 48) | (traceSeq.incrementAndGet() & 0xFFFF_FFFF_FFFFL);
        return c.send(new InternalPacket(PacketKind.FORWARD, msg.msgId(), msg.seq(), session.sessionId(),
                session.playerId(), session.kingdomId(), traceId, msg.body()));
    }

    /** 通知所有已连接后端：会话断开。 */
    public void notifySessionClosed(GatewaySession session) {
        InternalPacket p = InternalPacket.control(ControlMsgIds.SESSION_CLOSED, SessionClosed.newBuilder()
                .setSessionId(session.sessionId())
                .setPlayerId(session.playerId())
                .build());
        for (List<InternalClient> list : clients.values()) {
            for (InternalClient c : list) {
                c.send(p);
            }
        }
    }

    @Override
    public void onPacket(InternalClient client, InternalPacket p) {
        if (p.kind() != PacketKind.RESPONSE && p.kind() != PacketKind.PUSH) {
            log.warn("[{}] unexpected packet from {}: {}", self.key(), client.target(), p);
            return;
        }
        GatewaySession s = sessions.get(p.sessionId());
        if (s == null) {
            log.debug("session {} gone, drop {}", p.sessionId(), p);
            return;
        }
        // TODO 同 Tick 多条推送合并为 BatchPush；body 超阈值压缩
        s.send(new ClientMessage(p.msgId(), p.kind() == PacketKind.PUSH ? 0 : p.seq(), (byte) 0, p.body()));
    }

    @Override
    public void onDisconnected(InternalClient client) {
        log.warn("[{}] backend {} disconnected", self.key(), client.target());
    }

    public int readyCount(ServiceType type) {
        int n = 0;
        for (InternalClient c : clients.getOrDefault(type, List.of())) {
            if (c.isReady()) {
                n++;
            }
        }
        return n;
    }

    public void stop() {
        for (List<InternalClient> list : clients.values()) {
            list.forEach(InternalClient::close);
        }
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
    }
}
