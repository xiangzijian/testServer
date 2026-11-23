package com.slg.game;

import com.slg.common.config.ConfigLoader;
import com.slg.common.config.ServerConfig;
import com.slg.common.db.DbManage;
import com.slg.common.db.ModuleStore;
import com.slg.common.db.PersistService;
import com.slg.common.discovery.ServiceType;
import com.slg.common.discovery.StaticServiceDiscovery;
import com.slg.common.msg.MsgIds;
import com.slg.common.proto.PingReq;
import com.slg.common.sched.TimerEngine;
import com.slg.common.server.BackendServer;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.ThreadManage;
import com.slg.common.thread.ThreadManage.Phase;
import com.slg.common.thread.UniqueSerialExecutor;
import com.slg.game.handler.GamePingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Game 主城服入口，也是 game 服所有线程和组件的组装点：线程数、哪些用途隔离、启停顺序都在这里决定。
 * <p>持久化配置项（配置了 db.names 才启用，数据源配置见 {@link DbManage}）：
 * <pre>
 * persist.db=game                  # 实体表所在的数据源名
 * persist.writerThreads=4          # 写库线程数，加上其他访问该数据源的线程数不能超过连接池大小
 * persist.writerQueueCapacity=4096 # 单个玩家的写库排队上限
 * persist.buckets=10               # 按 playerId 分桶轮流刷盘
 * persist.flushIntervalMillis=10000 # 每个玩家的修改最长多久落库一次
 * timer.tickMillis=100
 * </pre>
 * PlayerActor 等见设计文档，均为 TODO。
 */
public final class GameServer {

    private static final Logger log = LoggerFactory.getLogger(GameServer.class);

    private final ThreadManage threads = new ThreadManage("game");
    private final TimerEngine timer;
    private final BackendServer backend;
    private final PersistService persist;

    public GameServer(ServerConfig config) {
        SerialExecutor biz = BackendServer.newBizExecutor(ServiceType.GAME, config);
        // TODO 会话关闭时通知对应 PlayerActor（下线/卸载流程）
        this.backend = new BackendServer(ServiceType.GAME, config, new StaticServiceDiscovery(config),
                (sessionId, playerId) -> log.info("session closed sessionId={} playerId={}", sessionId, playerId),
                biz);
        String node = backend.self().key();
        this.timer = new TimerEngine(node + "-timer", config.getLong("timer.tickMillis", 100), 512, -1);

        threads.register(Phase.BUSINESS, biz);
        threads.register(Phase.TIMER, timer);
        threads.register(Phase.INGRESS, backend);
        this.persist = setupPersist(config, node);
        registerHandlers();
    }

    /** 没有配置 db.names 时不启用持久化，返回 null。 */
    private PersistService setupPersist(ServerConfig config, String node) {
        if (config.getString("db.names", "").isBlank()) {
            log.info("[{}] db.names not configured, persistence disabled", node);
            return null;
        }
        DbManage dbManage = threads.register(Phase.RESOURCE, new DbManage(config));
        String dbName = config.getString("persist.db", "game");
        ModuleStore store = new ModuleStore(() -> dbManage.get(dbName));
        // 实体表在这里注册：store.register(HeroData.class);

        // 写库执行器先注册：同一阶段内先启动、后关闭，保证 PersistService 关闭刷盘时它还在运行
        UniqueSerialExecutor writer = threads.register(Phase.PERSIST, new UniqueSerialExecutor(node + "-db-writer",
                config.getInt("persist.writerThreads", 4), config.getInt("persist.writerQueueCapacity", 4096)));
        PersistService service = threads.register(Phase.PERSIST,
                new PersistService(node + "-persist", store, writer, config.getInt("persist.buckets", 10)));

        long flushInterval = config.getLong("persist.flushIntervalMillis", 10_000);
        timer.scheduleWithFixedDelay(Math.max(1, flushInterval / service.buckets()), service::tick);
        return service;
    }

    private void registerHandlers() {
        String nodeName = backend.self().key();
        backend.dispatcher()
                .register(MsgIds.GAME_PING_REQ, PingReq.parser(), new GamePingHandler(nodeName));
        // 新消息在此注册：.register(MsgIds.XXX_REQ, XxxReq.parser(), new XxxHandler())
    }

    public BackendServer backend() {
        return backend;
    }

    public TimerEngine timer() {
        return timer;
    }

    /** 未启用持久化时为 null。 */
    public PersistService persist() {
        return persist;
    }

    public void start() {
        threads.startAll();
    }

    public void stop() {
        if (!threads.shutdownAll()) {
            log.warn("[{}] some components did not shut down cleanly", backend.self().key());
        }
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ConfigLoader.load("game.properties");
        log.info("loading config from {}", config.source());
        GameServer server = new GameServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "game-shutdown"));
        server.start();
    }
}
