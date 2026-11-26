package com.slg.game;

import com.slg.common.config.ConfigLoader;
import com.slg.common.config.ServerConfig;
import com.slg.common.db.DbManage;
import com.slg.common.discovery.ServiceType;
import com.slg.common.discovery.StaticServiceDiscovery;
import com.slg.common.id.OwnerIdAllocator;
import com.slg.common.id.OwnerIdSeq;
import com.slg.common.id.UidGenerator;
import com.slg.common.msg.MsgIds;
import com.slg.common.proto.PingReq;
import com.slg.common.redis.RedisManage;
import com.slg.common.sched.TimerEngine;
import com.slg.common.server.BackendServer;
import com.slg.common.storage.buffer.WriteBuffer;
import com.slg.common.storage.dao.MysqlEntityDao;
import com.slg.common.storage.dao.RedisEntityDao;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.repo.EntityRepository;
import com.slg.common.storage.sync.DbSyncWorker;
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
 * persist.db=game                   # 实体表所在的数据源名
 * persist.writerThreads=4           # 写入线程数（直连 MySQL 时需计入连接池配额）
 * persist.writerQueueCapacity=4096  # 单个 owner 的写入排队上限
 * persist.buckets=10                # 按 ownerId 分桶轮流刷出
 * persist.flushIntervalMillis=10000 # 每个 owner 的修改最长多久写出一次（启用 Redis 时默认 5000）
 * timer.tickMillis=100
 *
 * # 以下配置了 persist.redis 才启用：内存修改先批量写 Redis，再定时同步到 MySQL；不配则直连 MySQL
 * persist.redis=game                # Redis 数据源名，需在 redis.names 中
 * persist.redis.namespace=game      # Redis key 前缀，共用一个 Redis 的服必须不同
 * persist.redis.shards=16           # 待同步索引的分片数，部署后不能修改
 * persist.redis.ttlSeconds=86400    # 同步到 MySQL 后数据在 Redis 中保留多久，0 表示不过期
 * persist.sync.threads=2            # 同步线程数，占用 MySQL 连接池配额
 * persist.sync.intervalMillis=60000 # 每个分片多久同步一次
 * persist.sync.batchOwners=100
 * persist.sync.leaseMillis=30000
 * </pre>
 * Redis 配置了 redis.names 才启用，配置项见 {@link RedisManage} 和 {@link com.slg.common.redis.RedisConfig}。
 * PlayerActor 等见设计文档，均为 TODO。
 */
public final class GameServer {

    private static final Logger log = LoggerFactory.getLogger(GameServer.class);

    private final ThreadManage threads = new ThreadManage("game");
    private final TimerEngine timer;
    private final BackendServer backend;
    private final RedisManage redis;
    private final EntityRepository storage;
    private final UidGenerator uids;
    private final OwnerIdAllocator ids;

    public GameServer(ServerConfig config) {
        SerialExecutor biz = BackendServer.newBizExecutor(ServiceType.GAME, config);
        // TODO 会话关闭时通知对应 PlayerActor（下线/卸载流程）
        this.backend = new BackendServer(ServiceType.GAME, config, new StaticServiceDiscovery(config),
                (sessionId, playerId) -> log.info("session closed sessionId={} playerId={}", sessionId, playerId),
                biz);
        String node = backend.self().key();
        // TODO 停服时保存 uids.lastSecond()，启动时作为 startAfterSecond 传入，防止重启时钟回拨产生重复 id
        this.uids = new UidGenerator(Integer.parseInt(config.getRequired("uid.nodeId")));
        this.timer = new TimerEngine(node + "-timer", config.getLong("timer.tickMillis", 100), 512, -1);

        threads.register(Phase.BUSINESS, biz);
        threads.register(Phase.TIMER, timer);
        threads.register(Phase.INGRESS, backend);
        this.redis = config.getString("redis.names", "").isBlank()
                ? null : threads.register(Phase.RESOURCE, new RedisManage(config));
        this.storage = setupStorage(config, node);
        this.ids = storage == null ? null : new OwnerIdAllocator(storage.writeBuffer());
        registerHandlers();
    }

    /** 没有配置 db.names 时不启用持久化，返回 null。 */
    private EntityRepository setupStorage(ServerConfig config, String node) {
        if (config.getString("db.names", "").isBlank()) {
            log.info("[{}] db.names not configured, persistence disabled", node);
            return null;
        }
        DbManage dbManage = threads.register(Phase.RESOURCE, new DbManage(config));
        String dbName = config.getString("persist.db", "game");

        EntityRegistry registry = new EntityRegistry();
        registry.register(OwnerIdSeq.class);
        // 实体表在这里注册：registry.register(HeroData.class);
        MysqlEntityDao mysql = new MysqlEntityDao(registry, () -> dbManage.get(dbName));

        // 执行器先注册：同一阶段内先启动、后关闭，保证服务关闭时它们还在运行
        UniqueSerialExecutor writer = threads.register(Phase.PERSIST, new UniqueSerialExecutor(node + "-write-buffer",
                config.getInt("persist.writerThreads", 4), config.getInt("persist.writerQueueCapacity", 4096)));
        int buckets = config.getInt("persist.buckets", 10);
        String redisName = config.getString("persist.redis", "");

        RedisEntityDao redisDao = null;
        WriteBuffer buffer;
        long flushInterval;
        if (redisName.isBlank()) {
            buffer = threads.register(Phase.PERSIST, new WriteBuffer(node + "-write-buffer", registry,
                    (ownerId, ops) -> mysql.writeBatch(ops), writer, buckets));
            flushInterval = config.getLong("persist.flushIntervalMillis", 10_000);
        } else {
            if (redis == null) {
                throw new IllegalStateException("persist.redis=" + redisName + " but redis.names is not configured");
            }
            redisDao = new RedisEntityDao(config.getString("persist.redis.namespace", "game"),
                    () -> redis.get(redisName), registry,
                    config.getInt("persist.redis.shards", 16), config.getLong("persist.redis.ttlSeconds", 86_400));
            UniqueSerialExecutor syncExec = threads.register(Phase.PERSIST, new UniqueSerialExecutor(node + "-db-sync",
                    config.getInt("persist.sync.threads", 2), 16));
            // 同步器先于写缓冲注册：关闭时 WriteBuffer 先把内存变更写进 Redis，DbSyncWorker 再做最后一次同步
            DbSyncWorker sync = threads.register(Phase.PERSIST, new DbSyncWorker(node + "-db-sync", redisDao, mysql, syncExec,
                    config.getInt("persist.sync.batchOwners", 100), config.getLong("persist.sync.leaseMillis", 30_000)));
            buffer = threads.register(Phase.PERSIST, new WriteBuffer(node + "-write-buffer", registry,
                    redisDao::writeOwnerBatch, writer, buckets));
            flushInterval = config.getLong("persist.flushIntervalMillis", 5_000);
            long syncInterval = config.getLong("persist.sync.intervalMillis", 60_000);
            timer.scheduleWithFixedDelay(Math.max(1, syncInterval / sync.shards()), sync::tick);
        }
        timer.scheduleWithFixedDelay(Math.max(1, flushInterval / buffer.buckets()), buffer::tick);
        return new EntityRepository(registry, mysql, redisDao, buffer);
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

    /** 邮件、拍卖单、战报等全局对象的唯一 id，可在业务线程上直接调用。 */
    public UidGenerator uids() {
        return uids;
    }

    /** 武将、装备等归属实例的 id，按 ownerId 分配；未启用持久化时为 null。 */
    public OwnerIdAllocator ids() {
        return ids;
    }

    /** 没有配置 redis.names 时为 null。 */
    public RedisManage redis() {
        return redis;
    }

    /** 统一存储入口（读/写/删/刷出）；未启用持久化时为 null。 */
    public EntityRepository storage() {
        return storage;
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
