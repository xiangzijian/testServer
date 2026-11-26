package com.slg.common.storage.repo;

import com.slg.common.redis.LocalRedis;
import com.slg.common.redis.Redis;
import com.slg.common.storage.buffer.WriteBuffer;
import com.slg.common.storage.dao.EntityDao;
import com.slg.common.storage.dao.RedisEntityDao;
import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.model.EntityRow;
import com.slg.common.storage.model.KeyCodec;
import com.slg.common.storage.model.RowOp;
import com.slg.common.storage.sync.DbSyncWorker;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import io.protostuff.Tag;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 完整存储链路集成测试：WriteBuffer -> RedisEntityDao -> DbSyncWorker -> FakeDb，以及 EntityRepository 读写。
 * 需要本机 redis-server，见 {@link LocalRedis}。
 */
class EntityRepositoryTest {

    static final class Hero extends EntityData {
        transient long ownerId;
        transient long heroId;
        @Tag(1) int level;

        Hero() {
        }

        Hero(long ownerId, long heroId, int level) {
            this.ownerId = ownerId;
            this.heroId = heroId;
            this.level = level;
        }

        @Override public long ownerId() { return ownerId; }
        @Override public String tableName() { return "player_hero"; }
        @Override public String[] keyName() { return new String[]{"player_id", "hero_id"}; }
        @Override public Object[] key() { return new Object[]{ownerId, heroId}; }

        @Override
        public void writeKeyParam(Object[] values) {
            ownerId = ((Number) values[0]).longValue();
            heroId = ((Number) values[1]).longValue();
        }
    }

    static final class BaseInfo extends EntityData {
        transient long ownerId;
        @Tag(1) String name;

        BaseInfo() {
        }

        BaseInfo(long ownerId, String name) {
            this.ownerId = ownerId;
            this.name = name;
        }

        @Override public long ownerId() { return ownerId; }
        @Override public String tableName() { return "owner_base"; }
        @Override public String[] keyName() { return new String[]{"owner_id"}; }
        @Override public Object[] key() { return new Object[]{ownerId}; }

        @Override
        public void writeKeyParam(Object[] values) {
            ownerId = ((Number) values[0]).longValue();
        }
    }

    /** 内存版 MySQL，带和真实 SQL 一样的版本保护。 */
    static final class FakeDb implements EntityDao {
        final Map<String, EntityRow> rows = new ConcurrentHashMap<>();
        final AtomicInteger readCount = new AtomicInteger();
        volatile boolean readForbidden;

        private final EntityRegistry registry;

        FakeDb(EntityRegistry registry) {
            this.registry = registry;
        }

        private String keyOf(Class<? extends EntityData> type, Object[] keys) {
            return registry.tableOf(type) + "|" + KeyCodec.encode(keys);
        }

        @Override
        public EntityRow get(Class<? extends EntityData> type, Object... keys) {
            if (readForbidden) {
                throw new AssertionError("should be served from redis");
            }
            readCount.incrementAndGet();
            return rows.get(keyOf(type, keys));
        }

        @Override
        public List<EntityRow> listByOwner(Class<? extends EntityData> type, long ownerId) {
            if (readForbidden) {
                throw new AssertionError("should be served from redis");
            }
            readCount.incrementAndGet();
            List<EntityRow> out = new ArrayList<>();
            for (EntityRow r : rows.values()) {
                if (r.type() == type && r.ownerId() == ownerId) {
                    out.add(r);
                }
            }
            return out;
        }

        @Override
        public void writeBatch(List<RowOp> ops) {
            for (RowOp op : ops) {
                String k = keyOf(op.type(), op.keys());
                if (op.isDelete()) {
                    rows.computeIfPresent(k, (key, old) -> old.version() <= op.version() ? null : old);
                } else {
                    rows.merge(k, op.toRow(), (old, cur) -> cur.version() > old.version() ? cur : old);
                }
            }
        }

        void seed(EntityData entity, long version) {
            EntityRow base = registry.snapshot(entity);
            EntityRow row = new EntityRow(base.type(), base.ownerId(), base.keys(), version, base.data());
            rows.put(keyOf(row.type(), row.keys()), row);
        }

        Integer heroLevel(long ownerId, long heroId) {
            EntityRow r = rows.get(keyOf(Hero.class, new Object[]{ownerId, heroId}));
            return r == null ? null : ((Hero) registry.decode(r)).level;
        }

        String baseName(long ownerId) {
            EntityRow r = rows.get(keyOf(BaseInfo.class, new Object[]{ownerId}));
            return r == null ? null : ((BaseInfo) registry.decode(r)).name;
        }
    }

    private static LocalRedis local;
    private static Redis redis;

    private SerialExecutor biz;
    private UniqueSerialExecutor writerExec;
    private UniqueSerialExecutor syncExec;
    private EntityRegistry registry;
    private FakeDb db;
    private RedisEntityDao redisDao;
    private WriteBuffer buffer;
    private DbSyncWorker syncWorker;
    private EntityRepository repo;

    @BeforeAll
    static void startServer() throws Exception {
        local = LocalRedis.start();
        redis = local.redis();
    }

    @AfterAll
    static void stopServer() {
        if (local != null) {
            local.stop();
        }
    }

    @BeforeEach
    void setUp() {
        redis.sync().flushdb();
        biz = new SerialExecutor("biz", 2, 1024);
        biz.start();
        writerExec = new UniqueSerialExecutor("writer", 2, 1024);
        writerExec.start();
        syncExec = new UniqueSerialExecutor("sync", 2, 16);
        syncExec.start();

        registry = new EntityRegistry();
        registry.register(Hero.class);
        registry.register(BaseInfo.class);
        db = new FakeDb(registry);
        redisDao = new RedisEntityDao("test", () -> redis, registry, 4, 3600);
        buffer = new WriteBuffer("wb", registry, redisDao::writeOwnerBatch, writerExec, 4);
        buffer.start();
        syncWorker = new DbSyncWorker("sync", redisDao, db, syncExec, 100, 10_000);
        syncWorker.start();
        repo = new EntityRepository(registry, db, redisDao, buffer);
    }

    @AfterEach
    void tearDown() {
        buffer.shutdown(2, TimeUnit.SECONDS);
        syncWorker.shutdown(2, TimeUnit.SECONDS);
        writerExec.shutdown(2, TimeUnit.SECONDS);
        syncExec.shutdown(2, TimeUnit.SECONDS);
        biz.shutdown(2, TimeUnit.SECONDS);
    }

    private void inSlot(long ownerId, Runnable action) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        biz.execute(ownerId, () -> {
            try {
                action.run();
                done.complete(null);
            } catch (Throwable e) {
                done.completeExceptionally(e);
            }
        });
        done.get(2, TimeUnit.SECONDS);
    }

    private void syncAll() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (int s = 0; s < redisDao.shards(); s++) {
            assertTrue(syncWorker.syncShard(s, deadline));
        }
    }

    @Test
    void saveFlushesToRedisThenSyncsToDbAcrossMultipleTables() throws Exception {
        inSlot(1001, () -> {
            repo.save(biz, new BaseInfo(1001, "Alice"));
            repo.save(biz, new Hero(1001, 1, 10));
            repo.save(biz, new Hero(1001, 2, 20));
        });

        // 读会自动把未刷出的缓冲写进 Redis
        BaseInfo base = repo.get(BaseInfo.class, 1001L);
        assertNotNull(base);
        assertEquals("Alice", base.name);
        assertEquals(1, base.getVersion());

        // 此时还没同步到 MySQL
        assertNull(db.baseName(1001));
        assertEquals(1, redisDao.dirtyOwners(redisDao.shardOf(1001), 10).size());

        syncAll();

        assertEquals("Alice", db.baseName(1001));
        assertEquals(10, db.heroLevel(1001, 1));
        assertEquals(20, db.heroLevel(1001, 2));
        assertTrue(redisDao.dirtyRows(1001).isEmpty());
        assertTrue(redisDao.dirtyOwners(redisDao.shardOf(1001), 10).isEmpty());
        assertTrue(redis.sync().ttl(redisDao.tableKey(Hero.class, 1001)) > 0);
        assertTrue(redis.sync().ttl(redisDao.tableKey(BaseInfo.class, 1001)) > 0);
    }

    @Test
    void getReadsRedisFirstFallsBackToDbAndBackfillsWithoutNullCaching() {
        // 不存在时查 DB 返回 null，且不回填空值
        assertNull(repo.get(BaseInfo.class, 2001L));
        assertEquals(1, db.readCount.get());
        assertNull(repo.get(BaseInfo.class, 2001L));
        assertEquals(2, db.readCount.get(), "missing row should not cache null");

        // DB 中有数据时回源并回填单行，下一次走 Redis 不碰 DB
        db.seed(new BaseInfo(2001, "Bob"), 3);
        BaseInfo first = repo.get(BaseInfo.class, 2001L);
        assertNotNull(first);
        assertEquals("Bob", first.name);
        assertEquals(3, first.getVersion());
        assertFalse(redisDao.readTable(BaseInfo.class, 2001).full(), "single-row backfill must not set _full");

        db.readForbidden = true;
        BaseInfo second = repo.get(BaseInfo.class, 2001L);
        assertNotNull(second);
        assertEquals("Bob", second.name);
        assertEquals(3, second.getVersion());
    }

    @Test
    void listMergesDbAndRedisByVersionThenBackfillsFullTable() {
        db.seed(new Hero(3001, 1, 11), 3);
        db.seed(new Hero(3001, 2, 21), 1);
        db.seed(new Hero(3001, 3, 31), 1);

        // Redis 中存在比 DB 更新的一行修改和一行墓碑（还未同步到 DB）
        Hero updated2 = new Hero(3001, 2, 22);
        EntityRow snap2 = registry.snapshot(updated2);
        redisDao.writeOwnerBatch(3001, List.of(
                RowOp.upsert(new EntityRow(Hero.class, 3001, snap2.keys(), 5, snap2.data())),
                RowOp.delete(Hero.class, 3001, new Object[]{3001L, 3L}, 2)));

        List<Hero> first = new ArrayList<>(repo.list(Hero.class, 3001));
        first.sort(Comparator.comparingLong(h -> h.heroId));

        assertEquals(2, first.size());
        assertEquals(11, first.get(0).level);
        assertEquals(3, first.get(0).getVersion());
        assertEquals(22, first.get(1).level);
        assertEquals(5, first.get(1).getVersion());
        assertTrue(redisDao.readTable(Hero.class, 3001).full());

        // 之后 list 和 get 都直接命中 Redis，不再查 DB
        db.readForbidden = true;
        List<Hero> second = repo.list(Hero.class, 3001);
        assertEquals(2, second.size());
        assertNull(repo.get(Hero.class, 3001L, 3L), "tombstone hides row");
        assertNull(repo.get(Hero.class, 3001L, 99L), "full table answers miss from redis");
    }

    @Test
    void deleteLeavesTombstoneUntilSyncedThenClears() throws Exception {
        Hero hero = new Hero(4001, 1, 10);
        inSlot(4001, () -> repo.save(biz, hero));
        assertTrue(buffer.flushAndWait(4001, 2, TimeUnit.SECONDS));
        syncAll();
        assertEquals(10, db.heroLevel(4001, 1));

        inSlot(4001, () -> repo.delete(biz, hero));
        assertNull(repo.get(Hero.class, 4001L, 1L), "tombstone hides row still in db");
        assertTrue(repo.list(Hero.class, 4001).isEmpty());

        syncAll();

        assertNull(db.heroLevel(4001, 1));
        assertFalse(redis.sync().hexists(redisDao.tableKey(Hero.class, 4001), "v:n4001|n1"), "tombstone cleared");
    }

    @Test
    void writeDuringSyncKeepsOwnerDirty() {
        Hero h1 = new Hero(5001, 1, 10);
        EntityRow r1 = registry.snapshot(h1);
        redisDao.writeOwnerBatch(5001, List.of(RowOp.upsert(new EntityRow(Hero.class, 5001, r1.keys(), 1, r1.data()))));

        RedisEntityDao.DirtyOwner dirtyOwner = redisDao.dirtyOwners(redisDao.shardOf(5001), 10).get(0);
        List<RedisEntityDao.DirtyRow> dirtyRows = redisDao.dirtyRows(5001);
        db.writeBatch(List.of(RowOp.upsert(dirtyRows.get(0).row())));

        // 同步期间写入新版本
        Hero h2 = new Hero(5001, 1, 11);
        EntityRow r2 = registry.snapshot(h2);
        redisDao.writeOwnerBatch(5001, List.of(RowOp.upsert(new EntityRow(Hero.class, 5001, r2.keys(), 2, r2.data()))));

        assertEquals(1, redisDao.clearSynced(5001, dirtyRows));
        redisDao.unindex(dirtyOwner);

        assertEquals(1, redisDao.dirtyOwners(redisDao.shardOf(5001), 10).size());
        syncAll();
        assertEquals(11, db.heroLevel(5001, 1));
    }

    @Test
    void tableExpiresOnlyWhenItsOwnDirtyRowsAreCleared() {
        Hero h = new Hero(6001, 1, 10);
        BaseInfo b = new BaseInfo(6001, "C");
        EntityRow rh = registry.snapshot(h);
        EntityRow rb = registry.snapshot(b);
        redisDao.writeOwnerBatch(6001, List.of(
                RowOp.upsert(new EntityRow(Hero.class, 6001, rh.keys(), 1, rh.data())),
                RowOp.upsert(new EntityRow(BaseInfo.class, 6001, rb.keys(), 1, rb.data()))));

        List<RedisEntityDao.DirtyRow> heroOnly = redisDao.dirtyRows(6001).stream()
                .filter(r -> r.type() == Hero.class).toList();
        assertEquals(1, redisDao.clearSynced(6001, heroOnly));

        assertTrue(redis.sync().ttl(redisDao.tableKey(Hero.class, 6001)) > 0);
        assertEquals(-1L, redis.sync().ttl(redisDao.tableKey(BaseInfo.class, 6001)));
    }

    @Test
    void leaseIsExclusive() {
        assertTrue(redisDao.tryLease(0, "a", 10_000));
        assertFalse(redisDao.tryLease(0, "b", 10_000));
        redisDao.releaseLease(0, "b");
        assertFalse(redisDao.tryLease(0, "b", 10_000));
        redisDao.releaseLease(0, "a");
        assertTrue(redisDao.tryLease(0, "b", 10_000));
    }

    @Test
    void shutdownFlushesWriteBufferThenSyncsAllShardsToDb() throws Exception {
        for (long id = 7000; id < 7010; id++) {
            long ownerId = id;
            inSlot(ownerId, () -> repo.save(biz, new Hero(ownerId, 1, (int) ownerId)));
        }

        // 先停 WriteBuffer（内存 -> Redis），再停 DbSyncWorker（Redis -> MySQL）
        assertTrue(buffer.shutdown(5, TimeUnit.SECONDS));
        assertTrue(syncWorker.shutdown(5, TimeUnit.SECONDS));

        for (long id = 7000; id < 7010; id++) {
            assertEquals((int) id, db.heroLevel(id, 1));
        }
        assertEquals(10, syncWorker.syncedOwners());
    }

    @Test
    void directMysqlModeWorksWithoutRedis() throws Exception {
        WriteBuffer directBuffer = new WriteBuffer("direct", registry, (ownerId, ops) -> db.writeBatch(ops), writerExec, 4);
        directBuffer.start();
        EntityRepository directRepo = new EntityRepository(registry, db, null, directBuffer);
        try {
            Hero hero = new Hero(8001, 1, 99);
            inSlot(8001, () -> directRepo.save(biz, hero));

            Hero loaded = directRepo.get(Hero.class, 8001L, 1L);
            assertNotNull(loaded);
            assertEquals(99, loaded.level);
            assertEquals(1, directRepo.list(Hero.class, 8001).size());

            inSlot(8001, () -> directRepo.delete(biz, hero));
            assertNull(directRepo.get(Hero.class, 8001L, 1L));
            assertTrue(directRepo.list(Hero.class, 8001).isEmpty());
        } finally {
            assertTrue(directBuffer.shutdown(2, TimeUnit.SECONDS));
        }
    }
}
