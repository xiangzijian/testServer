package com.slg.common.storage.sync;

import com.slg.common.storage.dao.EntityDao;
import com.slg.common.storage.dao.RedisEntityDao;
import com.slg.common.storage.model.RowOp;
import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * 异步落库同步器（Write-Behind 第二级：Redis -> MySQL）。
 * <p>按分片轮流从 Redis 脏队列拉取 ownerId，批量写进 MySQL 并清理脏标记和墓碑；每个分片同步前先在 Redis
 * 获取租约，多节点同时运行也安全。不创建任何线程，由各服组装：
 * <ul>
 *     <li>同步执行器由外部传入，必须和本服务注册在同一阶段且先注册；</li>
 *     <li>{@link #tick()} 由外部定时器周期调用，间隔 = 期望同步周期 / 分片数；</li>
 *     <li>必须先于 {@link com.slg.common.storage.buffer.WriteBuffer} 注册，保证关闭时 WriteBuffer 先把内存变更写进 Redis，
 *     本服务再做最后一次同步。</li>
 * </ul>
 */
public final class DbSyncWorker implements ManagedExecutor {

    private static final Logger log = LoggerFactory.getLogger(DbSyncWorker.class);

    private final String name;
    private final RedisEntityDao redis;
    private final EntityDao mysql;
    private final UniqueSerialExecutor executor;
    private final int batchOwners;
    private final long leaseMillis;
    private final int shards;
    private final String leaseOwner;
    private final AtomicBoolean running = new AtomicBoolean();
    private final LongAdder syncedOwners = new LongAdder();
    private final LongAdder failures = new LongAdder();
    /** 只在调用 {@link #tick()} 的定时线程读写。 */
    private int shardCursor;

    /**
     * @param batchOwners 每次从分片取多少个 ownerId 同步
     * @param leaseMillis 分片租约时长，单次同步最多持续租约的一半
     */
    public DbSyncWorker(String name, RedisEntityDao redis, EntityDao mysql, UniqueSerialExecutor executor,
                        int batchOwners, long leaseMillis) {
        if (batchOwners <= 0 || leaseMillis <= 0) {
            throw new IllegalArgumentException("batchOwners and leaseMillis must be positive");
        }
        this.name = Objects.requireNonNull(name, "name");
        this.redis = Objects.requireNonNull(redis, "redis");
        this.mysql = Objects.requireNonNull(mysql, "mysql");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.batchOwners = batchOwners;
        this.leaseMillis = leaseMillis;
        this.shards = redis.shards();
        this.leaseOwner = name + "-" + UUID.randomUUID();
    }

    @Override
    public String name() {
        return name;
    }

    public int shards() {
        return shards;
    }

    /**
     * @throws IllegalStateException 同步执行器还没启动，说明注册顺序错了
     */
    @Override
    public void start() {
        if (!executor.isRunning()) {
            throw new IllegalStateException("db-sync " + name + ": executor " + executor.name()
                    + " is not running, register it before this service in the same phase");
        }
        running.set(true);
    }

    /** 提交下一个分片的同步；同一分片还在排队时只保留一个任务。由外部定时器周期调用，不会抛出异常。 */
    public void tick() {
        if (!running.get()) {
            return;
        }
        int shard = shardCursor;
        shardCursor = (shardCursor + 1) % shards;
        try {
            executor.execute(shard, "sync", () -> syncShard(shard, System.nanoTime() + leaseNanos() / 2));
        } catch (RejectedExecutionException e) {
            log.warn("db-sync {}: shard {} rejected by executor", name, shard);
        }
    }

    /**
     * 停止定时同步，把所有分片剩余的数据同步到 MySQL；不会关闭同步执行器。
     * 超时未同步完的数据仍保存在 Redis 中，下次启动后会继续同步。
     */
    @Override
    public boolean shutdown(long timeout, TimeUnit unit) {
        if (!running.compareAndSet(true, false)) {
            return true;
        }
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        if (!executor.isRunning()) {
            log.error("db-sync {}: executor {} already shut down, final sync skipped; check shutdown order",
                    name, executor.name());
            return false;
        }
        CountDownLatch done = new CountDownLatch(shards);
        AtomicBoolean ok = new AtomicBoolean(true);
        for (int i = 0; i < shards; i++) {
            int shard = i;
            try {
                executor.execute(shard, "final", () -> {
                    try {
                        if (!syncShard(shard, deadline)) {
                            ok.set(false);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            } catch (RejectedExecutionException e) {
                ok.set(false);
                done.countDown();
            }
        }
        try {
            if (!done.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                log.error("db-sync {}: final sync timed out, remaining rows stay in redis", name);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (!ok.get()) {
            log.error("db-sync {}: final sync incomplete, remaining rows stay in redis", name);
        }
        return ok.get();
    }

    public long syncedOwners() {
        return syncedOwners.sum();
    }

    public long failureCount() {
        return failures.sum();
    }

    /** 持有租约时反复同步该分片，直到没有剩余或到达截止时间；租约被其他节点持有时跳过。 */
    public boolean syncShard(int shard, long deadlineNanos) {
        try {
            if (!redis.tryLease(shard, leaseOwner, leaseMillis)) {
                return true;
            }
        } catch (RuntimeException e) {
            failures.increment();
            log.error("db-sync {}: lease of shard {} failed", name, shard, e);
            return false;
        }
        try {
            while (true) {
                List<RedisEntityDao.DirtyOwner> owners = redis.dirtyOwners(shard, batchOwners);
                for (RedisEntityDao.DirtyOwner owner : owners) {
                    syncOwner(owner);
                }
                syncedOwners.add(owners.size());
                if (owners.size() < batchOwners) {
                    return true;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    return false;
                }
            }
        } catch (RuntimeException e) {
            failures.increment();
            log.error("db-sync {}: shard {} failed", name, shard, e);
            return false;
        } finally {
            try {
                redis.releaseLease(shard, leaseOwner);
            } catch (RuntimeException e) {
                log.warn("db-sync {}: release lease of shard {} failed, it expires in {} ms", name, shard, leaseMillis);
            }
        }
    }

    public void syncOwner(RedisEntityDao.DirtyOwner owner) {
        long ownerId = owner.ownerId();
        List<RedisEntityDao.DirtyRow> dirty = redis.dirtyRows(ownerId);
        List<RowOp> ops = new ArrayList<>(dirty.size());
        for (RedisEntityDao.DirtyRow r : dirty) {
            if (r.row() != null) {
                ops.add(RowOp.upsert(r.row()));
            } else if (r.version() >= 0) {
                ops.add(RowOp.delete(r.type(), ownerId, r.keys(), r.version()));
            }
        }
        if (!ops.isEmpty()) {
            mysql.writeBatch(ops);
        }
        if (redis.clearSynced(ownerId, dirty) == 0) {
            redis.unindex(owner);
        }
    }

    private long leaseNanos() {
        return TimeUnit.MILLISECONDS.toNanos(leaseMillis);
    }
}
