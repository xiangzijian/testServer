package com.slg.common.db;

import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * 实体落库调度：业务线程登记修改，按 playerId 分桶轮流刷盘，按行合并写入并在失败时重试。
 * <ul>
 *     <li>{@link #update}、{@link #updateNow}、{@link #delete} 只能在实体所属的业务线程调用，调用时立即生成快照；</li>
 *     <li>同一行（实体类型 + 主键）在一个刷盘周期内多次修改只写最后一次；同一玩家的写入按提交顺序串行执行；</li>
 *     <li>每一行在写成功之前一直保留最新版本，写失败按退避间隔重试，旧版本永远不会覆盖新版本。</li>
 * </ul>
 * 不创建任何线程，由 server 组装：
 * <ul>
 *     <li>写库执行器由外部传入并由外部管理生命周期；它必须和本服务注册在同一阶段且先注册，保证本服务先关闭（刷盘）、执行器后关闭；</li>
 *     <li>{@link #tick()} 由外部定时器周期调用，间隔 = 期望的刷盘周期 / 桶数；</li>
 *     <li>关闭时业务线程和定时器必须已经停止，剩余修改全部提交并等待写完。</li>
 * </ul>
 * 删除后用同一主键重建的实体，版本号必须从被删除实体的版本继续递增，否则新数据会被版本保护拦截。
 */
public final class PersistService implements ManagedExecutor {

    private static final Logger log = LoggerFactory.getLogger(PersistService.class);
    private static final long NO_RETRY = Long.MAX_VALUE;
    private static final long DEFAULT_RETRY_BASE_MILLIS = 200;
    private static final long DEFAULT_RETRY_MAX_MILLIS = TimeUnit.SECONDS.toMillis(30);

    /**
     * 一行数据的身份：实体类型 + 主键值。也是写入执行器的合并 key。
     */
    record RowKey(Class<? extends ModuleData> type, List<Object> keys) {
        static RowKey of(ModuleRow row) {
            return new RowKey(row.type(), List.of(row.keys()));
        }
    }

    /**
     * 一行待写入的最新操作；row 为 null 表示删除。
     */
    private record Op(RowKey key, long playerId, long version, ModuleRow row, int attempts, long nextRetryNanos) {
        Op retryAt(int attempts, long nextRetryNanos) {
            return new Op(key, playerId, version, row, attempts, nextRetryNanos);
        }
    }

    /**
     * 一个玩家在当前刷盘周期内修改过的行，只在 {@link ConcurrentHashMap#compute} 内修改。
     */
    private static final class PlayerDirty {
        final Map<RowKey, ModuleRow> rows = new LinkedHashMap<>();
    }

    private final String name;
    private final ModuleWriter writer;
    private final UniqueSerialExecutor executor;
    private final int buckets;
    private final long retryBaseNanos;
    private final long retryMaxNanos;
    private final ConcurrentHashMap<Long, PlayerDirty> dirty = new ConcurrentHashMap<>();
    /**
     * 已提交但还没写成功的行，每行只保留版本最高的操作。
     */
    private final ConcurrentHashMap<RowKey, Op> pending = new ConcurrentHashMap<>();
    private final LongAdder written = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final AtomicBoolean shutdownCalled = new AtomicBoolean();
    /**
     * 只在调用 {@link #tick()} 的定时线程读写。
     */
    private int bucketCursor;

    /**
     * @param executor 写库执行器，线程数加上其他访问同一数据源的线程数不能超过连接池大小；不要和其他用途共用
     * @param buckets  把玩家按 playerId 分成多少桶，每次 {@link #tick()} 刷一个桶，用来摊平写入流量
     */
    public PersistService(String name, ModuleStore store, UniqueSerialExecutor executor, int buckets) {
        this(name, store, executor, buckets, DEFAULT_RETRY_BASE_MILLIS, DEFAULT_RETRY_MAX_MILLIS);
    }

    PersistService(String name, ModuleWriter writer, UniqueSerialExecutor executor, int buckets,
                   long retryBaseMillis, long retryMaxMillis) {
        if (buckets <= 0) {
            throw new IllegalArgumentException("buckets must be positive");
        }
        this.name = name;
        this.writer = Objects.requireNonNull(writer, "writer");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.buckets = buckets;
        this.retryBaseNanos = TimeUnit.MILLISECONDS.toNanos(retryBaseMillis);
        this.retryMaxNanos = TimeUnit.MILLISECONDS.toNanos(retryMaxMillis);
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * @throws IllegalStateException 写库执行器还没启动，说明注册顺序错了
     */
    @Override
    public void start() {
        if (!executor.isRunning()) {
            throw new IllegalStateException("persist " + name + ": executor " + executor.name()
                    + " is not running, register it before this service in the same phase");
        }
    }

    public int buckets() {
        return buckets;
    }

    /**
     * 业务修改完成后调用：版本号加 1，生成快照，等到所在的桶轮到时落库。
     */
    public void update(SerialExecutor owner, ModuleData entity) {
        owner.assertInSlot(entity.playerId());
        entity.addVersion();
        ModuleRow row = writer.snapshot(entity);
        RowKey key = RowKey.of(row);
        dirty.compute(row.playerId(), (id, pd) -> {
            if (pd == null) {
                pd = new PlayerDirty();
            }
            pd.rows.put(key, row);
            return pd;
        });
    }

    /**
     * 关键修改：版本号加 1，立即提交落库，不等待刷盘周期。
     */
    public void updateNow(SerialExecutor owner, ModuleData entity) {
        owner.assertInSlot(entity.playerId());
        entity.addVersion();
        ModuleRow row = writer.snapshot(entity);
        RowKey key = RowKey.of(row);
        removeDirty(row.playerId(), key);
        submit(new Op(key, row.playerId(), row.version(), row, 0, NO_RETRY));
    }

    /**
     * 删除实体对应的行：丢弃还没提交的修改，提交一个带版本保护的删除。
     */
    public void delete(SerialExecutor owner, ModuleData entity) {
        owner.assertInSlot(entity.playerId());
        entity.addVersion();
        RowKey key = new RowKey(entity.getClass(), List.of(entity.key()));
        removeDirty(entity.playerId(), key);
        submit(new Op(key, entity.playerId(), entity.getVersion(), null, 0, NO_RETRY));
    }

    /**
     * 立即提交一个玩家的全部修改，例如下线或卸载时调用；任意线程可调用。
     */
    public void flushPlayer(long playerId) {
        PlayerDirty pd = dirty.remove(playerId);
        if (pd == null) {
            return;
        }
        for (Map.Entry<RowKey, ModuleRow> e : pd.rows.entrySet()) {
            ModuleRow row = e.getValue();
            submit(new Op(e.getKey(), playerId, row.version(), row, 0, NO_RETRY));
        }
    }

    /**
     * 立即提交所有玩家的修改。
     */
    public void flushAll() {
        for (Long playerId : dirty.keySet()) {
            flushPlayer(playerId);
        }
    }

    /**
     * 提交剩余修改，并在超时时间内等待全部写成功；不会关闭写库执行器。
     * 调用前业务线程和定时器必须已经停止。重复调用只返回当前是否已全部写完。
     *
     * @return 全部写成功返回 true；超时或写库执行器已提前关闭返回 false
     */
    @Override
    public boolean shutdown(long timeout, TimeUnit unit) {
        if (!shutdownCalled.compareAndSet(false, true)) {
            return pending.isEmpty() && dirty.isEmpty();
        }
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        flushAll();
        try {
            while (!pending.isEmpty()) {
                if (!executor.isRunning()) {
                    log.error("persist {}: executor {} already shut down, {} rows not written; check shutdown order",
                            name, executor.name(), pending.size());
                    return false;
                }
                if (System.nanoTime() >= deadline) {
                    log.error("persist {} shutdown timed out, {} rows not written", name, pending.size());
                    return false;
                }
                retryDue(true);
                Thread.sleep(Math.min(50, TimeUnit.NANOSECONDS.toMillis(retryBaseNanos) + 1));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    /**
     * 还没提交的修改涉及的玩家数。
     */
    public int dirtyPlayers() {
        return dirty.size();
    }

    /**
     * 已提交但还没写成功的行数，持续增长说明数据库写不进去。
     */
    public int pendingRows() {
        return pending.size();
    }

    public long writtenCount() {
        return written.sum();
    }

    public long failureCount() {
        return failures.sum();
    }

    /**
     * 刷下一个桶的玩家，并重试到期的失败写入。由外部定时器在同一个线程上周期调用，不会抛出异常。
     */
    public void tick() {
        try {
            flushBucket(bucketCursor);
            bucketCursor = (bucketCursor + 1) % buckets;
            retryDue(false);
        } catch (Throwable e) {
            log.error("persist {} tick failed", name, e);
        }
    }

    void flushBucket(int bucket) {
        for (Long playerId : dirty.keySet()) {
            if (Math.floorMod(playerId, buckets) == bucket) {
                flushPlayer(playerId);
            }
        }
    }

    private void removeDirty(long playerId, RowKey key) {
        dirty.computeIfPresent(playerId, (id, pd) -> {
            pd.rows.remove(key);
            return pd.rows.isEmpty() ? null : pd;
        });
    }

    private void submit(Op op) {
        pending.merge(op.key(), op, (old, cur) -> cur.version() >= old.version() ? cur : old);
        dispatch(op.key(), op.playerId());
    }

    private void dispatch(RowKey key, long playerId) {
        try {
            executor.execute(playerId, key, () -> writeLatest(key));
        } catch (RejectedExecutionException e) {
            long next = System.nanoTime() + retryBaseNanos;
            pending.computeIfPresent(key, (k, cur) -> cur.retryAt(cur.attempts(), next));
        }
    }

    /**
     * 写这一行当前最新的操作：同一行排队中的旧任务已被合并，失败重试的任务也总是写最新版本。
     */
    private void writeLatest(RowKey key) {
        Op op = pending.get(key);
        if (op == null) {
            return;
        }
        try {
            if (op.row() != null) {
                writer.upsert(op.row());
            } else {
                writer.delete(key.type(), key.keys().toArray(), op.version());
            }
            pending.computeIfPresent(key, (k, cur) -> cur == op ? null : cur);
            written.increment();
        } catch (RuntimeException e) {
            onFailure(op, e);
        }
    }

    private void onFailure(Op op, RuntimeException e) {
        failures.increment();
        int attempts = op.attempts() + 1;
        long backoff = Math.min(retryMaxNanos, retryBaseNanos << Math.min(attempts - 1, 16));
        long next = System.nanoTime() + backoff;
        pending.computeIfPresent(op.key(), (k, cur) -> cur == op ? op.retryAt(attempts, next) : cur);
        if (attempts == 1 || attempts % 10 == 0) {
            log.error("persist {} write failed, attempts={}, key={}, version={}",
                    name, attempts, op.key(), op.version(), e);
        }
    }

    private void retryDue(boolean force) {
        long now = System.nanoTime();
        List<Op> due = new ArrayList<>();
        for (Op op : pending.values()) {
            if (op.nextRetryNanos() != NO_RETRY && (force || op.nextRetryNanos() <= now)) {
                due.add(op);
            }
        }
        for (Op op : due) {
            pending.computeIfPresent(op.key(), (k, cur) -> cur == op ? cur.retryAt(cur.attempts(), NO_RETRY) : cur);
            dispatch(op.key(), op.playerId());
        }
    }
}
