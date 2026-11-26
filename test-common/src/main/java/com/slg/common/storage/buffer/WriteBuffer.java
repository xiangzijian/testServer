package com.slg.common.storage.buffer;

import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.model.RowKey;
import com.slg.common.storage.model.RowOp;
import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 内存写缓冲（Write-Behind 第一级：内存 -> Redis，或直连 MySQL）。
 * <ul>
 *     <li>{@link #save}、{@link #saveNow}、{@link #delete} 只能在实体所属业务线程调用，调用时递增版本号并立即生成快照；</li>
 *     <li>同一 ownerId 在一个刷出周期内的所有变更（可跨多张表）按行合并为最新版本，刷出时以 ownerId 为单位批量写入；</li>
 *     <li>写入失败按 ownerId 指数退避重试，重试期间有新修改会自动并入同一批，旧版本永远不会覆盖新版本。</li>
 * </ul>
 * 不创建任何线程，由各服组装：
 * <ul>
 *     <li>写入执行器由外部传入并管理生命周期，必须和本服务注册在同一阶段且先注册；</li>
 *     <li>{@link #tick()} 由外部定时器周期调用，间隔 = 期望刷出周期 / 桶数；</li>
 *     <li>关闭时业务线程和定时器必须已经停止，剩余变更全部批量刷出并等待写完。</li>
 * </ul>
 */
public final class WriteBuffer implements ManagedExecutor {

    private static final Logger log = LoggerFactory.getLogger(WriteBuffer.class);
    private static final Object WRITE_KEY = "write";
    private static final long NO_RETRY = Long.MAX_VALUE;
    private static final long DEFAULT_RETRY_BASE_MILLIS = 200;
    private static final long DEFAULT_RETRY_MAX_MILLIS = TimeUnit.SECONDS.toMillis(30);

    /** 一个 ownerId 在当前刷出周期内修改过的行，只在 dirty.compute 内访问。 */
    private static final class OwnerDirty {
        final Map<RowKey, RowOp> ops = new LinkedHashMap<>();
    }

    /** 一个 ownerId 已提交但尚未写成功的行，只在 pending.compute 内访问。 */
    private static final class OwnerPending {
        final Map<RowKey, RowOp> ops = new LinkedHashMap<>();
        int attempts;
        long nextRetryNanos = NO_RETRY;
    }

    private final String name;
    private final EntityRegistry registry;
    private final BatchWriter writer;
    private final UniqueSerialExecutor executor;
    private final int buckets;
    private final long retryBaseNanos;
    private final long retryMaxNanos;

    private final ConcurrentHashMap<Long, OwnerDirty> dirty = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, OwnerPending> pending = new ConcurrentHashMap<>();
    private final AtomicInteger pendingRowCount = new AtomicInteger();
    private final LongAdder written = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final AtomicBoolean shutdownCalled = new AtomicBoolean();
    /** 只在调用 {@link #tick()} 的定时线程读写。 */
    private int bucketCursor;

    /**
     * @param executor 写入执行器，不要和其他用途共用
     * @param buckets  把 ownerId 分成多少桶，每次 {@link #tick()} 刷一个桶，用来摊平写入流量
     */
    public WriteBuffer(String name, EntityRegistry registry, BatchWriter writer,
                       UniqueSerialExecutor executor, int buckets) {
        this(name, registry, writer, executor, buckets, DEFAULT_RETRY_BASE_MILLIS, DEFAULT_RETRY_MAX_MILLIS);
    }

    WriteBuffer(String name, EntityRegistry registry, BatchWriter writer, UniqueSerialExecutor executor,
                int buckets, long retryBaseMillis, long retryMaxMillis) {
        if (buckets <= 0) {
            throw new IllegalArgumentException("buckets must be positive");
        }
        this.name = Objects.requireNonNull(name, "name");
        this.registry = Objects.requireNonNull(registry, "registry");
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

    public int buckets() {
        return buckets;
    }

    /**
     * @throws IllegalStateException 写入执行器还没启动，说明注册顺序错了
     */
    @Override
    public void start() {
        if (!executor.isRunning()) {
            throw new IllegalStateException("write-buffer " + name + ": executor " + executor.name()
                    + " is not running, register it before this service in the same phase");
        }
    }

    /** 业务修改完成后调用：版本号加 1，生成快照，等到所在桶轮到时按 owner 批量写出。 */
    public void save(SerialExecutor ownerExecutor, EntityData entity) {
        ownerExecutor.assertInSlot(entity.ownerId());
        RowOp op = registry.nextUpsert(entity);
        RowKey key = op.rowKey();
        dirty.compute(op.ownerId(), (id, od) -> {
            if (od == null) {
                od = new OwnerDirty();
            }
            od.ops.put(key, op);
            return od;
        });
    }

    /** 关键修改：版本号加 1，立即提交写出，不等待刷出周期。 */
    public void saveNow(SerialExecutor ownerExecutor, EntityData entity) {
        ownerExecutor.assertInSlot(entity.ownerId());
        RowOp op = registry.nextUpsert(entity);
        removeDirty(op.ownerId(), op.rowKey());
        mergeIntoPending(op.ownerId(), List.of(op));
        dispatch(op.ownerId());
    }

    /** 删除实体对应的行：丢弃该行还没提交的修改，立即提交一个带版本保护的删除。 */
    public void delete(SerialExecutor ownerExecutor, EntityData entity) {
        ownerExecutor.assertInSlot(entity.ownerId());
        RowOp op = registry.nextDelete(entity);
        removeDirty(op.ownerId(), op.rowKey());
        mergeIntoPending(op.ownerId(), List.of(op));
        dispatch(op.ownerId());
    }

    /** 立即提交一个 ownerId 的全部待写变更，例如下线或卸载时调用；任意线程可调用。 */
    public void flush(long ownerId) {
        // 先并入 pending 再移出 dirty，任何时刻至少有一边能看到这些修改（flushAndWait 依赖这一点）
        boolean[] enqueued = {false};
        dirty.computeIfPresent(ownerId, (id, od) -> {
            mergeIntoPending(ownerId, od.ops.values());
            enqueued[0] = true;
            return null;
        });
        if (enqueued[0]) {
            dispatch(ownerId);
        }
    }

    /** 立即提交所有 ownerId 的变更。 */
    public void flushAll() {
        for (Long ownerId : dirty.keySet()) {
            flush(ownerId);
        }
    }

    /**
     * 立即提交该 ownerId 的全部变更，并阻塞等待全部写进下一层存储。加载实体前调用，保证读到最新数据；
     * 会阻塞，只能在加载这类 IO 线程调用。
     *
     * @return 在超时前全部写完返回 true；返回 false 时不能继续加载，否则可能读到旧数据
     */
    public boolean flushAndWait(long ownerId, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        flush(ownerId);
        retryOwner(ownerId, true);
        while (dirty.containsKey(ownerId) || pending.containsKey(ownerId)) {
            if (!executor.isRunning()) {
                return false;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            retryOwner(ownerId, false);
            Thread.sleep(5);
        }
        return true;
    }

    /**
     * 提交剩余变更，并在超时时间内等待全部写成功；不会关闭写入执行器。
     * 调用前业务线程和定时器必须已经停止。重复调用只返回当前是否已全部写完。
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
                    log.error("write-buffer {}: executor {} already shut down, {} rows not written; check shutdown order",
                            name, executor.name(), pendingRows());
                    return false;
                }
                if (System.nanoTime() >= deadline) {
                    log.error("write-buffer {} shutdown timed out, {} rows not written", name, pendingRows());
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

    /** 刷下一个桶的 ownerId，并重试到期的失败批次。由外部定时器在同一个线程上周期调用，不会抛出异常。 */
    public void tick() {
        try {
            flushBucket(bucketCursor);
            bucketCursor = (bucketCursor + 1) % buckets;
            retryDue(false);
        } catch (Throwable e) {
            log.error("write-buffer {} tick failed", name, e);
        }
    }

    void flushBucket(int bucket) {
        for (Long ownerId : dirty.keySet()) {
            if (Math.floorMod(ownerId, buckets) == bucket) {
                flush(ownerId);
            }
        }
    }

    public int dirtyOwners() {
        return dirty.size();
    }

    public int pendingRows() {
        return pendingRowCount.get();
    }

    public long writtenCount() {
        return written.sum();
    }

    public long failureCount() {
        return failures.sum();
    }

    private void removeDirty(long ownerId, RowKey key) {
        dirty.computeIfPresent(ownerId, (id, od) -> {
            od.ops.remove(key);
            return od.ops.isEmpty() ? null : od;
        });
    }

    private void mergeIntoPending(long ownerId, Collection<RowOp> incoming) {
        pending.compute(ownerId, (id, op) -> {
            if (op == null) {
                op = new OwnerPending();
            }
            for (RowOp next : incoming) {
                RowKey rk = next.rowKey();
                RowOp prev = op.ops.get(rk);
                if (prev == null) {
                    op.ops.put(rk, next);
                    pendingRowCount.incrementAndGet();
                } else if (next.version() >= prev.version()) {
                    op.ops.put(rk, next);
                }
            }
            op.nextRetryNanos = NO_RETRY;
            return op;
        });
    }

    private void dispatch(long ownerId) {
        try {
            executor.execute(ownerId, WRITE_KEY, () -> writeOwner(ownerId));
        } catch (RejectedExecutionException e) {
            long next = System.nanoTime() + retryBaseNanos;
            pending.computeIfPresent(ownerId, (id, op) -> {
                if (op.nextRetryNanos == NO_RETRY) {
                    op.nextRetryNanos = next;
                }
                return op;
            });
        }
    }

    private void writeOwner(long ownerId) {
        List<RowOp> batch = new ArrayList<>();
        pending.computeIfPresent(ownerId, (id, op) -> {
            batch.addAll(op.ops.values());
            return op;
        });
        if (batch.isEmpty()) {
            return;
        }
        try {
            writer.writeOwnerBatch(ownerId, batch);
            boolean[] hasMore = {false};
            pending.computeIfPresent(ownerId, (id, op) -> {
                for (RowOp done : batch) {
                    RowKey rk = done.rowKey();
                    if (op.ops.get(rk) == done) {
                        op.ops.remove(rk);
                        pendingRowCount.decrementAndGet();
                    }
                }
                if (op.ops.isEmpty()) {
                    return null;
                }
                op.attempts = 0;
                op.nextRetryNanos = NO_RETRY;
                hasMore[0] = true;
                return op;
            });
            written.add(batch.size());
            if (hasMore[0]) {
                dispatch(ownerId);
            }
        } catch (RuntimeException e) {
            failures.increment();
            pending.computeIfPresent(ownerId, (id, op) -> {
                op.attempts++;
                long backoff = Math.min(retryMaxNanos, retryBaseNanos << Math.min(op.attempts - 1, 16));
                op.nextRetryNanos = System.nanoTime() + backoff;
                if (op.attempts == 1 || op.attempts % 10 == 0) {
                    log.error("write-buffer {} write failed, attempts={}, ownerId={}, rows={}",
                            name, op.attempts, ownerId, op.ops.size(), e);
                }
                return op;
            });
        }
    }

    private void retryDue(boolean force) {
        for (Long ownerId : pending.keySet()) {
            retryOwner(ownerId, force);
        }
    }

    private void retryOwner(long ownerId, boolean force) {
        long now = System.nanoTime();
        boolean[] shouldDispatch = {false};
        pending.computeIfPresent(ownerId, (id, op) -> {
            if (op.nextRetryNanos != NO_RETRY && (force || op.nextRetryNanos <= now)) {
                op.nextRetryNanos = NO_RETRY;
                shouldDispatch[0] = true;
            }
            return op;
        });
        if (shouldDispatch[0]) {
            dispatch(ownerId);
        }
    }
}
