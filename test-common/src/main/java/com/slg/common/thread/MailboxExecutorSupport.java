package com.slg.common.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 按 id 串行的执行器（邮箱模型）的公共部分：每个 id 一个邮箱，所有邮箱共享固定数量的线程。
 * 同一 id 的任务严格按提交顺序执行，但不保证固定在同一条线程；不同 id 可并行，一个 id 的慢任务不会阻塞其他 id。
 * 约定：execute 正常返回的任务一定会被执行（唯一任务也可能被同 key 的新任务取代）；抛出拒绝异常的任务一定没有被执行。
 * 不支持 shutdown 后重新 start。
 */
public abstract class MailboxExecutorSupport<M extends AbstractMailbox, T> implements ManagedExecutor{

    private static final Logger log = LoggerFactory.getLogger(MailboxExecutorSupport.class);
    static final long DEFAULT_IDLE_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final long SWEEP_INTERVAL_SECONDS = 60;
    private static final int DEFAULT_PENDING_PER_THREAD_FACTOR = 16;

    private enum State { NEW, RUNNING, SHUTDOWN }

    private final String name;
    private final int threads;
    private final int mailboxCapacity;
    private final long maxPendingTasks;
    private final long idleNanos;
    private final ThreadPoolExecutor pool;
    private final ScheduledExecutorService sweeper;
    private final ConcurrentHashMap<Long, M> mailboxes = new ConcurrentHashMap<>();
    /** 提交持读锁、关闭持写锁：关闭开始时，所有已通过状态检查的提交都已入队。 */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final AtomicLong pending = new AtomicLong();
    private volatile State state = State.NEW;

    MailboxExecutorSupport(String name, int threads, int mailboxCapacity, long maxPendingTasks, long idleMillis) {
        if (mailboxCapacity <= 0) {
            throw new IllegalArgumentException("mailboxCapacity must be positive: " + mailboxCapacity);
        }
        if (maxPendingTasks <= 0) {
            throw new IllegalArgumentException("maxPendingTasks must be positive: " + maxPendingTasks);
        }
        this.name = name;
        this.threads = Math.max(1, threads);
        this.mailboxCapacity = mailboxCapacity;
        this.maxPendingTasks = maxPendingTasks;
        this.idleNanos = TimeUnit.MILLISECONDS.toNanos(idleMillis);
        // 池队列里放的是邮箱，每个邮箱最多出现一次，长度不超过活跃邮箱数，无界可以接受
        this.pool = new ThreadPoolExecutor(this.threads, this.threads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory(name, false));
        this.sweeper = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(name + "-sweeper", true));
    }

    /** 全局待执行上限的默认值：mailboxCapacity × threads × 16。 */
    static long defaultMaxPending(int mailboxCapacity, int threads) {
        return (long) mailboxCapacity * Math.max(1, threads) * DEFAULT_PENDING_PER_THREAD_FACTOR;
    }

    public final void start() {
        stateLock.writeLock().lock();
        try {
            if (state != State.NEW) {
                throw new IllegalStateException("executor " + name + " already started, restart is not supported");
            }
            state = State.RUNNING;
        } finally {
            stateLock.writeLock().unlock();
        }
        pool.prestartAllCoreThreads();
        sweeper.scheduleWithFixedDelay(this::sweepQuietly, SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * @throws RejectedExecutionException 未启动、已关闭、该 id 的队列已满或全局待执行数达到上限
     */
    public abstract void execute(long id, Runnable task);

    abstract M newMailbox(long id);

    abstract AbstractMailbox.OfferResult offer(M mailbox, T task);

    final void submit(long id, T task) {
        stateLock.readLock().lock();
        try {
            if (state != State.RUNNING) {
                throw rejected(id, 0);
            }
            for (;;) {
                M mailbox = mailboxes.computeIfAbsent(id, this::newMailbox);
                switch (offer(mailbox, task)) {
                    case ACCEPTED:
                        return;
                    case REJECTED:
                        throw rejected(id, mailbox.size());
                    case RETIRED:
                        mailboxes.remove(id, mailbox);
                        break;
                }
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * 已启动且还没开始关闭，此时才能提交任务。
     */
    public final boolean isRunning() {
        return state == State.RUNNING;
    }

    /**
     * 当前线程是否正在执行本执行器中该 id 的任务。
     */
    public final boolean inSlot(long id) {
        AbstractMailbox current = AbstractMailbox.current();
        return current != null && current.owner == this && current.id == id;
    }

    public final void assertInSlot(long id) {
        if (!inSlot(id)) {
            throw new IllegalStateException("not in task of id: name=" + name + ", id=" + id
                    + ", current=" + Thread.currentThread().getName());
        }
    }

    /**
     * 停止接收新任务，并在超时时间内等待已接收的任务全部执行完。
     *
     * @return 所有任务执行完且线程都已结束返回 true；超时或被中断返回 false
     */
    public final boolean shutdown(long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        stateLock.writeLock().lock();
        try {
            state = State.SHUTDOWN;
        } finally {
            stateLock.writeLock().unlock();
        }
        try {
            while (pending.get() > 0) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) {
                    return false;
                }
                Thread.sleep(Math.min(10, remainingMs));
            }
            sweeper.shutdownNow();
            pool.shutdown();
            return pool.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public final String name() {
        return name;
    }

    /**
     * 线程数量。
     */
    public final int size() {
        return threads;
    }

    public final int queueSize(long id) {
        M mailbox = mailboxes.get(id);
        return mailbox == null ? 0 : mailbox.size();
    }

    public final int mailboxCount() {
        return mailboxes.size();
    }

    /**
     * 已接收但还没执行完的任务数（包括正在执行的）。
     */
    public final int pendingTasks() {
        return (int) Math.min(Integer.MAX_VALUE, pending.get());
    }

    /**
     * 回收空闲超过阈值的邮箱，返回回收数量。
     */
    final int sweepIdleMailboxes() {
        long now = System.nanoTime();
        int removed = 0;
        for (M mailbox : mailboxes.values()) {
            if (mailbox.tryRetire(now, idleNanos) && mailboxes.remove(mailbox.id, mailbox)) {
                removed++;
            }
        }
        return removed;
    }

    final int mailboxCapacity() {
        return mailboxCapacity;
    }

    final boolean tryReservePending() {
        if (pending.incrementAndGet() > maxPendingTasks) {
            pending.decrementAndGet();
            return false;
        }
        return true;
    }

    /** 已占用名额但最终没有入队的任务归还名额。 */
    final void releasePending() {
        pending.decrementAndGet();
    }

    final void onExecuted() {
        pending.decrementAndGet();
    }

    final void schedule(AbstractMailbox mailbox) {
        pool.execute(mailbox);
    }

    private void sweepQuietly() {
        try {
            sweepIdleMailboxes();
        } catch (Throwable e) {
            log.error("executor {} failed to sweep idle mailboxes", name, e);
        }
    }

    private RejectedExecutionException rejected(long id, int queued) {
        return new RejectedExecutionException("executor rejected task: name=" + name + ", id=" + id
                + ", queued=" + queued + ", pending=" + pending.get() + ", running=" + (state == State.RUNNING));
    }
}
