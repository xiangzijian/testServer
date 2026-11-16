package com.slg.common.thread;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;

/**
 * 按 id 串行、支持唯一任务的执行器，邮箱加锁实现。
 * <p>和 {@link SerialExecutor} 是两套独立的邮箱：同一个 id 在两个执行器中的任务互不保证顺序，可能同时执行。
 * 所以这里的任务只能处理快照或外部副作用（落库、刷新缓存、推送），不能修改个体的内存状态。
 */
public final class UniqueSerialExecutor extends MailboxExecutorSupport<UniqueMailbox, MailTask> {

    private final LongAdder merged = new LongAdder();

    /**
     * @param mailboxCapacity 单个 id 最多排队的任务数；全局待执行上限默认为 mailboxCapacity × threads × 16
     */
    public UniqueSerialExecutor(String name, int threads, int mailboxCapacity) {
        this(name, threads, mailboxCapacity, defaultMaxPending(mailboxCapacity, threads), DEFAULT_IDLE_MILLIS);
    }

    public UniqueSerialExecutor(String name, int threads, int mailboxCapacity, long maxPendingTasks,
                                long idleMillis) {
        super(name, threads, mailboxCapacity, maxPendingTasks, idleMillis);
    }

    @Override
    public void execute(long id, Runnable task) {
        execute(id, null, task);
    }

    /**
     * uniqueKey 不为 null 时为唯一任务：同一 id 下同 uniqueKey 还在排队的旧任务会被删除，新任务排到队尾。
     *
     * @throws RejectedExecutionException 未启动、已关闭、该 id 的队列已满或全局待执行数达到上限
     */
    public void execute(long id, Long uniqueKey, Runnable task) {
        submit(id, new MailTask(id, uniqueKey, Objects.requireNonNull(task, "task")));
    }

    /** 唯一任务被同 key 新任务取代的次数。 */
    public long mergedCount() {
        return merged.sum();
    }

    void onMerged() {
        merged.increment();
    }

    @Override
    UniqueMailbox newMailbox(long id) {
        return new UniqueMailbox(this, id);
    }

    @Override
    AbstractMailbox.OfferResult offer(UniqueMailbox mailbox, MailTask task) {
        return mailbox.offer(task);
    }
}
