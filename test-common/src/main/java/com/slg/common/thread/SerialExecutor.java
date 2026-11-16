package com.slg.common.thread;

import java.util.Objects;

/**
 * 按 id 串行的普通执行器，邮箱无锁实现。业务状态只应在这里的任务中修改。
 */
public final class SerialExecutor extends MailboxExecutorSupport<SerialMailbox, MailTask> {

    /**
     * @param mailboxCapacity 单个 id 最多排队的任务数；全局待执行上限默认为 mailboxCapacity × threads × 16
     */
    public SerialExecutor(String name, int threads, int mailboxCapacity) {
        this(name, threads, mailboxCapacity, defaultMaxPending(mailboxCapacity, threads), DEFAULT_IDLE_MILLIS);
    }

    public SerialExecutor(String name, int threads, int mailboxCapacity, long maxPendingTasks, long idleMillis) {
        super(name, threads, mailboxCapacity, maxPendingTasks, idleMillis);
    }

    @Override
    public void execute(long id, Runnable task) {
        submit(id, new MailTask(id, null, Objects.requireNonNull(task, "task")));
    }

    @Override
    SerialMailbox newMailbox(long id) {
        return new SerialMailbox(this, id);
    }

    @Override
    AbstractMailbox.OfferResult offer(SerialMailbox mailbox, MailTask task) {
        return mailbox.offer(task);
    }
}
