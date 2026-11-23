package com.slg.common.thread;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 支持唯一任务的邮箱，加锁实现。
 * 唯一任务提交时会删除队列中同 uniqueKey 的旧任务，新任务排到队尾；已被取出执行的任务不受影响。
 */
final class UniqueMailbox extends AbstractMailbox {

    private final UniqueSerialExecutor executor;
    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<MailTask> queue = new ArrayDeque<>();
    /** 索引和队列在同一把锁下维护：索引中的任务一定还在队列里。 */
    private final HashMap<Object, MailTask> uniqueIndex = new HashMap<>();
    private boolean scheduled;
    private boolean retired;
    private long lastActiveNanos = System.nanoTime();

    UniqueMailbox(UniqueSerialExecutor executor, long id) {
        super(executor, id);
        this.executor = executor;
    }

    OfferResult offer(MailTask task) {
        boolean needSchedule;
        lock.lock();
        try {
            if (retired) {
                return OfferResult.RETIRED;
            }
            MailTask old = task.uniqueKey == null ? null : uniqueIndex.get(task.uniqueKey);
            if (old != null) {
                queue.remove(old);
                executor.onMerged();
            } else if (queue.size() >= owner.mailboxCapacity() || !owner.tryReservePending()) {
                return OfferResult.REJECTED;
            }
            queue.addLast(task);
            if (task.uniqueKey != null) {
                uniqueIndex.put(task.uniqueKey, task);
            }
            lastActiveNanos = System.nanoTime();
            needSchedule = !scheduled;
            scheduled = true;
        } finally {
            lock.unlock();
        }
        if (needSchedule) {
            owner.schedule(this);
        }
        return OfferResult.ACCEPTED;
    }

    @Override
    public void run() {
        enter();
        try {
            for (int i = 0; i < BATCH_SIZE; i++) {
                MailTask task;
                lock.lock();
                try {
                    task = queue.pollFirst();
                    if (task == null) {
                        scheduled = false;
                        lastActiveNanos = System.nanoTime();
                        return;
                    }
                    if (task.uniqueKey != null) {
                        uniqueIndex.remove(task.uniqueKey, task);
                    }
                } finally {
                    lock.unlock();
                }
                runTask(task.task);
                owner.onExecuted();
            }
        } finally {
            exit();
        }
        boolean more;
        lock.lock();
        try {
            more = !queue.isEmpty();
            if (!more) {
                scheduled = false;
                lastActiveNanos = System.nanoTime();
            }
        } finally {
            lock.unlock();
        }
        // 批次跑满后重新排到池队列末尾，让其他邮箱也有机会执行
        if (more) {
            owner.schedule(this);
        }
    }

    @Override
    boolean tryRetire(long nowNanos, long idleNanos) {
        lock.lock();
        try {
            if (!retired && !scheduled && queue.isEmpty() && nowNanos - lastActiveNanos >= idleNanos) {
                retired = true;
            }
            return retired;
        } finally {
            lock.unlock();
        }
    }

    @Override
    int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }
}
