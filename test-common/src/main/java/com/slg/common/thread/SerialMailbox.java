package com.slg.common.thread;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 普通任务邮箱，无锁实现（WIP 计数）。
 * <p>wip 记录“还没被执行方确认的提交次数”：提交方先入队再加一，把它从 0 变为 1 的提交方负责调度；
 * 执行方每轮减去自己已确认的次数，减到 0 才退出。所以 wip 大于 0 期间不会有第二个线程执行这个邮箱，
 * 执行方只要看到了某次加一，对应的任务一定已经在队列里。
 * <p>回收：wip 为 0（没有执行方、没有未确认的提交）且空闲足够久时，CAS 为 {@link #RETIRED}。
 * 提交方入队后发现已回收，就把自己的任务删掉并返回 RETIRED；删不掉说明上一轮执行方已经取走执行了，按接收处理。
 */
final class SerialMailbox extends AbstractMailbox {

    private static final int RETIRED = Integer.MIN_VALUE;

    /** 每次提交包一层 MailTask，保证按引用删除时只删掉自己的那一个。 */
    private final ConcurrentLinkedQueue<MailTask> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicInteger wip = new AtomicInteger();
    /** 只由执行方写入。 */
    private volatile long lastActiveNanos = System.nanoTime();

    SerialMailbox(MailboxExecutorSupport<?, ?> owner, long id) {
        super(owner, id);
    }

    OfferResult offer(MailTask task) {
        if (wip.get() == RETIRED) {
            return OfferResult.RETIRED;
        }
        if (size.incrementAndGet() > owner.mailboxCapacity()) {
            size.decrementAndGet();
            return OfferResult.REJECTED;
        }
        if (!owner.tryReservePending()) {
            size.decrementAndGet();
            return OfferResult.REJECTED;
        }
        queue.offer(task);
        for (;;) {
            int w = wip.get();
            if (w == RETIRED) {
                if (queue.remove(task)) {
                    size.decrementAndGet();
                    owner.releasePending();
                    return OfferResult.RETIRED;
                }
                return OfferResult.ACCEPTED;
            }
            if (wip.compareAndSet(w, w + 1)) {
                if (w == 0) {
                    owner.schedule(this);
                }
                return OfferResult.ACCEPTED;
            }
        }
    }

    @Override
    public void run() {
        boolean reschedule = false;
        enter();
        try {
            // 被调度时 wip 至少为 1，从 1 开始确认总是安全的；多出来的次数会在后续循环里减掉
            int missed = 1;
            int done = 0;
            outer:
            for (;;) {
                MailTask task;
                while ((task = queue.poll()) != null) {
                    size.decrementAndGet();
                    runTask(task.task);
                    owner.onExecuted();
                    if (++done == BATCH_SIZE) {
                        // 不确认，wip 保持大于 0，其他提交方不会重复调度
                        reschedule = true;
                        break outer;
                    }
                }
                lastActiveNanos = System.nanoTime();
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        } finally {
            exit();
        }
        // 批次跑满后重新排到池队列末尾，让其他邮箱也有机会执行
        if (reschedule) {
            owner.schedule(this);
        }
    }

    @Override
    boolean tryRetire(long nowNanos, long idleNanos) {
        if (wip.get() == RETIRED) {
            return true;
        }
        return nowNanos - lastActiveNanos >= idleNanos && wip.compareAndSet(0, RETIRED);
    }

    @Override
    int size() {
        return Math.max(0, size.get());
    }
}
