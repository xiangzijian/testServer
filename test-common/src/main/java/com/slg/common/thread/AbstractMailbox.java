package com.slg.common.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 单个 id 的任务队列。有任务时把自己交给共享线程池执行，同一时刻最多只有一个线程在执行它，
 * 所以同一个 id 的任务严格按提交顺序串行执行，但前后两批可能在不同线程上。
 */
abstract class AbstractMailbox implements Runnable {

    enum OfferResult { ACCEPTED, REJECTED, RETIRED }

    static final int BATCH_SIZE = 16;

    private static final Logger log = LoggerFactory.getLogger(AbstractMailbox.class);
    private static final long SLOW_TASK_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final ThreadLocal<AbstractMailbox> CURRENT = new ThreadLocal<>();

    final MailboxExecutorSupport<?, ?> owner;
    final long id;

    AbstractMailbox(MailboxExecutorSupport<?, ?> owner, long id) {
        this.owner = owner;
        this.id = id;
    }

    static AbstractMailbox current() {
        return CURRENT.get();
    }

    /**
     * 空闲足够久时标记为已回收，之后的提交会返回 {@link OfferResult#RETIRED}，由调用方换一个新邮箱重试。
     *
     * @return 邮箱是否处于已回收状态
     */
    abstract boolean tryRetire(long nowNanos, long idleNanos);

    abstract int size();

    final void enter() {
        CURRENT.set(this);
    }

    final void exit() {
        CURRENT.remove();
    }

    final void runTask(Runnable task) {
        long start = System.nanoTime();
        try {
            task.run();
        } catch (Throwable e) {
            log.error("task failed, executor={} id={}", owner.name(), id, e);
        } finally {
            // 残留的中断标志会影响同一线程上后续执行的其他邮箱
            Thread.interrupted();
        }
        long cost = System.nanoTime() - start;
        if (cost > SLOW_TASK_NANOS) {
            log.warn("slow task, executor={} id={} cost={}ms",
                    owner.name(), id, TimeUnit.NANOSECONDS.toMillis(cost));
        }
    }
}
