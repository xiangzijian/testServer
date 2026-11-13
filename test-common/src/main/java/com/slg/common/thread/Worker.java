package com.slg.common.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个槽位：一条线程 + 一个有界队列，队列内任务严格按入队顺序执行。
 * 约定：{@link #offer} 返回 true 的任务一定会被执行，返回 false 的任务一定没有被执行。
 */
final class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private static final int BATCH_SIZE = 16;
    private static final long POLL_TIMEOUT_MS = 100;
    private static final long SLOW_TASK_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private final Thread thread;
    private final BlockingQueue<SlotTask> queue;
    private final ConcurrentHashMap<Long, SlotTask> uniqueMap;
    /**
     * 保护 running 的检查与入队、以及 running 的修改，使二者不会交错。
     */
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean running;
    private boolean started;

    Worker(String name, int index, int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.thread = new Thread(this::runLoop, name + "-" + index);
        this.thread.setDaemon(false);
        this.thread.setUncaughtExceptionHandler(NamedThreadFactory.LOGGING_HANDLER);
        this.uniqueMap = new ConcurrentHashMap<>(capacity);
    }

    void start() {
        lock.lock();
        try {
            if (started) {
                throw new IllegalStateException("worker " + thread.getName() + " already started, restart is not supported");
            }
            started = true;
            running = true;
        } finally {
            lock.unlock();
        }
        thread.start();
    }

    boolean offer(SlotTask task) {
        lock.lock();
        try {
            if (!running) {
                return false;
            }
            if (task.uniqueKey() != null) {
                if (uniqueMap.get(task.uniqueKey()) != null) {
                    queue.remove(uniqueMap.get(task.uniqueKey()));
                }
                uniqueMap.put(task.uniqueKey(), task);
            }
            boolean result = queue.offer(task);
        } finally {
            lock.unlock();
        }
    }

    void shutdown() {
        lock.lock();
        try {
            running = false;
        } finally {
            lock.unlock();
        }
    }

    boolean awaitTermination(long timeoutMs) throws InterruptedException {
        if (timeoutMs > 0) {
            thread.join(timeoutMs);
        }
        return !thread.isAlive();
    }

    boolean inThisThread() {
        return Thread.currentThread() == thread;
    }

    boolean isRunning() {
        return running;
    }

    int queueSize() {
        return queue.size();
    }

    private void runLoop() {
        ArrayDeque<SlotTask> batch = new ArrayDeque<>(BATCH_SIZE);
        try {
            // 必须先读 running 再读 isEmpty，否则可能漏掉关闭前刚入队的任务
            while (running || !queue.isEmpty()) {
                SlotTask first = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, BATCH_SIZE - 1);
                SlotTask task;
                while ((task = batch.poll()) != null) {
                    runTask(task);

                }
            }
        } catch (InterruptedException e) {
            log.error("worker {} interrupted while waiting for tasks, stopping", thread.getName());
        } finally {
            shutdown();
            int dropped = batch.size() + queue.size();
            if (dropped > 0) {
                log.error("worker {} stopped with {} pending tasks dropped", thread.getName(), dropped);
                queue.clear();
            }
        }
    }

    private void runTask(SlotTask task) {
        long start = System.nanoTime();
        try {
            if (task.uniqueKey() != null ) {
                if (task != uniqueMap.get(task.uniqueKey())){
                    return;
                }
                uniqueMap.remove(task.uniqueKey(),task);
            }
            task.task().run();
        } catch (Throwable e) {
            log.error("task failed, worker={} routeId={}", thread.getName(), task.routeId(), e);
        } finally {
            // 任务残留的中断标志会让下一次 poll 立即抛出，导致整个槽位停止
            Thread.interrupted();
        }
        long cost = System.nanoTime() - start;
        if (cost > SLOW_TASK_NANOS) {
            log.warn("slow task, worker={} routeId={} cost={}ms",
                    thread.getName(), task.routeId(), TimeUnit.NANOSECONDS.toMillis(cost));
        }
    }
}
