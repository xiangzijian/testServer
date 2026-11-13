package com.slg.common.thread;

import com.slg.common.util.ThreadUtil;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 按 id 固定路由到单条线程的串行执行器：同一 id 的任务严格按提交顺序执行，不同 id 可并行。
 * 线程数向上取整为 2 的幂，启动后不可调整，否则 id 与线程的对应关系会改变。
 * 不支持 shutdown 后重新 start。
 */
public final class SlotExecutor {

    private final String name;
    private final int mask;
    private final Worker[] workers;

    public SlotExecutor(String name, int threads, int queueCapacity) {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive: " + queueCapacity);
        }
        this.name = name;
        int size = ThreadUtil.nextPowerOfTwo(threads);
        this.mask = size - 1;
        this.workers = new Worker[size];
        for (int i = 0; i < size; i++) {
            workers[i] = new Worker(name, i, queueCapacity);
        }
    }

    public void start() {
        for (Worker worker : workers) {
            worker.start();
        }
    }

    public int slotOf(long id) {
        return (int) ((id * 0x9E3779B97F4A7C15L) >>> 32) & mask;
    }

    /**
     * @throws RejectedExecutionException 未启动、已关闭或该槽位队列已满
     */
    public void execute(long id, Runnable task) {
        execute(id, null, task);
    }

    /**
     * @throws RejectedExecutionException 未启动、已关闭或该槽位队列已满
     */
    public void execute(long id, Long uniqueKey, Runnable task) {
        Objects.requireNonNull(task, "task");
        int slot = slotOf(id);
        Worker worker = workers[slot];
        if (!worker.offer(new SlotTask(id, uniqueKey, task))) {
            throw new RejectedExecutionException("slot executor rejected task: name=" + name
                    + ", slot=" + slot + ", routeId=" + id + ", queued=" + worker.queueSize()
                    + ", running=" + worker.isRunning());
        }
    }

    public boolean inSlot(long id) {
        return workers[slotOf(id)].inThisThread();
    }

    public void assertInSlot(long id) {
        if (!inSlot(id)) {
            throw new IllegalStateException("not in slot thread: name=" + name + ", routeId=" + id
                    + ", slot=" + slotOf(id) + ", current=" + Thread.currentThread().getName());
        }
    }

    /**
     * 停止接收新任务，并在超时时间内等待已接收的任务全部执行完。
     *
     * @return 所有槽位线程都已结束返回 true；超时或被中断返回 false
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        for (Worker worker : workers) {
            worker.shutdown();
        }
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        boolean terminated = true;
        for (Worker worker : workers) {
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            try {
                if (!worker.awaitTermination(remainingMs)) {
                    terminated = false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return terminated;
    }

    public String name() {
        return name;
    }

    /**
     * 槽位（线程）数量。
     */
    public int size() {
        return workers.length;
    }

    public int queueSize(int slot) {
        return workers[slot].queueSize();
    }

    public int pendingTasks() {
        int total = 0;
        for (Worker worker : workers) {
            total += worker.queueSize();
        }
        return total;
    }
}
