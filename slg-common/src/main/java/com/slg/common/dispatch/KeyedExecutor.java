package com.slg.common.dispatch;

import com.slg.common.util.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 按 key 哈希到固定的单线程执行器：同一 key（玩家/会话）的消息严格串行，不同 key 并行。
 * 这是业务线程池的最小占位实现，后续由自研 Actor 调度（设计文档 6.2）替换。
 * Netty IO 线程只做编解码与投递，handler 一律在这里执行。
 */
public final class KeyedExecutor {

    private final ExecutorService[] workers;

    public KeyedExecutor(String name, int threads) {
        if (threads <= 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }
        workers = new ExecutorService[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = Executors.newSingleThreadExecutor(new NamedThreadFactory(name + "-" + i, false));
        }
    }

    public void execute(long key, Runnable task) {
        int idx = (int) ((key ^ (key >>> 32)) & 0x7fffffff) % workers.length;
        workers[idx].execute(task);
    }

    public int size() {
        return workers.length;
    }

    public void shutdown() {
        for (ExecutorService w : workers) {
            w.shutdown();
        }
        for (ExecutorService w : workers) {
            try {
                w.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
