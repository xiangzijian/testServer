package com.slg.common.thread;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class ThreadManage {

    private final Map<ShutdownPhase, ArrayList<ManagedExecutor>> executorMap = new ConcurrentHashMap<>();

    public enum ShutdownPhase {
        TIMER, BUSINESS, PERSIST, IO
    }

    ShutdownPhase[] start = new ShutdownPhase[]{ShutdownPhase.IO, ShutdownPhase.PERSIST, ShutdownPhase.BUSINESS, ShutdownPhase.TIMER};
    ShutdownPhase[] shutdown = new ShutdownPhase[]{ShutdownPhase.TIMER, ShutdownPhase.BUSINESS, ShutdownPhase.PERSIST, ShutdownPhase.IO};

    public ThreadManage() {
        for (ShutdownPhase shutdownPhase : ShutdownPhase.values()) {
            executorMap.put(shutdownPhase, new ArrayList<>());
        }
    }

    public <E extends ManagedExecutor> E register(ShutdownPhase phase, E executor) {
        if (executor == null) {
            throw new NullPointerException("executor is null");
        }
        executorMap.get(phase).add(executor);
        return executor;
    }

    public void startAll() {
        for (ShutdownPhase shutdownPhase : start) {
            for (ManagedExecutor executor : executorMap.get(shutdownPhase)) {
                executor.start();
            }
        }
    }

    /**
     * 按阶段依次关闭，所有阶段共用一个截止时间。
     */
    public boolean shutdownAll(long timeout, TimeUnit unit) {
        for (ShutdownPhase shutdownPhase : shutdown) {
            for (ManagedExecutor executor : executorMap.get(shutdownPhase)) {
                executor.shutdown(timeout, unit);
            }
        }
        return true;
    }
}
