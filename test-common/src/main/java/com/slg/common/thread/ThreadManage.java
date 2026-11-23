package com.slg.common.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 一个 server 进程内所有线程和资源组件的生命周期管理。由各 server 的启动器创建并注册组件，common 中的组件不自己注册。
 * <p>启动按 {@link Phase} 声明顺序、同一阶段内按注册顺序；关闭严格倒序。
 * 任一组件启动失败时，已启动的组件按倒序关闭，然后抛出异常。
 * 关闭时每个阶段有独立的超时预算，前面的阶段超时不会挤占后面阶段（例如持久化）的时间；
 * 某个组件关闭失败只记录结果，继续关闭后续组件。
 */
public final class ThreadManage {

    private static final Logger log = LoggerFactory.getLogger(ThreadManage.class);

    public enum Phase {
        /** 数据库、缓存等外部资源，最先启动、最后关闭。 */
        RESOURCE(5),
        /** 落库调度和写库线程，关闭时要等剩余数据写完。 */
        PERSIST(60),
        /** 业务线程。 */
        BUSINESS(10),
        /** 定时器，所有执行器就绪后才开始触发，关闭时最先停止触发。 */
        TIMER(2),
        /** 网络入口和服务注册，最后对外开放、最先摘除。 */
        INGRESS(5);

        final long defaultTimeoutSeconds;

        Phase(long defaultTimeoutSeconds) {
            this.defaultTimeoutSeconds = defaultTimeoutSeconds;
        }
    }

    private enum State { NEW, RUNNING, STOPPED }

    private final String name;
    private final Map<Phase, List<ManagedExecutor>> components = new EnumMap<>(Phase.class);
    private final Map<Phase, Long> timeoutNanos = new EnumMap<>(Phase.class);
    private final List<Registered> started = new ArrayList<>();
    private State state = State.NEW;

    private record Registered(Phase phase, ManagedExecutor component) {
    }

    public ThreadManage(String name) {
        this.name = name;
        for (Phase phase : Phase.values()) {
            components.put(phase, new ArrayList<>());
            timeoutNanos.put(phase, TimeUnit.SECONDS.toNanos(phase.defaultTimeoutSeconds));
        }
    }

    public synchronized <E extends ManagedExecutor> E register(Phase phase, E component) {
        if (component == null) {
            throw new NullPointerException("component is null");
        }
        if (state != State.NEW) {
            throw new IllegalStateException(name + " already started, register before startAll");
        }
        components.get(phase).add(component);
        return component;
    }

    /** 设置某个阶段关闭时的超时预算，该阶段内所有组件共用。 */
    public synchronized ThreadManage shutdownTimeout(Phase phase, long timeout, TimeUnit unit) {
        timeoutNanos.put(phase, unit.toNanos(timeout));
        return this;
    }

    /**
     * @throws RuntimeException 某个组件启动失败；此时已启动的组件已经被关闭
     */
    public synchronized void startAll() {
        if (state != State.NEW) {
            throw new IllegalStateException(name + " already started");
        }
        state = State.RUNNING;
        for (Phase phase : Phase.values()) {
            for (ManagedExecutor component : components.get(phase)) {
                try {
                    component.start();
                } catch (RuntimeException | Error e) {
                    log.error("[{}] {} {} start failed, rolling back", name, phase, component.name(), e);
                    shutdownAll();
                    throw e;
                }
                started.add(new Registered(phase, component));
            }
        }
        log.info("[{}] started {} components", name, started.size());
    }

    /**
     * 按倒序关闭所有已启动的组件。重复调用直接返回 true。
     *
     * @return 所有组件都在预算内正常关闭返回 true
     */
    public synchronized boolean shutdownAll() {
        if (state == State.STOPPED) {
            return true;
        }
        state = State.STOPPED;
        boolean allOk = true;
        Phase current = null;
        long deadline = 0;
        for (int i = started.size() - 1; i >= 0; i--) {
            Registered r = started.get(i);
            if (r.phase() != current) {
                current = r.phase();
                deadline = System.nanoTime() + timeoutNanos.get(current);
            }
            boolean ok;
            try {
                ok = r.component().shutdown(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (Throwable e) {
                log.error("[{}] {} {} shutdown threw", name, r.phase(), r.component().name(), e);
                ok = false;
            }
            if (!ok) {
                log.error("[{}] {} {} did not shut down cleanly", name, r.phase(), r.component().name());
                allOk = false;
            }
        }
        started.clear();
        log.info("[{}] shutdown {}", name, allOk ? "completed" : "completed with errors");
        return allOk;
    }
}
