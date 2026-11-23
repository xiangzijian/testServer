package com.slg.common.sched;

import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.NamedThreadFactory;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class TimerEngine implements ManagedExecutor {
    private static final Logger log = LoggerFactory.getLogger(TimerEngine.class);

    private final String name;
    private final HashedWheelTimer wheel;
    /** 启动前登记的周期任务，启动时才开始计时。 */
    private final List<Repeating> deferred = new ArrayList<>();
    private boolean started;

    public TimerEngine(String name, long tickMillis, int ticksPerWheel, long maxPending) {
        this.name = name;
        this.wheel = new HashedWheelTimer(new NamedThreadFactory(name, true),
                tickMillis, TimeUnit.MILLISECONDS, ticksPerWheel, true, maxPending);
    }

    /** action 在定时线程上执行，只能做投递这类很快的操作。 */
    public TimerHandle schedule(long delayMillis, Runnable action) {
        Timeout t = wheel.newTimeout(timeout -> action.run(), Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
        return t::cancel;
    }

    /**
     * 每次执行完后间隔 periodMillis 再执行；action 抛出异常只记录日志，不影响后续执行。
     * 在 {@link #start()} 之前登记的任务，启动后才开始计时。action 同样只能做很快的操作。
     */
    public synchronized TimerHandle scheduleWithFixedDelay(long periodMillis, Runnable action) {
        if (periodMillis <= 0) {
            throw new IllegalArgumentException("periodMillis must be positive");
        }
        Repeating r = new Repeating(periodMillis, action);
        if (started) {
            r.arm();
        } else {
            deferred.add(r);
        }
        return r::cancel;
    }

    @Override public String name() { return name; }

    @Override
    public synchronized void start() {
        wheel.start();
        started = true;
        for (Repeating r : deferred) {
            r.arm();
        }
        deferred.clear();
    }

    @Override public boolean shutdown(long timeout, TimeUnit unit) { wheel.stop(); return true; }

    public interface TimerHandle { boolean cancel(); }

    private final class Repeating implements TimerTask {
        private final long periodMillis;
        private final Runnable action;
        private volatile boolean cancelled;
        private volatile Timeout current;

        Repeating(long periodMillis, Runnable action) {
            this.periodMillis = periodMillis;
            this.action = action;
        }

        void arm() {
            if (cancelled) {
                return;
            }
            try {
                current = wheel.newTimeout(this, periodMillis, TimeUnit.MILLISECONDS);
            } catch (IllegalStateException e) {
                // 定时器已停止
            }
        }

        @Override
        public void run(Timeout timeout) {
            if (cancelled) {
                return;
            }
            try {
                action.run();
            } catch (Throwable e) {
                log.error("timer {} repeating task failed", name, e);
            }
            arm();
        }

        boolean cancel() {
            cancelled = true;
            Timeout t = current;
            return t == null || t.cancel();
        }
    }
}
