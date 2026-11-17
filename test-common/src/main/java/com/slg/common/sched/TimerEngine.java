package com.slg.common.sched;

import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.NamedThreadFactory;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import java.util.concurrent.TimeUnit;

public final class TimerEngine implements ManagedExecutor {
    private final String name;
    private final HashedWheelTimer wheel;

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

    @Override public String name() { return name; }
    @Override public void start() { wheel.start(); }
    @Override public boolean shutdown(long timeout, TimeUnit unit) { wheel.stop(); return true; }

    public interface TimerHandle { boolean cancel(); }

}