package com.slg.common.thread;

import com.slg.common.thread.ThreadManage.Phase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadManageTest {

    private final List<String> events = Collections.synchronizedList(new ArrayList<>());

    private final class Fake implements ManagedExecutor {
        final String name;
        boolean failStart;
        boolean shutdownResult = true;
        RuntimeException shutdownError;
        long lastTimeoutMillis;

        Fake(String name) {
            this.name = name;
        }

        @Override public String name() { return name; }

        @Override
        public void start() {
            if (failStart) {
                throw new IllegalStateException("start failed: " + name);
            }
            events.add("start " + name);
        }

        @Override
        public boolean shutdown(long timeout, TimeUnit unit) {
            lastTimeoutMillis = unit.toMillis(timeout);
            events.add("stop " + name);
            if (shutdownError != null) {
                throw shutdownError;
            }
            return shutdownResult;
        }
    }

    @Test
    void startsByPhaseAndRegistrationOrderAndStopsInReverse() {
        ThreadManage tm = new ThreadManage("t");
        tm.register(Phase.INGRESS, new Fake("net"));
        tm.register(Phase.PERSIST, new Fake("writer"));
        tm.register(Phase.PERSIST, new Fake("persist"));
        tm.register(Phase.RESOURCE, new Fake("db"));
        tm.register(Phase.BUSINESS, new Fake("biz"));

        tm.startAll();
        assertTrue(tm.shutdownAll());

        assertEquals(List.of("start db", "start writer", "start persist", "start biz", "start net",
                "stop net", "stop biz", "stop persist", "stop writer", "stop db"), events);
    }

    @Test
    void startFailureRollsBackStartedComponents() {
        ThreadManage tm = new ThreadManage("t");
        tm.register(Phase.RESOURCE, new Fake("db"));
        Fake biz = tm.register(Phase.BUSINESS, new Fake("biz"));
        biz.failStart = true;
        tm.register(Phase.INGRESS, new Fake("net"));

        assertThrows(IllegalStateException.class, tm::startAll);

        assertEquals(List.of("start db", "stop db"), events);
    }

    @Test
    void shutdownContinuesAfterFailureAndReportsIt() {
        ThreadManage tm = new ThreadManage("t");
        tm.register(Phase.RESOURCE, new Fake("db"));
        Fake persist = tm.register(Phase.PERSIST, new Fake("persist"));
        persist.shutdownResult = false;
        Fake biz = tm.register(Phase.BUSINESS, new Fake("biz"));
        biz.shutdownError = new RuntimeException("boom");

        tm.startAll();
        assertFalse(tm.shutdownAll());

        assertTrue(events.containsAll(List.of("stop biz", "stop persist", "stop db")));
    }

    @Test
    void eachPhaseGetsItsOwnTimeoutBudget() {
        ThreadManage tm = new ThreadManage("t")
                .shutdownTimeout(Phase.BUSINESS, 1, TimeUnit.SECONDS)
                .shutdownTimeout(Phase.PERSIST, 30, TimeUnit.SECONDS);
        Fake persist = tm.register(Phase.PERSIST, new Fake("persist"));
        tm.register(Phase.BUSINESS, new Fake("biz"));

        tm.startAll();
        tm.shutdownAll();

        assertTrue(persist.lastTimeoutMillis > 29_000, "persist budget " + persist.lastTimeoutMillis);
    }

    @Test
    void shutdownIsIdempotentAndRegisterAfterStartIsRejected() {
        ThreadManage tm = new ThreadManage("t");
        Fake db = new Fake("db");
        assertSame(db, tm.register(Phase.RESOURCE, db));
        tm.startAll();

        assertThrows(IllegalStateException.class, () -> tm.register(Phase.BUSINESS, new Fake("late")));
        assertTrue(tm.shutdownAll());
        assertTrue(tm.shutdownAll());
        assertEquals(List.of("start db", "stop db"), events);
    }
}
