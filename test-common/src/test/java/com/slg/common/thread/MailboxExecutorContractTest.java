package com.slg.common.thread;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 两种串行执行器共同遵守的行为，由子类提供具体实现。
 */
abstract class MailboxExecutorContractTest {

    abstract MailboxExecutorSupport<?, ?> create(String name, int threads, int mailboxCapacity);

    abstract MailboxExecutorSupport<?, ?> create(String name, int threads, int mailboxCapacity,
                                                 long maxPendingTasks, long idleMillis);

    @Test
    void sameIdRunsInSubmitOrder() {
        var executor = create("order", 4, 20_000);
        executor.start();
        List<Integer> seen = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            int seq = i;
            executor.execute(42L, () -> seen.add(seq));
        }
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(10_000, seen.size());
        for (int i = 0; i < seen.size(); i++) {
            assertEquals(i, seen.get(i));
        }
    }

    @Test
    void threadCountIsAtLeastOne() {
        assertEquals(6, create("threads", 6, 16).size());
        assertEquals(1, create("threads", 0, 16).size());
    }

    @Test
    void sameIdNeverRunsConcurrentlyAndKeepsOrder() {
        var executor = create("serial", 8, 10_000);
        executor.start();
        int ids = 4;
        int perId = 2_000;
        AtomicInteger[] inFlight = new AtomicInteger[ids];
        List<List<Integer>> seen = new ArrayList<>();
        for (int i = 0; i < ids; i++) {
            inFlight[i] = new AtomicInteger();
            seen.add(new ArrayList<>());
        }
        AtomicBoolean overlapped = new AtomicBoolean();
        for (int seq = 0; seq < perId; seq++) {
            for (int i = 0; i < ids; i++) {
                int idx = i;
                int value = seq;
                executor.execute(idx, () -> {
                    if (inFlight[idx].incrementAndGet() > 1) {
                        overlapped.set(true);
                    }
                    seen.get(idx).add(value);
                    inFlight[idx].decrementAndGet();
                });
            }
        }
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertFalse(overlapped.get());
        for (List<Integer> values : seen) {
            assertEquals(perId, values.size());
            for (int i = 0; i < perId; i++) {
                assertEquals(i, values.get(i));
            }
        }
    }

    @Test
    void failingTaskDoesNotKillSlotThread() {
        var executor = create("fail", 1, 16);
        executor.start();
        AtomicReference<String> before = new AtomicReference<>();
        AtomicReference<String> after = new AtomicReference<>();
        executor.execute(1L, () -> {
            before.set(Thread.currentThread().getName());
            throw new RuntimeException("boom");
        });
        executor.execute(1L, () -> {
            throw new AssertionError("error is caught as well");
        });
        executor.execute(1L, () -> Thread.currentThread().interrupt());
        executor.execute(1L, () -> after.set(Thread.currentThread().getName()));
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertTrue(before.get().startsWith("fail-"));
        assertTrue(after.get().startsWith("fail-"));
    }

    @Test
    void rejectsWhenNotStartedOrQueueFull() throws InterruptedException {
        var executor = create("full", 2, 1);
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1L, () -> { }));
        assertEquals(0, executor.mailboxCount());

        executor.start();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        executor.execute(1L, () -> {
            running.countDown();
            awaitQuietly(release);
        });
        assertTrue(running.await(5, TimeUnit.SECONDS));

        executor.execute(1L, () -> { });
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1L, () -> { }));
        assertEquals(1, executor.queueSize(1L));

        CountDownLatch otherDone = new CountDownLatch(1);
        executor.execute(2L, otherDone::countDown);
        assertTrue(otherDone.await(5, TimeUnit.SECONDS));

        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
    }

    @Test
    void shutdownDrainsAcceptedTasksAndRejectsNewOnes() {
        var executor = create("drain", 4, 2048);
        executor.start();
        AtomicInteger done = new AtomicInteger();
        for (int i = 0; i < 1000; i++) {
            executor.execute(i, done::incrementAndGet);
        }
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(1000, done.get());
        assertEquals(0, executor.pendingTasks());
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1L, () -> { }));
    }

    @Test
    void acceptedEqualsExecutedUnderConcurrentShutdown() throws InterruptedException {
        for (int round = 0; round < 20; round++) {
            var executor = create("race", 4, 1024);
            executor.start();
            AtomicLong accepted = new AtomicLong();
            AtomicLong executed = new AtomicLong();
            AtomicBoolean stop = new AtomicBoolean();
            CountDownLatch go = new CountDownLatch(1);

            Thread[] producers = new Thread[8];
            for (int p = 0; p < producers.length; p++) {
                long base = p * 1_000_000L;
                producers[p] = new Thread(() -> {
                    awaitQuietly(go);
                    long id = base;
                    while (!stop.get()) {
                        try {
                            executor.execute(id++, executed::incrementAndGet);
                            accepted.incrementAndGet();
                        } catch (RejectedExecutionException ignored) {
                            Thread.onSpinWait();
                        }
                    }
                });
                producers[p].start();
            }

            go.countDown();
            Thread.sleep(20);
            assertTrue(executor.shutdown(10, TimeUnit.SECONDS));
            stop.set(true);
            for (Thread producer : producers) {
                producer.join();
            }

            assertTrue(accepted.get() > 0);
            assertEquals(accepted.get(), executed.get(), "round " + round);
        }
    }

    @Test
    void inSlotOnlyOnOwningThread() {
        var executor = create("owner", 4, 16);
        executor.start();
        AtomicBoolean insideOwn = new AtomicBoolean();
        AtomicBoolean insideOther = new AtomicBoolean(true);
        long id = 100L;
        long otherId = id + 1;
        executor.execute(id, () -> {
            insideOwn.set(executor.inSlot(id));
            insideOther.set(executor.inSlot(otherId));
            executor.assertInSlot(id);
        });
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertTrue(insideOwn.get());
        assertFalse(insideOther.get());
        assertFalse(executor.inSlot(id));
        assertThrows(IllegalStateException.class, () -> executor.assertInSlot(id));
    }

    @Test
    void restartIsNotSupported() {
        var executor = create("restart", 1, 16);
        executor.start();
        assertThrows(IllegalStateException.class, executor::start);
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
    }

    @Test
    void slowIdDoesNotBlockOtherIds() throws InterruptedException {
        var executor = create("hol", 2, 1024);
        executor.start();
        CountDownLatch release = block(executor, 1L);

        CountDownLatch otherDone = new CountDownLatch(1);
        executor.execute(2L, otherDone::countDown);
        assertTrue(otherDone.await(5, TimeUnit.SECONDS));

        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
    }

    @Test
    void busyIdYieldsThreadToOthers() throws InterruptedException {
        var executor = create("fair", 1, 2048);
        executor.start();
        CountDownLatch release = block(executor, 0L);
        List<String> ran = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 1000; i++) {
            executor.execute(1L, () -> ran.add("A"));
        }
        executor.execute(2L, () -> ran.add("B"));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(1001, ran.size());
        assertTrue(ran.indexOf("B") <= 16, "B ran at " + ran.indexOf("B"));
    }

    @Test
    void stateIsVisibleAcrossThreadSwitches() {
        var executor = create("visible", 4, 200_000);
        executor.start();
        long[] counter = new long[1];
        int n = 100_000;
        for (int i = 0; i < n; i++) {
            executor.execute(1L, () -> counter[0]++);
            executor.execute(2 + i % 50, () -> { });
        }
        assertTrue(executor.shutdown(10, TimeUnit.SECONDS));

        assertEquals(n, counter[0]);
    }

    @Test
    void retiringIdleMailboxesNeverLosesTasks() throws InterruptedException {
        var executor = create("retire", 4, 10_000, 1_000_000, 0);
        executor.start();
        AtomicLong accepted = new AtomicLong();
        AtomicLong executed = new AtomicLong();
        AtomicLong retired = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();

        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
            Thread producer = new Thread(() -> {
                long i = 0;
                while (!stop.get()) {
                    try {
                        executor.execute(i++ % 10, executed::incrementAndGet);
                        accepted.incrementAndGet();
                    } catch (RejectedExecutionException ignored) {
                        Thread.onSpinWait();
                    }
                }
            });
            threads.add(producer);
        }
        Thread sweeper = new Thread(() -> {
            while (!stop.get()) {
                retired.addAndGet(executor.sweepIdleMailboxes());
            }
        });
        threads.add(sweeper);
        threads.forEach(Thread::start);

        Thread.sleep(300);
        stop.set(true);
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(executor.shutdown(10, TimeUnit.SECONDS));

        assertTrue(retired.get() > 0);
        assertEquals(accepted.get(), executed.get());
    }

    @Test
    void globalPendingLimitRejectsAnyId() throws InterruptedException {
        var executor = create("global", 1, 100, 3, 60_000);
        executor.start();
        CountDownLatch release = block(executor, 0L);
        executor.execute(1L, () -> { });
        executor.execute(2L, () -> { });
        assertThrows(RejectedExecutionException.class, () -> executor.execute(3L, () -> { }));
        assertEquals(3, executor.pendingTasks());

        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
        assertEquals(0, executor.pendingTasks());
    }

    @Test
    void shutdownRunsAcceptedTasksAndStopsCreatingMailboxes() {
        var executor = create("close", 4, 1024);
        executor.start();
        AtomicLong done = new AtomicLong();
        for (long id = 0; id < 100; id++) {
            executor.execute(id, done::incrementAndGet);
        }
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
        int mailboxes = executor.mailboxCount();

        assertEquals(100, done.get());
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1_000L, () -> { }));
        assertEquals(mailboxes, executor.mailboxCount());
    }

    /**
     * 提交一个卡住的任务，并等它真正开始执行。
     */
    private static CountDownLatch block(MailboxExecutorSupport<?, ?> executor, long id) throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        executor.execute(id, () -> {
            started.countDown();
            awaitQuietly(release);
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        return release;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
