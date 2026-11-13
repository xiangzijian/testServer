package com.slg.common.thread;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

class SlotExecutorTest {

    @Test
    void sameIdRunsInSubmitOrder() {
        SlotExecutor executor = new SlotExecutor("order", 4, 20_000);
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
    void threadCountRoundsUpToPowerOfTwo() {
        assertEquals(8, new SlotExecutor("pow", 6, 16).size());
        assertEquals(1, new SlotExecutor("pow", 0, 16).size());
    }

    @Test
    void sameIdAlwaysOnSameThreadAndIdsCoverAllSlots() {
        SlotExecutor executor = new SlotExecutor("route", 8, 1024);
        executor.start();
        Set<String> threadsForOneId = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 100; i++) {
            executor.execute(7L, () -> threadsForOneId.add(Thread.currentThread().getName()));
        }
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
        assertEquals(1, threadsForOneId.size());

        Set<Integer> slots = new HashSet<>();
        for (long id = 0; id < 1000; id++) {
            int slot = executor.slotOf(id);
            assertTrue(slot >= 0 && slot < executor.size());
            slots.add(slot);
        }
        assertEquals(executor.size(), slots.size());
        assertTrue(executor.slotOf(-1L) >= 0);
    }

    @Test
    void failingTaskDoesNotKillSlotThread() {
        SlotExecutor executor = new SlotExecutor("fail", 1, 16);
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

        assertEquals("fail-0", before.get());
        assertEquals(before.get(), after.get());
    }

    @Test
    void rejectsWhenNotStartedOrQueueFull() throws InterruptedException {
        SlotExecutor executor = new SlotExecutor("full", 1, 1);
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1L, () -> { }));

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

        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
    }

    @Test
    void shutdownDrainsAcceptedTasksAndRejectsNewOnes() {
        SlotExecutor executor = new SlotExecutor("drain", 4, 2048);
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
            SlotExecutor executor = new SlotExecutor("race", 4, 1024);
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
        SlotExecutor executor = new SlotExecutor("owner", 4, 16);
        executor.start();
        AtomicBoolean insideOwn = new AtomicBoolean();
        AtomicBoolean insideOther = new AtomicBoolean(true);
        long id = 100L;
        long otherId = findIdInDifferentSlot(executor, id);
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
        SlotExecutor executor = new SlotExecutor("restart", 1, 16);
        executor.start();
        assertThrows(IllegalStateException.class, executor::start);
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));
    }

    private static long findIdInDifferentSlot(SlotExecutor executor, long id) {
        long other = id + 1;
        while (executor.slotOf(other) == executor.slotOf(id)) {
            other++;
        }
        return other;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
