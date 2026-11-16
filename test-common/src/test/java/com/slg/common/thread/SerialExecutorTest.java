package com.slg.common.thread;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerialExecutorTest extends MailboxExecutorContractTest {

    @Override
    MailboxExecutorSupport<?, ?> create(String name, int threads, int mailboxCapacity) {
        return new SerialExecutor(name, threads, mailboxCapacity);
    }

    @Override
    MailboxExecutorSupport<?, ?> create(String name, int threads, int mailboxCapacity,
                                        long maxPendingTasks, long idleMillis) {
        return new SerialExecutor(name, threads, mailboxCapacity, maxPendingTasks, idleMillis);
    }

    @Test
    void concurrentProducersOnSameIdStaySerialAndOrdered() throws InterruptedException {
        int producers = 8;
        int perProducer = 20_000;
        SerialExecutor executor = new SerialExecutor("same-id", 8, producers * perProducer);
        executor.start();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicBoolean overlapped = new AtomicBoolean();
        AtomicBoolean outOfOrder = new AtomicBoolean();
        int[] last = new int[producers];
        long[] executed = new long[1];

        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producer = p;
            Thread t = new Thread(() -> {
                awaitQuietly(go);
                for (int seq = 1; seq <= perProducer; seq++) {
                    int value = seq;
                    executor.execute(1L, () -> {
                        if (inFlight.incrementAndGet() > 1) {
                            overlapped.set(true);
                        }
                        if (last[producer] + 1 != value) {
                            outOfOrder.set(true);
                        }
                        last[producer] = value;
                        executed[0]++;
                        inFlight.decrementAndGet();
                    });
                }
            });
            threads.add(t);
            t.start();
        }
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(executor.shutdown(10, TimeUnit.SECONDS));

        assertFalse(overlapped.get());
        assertFalse(outOfOrder.get());
        assertEquals((long) producers * perProducer, executed[0]);
    }

    @Test
    void retiringWhileProducersHitSameIdsNeverLosesTasks() throws InterruptedException {
        for (int round = 0; round < 5; round++) {
            SerialExecutor executor = new SerialExecutor("retire-race", 2, 100_000, 10_000_000, 0);
            executor.start();
            AtomicLong accepted = new AtomicLong();
            AtomicLong executed = new AtomicLong();
            AtomicLong retired = new AtomicLong();
            AtomicBoolean stop = new AtomicBoolean();

            List<Thread> threads = new ArrayList<>();
            for (int p = 0; p < 4; p++) {
                threads.add(new Thread(() -> {
                    long i = 0;
                    while (!stop.get()) {
                        try {
                            executor.execute(i++ % 3, executed::incrementAndGet);
                            accepted.incrementAndGet();
                        } catch (RejectedExecutionException ignored) {
                            Thread.onSpinWait();
                        }
                        if (i % 64 == 0) {
                            Thread.yield();
                        }
                    }
                }));
            }
            for (int s = 0; s < 2; s++) {
                threads.add(new Thread(() -> {
                    while (!stop.get()) {
                        retired.addAndGet(executor.sweepIdleMailboxes());
                    }
                }));
            }
            threads.forEach(Thread::start);
            Thread.sleep(200);
            stop.set(true);
            for (Thread t : threads) {
                t.join();
            }
            assertTrue(executor.shutdown(10, TimeUnit.SECONDS));

            assertTrue(retired.get() > 0, "round " + round);
            assertEquals(accepted.get(), executed.get(), "round " + round);
            assertEquals(0, executor.pendingTasks(), "round " + round);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
