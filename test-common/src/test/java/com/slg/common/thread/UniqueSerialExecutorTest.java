package com.slg.common.thread;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 单线程执行器上，卡住唯一的线程后所有 id 的任务都只能排队，便于控制顺序。
 */
class UniqueSerialExecutorTest extends MailboxExecutorContractTest {

    @Override
    MailboxExecutorSupport<?, ?> create(String name, int threads, int mailboxCapacity) {
        return new UniqueSerialExecutor(name, threads, mailboxCapacity);
    }

    @Override
    MailboxExecutorSupport<?, ?> create(String name, int threads, int mailboxCapacity,
                                        long maxPendingTasks, long idleMillis) {
        return new UniqueSerialExecutor(name, threads, mailboxCapacity, maxPendingTasks, idleMillis);
    }

    @Test
    void waitingTaskIsReplacedByLatest() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1024);
        CountDownLatch release = blockSlot(executor);
        List<String> ran = new CopyOnWriteArrayList<>();
        for (int v = 1; v <= 100; v++) {
            String tag = "v" + v;
            executor.execute(1L, 1L, () -> ran.add(tag));
        }
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("v100"), ran);
        assertEquals(99, executor.mergedCount());
    }

    @Test
    void replacedTaskMovesToTail() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1024);
        CountDownLatch release = blockSlot(executor);
        List<String> ran = new CopyOnWriteArrayList<>();
        executor.execute(1L, 1L, () -> ran.add("A1"));
        executor.execute(1L, 2L, () -> ran.add("B1"));
        executor.execute(1L, 1L, () -> ran.add("A2"));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("B1", "A2"), ran);
        assertEquals(1, executor.mergedCount());
    }

    @Test
    void runningTaskIsNotReplaced() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1024);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<String> ran = new CopyOnWriteArrayList<>();
        executor.execute(1L, 1L, () -> {
            started.countDown();
            await(release);
            ran.add("K1");
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        executor.execute(1L, 1L, () -> ran.add("K2"));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("K1", "K2"), ran);
        assertEquals(0, executor.mergedCount());
    }

    @Test
    void sameKeyOnDifferentRouteIdsIsNotMerged() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1024);
        CountDownLatch release = blockSlot(executor);
        List<String> ran = new CopyOnWriteArrayList<>();
        executor.execute(1L, 7L, () -> ran.add("p1"));
        executor.execute(2L, 7L, () -> ran.add("p2"));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("p1", "p2"), ran);
        assertEquals(0, executor.mergedCount());
    }

    @Test
    void replacementSucceedsWhenQueueIsFull() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1);
        CountDownLatch release = blockSlot(executor);
        List<String> ran = new CopyOnWriteArrayList<>();
        executor.execute(1L, 1L, () -> ran.add("K1"));
        executor.execute(1L, 1L, () -> ran.add("K2"));
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1L, 2L, () -> ran.add("L1")));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("K2"), ran);
        assertEquals(1, executor.mergedCount());
    }

    @Test
    void normalTasksAreNeverMerged() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1024);
        CountDownLatch release = blockSlot(executor);
        List<String> ran = new CopyOnWriteArrayList<>();
        executor.execute(1L, () -> ran.add("N1"));
        executor.execute(1L, 1L, () -> ran.add("K1"));
        executor.execute(1L, () -> ran.add("N2"));
        executor.execute(1L, 1L, () -> ran.add("K2"));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("N1", "N2", "K2"), ran);
        assertEquals(1, executor.mergedCount());
    }

    @Test
    void concurrentProducersAlwaysEndWithLatestVersion() throws InterruptedException {
        int producers = 8;
        int keysPerProducer = 4;
        int versions = 500;
        UniqueSerialExecutor executor = new UniqueSerialExecutor("unique-concurrent", 4, 100_000);
        executor.start();

        Map<String, Integer> lastVersion = new ConcurrentHashMap<>();
        AtomicLong executed = new AtomicLong();
        AtomicBoolean outOfOrder = new AtomicBoolean();
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            long routeId = p;
            Thread t = new Thread(() -> {
                await(go);
                for (int v = 1; v <= versions; v++) {
                    for (long k = 0; k < keysPerProducer; k++) {
                        String key = routeId + "-" + k;
                        int version = v;
                        executor.execute(routeId, k, () -> {
                            Integer prev = lastVersion.put(key, version);
                            if (prev != null && prev >= version) {
                                outOfOrder.set(true);
                            }
                            executed.incrementAndGet();
                        });
                    }
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

        assertFalse(outOfOrder.get());
        assertEquals(producers * keysPerProducer, lastVersion.size());
        lastVersion.values().forEach(v -> assertEquals(versions, v));
        long accepted = (long) producers * keysPerProducer * versions;
        assertEquals(accepted, executed.get() + executor.mergedCount());
    }

    @Test
    void shutdownRunsAcceptedUniqueTasksAndRejectsNewOnes() throws InterruptedException {
        UniqueSerialExecutor executor = startedSingleSlot(1024);
        CountDownLatch release = blockSlot(executor);
        List<String> ran = new CopyOnWriteArrayList<>();
        executor.execute(1L, 1L, () -> ran.add("K1"));
        executor.execute(1L, 2L, () -> ran.add("L1"));

        assertFalse(executor.shutdown(0, TimeUnit.SECONDS));
        assertThrows(RejectedExecutionException.class, () -> executor.execute(1L, 1L, () -> ran.add("K2")));
        release.countDown();
        assertTrue(executor.shutdown(5, TimeUnit.SECONDS));

        assertEquals(List.of("K1", "L1"), ran);
        assertEquals(0, executor.mergedCount());
    }

    private static UniqueSerialExecutor startedSingleSlot(int queueCapacity) {
        UniqueSerialExecutor executor = new UniqueSerialExecutor("unique", 1, queueCapacity);
        executor.start();
        return executor;
    }

    /**
     * 提交一个卡住槽位的任务，并等它真正开始执行，保证它不再占用队列容量。
     */
    private static CountDownLatch blockSlot(UniqueSerialExecutor executor) throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        executor.execute(0L, () -> {
            started.countDown();
            await(release);
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        return release;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
