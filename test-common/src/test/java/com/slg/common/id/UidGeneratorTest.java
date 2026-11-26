package com.slg.common.id;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UidGeneratorTest {

    private static final long T0 = UidGenerator.EPOCH_SECOND + 1000;

    @Test
    void encodesTimeNodeAndSequence() {
        UidGenerator gen = new UidGenerator(7, 0, () -> T0);

        long a = gen.next();
        long b = gen.next();

        assertEquals(T0, UidGenerator.timeOf(a).getEpochSecond());
        assertEquals(7, UidGenerator.nodeOf(a));
        assertEquals(0, UidGenerator.seqOf(a));
        assertEquals(1, UidGenerator.seqOf(b));
        assertTrue(a > 0);
    }

    @Test
    void newSecondResetsSequence() {
        AtomicLong clock = new AtomicLong(T0);
        UidGenerator gen = new UidGenerator(1, 0, clock::get);
        gen.next();
        gen.next();

        clock.set(T0 + 1);
        long id = gen.next();

        assertEquals(0, UidGenerator.seqOf(id));
        assertEquals(T0 + 1, UidGenerator.timeOf(id).getEpochSecond());
    }

    @Test
    void borrowsNextSecondWhenSequenceExhausted() {
        UidGenerator gen = new UidGenerator(1, 0, () -> T0);
        long last = 0;
        for (long i = 0; i <= UidGenerator.MAX_SEQ; i++) {
            last = gen.next();
        }
        assertEquals(UidGenerator.MAX_SEQ, UidGenerator.seqOf(last));

        long borrowed = gen.next();

        assertEquals(T0 + 1, UidGenerator.timeOf(borrowed).getEpochSecond());
        assertTrue(borrowed > last);
    }

    @Test
    void clockMovingBackStillProducesIncreasingIds() {
        AtomicLong clock = new AtomicLong(T0);
        UidGenerator gen = new UidGenerator(1, 0, clock::get);
        long before = gen.next();

        clock.set(T0 - 3600);
        long after = gen.next();

        assertTrue(after > before);
        assertEquals(T0, UidGenerator.timeOf(after).getEpochSecond());
    }

    @Test
    void startAfterSecondAvoidsReusingPreviousRun() {
        UidGenerator previous = new UidGenerator(1, 0, () -> T0);
        long old = previous.next();

        UidGenerator restarted = new UidGenerator(1, previous.lastSecond(), () -> T0 - 5);
        long fresh = restarted.next();

        assertTrue(fresh > old);
        assertEquals(T0 + 1, UidGenerator.timeOf(fresh).getEpochSecond());
    }

    @Test
    void differentNodesNeverCollide() {
        UidGenerator a = new UidGenerator(1, 0, () -> T0);
        UidGenerator b = new UidGenerator(2, 0, () -> T0);

        assertTrue(a.next() != b.next());
    }

    @Test
    void concurrentCallsAreUnique() throws Exception {
        UidGenerator gen = new UidGenerator(3);
        int threads = 8;
        int perThread = 100_000;
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread w = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    ids.add(gen.next());
                }
            });
            w.start();
            workers.add(w);
        }
        start.countDown();
        for (Thread w : workers) {
            w.join();
        }

        assertEquals(threads * perThread, ids.size());
    }

    @Test
    void rejectsNodeIdOutOfRange() {
        assertThrows(IllegalArgumentException.class, () -> new UidGenerator(-1));
        assertThrows(IllegalArgumentException.class, () -> new UidGenerator(UidGenerator.MAX_NODE_ID + 1));
    }
}
