package com.slg.common.db;

import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class PersistServiceTest {

    static final class Item extends ModuleData {
        final long playerId;
        final int itemId;

        Item(long playerId, int itemId) {
            this.playerId = playerId;
            this.itemId = itemId;
        }

        @Override public long playerId() { return playerId; }
        @Override public String tableName() { return "player_item"; }
        @Override public String[] keyName() { return new String[]{"player_id", "item_id"}; }
        @Override public Object[] key() { return new Object[]{playerId, itemId}; }
        @Override public void writeKeyParam(Object[] values) { }
    }

    static final class FakeWriter implements ModuleWriter {
        final List<String> writes = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger failNext = new AtomicInteger();
        volatile boolean down;

        @Override
        public ModuleRow snapshot(ModuleData e) {
            return new ModuleRow(e.getClass(), e.playerId(), e.key(), e.getVersion(), new byte[0]);
        }

        @Override
        public void upsert(ModuleRow row) {
            check();
            writes.add("U" + Arrays.toString(row.keys()) + "v" + row.version());
        }

        @Override
        public boolean delete(Class<? extends ModuleData> type, Object[] keys, long version) {
            check();
            writes.add("D" + Arrays.toString(keys) + "v" + version);
            return true;
        }

        private void check() {
            if (down || failNext.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new IllegalStateException("db down");
            }
        }
    }

    private SerialExecutor owner;
    private UniqueSerialExecutor writerExecutor;
    private FakeWriter db;
    private PersistService persist;

    @BeforeEach
    void setUp() {
        owner = new SerialExecutor("biz", 2, 1024);
        owner.start();
        writerExecutor = new UniqueSerialExecutor("db-writer", 2, 1024);
        writerExecutor.start();
        db = new FakeWriter();
        // 没有接定时器，测试里手动调用 tick / flushAll
        persist = new PersistService("persist", db, writerExecutor, 4, 5, 50);
        persist.start();
    }

    @AfterEach
    void tearDown() {
        db.down = false;
        persist.shutdown(2, TimeUnit.SECONDS);
        writerExecutor.shutdown(2, TimeUnit.SECONDS);
        owner.shutdown(2, TimeUnit.SECONDS);
    }

    private void inSlot(long id, Runnable action) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        owner.execute(id, () -> {
            try {
                action.run();
                done.complete(null);
            } catch (Throwable e) {
                done.completeExceptionally(e);
            }
        });
        done.get(2, TimeUnit.SECONDS);
    }

    private void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met, writes=" + db.writes + ", pending=" + persist.pendingRows());
            }
            persist.tick();
            Thread.sleep(5);
        }
    }

    @Test
    void mergesUpdatesOfSameRowWithinOneCycle() throws Exception {
        Item item = new Item(1, 1);
        inSlot(1, () -> {
            persist.update(owner, item);
            persist.update(owner, item);
            persist.update(owner, item);
        });

        persist.flushAll();
        await(() -> persist.pendingRows() == 0 && persist.writtenCount() == 1);

        assertEquals(List.of("U[1, 1]v3"), db.writes);
    }

    @Test
    void differentRowsOfSameTableAreNotMerged() throws Exception {
        inSlot(1, () -> {
            persist.update(owner, new Item(1, 1));
            persist.update(owner, new Item(1, 2));
        });

        persist.flushAll();
        await(() -> persist.writtenCount() == 2);

        assertTrue(db.writes.containsAll(List.of("U[1, 1]v1", "U[1, 2]v1")));
    }

    @Test
    void flushBucketOnlyFlushesItsPlayers() throws Exception {
        inSlot(4, () -> persist.update(owner, new Item(4, 1)));
        inSlot(5, () -> persist.update(owner, new Item(5, 1)));

        persist.flushBucket(0);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (persist.writtenCount() < 1 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(List.of("U[4, 1]v1"), db.writes);
        assertEquals(1, persist.dirtyPlayers());
    }

    @Test
    void failedWriteIsRetriedUntilSuccess() throws Exception {
        db.failNext.set(2);
        inSlot(1, () -> persist.update(owner, new Item(1, 1)));

        persist.flushAll();
        await(() -> persist.pendingRows() == 0);

        assertEquals(List.of("U[1, 1]v1"), db.writes);
        assertEquals(2, persist.failureCount());
    }

    @Test
    void newerVersionReplacesFailedOlderVersion() throws Exception {
        db.down = true;
        Item item = new Item(1, 1);
        inSlot(1, () -> persist.update(owner, item));
        persist.flushAll();
        await(() -> persist.failureCount() >= 1);

        inSlot(1, () -> persist.update(owner, item));
        persist.flushAll();
        db.down = false;
        await(() -> persist.pendingRows() == 0);

        assertEquals(List.of("U[1, 1]v2"), db.writes);
    }

    @Test
    void deleteDiscardsUnflushedUpdate() throws Exception {
        Item item = new Item(1, 1);
        inSlot(1, () -> {
            persist.update(owner, item);
            persist.delete(owner, item);
        });

        await(() -> persist.pendingRows() == 0 && persist.writtenCount() == 1);

        assertEquals(List.of("D[1, 1]v2"), db.writes);
        assertEquals(0, persist.dirtyPlayers());
    }

    @Test
    void updateNowWritesWithoutWaitingForFlush() throws Exception {
        inSlot(1, () -> persist.updateNow(owner, new Item(1, 1)));

        await(() -> persist.writtenCount() == 1);

        assertEquals(List.of("U[1, 1]v1"), db.writes);
        assertEquals(0, persist.dirtyPlayers());
    }

    @Test
    void updateOutsideOwnerSlotIsRejected() {
        Item item = new Item(1, 1);
        assertThrows(IllegalStateException.class, () -> persist.update(owner, item));
        assertEquals(0, item.getVersion());
    }

    @Test
    void shutdownWritesRemainingDirtyRows() throws Exception {
        inSlot(1, () -> persist.update(owner, new Item(1, 1)));
        inSlot(2, () -> persist.update(owner, new Item(2, 7)));

        assertTrue(persist.shutdown(2, TimeUnit.SECONDS));

        assertTrue(db.writes.containsAll(List.of("U[1, 1]v1", "U[2, 7]v1")));
        assertEquals(0, persist.pendingRows());
    }

    @Test
    void shutdownReturnsFalseWhenDatabaseStaysDown() throws Exception {
        db.down = true;
        inSlot(1, () -> persist.update(owner, new Item(1, 1)));

        assertFalse(persist.shutdown(300, TimeUnit.MILLISECONDS));
        assertEquals(1, persist.pendingRows());
    }

    @Test
    void shutdownFailsFastWhenExecutorClosedFirst() throws Exception {
        db.down = true;
        inSlot(1, () -> persist.update(owner, new Item(1, 1)));
        persist.flushAll();
        writerExecutor.shutdown(1, TimeUnit.SECONDS);

        long start = System.nanoTime();
        assertFalse(persist.shutdown(5, TimeUnit.SECONDS));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1000);
    }

    @Test
    void startRequiresRunningExecutor() {
        UniqueSerialExecutor notStarted = new UniqueSerialExecutor("idle", 1, 16);
        PersistService service = new PersistService("p2", db, notStarted, 4, 5, 50);
        assertThrows(IllegalStateException.class, service::start);
    }
}
