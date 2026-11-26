package com.slg.common.storage.buffer;

import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.model.RowOp;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import io.protostuff.Tag;
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

class WriteBufferTest {

    static final class Item extends EntityData {
        transient long ownerId;
        transient int itemId;
        @Tag(1) int count;

        Item() {
        }

        Item(long ownerId, int itemId) {
            this.ownerId = ownerId;
            this.itemId = itemId;
        }

        @Override public long ownerId() { return ownerId; }
        @Override public String tableName() { return "player_item"; }
        @Override public String[] keyName() { return new String[]{"player_id", "item_id"}; }
        @Override public Object[] key() { return new Object[]{ownerId, itemId}; }
        @Override public void writeKeyParam(Object[] values) { }
    }

    static final class FakeBatchWriter implements BatchWriter {
        final List<String> writes = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger batchCount = new AtomicInteger();
        final AtomicInteger failNext = new AtomicInteger();
        volatile boolean down;

        @Override
        public void writeOwnerBatch(long ownerId, List<RowOp> ops) {
            if (down || failNext.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new IllegalStateException("storage down");
            }
            batchCount.incrementAndGet();
            for (RowOp op : ops) {
                writes.add((op.isDelete() ? "D" : "U") + Arrays.toString(op.keys()) + "v" + op.version());
            }
        }
    }

    private SerialExecutor owner;
    private UniqueSerialExecutor writerExecutor;
    private EntityRegistry registry;
    private FakeBatchWriter sink;
    private WriteBuffer buffer;

    @BeforeEach
    void setUp() {
        owner = new SerialExecutor("biz", 2, 1024);
        owner.start();
        writerExecutor = new UniqueSerialExecutor("writer", 2, 1024);
        writerExecutor.start();
        registry = new EntityRegistry();
        registry.register(Item.class);
        sink = new FakeBatchWriter();
        buffer = new WriteBuffer("wb", registry, sink, writerExecutor, 4, 5, 50);
        buffer.start();
    }

    @AfterEach
    void tearDown() {
        sink.down = false;
        buffer.shutdown(2, TimeUnit.SECONDS);
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
                fail("condition not met, writes=" + sink.writes + ", pending=" + buffer.pendingRows());
            }
            buffer.tick();
            Thread.sleep(5);
        }
    }

    @Test
    void mergesUpdatesOfSameRowWithinOneCycle() throws Exception {
        Item item = new Item(1, 1);
        inSlot(1, () -> {
            buffer.save(owner, item);
            buffer.save(owner, item);
            buffer.save(owner, item);
        });

        buffer.flushAll();
        await(() -> buffer.pendingRows() == 0 && buffer.writtenCount() == 1);

        assertEquals(List.of("U[1, 1]v3"), sink.writes);
        assertEquals(1, sink.batchCount.get());
    }

    @Test
    void multipleRowsOfSameOwnerFlushInSingleBatch() throws Exception {
        inSlot(1, () -> {
            buffer.save(owner, new Item(1, 1));
            buffer.save(owner, new Item(1, 2));
            buffer.save(owner, new Item(1, 3));
        });

        buffer.flushAll();
        await(() -> buffer.writtenCount() == 3);

        assertEquals(1, sink.batchCount.get(), "all dirty rows of an owner should flush together in one batch");
        assertTrue(sink.writes.containsAll(List.of("U[1, 1]v1", "U[1, 2]v1", "U[1, 3]v1")));
    }

    @Test
    void flushBucketOnlyFlushesItsOwners() throws Exception {
        inSlot(4, () -> buffer.save(owner, new Item(4, 1)));
        inSlot(5, () -> buffer.save(owner, new Item(5, 1)));

        buffer.flushBucket(0);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (buffer.writtenCount() < 1 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(List.of("U[4, 1]v1"), sink.writes);
        assertEquals(1, buffer.dirtyOwners());
    }

    @Test
    void failedWriteIsRetriedUntilSuccess() throws Exception {
        sink.failNext.set(2);
        inSlot(1, () -> buffer.save(owner, new Item(1, 1)));

        buffer.flushAll();
        await(() -> buffer.pendingRows() == 0);

        assertEquals(List.of("U[1, 1]v1"), sink.writes);
        assertEquals(2, buffer.failureCount());
    }

    @Test
    void newerVersionReplacesFailedOlderVersion() throws Exception {
        sink.down = true;
        Item item = new Item(1, 1);
        inSlot(1, () -> buffer.save(owner, item));
        buffer.flushAll();
        await(() -> buffer.failureCount() >= 1);

        inSlot(1, () -> buffer.save(owner, item));
        buffer.flushAll();
        sink.down = false;
        await(() -> buffer.pendingRows() == 0);

        assertEquals(List.of("U[1, 1]v2"), sink.writes);
    }

    @Test
    void deleteDiscardsUnflushedUpdate() throws Exception {
        Item item = new Item(1, 1);
        inSlot(1, () -> {
            buffer.save(owner, item);
            buffer.delete(owner, item);
        });

        await(() -> buffer.pendingRows() == 0 && buffer.writtenCount() == 1);

        assertEquals(List.of("D[1, 1]v2"), sink.writes);
        assertEquals(0, buffer.dirtyOwners());
    }

    @Test
    void saveNowWritesWithoutWaitingForFlush() throws Exception {
        inSlot(1, () -> buffer.saveNow(owner, new Item(1, 1)));

        await(() -> buffer.writtenCount() == 1);

        assertEquals(List.of("U[1, 1]v1"), sink.writes);
        assertEquals(0, buffer.dirtyOwners());
    }

    @Test
    void flushAndWaitBlocksUntilWritten() throws Exception {
        inSlot(1, () -> buffer.save(owner, new Item(1, 1)));

        assertTrue(buffer.flushAndWait(1, 2, TimeUnit.SECONDS));
        assertEquals(List.of("U[1, 1]v1"), sink.writes);
        assertEquals(0, buffer.pendingRows());
    }

    @Test
    void saveOutsideOwnerSlotIsRejected() {
        Item item = new Item(1, 1);
        assertThrows(IllegalStateException.class, () -> buffer.save(owner, item));
        assertEquals(0, item.getVersion());
    }

    @Test
    void shutdownWritesRemainingDirtyRows() throws Exception {
        inSlot(1, () -> buffer.save(owner, new Item(1, 1)));
        inSlot(2, () -> buffer.save(owner, new Item(2, 7)));

        assertTrue(buffer.shutdown(2, TimeUnit.SECONDS));

        assertTrue(sink.writes.containsAll(List.of("U[1, 1]v1", "U[2, 7]v1")));
        assertEquals(0, buffer.pendingRows());
    }

    @Test
    void shutdownReturnsFalseWhenStorageStaysDown() throws Exception {
        sink.down = true;
        inSlot(1, () -> buffer.save(owner, new Item(1, 1)));

        assertFalse(buffer.shutdown(300, TimeUnit.MILLISECONDS));
        assertEquals(1, buffer.pendingRows());
    }

    @Test
    void shutdownFailsFastWhenExecutorClosedFirst() throws Exception {
        sink.down = true;
        inSlot(1, () -> buffer.save(owner, new Item(1, 1)));
        buffer.flushAll();
        writerExecutor.shutdown(1, TimeUnit.SECONDS);

        long start = System.nanoTime();
        assertFalse(buffer.shutdown(5, TimeUnit.SECONDS));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1000);
    }

    @Test
    void startRequiresRunningExecutor() {
        UniqueSerialExecutor notStarted = new UniqueSerialExecutor("idle", 1, 16);
        WriteBuffer wb = new WriteBuffer("wb2", registry, sink, notStarted, 4, 5, 50);
        assertThrows(IllegalStateException.class, wb::start);
    }
}
