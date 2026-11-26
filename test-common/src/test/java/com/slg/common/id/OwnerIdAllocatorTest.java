package com.slg.common.id;

import com.slg.common.storage.buffer.WriteBuffer;
import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.Tag;
import io.protostuff.runtime.RuntimeSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnerIdAllocatorTest {

    static final class Hero extends EntityData implements SequencedEntity {
        static final String TABLE = "player_hero";
        transient long ownerId;
        transient long heroId;
        @Tag(1) int level;

        Hero() {
        }

        Hero(long ownerId, long heroId) {
            this.ownerId = ownerId;
            this.heroId = heroId;
        }

        @Override public long ownerId() { return ownerId; }
        @Override public String tableName() { return TABLE; }
        @Override public String[] keyName() { return new String[]{"player_id", "hero_id"}; }
        @Override public Object[] key() { return new Object[]{ownerId, heroId}; }
        @Override public void writeKeyParam(Object[] values) { }
        @Override public String sequenceName() { return TABLE; }
        @Override public long sequenceId() { return heroId; }
    }

    private SerialExecutor owner;
    private UniqueSerialExecutor writer;
    private WriteBuffer buffer;
    private OwnerIdAllocator ids;

    @BeforeEach
    void setUp() {
        owner = new SerialExecutor("biz", 2, 1024);
        owner.start();
        writer = new UniqueSerialExecutor("writer", 1, 1024);
        writer.start();
        EntityRegistry registry = new EntityRegistry();
        registry.register(OwnerIdSeq.class);
        registry.register(Hero.class);
        buffer = new WriteBuffer("wb", registry, (ownerId, ops) -> { }, writer, 4);
        buffer.start();
        ids = new OwnerIdAllocator(buffer);
    }

    @AfterEach
    void tearDown() {
        buffer.shutdown(2, TimeUnit.SECONDS);
        writer.shutdown(2, TimeUnit.SECONDS);
        owner.shutdown(2, TimeUnit.SECONDS);
    }

    private <T> T inSlot(long id, Callable<T> action) throws Exception {
        CompletableFuture<T> done = new CompletableFuture<>();
        owner.execute(id, () -> {
            try {
                done.complete(action.call());
            } catch (Throwable e) {
                done.completeExceptionally(e);
            }
        });
        return done.get(2, TimeUnit.SECONDS);
    }

    @Test
    void allocatesIncreasingIdsPerSequenceAndMarksDirty() throws Exception {
        OwnerIdSeq seq = new OwnerIdSeq(1);

        List<Long> got = inSlot(1, () -> List.of(
                ids.next(owner, seq, "player_hero"),
                ids.next(owner, seq, "player_hero"),
                ids.next(owner, seq, "player_equip")));

        assertEquals(List.of(1L, 2L, 1L), got);
        assertEquals(3, seq.getVersion());
        assertEquals(1, buffer.dirtyOwners());
    }

    @Test
    void repairRaisesCounterToLoadedMaximum() throws Exception {
        OwnerIdSeq seq = new OwnerIdSeq(1);
        List<EntityData> loaded = List.of(new Hero(1, 3), new Hero(1, 7), seq);

        boolean changed = inSlot(1, () -> ids.repair(owner, seq, loaded));
        long next = inSlot(1, () -> ids.next(owner, seq, Hero.TABLE));

        assertTrue(changed);
        assertEquals(8, next);
    }

    @Test
    void repairNeverLowersCounter() throws Exception {
        OwnerIdSeq seq = new OwnerIdSeq(1);
        inSlot(1, () -> {
            for (int i = 0; i < 10; i++) {
                ids.next(owner, seq, Hero.TABLE);
            }
            return null;
        });

        boolean changed = inSlot(1, () -> ids.repair(owner, seq, List.of(new Hero(1, 4))));

        assertFalse(changed);
        assertEquals(10, seq.last(Hero.TABLE));
    }

    @Test
    void repairRejectsOtherOwnersEntities() {
        OwnerIdSeq seq = new OwnerIdSeq(1);

        assertThrows(Exception.class, () -> inSlot(1, () -> ids.repair(owner, seq, List.of(new Hero(2, 1)))));
    }

    @Test
    void allocateOutsideOwnerSlotIsRejected() {
        OwnerIdSeq seq = new OwnerIdSeq(1);

        assertThrows(IllegalStateException.class, () -> ids.next(owner, seq, Hero.TABLE));
        assertEquals(0, seq.last(Hero.TABLE));
    }

    @Test
    void countersSurviveSerialization() {
        EntityRegistry registry = new EntityRegistry();
        registry.register(OwnerIdSeq.class);
        OwnerIdSeq seq = new OwnerIdSeq(1);
        seq.allocate("player_hero");
        seq.allocate("player_hero");
        Schema<OwnerIdSeq> schema = RuntimeSchema.getSchema(OwnerIdSeq.class);

        byte[] bytes = ProtostuffIOUtil.toByteArray(seq, schema, LinkedBuffer.allocate(256));
        OwnerIdSeq back = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, back, schema);

        assertEquals(2, back.last("player_hero"));
        assertEquals(0, back.ownerId());
    }
}
