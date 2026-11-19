package com.slg.common.db;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.Tag;
import io.protostuff.runtime.RuntimeSchema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModuleDataCodecTest {

    static final class HeroV1 extends ModuleData {
        static final String[] KEYS = {"player_id", "hero_id"};

        transient long playerId;
        transient int heroId;
        @Tag(1) int level;
        @Tag(2) Map<Integer, Integer> skills = new HashMap<>();

        @Override public long playerId() { return playerId; }
        @Override public String tableName() { return "player_hero"; }
        @Override public String[] keyName() { return KEYS; }
        @Override public Object[] key() { return new Object[]{playerId, heroId}; }

        @Override
        public void writeKeyParam(Object[] values) {
            playerId = ((Number) values[0]).longValue();
            heroId = ((Number) values[1]).intValue();
        }
    }

    static final class HeroV2 extends ModuleData {
        transient long playerId;
        transient int heroId;
        @Tag(1) int level;
        @Tag(2) Map<Integer, Integer> skills = new HashMap<>();
        @Tag(3) long skinExpireAt = -1;

        @Override public long playerId() { return playerId; }
        @Override public String tableName() { return "player_hero"; }
        @Override public String[] keyName() { return HeroV1.KEYS; }
        @Override public Object[] key() { return new Object[]{playerId, heroId}; }
        @Override public void writeKeyParam(Object[] values) { }
    }

    private static <T> byte[] encode(T obj, Schema<T> schema) {
        return ProtostuffIOUtil.toByteArray(obj, schema, LinkedBuffer.allocate(256));
    }

    private static <T> T decode(byte[] data, Schema<T> schema) {
        T obj = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data, obj, schema);
        return obj;
    }

    @Test
    void transientKeysAndVersionAreNotSerialized() {
        Schema<HeroV1> schema = RuntimeSchema.getSchema(HeroV1.class);
        HeroV1 hero = new HeroV1();
        hero.playerId = 10001;
        hero.heroId = 5;
        hero.setVersion(42);
        hero.level = 7;
        hero.skills.put(1, 3);

        HeroV1 back = decode(encode(hero, schema), schema);

        assertEquals(0, back.playerId);
        assertEquals(0, back.heroId);
        assertEquals(0, back.getVersion());
        assertEquals(7, back.level);
        assertEquals(Map.of(1, 3), back.skills);
    }

    @Test
    void emptyMapStaysNonNull() {
        Schema<HeroV1> schema = RuntimeSchema.getSchema(HeroV1.class);
        HeroV1 back = decode(encode(new HeroV1(), schema), schema);
        assertTrue(back.skills.isEmpty());
    }

    @Test
    void oldDataLoadsIntoNewClassWithDefaults() {
        HeroV1 old = new HeroV1();
        old.level = 9;
        old.skills.put(2, 4);
        byte[] bytes = encode(old, RuntimeSchema.getSchema(HeroV1.class));

        HeroV2 now = decode(bytes, RuntimeSchema.getSchema(HeroV2.class));

        assertEquals(9, now.level);
        assertEquals(Map.of(2, 4), now.skills);
        assertEquals(-1, now.skinExpireAt);
    }
}
