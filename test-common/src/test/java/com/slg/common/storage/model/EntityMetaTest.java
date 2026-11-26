package com.slg.common.storage.model;

import io.protostuff.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityMetaTest {

    static final class Hero extends EntityData {
        transient long ownerId;
        transient int heroId;
        @Tag(1) int level;

        @Override public long ownerId() { return ownerId; }
        @Override public String tableName() { return "player_hero"; }
        @Override public String[] keyName() { return new String[]{"player_id", "hero_id"}; }
        @Override public Object[] key() { return new Object[]{ownerId, heroId}; }
        @Override public void writeKeyParam(Object[] values) { }
    }

    static final class BadTable extends EntityData {
        @Override public long ownerId() { return 0; }
        @Override public String tableName() { return "player; drop table x"; }
        @Override public String[] keyName() { return new String[]{"player_id"}; }
        @Override public Object[] key() { return new Object[]{0L}; }
        @Override public void writeKeyParam(Object[] values) { }
    }

    static final class NoKey extends EntityData {
        @Override public long ownerId() { return 0; }
        @Override public String tableName() { return "no_key"; }
        @Override public String[] keyName() { return new String[0]; }
        @Override public Object[] key() { return new Object[0]; }
        @Override public void writeKeyParam(Object[] values) { }
    }

    @Test
    void generatesSqlForCompositeKey() {
        EntityMeta<Hero> meta = new EntityMeta<>(Hero.class);

        assertEquals("INSERT INTO player_hero (player_id, hero_id, version, data) VALUES (?, ?, ?, ?) AS n"
                + " ON DUPLICATE KEY UPDATE"
                + " data = IF(n.version > player_hero.version, n.data, player_hero.data),"
                + " version = GREATEST(player_hero.version, n.version)", meta.upsertSql());
        assertEquals("DELETE FROM player_hero WHERE player_id = ? AND hero_id = ? AND version <= ?", meta.deleteSql());
        assertEquals("SELECT player_id, hero_id, version, data FROM player_hero WHERE player_id = ?",
                meta.loadByOwnerSql());
        assertEquals("SELECT player_id, hero_id, version, data FROM player_hero WHERE player_id = ? AND hero_id = ?",
                meta.loadOneSql());
    }

    @Test
    void snapshotCarriesKeysVersionAndType() {
        EntityMeta<Hero> meta = new EntityMeta<>(Hero.class);
        Hero hero = new Hero();
        hero.ownerId = 10001;
        hero.heroId = 5;
        hero.level = 3;
        hero.setVersion(8);

        EntityRow row = meta.snapshot(hero);

        assertSame(Hero.class, row.type());
        assertEquals(10001, row.ownerId());
        assertArrayEquals(new Object[]{10001L, 5}, row.keys());
        assertEquals(8, row.version());

        Object[] args = meta.upsertArgs(row);
        assertEquals(4, args.length);
        assertEquals(8L, args[2]);
        assertSame(row.data(), args[3]);
    }

    @Test
    void rejectsWrongKeyCountAndMismatchedOwner() {
        EntityMeta<Hero> meta = new EntityMeta<>(Hero.class);
        assertThrows(IllegalArgumentException.class, () -> meta.deleteArgs(new Object[]{1L}, 1));

        EntityData mismatched = new EntityData() {
            @Override public long ownerId() { return 10; }
            @Override public String tableName() { return "t"; }
            @Override public String[] keyName() { return new String[]{"owner_id"}; }
            @Override public Object[] key() { return new Object[]{20L}; }
            @Override public void writeKeyParam(Object[] values) { }
        };
        assertThrows(IllegalArgumentException.class, () -> meta.checkEntityKeys(mismatched));
    }

    @Test
    void rejectsBadIdentifiersAndMissingKeys() {
        assertThrows(IllegalArgumentException.class, () -> new EntityMeta<>(BadTable.class));
        assertThrows(IllegalArgumentException.class, () -> new EntityMeta<>(NoKey.class));
    }

    @Test
    void keyCodecRoundTrips() {
        String pk = KeyCodec.encode(new Object[]{7L, 3, "abc"});
        assertEquals("n7|n3|sabc", pk);
        assertArrayEquals(new Object[]{7L, 3L, "abc"}, KeyCodec.decode(pk));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.encode(new Object[]{1L, "a|b"}));
    }
}
