package com.slg.common.storage.dao;

import com.slg.common.redis.Redis;
import com.slg.common.redis.RedisScript;
import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityMeta;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.model.EntityRow;
import com.slg.common.storage.model.KeyCodec;
import com.slg.common.storage.model.RowOp;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 纯 Redis 实体行存取，不感知 MySQL 和内存缓冲。按“归属实体 + 表”拆分 Hash，带版本保护、墓碑和表完整标记，
 * 并维护待同步到 MySQL 的脏队列。
 * <pre>
 * &lt;ns&gt;:{ownerId}:&lt;table&gt;   Hash：v:&lt;pk&gt; = 版本号，d:&lt;pk&gt; = 数据（只有 v 没有 d 表示墓碑），_full = 整表已从 DB 加载
 * &lt;ns&gt;:{ownerId}:dirty     Set：待同步到 MySQL 的 &lt;table&gt;|&lt;pk&gt;
 * &lt;ns&gt;:dirty:&lt;shard&gt;       ZSet：有待同步数据的 ownerId，shard = ownerId 取模
 * &lt;ns&gt;:dirty:&lt;shard&gt;:lease 同步租约，同一分片同一时刻只有一个同步器处理
 * </pre>
 * 同一 ownerId 的所有 key 用 {@code {ownerId}} 落在同一个 Redis Cluster slot，可在一个 Lua 脚本中原子批量写入。
 * 所有方法会阻塞访问 Redis，只能在 IO 线程调用。脚本兼容 Redis 3.0 及以上。
 */
public final class RedisEntityDao {

    private static final String FULL_FIELD = "_full";
    private static final byte[] EMPTY = new byte[0];

    /**
     * 按 ownerId 原子批量写入多张表的增删改。
     * KEYS: dirty, table1..tableN；
     * ARGV: N, 表名1..表名N, 然后每 5 个一组 (表序号, pk, version, 'u'|'d', data)。
     */
    private static final RedisScript WRITE_BATCH = new RedisScript("""
            local n = tonumber(ARGV[1])
            local written = 0
            for i = 2 + n, #ARGV, 5 do
                local t = tonumber(ARGV[i])
                local key = KEYS[1 + t]
                local pk = ARGV[i + 1]
                local cur = tonumber(redis.call('HGET', key, 'v:' .. pk) or '-1')
                if tonumber(ARGV[i + 2]) > cur then
                    redis.call('HSET', key, 'v:' .. pk, ARGV[i + 2])
                    if ARGV[i + 3] == 'u' then
                        redis.call('HSET', key, 'd:' .. pk, ARGV[i + 4])
                    else
                        redis.call('HDEL', key, 'd:' .. pk)
                    end
                    redis.call('SADD', KEYS[1], ARGV[1 + t] .. '|' .. pk)
                    redis.call('PERSIST', key)
                    written = written + 1
                end
            end
            return written
            """);

    /**
     * 回填单行：仅当版本更高时写入，不设置 _full；若该表当前没有待同步行则设置过期时间。
     * KEYS: table, dirty；ARGV: tableName, ttl, pk, version, data。
     */
    private static final RedisScript BACKFILL_ROW = new RedisScript("""
            local cur = tonumber(redis.call('HGET', KEYS[1], 'v:' .. ARGV[3]) or '-1')
            if tonumber(ARGV[4]) > cur then
                redis.call('HSET', KEYS[1], 'v:' .. ARGV[3], ARGV[4])
                redis.call('HSET', KEYS[1], 'd:' .. ARGV[3], ARGV[5])
            end
            if tonumber(ARGV[2]) > 0 then
                local prefix = ARGV[1] .. '|'
                local dirty = false
                for _, m in ipairs(redis.call('SMEMBERS', KEYS[2])) do
                    if string.sub(m, 1, #prefix) == prefix then
                        dirty = true
                        break
                    end
                end
                if not dirty then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
            end
            return 1
            """);

    /**
     * 回填整张表：仅写入比 Redis 版本更高的行（不覆盖更新的数据和墓碑），打上 _full 标记；
     * 若该表当前没有待同步行则设置过期时间。
     * KEYS: table, dirty；ARGV: tableName, ttl, 然后每 3 个一组 (pk, version, data)。
     */
    private static final RedisScript BACKFILL_TABLE = new RedisScript("""
            for i = 3, #ARGV, 3 do
                local cur = tonumber(redis.call('HGET', KEYS[1], 'v:' .. ARGV[i]) or '-1')
                if tonumber(ARGV[i + 1]) > cur then
                    redis.call('HSET', KEYS[1], 'v:' .. ARGV[i], ARGV[i + 1])
                    redis.call('HSET', KEYS[1], 'd:' .. ARGV[i], ARGV[i + 2])
                end
            end
            redis.call('HSET', KEYS[1], '_full', '1')
            if tonumber(ARGV[2]) > 0 then
                local prefix = ARGV[1] .. '|'
                local dirty = false
                for _, m in ipairs(redis.call('SMEMBERS', KEYS[2])) do
                    if string.sub(m, 1, #prefix) == prefix then
                        dirty = true
                        break
                    end
                end
                if not dirty then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
            end
            return 1
            """);

    /**
     * 清理已同步到 MySQL 的脏标记和墓碑；没有剩余脏行的表设置过期时间。返回剩余脏行数。
     * KEYS: dirty, table1..tableN；
     * ARGV: ttl, N, 表名1..表名N, 然后每 3 个一组 (表序号, pk, version)。
     */
    private static final RedisScript CLEAR = new RedisScript("""
            local n = tonumber(ARGV[2])
            for i = 3 + n, #ARGV, 3 do
                local t = tonumber(ARGV[i])
                local key = KEYS[1 + t]
                local pk = ARGV[i + 1]
                if (redis.call('HGET', key, 'v:' .. pk) or '-1') == ARGV[i + 2] then
                    redis.call('SREM', KEYS[1], ARGV[2 + t] .. '|' .. pk)
                    if redis.call('HEXISTS', key, 'd:' .. pk) == 0 then
                        redis.call('HDEL', key, 'v:' .. pk)
                    end
                end
            end
            local left = redis.call('SMEMBERS', KEYS[1])
            if tonumber(ARGV[1]) > 0 then
                local busy = {}
                for _, m in ipairs(left) do busy[string.match(m, '^[^|]+')] = true end
                for t = 1, n do
                    if not busy[ARGV[2 + t]] then redis.call('EXPIRE', KEYS[1 + t], ARGV[1]) end
                end
            end
            return #left
            """);

    /** KEYS: shard；ARGV: ownerId, score。score 未变时才移除。 */
    private static final RedisScript UNINDEX = new RedisScript("""
            local s = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if s and tonumber(s) == tonumber(ARGV[2]) then return redis.call('ZREM', KEYS[1], ARGV[1]) end
            return 0
            """);

    /** KEYS: lease；ARGV: owner。 */
    private static final RedisScript RELEASE = new RedisScript("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
            return 0
            """);

    /** Redis 中的一行：data 为 null 表示墓碑。 */
    public record Entry(long version, byte[] data) {
        public boolean deleted() {
            return data == null;
        }
    }

    /** 单行查询结果。 */
    public record RowLookup(EntityRow row, boolean deleted, boolean tableFull) {
        /** Redis 是否已经给出确切结论（命中数据、命中墓碑，或整表完整且不存在此行）。 */
        public boolean definitive() {
            return row != null || deleted || tableFull;
        }
    }

    /** 单张表在 Redis 中的视图。 */
    public record TableView(Map<String, Entry> rows, boolean full) {
    }

    /** 一行待同步数据；row 为 null 表示墓碑（需在 MySQL 删除）；version 为 -1 表示数据已不存在，只需清理脏标记。 */
    public record DirtyRow(Class<? extends EntityData> type, String table, String pk, Object[] keys,
                           long version, EntityRow row) {
    }

    /** 一个有待同步数据的 ownerId，score 用于同步完成后的条件移除。 */
    public record DirtyOwner(long ownerId, double score) {
    }

    private final String ns;
    private final Supplier<Redis> redisSupplier;
    private final EntityRegistry registry;
    private final int shards;
    private final long ttlSeconds;
    private final AtomicLong lastScore = new AtomicLong();

    /**
     * @param namespace  key 前缀，共用一个 Redis 的不同服必须不同，例如 game、world、alliance
     * @param redis      每次访问时获取连接，组装时 Redis 可以还没启动
     * @param registry   实体元数据注册表
     * @param shards     待同步索引的分片数，部署后不能修改
     * @param ttlSeconds 同步到 MySQL 后数据在 Redis 中保留多久，0 表示不过期
     */
    public RedisEntityDao(String namespace, Supplier<Redis> redis, EntityRegistry registry, int shards, long ttlSeconds) {
        if (namespace == null || !namespace.matches("[a-z][a-z0-9_]*")) {
            throw new IllegalArgumentException("invalid namespace: " + namespace);
        }
        if (shards <= 0) {
            throw new IllegalArgumentException("shards must be positive");
        }
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("ttlSeconds must be non-negative");
        }
        this.ns = namespace;
        this.redisSupplier = Objects.requireNonNull(redis, "redis");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.shards = shards;
        this.ttlSeconds = ttlSeconds;
    }

    public int shards() {
        return shards;
    }

    public int shardOf(long ownerId) {
        return Math.floorMod(ownerId, shards);
    }

    // ---------------------------------------------------------------- 写

    /**
     * 把同一个 ownerId 的一批增删改（可跨多张表）在一次 Lua 中原子写入 Redis，并登记到待同步队列。
     * 版本不高于现有版本的行会被忽略。
     */
    public void writeOwnerBatch(long ownerId, List<RowOp> ops) {
        if (ops.isEmpty()) {
            return;
        }
        List<String> tables = new ArrayList<>();
        Map<String, Integer> tableIndex = new HashMap<>();
        for (RowOp op : ops) {
            if (op.ownerId() != ownerId) {
                throw new IllegalArgumentException("batch for owner " + ownerId + " contains op of owner " + op.ownerId());
            }
            EntityMeta<?> meta = registry.meta(op.type());
            meta.checkKeys(op.keys());
            tableIndex.computeIfAbsent(meta.table(), t -> {
                tables.add(t);
                return tables.size();
            });
        }

        String[] keys = new String[1 + tables.size()];
        keys[0] = dirtyKey(ownerId);
        List<byte[]> args = new ArrayList<>(1 + tables.size() + ops.size() * 5);
        args.add(Redis.arg(tables.size()));
        for (int i = 0; i < tables.size(); i++) {
            String t = tables.get(i);
            keys[1 + i] = tableKey(t, ownerId);
            args.add(Redis.arg(t));
        }
        for (RowOp op : ops) {
            String table = registry.tableOf(op.type());
            args.add(Redis.arg(tableIndex.get(table)));
            args.add(Redis.arg(KeyCodec.encode(op.keys())));
            args.add(Redis.arg(op.version()));
            if (op.isDelete()) {
                args.add(Redis.arg("d"));
                args.add(EMPTY);
            } else {
                args.add(Redis.arg("u"));
                args.add(op.data());
            }
        }

        String shard = shardKey(shardOf(ownerId));
        byte[] owner = Redis.arg(ownerId);
        // 写入前后各登记一次：写入前保证宕机不丢索引，写入后保证和同步器条件移除并发时不丢索引
        redis().sync().zadd(shard, nextScore(), owner);
        WRITE_BATCH.<Long>eval(redis(), ScriptOutputType.INTEGER, keys, args.toArray(new byte[0][]));
        redis().sync().zadd(shard, nextScore(), owner);
    }

    // ---------------------------------------------------------------- 读与回填

    /** 按完整主键读取单行（只访问该行对应字段和 _full 标记，不扫描整张表）。 */
    public RowLookup readRow(Class<? extends EntityData> type, Object... keys) {
        EntityMeta<?> meta = registry.meta(type);
        meta.checkKeys(keys);
        long ownerId = KeyCodec.ownerOf(keys);
        String pk = KeyCodec.encode(keys);
        List<KeyValue<String, byte[]>> kv = redis().sync().hmget(
                tableKey(meta.table(), ownerId), FULL_FIELD, "v:" + pk, "d:" + pk);
        boolean full = kv.get(0).hasValue();
        boolean hasVer = kv.get(1).hasValue();
        boolean hasData = kv.get(2).hasValue();
        if (!hasVer) {
            return new RowLookup(null, false, full);
        }
        if (!hasData) {
            return new RowLookup(null, true, full);
        }
        long version = Long.parseLong(text(kv.get(1).getValue()));
        EntityRow row = new EntityRow(type, ownerId, KeyCodec.decode(pk), version, kv.get(2).getValue());
        return new RowLookup(row, false, full);
    }

    /** 读取 ownerId 在某张表下的全部字段（含墓碑与 _full 标记）。 */
    public TableView readTable(Class<? extends EntityData> type, long ownerId) {
        String table = registry.tableOf(type);
        Map<String, byte[]> hash = redis().sync().hgetall(tableKey(table, ownerId));
        boolean full = hash.containsKey(FULL_FIELD);
        Map<String, Entry> rows = new HashMap<>();
        for (Map.Entry<String, byte[]> e : hash.entrySet()) {
            String field = e.getKey();
            if (field.startsWith("v:")) {
                String pk = field.substring(2);
                rows.put(pk, new Entry(Long.parseLong(text(e.getValue())), hash.get("d:" + pk)));
            }
        }
        return new TableView(rows, full);
    }

    /** 把从 MySQL 读到的单行回填到 Redis（版本保护，不覆盖更新的行或墓碑，不打 _full 标记）。 */
    public void backfillRow(EntityRow row) {
        String table = registry.tableOf(row.type());
        String pk = KeyCodec.encode(row.keys());
        BACKFILL_ROW.<Long>eval(redis(), ScriptOutputType.INTEGER,
                new String[]{tableKey(table, row.ownerId()), dirtyKey(row.ownerId())},
                Redis.arg(table), Redis.arg(ttlSeconds), Redis.arg(pk), Redis.arg(row.version()), row.data());
    }

    /** 把从 MySQL 读到的整张表回填到 Redis（版本保护），并打上 _full 标记。 */
    public void backfillTable(Class<? extends EntityData> type, long ownerId, List<EntityRow> rows) {
        String table = registry.tableOf(type);
        List<byte[]> args = new ArrayList<>(2 + rows.size() * 3);
        args.add(Redis.arg(table));
        args.add(Redis.arg(ttlSeconds));
        for (EntityRow row : rows) {
            args.add(Redis.arg(KeyCodec.encode(row.keys())));
            args.add(Redis.arg(row.version()));
            args.add(row.data());
        }
        BACKFILL_TABLE.<Long>eval(redis(), ScriptOutputType.INTEGER,
                new String[]{tableKey(table, ownerId), dirtyKey(ownerId)}, args.toArray(new byte[0][]));
    }

    // ---------------------------------------------------------------- 同步

    /** 一个分片中最早有待同步数据的 ownerId，最多 limit 个。 */
    public List<DirtyOwner> dirtyOwners(int shard, int limit) {
        List<ScoredValue<byte[]>> list = redis().sync().zrangebyscoreWithScores(shardKey(shard),
                Range.unbounded(), Limit.create(0, limit));
        List<DirtyOwner> out = new ArrayList<>(list.size());
        for (ScoredValue<byte[]> v : list) {
            out.add(new DirtyOwner(Long.parseLong(text(v.getValue())), v.getScore()));
        }
        return out;
    }

    /** 读取一个 ownerId 所有待同步的行及其当前数据（按表异步 pipeline 合并往返）。 */
    public List<DirtyRow> dirtyRows(long ownerId) {
        Set<byte[]> members = redis().sync().smembers(dirtyKey(ownerId));
        if (members.isEmpty()) {
            return List.of();
        }
        Map<String, List<String>> byTable = new LinkedHashMap<>();
        for (byte[] m : members) {
            String s = text(m);
            int sep = s.indexOf('|');
            byTable.computeIfAbsent(s.substring(0, sep), t -> new ArrayList<>()).add(s.substring(sep + 1));
        }
        List<RedisFuture<List<KeyValue<String, byte[]>>>> futures = new ArrayList<>(byTable.size());
        for (Map.Entry<String, List<String>> e : byTable.entrySet()) {
            String[] fields = new String[e.getValue().size() * 2];
            int i = 0;
            for (String pk : e.getValue()) {
                fields[i++] = "v:" + pk;
                fields[i++] = "d:" + pk;
            }
            futures.add(redis().async().hmget(tableKey(e.getKey(), ownerId), fields));
        }
        List<List<KeyValue<String, byte[]>>> results = redis().awaitAll(futures);

        List<DirtyRow> out = new ArrayList<>(members.size());
        int t = 0;
        for (Map.Entry<String, List<String>> e : byTable.entrySet()) {
            String table = e.getKey();
            EntityMeta<?> meta = registry.metaByTable(table);
            if (meta == null) {
                throw new IllegalStateException("redis row of unregistered table: " + table);
            }
            List<KeyValue<String, byte[]>> kv = results.get(t++);
            int i = 0;
            for (String pk : e.getValue()) {
                KeyValue<String, byte[]> v = kv.get(i++);
                KeyValue<String, byte[]> d = kv.get(i++);
                long version = v.hasValue() ? Long.parseLong(text(v.getValue())) : -1;
                Object[] keys = KeyCodec.decode(pk);
                EntityRow row = d.hasValue() && version >= 0
                        ? new EntityRow(meta.type(), ownerId, keys, version, d.getValue()) : null;
                out.add(new DirtyRow(meta.type(), table, pk, keys, version, row));
            }
        }
        return out;
    }

    /**
     * 同步到 MySQL 之后调用：版本未变的行移出待同步集合，墓碑同时清除；没有剩余脏行的表开始计时过期。
     *
     * @return 该 ownerId 剩余的待同步行数；大于 0 说明同步期间又有新的写入
     */
    public long clearSynced(long ownerId, List<DirtyRow> synced) {
        if (synced.isEmpty()) {
            Long left = redis().sync().scard(dirtyKey(ownerId));
            return left == null ? 0 : left;
        }
        List<String> tables = new ArrayList<>();
        Map<String, Integer> tableIndex = new HashMap<>();
        for (DirtyRow r : synced) {
            tableIndex.computeIfAbsent(r.table(), t -> {
                tables.add(t);
                return tables.size();
            });
        }
        String[] keys = new String[1 + tables.size()];
        keys[0] = dirtyKey(ownerId);
        List<byte[]> args = new ArrayList<>(2 + tables.size() + synced.size() * 3);
        args.add(Redis.arg(ttlSeconds));
        args.add(Redis.arg(tables.size()));
        for (int i = 0; i < tables.size(); i++) {
            String t = tables.get(i);
            keys[1 + i] = tableKey(t, ownerId);
            args.add(Redis.arg(t));
        }
        for (DirtyRow r : synced) {
            args.add(Redis.arg(tableIndex.get(r.table())));
            args.add(Redis.arg(r.pk()));
            args.add(Redis.arg(r.version()));
        }
        Long left = CLEAR.eval(redis(), ScriptOutputType.INTEGER, keys, args.toArray(new byte[0][]));
        return left == null ? 0 : left;
    }

    /** ownerId 已无待同步数据时，从分片索引中移除；期间有新的写入则保留。 */
    public void unindex(DirtyOwner owner) {
        UNINDEX.<Long>eval(redis(), ScriptOutputType.INTEGER, new String[]{shardKey(shardOf(owner.ownerId()))},
                Redis.arg(owner.ownerId()), Redis.arg(Double.toString(owner.score())));
    }

    /** 尝试获取分片的同步租约，获取成功返回 true。 */
    public boolean tryLease(int shard, String owner, long leaseMillis) {
        return "OK".equals(redis().sync().set(leaseKey(shard), Redis.arg(owner), SetArgs.Builder.nx().px(leaseMillis)));
    }

    public void releaseLease(int shard, String owner) {
        RELEASE.<Long>eval(redis(), ScriptOutputType.INTEGER, new String[]{leaseKey(shard)}, Redis.arg(owner));
    }

    // ---------------------------------------------------------------- key

    public String tableKey(Class<? extends EntityData> type, long ownerId) {
        return tableKey(registry.tableOf(type), ownerId);
    }

    private String tableKey(String table, long ownerId) {
        return ns + ":{" + ownerId + "}:" + table;
    }

    private String dirtyKey(long ownerId) {
        return ns + ":{" + ownerId + "}:dirty";
    }

    private String shardKey(int shard) {
        return ns + ":dirty:" + shard;
    }

    private String leaseKey(int shard) {
        return ns + ":dirty:" + shard + ":lease";
    }

    /** 写入序号：毫秒时间 * 1000 加上递增值，保证单调递增，用作分片索引的 score。 */
    private double nextScore() {
        long now = System.currentTimeMillis() * 1000;
        return lastScore.updateAndGet(prev -> Math.max(prev + 1, now));
    }

    private Redis redis() {
        return redisSupplier.get();
    }

    private static String text(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }
}
