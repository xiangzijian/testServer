package com.slg.common.storage.repo;

import com.slg.common.storage.buffer.WriteBuffer;
import com.slg.common.storage.dao.EntityDao;
import com.slg.common.storage.dao.RedisEntityDao;
import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.model.EntityRow;
import com.slg.common.storage.model.KeyCodec;
import com.slg.common.thread.SerialExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 面向各业务服的统一存储门面，支持两种模式：
 * <ul>
 *     <li><b>Redis + MySQL</b>（{@code redis != null}）：读走单表 Cache-Aside（先 Redis，未命中查 MySQL 并按版本合并回填）；
 *     写经 {@link WriteBuffer} 按 owner 批量写 Redis，再由 {@link com.slg.common.storage.sync.DbSyncWorker} 异步同步到 MySQL；</li>
 *     <li><b>直连 MySQL</b>（{@code redis == null}）：读直接查 MySQL，写经 {@link WriteBuffer} 批量写 MySQL。</li>
 * </ul>
 * 只按单张表提供存取，不感知玩家、联盟、地块等业务聚合概念；一次加载哪些表由各业务服自行决定。
 */
public final class EntityRepository {

    private static final long DEFAULT_READ_FLUSH_TIMEOUT_MS = 5_000;

    private final EntityRegistry registry;
    private final EntityDao mysql;
    private final RedisEntityDao redis;
    private final WriteBuffer writeBuffer;
    private final long readFlushTimeoutMs;

    /**
     * @param redis       为 null 时走直连 MySQL 模式
     * @param writeBuffer 为 null 时仅允许只读操作
     */
    public EntityRepository(EntityRegistry registry, EntityDao mysql, RedisEntityDao redis, WriteBuffer writeBuffer) {
        this(registry, mysql, redis, writeBuffer, DEFAULT_READ_FLUSH_TIMEOUT_MS);
    }

    public EntityRepository(EntityRegistry registry, EntityDao mysql, RedisEntityDao redis,
                            WriteBuffer writeBuffer, long readFlushTimeoutMs) {
        if (readFlushTimeoutMs <= 0) {
            throw new IllegalArgumentException("readFlushTimeoutMs must be positive");
        }
        this.registry = Objects.requireNonNull(registry, "registry");
        this.mysql = Objects.requireNonNull(mysql, "mysql");
        this.redis = redis;
        this.writeBuffer = writeBuffer;
        this.readFlushTimeoutMs = readFlushTimeoutMs;
    }

    // ---------------------------------------------------------------- 读（IO 线程调用）

    /**
     * 按完整主键读取单行（先刷出本地未写缓冲 -> 查 Redis -> 未命中查 MySQL 并回填单行）。
     * 不存在时返回 null，不回填空值。会阻塞，只能在 IO 线程调用。
     */
    public <T extends EntityData> T get(Class<T> type, Object... keys) {
        registry.meta(type).checkKeys(keys);
        long ownerId = KeyCodec.ownerOf(keys);
        awaitBufferedWrites(ownerId);
        if (redis == null) {
            EntityRow row = mysql.get(type, keys);
            return row == null ? null : registry.decode(row);
        }
        RedisEntityDao.RowLookup lookup = redis.readRow(type, keys);
        if (lookup.definitive()) {
            return lookup.row() == null ? null : registry.decode(lookup.row());
        }
        EntityRow dbRow = mysql.get(type, keys);
        if (dbRow == null) {
            return null;
        }
        redis.backfillRow(dbRow);
        return registry.decode(dbRow);
    }

    /**
     * 读取 ownerId 在某张表下的全部行（先刷出本地未写缓冲 -> 查 Redis -> 无完整标记时查 MySQL 按版本合并并回填整表）。
     * 整数主键统一还原为 Long。会阻塞，只能在 IO 线程调用。
     */
    public <T extends EntityData> List<T> list(Class<T> type, long ownerId) {
        registry.meta(type);
        awaitBufferedWrites(ownerId);
        if (redis == null) {
            List<EntityRow> rows = mysql.listByOwner(type, ownerId);
            List<T> out = new ArrayList<>(rows.size());
            for (EntityRow row : rows) {
                out.add(registry.decode(row));
            }
            return out;
        }
        RedisEntityDao.TableView view = redis.readTable(type, ownerId);
        Map<String, RedisEntityDao.Entry> merged;
        if (view.full()) {
            merged = view.rows();
        } else {
            List<EntityRow> dbRows = mysql.listByOwner(type, ownerId);
            merged = new HashMap<>();
            for (EntityRow row : dbRows) {
                merged.put(KeyCodec.encode(row.keys()), new RedisEntityDao.Entry(row.version(), row.data()));
            }
            for (Map.Entry<String, RedisEntityDao.Entry> e : view.rows().entrySet()) {
                RedisEntityDao.Entry inDb = merged.get(e.getKey());
                if (inDb == null || e.getValue().version() >= inDb.version()) {
                    merged.put(e.getKey(), e.getValue());
                }
            }
            redis.backfillTable(type, ownerId, dbRows);
        }
        List<T> out = new ArrayList<>(merged.size());
        for (Map.Entry<String, RedisEntityDao.Entry> e : merged.entrySet()) {
            RedisEntityDao.Entry entry = e.getValue();
            if (!entry.deleted()) {
                EntityRow row = new EntityRow(type, ownerId, KeyCodec.decode(e.getKey()), entry.version(), entry.data());
                out.add(registry.decode(row));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- 写（业务线程调用）

    /** 延迟保存：递增版本、生成快照，等待周期按 owner 批量写出。 */
    public void save(SerialExecutor ownerExecutor, EntityData entity) {
        requireWriteBuffer().save(ownerExecutor, entity);
    }

    /** 关键保存：递增版本、生成快照，立即提交写出。 */
    public void saveNow(SerialExecutor ownerExecutor, EntityData entity) {
        requireWriteBuffer().saveNow(ownerExecutor, entity);
    }

    /** 删除一行：丢弃该行未写出的修改，立即提交带版本保护的删除。 */
    public void delete(SerialExecutor ownerExecutor, EntityData entity) {
        requireWriteBuffer().delete(ownerExecutor, entity);
    }

    /** 立即提交该 ownerId 在本节点尚未写出的所有修改（例如对象卸载、下线前调用）。 */
    public void flush(long ownerId) {
        requireWriteBuffer().flush(ownerId);
    }

    public EntityRegistry registry() {
        return registry;
    }

    public WriteBuffer writeBuffer() {
        return writeBuffer;
    }

    private WriteBuffer requireWriteBuffer() {
        if (writeBuffer == null) {
            throw new IllegalStateException("repository is read-only (no WriteBuffer configured)");
        }
        return writeBuffer;
    }

    private void awaitBufferedWrites(long ownerId) {
        if (writeBuffer == null) {
            return;
        }
        try {
            if (!writeBuffer.flushAndWait(ownerId, readFlushTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("timed out waiting for pending writes of owner " + ownerId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for pending writes of owner " + ownerId, e);
        }
    }
}
