package com.slg.common.storage.dao;

import com.slg.common.db.Db;
import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityMeta;
import com.slg.common.storage.model.EntityRegistry;
import com.slg.common.storage.model.EntityRow;
import com.slg.common.storage.model.RowOp;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 纯 MySQL 实体行 CRUD，不感知 Redis 和内存缓冲。
 * <p>写入和删除均带版本保护：旧版本不会覆盖新版本，过期的删除不会删掉更新的数据。
 * 所有方法会阻塞访问数据库，只能在 IO 线程调用。
 */
public final class MysqlEntityDao implements EntityDao {

    private final EntityRegistry registry;
    private final Supplier<Db> db;

    /**
     * @param db 每次访问数据库时获取连接；可以在数据源启动前构造，例如 {@code () -> dbManage.get("game")}
     */
    public MysqlEntityDao(EntityRegistry registry, Supplier<Db> db) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.db = Objects.requireNonNull(db, "db");
    }

    public MysqlEntityDao(EntityRegistry registry, Db db) {
        this(registry, () -> Objects.requireNonNull(db, "db"));
    }

    @Override
    public EntityRow get(Class<? extends EntityData> type, Object... keys) {
        EntityMeta<?> meta = registry.meta(type);
        List<EntityRow> list = db.get().query(meta.loadOneSql(), (rs, i) -> meta.readRow(rs), meta.checkKeys(keys));
        return list.isEmpty() ? null : list.get(0);
    }

    @Override
    public List<EntityRow> listByOwner(Class<? extends EntityData> type, long ownerId) {
        EntityMeta<?> meta = registry.meta(type);
        return db.get().query(meta.loadByOwnerSql(), (rs, i) -> meta.readRow(rs), ownerId);
    }

    @Override
    public void writeBatch(List<RowOp> ops) {
        if (ops.isEmpty()) {
            return;
        }
        Map<EntityMeta<?>, List<Object[]>> upserts = new LinkedHashMap<>();
        Map<EntityMeta<?>, List<Object[]>> deletes = new LinkedHashMap<>();
        for (RowOp op : ops) {
            EntityMeta<?> meta = registry.meta(op.type());
            if (op.isDelete()) {
                deletes.computeIfAbsent(meta, m -> new ArrayList<>()).add(meta.deleteArgs(op.keys(), op.version()));
            } else {
                upserts.computeIfAbsent(meta, m -> new ArrayList<>()).add(meta.upsertArgs(op.toRow()));
            }
        }
        Db target = db.get();
        for (Map.Entry<EntityMeta<?>, List<Object[]>> e : upserts.entrySet()) {
            target.batchUpdate(e.getKey().upsertSql(), e.getValue());
        }
        for (Map.Entry<EntityMeta<?>, List<Object[]>> e : deletes.entrySet()) {
            target.batchUpdate(e.getKey().deleteSql(), e.getValue());
        }
    }
}
