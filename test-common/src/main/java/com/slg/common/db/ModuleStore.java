package com.slg.common.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 所有实体表的统一读写入口，一个 {@link Db} 对应一个实例。
 * <p>实体类需在启动时 {@link #register} 一次：解析表结构、生成 SQL，配置错误在启动时直接抛出。
 * <p>写入带版本保护：旧版本不会覆盖新版本，过期的删除不会删掉更新的数据。
 * <p>{@link #snapshot} 只能在实体所属的业务线程调用；其余方法会阻塞访问数据库，只能在持久化线程调用。
 */
public final class ModuleStore implements ModuleWriter {

    private final Supplier<Db> db;
    private final Map<Class<?>, TableMeta<?>> tables = new ConcurrentHashMap<>();
    private final Map<String, Class<?>> tableOwners = new ConcurrentHashMap<>();

    /**
     * @param db 每次访问数据库时调用；可以在数据源启动之前构造本对象，例如 {@code () -> dbManage.get("game")}
     */
    public ModuleStore(Supplier<Db> db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    public ModuleStore(Db db) {
        Objects.requireNonNull(db, "db");
        this.db = () -> db;
    }

    /**
     * @throws IllegalArgumentException 实体类重复注册、表名被其他实体类占用，或表结构定义不合法
     */
    public synchronized void register(Class<? extends ModuleData> type) {
        if (tables.containsKey(type)) {
            throw new IllegalArgumentException("already registered: " + type.getName());
        }
        TableMeta<?> meta = new TableMeta<>(type);
        Class<?> owner = tableOwners.putIfAbsent(meta.table, type);
        if (owner != null) {
            throw new IllegalArgumentException("table " + meta.table + " already used by " + owner.getName());
        }
        tables.put(type, meta);
    }

    /** 在业务线程生成快照：取出主键和版本号，并把实体序列化成字节。 */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ModuleRow snapshot(ModuleData entity) {
        TableMeta meta = meta(entity.getClass());
        return meta.snapshot(entity);
    }

    @Override
    public void upsert(ModuleRow row) {
        TableMeta<?> meta = meta(row.type());
        db.get().update(meta.upsertSql, meta.upsertArgs(row));
    }

    /** 批量写入，可以混合多张表：按表分组，每张表执行一次批量语句。 */
    public void upsertBatch(List<ModuleRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        Map<TableMeta<?>, List<Object[]>> grouped = new LinkedHashMap<>();
        for (ModuleRow row : rows) {
            TableMeta<?> meta = meta(row.type());
            grouped.computeIfAbsent(meta, m -> new ArrayList<>()).add(meta.upsertArgs(row));
        }
        for (Map.Entry<TableMeta<?>, List<Object[]>> e : grouped.entrySet()) {
            db.get().batchUpdate(e.getKey().upsertSql, e.getValue());
        }
    }

    /**
     * 删除主键对应的行，只有数据库中的版本号不大于 version 时才删除。
     *
     * @return 是否删除了数据
     */
    @Override
    public boolean delete(Class<? extends ModuleData> type, Object[] keys, long version) {
        TableMeta<?> meta = meta(type);
        return db.get().update(meta.deleteSql, meta.deleteArgs(keys, version)) > 0;
    }

    /** 加载归属实体在这张表下的所有行。 */
    public <T extends ModuleData> List<T> loadByOwner(Class<T> type, long ownerId) {
        TableMeta<T> meta = meta(type);
        return db.get().query(meta.loadByOwnerSql, (rs, i) -> meta.read(rs), ownerId);
    }

    /** 按完整主键加载一行，不存在返回 null。 */
    public <T extends ModuleData> T loadOne(Class<T> type, Object... keys) {
        TableMeta<T> meta = meta(type);
        List<T> list = db.get().query(meta.loadOneSql, (rs, i) -> meta.read(rs), meta.checkKeys(keys));
        return list.isEmpty() ? null : list.get(0);
    }

    public boolean isRegistered(Class<? extends ModuleData> type) {
        return tables.containsKey(type);
    }

    @SuppressWarnings("unchecked")
    private <T extends ModuleData> TableMeta<T> meta(Class<?> type) {
        TableMeta<?> meta = tables.get(type);
        if (meta == null) {
            throw new IllegalStateException("entity not registered: " + type.getName());
        }
        return (TableMeta<T>) meta;
    }
}
