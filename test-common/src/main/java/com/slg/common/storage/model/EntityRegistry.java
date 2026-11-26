package com.slg.common.storage.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实体元数据注册表：纯内存，不访问任何存储。
 * <p>启动时把本服所有实体类 {@link #register} 进来，解析表结构、校验表名和主键、生成 SQL 和编解码 Schema；
 * 运行时供 DAO、写缓冲和仓储共享。
 */
public final class EntityRegistry {

    private final Map<Class<?>, EntityMeta<?>> byType = new ConcurrentHashMap<>();
    private final Map<String, EntityMeta<?>> byTable = new ConcurrentHashMap<>();

    /**
     * @throws IllegalArgumentException 实体类重复注册、表名被其他实体类占用，或表结构定义不合法
     */
    public synchronized <T extends EntityData> EntityMeta<T> register(Class<T> type) {
        if (byType.containsKey(type)) {
            throw new IllegalArgumentException("already registered: " + type.getName());
        }
        EntityMeta<T> meta = new EntityMeta<>(type);
        EntityMeta<?> owner = byTable.putIfAbsent(meta.table(), meta);
        if (owner != null) {
            throw new IllegalArgumentException("table " + meta.table() + " already used by " + owner.type().getName());
        }
        byType.put(type, meta);
        return meta;
    }

    public boolean isRegistered(Class<? extends EntityData> type) {
        return byType.containsKey(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends EntityData> EntityMeta<T> meta(Class<T> type) {
        EntityMeta<?> meta = byType.get(type);
        if (meta == null) {
            throw new IllegalStateException("entity not registered: " + type.getName());
        }
        return (EntityMeta<T>) meta;
    }

    /** 按表名查找元数据，未注册返回 null。 */
    public EntityMeta<?> metaByTable(String table) {
        return byTable.get(table);
    }

    public String tableOf(Class<? extends EntityData> type) {
        return meta(type).table();
    }

    /** 生成当前版本的只读快照，不递增版本号。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public EntityRow snapshot(EntityData entity) {
        EntityMeta meta = meta(entity.getClass());
        return meta.snapshot(entity);
    }

    /** 版本号加 1 并生成写入操作，只能在实体所属业务线程调用。 */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public RowOp nextUpsert(EntityData entity) {
        EntityMeta meta = meta(entity.getClass());
        meta.checkEntityKeys(entity);
        entity.incrementVersion();
        return RowOp.upsert(meta.snapshot(entity));
    }

    /** 版本号加 1 并生成删除操作，只能在实体所属业务线程调用。 */
    public RowOp nextDelete(EntityData entity) {
        EntityMeta<?> meta = meta(entity.getClass());
        Object[] keys = meta.checkEntityKeys(entity);
        long version = entity.incrementVersion();
        return RowOp.delete(entity.getClass(), entity.ownerId(), keys, version);
    }

    @SuppressWarnings("unchecked")
    public <T extends EntityData> T decode(EntityRow row) {
        return (T) meta(row.type()).decode(row);
    }
}
