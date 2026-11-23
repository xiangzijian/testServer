package com.slg.common.db;

/**
 * {@link PersistService} 依赖的存储操作，由 {@link ModuleStore} 实现；测试可替换为内存实现。
 */
interface ModuleWriter {

    ModuleRow snapshot(ModuleData entity);

    void upsert(ModuleRow row);

    boolean delete(Class<? extends ModuleData> type, Object[] keys, long version);
}
