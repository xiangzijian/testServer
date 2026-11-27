package com.slg.common.storage.cache;

import com.slg.common.storage.model.EntityData;

import java.util.Collection;

/**
 * 单张表从存储加载完成并挂载进内存后的回调监听器，由业务服按需实现。
 * <p>典型用途：
 * <ul>
 *     <li>对实现 {@link com.slg.common.id.SequencedEntity} 的表，触发 {@code OwnerIdAllocator.repair(...)} 修正自增计数器；</li>
 *     <li>为该表建立业务侧需要的内存二级索引。</li>
 * </ul>
 * <p><b>线程约束：</b>在 {@code ownerId} 对应的 {@code bizExecutor} 槽位线程上执行，且早于等待队列中的业务回调执行。
 */
@FunctionalInterface
public interface TableLoadListener {

    TableLoadListener NOOP = (ownerId, type, entities) -> {
    };

    /**
     * @param ownerId  归属实体 id
     * @param type     刚加载完成的表类型
     * @param entities 该表当前挂载进内存的全部实体（可能为空集合）
     */
    void onLoaded(long ownerId, Class<? extends EntityData> type, Collection<? extends EntityData> entities);
}
