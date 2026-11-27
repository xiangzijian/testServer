package com.slg.common.storage.cache;

import com.slg.common.storage.model.EntityData;

/**
 * 内存表淘汰策略，由各业务服自行实现并注入 {@link EntityCache}，让 common 不耦合任何业务规则。
 * <p>例如在 Game 服可以实现分层策略：
 * <ul>
 *     <li>在线玩家的核心表：始终返回 {@code false}（常驻不淘汰）；</li>
 *     <li>在线玩家的冷表（邮件、战报等）：{@code idleMillis >= 10 分钟} 返回 {@code true}；</li>
 *     <li>离线玩家的所有表：{@code idleMillis >= 5 分钟} 返回 {@code true}。</li>
 * </ul>
 * <p><b>线程约束：</b>所有回调都在该 {@code ownerId} 对应的 {@code bizExecutor} 槽位线程上执行。
 */
@FunctionalInterface
public interface EvictPolicy {

    /**
     * 决定是否允许把 {@code (ownerId, type)} 从内存缓存中卸载。
     * <p>调用本方法之前，{@link EntityCache} 应先保证该表处于 {@code LOADED} 状态、没有等待者，
     * 且 {@code WriteBuffer} 中不存在尚未写进 Redis/MySQL 的未落盘修改。
     *
     * @param ownerId    归属实体 id
     * @param type       实体表类型
     * @param idleMillis 距上次访问已经空闲了多少毫秒
     * @return 返回 {@code true} 允许卸载；返回 {@code false} 保留在内存中
     */
    boolean canEvict(long ownerId, Class<? extends EntityData> type, long idleMillis);

    /**
     * 表从内存中卸载后的通知钩子（可选），业务可在此清理与该表关联的派生内存索引。
     */
    default void onEvicted(long ownerId, Class<? extends EntityData> type) {
    }
}
