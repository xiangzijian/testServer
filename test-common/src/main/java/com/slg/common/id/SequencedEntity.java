package com.slg.common.id;

/**
 * 实例 id 由 {@link OwnerIdAllocator} 按归属实体分配的实体（如玩家的武将、装备等）实现此接口，
 * 加载完成后 {@link OwnerIdAllocator#repair} 据此把计数器修正到不小于已存在的最大 id。
 */
public interface SequencedEntity {

    /** 计数器名：同一类实体返回同一个常量，建议直接用表名；改名会让计数器从 0 重新开始。 */
    String sequenceName();

    /** 本实例由计数器分配的 id。 */
    long sequenceId();
}
