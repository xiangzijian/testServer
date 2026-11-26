package com.slg.common.storage.model;

/**
 * 实体在某一时刻的序列化行快照。
 * <p>keys 和 data 是数组，record 的 equals 按引用比较；生成后不能再修改其内容。
 */
public record EntityRow(Class<? extends EntityData> type, long ownerId, Object[] keys, long version, byte[] data) {
}
