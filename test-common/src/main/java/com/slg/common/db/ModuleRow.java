package com.slg.common.db;

/**
 * 实体在某一时刻的落库快照，在业务线程生成，交给持久化线程写入。
 * <p>keys 和 data 是数组，record 的 equals 按引用比较；生成后不能再修改其内容。
 */
public record ModuleRow(Class<? extends ModuleData> type, long playerId, Object[] keys, long version, byte[] data) {
}
