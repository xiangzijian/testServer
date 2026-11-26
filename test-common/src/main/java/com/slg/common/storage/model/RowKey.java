package com.slg.common.storage.model;

import java.util.List;

/**
 * 一行数据的身份标识：实体类型 + 主键值列表，用作写缓冲中的去重/合并键。
 */
public record RowKey(Class<? extends EntityData> type, List<Object> keys) {

    public static RowKey of(Class<? extends EntityData> type, Object[] keys) {
        return new RowKey(type, List.of(keys));
    }
}
