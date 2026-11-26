package com.slg.common.storage.model;

import java.util.Objects;

/**
 * 对一行数据的一次写入操作：新增/修改（data 非 null）或删除（data 为 null）。
 */
public record RowOp(Class<? extends EntityData> type, long ownerId, Object[] keys, long version, byte[] data) {

    public static RowOp upsert(EntityRow row) {
        Objects.requireNonNull(row, "row");
        Objects.requireNonNull(row.data(), "row.data");
        return new RowOp(row.type(), row.ownerId(), row.keys(), row.version(), row.data());
    }

    public static RowOp delete(Class<? extends EntityData> type, long ownerId, Object[] keys, long version) {
        return new RowOp(type, ownerId, keys, version, null);
    }

    public boolean isDelete() {
        return data == null;
    }

    public RowKey rowKey() {
        return RowKey.of(type, keys);
    }

    public EntityRow toRow() {
        if (data == null) {
            throw new IllegalStateException("delete op has no row data");
        }
        return new EntityRow(type, ownerId, keys, version, data);
    }
}
