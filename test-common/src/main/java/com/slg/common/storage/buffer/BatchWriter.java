package com.slg.common.storage.buffer;

import com.slg.common.storage.model.RowOp;

import java.util.List;

/**
 * {@link WriteBuffer} 的下游批量写入目标：同一批操作属于同一个 ownerId，可包含多张表的增删改。
 */
@FunctionalInterface
public interface BatchWriter {

    void writeOwnerBatch(long ownerId, List<RowOp> ops);
}
