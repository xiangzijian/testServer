package com.slg.common.storage.dao;

import com.slg.common.storage.model.EntityData;
import com.slg.common.storage.model.EntityRow;
import com.slg.common.storage.model.RowOp;

import java.util.List;

/**
 * 底层持久化存储（MySQL）的行级 CRUD 接口，由 {@link MysqlEntityDao} 实现；测试可替换为内存实现。
 */
public interface EntityDao {

    /** 按完整主键读取一行，不存在返回 null。 */
    EntityRow get(Class<? extends EntityData> type, Object... keys);

    /** 读取 ownerId 在这张表下的全部行。 */
    List<EntityRow> listByOwner(Class<? extends EntityData> type, long ownerId);

    /** 批量执行写入和删除（可混合多张表），均带版本保护。 */
    void writeBatch(List<RowOp> ops);
}
