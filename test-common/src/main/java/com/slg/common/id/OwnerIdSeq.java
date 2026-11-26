package com.slg.common.id;

import com.slg.common.storage.model.EntityData;
import io.protostuff.Tag;

import java.util.HashMap;
import java.util.Map;

/**
 * 每个归属实体（如玩家、联盟）一行的实例 id 计数器，按计数器名（通常是实体表名）分别记录最后分配的 id。
 * 只通过 {@link OwnerIdAllocator} 修改，保证每次分配都会登记落库。
 * <pre>
 * CREATE TABLE owner_id_seq (
 *   owner_id BIGINT NOT NULL,
 *   version  BIGINT NOT NULL,
 *   data     BLOB   NOT NULL,
 *   PRIMARY KEY (owner_id)
 * );
 * </pre>
 */
public final class OwnerIdSeq extends EntityData {

    public static final String TABLE = "owner_id_seq";
    private static final String[] KEYS = {"owner_id"};

    private transient long ownerId;
    @Tag(1)
    private Map<String, Long> last = new HashMap<>();

    /** 反序列化用。 */
    public OwnerIdSeq() {
    }

    /** 归属实体第一次创建时使用。 */
    public OwnerIdSeq(long ownerId) {
        this.ownerId = ownerId;
    }

    @Override
    public long ownerId() {
        return ownerId;
    }

    @Override
    public String tableName() {
        return TABLE;
    }

    @Override
    public String[] keyName() {
        return KEYS;
    }

    @Override
    public Object[] key() {
        return new Object[]{ownerId};
    }

    @Override
    public void writeKeyParam(Object[] values) {
        ownerId = ((Number) values[0]).longValue();
    }

    /** 该计数器最后分配的 id，没有分配过返回 0。 */
    public long last(String sequence) {
        return last.getOrDefault(sequence, 0L);
    }

    long allocate(String sequence) {
        long id = last(sequence) + 1;
        last.put(sequence, id);
        return id;
    }

    /** 计数器小于 usedId 时调高到 usedId，返回是否有修改。只增不减。 */
    boolean raiseTo(String sequence, long usedId) {
        if (usedId > last(sequence)) {
            last.put(sequence, usedId);
            return true;
        }
        return false;
    }
}
