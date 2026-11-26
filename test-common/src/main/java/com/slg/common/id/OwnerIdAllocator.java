package com.slg.common.id;

import com.slg.common.storage.buffer.WriteBuffer;
import com.slg.common.storage.model.EntityData;
import com.slg.common.thread.SerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 按归属实体分配实例 id：id 只在同一 ownerId、同一计数器内唯一，实体主键第一列必须是 ownerId。
 * 全局对象（邮件、拍卖单、战报等）使用 {@link UidGenerator}。
 * <p>所有方法只能在实体所属的业务线程调用。规则：
 * <ul>
 *     <li>id 只增不减、永不复用：复用已删除实例的 id 时，新数据的版本号低于库里尚未删除的旧行，会被版本保护拒绝；</li>
 *     <li>每次分配都会把计数器登记写缓冲；计数器和实例是不同的行，异常宕机时写入可能不同步，
 *     所以加载完成后、处理请求前必须调用 {@link #repair}。</li>
 * </ul>
 */
public final class OwnerIdAllocator {

    private static final Logger log = LoggerFactory.getLogger(OwnerIdAllocator.class);

    private final WriteBuffer writeBuffer;

    public OwnerIdAllocator(WriteBuffer writeBuffer) {
        this.writeBuffer = Objects.requireNonNull(writeBuffer, "writeBuffer");
    }

    /** 分配下一个 id 并登记计数器落库。 */
    public long next(SerialExecutor ownerExecutor, OwnerIdSeq seq, String sequence) {
        ownerExecutor.assertInSlot(seq.ownerId());
        long id = seq.allocate(sequence);
        writeBuffer.save(ownerExecutor, seq);
        return id;
    }

    /**
     * 把计数器修正到不小于已加载实例的最大 id。loaded 必须是该归属实体加载到的全部相关实体。
     *
     * @return 是否修正了计数器；修正说明上次计数器没来得及写出
     */
    public boolean repair(SerialExecutor ownerExecutor, OwnerIdSeq seq, Iterable<? extends EntityData> loaded) {
        ownerExecutor.assertInSlot(seq.ownerId());
        boolean changed = false;
        for (EntityData m : loaded) {
            if (!(m instanceof SequencedEntity s)) {
                continue;
            }
            if (m.ownerId() != seq.ownerId()) {
                throw new IllegalArgumentException("entity of owner " + m.ownerId()
                        + " passed to repair of owner " + seq.ownerId());
            }
            if (seq.raiseTo(s.sequenceName(), s.sequenceId())) {
                log.warn("owner {} id sequence {} raised to {}", seq.ownerId(), s.sequenceName(), s.sequenceId());
                changed = true;
            }
        }
        if (changed) {
            writeBuffer.save(ownerExecutor, seq);
        }
        return changed;
    }
}
