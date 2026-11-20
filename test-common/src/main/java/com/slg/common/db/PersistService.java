package com.slg.common.db;

import com.slg.common.thread.ManagedExecutor;
import com.slg.common.thread.SerialExecutor;
import com.slg.common.thread.UniqueSerialExecutor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class PersistService implements ManagedExecutor {
    private final ModuleStore store;
    private final UniqueSerialExecutor dbExecutor;
    /** 外层是并发 Map；内层只在该玩家的业务线程里读写。 */
    private final ConcurrentHashMap<Long, PlayerDirty> dirty = new ConcurrentHashMap<>();

    static final class PlayerDirty {
        final SerialExecutor owner;
        final Map<Object, ModuleData> modules = new LinkedHashMap<>();   // key = 表名 + 主键
        PlayerDirty(SerialExecutor owner) { this.owner = owner; }
    }

    /** dao.update 调用，在玩家的业务线程里执行。 */
    void markDirty(ModuleData e, SerialExecutor owner) {
        e.version++;
        dirty.computeIfAbsent(e.playerId(), id -> new PlayerDirty(owner))
                .modules.put(keyOf(e), e);       // 同一个模块重复 put，只保留最新的对象
    }

    /** 定时线程：只投递任务，不修改 Map。 */
    void flushTick() {
        for (Map.Entry<Long, PlayerDirty> en : dirty.entrySet()) {
            long playerId = en.getKey();
            en.getValue().owner.execute(playerId, () -> flushPlayer(playerId));
        }
    }

    /** 在玩家的业务线程里执行：取出这个玩家修改过的模块，生成快照，交给数据库执行器。 */
    void flushPlayer(long playerId) {
        PlayerDirty pd = dirty.remove(playerId);
        if (pd == null) {
            return;                           // 已经被别的刷盘任务处理过了
        }
        for (ModuleData e : pd.modules.values()) {
            ModuleRow row = store.snapshot(e);
            dbExecutor.execute(playerId, uniqueKeyOf(row), () -> writeWithRetry(row));
        }
    }
}