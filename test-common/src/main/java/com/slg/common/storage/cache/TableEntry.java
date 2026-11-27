package com.slg.common.storage.cache;

import com.slg.common.storage.model.EntityData;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 某个 {@code ownerId} 在某一张表下的内存缓存条目，挂在 {@link OwnerSlot#tables} 中。
 * <p><b>状态流转：</b>
 * <pre>
 * [不存在] ──▶ LOADING（已向 IO 线程发起加载，新请求进入 waiters 排队）
 *                 ├── 加载成功 ──▶ LOADED（挂载 rows，回调监听器，按顺序执行 waiters）
 *                 └── 加载失败 ──▶ 从 OwnerSlot 移除（通知 waiters 失败回调，避免请求永久挂起）
 * LOADED   ──▶ 空闲检查通过、无 waiters 且 WriteBuffer 无未落盘数据 ──▶ 从 OwnerSlot 移除
 * </pre>
 * <p><b>线程约束：</b>所有字段只在对应 {@code ownerId} 的 {@code bizExecutor} 槽位线程读写，无需加锁或 volatile。
 */
final class TableEntry {

    enum State {
        /** 正在 IO 线程从 Redis / MySQL 加载中。 */
        LOADING,
        /** 已加载完成，可直接同步读写内存行数据。 */
        LOADED
    }

    /** LOADING 期间等待本表就绪的回调（成功执行 onReady，失败执行 onError）。 */
    record Waiter(Runnable onReady, Consumer<Throwable> onError) {
    }

    State state = State.LOADING;
    /**
     * 本表当前内存中的所有行，key 建议用 {@link com.slg.common.storage.model.KeyCodec#encode(Object[])}
     * 统一编码主键（避免 int 与 long 主键在 Map 查找时不相等）。1:1 表最多 1 行，1:N 表多行。
     */
    final Map<String, EntityData> rows = new LinkedHashMap<>();
    /** LOADING 期间到达的等待者队列，加载完成或失败后清空。 */
    final List<Waiter> waiters = new ArrayList<>();
    /** 最近一次被业务访问的时间（System.nanoTime），由 tryEvictOwner 在 biz 线程计算空闲时长。 */
    long lastAccessNanos = System.nanoTime();

    void touch() {
        this.lastAccessNanos = System.nanoTime();
    }
}
