package com.slg.common.storage.cache;

import com.slg.common.storage.model.EntityData;

import java.util.HashMap;
import java.util.Map;

/**
 * 单个 {@code ownerId} 在内存中的表集合容器。
 * <p>因为 {@link com.slg.common.thread.SerialExecutor} 保证了同一个 {@code ownerId} 的所有业务请求、
 * 加载完成挂载、淘汰检查全部在同一个槽位线程串行执行，所以本对象内部无需并发容器，直接用普通 {@link HashMap}，
 * 严禁在定时线程或 IO 加载线程访问本对象。
 * <p>当 {@code tables} 被全部卸载为空时，{@link EntityCache} 会将该 {@code ownerId} 从外层字典移除以释放内存。
 */
final class OwnerSlot {

    /** 该 ownerId 当前已加载或正在加载的表：表类型 -> 单表缓存条目。 */
    final Map<Class<? extends EntityData>, TableEntry> tables = new HashMap<>();
}
