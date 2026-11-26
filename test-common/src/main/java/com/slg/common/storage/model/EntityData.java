package com.slg.common.storage.model;

/**
 * 可持久化实体的基类。一张表对应一个子类，表结构固定为：主键列 + version + data；使用前需注册到 {@link EntityRegistry}。
 * <p>主键列由 {@link #keyName()} 描述，第一列必须是归属实体 id（{@link #ownerId()}，例如玩家 id、联盟 id、地块 id）；
 * 其余字段由 Protostuff 序列化进 data 列。
 * 子类要求：有无参构造方法；字段不能是 final；主键字段和纯内存字段必须标记 transient；
 * 一旦有字段使用 {@code @Tag}，其余非 transient 字段也都必须加 {@code @Tag}。
 * <p>所有方法只能在实体所属的业务线程里调用。
 */
public abstract class EntityData {

    /**
     * 存储层维护的版本号，业务代码不能修改。
     */
    private transient long version;

    /**
     * 归属实体 id（主键第一列），决定写入和路由落到哪个邮箱/分片。
     */
    public abstract long ownerId();

    /**
     * 表名，应返回常量；注册时通过空实体读取一次。
     */
    public abstract String tableName();

    /**
     * 主键列名，顺序和 {@link #key()}、{@link #writeKeyParam(Object[])} 一致；应返回 static final 常量。
     */
    public abstract String[] keyName();

    /**
     * 当前对象的主键值，第一项必须等于 {@link #ownerId()}。
     */
    public abstract Object[] key();

    /**
     * 加载时写回主键值；数值类型请通过 {@link Number} 转换。
     */
    public abstract void writeKeyParam(Object[] values);

    public final long getVersion() {
        return version;
    }

    final void setVersion(long version) {
        this.version = version;
    }

    final long incrementVersion() {
        return ++this.version;
    }
}
