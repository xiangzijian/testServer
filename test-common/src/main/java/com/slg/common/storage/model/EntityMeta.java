package com.slg.common.storage.model;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 一张实体表的元数据：注册时解析一次实体类，生成 Schema 和全部 SQL，之后只读。
 */
public final class EntityMeta<T extends EntityData> {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]*");
    private static final ThreadLocal<LinkedBuffer> BUFFER =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

    private final Class<T> type;
    private final String table;
    private final String[] keyNames;
    private final String upsertSql;
    private final String deleteSql;
    private final String loadByOwnerSql;
    private final String loadOneSql;
    private final Schema<T> schema;

    EntityMeta(Class<T> type) {
        this.type = type;
        this.schema = RuntimeSchema.getSchema(type);
        T prototype = schema.newMessage();
        this.table = checkName(prototype.tableName());
        String[] names = prototype.keyName();
        if (names == null || names.length == 0) {
            throw new IllegalArgumentException(type.getName() + " has no key column");
        }
        this.keyNames = names.clone();
        for (String name : keyNames) {
            checkName(name);
        }

        String keyCols = String.join(", ", keyNames);
        String marks = String.join(", ", Collections.nCopies(keyNames.length + 2, "?"));
        String keyMatch = Arrays.stream(keyNames).map(c -> c + " = ?").collect(Collectors.joining(" AND "));
        // MySQL 8.0.19+ 的行别名写法；data 必须在 version 之前赋值，否则比较的是已更新的 version
        this.upsertSql = "INSERT INTO " + table + " (" + keyCols + ", version, data) VALUES (" + marks + ") AS n"
                + " ON DUPLICATE KEY UPDATE"
                + " data = IF(n.version > " + table + ".version, n.data, " + table + ".data),"
                + " version = GREATEST(" + table + ".version, n.version)";
        this.deleteSql = "DELETE FROM " + table + " WHERE " + keyMatch + " AND version <= ?";
        this.loadByOwnerSql = "SELECT " + keyCols + ", version, data FROM " + table
                + " WHERE " + keyNames[0] + " = ?";
        this.loadOneSql = "SELECT " + keyCols + ", version, data FROM " + table + " WHERE " + keyMatch;
    }

    public Class<T> type() {
        return type;
    }

    public String table() {
        return table;
    }

    public String[] keyNames() {
        return keyNames.clone();
    }

    public String upsertSql() {
        return upsertSql;
    }

    public String deleteSql() {
        return deleteSql;
    }

    public String loadByOwnerSql() {
        return loadByOwnerSql;
    }

    public String loadOneSql() {
        return loadOneSql;
    }

    public EntityRow snapshot(T entity) {
        Object[] keys = checkEntityKeys(entity);
        LinkedBuffer buffer = BUFFER.get();
        try {
            byte[] data = ProtostuffIOUtil.toByteArray(entity, schema, buffer);
            return new EntityRow(type, entity.ownerId(), keys, entity.getVersion(), data);
        } finally {
            buffer.clear();
        }
    }

    public EntityRow readRow(ResultSet rs) throws SQLException {
        Object[] keys = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; i++) {
            keys[i] = rs.getObject(keyNames[i]);
        }
        return new EntityRow(type, KeyCodec.ownerOf(keys), keys, rs.getLong("version"), rs.getBytes("data"));
    }

    /** 把一行数据还原成实体：反序列化数据，写回主键和版本号。 */
    public T decode(EntityRow row) {
        T entity = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(row.data(), entity, schema);
        entity.writeKeyParam(row.keys());
        entity.setVersion(row.version());
        return entity;
    }

    public Object[] upsertArgs(EntityRow row) {
        Object[] keys = checkKeys(row.keys());
        Object[] args = Arrays.copyOf(keys, keys.length + 2);
        args[keys.length] = row.version();
        args[keys.length + 1] = row.data();
        return args;
    }

    public Object[] deleteArgs(Object[] keys, long version) {
        Object[] checked = checkKeys(keys);
        Object[] args = Arrays.copyOf(checked, checked.length + 1);
        args[checked.length] = version;
        return args;
    }

    public Object[] checkKeys(Object[] keys) {
        if (keys == null || keys.length != keyNames.length) {
            throw new IllegalArgumentException("table " + table + " expects " + keyNames.length
                    + " key values " + Arrays.toString(keyNames) + ", got " + Arrays.toString(keys));
        }
        return keys;
    }

    Object[] checkEntityKeys(EntityData entity) {
        Object[] keys = checkKeys(entity.key());
        long firstKey = KeyCodec.ownerOf(keys);
        if (firstKey != entity.ownerId()) {
            throw new IllegalArgumentException("table " + table + " key[0]=" + firstKey
                    + " does not match ownerId=" + entity.ownerId());
        }
        return keys;
    }

    private static String checkName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("bad sql identifier: " + name);
        }
        return name;
    }
}
