package com.slg.common.db;

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
final class TableMeta<T extends ModuleData> {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]*");
    private static final ThreadLocal<LinkedBuffer> BUFFER =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

    final Class<T> type;
    final String table;
    final String[] keyNames;
    final String upsertSql;
    final String deleteSql;
    final String loadByOwnerSql;
    final String loadOneSql;
    private final Schema<T> schema;

    TableMeta(Class<T> type) {
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

    ModuleRow snapshot(T entity) {
        Object[] keys = checkKeys(entity.key());
        LinkedBuffer buffer = BUFFER.get();
        try {
            byte[] data = ProtostuffIOUtil.toByteArray(entity, schema, buffer);
            return new ModuleRow(type, entity.playerId(), keys, entity.getVersion(), data);
        } finally {
            buffer.clear();
        }
    }

    T read(ResultSet rs) throws SQLException {
        T entity = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(rs.getBytes("data"), entity, schema);
        Object[] keys = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; i++) {
            keys[i] = rs.getObject(keyNames[i]);
        }
        entity.writeKeyParam(keys);
        entity.setVersion(rs.getLong("version"));
        return entity;
    }

    Object[] upsertArgs(ModuleRow row) {
        Object[] keys = checkKeys(row.keys());
        Object[] args = Arrays.copyOf(keys, keys.length + 2);
        args[keys.length] = row.version();
        args[keys.length + 1] = row.data();
        return args;
    }

    Object[] deleteArgs(Object[] keys, long version) {
        Object[] checked = checkKeys(keys);
        Object[] args = Arrays.copyOf(checked, checked.length + 1);
        args[checked.length] = version;
        return args;
    }

    Object[] checkKeys(Object[] keys) {
        if (keys == null || keys.length != keyNames.length) {
            throw new IllegalArgumentException("table " + table + " expects " + keyNames.length
                    + " key values " + Arrays.toString(keyNames) + ", got " + Arrays.toString(keys));
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
