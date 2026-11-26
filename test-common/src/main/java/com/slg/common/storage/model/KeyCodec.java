package com.slg.common.storage.model;

/**
 * 主键数组的无歧义文本编码，供 Redis 字段名和复合键比对使用。
 * 整数编码为 {@code n<long>}，字符串编码为 {@code s<value>}，各列用 {@code |} 连接。
 */
public final class KeyCodec {

    private KeyCodec() {
    }

    public static long ownerOf(Object[] keys) {
        if (keys == null || keys.length == 0 || !isIntegral(keys[0])) {
            throw new IllegalArgumentException("first key column must be an integer owner id");
        }
        return ((Number) keys[0]).longValue();
    }

    public static String encode(Object[] keys) {
        StringBuilder sb = new StringBuilder();
        for (Object k : keys) {
            if (!sb.isEmpty()) {
                sb.append('|');
            }
            if (isIntegral(k)) {
                sb.append('n').append(((Number) k).longValue());
            } else if (k instanceof String s) {
                if (s.indexOf('|') >= 0) {
                    throw new IllegalArgumentException("string key must not contain '|': " + s);
                }
                sb.append('s').append(s);
            } else {
                throw new IllegalArgumentException("unsupported key type "
                        + (k == null ? "null" : k.getClass().getName())
                        + ", only integer and string keys are supported");
            }
        }
        return sb.toString();
    }

    /** 整数主键统一还原为 Long。 */
    public static Object[] decode(String encoded) {
        String[] parts = encoded.split("\\|", -1);
        Object[] keys = new Object[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            keys[i] = p.charAt(0) == 'n' ? (Object) Long.parseLong(p.substring(1)) : p.substring(1);
        }
        return keys;
    }

    private static boolean isIntegral(Object k) {
        return k instanceof Long || k instanceof Integer || k instanceof Short || k instanceof Byte;
    }
}
