package com.slg.common.util;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public final class ProtostuffCodec<T> {
    private static final ThreadLocal<LinkedBuffer> BUFFER =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

    private final Schema<T> schema;

    public ProtostuffCodec(Class<T> type) {
        this.schema = RuntimeSchema.getSchema(type);   // 内部有缓存，每个类只解析一次
    }

    public byte[] encode(T obj) {
        LinkedBuffer buffer = BUFFER.get();
        try {
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } finally {
            buffer.clear();                            // 必须清空，否则下次会接着上次的内容写
        }
    }

    public T decode(byte[] data) {
        T obj = schema.newMessage();                   // 调用无参构造方法，字段初始值在这里生效
        ProtostuffIOUtil.mergeFrom(data, obj, schema);
        return obj;
    }
}