package com.slg.common.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 需要本机 redis-server，见 {@link LocalRedis}。
 */
class RedisIntegrationTest {

    private static LocalRedis local;
    private static RedisManage manage;
    private static Redis redis;

    @BeforeAll
    static void startServer() throws Exception {
        local = LocalRedis.start();
        manage = local.manage();
        redis = local.redis();
    }

    @AfterAll
    static void stopServer() {
        if (local != null) {
            local.stop();
        }
    }

    @Test
    void storesRawBytes() {
        byte[] value = {0, 1, 2, (byte) 0xFF};
        redis.sync().set("t:bytes", value);

        assertArrayEquals(value, redis.sync().get("t:bytes"));
    }

    @Test
    void pipelinesAsyncCommands() {
        List<RedisFuture<String>> writes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            writes.add(redis.async().set("t:p:" + i, Redis.arg(i)));
        }
        redis.awaitAll(writes);

        List<RedisFuture<byte[]>> reads = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            reads.add(redis.async().get("t:p:" + i));
        }
        List<byte[]> values = redis.awaitAll(reads);

        assertEquals("99", new String(values.get(99), StandardCharsets.US_ASCII));
    }

    @Test
    void scriptFallsBackToEvalWhenNotCached() {
        RedisScript setIfNewer = new RedisScript("""
                local cur = tonumber(redis.call('HGET', KEYS[1], 'v') or '0')
                if tonumber(ARGV[1]) > cur then
                    redis.call('HMSET', KEYS[1], 'v', ARGV[1], 'd', ARGV[2])
                    return 1
                end
                return 0
                """);
        redis.sync().scriptFlush();
        String[] keys = {"t:row"};

        Long first = setIfNewer.eval(redis, ScriptOutputType.INTEGER, keys, Redis.arg(2), Redis.arg("new"));
        Long stale = setIfNewer.eval(redis, ScriptOutputType.INTEGER, keys, Redis.arg(1), Redis.arg("old"));
        redis.sync().scriptFlush();
        Long newer = setIfNewer.<Long>evalAsync(redis, ScriptOutputType.INTEGER, keys, Redis.arg(3), Redis.arg("x")).join();

        assertEquals(1L, first);
        assertEquals(0L, stale);
        assertEquals(1L, newer);
        assertEquals("x", new String(redis.sync().hget("t:row", "d"), StandardCharsets.UTF_8));
    }

    @Test
    void getUnknownNameFails() {
        assertThrows(IllegalStateException.class, () -> manage.get("nope"));
    }
}
