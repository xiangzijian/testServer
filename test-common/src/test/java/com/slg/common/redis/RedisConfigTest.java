package com.slg.common.redis;

import com.slg.common.config.ServerConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisConfigTest {

    private static ServerConfig config(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return new ServerConfig(p, "test");
    }

    @Test
    void parsesStandaloneWithDefaults() {
        RedisConfig c = RedisConfig.from(config("redis.cache.uri", "redis://127.0.0.1:6379"), "cache");

        assertEquals(RedisConfig.Mode.STANDALONE, c.mode());
        assertEquals(List.of("redis://127.0.0.1:6379"), c.uris());
        assertEquals(2000, c.commandTimeoutMs());
    }

    @Test
    void parsesClusterSeedNodes() {
        RedisConfig c = RedisConfig.from(config(
                "redis.cache.mode", "cluster",
                "redis.cache.uri", "redis://10.0.0.1:6379, redis://10.0.0.2:6379"), "cache");

        assertEquals(RedisConfig.Mode.CLUSTER, c.mode());
        assertEquals(2, c.uris().size());
    }

    @Test
    void rejectsInvalidCombinations() {
        assertThrows(IllegalArgumentException.class, () -> RedisConfig.from(config(
                "redis.cache.uri", "redis://a:6379,redis://b:6379"), "cache"));
        assertThrows(IllegalArgumentException.class, () -> RedisConfig.from(config(
                "redis.cache.mode", "cluster", "redis.cache.uri", "redis://a:6379", "redis.cache.database", "1"), "cache"));
        assertThrows(IllegalArgumentException.class, () -> RedisConfig.from(config(
                "redis.cache.uri", "redis://:secret@a:6379"), "cache"));
    }

    @Test
    void toStringHidesPassword() {
        RedisConfig c = RedisConfig.from(config(
                "redis.cache.uri", "redis://a:6379", "redis.cache.password", "secret"), "cache");

        assertFalse(c.toString().contains("secret"));
    }
}
