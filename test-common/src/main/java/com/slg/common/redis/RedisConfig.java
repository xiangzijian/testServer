package com.slg.common.redis;

import com.slg.common.config.ServerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * 一个 Redis 数据源的配置，前缀为 {@code redis.<name>.}：
 * <pre>
 * redis.names=cache
 * redis.cache.mode=standalone          # standalone（单机/主从，也支持 redis-sentinel:// 地址）| cluster
 * redis.cache.uri=redis://127.0.0.1:6379   # cluster 模式可填多个种子节点，逗号分隔；TLS 用 rediss://
 * redis.cache.username=                # Redis 6 ACL 用户名，可空
 * redis.cache.password=                # 不要写进 uri，避免出现在日志里
 * redis.cache.database=0               # cluster 模式只能是 0
 * redis.cache.connectTimeoutMs=3000
 * redis.cache.commandTimeoutMs=2000    # 超时的命令按失败处理，交给调用方重试
 * redis.cache.topologyRefreshSeconds=30 # 仅 cluster：定期刷新拓扑，配合自适应刷新应对主从切换
 * </pre>
 */
public record RedisConfig(String name, Mode mode, List<String> uris, String username, String password,
                          int database, long connectTimeoutMs, long commandTimeoutMs, long topologyRefreshSeconds) {

    public enum Mode { STANDALONE, CLUSTER }

    public RedisConfig {
        uris = List.copyOf(uris);
        if (uris.isEmpty()) {
            throw new IllegalArgumentException("redis " + name + ": uri is empty");
        }
        if (mode == Mode.STANDALONE && uris.size() != 1) {
            throw new IllegalArgumentException("redis " + name + ": standalone mode takes exactly one uri");
        }
        if (mode == Mode.CLUSTER && database != 0) {
            throw new IllegalArgumentException("redis " + name + ": cluster mode only supports database 0");
        }
        for (String uri : uris) {
            if (uri.contains("@")) {
                throw new IllegalArgumentException("redis " + name + ": put credentials in username/password, not in uri");
            }
        }
    }

    public static RedisConfig from(ServerConfig cfg, String name) {
        String p = "redis." + name + ".";
        List<String> uris = Arrays.stream(cfg.getRequired(p + "uri").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
        return new RedisConfig(name,
                Mode.valueOf(cfg.getString(p + "mode", "standalone").toUpperCase(Locale.ROOT)),
                uris,
                cfg.getString(p + "username", ""),
                cfg.getString(p + "password", ""),
                cfg.getInt(p + "database", 0),
                cfg.getLong(p + "connectTimeoutMs", 3000),
                cfg.getLong(p + "commandTimeoutMs", 2000),
                cfg.getLong(p + "topologyRefreshSeconds", 30));
    }

    @Override
    public String toString() {   // record 默认的 toString 会把密码打进日志
        return "RedisConfig[name=" + name + ", mode=" + mode + ", uris=" + uris
                + (username.isEmpty() ? "" : ", user=" + username) + ", db=" + database
                + ", timeout=" + commandTimeoutMs + "ms]";
    }
}
