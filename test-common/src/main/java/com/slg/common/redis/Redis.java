package com.slg.common.redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一个 Redis 数据源：一个 Lettuce 客户端 + 一个所有线程共用的连接。key 是 UTF-8 字符串，value 是原始字节。
 * <p>单机/主从和集群模式对外都暴露为集群命令接口，业务代码不用区分部署形态；
 * 集群模式下多 key 命令和 Lua 脚本里的 key 必须落在同一个 slot（用 {@code {hashTag}}）。
 * <p>{@link #sync()} 会阻塞调用线程，只能在持久化、加载这类 IO 线程使用，不能在业务线程或 Lettuce 回调里使用。
 * 不要在共享连接上执行 BLPOP 这类阻塞命令和 MULTI 事务。
 */
public final class Redis {

    private static final Logger log = LoggerFactory.getLogger(Redis.class);

    static final RedisCodec<String, byte[]> CODEC = RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

    private final RedisConfig config;
    private final AbstractRedisClient client;
    private final StatefulConnection<String, byte[]> connection;
    private final RedisClusterCommands<String, byte[]> sync;
    private final RedisClusterAsyncCommands<String, byte[]> async;

    Redis(RedisConfig c, ClientResources resources) {
        this.config = c;
        List<RedisURI> uris = new ArrayList<>();
        for (String s : c.uris()) {
            uris.add(toUri(c, s));
        }
        if (c.mode() == RedisConfig.Mode.CLUSTER) {
            RedisClusterClient cluster = RedisClusterClient.create(resources, uris);
            cluster.setOptions(clusterOptions(c));
            this.client = cluster;
            try {
                StatefulRedisClusterConnection<String, byte[]> conn = cluster.connect(CODEC);
                this.connection = conn;
                this.sync = conn.sync();
                this.async = conn.async();
            } catch (RuntimeException e) {
                cluster.shutdown();
                throw e;
            }
        } else {
            RedisClient single = RedisClient.create(resources, uris.get(0));
            single.setOptions(clientOptions(c));
            this.client = single;
            try {
                StatefulRedisConnection<String, byte[]> conn = single.connect(CODEC);
                this.connection = conn;
                this.sync = conn.sync();
                this.async = conn.async();
            } catch (RuntimeException e) {
                single.shutdown();
                throw e;
            }
        }
    }

    private static RedisURI toUri(RedisConfig c, String s) {
        RedisURI.Builder b = RedisURI.builder(RedisURI.create(s))
                .withTimeout(Duration.ofMillis(c.commandTimeoutMs()))
                .withDatabase(c.database());
        if (!c.password().isEmpty()) {
            if (c.username().isEmpty()) {
                b.withPassword(c.password().toCharArray());
            } else {
                b.withAuthentication(c.username(), c.password());
            }
        }
        return b.build();
    }

    private static SocketOptions socketOptions(RedisConfig c) {
        return SocketOptions.builder()
                .connectTimeout(Duration.ofMillis(c.connectTimeoutMs()))
                .keepAlive(true)
                .build();
    }

    private static ClientOptions clientOptions(RedisConfig c) {
        return ClientOptions.builder()
                .socketOptions(socketOptions(c))
                // 异步命令也按超时失败，否则断线时 future 可能一直挂着
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(c.commandTimeoutMs())))
                // 断线期间直接拒绝命令，由调用方重试，不在客户端内存里无限堆积
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build();
    }

    private static ClusterClientOptions clusterOptions(RedisConfig c) {
        return ClusterClientOptions.builder()
                .socketOptions(socketOptions(c))
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(c.commandTimeoutMs())))
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                        .enablePeriodicRefresh(Duration.ofSeconds(c.topologyRefreshSeconds()))
                        .enableAllAdaptiveRefreshTriggers()
                        .build())
                .build();
    }

    /** 阻塞命令接口，只能在 IO 线程使用。 */
    public RedisClusterCommands<String, byte[]> sync() {
        return sync;
    }

    /** 异步命令接口：连续发出的命令会自动 pipeline，再用 {@link #awaitAll} 统一等待结果。 */
    public RedisClusterAsyncCommands<String, byte[]> async() {
        return async;
    }

    public RedisConfig config() {
        return config;
    }

    /**
     * 在一个命令超时时间内等待所有异步命令完成，按顺序返回结果。
     *
     * @throws RedisCommandTimeoutException 超时
     * @throws RuntimeException             任一命令失败时抛出它的异常
     */
    public <T> List<T> awaitAll(List<? extends Future<? extends T>> futures) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.commandTimeoutMs());
        List<T> results = new ArrayList<>(futures.size());
        try {
            for (Future<? extends T> f : futures) {
                results.add(f.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
            }
        } catch (TimeoutException e) {
            throw new RedisCommandTimeoutException("redis " + config.name() + ": " + futures.size()
                    + " commands not completed in " + config.commandTimeoutMs() + "ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RedisCommandInterruptedException(e);
        } catch (ExecutionException e) {
            throw unwrap(e.getCause());
        }
        return results;
    }

    static RuntimeException unwrap(Throwable e) {
        while ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e instanceof RuntimeException r ? r : new IllegalStateException(e);
    }

    /** Lua 脚本参数：整数按十进制字符串传，脚本里用 tonumber 读取。 */
    public static byte[] arg(long value) {
        return Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    }

    public static byte[] arg(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    void ping() {
        sync.ping();
        String info = sync.info("server");
        String version = info.lines().filter(l -> l.startsWith("redis_version:"))
                .map(l -> l.substring("redis_version:".length())).findFirst().orElse("unknown");
        log.info("redis [{}] ready, version {}, {}", config.name(), version, config);
    }

    void close() {
        try {
            connection.close();
        } finally {
            client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
        }
    }
}
