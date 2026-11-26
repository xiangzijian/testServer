package com.slg.common.redis;

import com.slg.common.config.ServerConfig;
import com.slg.common.thread.ManagedExecutor;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 进程内所有 Redis 数据源的生命周期，注册在 RESOURCE 阶段。所有数据源共用一组 Lettuce IO 线程。
 * <pre>
 * redis.names=cache            # 逗号分隔，每个名字的配置见 {@link RedisConfig}
 * redis.ioThreads=0            # Lettuce IO 线程数，0 = Lettuce 默认（CPU 核数）
 * </pre>
 * 启动时逐个建立连接并检查连通性，任一失败则关闭已建立的连接并抛出异常。
 */
public final class RedisManage implements ManagedExecutor {

    private static final Logger log = LoggerFactory.getLogger(RedisManage.class);

    private final List<RedisConfig> configs;
    private final int ioThreads;
    private final Map<String, Redis> redis = new LinkedHashMap<>();
    private ClientResources resources;
    private volatile boolean started;

    public RedisManage(ServerConfig cfg) {
        this.configs = Arrays.stream(cfg.getRequired("redis.names").split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(n -> RedisConfig.from(cfg, n)).toList();
        this.ioThreads = cfg.getInt("redis.ioThreads", 0);
    }

    @Override
    public String name() {
        return "redis";
    }

    @Override
    public synchronized void start() {
        if (started || resources != null) {
            throw new IllegalStateException("already started");
        }
        DefaultClientResources.Builder b = DefaultClientResources.builder();
        if (ioThreads > 0) {
            b.ioThreadPoolSize(ioThreads);
        }
        resources = b.build();
        try {
            for (RedisConfig c : configs) {
                Redis r = new Redis(c, resources);
                redis.put(c.name(), r);
                r.ping();
            }
            started = true;
        } catch (RuntimeException e) {
            closeAll(2, TimeUnit.SECONDS);
            throw e;
        }
    }

    /**
     * @throws IllegalStateException 未启动、已关闭或没有这个名字的数据源
     */
    public Redis get(String name) {
        Redis r = started ? redis.get(name) : null;
        if (r == null) {
            throw new IllegalStateException("redis not available: " + name);
        }
        return r;
    }

    @Override
    public synchronized boolean shutdown(long timeout, TimeUnit unit) {
        started = false;
        return closeAll(timeout, unit);
    }

    private boolean closeAll(long timeout, TimeUnit unit) {
        boolean ok = true;
        List<Redis> list = new ArrayList<>(redis.values());
        Collections.reverse(list);
        for (Redis r : list) {
            try {
                r.close();
            } catch (RuntimeException e) {
                log.warn("redis [{}] close failed", r.config().name(), e);
                ok = false;
            }
        }
        redis.clear();
        if (resources != null) {
            try {
                ok &= resources.shutdown(0, timeout, unit).get(timeout, unit);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.warn("redis client resources shutdown failed", e);
                ok = false;
            }
            resources = null;
        }
        return ok;
    }
}
