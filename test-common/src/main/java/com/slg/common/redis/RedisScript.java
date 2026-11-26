package com.slg.common.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;

/**
 * 一段 Lua 脚本：优先用 EVALSHA 只发送摘要；服务端没有缓存该脚本（重启、主从切换、新的集群节点）时自动改用 EVAL。
 * 定义为常量复用即可，线程安全。集群模式下脚本访问的 key 必须都通过 keys 传入，并且落在同一个 slot。
 */
public final class RedisScript {

    private final String source;
    private final String sha;

    public RedisScript(String source) {
        this.source = source;
        this.sha = sha1Hex(source);
    }

    public String sha() {
        return sha;
    }

    /** 阻塞执行，只能在 IO 线程调用。 */
    public <T> T eval(Redis redis, ScriptOutputType type, String[] keys, byte[]... args) {
        try {
            return redis.sync().evalsha(sha, type, keys, args);
        } catch (RedisNoScriptException e) {
            return redis.sync().eval(source, type, keys, args);
        }
    }

    /** 异步执行，可以和其他异步命令一起 pipeline，再用 {@link Redis#awaitAll} 等待。 */
    public <T> CompletableFuture<T> evalAsync(Redis redis, ScriptOutputType type, String[] keys, byte[]... args) {
        CompletableFuture<T> bySha = redis.async().<T>evalsha(sha, type, keys, args).toCompletableFuture();
        return bySha.exceptionallyCompose(e -> Redis.unwrap(e) instanceof RedisNoScriptException
                ? redis.async().<T>eval(source, type, keys, args).toCompletableFuture()
                : CompletableFuture.failedFuture(e));
    }

    private static String sha1Hex(String s) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
