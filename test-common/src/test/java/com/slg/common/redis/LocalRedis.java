package com.slg.common.redis;

import com.slg.common.config.ServerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 测试用：在随机端口启动本机的 redis-server（需要在 PATH 中），没有则跳过当前测试类。
 */
public final class LocalRedis {

    private final Process server;
    private final RedisManage manage;

    private LocalRedis(Process server, RedisManage manage) {
        this.server = server;
        this.manage = manage;
    }

    public static LocalRedis start() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Process server = null;
        try {
            server = new ProcessBuilder("redis-server", "--port", String.valueOf(port), "--save", "", "--appendonly", "no")
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        } catch (IOException e) {
            assumeTrue(false, "redis-server not found on PATH");
        }
        long deadline = System.currentTimeMillis() + 5000;
        while (!canConnect(port)) {
            if (System.currentTimeMillis() >= deadline || !server.isAlive()) {
                server.destroyForcibly();
                assumeTrue(false, "redis-server did not start");
            }
            Thread.sleep(50);
        }
        Properties p = new Properties();
        p.setProperty("redis.names", "cache");
        p.setProperty("redis.cache.uri", "redis://127.0.0.1:" + port);
        p.setProperty("redis.ioThreads", "2");
        RedisManage manage = new RedisManage(new ServerConfig(p, "test"));
        manage.start();
        return new LocalRedis(server, manage);
    }

    public RedisManage manage() {
        return manage;
    }

    public Redis redis() {
        return manage.get("cache");
    }

    public void stop() {
        try {
            assertTrue(manage.shutdown(5, TimeUnit.SECONDS));
        } finally {
            server.destroyForcibly();
        }
    }

    private static boolean canConnect(int port) {
        try (Socket ignored = new Socket("127.0.0.1", port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
