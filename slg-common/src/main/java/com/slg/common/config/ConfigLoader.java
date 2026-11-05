package com.slg.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 加载 properties 配置（UTF-8）。查找顺序：
 * <ol>
 *     <li>系统属性 {@code -Dslg.config=路径}（Windows 路径如 E:\testServer\conf\gateway.properties 亦可）</li>
 *     <li>工作目录下 {@code config/<name>}</li>
 *     <li>classpath 下 {@code <name>}（jar 内默认配置）</li>
 * </ol>
 */
public final class ConfigLoader {

    public static final String CONFIG_PATH_PROPERTY = "slg.config";

    private ConfigLoader() {
    }

    public static ServerConfig load(String name) {
        String explicit = System.getProperty(CONFIG_PATH_PROPERTY);
        if (explicit != null && !explicit.isBlank()) {
            return fromFile(Paths.get(explicit));
        }
        Path local = Paths.get("config", name);
        if (Files.isRegularFile(local)) {
            return fromFile(local);
        }
        return fromClasspath(name);
    }

    public static ServerConfig fromFile(Path path) {
        try (Reader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            Properties p = new Properties();
            p.load(r);
            return new ServerConfig(p, path.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new IllegalStateException("failed to load config " + path.toAbsolutePath(), e);
        }
    }

    public static ServerConfig fromClasspath(String name) {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("config not found on classpath: " + name);
            }
            Properties p = new Properties();
            p.load(new InputStreamReader(in, StandardCharsets.UTF_8));
            return new ServerConfig(p, "classpath:" + name);
        } catch (IOException e) {
            throw new IllegalStateException("failed to load config classpath:" + name, e);
        }
    }
}
