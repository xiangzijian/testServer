package com.slg.common.config;

import java.util.Properties;

/**
 * 只读配置视图。取值优先级：JVM 系统属性（-Dkey=value） > 配置文件。
 */
public final class ServerConfig {

    private final Properties props;
    private final String source;

    public ServerConfig(Properties props, String source) {
        this.props = props;
        this.source = source;
    }

    public String source() {
        return source;
    }

    public String getString(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) {
            v = props.getProperty(key);
        }
        return v == null ? def : v.trim();
    }

    public String getRequired(String key) {
        String v = getString(key, null);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException("missing config key '" + key + "' in " + source);
        }
        return v;
    }

    public int getInt(String key, int def) {
        String v = getString(key, null);
        return v == null || v.isEmpty() ? def : Integer.parseInt(v);
    }

    public long getLong(String key, long def) {
        String v = getString(key, null);
        return v == null || v.isEmpty() ? def : Long.parseLong(v);
    }

    public boolean getBoolean(String key, boolean def) {
        String v = getString(key, null);
        return v == null || v.isEmpty() ? def : Boolean.parseBoolean(v);
    }

    /** 返回一个覆盖了部分 key 的新配置（主要用于测试）。 */
    public ServerConfig with(String key, String value) {
        Properties copy = new Properties();
        copy.putAll(props);
        copy.setProperty(key, value);
        return new ServerConfig(copy, source + "+override");
    }
}
