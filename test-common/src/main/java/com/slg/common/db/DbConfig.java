package com.slg.common.db;

import com.slg.common.config.ServerConfig;

public record DbConfig(String name, String url, String username, String password,
                       int maxPoolSize, int minIdle,
                       long connectionTimeoutMs, long maxLifetimeMs, long slowSqlMs) {
    public static DbConfig from(ServerConfig cfg, String name) {
        String p = "db." + name + ".";
        int max = cfg.getInt(p + "maxPoolSize", 8);
        return new DbConfig(name,
                cfg.getRequired(p + "url"),
                cfg.getRequired(p + "username"),
                cfg.getString(p + "password", ""),
                max,
                cfg.getInt(p + "minIdle", max),
                cfg.getLong(p + "connectionTimeoutMs", 3000),
                cfg.getLong(p + "maxLifetimeMs", 30 * 60_000),
                cfg.getLong(p + "slowSqlMs", 100));
    }

    @Override
    public String toString() {   // record 默认的 toString 会把密码打进日志
        return "DbConfig[name=" + name + ", url=" + url + ", user=" + username
                + ", pool=" + minIdle + "/" + maxPoolSize + "]";
    }
}
