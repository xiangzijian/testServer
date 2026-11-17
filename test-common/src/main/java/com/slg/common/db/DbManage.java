package com.slg.common.db;

import com.slg.common.config.ServerConfig;
import com.slg.common.thread.ManagedExecutor;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class DbManage implements ManagedExecutor {
    private final List<DbConfig> configs;
    private final Map<String, Db> dbs = new LinkedHashMap<>();
    private volatile boolean started;

    public DbManage(ServerConfig cfg) {
        this.configs = Arrays.stream(cfg.getRequired("db.names").split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(n -> DbConfig.from(cfg, n)).toList();
    }

    @Override public String name() { return "db"; }

    @Override
    public synchronized void start() {
        if (started) throw new IllegalStateException("already started");
        try {
            for (DbConfig c : configs) {
                Db db = new Db(c);
                dbs.put(c.name(), db);
                db.ping();
            }
            started = true;
        } catch (RuntimeException e) {
            closeAll();
            throw e;
        }
    }

    public Db get(String name) {
        Db db = started ? dbs.get(name) : null;
        if (db == null) throw new IllegalStateException("db not available: " + name);
        return db;
    }

    @Override
    public synchronized boolean shutdown(long timeout, TimeUnit unit) {
        started = false;
        closeAll();
        return true;
    }

    private void closeAll() {
        List<Db> list = new ArrayList<>(dbs.values());
        Collections.reverse(list);
        for (Db db : list) {
            try { db.close(); } catch (RuntimeException e) { /* 记录日志，继续关闭下一个 */ }
        }
        dbs.clear();
    }
}
