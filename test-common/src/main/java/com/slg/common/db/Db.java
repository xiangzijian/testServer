package com.slg.common.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

public final class Db {
    private static final Logger log = LoggerFactory.getLogger(Db.class);

    private final DbConfig config;
    private final HikariDataSource dataSource;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    Db(DbConfig c) {
        HikariConfig h = getHikariConfig(c);
        h.addDataSourceProperty("cachePrepStmts", "true");
        h.addDataSourceProperty("prepStmtCacheSize", "250");
        h.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        h.addDataSourceProperty("useServerPrepStmts", "true");
        h.addDataSourceProperty("rewriteBatchedStatements", "true");  // 批量 upsert 必须开
        this.config = c;
        this.dataSource = new HikariDataSource(h);
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    private static HikariConfig getHikariConfig(DbConfig c) {
        HikariConfig h = new HikariConfig();
        h.setPoolName("db-" + c.name());
        h.setJdbcUrl(c.url());
        h.setUsername(c.username());
        h.setPassword(c.password());
        h.setMaximumPoolSize(c.maxPoolSize());
        h.setMinimumIdle(c.minIdle());
        h.setConnectionTimeout(c.connectionTimeoutMs());
        h.setMaxLifetime(c.maxLifetimeMs());      // 要小于 MySQL 的 wait_timeout
        h.setKeepaliveTime(120_000);
        h.setInitializationFailTimeout(1);        // 连不上就在构造时直接失败
        return h;
    }

    public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
        long start = System.nanoTime();
        try {
            return jdbc.query(sql, mapper, args);
        } finally {
            checkSlow(sql, start);
        }
    }

    public int update(String sql, Object... args) {
        long start = System.nanoTime();
        try {
            return jdbc.update(sql, args);
        } finally {
            checkSlow(sql, start);
        }
    }

    public int[] batchUpdate(String sql, List<Object[]> batchArgs) {
        long start = System.nanoTime();
        try {
            return jdbc.batchUpdate(sql, batchArgs);
        } finally {
            checkSlow("batch(" + batchArgs.size() + "):" + sql, start);
        }
    }

    public <T> T inTransaction(String label, TransactionCallback<T> action) {
        long start = System.nanoTime();
        try {
            return tx.execute(action);
        } finally {
            checkSlow("tx:" + label, start);
        }
    }

    /** 不计时，只给框架内部或特殊场景使用。 */
    public JdbcTemplate jdbc() { return jdbc; }

    void ping() {
        String version = jdbc.queryForObject("SELECT VERSION()", String.class);
        log.info("db [{}] ready, mysql {}, {}", config.name(), version, config);
    }

    public PoolStats stats() {
        HikariPoolMXBean m = dataSource.getHikariPoolMXBean();
        return new PoolStats(m.getActiveConnections(), m.getIdleConnections(),
                m.getTotalConnections(), m.getThreadsAwaitingConnection());
    }

    void close() { dataSource.close(); }

    private void checkSlow(String sql, long startNanos) {
        long ms = (System.nanoTime() - startNanos) / 1_000_000;
        if (ms >= config.slowSqlMs()) {
            log.warn("slow sql {}ms db={} sql={}", ms, config.name(), sql);   // 不打印参数
        }
    }

    public record PoolStats(int active, int idle, int total, int waiting) {}
}