package com.slg.common.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamedThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(NamedThreadFactory.class);

    static final Thread.UncaughtExceptionHandler LOGGING_HANDLER =
            (thread, ex) -> log.error("uncaught exception in thread {}", thread.getName(), ex);

    private final String prefix;
    private final boolean daemon;
    private final Thread.UncaughtExceptionHandler handler;
    private final AtomicInteger counter = new AtomicInteger();

    public NamedThreadFactory(String prefix, boolean daemon) {
        this(prefix, daemon, LOGGING_HANDLER);
    }

    public NamedThreadFactory(String prefix, boolean daemon, Thread.UncaughtExceptionHandler handler) {
        this.prefix = prefix;
        this.daemon = daemon;
        this.handler = handler;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
        t.setDaemon(daemon);
        t.setUncaughtExceptionHandler(handler);
        return t;
    }
}
