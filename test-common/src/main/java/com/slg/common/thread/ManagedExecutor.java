package com.slg.common.thread;

import java.util.concurrent.TimeUnit;

public interface ManagedExecutor {
    String name();
    void start();
    boolean shutdown(long timeout, TimeUnit unit);
}
