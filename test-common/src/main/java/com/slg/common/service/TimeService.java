package com.slg.common.service;

public class TimeService {

    private long offset;

    public long now() {
        return System.currentTimeMillis() + offset;
    }

    public boolean setOffset(long offset) {
        this.offset = offset;
        return true;
    }
}
