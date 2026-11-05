package com.slg.common.discovery;

public enum ServiceType {
    GATEWAY,
    GAME,
    WORLD;

    public String lowerName() {
        return name().toLowerCase();
    }

    public static ServiceType parse(String s) {
        return ServiceType.valueOf(s.trim().toUpperCase());
    }
}
