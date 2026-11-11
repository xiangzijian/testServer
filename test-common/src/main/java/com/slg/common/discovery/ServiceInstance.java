package com.slg.common.discovery;

/**
 * 一个后端节点。nodeId 在同一 serviceType 内唯一。
 */
public record ServiceInstance(ServiceType type, int nodeId, String host, int port) {

    public String key() {
        return type.lowerName() + "-" + nodeId;
    }

    @Override
    public String toString() {
        return key() + "@" + host + ":" + port;
    }
}
