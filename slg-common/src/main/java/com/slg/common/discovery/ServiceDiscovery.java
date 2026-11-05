package com.slg.common.discovery;

import java.util.List;

/**
 * 服务发现接口。当前只有 {@link StaticServiceDiscovery}；
 * 接注册中心（Nacos/etcd）时新增实现，并在节点变化时回调 {@link Listener}。
 */
public interface ServiceDiscovery {

    List<ServiceInstance> getInstances(ServiceType type);

    /** 订阅某类服务的节点变化；静态实现不会回调。 */
    default void subscribe(ServiceType type, Listener listener) {
    }

    /** 本节点注册到注册中心；静态实现为空操作。 */
    default void register(ServiceInstance self) {
    }

    default void close() {
    }

    @FunctionalInterface
    interface Listener {
        void onChanged(ServiceType type, List<ServiceInstance> instances);
    }
}
