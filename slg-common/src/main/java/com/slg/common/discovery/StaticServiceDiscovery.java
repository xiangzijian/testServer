package com.slg.common.discovery;

import com.slg.common.config.ServerConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 从配置读取静态节点列表。格式：
 * <pre>
 * discovery.game.nodes=1@127.0.0.1:9201,2@127.0.0.1:9202
 * discovery.world.nodes=1@127.0.0.1:9101
 * </pre>
 */
public final class StaticServiceDiscovery implements ServiceDiscovery {

    private final Map<ServiceType, List<ServiceInstance>> instances = new EnumMap<>(ServiceType.class);

    public StaticServiceDiscovery(ServerConfig config) {
        for (ServiceType type : ServiceType.values()) {
            String raw = config.getString("discovery." + type.lowerName() + ".nodes", "");
            instances.put(type, Collections.unmodifiableList(parse(type, raw)));
        }
    }

    static List<ServiceInstance> parse(ServiceType type, String raw) {
        List<ServiceInstance> list = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return list;
        }
        for (String item : raw.split(",")) {
            String s = item.trim();
            if (s.isEmpty()) {
                continue;
            }
            int at = s.indexOf('@');
            int colon = s.lastIndexOf(':');
            if (at <= 0 || colon <= at) {
                throw new IllegalArgumentException("bad node spec '" + s + "', expect nodeId@host:port");
            }
            int nodeId = Integer.parseInt(s.substring(0, at));
            String host = s.substring(at + 1, colon);
            int port = Integer.parseInt(s.substring(colon + 1));
            list.add(new ServiceInstance(type, nodeId, host, port));
        }
        return list;
    }

    @Override
    public List<ServiceInstance> getInstances(ServiceType type) {
        return instances.getOrDefault(type, List.of());
    }
}
