package com.slg.common.msg;

import com.slg.common.discovery.ServiceType;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 消息 ID → 目标服务 的路由表。先查精确映射，再按号段区间匹配。
 */
public final class RouteTable {

    private final Map<Integer, ServiceType> exact = new HashMap<>();
    /** key = 区间起点（含），value = [end(含), service] */
    private final NavigableMap<Integer, Range> ranges = new TreeMap<>();

    private record Range(int start, int end, ServiceType service) {
    }

    /** 默认号段：1xxxx→GATEWAY(本地处理)，2xxxx→GAME，3xxxx→WORLD。 */
    public static RouteTable defaults() {
        return new RouteTable()
                .range(10000, 19999, ServiceType.GATEWAY)
                .range(20000, 29999, ServiceType.GAME)
                .range(30000, 39999, ServiceType.WORLD);
    }

    public RouteTable range(int startInclusive, int endInclusive, ServiceType service) {
        if (startInclusive > endInclusive) {
            throw new IllegalArgumentException("bad range " + startInclusive + "-" + endInclusive);
        }
        Map.Entry<Integer, Range> floor = ranges.floorEntry(endInclusive);
        if (floor != null && floor.getValue().end >= startInclusive) {
            throw new IllegalArgumentException("range " + startInclusive + "-" + endInclusive
                    + " overlaps " + floor.getValue());
        }
        ranges.put(startInclusive, new Range(startInclusive, endInclusive, service));
        return this;
    }

    public RouteTable route(int msgId, ServiceType service) {
        exact.put(msgId, service);
        return this;
    }

    /** @return 目标服务；未知消息返回 null */
    public ServiceType lookup(int msgId) {
        ServiceType s = exact.get(msgId);
        if (s != null) {
            return s;
        }
        Map.Entry<Integer, Range> e = ranges.floorEntry(msgId);
        if (e != null && msgId <= e.getValue().end) {
            return e.getValue().service;
        }
        return null;
    }
}
