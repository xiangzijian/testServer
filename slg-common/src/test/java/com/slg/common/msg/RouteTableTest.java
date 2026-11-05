package com.slg.common.msg;

import com.slg.common.discovery.ServiceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RouteTableTest {

    @Test
    void segmentsAndExactOverride() {
        RouteTable t = RouteTable.defaults().route(20500, ServiceType.WORLD);
        assertEquals(ServiceType.GATEWAY, t.lookup(MsgIds.HEARTBEAT_REQ));
        assertEquals(ServiceType.GAME, t.lookup(MsgIds.GAME_PING_REQ));
        assertEquals(ServiceType.WORLD, t.lookup(MsgIds.WORLD_PING_REQ));
        assertEquals(ServiceType.WORLD, t.lookup(20500));
        assertNull(t.lookup(99999));
    }

    @Test
    void overlappingRangeRejected() {
        assertThrows(IllegalArgumentException.class, () -> RouteTable.defaults().range(29000, 30500, ServiceType.GAME));
    }
}
