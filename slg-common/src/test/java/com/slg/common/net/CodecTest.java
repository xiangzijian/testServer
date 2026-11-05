package com.slg.common.net;

import com.slg.common.net.client.ClientCodec;
import com.slg.common.net.client.ClientMessage;
import com.slg.common.net.internal.InternalCodec;
import com.slg.common.net.internal.InternalPacket;
import com.slg.common.net.internal.PacketKind;
import com.slg.common.proto.PingReq;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CodecTest {

    @Test
    void clientFrameRoundTrip() {
        EmbeddedChannel ch = new EmbeddedChannel();
        ClientCodec.install(ch.pipeline(), ClientCodec.DEFAULT_MAX_FRAME);
        ClientMessage msg = ClientMessage.of(20001, 7, PingReq.newBuilder().setContent("hi").build());

        ch.writeOutbound(msg);
        ByteBuf encoded = ch.readOutbound();
        assertEquals(4 + ClientCodec.HEADER_SIZE + msg.body().length, encoded.readableBytes());

        // 拆成两段写入，验证半包处理
        ch.writeInbound(encoded.readRetainedSlice(5));
        assertNull(ch.readInbound());
        ch.writeInbound(encoded);
        ClientMessage decoded = ch.readInbound();
        assertEquals(20001, decoded.msgId());
        assertEquals(7, decoded.seq());
        assertArrayEquals(msg.body(), decoded.body());
    }

    @Test
    void internalPacketRoundTrip() {
        EmbeddedChannel ch = new EmbeddedChannel();
        InternalCodec.install(ch.pipeline(), InternalCodec.DEFAULT_MAX_FRAME);
        InternalPacket p = new InternalPacket(PacketKind.FORWARD, 30001, 3, 99L, 12345L, 1, 777L, new byte[]{1, 2, 3});

        ch.writeOutbound(p);
        ByteBuf encoded = ch.readOutbound();
        ch.writeInbound(encoded);
        InternalPacket d = ch.readInbound();
        assertEquals(PacketKind.FORWARD, d.kind());
        assertEquals(30001, d.msgId());
        assertEquals(3, d.seq());
        assertEquals(99L, d.sessionId());
        assertEquals(12345L, d.playerId());
        assertEquals(1, d.kingdomId());
        assertEquals(777L, d.traceId());
        assertArrayEquals(new byte[]{1, 2, 3}, d.body());
    }
}
