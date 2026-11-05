package com.slg.common.net.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/** 内部协议编解码，见 {@link InternalPacket} 的帧格式说明。 */
public final class InternalCodec {

    /** kind(1) + msgId(4) + seq(4) + sessionId(8) + playerId(8) + kingdomId(4) + traceId(8) */
    public static final int HEADER_SIZE = 37;
    public static final int DEFAULT_MAX_FRAME = 4 * 1024 * 1024;

    private static final Encoder ENCODER = new Encoder();

    private InternalCodec() {
    }

    public static void install(ChannelPipeline p, int maxFrameLength) {
        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
        p.addLast("internalDecoder", new Decoder());
        p.addLast("internalEncoder", ENCODER);
    }

    public static final class Decoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < HEADER_SIZE) {
                throw new CorruptedFrameException("internal frame too short: " + in.readableBytes());
            }
            PacketKind kind = PacketKind.of(in.readByte());
            int msgId = in.readInt();
            int seq = in.readInt();
            long sessionId = in.readLong();
            long playerId = in.readLong();
            int kingdomId = in.readInt();
            long traceId = in.readLong();
            byte[] body = new byte[in.readableBytes()];
            in.readBytes(body);
            out.add(new InternalPacket(kind, msgId, seq, sessionId, playerId, kingdomId, traceId, body));
        }
    }

    @ChannelHandler.Sharable
    public static final class Encoder extends MessageToByteEncoder<InternalPacket> {
        @Override
        protected void encode(ChannelHandlerContext ctx, InternalPacket p, ByteBuf out) {
            byte[] body = p.body() == null ? new byte[0] : p.body();
            out.writeInt(HEADER_SIZE + body.length);
            out.writeByte(p.kind().code());
            out.writeInt(p.msgId());
            out.writeInt(p.seq());
            out.writeLong(p.sessionId());
            out.writeLong(p.playerId());
            out.writeInt(p.kingdomId());
            out.writeLong(p.traceId());
            out.writeBytes(body);
        }
    }
}
