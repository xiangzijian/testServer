package com.slg.common.net.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/**
 * 客户端帧编解码。服务端（gateway）和测试客户端（robot）共用。
 */
public final class ClientCodec {

    /** msgId(4) + seq(4) + flags(1) */
    public static final int HEADER_SIZE = 9;
    public static final int DEFAULT_MAX_FRAME = 1024 * 1024;

    private ClientCodec() {
    }

    /** 依次加入：拆包 → 解码 → 编码。 */
    public static void install(ChannelPipeline p, int maxFrameLength) {
        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
        p.addLast("clientDecoder", new Decoder());
        p.addLast("clientEncoder", ENCODER);
    }

    private static final Encoder ENCODER = new Encoder();

    public static final class Decoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < HEADER_SIZE) {
                throw new CorruptedFrameException("client frame too short: " + in.readableBytes());
            }
            int msgId = in.readInt();
            int seq = in.readInt();
            byte flags = in.readByte();
            byte[] body = new byte[in.readableBytes()];
            in.readBytes(body);
            out.add(new ClientMessage(msgId, seq, flags, body));
        }
    }

    @ChannelHandler.Sharable
    public static final class Encoder extends MessageToByteEncoder<ClientMessage> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ClientMessage msg, ByteBuf out) {
            byte[] body = msg.body() == null ? new byte[0] : msg.body();
            out.writeInt(HEADER_SIZE + body.length);
            out.writeInt(msg.msgId());
            out.writeInt(msg.seq());
            out.writeByte(msg.flags());
            out.writeBytes(body);
        }
    }
}
