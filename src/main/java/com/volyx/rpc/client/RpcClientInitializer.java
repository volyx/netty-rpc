package com.volyx.rpc.client;

import com.volyx.rpc.api.ExceptionListener;
import com.volyx.rpc.common.Constants;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by dima.v on 24.10.2016.
 */
public class RpcClientInitializer extends ChannelInitializer<SocketChannel> {
    @Nonnull
    private final Map<Class<?>, Object> implementations;
    @Nonnull
    private final ExceptionListener[] listeners;
    @Nonnull
    private ClassResolver classResolver;
    /**
     * Handles the idle channels.
     */
    private static final HeartbeatHandler heartbeatHandler = new HeartbeatHandler();

    /**
     * Decodes a received string.
     */
    private static final StringDecoder utf8Decoder = new StringDecoder(CharsetUtil.UTF_8);

    /**
     * Encodes a requested string.
     */
    private static final StringEncoder utf8Encoder = new StringEncoder(CharsetUtil.UTF_8);

    /**
     * The most important channel handler for processing business logic.
     */
    private static final ChannelHandlerAdapter clientHandler = new ChannelHandlerAdapter();

    public RpcClientInitializer(@Nonnull Map<Class<?>, Object> implementations, @Nonnull ExceptionListener[] listeners, @Nonnull ClassResolver classResolver) {
        this.implementations = implementations;
        this.listeners = listeners;
        this.classResolver = classResolver;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
//        ch.pipeline().addLast(new ObjectEncoder());
//        ch.pipeline().addLast(new ObjectDecoder(classResolver));
//        ch.pipeline().addLast(new RpcClientHandler(implementations, listeners, classResolver));

        // use the IdleStateHandler to get notified if you haven't received or sent data for dozens of seconds.
        // If this is the case, a heartbeat will be written to the remote peer, and if this fails the connection is closed.
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, Constants.HEARTBEAT_PERIOD, TimeUnit.SECONDS));
        pipeline.addLast("heartbeatHandler", heartbeatHandler);

        // NUL (0x00) is a message delimiter
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.nulDelimiter()));

        // string encoder / decoder are responsible for encoding / decoding an UTF-8 string
        pipeline.addLast("encoder", utf8Encoder);
        pipeline.addLast("decoder", utf8Decoder);

        // client hander is responsible for as a remoting call stub
        pipeline.addLast("clientHandler", clientHandler);
    }
}
