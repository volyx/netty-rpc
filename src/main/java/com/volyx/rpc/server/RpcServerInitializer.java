package com.volyx.rpc.server;

import com.volyx.rpc.api.ClientListener;
import com.volyx.rpc.api.ExceptionListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import javax.annotation.Nonnull;
import java.util.Map;

public class RpcServerInitializer extends ChannelInitializer<SocketChannel> {

    private ClientRepository clients;
    private final Map<Class<?>, ServerProvider<?>> implementations;
    private final ExceptionListener[] exceptionListeners;
    private final ClientListener[] clientListeners;
    private final ClassResolver classResolver;

    private static final StringDecoder utf8Decoder = new StringDecoder(CharsetUtil.UTF_8);
    private static final StringEncoder utf8Encoder = new StringEncoder(CharsetUtil.UTF_8);
    private static final ChannelHandlerAdapter serverHandler = new ChannelHandlerAdapter();

    public RpcServerInitializer(ClientRepository clients, @Nonnull Map<Class<?>, ServerProvider<?>> implementations, @Nonnull ExceptionListener[] exceptionListeners, @Nonnull ClientListener[] clientListeners, @Nonnull ClassResolver classResolver) {
        this.clients = clients;
        this.implementations = implementations;
        this.exceptionListeners = exceptionListeners;
        this.clientListeners = clientListeners;
        this.classResolver = classResolver;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

//        ch.pipeline().addLast(new ObjectEncoder());
//        ch.pipeline().addLast(new ObjectDecoder(classResolver));
//        ch.pipeline().addLast(new RpcServerHandler(clients, implementations, exceptionListeners, clientListeners, classResolver));
        ChannelPipeline pipeline = ch.pipeline();
        // NUL (0x00) is a message delimiter
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.nulDelimiter()));

        // string encoder / decoder are responsible for encoding / decoding an UTF-8 string
        pipeline.addLast("encoder", utf8Encoder);
        pipeline.addLast("decoder", utf8Decoder);

        // server hander is responsible for as a remoting call skeleton
        pipeline.addLast("serverHandler", serverHandler);
    }

}
