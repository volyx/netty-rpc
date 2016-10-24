package com.volyx.rpc.client;

import com.volyx.rpc.api.ExceptionListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import javax.annotation.Nonnull;
import java.util.Map;

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

    public RpcClientInitializer(@Nonnull Map<Class<?>, Object> implementations, @Nonnull ExceptionListener[] listeners, @Nonnull ClassResolver classResolver) {
        this.implementations = implementations;
        this.listeners = listeners;
        this.classResolver = classResolver;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ObjectEncoder());
        ch.pipeline().addLast(new ObjectDecoder(classResolver));
        ch.pipeline().addLast(new RpcClientHandler(implementations, listeners, classResolver));
    }
}
