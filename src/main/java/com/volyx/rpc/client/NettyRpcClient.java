package com.volyx.rpc.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.volyx.rpc.api.ExceptionListener;
import com.volyx.rpc.api.RpcClient;
import com.volyx.rpc.common.NettyRemote;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.volyx.rpc.api.Remote;
import com.volyx.rpc.api.exception.TransportException;
import com.volyx.rpc.common.KeepAliveTimer;
import com.volyx.rpc.common.message.ExceptionNotify;
import com.volyx.rpc.common.message.HandshakeFromClient;
import com.volyx.rpc.common.message.HandshakeFromServer;
import com.volyx.rpc.common.message.InvocationRequest;
import com.volyx.rpc.common.message.KeepAlive;
import com.volyx.rpc.common.message.RpcMessage;

import static com.volyx.rpc.common.Util.foldClassesToStrings;
import static com.volyx.rpc.common.Util.invokeMethod;
import static com.volyx.rpc.common.Util.unfoldStringToClasses;

public class NettyRpcClient implements RpcClient {

    private final Logger log = LoggerFactory.getLogger(NettyRpcClient.class);

    private final ClassResolver classResolver;
    private KeepAliveTimer keepAliveTimer;
    private CountDownLatch latch;

    private final Map<Class<?>, Object> implementations;
    private final ExceptionListener[] listeners;

    private Channel channel;
    private volatile NettyRemote remote;
    private ChannelId id;
    private final NioEventLoopGroup workerGroup;

    public NettyRpcClient(final InetSocketAddress remoteAddress, final Map<Class<?>, Object> implementations, final ExceptionListener[] listeners, final ClassResolver classResolver, final long keepalivePeriod) {
        this.implementations = implementations;
        this.listeners = listeners;
        this.classResolver = classResolver;

        Bootstrap bootstrap = new Bootstrap();
        workerGroup = new NioEventLoopGroup();
        try {
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class); // (3)
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            bootstrap.remoteAddress(remoteAddress);
            bootstrap.handler(new RpcClientInitializer(implementations, listeners, classResolver));

            // connect the client to remote server the wait until the awaitUninterruptibly() method is completed
            final ChannelFuture future = bootstrap.connect().awaitUninterruptibly();

            if (future.isDone()) {
                if (!future.isSuccess()) {
//                bootstrap.releaseExternalResources();
                    throw new TransportException("failed to connect to " + remoteAddress, future.cause());
                }

                latch = new CountDownLatch(1);
                channel = future.channel();
                channel.writeAndFlush(new HandshakeFromClient(foldClassesToStrings(new ArrayList<Class<?>>(implementations.keySet()))));
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException("interrupted waiting for handshake", e);
                }

                keepAliveTimer = new KeepAliveTimer(Collections.singleton(remote), keepalivePeriod);
            }



        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();

        }
    }

    @Override
    public void shutdown() {
        keepAliveTimer.stop();
        channel.close().awaitUninterruptibly();
//        bootstrap.releaseExternalResources();
        workerGroup.shutdownGracefully();
    }

    @Override
    public Remote getRemote() {
        return remote;
    }

    public ChannelId getId() {
        return id;
    }

}
