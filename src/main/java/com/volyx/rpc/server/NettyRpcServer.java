package com.volyx.rpc.server;

import com.volyx.rpc.api.ClientListener;
import com.volyx.rpc.common.NettyRemote;
import com.volyx.rpc.common.Util;
import com.volyx.rpc.common.message.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.volyx.rpc.api.ExceptionListener;
import com.volyx.rpc.api.Remote;
import com.volyx.rpc.api.RpcServer;
import com.volyx.rpc.api.exception.TransportException;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.unmodifiableCollection;

public class NettyRpcServer implements RpcServer {

    private final Logger log = LoggerFactory.getLogger(NettyRpcServer.class);

    private final ClientRepository clients = new ClientRepository();
    private final ClassResolver classResolver;

    private final Map<Class<?>, ServerProvider<?>> implementations;
    private final ExceptionListener[] exceptionListeners;
    private final ClientListener[] clientListeners;
    private Channel acceptChannel;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    public NettyRpcServer(final InetSocketAddress bindAddress, final Map<Class<?>, ServerProvider<?>> implementations, final ExceptionListener[] exceptionListeners, final ClientListener[] clientListeners, final ClassResolver classResolver) {
        this.exceptionListeners = exceptionListeners;
        this.clientListeners = clientListeners;
        this.implementations = implementations;
        this.classResolver = classResolver;
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();


            bootstrap.group(bossGroup, workerGroup);

            bootstrap.channel(NioServerSocketChannel.class)
                    .localAddress(bindAddress.getPort())
                    .childHandler(new RpcServerInitializer(clients, implementations, exceptionListeners, clientListeners, classResolver))
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture future = bootstrap.bind().awaitUninterruptibly();

            if (future.isDone()) {
                if (future.isSuccess()) {
                    acceptChannel = future.channel();

                    StringBuilder channelInfo = new StringBuilder(50);
                    int i = 0;
                    channelInfo.append(String.format("ServerChannel-%02d", i + 1));
                    channelInfo.append('/');
                    channelInfo.append("0:0:0:0").append(':');
                    channelInfo.append(bindAddress.getPort());

                    log.info("Finish to start up a Netty Server. channel info: {}", channelInfo);

                } else {
                    Throwable cause = future.cause();
                    log.error("Fail to bind server on port. local port: {}", bindAddress.getPort(), cause);
                }
            }


            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
//            f.channel().closeFuture().sync();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    @Override
    public void shutdown() {
        try {

            // close main channel
            acceptChannel.close().await();

            // send close message to all clients
            List<ChannelFuture> futures = new LinkedList<ChannelFuture>();
            for (NettyRemote client : clients.getClients()) {
                if (client.getChannel().isOpen()) {
                    futures.add(client.getChannel().close());
                }
            }

            // wait for close to complete
            for (ChannelFuture future : futures) {
                future.await();
            }

            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();


        } catch (InterruptedException e) {
            throw new RuntimeException("failed to shutdown properly", e);
        }
    }

    @Override
    public Remote getClient(ChannelId clientId) {
        return clients.getClient(clientId);
    }

    @Override
    public Collection<Remote> getClients() {
        return unmodifiableCollection((Collection<? extends Remote>) clients.getClients());
    }


}
