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

    public NettyRpcServer(final InetSocketAddress bindAddress, final Map<Class<?>, ServerProvider<?>> implementations, final ExceptionListener[] exceptionListeners, final ClientListener[] clientListeners, final ClassResolver classResolver) {
        this.exceptionListeners = exceptionListeners;
        this.clientListeners = clientListeners;
        this.implementations = implementations;
        this.classResolver = classResolver;
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();


            bootstrap.group(bossGroup, workerGroup);

            bootstrap.channel(NioServerSocketChannel.class)
                    .localAddress(bindAddress.getPort())
//                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new ObjectEncoder());
                            ch.pipeline().addLast(new ObjectDecoder(classResolver));
                            ch.pipeline().addLast(new RpcHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            // (7)
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

//            workerGroup.shutdownGracefully();
//            bossGroup.shutdownGracefully();


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

    private void fireException(Remote remote, Exception exc) {
        for (ExceptionListener listener : exceptionListeners) {
            try {
                listener.onExceptionCaught(remote, exc);
            } catch (Exception e) {
                log.error("exception listener " + listener + " threw exception", e);
            }
        }
    }

    private void fireClientConnect(Remote remote) {
        for (ClientListener listener : clientListeners) {
            try {
                listener.onClientConnect(remote);
            } catch (Exception e) {
                log.error("remote listener " + listener + " threw exception", e);
            }
        }
    }

    private void fireClientDisconnect(Remote remote) {
        for (ClientListener listener : clientListeners) {
            try {
                listener.onClientDisconnect(remote);
            } catch (Exception e) {
                log.error("remote listener " + listener + " threw exception", e);
            }
        }
    }

    private class RpcHandler extends SimpleChannelInboundHandler implements RpcMessage.Visitor {

        private final ConcurrentMap<Class<?>, Object> cache = new ConcurrentHashMap<Class<?>, Object>();

        private Channel channel;
        private NettyRemote remote;

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            channel = ctx.channel();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
//            fireClientDisconnect(remote);
//            clients.removeClient(remote.getId());
        }


        @Override
        public void acceptExceptionNotify(ExceptionNotify msg) {
            fireException(remote, msg.exc);
        }

        @Override
        public void acceptHandshakeFromClient(HandshakeFromClient msg) {
            try {
                remote = new NettyRemote(channel, new HashSet<Class<?>>(Util.unfoldStringToClasses(classResolver, msg.classNames)));
                channel.writeAndFlush(new HandshakeFromServer(remote.getId(), Util.foldClassesToStrings(new ArrayList<Class<?>>(implementations.keySet()))));
                clients.addClient(remote);
                fireClientConnect(remote);
            } catch (ClassNotFoundException e) {
                log.error("interfaces registered on client side are not in the classpath", e);
                throw new RuntimeException("interfaces registered on client side are not in the classpath", e);
            }
        }

        @Override
        public void acceptHandshakeFromServer(HandshakeFromServer msg) {
            // ignore // shudn't happen
        }

        @Override
        public void acceptInvocationRequest(InvocationRequest msg) {
            try {
                Class<?> clazz = classResolver.resolve(msg.className);
                Object impl = getImplementation(clazz);
                Util.invokeMethod(msg, impl, classResolver);
            } catch (Exception exc) {
                log.error("caught exception while trying to invoke implementation", exc);
                channel.writeAndFlush(new ExceptionNotify(exc));
            }
        }

        @Override
        public void acceptKeepAlive(KeepAlive msg) {
            // ignore
        }

        private Object getImplementation(Class<?> clazz) {
            Object impl = cache.get(clazz);
            if (impl == null) {
                impl = createImplementation(clazz);
                cache.put(clazz, impl);
            }
            return impl;
        }

        private Object createImplementation(Class<?> clazz) {
            ServerProvider<?> provider = implementations.get(clazz);
            if (provider == null) {
                throw new RuntimeException("interface is not registered on server: " + clazz.getCanonicalName());
            }
            return provider.provideFor(remote);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            RpcMessage message = (RpcMessage) msg;
            log.debug("server got message {} from {}", message.toString(), ctx.channel().toString());
            message.visit(this);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            super.exceptionCaught(ctx, cause);
            fireException(remote, new TransportException(cause));
        }

    }

}
