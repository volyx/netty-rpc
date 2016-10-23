package ru.alepar.rpc.server;

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
import ru.alepar.rpc.api.ClientListener;
import ru.alepar.rpc.api.ExceptionListener;
import ru.alepar.rpc.api.Remote;
import ru.alepar.rpc.api.RpcServer;
import ru.alepar.rpc.api.exception.TransportException;
import ru.alepar.rpc.common.NettyId;
import ru.alepar.rpc.common.NettyRemote;
import ru.alepar.rpc.common.message.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.unmodifiableCollection;
import static ru.alepar.rpc.common.Util.*;

public class NettyRpcServer implements RpcServer {

    private final Logger log = LoggerFactory.getLogger(NettyRpcServer.class);

    private final ClientRepository clients = new ClientRepository();
    private final ClassResolver classResolver;

    private final Map<Class<?>, ServerProvider<?>> implementations;
    private final ExceptionListener[] exceptionListeners;
    private final ClientListener[] clientListeners;

    private final ServerBootstrap bootstrap;

    public NettyRpcServer(final InetSocketAddress bindAddress, final Map<Class<?>, ServerProvider<?>> implementations, final ExceptionListener[] exceptionListeners, final ClientListener[] clientListeners, final ClassResolver classResolver) {
        this.exceptionListeners = exceptionListeners;
        this.clientListeners = clientListeners;
        this.implementations = implementations;
        this.classResolver = classResolver;
            final EventLoopGroup bossGroup = new NioEventLoopGroup();
            final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            bootstrap = new ServerBootstrap();


            bootstrap.group(bossGroup, workerGroup);

            bootstrap.channel(NioServerSocketChannel.class)
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
            ChannelFuture f = bootstrap.bind(bindAddress.getPort()).sync(); // (7)
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @Override
    public void shutdown() {
        try {
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

        } catch (InterruptedException e) {
            throw new RuntimeException("failed to shutdown properly", e);
        }
    }

    @Override
    public Remote getClient(Remote.Id clientId) {
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
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            fireClientDisconnect(remote);
            clients.removeClient(remote.getId());
        }


        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            RpcMessage message = (RpcMessage) msg;
            log.debug("server got message {} from {}", message.toString(), ctx.channel().toString());
            message.visit(this);
        }


        @Override
        public void acceptExceptionNotify(ExceptionNotify msg) {
            fireException(remote, msg.exc);
        }

        @Override
        public void acceptHandshakeFromClient(HandshakeFromClient msg) {
            try {
                ChannelId channelId = channel.id();
                remote = new NettyRemote(channel, new NettyId(0), new HashSet<Class<?>>(unfoldStringToClasses(classResolver, msg.classNames)));
                channel.write(new HandshakeFromServer(remote.getId(), foldClassesToStrings(new ArrayList<Class<?>>(implementations.keySet()))));
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
                invokeMethod(msg, impl, classResolver);
            } catch (Exception exc) {
                log.error("caught exception while trying to invoke implementation", exc);
                channel.write(new ExceptionNotify(exc));
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
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            super.exceptionCaught(ctx, cause);
            fireException(remote, new TransportException(cause));
        }

    }

}
