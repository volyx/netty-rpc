package ru.alepar.rpc.client;

import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

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
import ru.alepar.rpc.api.ExceptionListener;
import ru.alepar.rpc.api.Remote;
import ru.alepar.rpc.api.RpcClient;
import ru.alepar.rpc.api.exception.TransportException;
import ru.alepar.rpc.common.KeepAliveTimer;
import ru.alepar.rpc.common.NettyRemote;
import ru.alepar.rpc.common.message.ExceptionNotify;
import ru.alepar.rpc.common.message.HandshakeFromClient;
import ru.alepar.rpc.common.message.HandshakeFromServer;
import ru.alepar.rpc.common.message.InvocationRequest;
import ru.alepar.rpc.common.message.KeepAlive;
import ru.alepar.rpc.common.message.RpcMessage;

import static ru.alepar.rpc.common.Util.foldClassesToStrings;
import static ru.alepar.rpc.common.Util.invokeMethod;
import static ru.alepar.rpc.common.Util.unfoldStringToClasses;

public class NettyRpcClient implements RpcClient {

    private final Logger log = LoggerFactory.getLogger(NettyRpcClient.class);

    private final ClassResolver classResolver;
    private final KeepAliveTimer keepAliveTimer;
    private final CountDownLatch latch;

    private final Map<Class<?>, Object> implementations;
    private final ExceptionListener[] listeners;

    private final Bootstrap bootstrap;
    private final Channel channel;
    private volatile NettyRemote remote;

    public NettyRpcClient(final InetSocketAddress remoteAddress, final Map<Class<?>, Object> implementations, final ExceptionListener[] listeners, final ClassResolver classResolver, final long keepalivePeriod) {
        this.implementations = implementations;
        this.listeners = listeners;
        this.classResolver = classResolver;

        bootstrap = new Bootstrap();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class); // (3)
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ObjectEncoder());
                    ch.pipeline().addLast(new ObjectDecoder(classResolver));
                    ch.pipeline().addLast(new RpcHandler());
                }
            });

            final ChannelFuture future = bootstrap.connect(remoteAddress);
            channel = future.awaitUninterruptibly().channel();

            // Start the client.
            ChannelFuture f = bootstrap.connect(remoteAddress).sync(); // (5)

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();

            if (!future.isSuccess()) {
//            bootstrap.relereleaseExternalResources();
                throw new TransportException("failed to connect to " + remoteAddress, future.cause());
            }

            latch = new CountDownLatch(1);
            channel.write(new HandshakeFromClient(foldClassesToStrings(new ArrayList<Class<?>>(implementations.keySet()))));
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted waiting for handshake", e);
            }

            keepAliveTimer = new KeepAliveTimer(Collections.singleton(remote), keepalivePeriod);
        } catch (Exception e) {
            log.error("",e);
            throw new RuntimeException();

        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public void shutdown() {
        keepAliveTimer.stop();
        channel.close().awaitUninterruptibly();
//        bootstrap.releaseExternalResources();
    }

    @Override
    public Remote getRemote() {
        return remote;
    }

    private void fireException(Exception exc) {
        for (ExceptionListener listener : listeners) {
            try {
                listener.onExceptionCaught(remote, exc);
            } catch (Exception e) {
                log.error("exception listener " + listener + " threw exception", exc);
            }
        }
    }

    private class RpcHandler extends SimpleChannelInboundHandler implements RpcMessage.Visitor {


        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            RpcMessage message = (RpcMessage) msg;
            log.debug("client got message {}", message.toString());

            message.visit(this);
        }

        @Override
        public void acceptExceptionNotify(ExceptionNotify msg) {
            fireException(msg.exc);
        }

        @Override
        public void acceptHandshakeFromClient(HandshakeFromClient msg) {
            // ignore // shudn't happen
        }

        @Override
        public void acceptHandshakeFromServer(HandshakeFromServer msg) {
            try {
                remote = new NettyRemote(channel, msg.clientId, new HashSet<Class<?>>(unfoldStringToClasses(classResolver, msg.classNames)));
            } catch (ClassNotFoundException e) {
                log.error("interfaces registered on server side are not in the classpath", e);
                throw new RuntimeException("interfaces registered on server side are not in the classpath", e);
            } finally {
                latch.countDown();
            }
        }

        @Override
        public void acceptInvocationRequest(InvocationRequest msg) {
            try {
                Class<?> clazz = classResolver.resolve(msg.className);
                Object impl = getImplementation(msg, clazz);

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

        private Object getImplementation(InvocationRequest msg, Class<?> clazz) {
            Object impl = implementations.get(clazz);
            if (impl == null) {
                throw new RuntimeException("interface is not registered on client: " + msg.className);
            }
            return impl;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            fireException(new TransportException(cause));
        }
    }

}
