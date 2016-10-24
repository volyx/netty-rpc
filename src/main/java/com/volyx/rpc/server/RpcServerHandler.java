package com.volyx.rpc.server;

import com.volyx.rpc.api.ClientListener;
import com.volyx.rpc.api.ExceptionListener;
import com.volyx.rpc.api.Remote;
import com.volyx.rpc.api.exception.TransportException;
import com.volyx.rpc.common.NettyRemote;
import com.volyx.rpc.common.Util;
import com.volyx.rpc.common.message.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.serialization.ClassResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RpcServerHandler extends SimpleChannelInboundHandler implements RpcMessage.Visitor {

    private final ConcurrentMap<Class<?>, Object> cache = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(RpcServerHandler.class);

    private Channel channel;
    private NettyRemote remote;
    private ClientRepository clients;
    private final Map<Class<?>, ServerProvider<?>> implementations;
    private final ExceptionListener[] exceptionListeners;
    private final ClientListener[] clientListeners;
    private final ClassResolver classResolver;

    public RpcServerHandler(ClientRepository clients, Map<Class<?>, ServerProvider<?>> implementations, ExceptionListener[] exceptionListeners, ClientListener[] clientListeners, @Nonnull ClassResolver classResolver) {
        this.clients = clients;
        this.implementations = implementations;
        this.exceptionListeners = exceptionListeners;
        this.clientListeners = clientListeners;
        this.classResolver = classResolver;
    }

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
            logger.error("interfaces registered on client side are not in the classpath", e);
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
            logger.error("caught exception while trying to invoke implementation", exc);
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
        logger.debug("server got message {} from {}", message.toString(), ctx.channel().toString());
        message.visit(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        fireException(remote, new TransportException(cause));
    }

    private void fireException(Remote remote, Exception exc) {
        for (ExceptionListener listener : exceptionListeners) {
            try {
                listener.onExceptionCaught(remote, exc);
            } catch (Exception e) {
                logger.error("exception listener " + listener + " threw exception", e);
            }
        }
    }

    private void fireClientConnect(Remote remote) {
        for (ClientListener listener : clientListeners) {
            try {
                listener.onClientConnect(remote);
            } catch (Exception e) {
                logger.error("remote listener " + listener + " threw exception", e);
            }
        }
    }

    private void fireClientDisconnect(Remote remote) {
        for (ClientListener listener : clientListeners) {
            try {
                listener.onClientDisconnect(remote);
            } catch (Exception e) {
                logger.error("remote listener " + listener + " threw exception", e);
            }
        }
    }
}
