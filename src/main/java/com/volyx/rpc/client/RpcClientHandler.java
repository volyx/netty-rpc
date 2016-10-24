package com.volyx.rpc.client;

import com.volyx.rpc.api.ExceptionListener;
import com.volyx.rpc.api.exception.TransportException;
import com.volyx.rpc.common.NettyRemote;
import com.volyx.rpc.common.message.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.serialization.ClassResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Map;

import static com.volyx.rpc.common.Util.invokeMethod;
import static com.volyx.rpc.common.Util.unfoldStringToClasses;

public class RpcClientHandler extends SimpleChannelInboundHandler implements RpcMessage.Visitor {

    private static final Logger logger = LoggerFactory.getLogger(RpcClientHandler.class);
    @Nonnull
    private final Map<Class<?>, Object> implementations;
    @Nonnull
    private final ExceptionListener[] listeners;
    @Nonnull
    private ClassResolver classResolver;

    public RpcClientHandler(Map<Class<?>, Object> implementations, ExceptionListener[] listeners, @Nonnull ClassResolver classResolver) {
        this.implementations = implementations;
        this.listeners = listeners;
        this.classResolver = classResolver;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        RpcMessage message = (RpcMessage) msg;
        logger.debug("client got message {}", message.toString());

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
        throw new RuntimeException();
//        try {
//            id = msg.getClientId();
//            remote = new NettyRemote(channel, new HashSet<Class<?>>(unfoldStringToClasses(classResolver, msg.classNames)));
//        } catch (ClassNotFoundException e) {
//            logger.error("interfaces registered on server side are not in the classpath", e);
//            throw new RuntimeException("interfaces registered on server side are not in the classpath", e);
//        } finally {
//            latch.countDown();
//        }
    }

    @Override
    public void acceptInvocationRequest(InvocationRequest msg) {
        throw new RuntimeException();
//        try {
//            Class<?> clazz = classResolver.resolve(msg.className);
//            Object impl = getImplementation(msg, clazz);
//
//            invokeMethod(msg, impl, classResolver);
//        } catch (Exception exc) {
//            logger.error("caught exception while trying to invoke implementation", exc);
//            channel.write(new ExceptionNotify(exc));
//        }
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

    private void fireException(Exception exc) {
        throw new RuntimeException();
//        for (ExceptionListener listener : listeners) {
//            try {
//                listener.onExceptionCaught(remote, exc);
//            } catch (Exception e) {
//                logger.error("exception listener " + listener + " threw exception", exc);
//            }
//        }
    }
}