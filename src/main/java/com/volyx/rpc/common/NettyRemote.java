package com.volyx.rpc.common;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Set;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import com.volyx.rpc.api.Remote;
import com.volyx.rpc.api.exception.ConfigurationException;
import com.volyx.rpc.common.message.InvocationRequest;

import static com.volyx.rpc.common.Util.foldClassesToStrings;
import static com.volyx.rpc.common.Util.toSerializable;

public class NettyRemote implements Remote, Serializable {

    private final Channel channel;
    private final Set<Class<?>> classes;

    public NettyRemote(Channel channel, Set<Class<?>> classes) {
        this.channel = channel;
        this.classes = classes;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <T> T getProxy(Class<T> clazz) {
        if (classes.contains(clazz)) {
            return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, new ProxyHandler());
        }

        throw new ConfigurationException("no implementation on remote side for " + clazz.getCanonicalName());
    }

    @Override
    public ChannelId getId() {
        return channel.id();
    }

    @Override
    public String getRemoteAddress() {
        return channel.remoteAddress().toString();
    }

    @Override
    public boolean isWritable() {
        return channel.isWritable();
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public int hashCode() {
        return channel.id().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NettyRemote that = (NettyRemote) o;

        return this.channel.id().equals(that.channel.id());
    }

    @Override
    public String toString() {
        return "NettyRemote{" + channel.id() + "}";
    }

    private class ProxyHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            channel.writeAndFlush(new InvocationRequest(
                    method.getDeclaringClass().getName(),
                    method.getName(),
                    toSerializable(args),
                    foldClassesToStrings(Arrays.asList(method.getParameterTypes())))
            );
            return null;
        }

    }

}
