package ru.alepar.rpc.common;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Set;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import ru.alepar.rpc.api.Remote;
import ru.alepar.rpc.api.exception.ConfigurationException;
import ru.alepar.rpc.common.message.InvocationRequest;

import static ru.alepar.rpc.common.Util.foldClassesToStrings;
import static ru.alepar.rpc.common.Util.toSerializable;

public class NettyRemote implements Remote, Serializable {

    private final Channel channel;
    private final ChannelId clientId;
    private final Set<Class<?>> classes;

    public NettyRemote(Channel channel, Set<Class<?>> classes) {
        this.channel = channel;
        this.clientId = channel.id();
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
        return clientId;
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
        return clientId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NettyRemote that = (NettyRemote) o;

        return this.clientId.equals(that.clientId);
    }

    @Override
    public String toString() {
        return "NettyRemote{" + clientId + "}";
    }

    private class ProxyHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            channel.write(new InvocationRequest(
                    method.getDeclaringClass().getName(),
                    method.getName(),
                    toSerializable(args),
                    foldClassesToStrings(Arrays.asList(method.getParameterTypes())))
            );
            return null;
        }

    }

}
