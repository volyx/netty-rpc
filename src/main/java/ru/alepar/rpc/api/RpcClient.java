package ru.alepar.rpc.api;

import io.netty.channel.ChannelId;

public interface RpcClient {

    /**
     * @return remote associated with server
     */
    Remote getRemote();

    /**
     * shutdowns client and releases all resources used
     */
    void shutdown();

    ChannelId getId();
}
