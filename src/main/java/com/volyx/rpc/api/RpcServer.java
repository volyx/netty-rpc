package com.volyx.rpc.api;

import io.netty.channel.ChannelId;

import java.util.Collection;

public interface RpcServer {

    /**
     * @param clientId id of a client connected to this server
     * @return remote associated with this client
     */
    Remote getClient(ChannelId clientId);

    /**
     * @return remotes for all the clients currently connected to this server<br/>
     * note that returned collection instance will be updated if clients connect/disconnect, but will not throw ConcurrentModificationException
     */
    Collection<Remote> getClients();

    /**
     * shutdowns server, closes connections to all clients, and releases all resources used
     */
    void shutdown();

}
