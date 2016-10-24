package com.volyx.rpc.server;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.volyx.rpc.common.NettyRemote;
import io.netty.channel.ChannelId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableCollection;

class ClientRepository {

    private static Logger logger = LoggerFactory.getLogger(ClientRepository.class);
    private final Map<ChannelId, NettyRemote> clients = new ConcurrentHashMap<>();

    public void addClient(NettyRemote remote) {
        logger.info("Add new client " + remote.getId());
        clients.put(remote.getId(), remote);
    }

    public void removeClient(ChannelId id) {
        logger.info("Remove client " + id);
        clients.remove(id);
    }

    public NettyRemote getClient(ChannelId clientId) {
        return clients.get(clientId);
    }

    public Collection<NettyRemote> getClients() {
        return unmodifiableCollection(clients.values());
    }
}
