package com.volyx.rpc.common.message;

import io.netty.channel.ChannelId;

public class HandshakeFromServer extends RpcMessage {

    public final ChannelId clientId;
    public final String[] classNames;

    public HandshakeFromServer(final ChannelId clientId, final String[] classNames) {
        this.clientId = clientId;
        this.classNames = classNames;
    }

    @Override
    public void visit(Visitor visitor) {
        visitor.acceptHandshakeFromServer(this);
    }

    public ChannelId getClientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return "HandshakeFromServer{" +
                "clientId=" + clientId +
                '}';
    }
}
