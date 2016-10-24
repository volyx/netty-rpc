package ru.alepar.rpc.common.message;

import io.netty.channel.ChannelId;
import ru.alepar.rpc.api.Remote;

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

    @Override
    public String toString() {
        return "HandshakeFromServer{" +
                "clientId=" + clientId +
                '}';
    }
}
