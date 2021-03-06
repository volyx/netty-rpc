package com.volyx.rpc.common.message;

public class KeepAlive extends RpcMessage {

    @Override
    public void visit(Visitor visitor) {
        visitor.acceptKeepAlive(this);
    }

}
