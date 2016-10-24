package com.volyx.rpc.server;

import com.volyx.rpc.api.Remote;

public class SimpleServerProvider<T> implements ServerProvider<T> {
    private final T impl;

    public SimpleServerProvider(T impl) {
        this.impl = impl;
    }

    @Override
    public T provideFor(Remote remote) {
        return impl;
    }
}
