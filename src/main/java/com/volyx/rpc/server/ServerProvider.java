package com.volyx.rpc.server;

import com.volyx.rpc.api.Remote;

public interface ServerProvider<T> {
    
    T provideFor(Remote remote);
    
}
