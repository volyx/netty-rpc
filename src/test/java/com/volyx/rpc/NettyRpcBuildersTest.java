package com.volyx.rpc;

import com.volyx.rpc.api.NettyRpcClientBuilder;
import com.volyx.rpc.api.NettyRpcServerBuilder;
import com.volyx.rpc.api.RpcClient;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;
import com.volyx.rpc.api.RpcServer;

public class NettyRpcBuildersTest {

    @Test
    public void buildersCreateClientAndServerWhichCanTalk() throws Exception {
        final NettyRpcServerBuilder serverBuilder = new NettyRpcServerBuilder(Config.BIND_ADDRESS);
        final NettyRpcClientBuilder clientBuilder = new NettyRpcClientBuilder(Config.BIND_ADDRESS);

        final RpcServer server = serverBuilder
                .addObject(ServerRemote.class, new ServerRemoteImpl())
                .build();

        final RpcClient client1 = clientBuilder
                .addObject(ClientService.class, new ClientServiceImpl())
                .build();

        final RpcClient client2 = clientBuilder
                .addObject(ClientService.class, new ClientServiceImpl())
                .build();

        server.getClient(client1.getId()).getProxy(ClientService.class).boom();

        server.getClient(client2.getId()).getProxy(ClientService.class).boom();

        try {
            ServerRemote proxy = client1.getRemote().getProxy(ServerRemote.class);
            proxy.call();
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client1.shutdown();
            client2.shutdown();
            server.shutdown();
        }
    }

    public interface ServerRemote {
        void call();
    }

    public interface ServerService {

        Result coolMethod(String a);
    }

    public interface ClientService {

        void boom();
    }

    public class Result {

        private String hello;

        public Result(String hello) {

            this.hello = hello;
        }

        public String getHello() {
            return hello;
        }
    }

    public class ClientServiceImpl implements ClientService {

        @Override
        public void boom() {
            System.out.println("\t\t\t\n\n\n\nBOOOM");
        }
    }

    private class ServerRemoteImpl implements ServerRemote {
        @Override
        public void call() {
            System.out.println("\t\t\t\n\n\nBDIISH");
        }
    }
}
