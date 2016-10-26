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

    private final Mockery mockery = new JUnit4Mockery();

    @Test
    public void buildersCreateClientAndServerWhichCanTalk() throws Exception {
        final NettyRpcServerBuilder serverBuilder = new NettyRpcServerBuilder(Config.BIND_ADDRESS);
        final NettyRpcClientBuilder clientBuilder = new NettyRpcClientBuilder(Config.BIND_ADDRESS);

        RpcServer server = serverBuilder
                .addObject(ServerRemote.class, new ServerRemoteImpl())
                .build();

        RpcClient client = clientBuilder
                .addObject(ClientService.class, new ClientServiceImpl())
                .setKeepAlive(50L)
                .build();

        ClientService clientService = server.getClient(client.getId()).getProxy(ClientService.class);
        clientService.boom();

        try {
            ServerRemote proxy = client.getRemote().getProxy(ServerRemote.class);
            proxy.call();
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
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
            System.out.println("\t\t\t\n\n\n\nBOOOM ON SERVER");
        }
    }

    public class ServerRemoteImpl implements ServerRemote {
        @Override
        public void call() {
            System.out.println("\t\t\t\n\n\nBDIISH FROM CLIENT");
        }
    }
}
