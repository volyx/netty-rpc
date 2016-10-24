package ru.alepar.rpc;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;
import ru.alepar.rpc.api.NettyRpcClientBuilder;
import ru.alepar.rpc.api.NettyRpcServerBuilder;
import ru.alepar.rpc.api.RpcClient;
import ru.alepar.rpc.api.RpcServer;

import static ru.alepar.rpc.Config.TIMEOUT;
import static ru.alepar.rpc.Config.giveTimeForMessagesToBeProcessed;

public class NettyRpcBuildersTest {

    private final Mockery mockery = new JUnit4Mockery();

    @Test
    public void buildersCreateClientAndServerWhichCanTalk() throws Exception {
        final ServerRemote mock = mockery.mock(ServerRemote.class);
        mockery.checking(new Expectations() {{
            one(mock).call();
        }});

        final NettyRpcServerBuilder serverBuilder = new NettyRpcServerBuilder(Config.BIND_ADDRESS);
        final NettyRpcClientBuilder clientBuilder = new NettyRpcClientBuilder(Config.BIND_ADDRESS);

        RpcServer server = serverBuilder
                .addObject(ServerRemote.class, mock)
                .addObject(ServerService.class, new ServerService() {
                    @Override
                    public Result coolMethod(String a) {
                        System.out.println("Hello " + a);
                        return new Result("Hello " + a);
                    }

                })
                .build();

        RpcClient client = clientBuilder
                .setKeepAlive(50L)
                .build();

        try {
            ServerRemote proxy = client.getRemote().getProxy(ServerRemote.class);
            ServerService serverService = client.getRemote().getProxy(ServerService.class);

            Result result = serverService.coolMethod("123");
            System.out.println("Result " + result.getHello());


            proxy.call();
            giveTimeForMessagesToBeProcessed();
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

    public class Result {

        private String hello;

        public Result(String hello) {

            this.hello = hello;
        }

        public String getHello() {
            return hello;
        }
    }
}
