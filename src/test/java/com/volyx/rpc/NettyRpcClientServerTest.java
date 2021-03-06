package com.volyx.rpc;

import java.io.Serializable;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.volyx.rpc.api.ClientListener;
import com.volyx.rpc.api.ExceptionListener;
import com.volyx.rpc.api.Inject;
import com.volyx.rpc.api.NettyRpcClientBuilder;
import com.volyx.rpc.api.NettyRpcServerBuilder;
import com.volyx.rpc.api.Remote;
import com.volyx.rpc.api.RpcClient;
import com.volyx.rpc.api.RpcServer;
import com.volyx.rpc.api.exception.ConfigurationException;
import com.volyx.rpc.api.exception.RemoteException;
import com.volyx.rpc.api.exception.TransportException;

import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(JMock.class)
public class NettyRpcClientServerTest {

    private final Mockery mockery = new JUnit4Mockery();

    @Test/*(timeout = TIMEOUT)*/
    public void serverShutdownDoesNotHangIfThereAreStillClientsConnected() throws Exception {
        // it can hang if server does not close client channels before releasing bootstrap resources
        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS).build();
        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();

        server.shutdown();
        client.shutdown();
    }

    @Test(timeout = Config.TIMEOUT)
    public void invokesMethodsWithNoParamsAndVoidReturnType() throws Exception {
        final NoParamsVoidReturn impl = mockery.mock(NoParamsVoidReturn.class);

        mockery.checking(new Expectations() {{
            one(impl).go();
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(NoParamsVoidReturn.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final NoParamsVoidReturn proxy = client.getRemote().getProxy(NoParamsVoidReturn.class);

        try {
            proxy.go();
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT, expected = ConfigurationException.class)
    public void doNotAllowMethodsWithNonVoidReturnTypeToBeProxied() throws Exception {
        final NoParamsIntegerReturn impl = mockery.mock(NoParamsIntegerReturn.class);

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(NoParamsIntegerReturn.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final NoParamsIntegerReturn proxy = client.getRemote().getProxy(NoParamsIntegerReturn.class);

        try {
            proxy.go();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void subsequentCallsWork() throws Exception {
        final NoParamsVoidReturn impl = mockery.mock(NoParamsVoidReturn.class);

        mockery.checking(new Expectations() {{
            one(impl).go();
            one(impl).go();
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(NoParamsVoidReturn.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final NoParamsVoidReturn proxy = client.getRemote().getProxy(NoParamsVoidReturn.class);

        try {
            proxy.go();
            proxy.go();
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void methodParametersArePassedToRemoteSideProperly() throws Exception {
        final IntegerParam impl = mockery.mock(IntegerParam.class);
        final Integer param = 5;

        mockery.checking(new Expectations() {{
            one(impl).go(with(equalTo(param)));
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(IntegerParam.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final IntegerParam proxy = client.getRemote().getProxy(IntegerParam.class);

        try {
            proxy.go(param);
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void primitiveMethodParametersArePassedToRemoteSideProperly() throws Exception {
        final IntLongParam impl = mockery.mock(IntLongParam.class);
        final int paramInt = 5;
        final long paramLong = 10l;

        mockery.checking(new Expectations() {{
            one(impl).go(with(equalTo(paramInt)), with(equalTo(paramLong)));
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(IntLongParam.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final IntLongParam proxy = client.getRemote().getProxy(IntLongParam.class);

        try {
            proxy.go(paramInt, paramLong);
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void choosesOverloadedMethodByCompileTimeTypesAsOpposedToRuntimeTypes() throws Exception {
        final OverloadedString impl = mockery.mock(OverloadedString.class);
        final String s = "some string";

        mockery.checking(new Expectations() {{
            one(impl).go(with(any(Serializable.class)));
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(OverloadedString.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final OverloadedString proxy = client.getRemote().getProxy(OverloadedString.class);

        try {
            proxy.go((Serializable) s);
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT, expected = ConfigurationException.class)
    public void throwsProtocolExceptionIfCannotSerializeParams() throws Exception {
        final NonSerializable impl = mockery.mock(NonSerializable.class);

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(NonSerializable.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();
        final NonSerializable proxy = client.getRemote().getProxy(NonSerializable.class);

        try {
            proxy.param("", new Object());
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void throwsTransportExceptionIfConnectionIsAbruptlyTerminated() throws Throwable {
        final InfinteWaiter impl = new InfinteWaiter() {
            @SuppressWarnings({"InfiniteLoopStatement"})
            @Override
            public void hang() {
                try {
                    sleep(1000L);
                } catch (InterruptedException ignored) {}
            }
        };

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(InfinteWaiter.class, impl)
                .build();

        final ExceptionSavingListener listener = new ExceptionSavingListener();
        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addExceptionListener(listener)
                .setKeepAlive(30l)
                .build();
        final InfinteWaiter proxy = client.getRemote().getProxy(InfinteWaiter.class);

        try {
            proxy.hang();
            server.shutdown();
            Config.giveTimeForMessagesToBeProcessed();
            assertThat(listener.lastException(), notNullValue());
            assertThat(listener.lastException().getClass(), equalTo((Class)TransportException.class));
        } finally {
            client.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void exceptionsFromServerSideImplementationArePassedToClient() throws Throwable {
        final ThrowableThrower impl = new ThrowableThrower() {
            @Override
            public void go() throws Throwable {
                throw new IllegalAccessException("some exception");
            }
        };
        final ExceptionListener listener = mockery.mock(ExceptionListener.class);
        mockery.checking(new Expectations(){{
            one(listener).onExceptionCaught(with(any(Remote.class)), with(any(RemoteException.class)));
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(ThrowableThrower.class, impl)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addExceptionListener(listener)
                .build();
        final ThrowableThrower proxy = client.getRemote().getProxy(ThrowableThrower.class);

        try {
            proxy.go();
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test//(timeout = TIMEOUT)
    public void exceptionsFromClientSideImplementationArePassedToServer() throws Throwable {
        final ThrowableThrower impl = new ThrowableThrower() {
            @Override
            public void go() throws Throwable {
                throw new IllegalAccessException("some exception");
            }
        };
        final ExceptionListener listener = mockery.mock(ExceptionListener.class);
        mockery.checking(new Expectations() {{
            one(listener).onExceptionCaught(with(any(Remote.class)), with(any(RemoteException.class)));
        }});

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addExceptionListener(listener)
                .addClass(NoParamsVoidReturn.class, CallClientBack.class)
                .build();

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addObject(ThrowableThrower.class, impl)
                .build();

        final NoParamsVoidReturn proxy = client.getRemote().getProxy(NoParamsVoidReturn.class);

        try {
            proxy.go();
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void feedbackToClientWorks() throws Exception {
        final String MSG = "echoed-hi";

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addClass(SomeServerApi.class, SomeServerImpl.class)
                .build();

        final ClientApi mockClient = mockery.mock(ClientApi.class);
        mockery.checking(new Expectations() {{
            one(mockClient).feedback(MSG);
        }});

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addObject(ClientApi.class, mockClient)
                .build();

        final SomeServerApi proxy = client.getRemote().getProxy(SomeServerApi.class);

        try {
            proxy.go(MSG);
            Config.giveTimeForMessagesToBeProcessed();
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void serverImplementationsAreCachedIeStatePersistsAcrossClientCalls() throws Exception {
        final String MSG = "some state";

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addClass(State.class, ServerState.class)
                .build();

        final ClientState clientState = new ClientState();
        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addObject(State.class, clientState)
                .build();

        final State serverStateProxy = client.getRemote().getProxy(State.class);

        try {
            serverStateProxy.set(MSG);
            serverStateProxy.get();
            Config.giveTimeForMessagesToBeProcessed();
            assertThat(clientState.state, equalTo(MSG));
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void differentClientsHaveSeparateStateOnServerSide() throws Exception {
        final String MSG1 = "one state";
        final String MSG2 = "second state";

        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addClass(State.class, ServerState.class)
                .build();

        final ClientState clientOneState = new ClientState();
        final RpcClient clientOne = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addObject(State.class, clientOneState)
                .build();
        final State proxyOne = clientOne.getRemote().getProxy(State.class);

        final ClientState clientTwoState = new ClientState();
        final RpcClient clientTwo = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addObject(State.class, clientTwoState)
                .build();
        final State proxyTwo = clientTwo.getRemote().getProxy(State.class);

        try {
            proxyOne.set(MSG1);
            proxyTwo.set(MSG2);
            proxyOne.get();
            proxyTwo.get();
            Config.giveTimeForMessagesToBeProcessed();
            assertThat(clientOneState.state, equalTo(MSG1));
            assertThat(clientTwoState.state, equalTo(MSG2));
        } finally {
            clientOne.shutdown();
            clientTwo.shutdown();
            server.shutdown();
        }
    }

    @Test(timeout = Config.TIMEOUT)
    public void serverNotifiesAboutClientConnectsAndDisconnects() throws Exception {
        final ClientListener mock = mockery.mock(ClientListener.class);
        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addClientListener(mock)
                .build();

        mockery.checking(new Expectations(){{
            one(mock).onClientConnect(with(any(Remote.class))); // don't know the id yet
        }});

        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();

        mockery.checking(new Expectations(){{
            one(mock).onClientDisconnect(with(any(Remote.class))); // matcher, matching remote.getClientId()
        }});

        client.shutdown();
        Config.giveTimeForMessagesToBeProcessed();
        server.shutdown();
    }

    @Test
    public void clientAndServerShareSameId() throws Exception {
        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS).build();
        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();

        try {
            assertThat(server.getClient(client.getRemote().getId()), not(nullValue()));
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyOnClientForNonRegisteredOnServerInterfaceThrowsProtocolException() throws Exception {
        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS)
                .addObject(IntegerParam.class, new IntegerParam() {  // this is optional, add some unrelated interface to complicate matters
                    @Override
                    public void go(Integer i) {
                        //ignore
                    }
                })
                .build();
        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS).build();

        try {
            client.getRemote().getProxy(NoParamsVoidReturn.class);
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyOnServerForNonRegisteredOnClientInterfaceThrowsProtocolException() throws Exception {
        final RpcServer server = new NettyRpcServerBuilder(Config.BIND_ADDRESS).build();
        final RpcClient client = new NettyRpcClientBuilder(Config.BIND_ADDRESS)
                .addObject(IntegerParam.class, new IntegerParam() {  // this is optional, add some unrelated interface to complicate matters
                    @Override
                    public void go(Integer i) {
                        //ignore
                    }
                })
                .build();

        try {
            server.getClient(client.getRemote().getId()).getProxy(NoParamsVoidReturn.class);
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    public interface NoParamsVoidReturn {
        void go();
    }
    public interface NoParamsIntegerReturn {
        Integer go();
    }

    public interface IntegerParam {
        void go(Integer i);
    }
    public interface IntLongParam {
        void go(int i, long l);
    }

    public interface OverloadedString {
        void go(String s); // though unused, is vital for correctnes of corresponding unit test
        void go(Serializable s);
    }
    public interface NonSerializable {
        void param(String s, Object o);
    }
    public interface InfinteWaiter {
        void hang();
    }
    public interface ThrowableThrower {
        void go() throws Throwable;
    }

    public interface SomeServerApi {
        void go(String msg);
    }

    public static class SomeServerImpl implements SomeServerApi {

        private final Remote remote;

        public SomeServerImpl(@Inject Remote remote) {
            this.remote = remote;
        }

        @Override
        public void go(String msg) {
            ClientApi clientProxy = remote.getProxy(ClientApi.class);
            clientProxy.feedback(msg);
        }
    }

    public interface ClientApi {
        void feedback(String msg);
    }

    public static class CallClientBack implements NoParamsVoidReturn {
        
        private final Remote remote;

        public CallClientBack(@Inject Remote remote) {
            this.remote = remote;
        }

        @Override
        public void go() {
            try {
                remote.getProxy(ThrowableThrower.class).go();
            } catch (Throwable ignored) {
                ignored.printStackTrace();
            }
        }
    }

    public interface State {
        void set(String state);
        void get();
    }
    
    public static class ServerState implements State {

        private final Remote remote;
        private String state;

        public ServerState(@Inject Remote remote) {
            this.remote = remote;
        }

        @Override
        public void set(String state) {
            this.state = state;
        }

        @Override
        public void get() {
            remote.getProxy(State.class).set(state);
        }
    }

    public static class ClientState implements State {

        private String state;

        @Override
        public void set(String state) {
            this.state = state;
        }

        @Override
        public void get() { }
    }

    public static class ExceptionSavingListener implements ExceptionListener {
        private volatile Exception lastException;

        @Override
        public void onExceptionCaught(Remote remote, Exception e) {
            this.lastException = e;
        }

        public Exception lastException() {
            return lastException;
        }
    }
}
