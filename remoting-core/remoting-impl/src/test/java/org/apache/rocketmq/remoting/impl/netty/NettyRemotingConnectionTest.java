/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.remoting.impl.netty;

import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.RemotingServer;
import org.apache.rocketmq.remoting.api.RequestProcessor;
import org.apache.rocketmq.remoting.api.channel.ChannelEventListener;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.interceptor.Interceptor;
import org.apache.rocketmq.remoting.api.interceptor.RequestContext;
import org.apache.rocketmq.remoting.api.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.config.RemotingClientConfig;
import org.apache.rocketmq.remoting.config.RemotingServerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyRemotingConnectionTest extends BaseTest {
    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    private static short requestCode = 123;
    private RemotingCommand request;

    private static String remoteAddr;
    private static RemotingChannel channelInServer;

    @BeforeClass
    public static void setUp() throws Exception {
        RemotingClientConfig clientConfig = new RemotingClientConfig();
        clientConfig.setRemotingShutdownQuietPeriodMillis(0);
        clientConfig.setRemotingShutdownTimeoutMillis(10);

        RemotingServerConfig serverConfig = new RemotingServerConfig();
        serverConfig.setRemotingShutdownQuietPeriodMillis(0);
        serverConfig.setRemotingShutdownTimeoutMillis(10);

        remotingClient = new NettyRemotingClient(clientConfig);
        remotingServer = new NettyRemotingServer(serverConfig);

        remotingServer.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingServer.commandFactory().createResponse(request);
                response.payload("Pong".getBytes());
                return response;
            }
        });

        remotingClient.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingServer.commandFactory().createResponse(request);
                response.payload("ClientPong".getBytes());
                return response;
            }
        });

        remotingServer.registerChannelEventListener(new ChannelEventListener() {
            @Override
            public void onChannelConnect(final RemotingChannel channel) {
                channelInServer = channel;
            }

            @Override
            public void onChannelClose(final RemotingChannel channel) {
                channelInServer = null;
            }

            @Override
            public void onChannelException(final RemotingChannel channel, final Throwable cause) {
                channelInServer = null;
            }

            @Override
            public void onChannelIdle(final RemotingChannel channel) {
                channelInServer = null;
            }
        });

        remotingServer.start();
        remotingClient.start();

        remoteAddr = "127.0.0.1:" + remotingServer.localListenPort();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        remotingClient.stop();
        remotingServer.stop();
    }

    @Before
    public void setUp0() throws Exception {
        request = remotingClient.commandFactory().createRequest();
        request.cmdCode(requestCode);

        if (channelInServer == null) {
            RemotingCommand rsp = remotingClient.invoke(remoteAddr, request, 3000);
            assertThat(new String(rsp.payload())).isEqualTo("Pong");

            // Refresh the command
            request = remotingClient.commandFactory().createRequest();
            request.cmdCode(requestCode);
        }
    }

    @Test
    public void invokeToServer_Success() {
        RemotingCommand rsp = remotingClient.invoke(remoteAddr, request, 3000);
        assertThat(new String(rsp.payload())).isEqualTo("Pong");
    }

    @Test
    public void invokeAsyncToServer_Success() {
        final ObjectFuture<RemotingCommand> objectFuture = newObjectFuture(1, 1000);

        remotingClient.invokeAsync(remoteAddr, request, new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {

            }

            @Override
            public void onSuccess(final RemotingCommand response) {
                objectFuture.putObject(response);
                objectFuture.release();
            }
        }, 3000);

        assertThat(new String(objectFuture.getObject().payload())).isEqualTo("Pong");
    }

    @Test
    public void invokeOnewayToServer_Success() {
        final ObjectFuture<RemotingCommand> requestFuture = newObjectFuture(1, 1000);
        final ObjectFuture<RemotingCommand> responseFuture = newObjectFuture(1, 1000);

        remotingServer.registerInterceptor(new Interceptor() {
            @Override
            public void beforeRequest(final RequestContext context) {
                requestFuture.putObject(context.getRequest());
                requestFuture.release();
            }

            @Override
            public void afterResponseReceived(final ResponseContext context) {
                responseFuture.putObject(context.getResponse());
                responseFuture.release();
            }
        });

        remotingClient.invokeOneWay(remoteAddr, request);

        assertThat(requestFuture.getObject()).isEqualTo(request);
        assertThat(new String(responseFuture.getObject().payload())).isEqualTo("Pong");
    }

    @Test
    public void invokeToClient_Success() {
        RemotingCommand rsp = remotingServer.invoke(channelInServer, request, 3000);
        assertThat(new String(rsp.payload())).isEqualTo("ClientPong");
    }

    @Test
    public void invokeAsyncToClient_Success() {
        final ObjectFuture<RemotingCommand> objectFuture = newObjectFuture(1, 1000);

        remotingServer.invokeAsync(channelInServer, request, new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {

            }

            @Override
            public void onSuccess(final RemotingCommand response) {
                objectFuture.putObject(response);
                objectFuture.release();
            }
        }, 3000);

        assertThat(new String(objectFuture.getObject().payload())).isEqualTo("ClientPong");
    }

    @Test
    public void invokeOnewayToClient_Success() {
        final ObjectFuture<RemotingCommand> requestFuture = newObjectFuture(1, 1000);
        final ObjectFuture<RemotingCommand> responseFuture = newObjectFuture(1, 1000);

        remotingClient.registerInterceptor(new Interceptor() {
            @Override
            public void beforeRequest(final RequestContext context) {
                requestFuture.putObject(context.getRequest());
                requestFuture.release();
            }

            @Override
            public void afterResponseReceived(final ResponseContext context) {
                responseFuture.putObject(context.getResponse());
                responseFuture.release();
            }
        });

        remotingServer.invokeOneWay(channelInServer, request);

        assertThat(requestFuture.getObject()).isEqualTo(request);
        assertThat(new String(responseFuture.getObject().payload())).isEqualTo("ClientPong");
    }
}
