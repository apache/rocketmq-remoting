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
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.interceptor.Interceptor;
import org.apache.rocketmq.remoting.api.interceptor.RequestContext;
import org.apache.rocketmq.remoting.api.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.config.RemotingClientConfig;
import org.apache.rocketmq.remoting.config.RemotingServerConfig;
import org.apache.rocketmq.remoting.internal.JvmUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EpollRemotingConnectionTest extends BaseTest {
    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    private static RemotingServer remotingEpollServer;
    private static RemotingClient remotingEpollClient;

    private static short requestCode = 123;
    private RemotingCommand request;

    private static String remoteAddr;
    private static String remoteEpollAddr;


    @Before
    public void enableOnLinux() throws Exception {
        Assume.assumeTrue(JvmUtils.isLinux());
    }

    @BeforeClass
    public static void setUp() throws Exception {
        RemotingClientConfig clientConfig = clientConfig();
        RemotingServerConfig serverConfig = serverConfig();

        RemotingClientConfig epollClientConfig = clientConfig();

        RemotingServerConfig epollServerConfig = serverConfig();
        epollServerConfig.setServerNativeEpollEnable(true);
        epollServerConfig.setServerListenPort(9999);

        remotingClient = new NettyRemotingClient(clientConfig);
        remotingServer = new NettyRemotingServer(serverConfig);

        remotingEpollServer = new NettyRemotingServer(epollServerConfig);
        remotingEpollClient = new NettyRemotingClient(epollClientConfig);

        remotingServer.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingServer.commandFactory().createResponse(request);
                response.payload("Pong".getBytes());
                return response;
            }
        });

        remotingEpollServer.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingServer.commandFactory().createResponse(request);
                response.payload("Pong".getBytes());
                return response;
            }
        });

        remotingServer.start();
        remotingClient.start();
        remotingEpollClient.start();
        remotingEpollServer.start();

        remoteAddr = "127.0.0.1:" + remotingServer.localListenPort();
        remoteEpollAddr = "127.0.0.1:" + remotingEpollServer.localListenPort();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        remotingClient.stop();
        remotingServer.stop();
        remotingEpollServer.stop();
        remotingEpollClient.stop();
    }

    public RemotingCommand requestCommand() {
        request = remotingClient.commandFactory().createRequest();
        request.cmdCode(requestCode);
        return request;
    }

    @Test
    public void invokeToServer_Success() {
        // Client to epoll server
        RemotingCommand rsp = remotingClient.invoke(remoteEpollAddr, requestCommand(), 3000);
        assertThat(new String(rsp.payload())).isEqualTo("Pong");

        // Epoll client to server
        rsp = remotingEpollClient.invoke(remoteAddr, requestCommand(), 3000);
        assertThat(new String(rsp.payload())).isEqualTo("Pong");

        // Epoll client to epoll server
        rsp = remotingEpollClient.invoke(remoteEpollAddr, requestCommand(), 3000);
        assertThat(new String(rsp.payload())).isEqualTo("Pong");
    }

    @Test
    public void invokeAsyncToServer_Success() {
        // Client to epoll server

        final ObjectFuture<RemotingCommand> objectFuture = newObjectFuture(1, 1000);

        remotingClient.invokeAsync(remoteEpollAddr, requestCommand(), new AsyncHandler() {
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

        // Epoll client to server
        remotingEpollClient.invokeAsync(remoteAddr, requestCommand(), new AsyncHandler() {
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

        // Epoll client to epoll server
        remotingEpollClient.invokeAsync(remoteEpollAddr, requestCommand(), new AsyncHandler() {
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
        final ObjectFuture<RemotingCommand> responseFuture = newObjectFuture(1, 1000);

        Interceptor interceptor = new Interceptor() {
            @Override
            public void beforeRequest(final RequestContext context) {
            }

            @Override
            public void afterResponseReceived(final ResponseContext context) {
                responseFuture.putObject(context.getResponse());
                responseFuture.release();
            }
        };

        remotingServer.registerInterceptor(interceptor);
        remotingEpollServer.registerInterceptor(interceptor);

        // Client to epoll server
        remotingClient.invokeOneWay(remoteEpollAddr, requestCommand());

        assertThat(new String(responseFuture.getObject().payload())).isEqualTo("Pong");

        // Epoll client to server
        remotingEpollClient.invokeOneWay(remoteAddr, requestCommand());

        assertThat(new String(responseFuture.getObject().payload())).isEqualTo("Pong");

        // Epoll client to epoll server
        remotingEpollClient.invokeOneWay(remoteEpollAddr, requestCommand());

        assertThat(new String(responseFuture.getObject().payload())).isEqualTo("Pong");
    }
}
