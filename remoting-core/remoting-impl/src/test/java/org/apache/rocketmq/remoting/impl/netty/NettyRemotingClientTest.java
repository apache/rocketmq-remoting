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

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.exception.RemotingConnectFailureException;
import org.apache.rocketmq.remoting.api.exception.RemotingTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingClientTest extends BaseTest {
    @Spy
    private NettyRemotingClient remotingClient = new NettyRemotingClient(clientConfig());

    @Mock
    private ClientChannelManager channelManager;

    @Mock
    private Channel mockedChannel;

    @Before
    public void setUp() {
        remotingClient.start();
        remotingClient.setClientChannelManager(channelManager);
        when(channelManager.createIfAbsent(any(String.class))).thenReturn(mockedChannel);
        when(mockedChannel.isActive()).thenReturn(true);
    }

    @After
    public void tearDown() {
        remotingClient.stop();
    }

    @Test
    public void invoke_Success() {
        RemotingCommand request = remotingClient.commandFactory().createRequest();
        final RemotingCommand response = remotingClient.commandFactory().createResponse(request);

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                return response;
            }
        }).when(remotingClient).invokeWithInterceptor(mockedChannel, request, 3000);

        RemotingCommand returnedResp = remotingClient.invoke("127.0.0.1:10911", request, 3000);

        assertThat(response).isEqualTo(returnedResp);
    }

    @Test
    public void invoke_ConnectFailureException() {
        RemotingCommand request = remotingClient.commandFactory().createRequest();
        when(mockedChannel.isActive()).thenReturn(false);

        try {
            RemotingCommand returnedResp = remotingClient.invoke("127.0.0.1:10911", request, 3000);
            failBecauseExceptionWasNotThrown(RemotingConnectFailureException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemotingConnectFailureException.class);
        }
    }

    @Test
    public void invoke_TimeoutException() {
        RemotingCommand request = remotingClient.commandFactory().createRequest();

        doThrow(new RemotingTimeoutException("Timeout exception occurred")).when(remotingClient).invokeWithInterceptor(mockedChannel, request, 3000);

        try {
            RemotingCommand returnedResp = remotingClient.invoke("127.0.0.1:10911", request, 3000);
            failBecauseExceptionWasNotThrown(RemotingTimeoutException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemotingTimeoutException.class);
        }
    }

    @Test
    public void invokeAsync_Success() {
        RemotingCommand request = remotingClient.commandFactory().createRequest();
        final RemotingCommand response = remotingClient.commandFactory().createResponse(request);

        final ObjectFuture<RemotingCommand> objectFuture = newObjectFuture(1, 100);

        final AsyncHandler asyncHandler = new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {

            }

            @Override
            public void onSuccess(final RemotingCommand response) {
                objectFuture.putObject(response);
                objectFuture.release();
            }
        };

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                asyncHandler.onSuccess(response);
                return null;
            }
        }).when(remotingClient).invokeAsyncWithInterceptor(mockedChannel, request, asyncHandler,3000);

        remotingClient.invokeAsync("127.0.0.1:10911", request, asyncHandler, 3000);

        assertThat(objectFuture.getObject()).isEqualTo(response);

    }
}