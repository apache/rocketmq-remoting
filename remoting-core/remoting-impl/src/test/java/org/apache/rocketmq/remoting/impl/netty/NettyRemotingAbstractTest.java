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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingEndPoint;
import org.apache.rocketmq.remoting.api.RequestProcessor;
import org.apache.rocketmq.remoting.api.channel.ChannelEventListener;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.exception.RemotingAccessException;
import org.apache.rocketmq.remoting.api.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.api.exception.SemaphoreExhaustedException;
import org.apache.rocketmq.remoting.api.interceptor.Interceptor;
import org.apache.rocketmq.remoting.api.interceptor.RequestContext;
import org.apache.rocketmq.remoting.api.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.channel.NettyChannelImpl;
import org.apache.rocketmq.remoting.impl.command.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.impl.netty.handler.Decoder;
import org.apache.rocketmq.remoting.impl.netty.handler.Encoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingAbstractTest extends BaseTest {

    private NettyRemotingAbstract remotingAbstract;

    @Mock
    private Channel mockedClientChannel;

    private EmbeddedChannel clientChannel;

    private EmbeddedChannel serverChannel;

    private RemotingCommand remotingRequest;

    private RemotingCommand remotingRequestAsync;

    private RemotingCommand remotingRequestOneway;

    private short requestCode = 123;
    private int semaphoreNum = 5;

    @Before
    public void setUp() {
        RemotingConfig remotingConfig = new RemotingConfig() {
            @Override
            public int getOnewayInvokeSemaphore() {
                return semaphoreNum;
            }

            @Override
            public int getAsyncInvokeSemaphore() {
                return semaphoreNum;
            }
        };

        remotingAbstract = new NettyRemotingAbstract(remotingConfig) {
        };

        clientChannel = new EmbeddedChannel(new Encoder(), new Decoder(), new SimpleChannelInboundHandler<RemotingCommand>() {

            @Override
            protected void channelRead0(final ChannelHandlerContext ctx, final RemotingCommand msg) throws Exception {
                remotingAbstract.processMessageReceived(ctx, msg);
            }
        });

        serverChannel = new EmbeddedChannel(new Encoder(), new Decoder(), new SimpleChannelInboundHandler<RemotingCommand>() {

            @Override
            protected void channelRead0(final ChannelHandlerContext ctx, final RemotingCommand msg) throws Exception {
                remotingAbstract.processMessageReceived(ctx, msg);
            }
        });

        remotingRequest = remotingAbstract.commandFactory().createRequest();
        remotingRequest.cmdCode(requestCode);
        remotingRequest.payload("Ping".getBytes());

        remotingRequestAsync = remotingAbstract.commandFactory().createRequest();
        remotingRequestAsync.cmdCode(requestCode);
        remotingRequestAsync.payload("Ping".getBytes());

        remotingRequestOneway = remotingAbstract.commandFactory().createRequest();
        remotingRequestOneway.cmdCode(requestCode);
        remotingRequestOneway.payload("Ping".getBytes());

        // Simulate the tcp stack
        scheduleInThreads(new Runnable() {
            @Override
            public void run() {
                ByteBuf msg = clientChannel.readOutbound();
                if (msg != null) {
                    serverChannel.writeInbound(msg);
                }

                msg = serverChannel.readOutbound();

                if (msg != null) {
                    clientChannel.writeInbound(msg);
                }
            }
        }, 1);

        remotingAbstract.start();
    }

    @After
    public void tearDown() {
        remotingAbstract.stop();
    }

    @Test
    public void putNettyEvent_Success() {
        final Throwable exception = new RuntimeException();
        final ObjectFuture objectFuture = newObjectFuture(4, 100);
        remotingAbstract.registerChannelEventListener(new ChannelEventListener() {
            @Override
            public void onChannelConnect(final RemotingChannel channel) {
                if (((NettyChannelImpl) channel).getChannel() == mockedClientChannel) {
                    objectFuture.release();
                }
            }

            @Override
            public void onChannelClose(final RemotingChannel channel) {
                if (((NettyChannelImpl) channel).getChannel() == mockedClientChannel) {
                    objectFuture.release();
                }
            }

            @Override
            public void onChannelException(final RemotingChannel channel, final Throwable cause) {
                if (((NettyChannelImpl) channel).getChannel() == mockedClientChannel && exception == cause) {
                    objectFuture.release();
                }
            }

            @Override
            public void onChannelIdle(final RemotingChannel channel) {
                if (((NettyChannelImpl) channel).getChannel() == mockedClientChannel) {
                    objectFuture.release();
                }
            }
        });

        remotingAbstract.channelEventExecutor.start();

        remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CONNECT, mockedClientChannel));
        remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CLOSE, mockedClientChannel));
        remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.IDLE, mockedClientChannel));
        remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.EXCEPTION, mockedClientChannel, exception));

        objectFuture.getObject();
    }

    @Test
    public void putNettyEvent_EventDropped() throws InterruptedException {
        final Semaphore eventCount = new Semaphore(0);
        final Semaphore droppedEvent = new Semaphore(0);

        remotingAbstract.registerChannelEventListener(new ChannelEventListener() {
            @Override
            public void onChannelConnect(final RemotingChannel channel) {
                eventCount.release();
            }

            @Override
            public void onChannelClose(final RemotingChannel channel) {
                droppedEvent.release();
            }

            @Override
            public void onChannelException(final RemotingChannel channel, final Throwable cause) {

            }

            @Override
            public void onChannelIdle(final RemotingChannel channel) {

            }
        });

        int maxLimit = 10001;

        for (int i = 0; i < maxLimit; i++) {
            remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CONNECT, mockedClientChannel));
        }

        // This event will be dropped
        remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CLOSE, mockedClientChannel));

        remotingAbstract.channelEventExecutor.start();

        assertThat(eventCount.tryAcquire(maxLimit, 3000, TimeUnit.MILLISECONDS)).isTrue();

        assertThat(droppedEvent.tryAcquire(1, 10, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    public void scanResponseTable_RemoveTimeoutRequest() throws InterruptedException {
        final ObjectFuture<Throwable> objectFuture = newObjectFuture(1, 100);

        remotingAbstract.invokeAsyncWithInterceptor(new EmbeddedChannel(),
            remotingAbstract.commandFactory().createRequest(),
            new AsyncHandler() {
                @Override
                public void onFailure(final RemotingCommand request, final Throwable cause) {
                    objectFuture.putObject(cause);
                    objectFuture.release();
                }

                @Override
                public void onSuccess(final RemotingCommand response) {

                }
            }, 100);

        TimeUnit.MILLISECONDS.sleep(15);
        remotingAbstract.scanResponseTable();

        assertThat(objectFuture.getObject()).isInstanceOf(RemotingTimeoutException.class);
    }

    @Test
    public void invokeWithInterceptor_Success() {
        registerNormalProcessor();

        RemotingCommand response = remotingAbstract.invokeWithInterceptor(clientChannel, remotingRequest, 3000);

        assertThat(new String(response.payload())).isEqualTo("Pong");
    }

    @Test
    public void invokeWithInterceptor_Timeout() {
        registerTimeoutProcessor(200);

        try {
            RemotingCommand response = remotingAbstract.invokeWithInterceptor(clientChannel, remotingRequest, 100);
            failBecauseExceptionWasNotThrown(RemotingTimeoutException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemotingTimeoutException.class);
        }
    }

    @Test
    public void invokeWithInterceptor_AccessException() {
        ChannelPromise channelPromise = new DefaultChannelPromise(mockedClientChannel, new DefaultEventLoop());

        when(mockedClientChannel.writeAndFlush(any(Object.class))).thenReturn(channelPromise);
        channelPromise.setFailure(new UnitTestException());

        try {
            RemotingCommand response = remotingAbstract.invokeWithInterceptor(mockedClientChannel, remotingRequest, 100);
            failBecauseExceptionWasNotThrown(RemotingAccessException.class);
        } catch (Exception e) {
            assertThat(e.getCause()).isInstanceOf(UnitTestException.class);
            assertThat(e).isInstanceOf(RemotingAccessException.class);
        }
    }

    @Test
    public void invokeAsyncWithInterceptor_Success() {
        registerNormalProcessor();

        final ObjectFuture<RemotingCommand> objectFuture = newObjectFuture(1, 100);

        remotingAbstract.invokeAsyncWithInterceptor(clientChannel, remotingRequest, new AsyncHandler() {
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
    public void invokeAsyncWithInterceptor_SemaphoreExhausted() {
        registerTimeoutProcessor(1000);

        final ObjectFuture<Throwable> objectFuture = newObjectFuture(1, 100);

        for (int i = 0; i < semaphoreNum; i++) {
            remotingAbstract.invokeAsyncWithInterceptor(clientChannel, remotingRequest, null, 100);
        }

        remotingAbstract.invokeAsyncWithInterceptor(clientChannel, remotingRequest, new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {
                objectFuture.putObject(cause);
                objectFuture.release();
            }

            @Override
            public void onSuccess(final RemotingCommand response) {

            }
        }, 100);

        assertThat(objectFuture.getObject()).isInstanceOf(SemaphoreExhaustedException.class);
    }

    @Test
    public void invokeAsyncWithInterceptor_AccessException() {
        ChannelPromise channelPromise = new DefaultChannelPromise(mockedClientChannel, new DefaultEventLoop());

        when(mockedClientChannel.writeAndFlush(any(Object.class))).thenReturn(channelPromise);
        channelPromise.setFailure(new UnitTestException());

        final ObjectFuture<Throwable> objectFuture = newObjectFuture(1, 100);

        remotingAbstract.invokeAsyncWithInterceptor(mockedClientChannel, remotingRequest, new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {
                objectFuture.putObject(cause);
                objectFuture.release();
            }

            @Override
            public void onSuccess(final RemotingCommand response) {

            }
        }, 100);

        assertThat(objectFuture.getObject().getCause()).isInstanceOf(UnitTestException.class);
    }

    @Test
    public void invokeOnewayWithInterceptor_Success() {
        ObjectFuture<RemotingCommand> objectFuture = newObjectFuture(1, 100);
        registerOnewayProcessor(objectFuture);

        remotingAbstract.invokeOnewayWithInterceptor(clientChannel, remotingRequest);

        // Receive the response directly
        assertThat(new String(objectFuture.getObject().payload())).isEqualTo("Pong");
    }

    @Test
    public void invokeOnewayWithInterceptor_AccessException() {
        ChannelPromise channelPromise = new DefaultChannelPromise(mockedClientChannel, new DefaultEventLoop());

        when(mockedClientChannel.writeAndFlush(any(Object.class))).thenReturn(channelPromise);
        channelPromise.setFailure(new UnitTestException());

        String expectedLog = "Send request command to channel null failed !";

        ObjectFuture<String> objectFuture = retrieveStringFromLog(expectedLog);

        remotingAbstract.invokeOnewayWithInterceptor(mockedClientChannel, remotingRequest);

        assertThat(objectFuture.getObject()).contains(expectedLog);
    }

    @Test
    public void invokeOnewayWithInterceptor_SemaphoreExhausted() {
        final ChannelPromise channelPromise = new DefaultChannelPromise(mockedClientChannel, new DefaultEventLoop());

        when(mockedClientChannel.writeAndFlush(any(Object.class))).thenReturn(channelPromise);

        for (int i = 0; i < semaphoreNum; i++) {
            remotingAbstract.invokeOnewayWithInterceptor(mockedClientChannel, remotingRequest);
        }

        runInThreads(new Runnable() {
            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ignore) {
                }
                channelPromise.setSuccess();
            }
        }, 1);
        String expectedLog = "No available oneway semaphore to issue the request";
        ObjectFuture<String> objectFuture = retrieveStringFromLog(expectedLog);

        remotingAbstract.invokeOnewayWithInterceptor(mockedClientChannel, remotingRequest);
        assertThat(objectFuture.getObject()).contains(expectedLog);
    }

    @Test
    public void unregisterRequestProcessor() {
        registerNormalProcessor();

        RemotingCommand response = remotingAbstract.invokeWithInterceptor(clientChannel, remotingRequest, 3000);
        assertThat(new String(response.payload())).isEqualTo("Pong");

        remotingAbstract.unregisterRequestProcessor(requestCode);

        response = remotingAbstract.invokeWithInterceptor(clientChannel, remotingRequest, 3000);
        assertThat(response.opCode()).isEqualTo(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void processor() {
        registerNormalProcessor();
        RemotingCommand response = remotingAbstract.processor(requestCode).getLeft().processRequest(mock(RemotingChannel.class), remotingRequest);
        assertThat(new String(response.payload())).isEqualTo("Pong");

        assertThat(remotingAbstract.processor((short) (requestCode + 1))).isNull();
    }

    @Test
    public void registerRequestProcessor_SpecificExecutor() {
        ExecutorService executor = ThreadUtils.newSingleThreadExecutor("CustomThread", true);

        remotingAbstract.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingAbstract.commandFactory().createResponse(request);
                response.payload(Thread.currentThread().getName().getBytes());
                return response;
            }
        }, executor);

        RemotingCommand response = remotingAbstract.invokeWithInterceptor(clientChannel, remotingRequest, 3000);
        assertThat(new String(response.payload())).startsWith("CustomThread");
    }

    @Test
    public void registerChannelEventListener_ExceptionThrown() {
        registerNormalProcessor();

        String expectedLog = "Exception thrown when dispatching channel event";

        ObjectFuture<String> objectFuture = retrieveStringFromLog(expectedLog);

        remotingAbstract.registerChannelEventListener(new ChannelEventListener() {
            @Override
            public void onChannelConnect(final RemotingChannel channel) {
                throw new UnitTestException();
            }

            @Override
            public void onChannelClose(final RemotingChannel channel) {

            }

            @Override
            public void onChannelException(final RemotingChannel channel, final Throwable cause) {

            }

            @Override
            public void onChannelIdle(final RemotingChannel channel) {

            }
        });

        remotingAbstract.channelEventExecutor.start();
        remotingAbstract.putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CONNECT, clientChannel));

        assertThat(objectFuture.getObject()).contains(expectedLog);
    }

    @Test
    public void registerInterceptor_NoModification() {
        registerNormalProcessor();
        final ObjectFuture<RemotingCommand> localRequest = newObjectFuture(1, 100);
        final ObjectFuture<RemotingCommand> localResponse = newObjectFuture(1, 100);

        final ObjectFuture<RemotingCommand> remoteRequest = newObjectFuture(1, 100);
        final ObjectFuture<RemotingCommand> remoteResponse = newObjectFuture(1, 100);

        remotingAbstract.registerInterceptor(new Interceptor() {
            @Override
            public void beforeRequest(final RequestContext context) {
                if (context.getRemotingEndPoint() == RemotingEndPoint.REQUEST) {
                    localRequest.putObject(context.getRequest());
                    localRequest.release();
                }

                if (context.getRemotingEndPoint() == RemotingEndPoint.RESPONSE) {
                    remoteRequest.putObject(context.getRequest());
                    remoteRequest.release();
                }
            }

            @Override
            public void afterResponseReceived(final ResponseContext context) {
                if (context.getRemotingEndPoint() == RemotingEndPoint.REQUEST) {
                    localResponse.putObject(context.getResponse());
                    localResponse.release();
                }

                if (context.getRemotingEndPoint() == RemotingEndPoint.RESPONSE) {
                    remoteResponse.putObject(context.getResponse());
                    remoteResponse.release();
                }
            }
        });

        RemotingCommand response = remotingAbstract.invokeWithInterceptor(clientChannel, remotingRequest, 3000);

        assertThat(new String(response.payload())).startsWith("Pong");

        assertThat(localRequest.getObject()).isEqualTo(remotingRequest);
        assertThat(localResponse.getObject()).isEqualTo(response);

        assertThat(remoteRequest.getObject()).isEqualTo(remotingRequest);
        assertThat(remoteResponse.getObject()).isEqualTo(response);

        remotingAbstract.invokeAsyncWithInterceptor(clientChannel, remotingRequestAsync, null,3000);

        assertThat(localRequest.getObject()).isEqualTo(remotingRequestAsync);


        assertThat(localResponse.getObject().requestID()).isEqualTo(remotingRequestAsync.requestID());

        assertThat(remoteRequest.getObject()).isEqualTo(remotingRequestAsync);
        assertThat(remoteResponse.getObject().requestID()).isEqualTo(remotingRequestAsync.requestID());

        remotingAbstract.invokeOnewayWithInterceptor(clientChannel, remotingRequestOneway);

        assertThat(localRequest.getObject()).isEqualTo(remotingRequestOneway);

        assertThat(remoteRequest.getObject()).isEqualTo(remotingRequestOneway);
        assertThat(remoteResponse.getObject().requestID()).isEqualTo(remotingRequestOneway.requestID());

    }

    private void registerTimeoutProcessor(final int timeoutMillis) {
        remotingAbstract.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingAbstract.commandFactory().createResponse(request);
                response.payload("Pong".getBytes());
                try {
                    TimeUnit.MILLISECONDS.sleep(timeoutMillis);
                } catch (InterruptedException ignore) {
                }
                return response;
            }
        });
    }

    private void registerNormalProcessor() {
        remotingAbstract.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingAbstract.commandFactory().createResponse(request);
                response.payload("Pong".getBytes());
                return response;
            }
        });
    }

    private void registerOnewayProcessor(final ObjectFuture<RemotingCommand> objectFuture) {
        remotingAbstract.registerRequestProcessor(requestCode, new RequestProcessor() {
            @Override
            public RemotingCommand processRequest(final RemotingChannel channel, final RemotingCommand request) {
                RemotingCommand response = remotingAbstract.commandFactory().createResponse(request);
                response.payload("Pong".getBytes());
                objectFuture.putObject(response);
                objectFuture.release();
                return response;
            }
        });
    }
}