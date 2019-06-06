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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.exception.RemoteConnectFailureException;
import org.apache.rocketmq.remoting.api.exception.RemoteTimeoutException;
import org.apache.rocketmq.remoting.config.RemotingClientConfig;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.netty.handler.Decoder;
import org.apache.rocketmq.remoting.impl.netty.handler.Encoder;
import org.apache.rocketmq.remoting.internal.JvmUtils;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private final Bootstrap clientBootstrap = new Bootstrap();
    private final EventLoopGroup ioGroup;
    private final Class<? extends SocketChannel> socketChannelClass;

    private final RemotingClientConfig clientConfig;

    private EventExecutorGroup workerGroup;
    private ClientChannelManager clientChannelManager;

    public NettyRemotingClient(final RemotingClientConfig clientConfig) {
        super(clientConfig);
        this.clientConfig = clientConfig;

        if (JvmUtils.isLinux() && this.clientConfig.isClientNativeEpollEnable()) {
            this.ioGroup = new EpollEventLoopGroup(clientConfig.getClientIoThreads(), ThreadUtils.newGenericThreadFactory("NettyClientEpollIoThreads",
                clientConfig.getClientWorkerThreads()));
            socketChannelClass = EpollSocketChannel.class;
        } else {
            this.ioGroup = new NioEventLoopGroup(clientConfig.getClientIoThreads(), ThreadUtils.newGenericThreadFactory("NettyClientNioIoThreads",
                clientConfig.getClientWorkerThreads()));
            socketChannelClass = NioSocketChannel.class;
        }

        this.clientChannelManager = new ClientChannelManager(clientBootstrap, clientConfig);

        this.workerGroup = new DefaultEventExecutorGroup(clientConfig.getClientWorkerThreads(),
            ThreadUtils.newGenericThreadFactory("NettyClientWorkerThreads", clientConfig.getClientWorkerThreads()));
    }

    @Override
    public void start() {
        super.start();

        this.clientBootstrap.group(this.ioGroup).channel(socketChannelClass)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(workerGroup,
                        new Decoder(),
                        new Encoder(),
                        new IdleStateHandler(clientConfig.getConnectionChannelReaderIdleSeconds(),
                            clientConfig.getConnectionChannelWriterIdleSeconds(), clientConfig.getConnectionChannelIdleSeconds()),
                        new ClientConnectionHandler(),
                        new RemotingCommandDispatcher());
                }
            });

        applyOptions(clientBootstrap);
    }

    @Override
    public void stop() {
        try {
            clientChannelManager.clear();

            this.ioGroup.shutdownGracefully(clientConfig.getRemotingShutdownQuietPeriodMillis(),
                clientConfig.getRemotingShutdownTimeoutMillis(), TimeUnit.MILLISECONDS).sync();

            this.workerGroup.shutdownGracefully(clientConfig.getRemotingShutdownQuietPeriodMillis(),
                clientConfig.getRemotingShutdownTimeoutMillis(), TimeUnit.MILLISECONDS).sync();
        } catch (Exception e) {
            LOG.warn("RemotingClient stopped error !", e);
        }

        super.stop();
    }

    private void applyOptions(Bootstrap bootstrap) {
        if (null != clientConfig) {
            if (clientConfig.getTcpSoSndBufSize() > 0) {
                bootstrap.option(ChannelOption.SO_SNDBUF, clientConfig.getTcpSoSndBufSize());
            }
            if (clientConfig.getTcpSoRcvBufSize() > 0) {
                bootstrap.option(ChannelOption.SO_RCVBUF, clientConfig.getTcpSoRcvBufSize());
            }

            bootstrap.option(ChannelOption.SO_KEEPALIVE, clientConfig.isTcpSoKeepAlive()).
                option(ChannelOption.TCP_NODELAY, clientConfig.isTcpSoNoDelay()).
                option(ChannelOption.CONNECT_TIMEOUT_MILLIS, clientConfig.getTcpSoTimeoutMillis()).
                option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(clientConfig.getWriteBufLowWaterMark(),
                    clientConfig.getWriteBufHighWaterMark()));
        }
    }

    @Override
    public RemotingCommand invoke(final String address, final RemotingCommand request, final long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_SYNC);

        Channel channel = this.clientChannelManager.createIfAbsent(address);
        if (channel != null && channel.isActive()) {
            try {
                return this.invokeWithInterceptor(channel, request, timeoutMillis);

            } catch (RemoteTimeoutException e) {
                if (this.clientConfig.isClientCloseSocketIfTimeout()) {
                    LOG.warn("invoke: timeout, so close the socket {} ms, {}", timeoutMillis, address);
                    this.clientChannelManager.closeChannel(address, channel);
                }

                LOG.warn("invoke: wait response timeout<{}ms> exception, so close the channel[{}]", timeoutMillis, address);
                throw e;
            } finally {
                if (this.clientConfig.isClientShortConnectionEnable()) {
                    this.clientChannelManager.closeChannel(address, channel);
                }
            }
        } else {
            this.clientChannelManager.closeChannel(address, channel);
            throw new RemoteConnectFailureException(address);
        }

    }

    @Override
    public void invokeAsync(final String address, final RemotingCommand request, final AsyncHandler asyncHandler,
        final long timeoutMillis) {

        final Channel channel = this.clientChannelManager.createIfAbsent(address);
        if (channel != null && channel.isActive()) {
            this.invokeAsyncWithInterceptor(channel, request, asyncHandler, timeoutMillis);
        } else {
            this.clientChannelManager.closeChannel(address, channel);
        }
    }

    @Override
    public void invokeOneWay(final String address, final RemotingCommand request) {
        final Channel channel = this.clientChannelManager.createIfAbsent(address);
        if (channel != null && channel.isActive()) {
            this.invokeOnewayWithInterceptor(channel, request);
        } else {
            this.clientChannelManager.closeChannel(address, channel);
        }
    }

    public void setClientChannelManager(final ClientChannelManager clientChannelManager) {
        this.clientChannelManager = clientChannelManager;
    }

    private class ClientConnectionHandler extends ChannelDuplexHandler {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise)
            throws Exception {
            LOG.info("Connected from {} to {}.", localAddress, remoteAddress);
            super.connect(ctx, remoteAddress, localAddress, promise);

            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CONNECT, ctx.channel()));
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            LOG.info("Remote address {} disconnect channel {}.", ctx.channel().remoteAddress(), ctx.channel());

            NettyRemotingClient.this.clientChannelManager.closeChannel(ctx.channel());

            super.disconnect(ctx, promise);

            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CLOSE, ctx.channel()));
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            LOG.info("Remote address {} close channel {}.", ctx.channel().remoteAddress(), ctx.channel());

            NettyRemotingClient.this.clientChannelManager.closeChannel(ctx.channel());

            super.close(ctx, promise);

            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.CLOSE, ctx.channel()));
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    LOG.info("Close channel {} because of idle event {} ", ctx.channel(), event);
                    NettyRemotingClient.this.clientChannelManager.closeChannel(ctx.channel());
                    putNettyEvent(new NettyChannelEvent(NettyChannelEventType.IDLE, ctx.channel()));
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.info("Close channel {} because of error {} ", ctx.channel(), cause);
            NettyRemotingClient.this.clientChannelManager.closeChannel(ctx.channel());
            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.EXCEPTION, ctx.channel(), cause));
        }
    }
}
