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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingEndPoint;
import org.apache.rocketmq.remoting.api.RemotingService;
import org.apache.rocketmq.remoting.api.RequestProcessor;
import org.apache.rocketmq.remoting.api.channel.ChannelEventListener;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.RemotingCommandFactory;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.exception.RemotingAccessException;
import org.apache.rocketmq.remoting.api.exception.RemotingRuntimeException;
import org.apache.rocketmq.remoting.api.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.api.exception.SemaphoreExhaustedException;
import org.apache.rocketmq.remoting.api.interceptor.Interceptor;
import org.apache.rocketmq.remoting.api.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.api.interceptor.RequestContext;
import org.apache.rocketmq.remoting.api.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.common.ChannelEventListenerGroup;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.ResponseFuture;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.channel.NettyChannelImpl;
import org.apache.rocketmq.remoting.impl.command.RemotingCommandFactoryImpl;
import org.apache.rocketmq.remoting.impl.command.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.internal.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NettyRemotingAbstract implements RemotingService {
    /**
     * Remoting logger instance.
     */
    protected static final Logger LOG = LoggerFactory.getLogger(NettyRemotingAbstract.class);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final ChannelEventExecutor channelEventExecutor = new ChannelEventExecutor("ChannelEventExecutor");

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    private final Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    private final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     */
    private final Map<Integer, ResponseFuture> ackTables = new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    private final Map<Short, Pair<RequestProcessor, ExecutorService>> processorTables = new ConcurrentHashMap<>();

    /**
     * This factory provides methods to create RemotingCommand.
     */
    private final RemotingCommandFactory remotingCommandFactory;

    /**
     * Executor to execute RequestProcessor without specific executor.
     */
    private final ExecutorService publicExecutor;

    /**
     * Invoke the async handler in this executor when process response.
     */
    private final ExecutorService asyncHandlerExecutor;

    /**
     * This scheduled executor provides the ability to govern on-going response table.
     */
    protected ScheduledExecutorService houseKeepingService = ThreadUtils.newSingleThreadScheduledExecutor("HouseKeepingService", true);

    /**
     * Provides custom interceptor at the occurrence of beforeRequest and afterResponseReceived event.
     */
    private InterceptorGroup interceptorGroup = new InterceptorGroup();

    /**
     * Provides listener mechanism to handle netty channel events.
     */
    private ChannelEventListenerGroup channelEventListenerGroup = new ChannelEventListenerGroup();

    NettyRemotingAbstract(RemotingConfig remotingConfig) {
        this.semaphoreOneway = new Semaphore(remotingConfig.getOnewayInvokeSemaphore(), true);
        this.semaphoreAsync = new Semaphore(remotingConfig.getAsyncInvokeSemaphore(), true);
        this.publicExecutor = ThreadUtils.newFixedThreadPool(
            remotingConfig.getPublicExecutorThreads(),
            10000, "Remoting-PublicExecutor", true);

        this.asyncHandlerExecutor = ThreadUtils.newFixedThreadPool(
            remotingConfig.getAsyncHandlerExecutorThreads(),
            10000, "Remoting-AsyncExecutor", true);
        this.remotingCommandFactory = new RemotingCommandFactoryImpl();
    }

    protected void putNettyEvent(final NettyChannelEvent event) {
        if (channelEventListenerGroup != null && channelEventListenerGroup.size() != 0) {
            this.channelEventExecutor.putNettyEvent(event);
        }
    }

    protected void startUpHouseKeepingService() {
        this.houseKeepingService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanResponseTable();
            }
        }, 3000, 1000, TimeUnit.MICROSECONDS);
    }

    void scanResponseTable() {
        final List<Integer> rList = new ArrayList<>();

        for (final Map.Entry<Integer, ResponseFuture> next : this.ackTables.entrySet()) {
            ResponseFuture responseFuture = next.getValue();

            if ((responseFuture.getBeginTimestamp() + responseFuture.getTimeoutMillis()) <= System.currentTimeMillis()) {
                rList.add(responseFuture.getRequestId());
            }
        }

        for (Integer requestID : rList) {
            ResponseFuture rf = this.ackTables.remove(requestID);

            if (rf != null) {
                LOG.warn("Removes timeout request {} ", rf.getRequestCommand());
                rf.setCause(new RemotingTimeoutException(String.format("Request to %s timeout", rf.getRemoteAddr()), rf.getTimeoutMillis()));
                executeAsyncHandler(rf);
            }
        }
    }

    @Override
    public void start() {
        startUpHouseKeepingService();

        if (this.channelEventListenerGroup.size() > 0) {
            this.channelEventExecutor.start();
        }
    }

    @Override
    public void stop() {
        ThreadUtils.shutdownGracefully(houseKeepingService, 3000, TimeUnit.MILLISECONDS);
        ThreadUtils.shutdownGracefully(publicExecutor, 2000, TimeUnit.MILLISECONDS);
        ThreadUtils.shutdownGracefully(asyncHandlerExecutor, 2000, TimeUnit.MILLISECONDS);
        ThreadUtils.shutdownGracefully(channelEventExecutor);
    }

    protected void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand command) throws Exception {
        if (command != null) {
            switch (command.trafficType()) {
                case REQUEST_ONEWAY:
                case REQUEST_ASYNC:
                case REQUEST_SYNC:
                    processRequestCommand(ctx, command);
                    break;
                case RESPONSE:
                    processResponseCommand(ctx, command);
                    break;
                default:
                    LOG.warn("The traffic type {} is NOT supported!", command.trafficType());
                    break;
            }
        }
    }

    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        Pair<RequestProcessor, ExecutorService> processorExecutorPair = this.processorTables.get(cmd.cmdCode());

        if (processorExecutorPair == null) {
            final RemotingCommand response = commandFactory().createResponse(cmd);
            response.opCode(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED);
            ctx.writeAndFlush(response);
            LOG.warn("The command code {} is NOT supported!", cmd.cmdCode());
            return;
        }

        RemotingChannel channel = new NettyChannelImpl(ctx.channel());

        Runnable run = buildProcessorTask(ctx, cmd, processorExecutorPair, channel);

        try {
            processorExecutorPair.getRight().submit(run);
        } catch (RejectedExecutionException e) {
            LOG.warn(String.format("Request %s from %s is rejected by server executor %s !", cmd,
                RemotingUtil.extractRemoteAddress(ctx.channel()), processorExecutorPair.getRight().toString()));

            if (cmd.trafficType() != TrafficType.REQUEST_ONEWAY) {
                RemotingCommand response = remotingCommandFactory.createResponse(cmd);
                response.opCode(RemotingSysResponseCode.SYSTEM_BUSY);
                response.remark("SYSTEM_BUSY");
                writeAndFlush(ctx.channel(), response);
            }
        }
    }

    private void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand response) {
        final ResponseFuture responseFuture = ackTables.remove(response.requestID());
        if (responseFuture != null) {
            responseFuture.setResponseCommand(response);
            responseFuture.release();

            this.interceptorGroup.afterResponseReceived(new ResponseContext(RemotingEndPoint.REQUEST,
                RemotingUtil.extractRemoteAddress(ctx.channel()), responseFuture.getRequestCommand(), response));

            if (responseFuture.getAsyncHandler() != null) {
                executeAsyncHandler(responseFuture);
            } else {
                responseFuture.putResponse(response);
                responseFuture.release();
            }
        } else {
            LOG.warn("Response {} from {} doesn't have a matched request!", response, RemotingUtil.extractRemoteAddress(ctx.channel()));
        }
    }

    private Runnable buildProcessorTask(final ChannelHandlerContext ctx, final RemotingCommand cmd,
        final Pair<RequestProcessor, ExecutorService> processorExecutorPair, final RemotingChannel channel) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.RESPONSE,
                        RemotingUtil.extractRemoteAddress(ctx.channel()), cmd));

                    RemotingCommand response = processorExecutorPair.getLeft().processRequest(channel, cmd);

                    interceptorGroup.afterResponseReceived(new ResponseContext(RemotingEndPoint.RESPONSE,
                        RemotingUtil.extractRemoteAddress(ctx.channel()), cmd, response));

                    handleResponse(response, cmd, ctx);
                } catch (Throwable e) {
                    LOG.error(String.format("Process request %s error !", cmd.toString()), e);

                    handleException(e, cmd, ctx);
                }
            }
        };
    }

    private void writeAndFlush(final Channel channel, final Object msg) {
        channel.writeAndFlush(msg);
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeAsyncHandler(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = asyncHandlerExecutor;
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeAsyncHandler();
                        } catch (Throwable e) {
                            LOG.warn("Execute async handler in specific executor exception, ", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Throwable e) {
                runInThisThread = true;
                LOG.warn("Execute async handler in executor exception, maybe the executor is busy now", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeAsyncHandler();
            } catch (Throwable e) {
                LOG.warn("Execute async handler in current thread exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    private void requestFail(final int requestID, final RemotingRuntimeException cause) {
        ResponseFuture responseFuture = ackTables.remove(requestID);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            responseFuture.setCause(cause);
            executeAsyncHandler(responseFuture);
        }
    }

    private void requestFail(final ResponseFuture responseFuture, final RemotingRuntimeException cause) {
        responseFuture.setCause(cause);
        executeAsyncHandler(responseFuture);
    }

    private void handleResponse(RemotingCommand response, RemotingCommand cmd, ChannelHandlerContext ctx) {
        if (cmd.trafficType() != TrafficType.REQUEST_ONEWAY) {
            if (response != null) {
                try {
                    writeAndFlush(ctx.channel(), response);
                } catch (Throwable e) {
                    LOG.error(String.format("Process request %s success, but transfer response %s failed !",
                        cmd.toString(), response.toString()), e);
                }
            }
        }

    }

    private void handleException(Throwable e, RemotingCommand cmd, ChannelHandlerContext ctx) {
        if (cmd.trafficType() != TrafficType.REQUEST_ONEWAY) {
            RemotingCommand response = remotingCommandFactory.createResponse(cmd);
            response.opCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.remark("SYSTEM_ERROR");
            writeAndFlush(ctx.channel(), response);
        }
    }

    public RemotingCommand invokeWithInterceptor(final Channel channel, final RemotingCommand request,
        long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_SYNC);

        final String remoteAddr = RemotingUtil.extractRemoteAddress(channel);

        this.interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.REQUEST, remoteAddr, request));

        return this.invoke0(remoteAddr, channel, request, timeoutMillis);
    }

    private RemotingCommand invoke0(final String remoteAddr, final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        try {
            final int requestID = request.requestID();
            final ResponseFuture responseFuture = new ResponseFuture(requestID, timeoutMillis);
            responseFuture.setRequestCommand(request);
            responseFuture.setRemoteAddr(remoteAddr);

            this.ackTables.put(requestID, responseFuture);

            ChannelFutureListener listener = new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);

                        ackTables.remove(requestID);
                        responseFuture.setCause(new RemotingAccessException(RemotingUtil.extractRemoteAddress(channel), f.cause()));
                        responseFuture.putResponse(null);

                        LOG.warn("Send request command to {} failed !", remoteAddr);
                    }
                }
            };

            this.writeAndFlush(channel, request, listener);

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);

            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    responseFuture.setCause(new RemotingTimeoutException(RemotingUtil.extractRemoteAddress(channel), timeoutMillis));
                    throw responseFuture.getCause();
                } else {
                    throw responseFuture.getCause();
                }
            }

            return responseCommand;
        } finally {
            this.ackTables.remove(request.requestID());
        }
    }

    private void writeAndFlush(final Channel channel, final Object msg, final ChannelFutureListener listener) {
        channel.writeAndFlush(msg).addListener(listener);
    }

    public void invokeAsyncWithInterceptor(final Channel channel, final RemotingCommand request,
        final AsyncHandler asyncHandler, long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_ASYNC);

        final String remoteAddr = RemotingUtil.extractRemoteAddress(channel);

        this.interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.REQUEST, remoteAddr, request));

        this.invokeAsync0(remoteAddr, channel, request, asyncHandler, timeoutMillis);
    }

    private void invokeAsync0(final String remoteAddr, final Channel channel, final RemotingCommand request,
        final AsyncHandler asyncHandler, final long timeoutMillis) {
        boolean acquired = this.semaphoreAsync.tryAcquire();
        if (acquired) {
            final int requestID = request.requestID();

            SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseFuture responseFuture = new ResponseFuture(requestID, timeoutMillis, asyncHandler, once);
            responseFuture.setRequestCommand(request);
            responseFuture.setRemoteAddr(remoteAddr);

            this.ackTables.put(requestID, responseFuture);
            try {
                ChannelFutureListener listener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) {
                        responseFuture.setSendRequestOK(f.isSuccess());
                        if (f.isSuccess()) {
                            return;
                        }

                        requestFail(requestID, new RemotingAccessException(RemotingUtil.extractRemoteAddress(channel), f.cause()));
                        LOG.warn("Send request command to channel  failed.", remoteAddr);
                    }
                };

                this.writeAndFlush(channel, request, listener);
            } catch (Exception e) {
                requestFail(requestID, new RemotingAccessException(RemotingUtil.extractRemoteAddress(channel), e));
                LOG.error("Send request command to channel " + channel + " error !", e);
            }
        } else {
            String info = String.format("No available async semaphore to issue the request request %s", request.toString());
            requestFail(new ResponseFuture(request.requestID(), timeoutMillis, asyncHandler, null), new SemaphoreExhaustedException(info));
            LOG.error(info);
        }
    }

    public void invokeOnewayWithInterceptor(final Channel channel, final RemotingCommand request) {
        request.trafficType(TrafficType.REQUEST_ONEWAY);

        this.interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.REQUEST, RemotingUtil.extractRemoteAddress(channel), request));
        this.invokeOneway0(channel, request);
    }

    private void invokeOneway0(final Channel channel, final RemotingCommand request) {
        boolean acquired = this.semaphoreOneway.tryAcquire();
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                final SocketAddress socketAddress = channel.remoteAddress();

                ChannelFutureListener listener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) {
                        once.release();
                        if (!f.isSuccess()) {
                            LOG.warn("Send request command to channel {} failed !", socketAddress);
                        }
                    }
                };

                this.writeAndFlush(channel, request, listener);
            } catch (Exception e) {
                once.release();
                LOG.error("Send request command to channel " + channel + " error !", e);
            }
        } else {
            String info = String.format("No available oneway semaphore to issue the request %s", request.toString());
            LOG.error(info);
        }
    }

    @Override
    public void registerInterceptor(Interceptor interceptor) {
        this.interceptorGroup.registerInterceptor(interceptor);
    }

    @Override
    public void registerRequestProcessor(short requestCode, RequestProcessor processor, ExecutorService executor) {
        Pair<RequestProcessor, ExecutorService> pair = new Pair<RequestProcessor, ExecutorService>(processor, executor);
        if (!this.processorTables.containsKey(requestCode)) {
            this.processorTables.put(requestCode, pair);
        }
    }

    @Override
    public void registerRequestProcessor(short requestCode, RequestProcessor processor) {
        this.registerRequestProcessor(requestCode, processor, publicExecutor);
    }

    @Override
    public void unregisterRequestProcessor(short requestCode) {
        this.processorTables.remove(requestCode);
    }

    @Override
    public Pair<RequestProcessor, ExecutorService> processor(short requestCode) {
        return processorTables.get(requestCode);
    }

    @Override
    public RemotingCommandFactory commandFactory() {
        return this.remotingCommandFactory;
    }

    @Override
    public void registerChannelEventListener(ChannelEventListener listener) {
        this.channelEventListenerGroup.registerChannelEventListener(listener);
    }

    class ChannelEventExecutor extends Thread {
        private final static int MAX_SIZE = 10000;
        private final LinkedBlockingQueue<NettyChannelEvent> eventQueue = new LinkedBlockingQueue<NettyChannelEvent>();
        private String name;

        public ChannelEventExecutor(String nettyEventExector) {
            super(nettyEventExector);
            this.name = nettyEventExector;
        }

        public void putNettyEvent(final NettyChannelEvent event) {
            if (this.eventQueue.size() <= MAX_SIZE) {
                this.eventQueue.add(event);
            } else {
                LOG.warn("Event queue size[{}] meets the limit, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            LOG.info(this.name + " service started");

            ChannelEventListenerGroup listener = NettyRemotingAbstract.this.channelEventListenerGroup;

            while (true) {
                try {
                    NettyChannelEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        RemotingChannel channel = new NettyChannelImpl(event.getChannel());

                        LOG.info("Dispatch received channel event, {}", event);

                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(channel);
                                break;
                            case CLOSE:
                                listener.onChannelClose(channel);
                                break;
                            case CONNECT:
                                listener.onChannelConnect(channel);
                                break;
                            case EXCEPTION:
                                listener.onChannelException(channel, event.getCause());
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Exception thrown when dispatching channel event", e);
                    break;
                }
            }
        }

    }

    protected class RemotingCommandDispatcher extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

}
