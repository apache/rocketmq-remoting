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

package org.apache.rocketmq.remoting.impl.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.rocketmq.remoting.api.buffer.RemotingBuffer;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.exception.RemotingCodecException;
import org.apache.rocketmq.remoting.impl.buffer.NettyRemotingBuffer;
import org.apache.rocketmq.remoting.impl.command.CodecHelper;
import org.apache.rocketmq.remoting.internal.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Encoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger LOG = LoggerFactory.getLogger(Encoder.class);

    public Encoder() {
    }

    @Override
    public void encode(final ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
        try {
            RemotingBuffer wrapper = new NettyRemotingBuffer(out);

            encode(remotingCommand, wrapper);
        } catch (final RemotingCodecException e) {
            String remoteAddress = RemotingUtil.extractRemoteAddress(ctx.channel());
            LOG.error(String.format("Error occurred when encoding command for channel %s", remoteAddress), e);

            ctx.channel().close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOG.warn("Close channel {} because of error {},result is {}", ctx.channel(), e, future.isSuccess());
                }
            });
        }
    }

    private void encode(final RemotingCommand remotingCommand, final RemotingBuffer out) {
        CodecHelper.encodeCommand(remotingCommand, out);
    }
}
