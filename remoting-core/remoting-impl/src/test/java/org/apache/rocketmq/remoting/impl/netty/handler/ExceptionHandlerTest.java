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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.impl.buffer.NettyRemotingBuffer;
import org.apache.rocketmq.remoting.impl.command.CodecHelper;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ExceptionHandlerTest extends BaseTest {

    @Test
    public void exceptionCaught_ExceptionThrown_ChannelClosed() {
        EmbeddedChannel channel = new EmbeddedChannel();
        ByteBuf buffer = ByteBufAllocator.DEFAULT.heapBuffer();
        channel.pipeline().addLast(new ByteToMessageDecoder() {

            @Override
            protected void decode(final ChannelHandlerContext ctx, final ByteBuf in,
                final List<Object> out) throws Exception {
                throw new NotImplementedException("Emtpy encode method");
            }
        }, new ExceptionHandler());

        CodecHelper.encodeCommand(randomRemotingCommand(), new NettyRemotingBuffer(buffer));
        channel.writeInbound(buffer);

        try {
            channel.flushInbound();
            failBecauseExceptionWasNotThrown(ClosedChannelException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(ClosedChannelException.class);
        }
    }
}