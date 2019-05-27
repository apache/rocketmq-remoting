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
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.channels.ClosedChannelException;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.impl.command.CodecHelper;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertEquals;

public class DecoderTest extends BaseTest {

    @Test
    public void decode_Success() {
        EmbeddedChannel channel = new EmbeddedChannel(new Encoder(), new Decoder());

        RemotingCommand request = randomRemotingCommand();
        channel.writeOutbound(request);
        channel.flushOutbound();

        ByteBuf buffer = channel.readOutbound();
        channel.writeInbound(buffer);
        channel.flushInbound();

        RemotingCommand decodedRequest = channel.readInbound();
        assertEquals(request, decodedRequest);
    }

    @Test
    public void decode_WrongMagicCode_ChannelClosed() {
        // Magic Code doesn't match
        EmbeddedChannel channel = new EmbeddedChannel(new Decoder());

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        buf.writeByte(0x15);
        buf.retain();

        flushChannelWithException(channel, buf);
    }

    @Test
    public void decode_LenOverLimit_ChannelClosed() {
        // Magic Code doesn't match
        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        EmbeddedChannel channel = new EmbeddedChannel(new Decoder());

        buf.resetReaderIndex();
        buf.resetWriterIndex();

        buf.writeByte(CodecHelper.PROTOCOL_MAGIC);
        buf.writeInt(CodecHelper.PACKET_MAX_LEN + 1);
        buf.writeBytes(new byte[CodecHelper.MIN_PROTOCOL_LEN - 1]);

        flushChannelWithException(channel, buf);
    }

    private void flushChannelWithException(final EmbeddedChannel channel, final ByteBuf buf) {
        try {
            channel.writeInbound(buf);
            channel.flushInbound();
            failBecauseExceptionWasNotThrown(ClosedChannelException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(ClosedChannelException.class);
        }
    }

}