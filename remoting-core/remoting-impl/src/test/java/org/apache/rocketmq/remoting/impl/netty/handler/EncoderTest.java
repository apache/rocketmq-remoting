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
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.channels.ClosedChannelException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.impl.buffer.NettyByteBufferWrapper;
import org.apache.rocketmq.remoting.impl.command.CodecHelper;
import org.junit.Test;

import static org.apache.rocketmq.remoting.impl.command.CodecHelper.PROTOCOL_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertEquals;

public class EncoderTest extends BaseTest {

    @Test
    public void encode_Success() {
        EmbeddedChannel channel = new EmbeddedChannel(new Encoder());

        RemotingCommand request = randomRemotingCommand();
        channel.writeOutbound(request);

        ByteBuf buffer = channel.readOutbound();

        // Skip magic code and total length
        assertEquals(PROTOCOL_MAGIC, buffer.readByte());
        buffer.readInt();

        RemotingCommand decodedRequest = CodecHelper.decode(new NettyByteBufferWrapper(buffer));

        assertEquals(request, decodedRequest);
    }

    @Test
    public void encode_LenOverLimit_ChannelClosed() {
        EmbeddedChannel channel = new EmbeddedChannel(new Encoder());

        RemotingCommand request = randomRemotingCommand();
        request.remark(RandomStringUtils.randomAlphabetic(Short.MAX_VALUE + 1));

        try {
            channel.writeOutbound(request);
            failBecauseExceptionWasNotThrown(ClosedChannelException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(ClosedChannelException.class);
        }
    }
}