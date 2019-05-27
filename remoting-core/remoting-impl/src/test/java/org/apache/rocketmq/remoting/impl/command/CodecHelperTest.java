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

package org.apache.rocketmq.remoting.impl.command;

import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.buffer.ByteBufferWrapper;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.exception.RemoteCodecException;
import org.apache.rocketmq.remoting.impl.buffer.NettyByteBufferWrapper;
import org.junit.Test;

import static org.apache.rocketmq.remoting.impl.command.CodecHelper.PAYLOAD_MAX_LEN;
import static org.apache.rocketmq.remoting.impl.command.CodecHelper.PROPERTY_MAX_LEN;
import static org.apache.rocketmq.remoting.impl.command.CodecHelper.PROTOCOL_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertEquals;

public class CodecHelperTest extends BaseTest {

    @Test
    public void encodeAndDecodeCommand() {
        ByteBufferWrapper buffer = new NettyByteBufferWrapper(ByteBufAllocator.DEFAULT.heapBuffer());
        RemotingCommand command = randomRemotingCommand();
        CodecHelper.encodeCommand(command, buffer);

        // Skip magic code and total length
        assertEquals(PROTOCOL_MAGIC, buffer.readByte());
        buffer.readInt();

        RemotingCommand decodedCommand = CodecHelper.decode(buffer);

        assertEquals(command, decodedCommand);
    }

    @Test
    public void encodeCommand_WithException() {
        ByteBufferWrapper buffer = new NettyByteBufferWrapper(ByteBufAllocator.DEFAULT.heapBuffer());
        RemotingCommand command = randomRemotingCommand();

        // Remark len exceed max limit
        command.remark(RandomStringUtils.randomAlphabetic(CodecHelper.REMARK_MAX_LEN + 1));
        try {
            CodecHelper.encodeCommand(command, buffer);
            failBecauseExceptionWasNotThrown(RemoteCodecException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemoteCodecException.class);
        }

        command = randomRemotingCommand();
        command.property("a", RandomStringUtils.randomAlphabetic(Short.MAX_VALUE));

        try {
            CodecHelper.encodeCommand(command, buffer);
            failBecauseExceptionWasNotThrown(RemoteCodecException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemoteCodecException.class);
        }

        command = randomRemotingCommand();
        command.property("a", RandomStringUtils.randomAlphabetic(CodecHelper.PROPERTY_MAX_LEN));

        try {
            CodecHelper.encodeCommand(command, buffer);
            failBecauseExceptionWasNotThrown(RemoteCodecException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemoteCodecException.class);
        }

        command = randomRemotingCommand();
        command.payload(RandomStringUtils.randomAlphabetic(CodecHelper.PAYLOAD_MAX_LEN + 1).getBytes());

        try {
            CodecHelper.encodeCommand(command, buffer);
            failBecauseExceptionWasNotThrown(RemoteCodecException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemoteCodecException.class);
        }
    }

    @Test
    public void decodeCommand_WithException() {
        ByteBufferWrapper buffer = new NettyByteBufferWrapper(ByteBufAllocator.DEFAULT.heapBuffer());

        buffer.writeShort((short) 0);
        buffer.writeShort((short) 0);
        buffer.writeInt(0);
        buffer.writeByte((byte) 0);
        buffer.writeShort((short) 0);

        buffer.writeShort((short) 0);
        int writerIndex = buffer.writerIndex();

        int propsSize = 1 + PROPERTY_MAX_LEN / Short.MAX_VALUE;
        buffer.writeShort((short) propsSize);

        for (int i = 0; i < propsSize; i++) {
            buffer.writeShort(Short.MAX_VALUE);
            buffer.writeBytes(new byte[Short.MAX_VALUE]);
        }

        try {
            CodecHelper.decode(buffer);
            failBecauseExceptionWasNotThrown(RemoteCodecException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemoteCodecException.class);
        }

        buffer.setReaderIndex(0);
        buffer.setWriterIndex(writerIndex);
        buffer.writeShort((short) 0);

        buffer.writeInt(PAYLOAD_MAX_LEN + 1);

        try {
            CodecHelper.decode(buffer);
            failBecauseExceptionWasNotThrown(RemoteCodecException.class);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RemoteCodecException.class);
        }
    }
}