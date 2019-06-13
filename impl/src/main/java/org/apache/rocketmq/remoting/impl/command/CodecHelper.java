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

import java.nio.charset.Charset;
import java.util.Map.Entry;
import org.apache.rocketmq.remoting.api.buffer.RemotingBuffer;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.exception.RemotingCodecException;

public class CodecHelper {
    // ProtocolMagic(1) + TotalLength(4) + CmdCode(2) + CmdVersion(2) + RequestID(4) + TrafficType(1) + OpCode(2)
    // + RemarkLen(2) + PropertiesSize(2) + PayloadLen(4);
    public final static int MIN_PROTOCOL_LEN = 1 + 4 + 2 + 2 + 4 + 1 + 2 + 2 + 2 + 4;
    public final static byte PROTOCOL_MAGIC = 0x14;
    final static int REMARK_MAX_LEN = Short.MAX_VALUE;
    final static int PROPERTY_MAX_LEN = 524288; // 512KB
    final static int PAYLOAD_MAX_LEN = 16777216; // 16MB
    public final static int PACKET_MAX_LEN = MIN_PROTOCOL_LEN + REMARK_MAX_LEN + PROPERTY_MAX_LEN + PAYLOAD_MAX_LEN;
    private final static char PROPERTY_SEPARATOR = '\n';
    private final static Charset REMOTING_CHARSET = Charset.forName("UTF-8");

    public static void encodeCommand(final RemotingCommand command, final RemotingBuffer out) {
        out.writeByte(PROTOCOL_MAGIC);

        short remarkLen = 0;
        byte[] remark = null;
        if (command.remark() != null) {
            remark = command.remark().getBytes(REMOTING_CHARSET);
            if (remark.length > REMARK_MAX_LEN) {
                throw new RemotingCodecException(String.format("Remark len: %d over max limit: %d", remark.length, REMARK_MAX_LEN));
            }
            remarkLen = (short) remark.length;
        }

        byte[][] props = null;
        int propsLen = 0;
        StringBuilder sb = new StringBuilder();
        if (command.properties() != null && !command.properties().isEmpty()) {
            props = new byte[command.properties().size()][];
            int i = 0;
            for (Entry<String, String> next : command.properties().entrySet()) {
                sb.setLength(0);
                sb.append(next.getKey());
                sb.append(PROPERTY_SEPARATOR);
                sb.append(next.getValue());

                props[i] = sb.toString().getBytes(REMOTING_CHARSET);

                if (props[i].length > Short.MAX_VALUE) {
                    throw new RemotingCodecException(String.format("Property KV len: %d over max limit: %d", props[i].length, Short.MAX_VALUE));
                }

                propsLen += 2;
                propsLen += props[i].length;
                i++;
            }
        }

        if (propsLen > PROPERTY_MAX_LEN) {
            throw new RemotingCodecException(String.format("Properties total len: %d over max limit: %d", propsLen, PROPERTY_MAX_LEN));
        }

        int payloadLen = command.payload() == null ? 0 : command.payload().length;

        if (payloadLen > PAYLOAD_MAX_LEN) {
            throw new RemotingCodecException(String.format("Payload len: %d over max limit: %d", payloadLen, PAYLOAD_MAX_LEN));
        }

        int totalLength = MIN_PROTOCOL_LEN
            + remarkLen
            + propsLen
            + payloadLen;

        out.writeInt(totalLength);
        out.writeShort(command.cmdCode());
        out.writeShort(command.cmdVersion());
        out.writeInt(command.requestID());
        out.writeByte((byte) command.trafficType().ordinal());
        out.writeShort(command.opCode());

        out.writeShort(remarkLen);
        if (remarkLen != 0) {
            out.writeBytes(remark);
        }

        if (props != null) {
            out.writeShort((short) props.length);
            for (byte[] prop : props) {
                out.writeShort((short) prop.length);
                out.writeBytes(prop);
            }
        } else {
            out.writeShort((short) 0);
        }

        out.writeInt(payloadLen);
        if (payloadLen != 0) {
            out.writeBytes(command.payload());
        }
    }

    public static RemotingCommand decode(final RemotingBuffer in) {
        RemotingCommandImpl cmd = new RemotingCommandImpl();

        cmd.cmdCode(in.readShort());
        cmd.cmdVersion(in.readShort());
        cmd.requestID(in.readInt());
        cmd.trafficType(TrafficType.parse(in.readByte()));
        cmd.opCode(in.readShort());

        short remarkLen = in.readShort();
        if (remarkLen > 0) {
            byte[] bytes = new byte[remarkLen];
            in.readBytes(bytes);
            String str = new String(bytes, REMOTING_CHARSET);
            cmd.remark(str);
        }

        short propsSize = in.readShort();
        int propsLen = 0;
        if (propsSize > 0) {
            for (int i = 0; i < propsSize; i++) {
                short length = in.readShort();
                if (length > 0) {
                    byte[] bytes = new byte[length];
                    in.readBytes(bytes);
                    String str = new String(bytes, REMOTING_CHARSET);
                    int index = str.indexOf(PROPERTY_SEPARATOR);
                    if (index > 0) {
                        String key = str.substring(0, index);
                        String value = str.substring(index + 1);
                        cmd.property(key, value);
                    }
                }

                propsLen += 2;
                propsLen += length;
                if (propsLen > PROPERTY_MAX_LEN) {
                    throw new RemotingCodecException(String.format("Properties total len: %d over max limit: %d", propsLen, PROPERTY_MAX_LEN));
                }
            }
        }

        int payloadLen = in.readInt();

        if (payloadLen > PAYLOAD_MAX_LEN) {
            throw new RemotingCodecException(String.format("Payload len: %d over max limit: %d", payloadLen, PAYLOAD_MAX_LEN));
        }

        if (payloadLen > 0) {
            byte[] bytes = new byte[payloadLen];
            in.readBytes(bytes);
            cmd.payload(bytes);
        }

        return cmd;
    }
}
