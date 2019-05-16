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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;

public class RemotingCommandImpl implements RemotingCommand {
    public final static RequestIdGenerator REQUEST_ID_GENERATOR = RequestIdGenerator.inst;

    private short cmdCode;
    private short cmdVersion;
    private volatile int requestId = REQUEST_ID_GENERATOR.incrementAndGet();
    private TrafficType trafficType = TrafficType.REQUEST_SYNC;
    private short opCode = RemotingSysResponseCode.SUCCESS;
    private String remark = "";
    private Map<String, String> properties = new HashMap<>();
    private byte[] payload;

    protected RemotingCommandImpl() {
    }

    @Override
    public short cmdCode() {
        return this.cmdCode;
    }

    @Override
    public void cmdCode(short code) {
        this.cmdCode = code;
    }

    @Override
    public short cmdVersion() {
        return this.cmdVersion;
    }

    @Override
    public void cmdVersion(short version) {
        this.cmdVersion = version;
    }

    @Override
    public int requestID() {
        return requestId;
    }

    @Override
    public void requestID(int value) {
        this.requestId = value;
    }

    @Override
    public TrafficType trafficType() {
        return this.trafficType;
    }

    @Override
    public void trafficType(TrafficType value) {
        this.trafficType = value;
    }

    @Override
    public short opCode() {
        return this.opCode;
    }

    @Override
    public void opCode(short value) {
        this.opCode = value;
    }

    @Override
    public String remark() {
        return this.remark;
    }

    @Override
    public void remark(String value) {
        this.remark = value;
    }

    @Override
    public Map<String, String> properties() {
        return this.properties;
    }

    @Override
    public void properties(Map<String, String> value) {
        this.properties = value;
    }

    @Override
    public String property(String key) {
        return this.properties.get(key);
    }

    @Override
    public void property(String key, String value) {
        this.properties.put(key, value);
    }

    @Override
    public byte[] payload() {
        return this.payload;
    }

    @Override
    public void payload(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
