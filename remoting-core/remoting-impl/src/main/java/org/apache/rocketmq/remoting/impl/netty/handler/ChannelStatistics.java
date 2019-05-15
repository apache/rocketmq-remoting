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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.common.metrics.ChannelMetrics;

public class ChannelStatistics extends ChannelDuplexHandler implements ChannelMetrics {
    public static final String NAME = ChannelStatistics.class.getSimpleName();
    private final AtomicInteger channelCount = new AtomicInteger(0);
    private final ChannelGroup allChannels;

    public ChannelStatistics(ChannelGroup allChannels) {
        this.allChannels = allChannels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // connect
        channelCount.incrementAndGet();
        allChannels.add(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // disconnect
        channelCount.decrementAndGet();
        allChannels.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public Integer getChannelCount() {
        return channelCount.get();
    }

    @Override
    public ChannelGroup getChannels() {
        return allChannels;
    }

}
