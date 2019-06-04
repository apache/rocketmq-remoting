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
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoop;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientChannelManagerTest extends BaseTest {
    private final static String TARGET_ADDR = "127.0.0.1:8080";
    @Mock
    private Bootstrap clientBootstrap;

    @Mock
    private Channel channel;

    private ClientChannelManager channelManager;

    private ChannelPromise channelPromise;

    @Before
    public void init() {
        channelPromise = new DefaultChannelPromise(channel, new DefaultEventLoop());

        when(channel.isActive()).thenReturn(true);
        when(clientBootstrap.connect(any(SocketAddress.class))).thenReturn(channelPromise);
        when(channel.close()).thenReturn(channelPromise);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress(8080));

        channelManager = new ClientChannelManager(clientBootstrap, new RemotingConfig());
    }

    @Test
    public void clear() {
        channelPromise.setSuccess();
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);

        assertThat(targetChannel).isNotNull();
        assertThat(channelManager.channelTables.size()).isEqualTo(1);

        channelManager.clear();
        assertThat(channelManager.channelTables.size()).isEqualTo(0);

    }

    @Test
    public void createIfAbsent_UseExistingConnection_Success() {
        channelPromise.setSuccess();
        assertThat(channelManager.channelTables.size()).isEqualTo(0);
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);
        assertThat(targetChannel).isEqualTo(channel);

        assertThat(channelManager.channelTables.size()).isEqualTo(1);
        targetChannel = channelManager.createIfAbsent(TARGET_ADDR);
        assertThat(targetChannel).isEqualTo(channel);
        assertThat(channelManager.channelTables.size()).isEqualTo(1);
    }

    @Test
    public void createIfAbsent_CreateNewConnection_Success() {
        channelPromise.setSuccess();
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);
        assertThat(targetChannel).isEqualTo(channel);
    }

    @Test
    public void createIfAbsent_Concurrent_Success() throws InterruptedException {
        int concurrentNum = 3;
        final boolean[] channelMismatch = {false};

        runInThreads(new Runnable() {
            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    channelPromise.setSuccess();
                } catch (InterruptedException ignore) {
                }
            }
        }, 1);
        runInThreads(new Runnable() {
            @Override
            public void run() {
                Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);
                if (targetChannel != channel) {
                    channelMismatch[0] = true;
                }
            }
        }, concurrentNum, 3000);

        assertThat(channelMismatch[0]).isFalse();
        assertThat(channelManager.channelTables.size()).isEqualTo(1);
    }

    @Test
    public void createIfAbsent_ClosedChannel_NullReturn() {
        channelPromise.setSuccess();
        when(channel.isActive()).thenReturn(false);
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);
        assertThat(targetChannel).isNull();
    }

    @Test
    public void closeChannel_WithChannel_Success() {
        channelPromise.setSuccess();
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);

        channelManager.closeChannel(targetChannel);
        assertThat(channelManager.channelTables.size()).isEqualTo(0);
    }

    @Test
    public void closeChannel_WithAddr_Success() {
        channelPromise.setSuccess();
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);

        channelManager.closeChannel(TARGET_ADDR, targetChannel);
        assertThat(channelManager.channelTables.size()).isEqualTo(0);
    }

    @Test
    public void closeChannel_NonExistingChannel_Success() {
        channelPromise.setSuccess();
        Channel targetChannel = channelManager.createIfAbsent(TARGET_ADDR);

        channelManager.closeChannel(mock(Channel.class));
        assertThat(channelManager.channelTables.size()).isEqualTo(1);
    }
}