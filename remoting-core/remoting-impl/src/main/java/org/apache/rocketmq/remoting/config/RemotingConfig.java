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

package org.apache.rocketmq.remoting.config;

public abstract class RemotingConfig extends TcpSocketConfig {
    /**
     * IdleStateEvent will be triggered when neither read nor write was
     * performed for the specified period of this time. Specify {@code 0} to
     * disable
     */
    private int connectionChannelReaderIdleSeconds = 0;
    private int connectionChannelWriterIdleSeconds = 0;
    private int connectionChannelIdleSeconds = 120;

    private int writeBufLowWaterMark = 32 * 10240;
    private int writeBufHighWaterMark = 64 * 10240;

    private int asyncHandlerExecutorThreads = Runtime.getRuntime().availableProcessors();

    private int publicExecutorThreads = 4;

    public abstract int getOnewayInvokeSemaphore();

    public abstract int getAsyncInvokeSemaphore();

    public int getConnectionChannelReaderIdleSeconds() {
        return connectionChannelReaderIdleSeconds;
    }

    public void setConnectionChannelReaderIdleSeconds(final int connectionChannelReaderIdleSeconds) {
        this.connectionChannelReaderIdleSeconds = connectionChannelReaderIdleSeconds;
    }

    public int getConnectionChannelWriterIdleSeconds() {
        return connectionChannelWriterIdleSeconds;
    }

    public void setConnectionChannelWriterIdleSeconds(final int connectionChannelWriterIdleSeconds) {
        this.connectionChannelWriterIdleSeconds = connectionChannelWriterIdleSeconds;
    }

    public int getConnectionChannelIdleSeconds() {
        return connectionChannelIdleSeconds;
    }

    public void setConnectionChannelIdleSeconds(final int connectionChannelIdleSeconds) {
        this.connectionChannelIdleSeconds = connectionChannelIdleSeconds;
    }

    public int getWriteBufLowWaterMark() {
        return writeBufLowWaterMark;
    }

    public void setWriteBufLowWaterMark(final int writeBufLowWaterMark) {
        this.writeBufLowWaterMark = writeBufLowWaterMark;
    }

    public int getWriteBufHighWaterMark() {
        return writeBufHighWaterMark;
    }

    public void setWriteBufHighWaterMark(final int writeBufHighWaterMark) {
        this.writeBufHighWaterMark = writeBufHighWaterMark;
    }

    public int getAsyncHandlerExecutorThreads() {
        return asyncHandlerExecutorThreads;
    }

    public void setAsyncHandlerExecutorThreads(final int asyncHandlerExecutorThreads) {
        this.asyncHandlerExecutorThreads = asyncHandlerExecutorThreads;
    }

    public int getPublicExecutorThreads() {
        return publicExecutorThreads;
    }

    public void setPublicExecutorThreads(final int publicExecutorThreads) {
        this.publicExecutorThreads = publicExecutorThreads;
    }
}
