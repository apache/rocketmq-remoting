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

public class RemotingClientConfig extends RemotingConfig {
    private int connectTimeoutMillis = 3000;

    private boolean clientNativeEpollEnable = false;

    private int clientIoThreads = 1;
    private int clientWorkerThreads = 4;

    private int clientOnewayInvokeSemaphore = 65535;
    private int clientAsyncInvokeSemaphore = 65535;

    private boolean clientPooledBytebufAllocatorEnable = false;

    private boolean clientCloseSocketIfTimeout = false;
    private boolean clientShortConnectionEnable = false;

    public boolean isClientNativeEpollEnable() {
        return clientNativeEpollEnable;
    }

    public void setClientNativeEpollEnable(final boolean clientNativeEpollEnable) {
        this.clientNativeEpollEnable = clientNativeEpollEnable;
    }

    public int getClientIoThreads() {
        return clientIoThreads;
    }

    public void setClientIoThreads(final int clientIoThreads) {
        this.clientIoThreads = clientIoThreads;
    }

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }

    public void setClientWorkerThreads(final int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }

    public int getClientOnewayInvokeSemaphore() {
        return clientOnewayInvokeSemaphore;
    }

    public void setClientOnewayInvokeSemaphore(final int clientOnewayInvokeSemaphore) {
        this.clientOnewayInvokeSemaphore = clientOnewayInvokeSemaphore;
    }

    public int getClientAsyncInvokeSemaphore() {
        return clientAsyncInvokeSemaphore;
    }

    public void setClientAsyncInvokeSemaphore(final int clientAsyncInvokeSemaphore) {
        this.clientAsyncInvokeSemaphore = clientAsyncInvokeSemaphore;
    }

    public boolean isClientPooledBytebufAllocatorEnable() {
        return clientPooledBytebufAllocatorEnable;
    }

    public void setClientPooledBytebufAllocatorEnable(final boolean clientPooledBytebufAllocatorEnable) {
        this.clientPooledBytebufAllocatorEnable = clientPooledBytebufAllocatorEnable;
    }

    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }

    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }

    public boolean isClientShortConnectionEnable() {
        return clientShortConnectionEnable;
    }

    public void setClientShortConnectionEnable(final boolean clientShortConnectionEnable) {
        this.clientShortConnectionEnable = clientShortConnectionEnable;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(final int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    @Override
    public int getOnewayInvokeSemaphore() {
        return this.clientOnewayInvokeSemaphore;
    }

    @Override
    public int getAsyncInvokeSemaphore() {
        return this.clientAsyncInvokeSemaphore;
    }
}
