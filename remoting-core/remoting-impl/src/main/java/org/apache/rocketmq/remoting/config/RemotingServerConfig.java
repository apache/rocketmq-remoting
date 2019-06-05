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

public class RemotingServerConfig extends RemotingConfig {
    private int serverListenPort = 8888;
    /**
     * If server only listened 1 port,recommend to set the value to 1
     */
    private int serverAcceptorThreads = 1;
    private int serverIoThreads = 3;
    private int serverWorkerThreads = 8;

    private int serverOnewayInvokeSemaphore = 256;
    private int serverAsyncInvokeSemaphore = 64;

    private boolean serverNativeEpollEnable = false;
    private boolean serverPooledBytebufAllocatorEnable = true;

    public int getServerListenPort() {
        return serverListenPort;
    }

    public void setServerListenPort(final int serverListenPort) {
        this.serverListenPort = serverListenPort;
    }

    public int getServerAcceptorThreads() {
        return serverAcceptorThreads;
    }

    public void setServerAcceptorThreads(final int serverAcceptorThreads) {
        this.serverAcceptorThreads = serverAcceptorThreads;
    }

    public int getServerIoThreads() {
        return serverIoThreads;
    }

    public void setServerIoThreads(final int serverIoThreads) {
        this.serverIoThreads = serverIoThreads;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(final int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerOnewayInvokeSemaphore() {
        return serverOnewayInvokeSemaphore;
    }

    public void setServerOnewayInvokeSemaphore(final int serverOnewayInvokeSemaphore) {
        this.serverOnewayInvokeSemaphore = serverOnewayInvokeSemaphore;
    }

    public int getServerAsyncInvokeSemaphore() {
        return serverAsyncInvokeSemaphore;
    }

    public void setServerAsyncInvokeSemaphore(final int serverAsyncInvokeSemaphore) {
        this.serverAsyncInvokeSemaphore = serverAsyncInvokeSemaphore;
    }

    public boolean isServerNativeEpollEnable() {
        return serverNativeEpollEnable;
    }

    public void setServerNativeEpollEnable(final boolean serverNativeEpollEnable) {
        this.serverNativeEpollEnable = serverNativeEpollEnable;
    }

    public boolean isServerPooledBytebufAllocatorEnable() {
        return serverPooledBytebufAllocatorEnable;
    }

    public void setServerPooledBytebufAllocatorEnable(final boolean serverPooledBytebufAllocatorEnable) {
        this.serverPooledBytebufAllocatorEnable = serverPooledBytebufAllocatorEnable;
    }

    @Override
    public int getOnewayInvokeSemaphore() {
        return this.serverOnewayInvokeSemaphore;
    }

    @Override
    public int getAsyncInvokeSemaphore() {
        return this.serverAsyncInvokeSemaphore;
    }
}
