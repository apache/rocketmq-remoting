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

/**
 * TCP socket configuration
 *
 * @see java.net.SocketOptions
 */
public class TcpSocketConfig {
    private boolean tcpSoReuseAddress = true;
    private boolean tcpSoKeepAlive = false;
    private boolean tcpSoNoDelay = true;
    private int tcpSoSndBufSize = 65535;  // see /proc/sys/net/ipv4/tcp_rmem
    private int tcpSoRcvBufSize = 65535;  // see /proc/sys/net/ipv4/tcp_wmem
    private int tcpSoBacklogSize = 1024;
    private int tcpSoLinger = -1;
    private int tcpSoTimeoutMillis = 3000;

    public boolean isTcpSoReuseAddress() {
        return tcpSoReuseAddress;
    }

    public void setTcpSoReuseAddress(final boolean tcpSoReuseAddress) {
        this.tcpSoReuseAddress = tcpSoReuseAddress;
    }

    public boolean isTcpSoKeepAlive() {
        return tcpSoKeepAlive;
    }

    public void setTcpSoKeepAlive(final boolean tcpSoKeepAlive) {
        this.tcpSoKeepAlive = tcpSoKeepAlive;
    }

    public boolean isTcpSoNoDelay() {
        return tcpSoNoDelay;
    }

    public void setTcpSoNoDelay(final boolean tcpSoNoDelay) {
        this.tcpSoNoDelay = tcpSoNoDelay;
    }

    public int getTcpSoSndBufSize() {
        return tcpSoSndBufSize;
    }

    public void setTcpSoSndBufSize(final int tcpSoSndBufSize) {
        this.tcpSoSndBufSize = tcpSoSndBufSize;
    }

    public int getTcpSoRcvBufSize() {
        return tcpSoRcvBufSize;
    }

    public void setTcpSoRcvBufSize(final int tcpSoRcvBufSize) {
        this.tcpSoRcvBufSize = tcpSoRcvBufSize;
    }

    public int getTcpSoBacklogSize() {
        return tcpSoBacklogSize;
    }

    public void setTcpSoBacklogSize(final int tcpSoBacklogSize) {
        this.tcpSoBacklogSize = tcpSoBacklogSize;
    }

    public int getTcpSoLinger() {
        return tcpSoLinger;
    }

    public void setTcpSoLinger(final int tcpSoLinger) {
        this.tcpSoLinger = tcpSoLinger;
    }

    public int getTcpSoTimeoutMillis() {
        return tcpSoTimeoutMillis;
    }

    public void setTcpSoTimeoutMillis(final int tcpSoTimeoutMillis) {
        this.tcpSoTimeoutMillis = tcpSoTimeoutMillis;
    }
}
