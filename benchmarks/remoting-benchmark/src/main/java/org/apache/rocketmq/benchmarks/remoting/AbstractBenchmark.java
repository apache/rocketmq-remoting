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

package org.apache.rocketmq.benchmarks.remoting;

import org.apache.rocketmq.remoting.RemotingBootstrapFactory;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.RemotingServer;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractBenchmark {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractBenchmark.class);

    /**
     * Standard message sizes.
     */
    public enum MessageSize {
        SMALL(16), MEDIUM(1024), LARGE(65536), JUMBO(1048576);

        private final int bytes;
        MessageSize(int bytes) {
            this.bytes = bytes;
        }

        public int bytes() {
            return bytes;
        }
    }

    /**
     * Support channel types.
     */
    public enum ChannelType {
        NIO, LOCAL;
    }

    public static void main(String[] args) throws InterruptedException {
        RemotingServer server = RemotingBootstrapFactory.createRemotingServer(new RemotingConfig());

        server.registerRequestProcessor((short) 1, (channel, request) -> {
            RemotingCommand response = server.commandFactory().createResponse(request);
            response.payload("zhouxinyu".getBytes());
            System.out.println(new String(request.payload()));
            return response;
        });
        server.start();

        RemotingClient client = RemotingBootstrapFactory.createRemotingClient(new RemotingConfig());
        client.start();

        RemotingCommand request = client.commandFactory().createRequest();
        request.cmdCode((short) 1);
        request.cmdVersion((short) 1);
        request.payload("hello".getBytes());
        RemotingCommand response = client.invoke("127.0.0.1:8888", request, 3000);
        System.out.println(new String(response.payload()));

        client.stop();
        server.stop();
    }
}
