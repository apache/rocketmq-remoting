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

package org.apache.rocketmq.remoting;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.command.RemotingCommandFactoryImpl;

public class BaseTest {
    protected void runInThreads(final Runnable runnable, int threadsNum) {
        ExecutorService executor = Executors.newFixedThreadPool(threadsNum);
        for (int i = 0; i < threadsNum; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    runnable.run();
                }
            });
        }

        ThreadUtils.shutdownGracefully(executor, 5, TimeUnit.SECONDS);
    }

    protected void runInThreads(final Runnable runnable, int threadsNum, int timeoutMillis) throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);

        runInThreads(new Runnable() {
            @Override
            public void run() {
                runnable.run();
                semaphore.release();
            }
        }, threadsNum);

        semaphore.tryAcquire(threadsNum, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    protected void shouldNotReachHere() {
        throw new RuntimeException("shouldn't reach here");
    }

    protected RemotingCommand randomRemotingCommand() {
        RemotingCommand command = new RemotingCommandFactoryImpl().createRequest();

        Random random = new Random(System.currentTimeMillis());

        command.cmdCode((short) random.nextInt());
        command.cmdVersion((short) random.nextInt());
        command.remark(RandomStringUtils.random(1024));
        command.opCode((short) random.nextInt());
        command.payload(RandomStringUtils.random(2048).getBytes());

        command.requestID(random.nextInt());
        command.trafficType(TrafficType.REQUEST_SYNC);

        int propertiesLen = 1 + random.nextInt(10);

        for (int i = 0; i < propertiesLen; i++) {
            command.property(RandomStringUtils.random(512), RandomStringUtils.random(1024));
        }

        return command;
    }
}
