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

import java.io.PrintStream;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.command.RemotingCommandFactoryImpl;
import org.assertj.core.api.Fail;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class BaseTest {
    @Mock
    protected PrintStream fakeOut;

    @Test
    public void emptyTest() {

    }

    protected void scheduleInThreads(final Runnable runnable, int periodMillis) {
        final ScheduledExecutorService executor = ThreadUtils.newSingleThreadScheduledExecutor("UnitTests", true);
        executor.scheduleAtFixedRate(runnable, 0, periodMillis, TimeUnit.MILLISECONDS);
    }

    protected void runInThreads(final Runnable runnable, int concurrentNum) {
        final ExecutorService executor = ThreadUtils.newFixedThreadPool(concurrentNum, 1000, "UnitTests", true);
        for (int i = 0; i < concurrentNum; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    runnable.run();
                }
            });
        }
    }

    protected void runInThreads(final Runnable runnable, int threadsNum,
        int timeoutMillis) throws InterruptedException {
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

    protected <T> ObjectFuture<T> newObjectFuture(int permits, int timeoutMillis) {
        return new ObjectFuture<>(permits, timeoutMillis);
    }

    protected ObjectFuture<String> retrieveStringFromLog(final String targetString) {
        final ObjectFuture<String> objectFuture = newObjectFuture(1, 3000);
        final PrintStream originStream = System.err;
        System.setErr(fakeOut);

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();

                String str = arguments[0].toString();

                if (str.contains(targetString)) {
                    System.setErr(originStream);
                    objectFuture.putObject(str);
                    objectFuture.release();
                }

                return null;
            }
        }).when(fakeOut).println(any(String.class));

        return objectFuture;
    }

    protected class ObjectFuture<T> {
        volatile private T object;
        private Semaphore semaphore;
        private int permits;
        private int timeoutMillis;

        public ObjectFuture(int permits, int timeoutMillis) {
            semaphore = new Semaphore(0);
            this.permits = permits;
            this.timeoutMillis = timeoutMillis;
        }

        public void release() {
            semaphore.release();
        }

        public void putObject(T object) {
            this.object = object;
        }

        public T getObject() {
            try {
                if (!semaphore.tryAcquire(permits, timeoutMillis, TimeUnit.MILLISECONDS)) {
                    Fail.fail("Get permits failed");
                }
            } catch (InterruptedException e) {
                Fail.fail("Get object failed", e);
            }
            return this.object;
        }
    }

    public class UnitTestException extends RuntimeException {

    }
}
