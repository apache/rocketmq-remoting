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

package org.apache.rocketmq.remoting.common;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.BaseTest;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.exception.RemotingAccessException;
import org.apache.rocketmq.remoting.api.exception.RemotingRuntimeException;
import org.apache.rocketmq.remoting.impl.command.RemotingCommandFactoryImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ResponseFutureTest extends BaseTest {
    private ResponseFuture future;
    private RemotingCommandFactoryImpl factory = new RemotingCommandFactoryImpl();

    @Test
    public void executeAsyncHandler_Success() {
        final RemotingCommand reqCommand = factory.createRequest();
        final RemotingCommand resCommand = factory.createResponse(reqCommand);
        future = new ResponseFuture(1, 3000, new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {
                shouldNotReachHere();
            }

            @Override
            public void onSuccess(final RemotingCommand response) {
                assertEquals(resCommand, response);
            }
        }, null);

        future.setRequestCommand(reqCommand);
        future.setResponseCommand(resCommand);
        future.executeAsyncHandler();
    }

    @Test
    public void executeAsyncHandler_Failure() {
        final RemotingCommand reqCommand = factory.createRequest();
        final RemotingCommand resCommand = factory.createResponse(reqCommand);
        final RemotingRuntimeException exception = new RemotingAccessException("Test Exception");
        future = new ResponseFuture(1, 3000, new AsyncHandler() {
            @Override
            public void onFailure(final RemotingCommand request, final Throwable cause) {
                assertEquals(reqCommand, request);
                assertNull(future.getResponseCommand());
                assertEquals(exception, cause);
            }

            @Override
            public void onSuccess(final RemotingCommand response) {
                assertEquals(resCommand, response);
            }
        }, null);

        future.setRequestCommand(reqCommand);
        future.setCause(exception);
        future.executeAsyncHandler();
    }

    @Test
    public void waitResponse_Success() {
        future = new ResponseFuture(1, 1000, null, null);
        final RemotingCommand reqCommand = factory.createRequest();
        final RemotingCommand resCommand = factory.createResponse(reqCommand);

        runInThreads(new Runnable() {
            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ignore) {
                }
                future.putResponse(resCommand);
            }
        }, 1);
        RemotingCommand response = future.waitResponse(1000);
        assertEquals(response, resCommand);
    }

    @Test
    public void waitResponse_Timeout() {
        future = new ResponseFuture(1, 1000, null, null);
        RemotingCommand response = future.waitResponse(10);
        assertNull(response);
    }
}