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
package org.apache.rocketmq.remoting.api.exception;

/**
 * RemotingTimeoutException will be thrown when the execution
 * of the target method did not complete before a configurable
 * timeout, for example when a reply message was not received.
 *
 * @since 1.0.0
 */
public class RemotingTimeoutException extends RemotingAccessException {
    private static final long serialVersionUID = 8710772392914461626L;

    /**
     * Constructor for RemotingTimeoutException with the specified detail message,configurable timeout.
     *
     * @param msg the detail message
     * @param timeoutMillis configurable timeout
     */
    public RemotingTimeoutException(String msg, long timeoutMillis) {
        this(msg, timeoutMillis, null);
    }

    /**
     * Constructor for RemotingTimeoutException with the specified detail message,configurable timeout
     * and nested exception..
     *
     * @param msg the detail message
     * @param timeoutMillis configurable timeout
     * @param cause Exception cause
     */
    public RemotingTimeoutException(String msg, long timeoutMillis, Throwable cause) {
        super(String.format("%s, waiting for %s ms", msg, timeoutMillis), cause);
    }

    /**
     * Constructor for RemotingTimeoutException with the specified detail message.
     *
     * @param msg the detail message
     */
    public RemotingTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Constructor for RemotingTimeoutException with the specified detail message
     * and nested exception.
     *
     * @param msg the detail message
     * @param cause the root cause from the remoting API in use
     */
    public RemotingTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
