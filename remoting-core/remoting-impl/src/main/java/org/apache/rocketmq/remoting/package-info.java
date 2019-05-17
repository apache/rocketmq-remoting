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

/**
 * This package contains all the transport classes that can be reused any times.
 *
 * Remoting wire-format protocol description:
 *
 * <pre>
 * 1.Protocol Magic                            1 byte(0x14)
 * 2.Total Length                              4 byte,exclude protocol type size
 * 3.Command Code                              2 byte, command key
 * 4.Command Version                           2 byte, command version
 * 5.RequestID                                 4 byte,used for repeatable requests,connection reuse.an requestID string
 * representing a client-generated, globally unique for some time unit, identifier for the request
 * 6.Traffic Type                              1 byte,0-sync;1-async;2-oneway;3-response
 * 7.OpCode                                    2 byte, operation result code(success or error)
 * 8.Remark Length                             2 byte
 * 9.Remark                                    variant length,utf8 string
 * 10.Properties Size                          2 byte
 * 11.Property Length                          2 byte
 * 12.Property Body                            variant length,utf8,Key\nValue
 * 13.Inbound or OutBound payload length       4 byte
 * 14.Inbound or OutBound payload              variant length, max size limitation is 16M
 *
 * </pre>
 */
package org.apache.rocketmq.remoting;