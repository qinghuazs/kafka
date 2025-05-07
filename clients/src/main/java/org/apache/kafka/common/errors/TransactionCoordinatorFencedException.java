/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.errors;

/**
 * 当事务协调器被隔离（fenced）时抛出的异常。
 * 这个异常通常在以下场景抛出：
 * 1. 事务协调器发生故障转移，旧的协调器被隔离
 * 2. 事务协调器的世代（generation）发生变化，导致之前的协调器被隔离
 * 3. 客户端与已被隔离的事务协调器进行通信时
 */
public class TransactionCoordinatorFencedException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造事务协调器隔离异常
     * @param message 描述事务协调器被隔离原因的错误消息
     */
    public TransactionCoordinatorFencedException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造事务协调器隔离异常
     * @param message 描述事务协调器被隔离原因的错误消息
     * @param cause 导致事务协调器被隔离的底层异常
     */
    public TransactionCoordinatorFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}
