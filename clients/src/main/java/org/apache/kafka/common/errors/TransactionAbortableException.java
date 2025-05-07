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
 * 表示事务处于可中止状态的异常。
 * 这个异常通常在以下场景抛出：
 * 1. 事务正在进行中，但由于某些原因（如超时、资源限制等）可能需要被中止
 * 2. 事务协调器检测到事务可能需要被中止时
 * 3. 提示客户端当前事务状态不稳定，可能需要执行中止操作
 */
public class TransactionAbortableException extends ApiException {
    /**
     * 使用指定的错误消息构造事务可中止异常
     * @param message 描述事务可能被中止原因的错误消息
     */
    public TransactionAbortableException(String message) {
        super(message);
    }
}
