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
 * 当事务被中止时，用于处理任何未完成（未排空）的批次的异常。
 * 这个异常通常在以下场景抛出：
 * 1. 用户主动选择中止事务
 * 2. 事务在处理过程中被显式中止，且没有明确的底层原因
 * 3. 事务中的批次因事务中止而失败
 */
public class TransactionAbortedException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息和原因构造事务中止异常
     * @param message 描述事务中止原因的错误消息
     * @param cause 导致事务中止的底层异常
     */
    public TransactionAbortedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的错误消息构造事务中止异常
     * @param message 描述事务中止原因的错误消息
     */
    public TransactionAbortedException(String message) {
        super(message);
    }

    /**
     * 构造一个默认的事务中止异常，使用预设的错误消息
     * 通常用于表示批次因事务被中止而失败的场景
     */
    public TransactionAbortedException() {
        super("Failing batch since transaction was aborted");
    }
}
