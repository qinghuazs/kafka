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
 * 表示事务状态无效的异常。
 * 
 * 应用场景：
 * 1. 当生产者在错误的事务状态下执行操作，如在未开启事务时提交事务
 * 2. 当事务状态机转换失败，如在事务未完成时尝试开启新事务
 * 3. 当多个生产者实例同时操作同一个事务时可能发生状态冲突
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表示这是一个不可重试的异常
 * 2. 用于维护事务的完整性和一致性
 * 3. 帮助开发者快速定位事务状态问题
 */
public class InvalidTxnStateException extends ApiException {
    public InvalidTxnStateException(String message) {
        super(message);
    }
}
