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
 * 并发事务异常类
 * 当多个事务尝试同时访问相同资源时抛出此异常。
 * 继承自RetriableException表示这是一个可重试的异常。
 * 
 * 应用场景：
 * 1. 事务冲突：多个事务同时操作相同资源
 * 2. 并发控制：处理事务并发访问冲突
 * 3. 资源竞争：多个事务争用同一资源
 * 4. 重试机制：支持事务操作的重试处理
 *
 * 设计考虑：
 * 1. 继承性：继承自RetriableException以支持重试机制
 * 2. 序列化：支持异常的序列化传输
 * 3. 简单性：仅提供基本的异常信息传递
 * 4. 重试策略：允许客户端重试失败的事务
 */
public class ConcurrentTransactionsException extends RetriableException {
    
    /**
     * 序列化版本ID
     * 用于确保序列化的兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用错误消息构造并发事务异常
     * 
     * 实现说明：
     * - 调用父类构造函数传递错误消息
     * - 用于描述具体的并发冲突情况
     *
     * @param message 描述并发事务冲突的消息
     */
    public ConcurrentTransactionsException(final String message) {
        // 调用父类构造函数，传递错误消息
        super(message);
    }
}
