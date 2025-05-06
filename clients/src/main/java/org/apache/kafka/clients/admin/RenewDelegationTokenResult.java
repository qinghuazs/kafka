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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Kafka委托令牌续期操作的结果类。
 * 
 * 该类用于处理{@link KafkaAdminClient#renewDelegationToken(byte[], RenewDelegationTokenOptions)}方法的异步执行结果。
 * 主要功能：
 * 1. 存储委托令牌续期后的新过期时间戳
 * 2. 提供异步方式获取续期操作的结果
 * 
 * 应用场景：
 * - 在需要延长委托令牌有效期时使用
 * - 用于管理和跟踪令牌续期操作的结果
 * - 支持异步获取续期后的新过期时间
 * 
 * 该类的API仍在演进中，详情请参见{@link Admin}。
 */
@InterfaceStability.Evolving
public class RenewDelegationTokenResult {
    /**
     * 存储委托令牌续期后的新过期时间戳的Future对象
     * 使用KafkaFuture而不是CompletableFuture，以提供更好的异常处理和类型安全
     */
    private final KafkaFuture<Long> expiryTimestamp;

    /**
     * 构造函数，初始化续期结果对象
     * 
     * @param expiryTimestamp 包含令牌新过期时间戳的Future对象
     */
    RenewDelegationTokenResult(KafkaFuture<Long> expiryTimestamp) {
        // 初始化expiryTimestamp字段，存储异步获取的新过期时间戳
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * 获取包含令牌新过期时间戳的Future对象
     * 
     * @return 返回一个KafkaFuture对象，当完成时将产生续期后的过期时间戳（以毫秒为单位的UNIX时间戳）
     */
    public KafkaFuture<Long> expiryTimestamp() {
        // 返回存储的Future对象，允许调用者异步获取新的过期时间戳
        return expiryTimestamp;
    }
}
