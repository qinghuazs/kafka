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
 * {@link KafkaAdminClient#expireDelegationToken(byte[], ExpireDelegationTokenOptions)} 调用的结果类。
 * 该类用于获取Kafka委托令牌过期操作的结果信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class ExpireDelegationTokenResult {
    // 存储令牌过期时间戳的Future
    private final KafkaFuture<Long> expiryTimestamp;

    /**
     * 构造函数，初始化令牌过期结果
     * 
     * @param expiryTimestamp 包含令牌过期时间戳的Future
     */
    ExpireDelegationTokenResult(KafkaFuture<Long> expiryTimestamp) {
        // 初始化expiryTimestamp字段，存储过期时间戳Future
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * 获取包含令牌过期时间戳的Future
     * 
     * @return 返回包含令牌过期时间戳的Future
     */
    public KafkaFuture<Long> expiryTimestamp() {
        // 返回过期时间戳Future
        return expiryTimestamp;
    }
}
