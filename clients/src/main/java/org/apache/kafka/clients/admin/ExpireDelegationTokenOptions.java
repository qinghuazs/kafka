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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 用于 {@link Admin#expireDelegationToken(byte[], ExpireDelegationTokenOptions)} 的配置选项类。
 * 该类用于配置Kafka委托令牌的过期时间和相关参数。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class ExpireDelegationTokenOptions extends AbstractOptions<ExpireDelegationTokenOptions> {
    // 令牌的过期时间周期（毫秒），默认值-1表示立即过期
    private long expiryTimePeriodMs = -1L;

    /**
     * 设置令牌的过期时间周期
     * 
     * @param expiryTimePeriodMs 令牌应该过期的时间周期
     * {@code expiryTimePeriodMs} >= 0: 令牌的过期时间戳将被更新为 min(当前时间 + expiryTimePeriodMs, 最大时间戳)
     * {@code expiryTimePeriodMs} < 0: 令牌将立即过期
     * @return 当前ExpireDelegationTokenOptions实例，支持链式调用
     */
    public ExpireDelegationTokenOptions expiryTimePeriodMs(
        long expiryTimePeriodMs
    ) {
        // 设置过期时间周期
        this.expiryTimePeriodMs = expiryTimePeriodMs;
        // 返回当前实例以支持方法链式调用
        return this;
    }

    /**
     * 获取令牌的过期时间周期
     * 
     * @return 返回设置的过期时间周期（毫秒）
     */
    public long expiryTimePeriodMs() {
        // 返回过期时间周期值
        return expiryTimePeriodMs;
    }
}
