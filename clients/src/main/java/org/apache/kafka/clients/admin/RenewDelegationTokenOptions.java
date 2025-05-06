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
 * 用于配置Kafka委托令牌续期操作的选项类，与{@link Admin#renewDelegationToken(byte[], RenewDelegationTokenOptions)}方法配合使用。
 * 
 * 该类主要用于：
 * 1. 设置委托令牌的续期时间
 * 2. 作为Admin API续期令牌操作的配置参数
 * 
 * 应用场景：
 * - 当委托令牌即将过期时，可以使用此类来延长令牌的有效期
 * - 在需要持续访问Kafka集群但不想频繁创建新令牌时使用
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class RenewDelegationTokenOptions extends AbstractOptions<RenewDelegationTokenOptions> {
    /**
     * 委托令牌的续期时间（以毫秒为单位）
     * 默认值为-1，表示使用服务器端的默认续期时间配置
     */
    private long renewTimePeriodMs = -1;

    /**
     * 设置委托令牌的续期时间
     * 
     * @param renewTimePeriodMs 续期时间（毫秒），如果设置为-1，将使用服务器端的默认续期时间
     * @return 当前RenewDelegationTokenOptions实例，支持链式调用
     */
    public RenewDelegationTokenOptions renewTimePeriodMs(long renewTimePeriodMs) {
        this.renewTimePeriodMs = renewTimePeriodMs;
        return this;
    }

    /**
     * 获取配置的委托令牌续期时间
     * 
     * @return 续期时间（毫秒），如果为-1表示使用服务器端默认配置
     */
    public long renewTimePeriodMs() {
        return renewTimePeriodMs;
    }
}
