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
import org.apache.kafka.common.security.token.delegation.DelegationToken;

import java.util.List;

/**
 * {@link KafkaAdminClient#describeDelegationToken(DescribeDelegationTokenOptions)} 调用的结果类。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeDelegationTokenResult {
    // 存储委托令牌列表的Future对象
    // 使用KafkaFuture而不是CompletableFuture是为了提供更好的异常处理和类型安全
    private final KafkaFuture<List<DelegationToken>> delegationTokens;

    /**
     * 构造函数，初始化委托令牌描述结果
     * 
     * @param delegationTokens 包含委托令牌列表的Future对象
     */
    DescribeDelegationTokenResult(KafkaFuture<List<DelegationToken>> delegationTokens) {
        // 初始化delegationTokens字段，存储异步获取的委托令牌列表
        this.delegationTokens = delegationTokens;
    }

    /**
     * 获取包含委托令牌列表的Future对象
     * 
     * @return 返回一个KafkaFuture，当完成时将产生委托令牌列表
     */
    public KafkaFuture<List<DelegationToken>> delegationTokens() {
        // 返回存储的Future对象，允许调用者异步获取委托令牌列表
        return delegationTokens;
    }
}
