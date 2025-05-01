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

/**
 * 创建委派令牌操作的结果类，封装了{@link KafkaAdminClient#createDelegationToken(CreateDelegationTokenOptions)}调用的返回值。
 * 
 * 委派令牌（Delegation Token）是Kafka安全机制中的一个重要组件，用于在不同服务之间进行身份认证。
 * 当一个服务需要代表用户访问Kafka集群时，可以使用委派令牌来证明其已被授权。
 * 
 * 该类的API仍在演进中，详细信息请参考{@link Admin}。
 */
@InterfaceStability.Evolving
public class CreateDelegationTokenResult {
    /**
     * 用于异步获取创建的委派令牌的Future对象。
     * KafkaFuture是Kafka自定义的Future实现，提供了更多的功能和更好的异常处理机制。
     * DelegationToken包含了令牌的详细信息，如令牌ID、主体、发行时间和过期时间等。
     */
    private final KafkaFuture<DelegationToken> delegationToken;

    /**
     * 构造函数，初始化委派令牌的Future对象。
     * 
     * @param delegationToken 包含委派令牌的Future对象，当令牌创建完成时，该Future将完成并返回令牌信息
     */
    CreateDelegationTokenResult(KafkaFuture<DelegationToken> delegationToken) {
        this.delegationToken = delegationToken;
    }

    /**
     * 获取委派令牌的Future对象。
     * 
     * @return 返回一个KafkaFuture对象，当异步操作完成时，可以通过该对象获取创建的委派令牌
     *         如果令牌创建失败，Future会包含相应的异常信息
     */
    public KafkaFuture<DelegationToken> delegationToken() {
        return delegationToken;
    }
}
