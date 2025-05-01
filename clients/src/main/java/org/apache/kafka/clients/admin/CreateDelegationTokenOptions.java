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
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * 用于配置创建Kafka委派令牌(Delegation Token)的选项类。
 * 委派令牌是Kafka安全认证机制的一部分，允许客户端在不提供原始凭证的情况下访问Kafka集群。
 * 
 * 该类提供了以下核心功能：
 * 1. 设置令牌的最大生命周期
 * 2. 指定可以续期令牌的主体列表
 * 3. 定义令牌的所有者
 *
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class CreateDelegationTokenOptions extends AbstractOptions<CreateDelegationTokenOptions> {
    /**
     * 令牌的最大生命周期（以毫秒为单位）
     * 默认值为-1，表示使用服务器端的默认配置
     */
    private long maxLifetimeMs = -1;

    /**
     * 可以续期该令牌的Kafka主体列表
     * 这些主体将有权限延长令牌的有效期
     */
    private List<KafkaPrincipal> renewers = new LinkedList<>();

    /**
     * 令牌的所有者
     * 如果未指定，则默认为当前认证的主体
     */
    private KafkaPrincipal owner = null;

    /**
     * 设置可以续期该令牌的主体列表
     * @param renewers 具有续期权限的Kafka主体列表
     * @return 当前对象实例，支持方法链式调用
     */
    public CreateDelegationTokenOptions renewers(List<KafkaPrincipal> renewers) {
        this.renewers = renewers;
        return this;
    }

    /**
     * 获取当前配置的续期主体列表
     * @return 可以续期该令牌的Kafka主体列表
     */
    public List<KafkaPrincipal> renewers() {
        return renewers;
    }

    /**
     * 设置令牌的所有者
     * @param owner 令牌所有者的Kafka主体
     * @return 当前对象实例，支持方法链式调用
     */
    public CreateDelegationTokenOptions owner(KafkaPrincipal owner) {
        this.owner = owner;
        return this;
    }

    /**
     * 获取令牌所有者
     * @return 包装在Optional中的令牌所有者，如果未设置则返回空Optional
     */
    public Optional<KafkaPrincipal> owner() {
        return Optional.ofNullable(owner);
    }

    /**
     * 设置令牌的最大生命周期（已废弃的方法）
     * @deprecated 自4.0版本起已废弃，请使用maxLifetimeMs()方法
     */
    @Deprecated
    public CreateDelegationTokenOptions maxlifeTimeMs(long maxLifetimeMs) {
        this.maxLifetimeMs = maxLifetimeMs;
        return this;
    }

    /**
     * 设置令牌的最大生命周期
     * @param maxLifetimeMs 令牌的最大有效期（毫秒）
     * @return 当前对象实例，支持方法链式调用
     */
    public CreateDelegationTokenOptions maxLifetimeMs(long maxLifetimeMs) {
        this.maxLifetimeMs = maxLifetimeMs;
        return this;
    }

    /**
     * 获取令牌的最大生命周期（已废弃的方法）
     * @deprecated 自4.0版本起已废弃，请使用maxLifetimeMs()方法
     */
    @Deprecated
    public long maxlifeTimeMs() {
        return maxLifetimeMs;
    }

    /**
     * 获取令牌的最大生命周期
     * @return 令牌的最大有效期（毫秒）
     */
    public long maxLifetimeMs() {
        return maxLifetimeMs;
    }
}
