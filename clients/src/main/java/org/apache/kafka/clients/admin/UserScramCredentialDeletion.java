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

import java.util.Objects;

/**
 * A request to delete a SASL/SCRAM credential for a user.
 * 用于删除用户的SASL/SCRAM凭证的请求类。
 * 
 * 这个类是Kafka中用于处理SASL/SCRAM安全认证的重要组件之一。
 * 主要用于以下场景：
 * 1. 当用户需要删除某个认证机制的凭证时（例如：用户离职或权限调整）
 * 2. 当需要清理过期或不再使用的认证凭证时
 * 3. 在进行认证机制迁移时，需要删除旧的认证机制
 * 
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class UserScramCredentialDeletion extends UserScramCredentialAlteration {
    /**
     * SCRAM认证机制
     * 这个字段是final的，表示一旦设置就不能修改
     * 用于指定要删除的具体SCRAM认证机制类型（如SCRAM-SHA-256或SCRAM-SHA-512）
     */
    private final ScramMechanism mechanism;

    /**
     * 构造函数
     * @param user 必需的用户名参数，指定要删除其凭证的用户
     * @param mechanism 必需的SCRAM认证机制参数，指定要删除的认证机制类型
     * 
     * 实现细节：
     * 1. 调用父类构造函数设置用户名
     * 2. 使用Objects.requireNonNull确保mechanism参数不为null
     * 3. 如果传入null值，将抛出NullPointerException
     */
    public UserScramCredentialDeletion(String user, ScramMechanism mechanism) {
        super(user);
        this.mechanism = Objects.requireNonNull(mechanism);
    }

    /**
     * 获取SCRAM认证机制的方法
     * @return 返回非空的SCRAM认证机制
     * 
     * 实现细节：
     * 1. 由于mechanism字段是final的，所以这个方法总是返回相同的值
     * 2. 返回值永远不会为null，因为在构造函数中已经进行了null检查
     */
    public ScramMechanism mechanism() {
        return mechanism;
    }
}
