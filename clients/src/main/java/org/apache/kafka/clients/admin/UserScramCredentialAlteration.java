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
 * A request to alter a user's SASL/SCRAM credentials.
 * 用于修改用户的SASL/SCRAM凭证的请求。
 * 
 * SASL/SCRAM是Kafka支持的一种安全认证机制，用于验证客户端身份。
 * 这个抽象类作为基类，提供了修改用户凭证的基本功能，包括：
 * 1. 更新/插入凭证（UserScramCredentialUpsertion）
 * 2. 删除凭证（UserScramCredentialDeletion）
 * 
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public abstract class UserScramCredentialAlteration {
    /**
     * 用户名字段
     * 这个字段是final的，表示一旦设置就不能修改
     * 在子类中可以访问这个字段，因为它是protected的
     */
    protected final String user;

    /**
     * 构造函数
     * @param user 必需的用户名参数
     * 使用Objects.requireNonNull确保用户名不为null
     * 如果传入null值，将抛出NullPointerException
     */
    protected UserScramCredentialAlteration(String user) {
        this.user = Objects.requireNonNull(user);
    }

    /**
     * 获取用户名的方法
     * @return 返回非空的用户名
     * 这个方法是公开的，允许外部代码获取用户名
     * 由于user字段是final的，所以这个方法总是返回相同的值
     */
    public String user() {
        return this.user;
    }
}
