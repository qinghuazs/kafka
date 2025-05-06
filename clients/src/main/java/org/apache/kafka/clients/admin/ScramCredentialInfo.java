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
 * 用于存储和管理与用户关联的SASL/SCRAM认证凭证的机制和迭代次数信息。
 * SASL/SCRAM是一种安全认证机制，用于在Kafka中进行用户身份验证。
 * 该类作为Kafka broker端SCRAM配置API的一部分，用于管理用户的认证凭证信息。
 *
 * 应用场景：
 * 1. 创建新用户的SCRAM认证凭证时，指定认证机制和迭代次数
 * 2. 更新现有用户的认证凭证配置
 * 3. 查询用户的认证机制信息
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class ScramCredentialInfo {
    /**
     * SCRAM认证机制类型，如SCRAM-SHA-256或SCRAM-SHA-512
     * 这是一个不可变字段，在创建后不能修改
     */
    private final ScramMechanism mechanism;

    /**
     * 创建凭证时使用的迭代次数
     * 迭代次数越高，暴力破解的难度就越大，但服务器端的计算开销也越大
     * 这是一个不可变字段，在创建后不能修改
     */
    private final int iterations;

    /**
     * 创建一个新的SCRAM凭证信息实例
     *
     * @param mechanism 必需的SCRAM认证机制类型，不能为null
     * @param iterations 创建凭证时使用的迭代次数，用于增加密码哈希的计算复杂度
     */
    public ScramCredentialInfo(ScramMechanism mechanism, int iterations) {
        // 确保mechanism参数不为null，否则抛出NullPointerException
        this.mechanism = Objects.requireNonNull(mechanism);
        this.iterations = iterations;
    }

    /**
     * 获取SCRAM认证机制类型
     *
     * @return 返回当前凭证使用的SCRAM认证机制类型
     */
    public ScramMechanism mechanism() {
        return mechanism;
    }

    /**
     * 获取凭证创建时使用的迭代次数
     *
     * @return 返回用于创建凭证时的迭代次数，该值影响密码哈希的强度
     */
    public int iterations() {
        return iterations;
    }

    @Override
    public String toString() {
        return "ScramCredentialInfo{" +
                "mechanism=" + mechanism +
                ", iterations=" + iterations +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScramCredentialInfo that = (ScramCredentialInfo) o;
        return iterations == that.iterations &&
                mechanism == that.mechanism;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mechanism, iterations);
    }
}
