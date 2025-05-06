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

import java.util.List;
import java.util.Objects;

/**
 * Representation of all SASL/SCRAM credentials associated with a user that can be retrieved, or an exception indicating
 * why credentials could not be retrieved.
 * 表示与用户关联的所有可检索的SASL/SCRAM凭证信息，或者表示无法检索凭证的异常。
 *
 * 应用场景：
 * 1. 用于描述Kafka用户的SCRAM认证凭证信息
 * 2. 在查询用户凭证时返回用户的认证配置详情
 * 3. 作为AdminClient.describeUserScramCredentials()方法的返回结果
 * 
 * 设计考虑：
 * 1. 使用不可变字段确保线程安全
 * 2. 通过List.copyOf确保凭证列表不可修改
 * 3. 提供完整的equals、hashCode实现支持集合操作
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class UserScramCredentialsDescription {
    /**
     * 用户名
     * 这是一个不可变字段，用于标识凭证所属的用户
     */
    private final String name;

    /**
     * 用户的SCRAM凭证信息列表
     * 包含了用户所有的SCRAM认证机制配置信息
     * 这是一个不可变列表，创建后不能修改其内容
     */
    private final List<ScramCredentialInfo> credentialInfos;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserScramCredentialsDescription that = (UserScramCredentialsDescription) o;
        return name.equals(that.name) &&
                credentialInfos.equals(that.credentialInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, credentialInfos);
    }

    @Override
    public String toString() {
        return "UserScramCredentialsDescription{" +
                "name='" + name + '\'' +
                ", credentialInfos=" + credentialInfos +
                '}';
    }

    /**
     * 创建用户SCRAM凭证描述信息的构造函数
     *
     * @param name 必需的用户名，用于标识凭证所属的用户
     * @param credentialInfos 用户的SASL/SCRAM凭证信息列表，包含认证机制和配置
     * 
     * 实现细节：
     * 1. 使用Objects.requireNonNull确保用户名不为null
     * 2. 使用List.copyOf创建不可修改的凭证列表副本
     */
    public UserScramCredentialsDescription(String name, List<ScramCredentialInfo> credentialInfos) {
        this.name = Objects.requireNonNull(name);
        this.credentialInfos = List.copyOf(credentialInfos);
    }

    /**
     * 获取用户名
     *
     * @return 返回凭证所属的用户名
     * 
     * 实现细节：
     * 直接返回不可变的name字段，确保线程安全
     */
    public String name() {
        return name;
    }

    /**
     * 获取用户的SCRAM凭证信息列表
     *
     * @return 返回用户的SASL/SCRAM凭证信息的不可修改列表，该列表永远不为null
     * 
     * 实现细节：
     * 返回在构造时创建的不可修改列表，确保凭证信息不会被外部修改
     */
    public List<ScramCredentialInfo> credentialInfos() {
        return credentialInfos;
    }
}
