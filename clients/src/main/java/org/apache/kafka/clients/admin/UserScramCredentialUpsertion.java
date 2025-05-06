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

import org.apache.kafka.common.security.scram.internals.ScramFormatter;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Objects;

/**
 * A request to update/insert a SASL/SCRAM credential for a user.
 * 用于更新/插入用户的SASL/SCRAM凭证的请求类。
 * 
 * 这个类是Kafka中用于处理SASL/SCRAM安全认证的重要组件之一。
 * 主要应用场景：
 * 1. 创建新用户时，初始化其SCRAM认证凭证
 * 2. 更新现有用户的认证凭证（如密码修改）
 * 3. 在多种认证机制并存时，为用户添加新的认证机制
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class UserScramCredentialUpsertion extends UserScramCredentialAlteration {
    /**
     * SCRAM凭证信息，包含认证机制和迭代次数
     * 这个字段是final的，表示一旦设置就不能修改
     */
    private final ScramCredentialInfo info;

    /**
     * 用于密码哈希的盐值
     * 盐值的作用是增加密码哈希的随机性，防止彩虹表攻击
     */
    private final byte[] salt;

    /**
     * 用户密码的字节数组
     * 使用字节数组存储密码，支持各种字符编码
     */
    private final byte[] password;

    /**
     * 构造函数，会生成随机盐值
     * 这个构造函数接收字符串形式的密码，并将其转换为UTF-8编码的字节数组
     *
     * @param user 要更新/插入凭证的用户名
     * @param credentialInfo 包含认证机制和迭代次数的凭证信息
     * @param password 用户的密码（字符串形式）
     * 
     * 实现细节：
     * 1. 将字符串密码转换为UTF-8编码的字节数组
     * 2. 调用另一个构造函数完成实例化
     */
    public UserScramCredentialUpsertion(String user, ScramCredentialInfo credentialInfo, String password) {
        this(user, credentialInfo, password.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 构造函数，会生成随机盐值
     * 这个构造函数直接接收字节数组形式的密码
     *
     * @param user 要更新/插入凭证的用户名
     * @param credentialInfo 包含认证机制和迭代次数的凭证信息
     * @param password 用户的密码（字节数组形式）
     * 
     * 实现细节：
     * 1. 调用generateRandomSalt()生成随机盐值
     * 2. 调用完整的构造函数完成实例化
     */
    public UserScramCredentialUpsertion(String user, ScramCredentialInfo credentialInfo, byte[] password) {
        this(user, credentialInfo, password, generateRandomSalt());
    }

    /**
     * 完整的构造函数，接受显式指定的盐值
     * 这个构造函数允许外部提供盐值，适用于特殊场景（如凭证迁移）
     *
     * @param user 要更新/插入凭证的用户名
     * @param credentialInfo 包含认证机制和迭代次数的凭证信息
     * @param password 用户的密码（字节数组形式）
     * @param salt 用于密码哈希的盐值
     * 
     * 实现细节：
     * 1. 调用父类构造函数设置用户名
     * 2. 使用Objects.requireNonNull确保所有参数不为null
     * 3. 初始化所有final字段
     */
    public UserScramCredentialUpsertion(String user, ScramCredentialInfo credentialInfo, byte[] password, byte[] salt) {
        super(Objects.requireNonNull(user));
        this.info = Objects.requireNonNull(credentialInfo);
        this.password = Objects.requireNonNull(password);
        this.salt = Objects.requireNonNull(salt);
    }

    /**
     * 获取SCRAM凭证信息
     *
     * @return 返回包含认证机制和迭代次数的凭证信息对象
     */
    public ScramCredentialInfo credentialInfo() {
        return info;
    }

    /**
     * 获取密码哈希使用的盐值
     *
     * @return 返回用于密码哈希的盐值字节数组
     */
    public byte[] salt() {
        return salt;
    }

    /**
     * 获取用户密码
     *
     * @return 返回密码的字节数组
     */
    public byte[] password() {
        return password;
    }

    /**
     * 生成随机盐值的私有静态方法
     * 使用SecureRandom生成加密安全的随机字节序列
     *
     * @return 返回随机生成的盐值字节数组
     * 
     * 实现细节：
     * 1. 创建SecureRandom实例作为安全的随机数生成器
     * 2. 使用ScramFormatter生成随机字节序列
     */
    private static byte[] generateRandomSalt() {
        return ScramFormatter.secureRandomBytes(new SecureRandom());
    }
}
