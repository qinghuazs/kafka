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

import java.util.Arrays;

/**
 * SASL/SCRAM认证机制的枚举表示。
 * 
 * 应用场景：
 * 1. 在Kafka的安全认证过程中，用于标识和管理不同的SCRAM认证机制
 * 2. 在创建或更新用户凭证时，指定使用的SCRAM机制类型
 * 3. 在认证过程中，用于验证客户端和服务器之间的认证机制匹配
 * 
 * 设计考虑：
 * 1. 使用枚举确保类型安全，避免使用字符串导致的错误
 * 2. 提供类型标识符和机制名称的双向映射
 * 3. 保持与内部实现的一致性
 * 
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 *
 * 注意：此代码在org.apache.kafka.common.security.scram.internals.ScramMechanism中有副本。
 * type字段在两个文件中必须匹配且不能更改。type字段用于传递ScramCredentialUpsertion和
 * 内部UserScramCredentialRecord。请勿更改type字段。
 */
public enum ScramMechanism {
    // 未知的SCRAM机制类型，用于处理异常情况
    UNKNOWN((byte) 0),
    // SHA-256哈希算法的SCRAM机制
    SCRAM_SHA_256((byte) 1),
    // SHA-512哈希算法的SCRAM机制，提供更强的安全性
    SCRAM_SHA_512((byte) 2);

    // 缓存所有枚举值，提高查找性能
    private static final ScramMechanism[] VALUES = values();

    /**
     * 根据类型标识符查找对应的SCRAM机制实例
     * 
     * @param type SCRAM机制的类型标识符
     * @return 返回对应的SCRAM机制实例，如果未找到则返回{@link #UNKNOWN}
     * 
     * 实现细节：
     * 1. 遍历缓存的VALUES数组，避免重复创建枚举值
     * 2. 通过类型标识符精确匹配
     * 3. 找不到匹配项时返回UNKNOWN，确保安全处理
     */
    public static ScramMechanism fromType(byte type) {
        // 遍历所有SCRAM机制类型
        for (ScramMechanism scramMechanism : VALUES) {
            // 找到匹配的类型标识符
            if (scramMechanism.type == type) {
                return scramMechanism;
            }
        }
        // 未找到匹配项，返回UNKNOWN
        return UNKNOWN;
    }

    /**
     * 根据机制名称查找对应的SCRAM机制实例
     * 
     * @param mechanismName SASL SCRAM机制名称（如"SCRAM-SHA-256"）
     * @return 返回对应的SCRAM机制枚举实例，如果未找到则返回{@link #UNKNOWN}
     * @see <a href="https://tools.ietf.org/html/rfc5802#section-4">
     *     Salted Challenge Response Authentication Mechanism (SCRAM) SASL and GSS-API Mechanisms, Section 4</a>
     * 
     * 实现细节：
     * 1. 使用Java 8 Stream API进行函数式处理
     * 2. 通过机制名称进行精确匹配
     * 3. 使用Optional处理可能的空值情况
     */
    public static ScramMechanism fromMechanismName(String mechanismName) {
        // 使用Stream API查找匹配的机制名称
        return Arrays.stream(VALUES)
            // 过滤出匹配的机制名称
            .filter(mechanism -> mechanism.mechanismName.equals(mechanismName))
            // 获取第一个匹配项
            .findFirst()
            // 如果未找到则返回UNKNOWN
            .orElse(UNKNOWN);
    }

    /**
     * 获取SCRAM机制的标准名称
     * 
     * @return 返回标准的SASL SCRAM机制名称（如"SCRAM-SHA-256"）
     * @see <a href="https://tools.ietf.org/html/rfc5802#section-4">
     *     Salted Challenge Response Authentication Mechanism (SCRAM) SASL and GSS-API Mechanisms, Section 4</a>
     */
    public String mechanismName() {
        return this.mechanismName;
    }

    /**
     * 获取SCRAM机制的类型标识符
     * 
     * @return 返回用于标识SCRAM机制的字节类型值
     */
    public byte type() {
        return this.type;
    }

    // SCRAM机制的类型标识符，用于内部存储和传输
    private final byte type;
    // SCRAM机制的标准名称，用于外部显示和匹配
    private final String mechanismName;

    /**
     * 构造函数
     * 
     * @param type SCRAM机制的类型标识符
     * 
     * 实现细节：
     * 1. 将枚举名称中的下划线替换为连字符，符合SCRAM机制的命名规范
     * 2. 使用final字段确保不可变性
     */
    ScramMechanism(byte type) {
        this.type = type;
        // 将枚举名称转换为标准的机制名称格式
        this.mechanismName = toString().replace('_', '-');
    }
}
