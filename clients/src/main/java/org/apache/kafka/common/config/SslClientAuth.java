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

package org.apache.kafka.common.config;

import java.util.List;
import java.util.Locale;

/**
 * SSL客户端认证枚举类
 * 描述服务器是否应该要求或请求客户端认证。
 * 
 * 应用场景：
 * 1. SSL/TLS配置：配置服务器的客户端认证要求
 * 2. 安全级别控制：定义不同的认证要求级别
 * 3. 配置解析：支持从配置字符串解析认证模式
 * 4. 安全策略实现：实现不同的客户端认证策略
 *
 * 设计考虑：
 * 1. 枚举值：提供三种认证级别选项
 * 2. 配置解析：支持大小写不敏感的配置解析
 * 3. 默认行为：未配置时默认为NONE
 * 4. 字符串表示：统一使用小写形式
 */
public enum SslClientAuth {
    /**
     * 必需的客户端认证
     * 服务器要求客户端必须提供有效的证书
     */
    REQUIRED,

    /**
     * 请求的客户端认证
     * 服务器请求但不强制要求客户端提供证书
     */
    REQUESTED,

    /**
     * 无客户端认证
     * 服务器不要求客户端提供证书
     */
    NONE;

    /**
     * 所有SSL客户端认证模式的列表
     * 使用不可变列表存储所有枚举值
     */
    public static final List<SslClientAuth> VALUES = List.of(SslClientAuth.values());

    /**
     * 从配置字符串解析SSL客户端认证模式
     * 
     * 实现说明：
     * - 支持大小写不敏感的配置值
     * - null值默认返回NONE
     * - 无效值返回null
     *
     * @param key 配置字符串
     * @return 对应的SSL客户端认证模式，如果配置无效则返回null
     */
    public static SslClientAuth forConfig(String key) {
        // 如果配置键为null，返回NONE模式
        if (key == null) {
            return SslClientAuth.NONE;
        }
        // 将配置键转换为大写以进行大小写不敏感的比较
        String upperCaseKey = key.toUpperCase(Locale.ROOT);
        // 遍历所有认证模式
        for (SslClientAuth auth : VALUES) {
            // 比较枚举名称和配置键
            if (auth.name().equals(upperCaseKey)) {
                return auth;
            }
        }
        // 如果没有找到匹配的模式，返回null
        return null;
    }

    /**
     * 返回认证模式的字符串表示
     * 统一使用小写形式
     *
     * @return 小写形式的认证模式名称
     */
    @Override
    public String toString() {
        // 将枚举名称转换为小写
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
