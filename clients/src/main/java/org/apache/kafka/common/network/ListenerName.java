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
package org.apache.kafka.common.network;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.Locale;
import java.util.Objects;

/**
 * Kafka网络监听器名称类，用于管理和标识不同的网络监听器配置。
 * 该类是不可变的(final)，主要用于：
 * 1. 处理安全协议相关的监听器配置
 * 2. 规范化监听器名称（统一大小写）
 * 3. 生成配置前缀，特别是在SASL机制配置中
 */
public final class ListenerName {

    /**
     * 监听器配置的静态前缀
     * 用于构建配置项的完整键名，如："listener.name.{listenerName}.{saslMechanism}"
     */
    private static final String CONFIG_STATIC_PREFIX = "listener.name";

    /**
     * 使用安全协议名称创建ListenerName实例
     * 
     * @param securityProtocol 安全协议对象，其name属性将作为监听器名称
     * @return 新的ListenerName实例
     */
    public static ListenerName forSecurityProtocol(SecurityProtocol securityProtocol) {
        // 直接使用安全协议的名称作为监听器名称
        return new ListenerName(securityProtocol.name);
    }

    /**
     * 创建规范化的ListenerName实例
     * 将输入值转换为大写以确保名称的一致性
     * 
     * @param value 监听器名称
     * @return 规范化的ListenerName实例
     * @throws ConfigException 如果输入值为null或空字符串
     */
    public static ListenerName normalised(String value) {
        // 检查输入值是否为空
        if (Utils.isBlank(value)) {
            throw new ConfigException("The provided listener name is null or empty string");
        }
        // 转换为大写并创建新实例
        return new ListenerName(value.toUpperCase(Locale.ROOT));
    }

    /**
     * 存储监听器名称的值
     * 一旦设置不可更改，确保线程安全
     */
    private final String value;

    /**
     * 构造函数
     * 
     * @param value 监听器名称
     * @throws NullPointerException 如果value为null
     */
    public ListenerName(String value) {
        Objects.requireNonNull(value, "value should not be null");
        this.value = value;
    }

    /**
     * 获取监听器名称
     * 
     * @return 监听器名称字符串
     */
    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ListenerName))
            return false;
        ListenerName that = (ListenerName) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "ListenerName(" + value + ")";
    }

    /**
     * 生成监听器配置的前缀
     * 用于构建特定监听器的配置项键名
     * 
     * @return 配置前缀，格式为："listener.name.{listenerName}."
     */
    public String configPrefix() {
        // 将监听器名称转换为小写，确保配置键的一致性
        return CONFIG_STATIC_PREFIX + "." + value.toLowerCase(Locale.ROOT) + ".";
    }

    /**
     * 生成特定SASL机制的配置前缀
     * 
     * @param saslMechanism SASL认证机制名称
     * @return 完整的配置前缀，格式为："listener.name.{listenerName}.{saslMechanism}."
     */
    public String saslMechanismConfigPrefix(String saslMechanism) {
        // 组合监听器配置前缀和SASL机制前缀
        return configPrefix() + saslMechanismPrefix(saslMechanism);
    }

    /**
     * 生成SASL机制的标准前缀
     * 
     * @param saslMechanism SASL认证机制名称
     * @return SASL机制的标准前缀（小写）
     */
    public static String saslMechanismPrefix(String saslMechanism) {
        // 将SASL机制名称转换为小写，确保配置键的一致性
        return saslMechanism.toLowerCase(Locale.ROOT) + ".";
    }
}
