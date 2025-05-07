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

import org.apache.kafka.common.KafkaException;

/**
 * Thrown if the user supplies an invalid configuration
 */
/**
 * 配置异常类
 * 当用户提供了无效的配置时抛出此异常。
 * 
 * 应用场景：
 * 1. 配置验证：验证用户提供的配置值是否有效
 * 2. 参数检查：检查配置参数的合法性
 * 3. 错误提示：提供详细的配置错误信息
 * 4. 异常处理：统一处理配置相关的异常情况
 *
 * 设计考虑：
 * 1. 继承性：继承自KafkaException以统一异常处理
 * 2. 序列化：支持异常的序列化
 * 3. 信息完整：提供详细的错误信息
 * 4. 灵活构造：支持多种构造方式
 */
public class ConfigException extends KafkaException {

    /**
     * 序列化版本ID
     * 用于确保序列化的兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用错误消息构造配置异常
     * 
     * 实现说明：
     * - 直接传递错误消息给父类构造函数
     *
     * @param message 错误消息
     */
    public ConfigException(String message) {
        // 调用父类构造函数，传入错误消息
        super(message);
    }

    /**
     * 使用配置名称和值构造配置异常
     * 
     * 实现说明：
     * - 调用三参数构造函数，message参数为null
     *
     * @param name 配置名称
     * @param value 配置值
     */
    public ConfigException(String name, Object value) {
        // 调用三参数构造函数，message为null
        this(name, value, null);
    }

    /**
     * 使用配置名称、值和附加消息构造配置异常
     * 
     * 实现说明：
     * - 构造完整的错误消息字符串
     * - 包含配置名称、值和可选的附加消息
     * - 使用三元运算符处理可选消息
     *
     * @param name 配置名称
     * @param value 配置值
     * @param message 附加的错误消息，可以为null
     */
    public ConfigException(String name, Object value, String message) {
        // 构造完整的错误消息并传递给父类构造函数
        // 格式：Invalid value {value} for configuration {name}: {message}
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
}
