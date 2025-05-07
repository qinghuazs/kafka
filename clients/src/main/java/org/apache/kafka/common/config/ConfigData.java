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

import org.apache.kafka.common.config.provider.ConfigProvider;

import java.util.Map;

/**
 * Configuration data from a {@link ConfigProvider}.
 */
/**
 * 配置数据容器类
 * 用于存储从{@link ConfigProvider}获取的配置数据。
 * 
 * 应用场景：
 * 1. 配置数据传输：在系统组件间传递配置信息
 * 2. 缓存管理：支持配置数据的TTL（存活时间）控制
 * 3. 动态配置：存储可能随时间变化的配置数据
 * 4. 分布式配置：在分布式系统中同步配置信息
 *
 * 设计考虑：
 * 1. 不可变性：使用final确保数据和TTL不可修改
 * 2. 灵活性：支持有TTL和无TTL两种配置模式
 * 3. 简单性：仅包含必要的数据结构和访问方法
 * 4. 类型安全：使用泛型Map确保类型安全
 */
public class ConfigData {

    /**
     * 配置数据映射
     * 存储键值对形式的配置数据
     * final修饰确保引用不可变
     */
    private final Map<String, String> data;

    /**
     * 配置数据的存活时间（TTL）
     * 以毫秒为单位，null表示永不过期
     * final修饰确保引用不可变
     */
    private final Long ttl;

    /**
     * 创建带TTL的配置数据对象
     * 
     * 实现说明：
     * - 存储配置数据映射
     * - 设置配置数据的存活时间
     * - 通过final字段确保不可变性
     *
     * @param data 键值对形式的配置数据映射
     * @param ttl 配置数据的存活时间（毫秒），null表示永不过期
     */
    public ConfigData(Map<String, String> data, Long ttl) {
        // 存储配置数据映射
        this.data = data;
        // 设置TTL值
        this.ttl = ttl;
    }

    /**
     * 创建无TTL的配置数据对象
     * 配置数据永不过期
     * 
     * 实现说明：
     * - 调用主构造函数
     * - TTL设置为null表示永不过期
     *
     * @param data 键值对形式的配置数据映射
     */
    public ConfigData(Map<String, String> data) {
        // 调用主构造函数，TTL设为null
        this(data, null);
    }

    /**
     * 获取配置数据映射
     * 
     * 实现说明：
     * - 直接返回数据映射引用
     * - 由于Map是final的，保证了一定程度的不可变性
     *
     * @return 键值对形式的配置数据映射
     */
    public Map<String, String> data() {
        // 返回配置数据映射
        return data;
    }

    /**
     * 获取配置数据的TTL值
     * 
     * 实现说明：
     * - 返回TTL值（毫秒）
     * - null表示配置永不过期
     *
     * @return 配置数据的存活时间（毫秒），null表示永不过期
     */
    public Long ttl() {
        // 返回TTL值
        return ttl;
    }
}
