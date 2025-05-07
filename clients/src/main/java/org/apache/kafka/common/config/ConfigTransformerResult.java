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
 * 配置转换结果类
 * 存储来自{@link ConfigTransformer}的转换结果。
 * 
 * 应用场景：
 * 1. 配置转换：存储配置变量替换后的结果
 * 2. TTL管理：管理不同路径的配置过期时间
 * 3. 数据传输：在系统组件间传递转换后的配置
 * 4. 缓存控制：通过TTL实现配置缓存管理
 *
 * 设计考虑：
 * 1. 不可变性：使用final确保数据不可修改
 * 2. 数据封装：提供受控的数据访问方法
 * 3. 分离关注：区分配置数据和TTL管理
 * 4. 类型安全：使用泛型确保类型安全
 */
public class ConfigTransformerResult {

    /**
     * TTL映射
     * 存储路径到其TTL值（毫秒）的映射
     * final修饰确保引用不可变
     */
    private final Map<String, Long> ttls;

    /**
     * 配置数据映射
     * 存储转换后的配置键值对
     * final修饰确保引用不可变
     */
    private final Map<String, String> data;

    /**
     * 创建配置转换结果实例
     * 
     * 实现说明：
     * - 存储转换后的配置数据
     * - 存储路径对应的TTL值
     * - 通过final字段确保不可变性
     *
     * @param data 键值对形式的配置数据
     * @param ttls 路径到TTL值（毫秒）的映射
     */
    public ConfigTransformerResult(Map<String, String> data, Map<String, Long> ttls) {
        // 存储配置数据映射
        this.data = data;
        // 存储TTL映射
        this.ttls = ttls;
    }

    /**
     * 获取转换后的配置数据
     * 返回变量已被替换为对应ConfigProvider实例值的配置数据。
     * 
     * 实现说明：
     * - 直接返回数据映射引用
     * - 修改返回的数据不会影响ConfigProvider
     * - 也不会影响原始的转换源数据
     *
     * @return 键值对形式的配置数据
     */
    public Map<String, String> data() {
        // 返回配置数据映射
        return data;
    }

    /**
     * 获取TTL值映射
     * 返回从ConfigProvider实例获取的路径TTL值（毫秒）。
     * 
     * 实现说明：
     * - 直接返回TTL映射引用
     * - TTL用于控制配置的有效期
     *
     * @return 路径到TTL值的映射
     */
    public Map<String, Long> ttls() {
        // 返回TTL映射
        return ttls;
    }
}
