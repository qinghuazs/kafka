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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 配置资源类
 * 表示具有配置的资源。
 * 
 * 应用场景：
 * 1. 资源管理：统一管理Kafka中的可配置资源
 * 2. 配置标识：唯一标识不同类型的配置资源
 * 3. 资源分类：对不同类型的资源进行分类管理
 * 4. 默认资源处理：支持资源类型的默认配置
 *
 * 设计考虑：
 * 1. 不可变性：使用final确保类和字段不可变
 * 2. 类型安全：使用枚举定义资源类型
 * 3. 空值保护：构造函数进行非空检查
 * 4. 标识唯一：通过类型和名称唯一标识资源
 */
public final class ConfigResource {

    /**
     * 资源类型枚举
     * 定义了所有支持的资源类型
     */
    public enum Type {
        /** 消费者组配置 */
        GROUP((byte) 32),
        /** 客户端度量配置 */
        CLIENT_METRICS((byte) 16),
        /** Broker日志配置 */
        BROKER_LOGGER((byte) 8),
        /** Broker配置 */
        BROKER((byte) 4),
        /** 主题配置 */
        TOPIC((byte) 2),
        /** 未知类型 */
        UNKNOWN((byte) 0);

        /**
         * 类型ID映射表
         * 用于快速通过ID查找对应的类型
         * 使用不可变Map存储，确保线程安全
         */
        private static final Map<Byte, Type> TYPES = Collections.unmodifiableMap(
            // 使用Stream API构建类型映射
            Arrays.stream(values()).collect(Collectors.toMap(Type::id, Function.identity()))
        );

        /**
         * 类型ID
         * 用字节表示资源类型的唯一标识
         */
        private final byte id;

        /**
         * 构造函数
         * 初始化资源类型的ID
         *
         * @param id 类型ID
         */
        Type(final byte id) {
            this.id = id;
        }

        /**
         * 获取类型ID
         *
         * @return 类型ID
         */
        public byte id() {
            return id;
        }

        /**
         * 通过ID查找对应的类型
         * 如果找不到对应类型则返回UNKNOWN
         *
         * @param id 类型ID
         * @return 对应的资源类型，如果未找到则返回UNKNOWN
         */
        public static Type forId(final byte id) {
            // 从映射表中查找类型，如果未找到则返回UNKNOWN
            return TYPES.getOrDefault(id, UNKNOWN);
        }
    }

    /**
     * 资源类型
     * final修饰确保不可变
     */
    private final Type type;

    /**
     * 资源名称
     * final修饰确保不可变
     */
    private final String name;

    /**
     * 创建配置资源实例
     * 
     * 实现说明：
     * - 验证参数非空
     * - 初始化不可变字段
     *
     * @param type 资源类型，不能为null
     * @param name 资源名称，不能为null
     * @throws NullPointerException 如果type或name为null
     */
    public ConfigResource(Type type, String name) {
        // 检查type参数非空
        Objects.requireNonNull(type, "type should not be null");
        // 检查name参数非空
        Objects.requireNonNull(name, "name should not be null");
        // 初始化字段
        this.type = type;
        this.name = name;
    }

    /**
     * 获取资源类型
     *
     * @return 资源类型
     */
    public Type type() {
        return type;
    }

    /**
     * 获取资源名称
     *
     * @return 资源名称
     */
    public String name() {
        return name;
    }

    /**
     * 判断是否为默认资源
     * 资源名称为空表示默认资源
     *
     * @return 如果是默认资源则返回true
     */
    public boolean isDefault() {
        // 通过检查名称是否为空判断是否为默认资源
        return name.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConfigResource that = (ConfigResource) o;

        return type == that.type && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConfigResource(type=" + type + ", name='" + name + "')";
    }
}
