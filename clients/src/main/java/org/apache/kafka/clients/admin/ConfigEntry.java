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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 表示一个包含名称、值和额外元数据的配置项。
 * 这个类用于管理Kafka中的各种配置，包括主题配置、broker配置等。
 * 每个配置项都可以有不同的来源、敏感性、只读性等属性。
 *
 * 应用场景：
 * 1. 用于管理Kafka集群中的各种配置项
 * 2. 支持动态配置更新和查询
 * 3. 处理敏感配置信息的保护
 * 4. 维护配置的层次结构（通过同义词机制）
 *
 * 该类的API仍在演进中，详细信息请参考{@link Admin}。
 */
@InterfaceStability.Evolving
public class ConfigEntry {

    /**
     * 配置项的名称
     * 用于唯一标识一个配置项
     */
    private final String name;

    /**
     * 配置项的值
     * 如果配置项是敏感的，该值可能为null
     */
    private final String value;

    /**
     * 配置项的来源
     * 表示该配置是来自默认值、动态配置还是静态配置文件等
     */
    private final ConfigSource source;

    /**
     * 是否是敏感配置
     * 如果为true，broker在返回配置时会将值设置为null
     */
    private final boolean isSensitive;

    /**
     * 是否是只读配置
     * 如果为true，表示该配置不能被修改
     */
    private final boolean isReadOnly;

    /**
     * 配置的同义词列表
     * 按优先级排序，用于支持配置的层次结构
     */
    private final List<ConfigSynonym> synonyms;

    /**
     * 配置值的数据类型
     */
    private final ConfigType type;

    /**
     * 配置项的文档说明
     */
    private final String documentation;

    /**
     * 创建一个基本的配置项
     * 使用默认值初始化其他属性：未知来源、非敏感、非只读、无同义词
     *
     * @param name 配置项名称，不能为null
     * @param value 配置项的值，可以为null
     */
    public ConfigEntry(String name, String value) {
        this(name, value, ConfigSource.UNKNOWN, false, false,
            Collections.emptyList(), ConfigType.UNKNOWN, null);
    }

    /**
     * 创建一个完整的配置项，包含所有属性
     *
     * @param name 配置项名称，不能为null
     * @param value 配置项的值，可以为null
     * @param source 配置项的来源
     * @param isSensitive 是否是敏感配置，如果为true，broker永远不会返回实际值
     * @param isReadOnly 是否是只读配置，如果为true，则不能更新
     * @param synonyms 按优先级排序的同义词配置列表
     * @param type 配置值的数据类型
     * @param documentation 配置的文档说明
     */
    public ConfigEntry(String name,
            String value,
            ConfigSource source,
            boolean isSensitive,
            boolean isReadOnly,
            List<ConfigSynonym> synonyms,
            ConfigType type,
            String documentation) {
        // 确保name不为null，这是配置项的唯一标识
        Objects.requireNonNull(name, "name should not be null");
        this.name = name;
        this.value = value;
        this.source = source;
        this.isSensitive = isSensitive;
        this.isReadOnly = isReadOnly;
        this.synonyms = synonyms;
        this.type = type;
        this.documentation = documentation;
    }

    /**
     * 获取配置项的名称
     * 
     * @return 配置项的名称，永远不会为null
     */
    public String name() {
        return name;
    }

    /**
     * 获取配置项的值
     * 
     * @return 配置项的值，如果配置未设置或者是敏感配置，则返回null
     */
    public String value() {
        return value;
    }

    /**
     * 获取配置项的来源
     * 
     * @return 配置项的来源，表示配置的生效级别和方式
     */
    public ConfigSource source() {
        return source;
    }

    /**
     * 判断配置项是否使用默认值
     * 
     * @return 如果配置使用默认值返回true，否则返回false
     */
    public boolean isDefault() {
        return source == ConfigSource.DEFAULT_CONFIG;
    }

    /**
     * 判断配置项是否敏感
     * 敏感配置的值在broker返回时会被设置为null
     * 
     * @return 如果是敏感配置返回true，否则返回false
     */
    public boolean isSensitive() {
        return isSensitive;
    }

    /**
     * 判断配置项是否只读
     * 
     * @return 如果是只读配置返回true，否则返回false
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * 获取配置项的同义词列表
     * 同义词按优先级排序，列表中的第一个值就是当前ConfigEntry中的值
     * 如果没有通过{@link DescribeConfigsOptions#includeSynonyms(boolean)}请求同义词，则返回空列表
     * 
     * @return 配置项的同义词列表
     */
    public List<ConfigSynonym> synonyms() {
        return  synonyms;
    }

    /**
     * 获取配置项的数据类型
     * 
     * @return 配置值的数据类型
     */
    public ConfigType type() {
        return type;
    }

    /**
     * 获取配置项的文档说明
     * 
     * @return 配置的文档说明
     */
    public String documentation() {
        return documentation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConfigEntry that = (ConfigEntry) o;

        return this.name.equals(that.name) &&
                Objects.equals(this.value, that.value) &&
                this.isSensitive == that.isSensitive &&
                this.isReadOnly == that.isReadOnly &&
                Objects.equals(this.source, that.source) &&
                Objects.equals(this.synonyms, that.synonyms) &&
                Objects.equals(this.type, that.type) &&
                Objects.equals(this.documentation, that.documentation);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
        result = prime * result + Objects.hashCode(value);
        result = prime * result + (isSensitive ? 1 : 0);
        result = prime * result + (isReadOnly ? 1 : 0);
        result = prime * result + Objects.hashCode(source);
        result = prime * result + Objects.hashCode(synonyms);
        result = prime * result + Objects.hashCode(type);
        result = prime * result + Objects.hashCode(documentation);
        return result;
    }

    /**
     * Override toString to redact sensitive value.
     * WARNING, user should be responsible to set the correct "isSensitive" field for each config entry.
     */
    @Override
    public String toString() {
        return "ConfigEntry(" +
                "name=" + name +
                ", value=" + (isSensitive ? "Redacted" : value) +
                ", source=" + source +
                ", isSensitive=" + isSensitive +
                ", isReadOnly=" + isReadOnly +
                ", synonyms=" + synonyms +
                ", type=" + type +
                ", documentation=" + documentation +
                ")";
    }

    /**
     * 配置项的数据类型
     * 用于标识配置值的具体类型，帮助进行类型检查和转换
     */
    public enum ConfigType {
        /** 未知类型 */
        UNKNOWN,
        /** 布尔类型 */
        BOOLEAN,
        /** 字符串类型 */
        STRING,
        /** 整数类型 */
        INT,
        /** 短整数类型 */
        SHORT,
        /** 长整数类型 */
        LONG,
        /** 双精度浮点数类型 */
        DOUBLE,
        /** 列表类型 */
        LIST,
        /** 类类型，用于指定实现类 */
        CLASS,
        /** 密码类型，通常作为敏感配置处理 */
        PASSWORD
    }

    /**
     * 配置项的来源
     * 表示配置的生效范围和更新方式
     */
    public enum ConfigSource {
        /** 动态主题配置：为特定主题配置的动态配置 */
        DYNAMIC_TOPIC_CONFIG,
        /** 动态Broker日志配置：为特定Broker配置的动态日志配置 */
        DYNAMIC_BROKER_LOGGER_CONFIG,
        /** 动态Broker配置：为特定Broker配置的动态配置 */
        DYNAMIC_BROKER_CONFIG,
        /** 动态默认Broker配置：为集群中所有Broker配置的默认动态配置 */
        DYNAMIC_DEFAULT_BROKER_CONFIG,
        /** 动态客户端度量配置：为所有客户端配置的动态度量订阅配置 */
        DYNAMIC_CLIENT_METRICS_CONFIG,
        /** 动态消费者组配置：为特定消费者组配置的动态配置 */
        DYNAMIC_GROUP_CONFIG,
        /** 静态Broker配置：在Broker启动时通过配置文件提供的静态配置（如server.properties文件） */
        STATIC_BROKER_CONFIG,
        /** 默认配置：具有默认值的内置配置 */
        DEFAULT_CONFIG,
        /** 未知来源：例如在用于修改请求的ConfigEntry中，未设置来源时使用 */
        UNKNOWN
    }

    /**
     * 表示{@link ConfigEntry}的同义词配置
     * 用于支持配置的层次结构和优先级机制
     */
    public static class ConfigSynonym {

        /**
         * 同义词配置的名称
         * 可能与关联的{@link ConfigEntry}的名称不同
         */
        private final String name;

        /**
         * 同义词配置的值
         * 如果配置是敏感的，该值可能为null
         */
        private final String value;

        /**
         * 同义词配置的来源
         * 用于确定配置的优先级
         */
        private final ConfigSource source;

        /**
         * 创建一个配置同义词
         *
         * @param name 配置名称（可能与关联的{@link ConfigEntry}的名称不同）
         * @param value 配置值
         * @param source 配置的来源{@link ConfigSource}
         */
        ConfigSynonym(String name, String value, ConfigSource source) {
            this.name = name;
            this.value = value;
            this.source = source;
        }

        /**
         * 获取同义词配置的名称
         * 
         * @return 配置名称
         */
        public String name() {
            return name;
        }

        /**
         * 获取同义词配置的值
         * 如果配置是敏感的，可能返回null
         * 
         * @return 配置值
         */
        public String value() {
            return value;
        }

        /**
         * 获取同义词配置的来源
         * 
         * @return 配置来源
         */
        public ConfigSource source() {
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConfigSynonym that = (ConfigSynonym) o;
            return Objects.equals(name, that.name) && Objects.equals(value, that.value) && source == that.source;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value, source);
        }

        @Override
        public String toString() {
            return "ConfigSynonym(" +
                    "name=" + name +
                    ", value=" + value +
                    ", source=" + source +
                    ")";
        }
    }
}
