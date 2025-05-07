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
import org.apache.kafka.common.config.provider.FileConfigProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 配置转换器类
 * 封装了一组{@link ConfigProvider}实例并使用它们执行配置转换。
 * 
 * 应用场景：
 * 1. 配置变量替换：支持配置值中的变量引用
 * 2. 动态配置：从不同配置提供者获取配置值
 * 3. 配置整合：统一管理多个配置源
 * 4. 配置模板化：支持配置模板和变量替换
 *
 * 默认变量模式：${provider:[path:]key}
 * - provider：对应ConfigProvider实例的名称
 * - path：可选的配置路径
 * - key：配置键名
 *
 * 示例：
 * 配置提供者映射包含：
 * - 名称："file"
 * - 实例：{@link FileConfigProvider}
 * 属性文件"/tmp/properties.txt"内容：
 * fileKey=someValue
 * 
 * 输入配置：{"someKey": "${file:/tmp/properties.txt:fileKey}"}
 * 输出配置：{"someKey": "someValue"}
 *
 * 设计考虑：
 * 1. 模式匹配：使用正则表达式解析变量引用
 * 2. 可扩展性：支持多个配置提供者
 * 3. 性能优化：批量处理配置值
 * 4. 错误处理：优雅处理无效引用
 */
public class ConfigTransformer {
    /**
     * 默认变量模式的正则表达式
     * 匹配形如${provider:[path:]key}的模式
     */
    public static final Pattern DEFAULT_PATTERN = Pattern.compile("\\$\\{([^}]*?):(([^}]*?):)?([^}]*?)\\}");

    /**
     * 空路径常量
     * 用于表示没有指定路径的情况
     */
    private static final String EMPTY_PATH = "";

    /**
     * 配置提供者映射
     * 存储提供者名称到实例的映射关系
     */
    private final Map<String, ConfigProvider> configProviders;

    /**
     * 构造函数
     * 创建具有默认模式的配置转换器
     *
     * @param configProviders 提供者名称到实例的映射
     */
    public ConfigTransformer(Map<String, ConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * 转换配置数据
     * 使用配置提供者查找并替换配置值中的变量引用
     *
     * @param configs 要转换的配置值映射
     * @return 转换结果，包含转换后的配置和TTL信息
     */
    public ConfigTransformerResult transform(Map<String, String> configs) {
        // 创建按提供者分组的键集合映射
        Map<String, Map<String, Set<String>>> keysByProvider = new HashMap<>();
        // 创建按提供者分组的查找结果映射
        Map<String, Map<String, Map<String, String>>> lookupsByProvider = new HashMap<>();

        // 收集需要转换的变量
        for (Map.Entry<String, String> config : configs.entrySet()) {
            if (config.getValue() != null) {
                // 解析配置值中的变量引用
                List<ConfigVariable> configVars = getVars(config.getValue(), DEFAULT_PATTERN);
                // 按提供者和路径组织变量
                for (ConfigVariable configVar : configVars) {
                    // 获取或创建提供者的路径映射
                    Map<String, Set<String>> keysByPath = keysByProvider.computeIfAbsent(configVar.providerName, k -> new HashMap<>());
                    // 获取或创建路径的键集合
                    Set<String> keys = keysByPath.computeIfAbsent(configVar.path, k -> new HashSet<>());
                    // 添加变量键
                    keys.add(configVar.variable);
                }
            }
        }

        // 从配置提供者获取请求的变量值
        Map<String, Long> ttls = new HashMap<>();
        for (Map.Entry<String, Map<String, Set<String>>> entry : keysByProvider.entrySet()) {
            String providerName = entry.getKey();
            // 获取配置提供者实例
            ConfigProvider provider = configProviders.get(providerName);
            Map<String, Set<String>> keysByPath = entry.getValue();
            if (provider != null && keysByPath != null) {
                // 处理每个路径的键集合
                for (Map.Entry<String, Set<String>> pathWithKeys : keysByPath.entrySet()) {
                    String path = pathWithKeys.getKey();
                    // 创建键集合的副本
                    Set<String> keys = new HashSet<>(pathWithKeys.getValue());
                    // 获取配置数据
                    ConfigData configData = provider.get(path, keys);
                    Map<String, String> data = configData.data();
                    // 处理TTL信息
                    Long ttl = configData.ttl();
                    if (ttl != null && ttl >= 0) {
                        ttls.put(path, ttl);
                    }
                    // 存储查找结果
                    Map<String, Map<String, String>> keyValuesByPath =
                            lookupsByProvider.computeIfAbsent(providerName, k -> new HashMap<>());
                    keyValuesByPath.put(path, data);
                }
            }
        }

        // 执行变量替换转换
        Map<String, String> data = new HashMap<>(configs);
        for (Map.Entry<String, String> config : configs.entrySet()) {
            // 替换配置值中的变量引用
            data.put(config.getKey(), replace(lookupsByProvider, config.getValue(), DEFAULT_PATTERN));
        }
        return new ConfigTransformerResult(data, ttls);
    }

    /**
     * 获取字符串中的变量引用
     * 解析并返回所有匹配模式的变量
     *
     * @param value 要解析的字符串
     * @param pattern 变量模式
     * @return 变量列表
     */
    private static List<ConfigVariable> getVars(String value, Pattern pattern) {
        List<ConfigVariable> configVars = new ArrayList<>();
        // 创建模式匹配器
        Matcher matcher = pattern.matcher(value);
        // 查找所有匹配
        while (matcher.find()) {
            // 为每个匹配创建变量对象
            configVars.add(new ConfigVariable(matcher));
        }
        return configVars;
    }

    /**
     * 替换字符串中的变量引用
     * 使用查找结果替换所有匹配的变量
     *
     * @param lookupsByProvider 查找结果映射
     * @param value 要处理的字符串
     * @param pattern 变量模式
     * @return 替换后的字符串
     */
    private static String replace(Map<String, Map<String, Map<String, String>>> lookupsByProvider,
                                String value,
                                Pattern pattern) {
        if (value == null) {
            return null;
        }
        // 创建模式匹配器
        Matcher matcher = pattern.matcher(value);
        StringBuilder builder = new StringBuilder();
        int i = 0;
        // 处理每个匹配
        while (matcher.find()) {
            // 解析变量引用
            ConfigVariable configVar = new ConfigVariable(matcher);
            // 获取提供者的查找结果
            Map<String, Map<String, String>> lookupsByPath = lookupsByProvider.get(configVar.providerName);
            if (lookupsByPath != null) {
                // 获取路径的键值映射
                Map<String, String> keyValues = lookupsByPath.get(configVar.path);
                // 获取替换值
                String replacement = keyValues.get(configVar.variable);
                // 添加未匹配部分
                builder.append(value, i, matcher.start());
                if (replacement == null) {
                    // 如果没有替换值，保留原始变量引用
                    builder.append(matcher.group(0));
                } else {
                    // 添加替换值
                    builder.append(replacement);
                }
                i = matcher.end();
            }
        }
        // 添加剩余部分
        builder.append(value, i, value.length());
        return builder.toString();
    }

    /**
     * 配置变量类
     * 表示配置中的变量引用
     */
    private static class ConfigVariable {
        /** 提供者名称 */
        final String providerName;
        /** 配置路径 */
        final String path;
        /** 变量名称 */
        final String variable;

        /**
         * 从匹配结果构造变量对象
         *
         * @param matcher 正则表达式匹配结果
         */
        ConfigVariable(Matcher matcher) {
            // 提取提供者名称（组1）
            this.providerName = matcher.group(1);
            // 提取路径（组3，如果存在）
            this.path = matcher.group(3) != null ? matcher.group(3) : EMPTY_PATH;
            // 提取变量名称（组4）
            this.variable = matcher.group(4);
        }

        @Override
        public String toString() {
            return "(" + providerName + ":" + (path != null ? path + ":" : "") + variable + ")";
        }
    }
}
