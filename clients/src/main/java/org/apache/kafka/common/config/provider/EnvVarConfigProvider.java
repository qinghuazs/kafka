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
package org.apache.kafka.common.config.provider;

import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 基于环境变量的配置提供者实现
 * 键对应环境变量的名称，当前不使用路径参数。
 * 通过使用允许列表模式 {@link EnvVarConfigProvider#ALLOWLIST_PATTERN_CONFIG}（支持正则表达式），
 * 可以限制对特定环境变量的访问。默认的允许列表模式是".*"。
 * 
 * 应用场景：
 * 1. 环境变量配置管理：从系统环境变量读取配置
 * 2. 敏感信息管理：通过环境变量存储敏感配置
 * 3. 动态配置：支持运行时环境变量更新
 * 4. 安全访问控制：通过正则表达式限制环境变量访问
 *
 * 设计考虑：
 * 1. 安全性：支持访问控制模式
 * 2. 灵活性：支持正则表达式匹配
 * 3. 容错性：优雅处理异常情况
 * 4. 资源管理：自动关闭资源
 */
public class EnvVarConfigProvider implements ConfigProvider {

    /**
     * 日志记录器
     * 用于记录配置访问和错误信息
     */
    private static final Logger log = LoggerFactory.getLogger(EnvVarConfigProvider.class);

    /**
     * 允许列表模式配置键
     * 用于指定环境变量访问的正则表达式模式
     */
    public static final String ALLOWLIST_PATTERN_CONFIG = "allowlist.pattern";

    /**
     * 允许列表模式配置的文档说明
     * 描述了用于匹配环境变量的模式/正则表达式
     */
    public static final String ALLOWLIST_PATTERN_CONFIG_DOC = "A pattern / regular expression that needs to match for environment variables" +
            " to be used by this config provider.";

    /**
     * 原始环境变量映射
     * 存储所有系统环境变量
     */
    private final Map<String, String> envVarMap;

    /**
     * 经过过滤的环境变量映射
     * 存储符合允许列表模式的环境变量
     */
    private Map<String, String> filteredEnvVarMap;

    /**
     * 默认构造函数
     * 从系统获取环境变量
     */
    public EnvVarConfigProvider() {
        // 通过getEnvVars方法获取系统环境变量
        envVarMap = getEnvVars();
    }

    /**
     * 带参数构造函数
     * 用于测试场景，接受预定义的环境变量映射
     *
     * @param envVarsAsArgument 预定义的环境变量映射
     */
    public EnvVarConfigProvider(Map<String, String> envVarsAsArgument) {
        // 直接使用提供的环境变量映射
        envVarMap = envVarsAsArgument;
    }

    /**
     * 配置提供者
     * 设置并应用环境变量访问控制模式
     *
     * @param configs 配置映射
     */
    @Override
    public void configure(Map<String, ?> configs) {
        Pattern envVarPattern;

        // 检查是否提供了允许列表模式
        if (configs.containsKey(ALLOWLIST_PATTERN_CONFIG)) {
            // 编译用户提供的正则表达式模式
            envVarPattern = Pattern.compile(
                    String.valueOf(configs.get(ALLOWLIST_PATTERN_CONFIG))
            );
        } else {
            // 使用默认模式".*"（匹配所有环境变量）
            envVarPattern = Pattern.compile(".*");
            log.info("No pattern for environment variables provided. Using default pattern '(.*)'.");
        }

        // 过滤环境变量，只保留匹配模式的变量
        filteredEnvVarMap = envVarMap.entrySet().stream()
                .filter(envVar -> envVarPattern.matcher(envVar.getKey()).matches())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                );
    }

    /**
     * 关闭提供者
     * 当前实现为空，因为没有需要清理的资源
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * 获取所有允许的环境变量配置
     * 
     * @param path 未使用的路径参数
     * @return 配置数据对象
     */
    @Override
    public ConfigData get(String path) {
        // 调用重载方法，传入null表示获取所有允许的环境变量
        return get(path, null);
    }

    /**
     * 获取指定键的环境变量配置
     * 
     * @param path 未使用的路径参数
     * @param keys 要获取的环境变量名称集合
     * @return 配置数据对象
     * @throws ConfigException 如果提供了非空路径
     */
    @Override
    public ConfigData get(String path, Set<String> keys) {
        // 检查路径参数（不支持路径）
        if (path != null && !path.isEmpty()) {
            log.error("Path is not supported for EnvVarConfigProvider, invalid value '{}'", path);
            throw new ConfigException("Path is not supported for EnvVarConfigProvider, invalid value '" + path + "'");
        }

        // 如果未指定键，返回所有过滤后的环境变量
        if (keys == null) {
            return new ConfigData(filteredEnvVarMap);
        }

        // 创建新的映射并只保留指定的键
        Map<String, String> filteredData = new HashMap<>(filteredEnvVarMap);
        filteredData.keySet().retainAll(keys);

        return new ConfigData(filteredData);
    }

    /**
     * 获取系统环境变量
     * 
     * @return 环境变量映射
     * @throws ConfigException 如果无法读取环境变量
     */
    private Map<String, String> getEnvVars() {
        try {
            // 获取系统环境变量
            return System.getenv();
        } catch (Exception e) {
            // 记录错误并抛出异常
            log.error("Could not read environment variables", e);
            throw new ConfigException("Could not read environment variables");
        }
    }
}
