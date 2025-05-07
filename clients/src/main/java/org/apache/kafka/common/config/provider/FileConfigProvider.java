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
import org.apache.kafka.common.config.internals.AllowedPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 属性文件配置提供者实现类
 * 代表一个Properties文件的配置提供者实现。
 * 所有的属性键和值都以明文形式存储。
 * 
 * 应用场景：
 * 1. 配置文件管理：从Properties文件读取配置
 * 2. 系统参数配置：管理系统级别的配置项
 * 3. 应用程序设置：存储应用程序的配置信息
 * 4. 多环境配置：支持不同环境的配置文件管理
 *
 * 设计考虑：
 * 1. 安全性：支持路径访问控制
 * 2. 资源管理：自动关闭文件资源
 * 3. 容错处理：优雅处理各种异常情况
 * 4. 灵活性：支持全量和选择性配置获取
 */
public class FileConfigProvider implements ConfigProvider {

    /**
     * 日志记录器
     * 用于记录配置访问和错误信息
     */
    private static final Logger log = LoggerFactory.getLogger(FileConfigProvider.class);

    /**
     * 允许访问路径的配置键
     * 用于指定该配置提供者可以访问的路径列表
     */
    public static final String ALLOWED_PATHS_CONFIG = "allowed.paths";

    /**
     * 允许访问路径配置的文档说明
     * 描述了配置格式和默认行为
     */
    public static final String ALLOWED_PATHS_DOC = "A comma separated list of paths that this config provider is " +
            "allowed to access. If not set, all paths are allowed.";

    /**
     * 允许访问的路径管理器
     * volatile保证多线程可见性
     */
    private volatile AllowedPaths allowedPaths;

    /**
     * 配置该提供者
     * 初始化允许访问的路径列表
     *
     * @param configs 配置映射
     */
    public void configure(Map<String, ?> configs) {
        // 从配置中获取允许路径列表，如果未配置则为null
        allowedPaths = new AllowedPaths((String) configs.getOrDefault(ALLOWED_PATHS_CONFIG, null));
    }

    /**
     * 获取指定Properties文件中的所有配置数据
     * 
     * 实现细节：
     * 1. 验证提供者状态
     * 2. 检查路径合法性
     * 3. 读取并解析Properties文件
     * 4. 处理异常情况
     *
     * @param path 配置文件的路径
     * @return 配置数据对象
     * @throws IllegalStateException 如果提供者未配置
     * @throws ConfigException 如果无法读取配置文件
     */
    public ConfigData get(String path) {
        // 检查提供者是否已配置
        if (allowedPaths == null) {
            throw new IllegalStateException("The provider has not been configured yet.");
        }

        // 创建空的配置映射
        Map<String, String> data = new HashMap<>();
        // 检查路径是否有效
        if (path == null || path.isEmpty()) {
            return new ConfigData(data);
        }

        // 验证路径是否允许访问
        Path filePath = allowedPaths.parseUntrustedPath(path);
        if (filePath == null) {
            // 记录警告并返回空配置
            log.warn("The path {} is not allowed to be accessed", path);
            return new ConfigData(data);
        }

        // 读取并解析Properties文件
        try (Reader reader = reader(filePath)) {
            Properties properties = new Properties();
            // 加载属性文件
            properties.load(reader);
            // 获取所有键的枚举
            Enumeration<Object> keys = properties.keys();
            // 遍历所有键值对
            while (keys.hasMoreElements()) {
                String key = keys.nextElement().toString();
                String value = properties.getProperty(key);
                // 只添加非空值
                if (value != null) {
                    data.put(key, value);
                }
            }
            return new ConfigData(data);
        } catch (IOException e) {
            // 记录错误并抛出异常
            log.error("Could not read properties from file {}", path, e);
            throw new ConfigException("Could not read properties from file " + path);
        }
    }

    /**
     * 获取指定Properties文件中特定键的配置数据
     * 
     * 实现细节：
     * 1. 验证提供者状态
     * 2. 检查路径合法性
     * 3. 选择性读取指定键的值
     * 4. 处理异常情况
     *
     * @param path 配置文件的路径
     * @param keys 要获取的配置键集合
     * @return 配置数据对象
     * @throws IllegalStateException 如果提供者未配置
     * @throws ConfigException 如果无法读取配置文件
     */
    public ConfigData get(String path, Set<String> keys) {
        // 检查提供者是否已配置
        if (allowedPaths == null) {
            throw new IllegalStateException("The provider has not been configured yet.");
        }

        // 创建空的配置映射
        Map<String, String> data = new HashMap<>();
        // 检查路径是否有效
        if (path == null || path.isEmpty()) {
            return new ConfigData(data);
        }

        // 验证路径是否允许访问
        Path filePath = allowedPaths.parseUntrustedPath(path);
        if (filePath == null) {
            // 记录警告并返回空配置
            log.warn("The path {} is not allowed to be accessed", path);
            return new ConfigData(data);
        }

        // 读取并解析Properties文件
        try (Reader reader = reader(filePath)) {
            Properties properties = new Properties();
            // 加载属性文件
            properties.load(reader);
            // 只获取指定键的值
            for (String key : keys) {
                String value = properties.getProperty(key);
                // 只添加非空值
                if (value != null) {
                    data.put(key, value);
                }
            }
            return new ConfigData(data);
        } catch (IOException e) {
            // 记录错误并抛出异常
            log.error("Could not read properties from file {}", path, e);
            throw new ConfigException("Could not read properties from file " + path);
        }
    }

    /**
     * 创建文件读取器
     * 用于测试的可见方法
     * 
     * @param path 文件路径
     * @return 缓冲读取器
     * @throws IOException 如果无法创建读取器
     */
    protected Reader reader(Path path) throws IOException {
        // 创建UTF-8编码的缓冲读取器
        return Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    /**
     * 关闭提供者
     * 当前实现为空，因为没有需要清理的资源
     */
    public void close() {
    }
}
