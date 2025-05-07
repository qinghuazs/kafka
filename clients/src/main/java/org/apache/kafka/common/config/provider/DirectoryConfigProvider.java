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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * 基于文件目录的配置提供者实现
 * 通过读取指定目录中的文件来提供配置数据。
 * 属性键对应目录中常规文件（非目录）的名称，
 * 属性值对应这些文件的内容。
 * 
 * 应用场景：
 * 1. 文件系统配置管理：从文件系统读取配置
 * 2. 敏感信息存储：将敏感配置存储在独立文件中
 * 3. 动态配置更新：支持通过文件修改更新配置
 * 4. 分布式配置：在多节点间共享配置文件
 *
 * 设计考虑：
 * 1. 安全性：支持路径访问控制
 * 2. 容错性：优雅处理文件系统异常
 * 3. 灵活性：支持选择性读取指定文件
 * 4. 资源管理：自动关闭文件流
 */
public class DirectoryConfigProvider implements ConfigProvider {

    /**
     * 日志记录器
     * 用于记录配置访问和错误信息
     */
    private static final Logger log = LoggerFactory.getLogger(DirectoryConfigProvider.class);

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
    @Override
    public void configure(Map<String, ?> configs) {
        // 从配置中获取允许路径列表，如果未配置则为null
        allowedPaths = new AllowedPaths((String) configs.getOrDefault(ALLOWED_PATHS_CONFIG, null));
    }

    /**
     * 关闭该提供者
     * 当前实现为空，因为没有需要清理的资源
     */
    @Override
    public void close() throws IOException { }

    /**
     * 获取指定目录中所有常规文件的配置数据
     * 忽略目录中的非常规文件（如目录）
     *
     * @param path 配置文件所在的目录路径
     * @return 配置数据对象
     */
    @Override
    public ConfigData get(String path) {
        // 使用Files.isRegularFile作为文件过滤器
        return get(path, Files::isRegularFile);
    }

    /**
     * 获取指定目录中特定文件名的配置数据
     * 只读取文件名匹配指定键集合的常规文件
     *
     * @param path 配置文件所在的目录路径
     * @param keys 要获取的配置键集合（文件名）
     * @return 配置数据对象
     */
    @Override
    public ConfigData get(String path, Set<String> keys) {
        // 创建文件过滤器：必须是常规文件且文件名在keys集合中
        return get(path, pathname ->
                Files.isRegularFile(pathname)
                        && keys.contains(pathname.getFileName().toString()));
    }

    /**
     * 内部获取配置数据的实现方法
     * 
     * 实现细节：
     * 1. 验证提供者状态和路径合法性
     * 2. 过滤并读取符合条件的文件
     * 3. 处理各种异常情况
     *
     * @param path 配置文件所在的目录路径
     * @param fileFilter 文件过滤器
     * @return 配置数据对象
     * @throws IllegalStateException 如果提供者未配置
     * @throws ConfigException 如果无法列出目录内容
     */
    private ConfigData get(String path, Predicate<Path> fileFilter) {
        // 检查提供者是否已配置
        if (allowedPaths == null) {
            throw new IllegalStateException("The provider has not been configured yet.");
        }

        // 创建空的配置映射
        Map<String, String> map = emptyMap();

        // 检查路径是否有效
        if (path != null && !path.isEmpty()) {
            // 验证路径是否允许访问
            Path dir = allowedPaths.parseUntrustedPath(path);
            if (dir == null) {
                // 记录警告并返回空配置
                log.warn("The path {} is not allowed to be accessed", path);
                return new ConfigData(map);
            }

            // 检查路径是否为目录
            if (!Files.isDirectory(dir)) {
                log.warn("The path {} is not a directory", path);
            } else {
                try (Stream<Path> stream = Files.list(dir)) {
                    // 过滤并读取符合条件的文件
                    map = stream
                        .filter(fileFilter)
                        .collect(Collectors.toMap(
                            p -> p.getFileName().toString(), // 使用文件名作为键
                            p -> read(p))); // 读取文件内容作为值
                } catch (IOException e) {
                    // 记录错误并抛出异常
                    log.error("Could not list directory {}", dir, e);
                    throw new ConfigException("Could not list directory " + dir);
                }
            }
        }
        // 返回配置数据
        return new ConfigData(map);
    }

    /**
     * 读取文件内容的辅助方法
     * 
     * 实现细节：
     * 1. 使用Files.readString读取整个文件
     * 2. 处理IO异常并转换为ConfigException
     * 3. 记录错误信息
     *
     * @param path 要读取的文件路径
     * @return 文件内容字符串
     * @throws ConfigException 如果无法读取文件
     */
    private static String read(Path path) {
        try {
            // 读取整个文件内容
            return Files.readString(path);
        } catch (IOException e) {
            // 记录错误并抛出异常
            log.error("Could not read file {} for property {}", path, path.getFileName(), e);
            throw new ConfigException("Could not read file " + path + " for property " + path.getFileName());
        }
    }
}
