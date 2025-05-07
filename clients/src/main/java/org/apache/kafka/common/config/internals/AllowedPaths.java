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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.config.ConfigException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 路径访问控制类
 * 用于管理和验证允许访问的路径列表。
 * 
 * 应用场景：
 * 1. 文件系统访问控制：限制程序只能访问指定的目录
 * 2. 安全性控制：防止未授权的文件系统访问
 * 3. 配置文件路径验证：确保配置的路径合法且存在
 * 4. 路径规范化处理：统一处理路径格式
 *
 * 设计考虑：
 * 1. 路径验证：确保路径为绝对路径且存在
 * 2. 路径规范化：处理路径中的 . 和 .. 等特殊元素
 * 3. 空值处理：支持未配置允许路径的场景
 * 4. 线程安全：通过不可变列表保证线程安全
 */
public class AllowedPaths {
    /**
     * 允许访问的路径列表
     * 存储经过验证和规范化的绝对路径
     * null表示未配置允许路径（即允许所有路径）
     */
    private final List<Path> allowedPaths;

    /**
     * 构造函数
     * 根据配置值初始化允许访问的路径列表
     *
     * @param configValue 包含逗号分隔的路径列表的配置字符串
     * @throws ConfigException 如果任何路径不是绝对路径或不存在
     */
    public AllowedPaths(String configValue) {
        // 通过getAllowedPaths方法解析和验证配置的路径
        this.allowedPaths = getAllowedPaths(configValue);
    }

    /**
     * 解析和验证允许访问的路径列表
     * 
     * 实现细节：
     * 1. 验证路径的合法性
     * 2. 规范化路径格式
     * 3. 检查路径是否存在
     *
     * @param configValue 配置值字符串
     * @return 允许访问的路径列表，如果配置为空则返回null
     * @throws ConfigException 如果路径验证失败
     */
    private List<Path> getAllowedPaths(String configValue) {
        // 检查配置值是否有效
        if (configValue != null && !configValue.isEmpty()) {
            // 创建路径列表用于存储验证通过的路径
            List<Path> allowedPaths = new ArrayList<>();

            // 分割并处理每个配置的路径
            Arrays.stream(configValue.split(",")).forEach(b -> {
                // 获取规范化的路径（处理.和..等特殊元素）
                Path normalisedPath = Paths.get(b).normalize();

                // 验证路径是否为绝对路径
                if (!normalisedPath.isAbsolute()) {
                    throw new ConfigException("Path " + normalisedPath + " is not absolute");
                // 验证路径是否存在
                } else if (!Files.exists(normalisedPath)) {
                    throw new ConfigException("Path " + normalisedPath + " does not exist");
                } else {
                    // 将验证通过的路径添加到列表
                    allowedPaths.add(normalisedPath);
                }
            });

            return allowedPaths;
        }

        // 如果配置为空，返回null表示未限制路径访问
        return null;
    }

    /**
     * 验证给定路径是否允许访问
     * 检查路径是否位于配置的允许路径列表中
     * 
     * 实现细节：
     * 1. 规范化待验证的路径
     * 2. 检查是否在允许的路径范围内
     * 3. 支持未配置允许路径的场景
     *
     * @param path 要验证的路径字符串
     * @return 如果路径允许访问则返回规范化的路径，否则返回null
     */
    public Path parseUntrustedPath(String path) {
        // 解析并创建Path对象
        Path parsedPath = Paths.get(path);

        // 如果已配置允许路径列表，则进行验证
        if (allowedPaths != null) {
            // 规范化待验证的路径
            Path normalisedPath = parsedPath.normalize();
            // 检查是否有任何允许的路径是待验证路径的前缀
            long allowed = allowedPaths.stream().filter(normalisedPath::startsWith).count();
            // 如果没有匹配的允许路径，返回null
            if (allowed == 0) {
                return null;
            }
            // 返回规范化的路径
            return normalisedPath;
        }

        // 如果未配置允许路径列表，则允许访问所有路径
        return parsedPath;
    }
}
