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

/**
 * 安全配置类
 * 包含SSL和SASL的通用安全配置。
 * 
 * 应用场景：
 * 1. 安全提供者配置：管理安全算法的提供者
 * 2. SSL配置：配置SSL安全连接参数
 * 3. SASL配置：配置SASL认证参数
 * 4. 安全扩展：支持自定义安全算法实现
 *
 * 设计考虑：
 * 1. 可扩展性：支持自定义安全提供者
 * 2. 统一管理：集中管理安全相关配置
 * 3. 接口规范：要求实现特定接口
 * 4. 配置灵活：支持多个提供者配置
 */
public class SecurityConfig {

    /**
     * 安全提供者配置键
     * 用于配置安全算法提供者的创建类列表
     */
    public static final String SECURITY_PROVIDERS_CONFIG = "security.providers";

    /**
     * 安全提供者配置文档
     * 描述了配置值的要求和用途：
     * - 配置值应为创建者类的列表
     * - 每个类都应返回一个实现安全算法的提供者
     * - 这些类必须实现SecurityProviderCreator接口
     */
    public static final String SECURITY_PROVIDERS_DOC = "A list of configurable creator classes each returning a provider" +
        " implementing security algorithms. These classes should implement the" +
        " <code>org.apache.kafka.common.security.auth.SecurityProviderCreator</code> interface.";
}
