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
package org.apache.kafka.common.network;

import java.io.Closeable;

/**
 * 通道元数据注册表接口
 * 
 * 该接口用于在Kafka网络栈的不同层级收集和管理通道相关的元数据信息。
 * 主要功能包括：
 * 1. SSL加密信息管理：存储和获取当前使用的加密套件信息
 * 2. 客户端信息管理：维护客户端版本等信息
 * 3. 资源管理：支持注册表的关闭和资源释放
 * 
 * 应用场景：
 * 1. 安全通信：在SSL通信中跟踪加密套件的使用情况
 * 2. 客户端兼容性：通过存储客户端信息来处理不同版本的兼容性
 * 3. 监控和调试：为网络连接提供元数据信息，便于问题诊断
 */
public interface ChannelMetadataRegistry extends Closeable {

    /**
     * 注册SSL加密套件信息
     * 
     * 功能：
     * 1. 记录当前通道使用的SSL加密套件详情
     * 2. 支持动态更新加密信息
     * 
     * 特点：
     * - 如果重复注册，新的信息会覆盖旧的信息
     * - 用于跟踪和管理SSL安全配置
     * 
     * @param cipherInformation SSL加密套件信息对象
     */
    void registerCipherInformation(CipherInformation cipherInformation);

    /**
     * 获取当前注册的SSL加密套件信息
     * 
     * 使用场景：
     * 1. 安全审计：检查当前使用的加密算法
     * 2. 连接诊断：排查SSL相关问题
     * 
     * @return 返回当前的加密套件信息，如果未注册则返回null
     */
    CipherInformation cipherInformation();

    /**
     * 注册客户端信息
     * 
     * 功能：
     * 1. 存储客户端版本、功能特性等信息
     * 2. 支持动态更新客户端信息
     * 
     * 特点：
     * - ApiVersionsRequest可能多次接收或完全不接收
     * - 重复注册会覆盖之前的信息
     * - 用于处理客户端兼容性和功能协商
     * 
     * @param clientInformation 客户端信息对象
     */
    void registerClientInformation(ClientInformation clientInformation);

    /**
     * 获取当前注册的客户端信息
     * 
     * 使用场景：
     * 1. 版本兼容性检查
     * 2. 客户端功能协商
     * 3. 连接监控和统计
     * 
     * @return 返回当前的客户端信息，如果未注册则返回null
     */
    ClientInformation clientInformation();

    /**
     * 注销所有已注册的信息并关闭注册表
     * 
     * 执行操作：
     * 1. 清除所有已注册的加密套件信息
     * 2. 清除所有已注册的客户端信息
     * 3. 释放相关资源
     * 
     * 调用时机：
     * - 通道关闭时
     * - 需要重置注册表状态时
     */
    void close();
}
