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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.common.config.ConfigData;

import java.io.Closeable;
import java.util.Set;

/**
 * 配置数据提供者接口
 * 提供配置数据访问功能，可选择性地支持配置变更订阅。
 * 
 * 应用场景：
 * 1. 外部配置系统集成：如ZooKeeper、Consul等
 * 2. 动态配置管理：支持运行时配置更新
 * 3. 分布式配置中心：统一管理多节点配置
 * 4. 敏感信息管理：安全凭证和密钥的动态获取
 * 
 * 设计考虑：
 * 1. 线程安全：所有实现类必须支持并发调用
 * 2. 可扩展性：通过ServiceLoader机制支持插件式扩展
 * 3. 订阅机制：可选支持配置变更通知
 * 4. 资源管理：继承Closeable接口确保资源正确释放
 *
 * 实现要求：
 * <p>实现类必须确保所有接口方法的并发调用安全。
 * <p>Kafka Connect使用Java的ServiceLoader机制发现该接口的实现。
 * 为支持这一机制，实现类还需要提供服务提供者配置文件，位于：
 * {@code META-INF/services/org.apache.kafka.common.config.provider.ConfigProvider}。
 */
public interface ConfigProvider extends Configurable, Closeable {

    /**
     * 获取指定路径的配置数据
     * 
     * 实现说明：
     * - 路径可以是配置系统中的任意有效位置
     * - 返回该路径下的所有配置数据
     * - 实现类应处理路径不存在的情况
     *
     * @param path 配置数据所在的路径
     * @return 配置数据对象
     */
    ConfigData get(String path);

    /**
     * 获取指定路径下特定键的配置数据
     * 
     * 实现说明：
     * - 只返回指定键的配置数据
     * - 提供更精确的数据获取方式
     * - 可以优化数据传输量
     *
     * @param path 配置数据所在的路径
     * @param keys 需要获取的配置键集合
     * @return 配置数据对象
     */
    ConfigData get(String path, Set<String> keys);

    /**
     * 订阅指定路径下特定键的配置变更（可选操作）
     * 
     * 实现说明：
     * - 当配置发生变更时触发回调
     * - 支持细粒度的配置监控
     * - 默认实现抛出UnsupportedOperationException
     *
     * @param path 配置数据所在的路径
     * @param keys 需要监控的配置键集合
     * @param callback 配置变更时的回调函数
     * @throws UnsupportedOperationException 如果不支持订阅操作
     */
    default void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
        // 默认实现：抛出不支持操作异常
        throw new UnsupportedOperationException();
    }

    /**
     * 取消订阅指定路径下特定键的配置变更（可选操作）
     * 
     * 实现说明：
     * - 移除特定配置的变更监听
     * - 支持精确的订阅管理
     * - 默认实现抛出UnsupportedOperationException
     *
     * @param path 配置数据所在的路径
     * @param keys 需要取消监控的配置键集合
     * @param callback 要取消的回调函数
     * @throws UnsupportedOperationException 如果不支持取消订阅操作
     */
    default void unsubscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
        // 默认实现：抛出不支持操作异常
        throw new UnsupportedOperationException();
    }

    /**
     * 清除所有配置订阅（可选操作）
     * 
     * 实现说明：
     * - 批量取消所有配置监听
     * - 用于资源清理和重置
     * - 默认实现抛出UnsupportedOperationException
     *
     * @throws UnsupportedOperationException 如果不支持清除所有订阅操作
     */
    default void unsubscribeAll() {
        // 默认实现：抛出不支持操作异常
        throw new UnsupportedOperationException();
    }
}
