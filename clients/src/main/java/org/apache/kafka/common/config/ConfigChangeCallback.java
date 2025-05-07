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

/**
 * A callback passed to {@link ConfigProvider} for subscribing to changes.
 */
/**
 * 配置变更回调接口
 * 传递给{@link ConfigProvider}用于订阅配置变更。
 * 
 * 应用场景：
 * 1. 动态配置更新：实时响应配置变化
 * 2. 配置监听：监控特定配置项的变更
 * 3. 热重载：支持运行时配置更新
 * 4. 事件通知：配置变更时触发相关操作
 *
 * 设计考虑：
 * 1. 回调机制：采用回调接口实现异步通知
 * 2. 灵活性：支持自定义变更处理逻辑
 * 3. 解耦：将配置变更检测与处理逻辑分离
 * 4. 可扩展：允许多个监听器订阅同一配置
 */
public interface ConfigChangeCallback {

    /**
     * 当配置数据发生变更时执行操作
     * 
     * 实现说明：
     * - 在配置发生变化时被调用
     * - 实现类需要处理配置变更逻辑
     * - 支持对特定路径的配置变更进行响应
     * - 提供新的配置数据用于更新
     *
     * @param path 配置数据所在的路径
     * @param data 新的配置数据
     */
    void onChange(String path, ConfigData data);
}
