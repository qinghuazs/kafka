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

import java.util.Map;

/**
 * Kafka管理客户端中用于配置修改操作的选项类。
 * 这个类提供了对配置修改操作的控制选项，包括验证模式和超时设置。
 * 
 * 主要功能：
 * 1. 支持配置修改的预验证（通过validateOnly选项）
 * 2. 提供操作超时控制
 * 3. 支持链式调用方式设置选项
 * 
 * 应用场景：
 * 1. 在实际修改配置前进行验证检查
 * 2. 设置特定的操作超时时间
 * 3. 用于Admin#incrementalAlterConfigs(Map)方法的选项配置
 * 
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AlterConfigsOptions extends AbstractOptions<AlterConfigsOptions> {

    /**
     * 验证模式标志
     * 当设置为true时，仅验证配置修改请求的有效性，不实际执行修改
     * 默认值为false，表示执行实际的配置修改
     */
    private boolean validateOnly = false;

    /**
     * 设置操作的超时时间（毫秒）
     * 
     * @param timeoutMs 超时时间，如果为null则使用AdminClient的默认超时时间
     * @return 当前对象实例，支持链式调用
     * 
     * 实现说明：
     * 1. 通过设置timeoutMs字段控制操作超时
     * 2. 返回this以支持方法链式调用
     * 3. 该方法保留是为了保持与0.11版本的二进制兼容性
     */
    // This method is retained to keep binary compatibility with 0.11
    public AlterConfigsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * 获取验证模式的状态
     * 
     * @return 如果是仅验证模式返回true，否则返回false
     * 
     * 使用场景：
     * 1. 在执行配置修改前检查是否处于验证模式
     * 2. 用于判断是否需要实际执行配置修改
     */
    public boolean shouldValidateOnly() {
        return validateOnly;
    }

    /**
     * 设置是否启用仅验证模式
     * 
     * @param validateOnly true表示仅验证配置修改请求而不实际执行，false表示执行实际的配置修改
     * @return 当前对象实例，支持链式调用
     * 
     * 使用场景：
     * 1. 在进行重要配置修改前，先进行验证检查
     * 2. 用于测试配置修改的有效性
     * 3. 在实际修改之前预检查配置变更的影响
     */
    public AlterConfigsOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }
}
