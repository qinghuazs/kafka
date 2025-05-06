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
 * Options for {@link AdminClient#updateFeatures(Map, UpdateFeaturesOptions)}.
 * 用于配置Kafka特性更新操作的选项类。
 *
 * The API of this class is evolving. See {@link Admin} for details.
 * 该类的API仍在演进中，详情请参见{@link Admin}。
 *
 * 主要功能：
 * 1. 提供特性更新操作的配置选项
 * 2. 支持验证模式，可以在实际更新前进行预检查
 * 3. 继承自AbstractOptions，获取通用的选项处理功能
 *
 * 应用场景：
 * 1. 在Kafka集群中更新或升级特性时使用
 * 2. 在实际应用特性更新前进行验证检查
 * 3. 用于控制特性更新的行为和参数
 */
@InterfaceStability.Evolving
public class UpdateFeaturesOptions extends AbstractOptions<UpdateFeaturesOptions> {
    /**
     * 验证模式标志
     * 当设置为true时，仅验证特性更新请求的有效性，不实际执行更新
     * 默认值为false，表示执行实际的特性更新操作
     */
    private boolean validateOnly = false;

    /**
     * 获取验证模式的状态
     *
     * @return 如果是仅验证模式返回true，否则返回false
     *
     * 使用场景：
     * 1. 在执行特性更新前检查是否处于验证模式
     * 2. 用于判断是否需要实际执行特性更新
     */
    public boolean validateOnly() {
        return validateOnly;
    }

    /**
     * 设置是否启用仅验证模式
     *
     * @param validateOnly true表示仅验证特性更新请求而不实际执行，false表示执行实际的特性更新
     * @return 当前对象实例，支持链式调用
     *
     * 使用场景：
     * 1. 在进行重要特性更新前，先进行验证检查
     * 2. 用于测试特性更新的有效性
     * 3. 在实际更新之前预检查变更的影响
     */
    public UpdateFeaturesOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }
}
