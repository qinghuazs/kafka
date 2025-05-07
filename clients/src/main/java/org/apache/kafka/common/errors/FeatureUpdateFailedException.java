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
package org.apache.kafka.common.errors;

/**
 * 功能更新失败异常
 * 
 * 该异常在Kafka尝试更新或修改功能配置时失败时抛出。
 * 主要应用场景：
 * 1. 动态更新Kafka特性配置
 * 2. 启用或禁用某些功能特性
 * 3. 修改现有功能的参数设置
 * 
 * 可能的失败原因：
 * - 配置值不合法
 * - 功能依赖条件不满足
 * - 集群状态不允许更新
 * - 版本兼容性问题
 * 
 * 处理建议：
 * - 检查配置值的合法性
 * - 验证集群状态和版本兼容性
 * - 确保所有必要的依赖条件都满足
 */
public class FeatureUpdateFailedException extends ApiException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造功能更新失败异常
     * 
     * @param message 描述功能更新失败原因的错误消息
     */
    public FeatureUpdateFailedException(final String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造功能更新失败异常
     * 
     * @param message 描述功能更新失败原因的错误消息
     * @param cause 导致该异常的原始异常
     */
    public FeatureUpdateFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
