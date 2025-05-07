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
 * 表示Kafka配置参数无效的异常
 *
 * 此异常继承自ApiException，当Kafka客户端或服务器的配置参数不符合要求时抛出。
 * Kafka的配置参数涉及多个方面，包括客户端行为、性能调优、安全设置等。
 *
 * 触发场景：
 * 1. 必需的配置参数缺失
 * 2. 配置参数值的类型错误
 * 3. 配置参数值超出有效范围
 * 4. 配置参数之间存在冲突
 * 5. 安全配置（如SSL、SASL）参数错误
 *
 * 影响：
 * 1. 客户端或服务器组件无法正常启动
 * 2. 特定功能无法正常使用
 * 3. 可能影响系统性能或安全性
 *
 * 最佳实践：
 * 1. 仔细阅读配置参数的文档说明
 * 2. 使用推荐的配置值范围
 * 3. 在测试环境验证配置更改
 * 4. 确保相关配置参数之间的一致性
 */
public class InvalidConfigurationException extends ApiException {

    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息创建InvalidConfigurationException实例
     *
     * @param message 详细描述异常原因的错误消息
     */
    public InvalidConfigurationException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因创建InvalidConfigurationException实例
     *
     * @param message 详细描述异常原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

}
