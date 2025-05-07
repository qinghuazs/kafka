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
 * Broker ID未注册异常类
 * 当尝试使用未在ZooKeeper中注册的Broker ID时抛出此异常。
 * 
 * 应用场景：
 * 1. Broker注册：Broker启动时的ID验证
 * 2. 集群管理：检测无效的Broker ID
 * 3. 配置验证：验证Broker配置的有效性
 * 4. 故障检测：识别未正确注册的Broker
 *
 * 设计考虑：
 * 1. 继承性：继承自ApiException以统一异常处理
 * 2. 错误信息：提供详细的错误描述
 * 3. 异常链：支持异常原因的传递
 * 4. 诊断支持：便于问题诊断和排查
 */
public class BrokerIdNotRegisteredException extends ApiException {

    /**
     * 使用错误消息构造Broker ID未注册异常
     *
     * @param message 描述Broker ID未注册原因的消息
     */
    public BrokerIdNotRegisteredException(String message) {
        // 调用父类构造函数，传递错误消息
        super(message);
    }

    /**
     * 使用错误消息和原因构造Broker ID未注册异常
     *
     * @param message 描述Broker ID未注册原因的消息
     * @param throwable 导致异常的原始异常
     */
    public BrokerIdNotRegisteredException(String message, Throwable throwable) {
        // 调用父类构造函数，传递错误消息和原因
        super(message, throwable);
    }
}
