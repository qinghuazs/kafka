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
 * 代理重复注册异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当一个Broker尝试使用已被其他Broker使用的ID注册到集群时
 * 2. 当同一个Broker ID的多个实例同时尝试加入集群时
 * 
 * 这个异常的出现通常表示：
 * - 配置错误：多个Broker被错误地配置了相同的ID
 * - 运维问题：在旧的Broker实例还在运行时就启动了新的实例
 * - 网络分区：由于网络问题导致集群出现脑裂，多个相同ID的Broker同时存在
 * 
 * 为了保持集群的一致性和正常运行，每个Broker必须具有唯一的ID。
 */
public class DuplicateBrokerRegistrationException extends ApiException {

    /**
     * 构造函数
     * 
     * @param message 异常消息，描述代理重复注册的具体原因和相关信息
     */
    public DuplicateBrokerRegistrationException(String message) {
        super(message);
    }

    /**
     * 构造函数
     * 
     * @param message 异常消息，描述代理重复注册的具体原因和相关信息
     * @param throwable 导致此异常的原始异常，可能包含更详细的错误信息
     */
    public DuplicateBrokerRegistrationException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
