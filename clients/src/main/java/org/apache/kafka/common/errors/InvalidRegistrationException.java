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
 * 当代理节点(Broker)的注册请求被控制器(Controller)认为无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 在Kafka集群中，每个Broker在启动时都需要向Controller注册
 * 2. 如果注册信息不完整或格式错误（如无效的Broker ID）
 * 3. 如果Broker配置与集群要求不匹配
 * 4. 当现有Broker尝试使用已被占用的Broker ID重新注册
 * 
 * 设计考虑：
 * - 确保集群中的Broker注册信息的一致性和有效性
 * - 帮助快速识别Broker配置或部署问题
 * - 防止重复或无效的Broker ID导致的集群混乱
 */
public class InvalidRegistrationException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidRegistrationException(String message) {
        super(message);
    }
}
