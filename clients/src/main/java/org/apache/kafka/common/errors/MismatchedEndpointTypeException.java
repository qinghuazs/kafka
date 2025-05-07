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
 * 当客户端连接到的端点类型与预期不匹配时抛出此异常
 *
 * 应用场景：
 * 1. 客户端尝试使用错误的协议或端点类型连接到Broker
 * 2. 在使用多种协议（如PLAINTEXT、SSL）的集群中，连接到了错误类型的监听器
 * 3. 当Broker配置了特定类型的端点，但客户端使用了不兼容的连接方式
 *
 * 设计考虑：
 * 1. 继承自ApiException，用于处理客户端连接协议相关的错误
 * 2. 提供明确的错误信息，帮助快速定位连接配置问题
 * 3. 作为安全机制的一部分，防止不当的协议访问
 */
public class MismatchedEndpointTypeException extends ApiException {
    public MismatchedEndpointTypeException(String message) {
        super(message);
    }
}
