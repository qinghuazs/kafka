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
package org.apache.kafka.common.security.auth;

import java.net.InetAddress;


/**
 * 表示认证会话上下文信息的对象。
 * 这个接口是Kafka认证系统的核心接口，仅在Broker端使用。它提供了访问认证会话相关信息的标准方法。
 * 
 * 该接口有三个具体实现：
 * {@link PlaintextAuthenticationContext} - 用于明文通信场景，不提供任何安全保护
 * {@link SaslAuthenticationContext} - 用于SASL认证场景，支持各种SASL机制（如PLAIN、SCRAM、GSSAPI等）
 * {@link SslAuthenticationContext} - 用于SSL/TLS认证场景，提供传输层安全性
 * 
 * 通过这个接口，Broker可以：
 * 1. 确定客户端使用的安全协议类型
 * 2. 获取已认证客户端的网络地址
 * 3. 了解客户端连接使用的监听器名称
 * 
 * 这些信息对于实现访问控制、审计日志和监控等安全特性非常重要。
 */
public interface AuthenticationContext {
    /**
     * 获取认证会话使用的底层安全协议。
     * 
     * 这个方法返回的SecurityProtocol枚举值表明了客户端与Broker之间使用的通信安全协议类型，可能的值包括：
     * - PLAINTEXT：无安全保护的明文通信
     * - SSL：使用SSL/TLS加密的安全通信
     * - SASL_PLAINTEXT：使用SASL进行认证，但传输层是明文
     * - SASL_SSL：同时使用SASL认证和SSL/TLS加密，提供最高级别的安全保护
     * 
     * @return 当前认证会话使用的安全协议类型
     */
    SecurityProtocol securityProtocol();

    /**
     * 获取已认证客户端的网络地址。
     * 
     * 这个方法返回已完成认证的客户端的InetAddress对象，包含了客户端的IP地址信息。
     * 这个信息可用于：
     * 1. 实现基于IP的访问控制
     * 2. 记录审计日志
     * 3. 进行连接监控和故障排查
     * 
     * @return 客户端的网络地址
     */
    InetAddress clientAddress();

    /**
     * 获取用于建立连接的监听器名称。
     * 
     * Kafka Broker可以配置多个监听器，每个监听器可以：
     * 1. 使用不同的主机名和端口
     * 2. 配置不同的安全协议
     * 3. 应用不同的访问控制策略
     * 
     * 这个方法返回客户端连接所使用的监听器的名称，对于：
     * - 确定连接的安全配置
     * - 实现基于监听器的访问控制
     * - 区分内部和外部客户端连接
     * 等场景非常重要。
     * 
     * @return 监听器名称
     */
    String listenerName();
}
