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
 * 当客户端请求的SASL认证机制在broker上未启用时抛出此异常。
 * 
 * SASL（Simple Authentication and Security Layer）是Kafka支持的一种认证框架，
 * 支持多种认证机制，如PLAIN、GSSAPI（Kerberos）、SCRAM等。
 * 
 * 应用场景：
 * 1. 客户端配置了broker未开启的SASL机制
 * 2. broker的安全配置中未包含客户端请求的认证方式
 * 3. SASL机制配置不匹配，例如客户端使用PLAIN而服务器只配置了GSSAPI
 * 
 * 解决方案：
 * 1. 检查broker的security.inter.broker.protocol配置
 * 2. 确保broker的listeners配置包含正确的SASL机制
 * 3. 确保客户端和服务器的SASL配置一致
 */
public class UnsupportedSaslMechanismException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述不支持的SASL机制的错误消息
     */
    public UnsupportedSaslMechanismException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述不支持的SASL机制的错误消息
     * @param cause 导致此异常的原始异常
     */
    public UnsupportedSaslMechanismException(String message, Throwable cause) {
        super(message, cause);
    }

}
