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

import javax.net.ssl.SSLException;

/**
 * 此异常表示SASL（Simple Authentication and Security Layer，简单认证与安全层）认证失败。
 * 当认证失败时，客户端会中止请求的操作并抛出此异常的以下子类之一：
 * <ul>
 *   </li>{@link SaslAuthenticationException} 当SASL握手因无效凭证失败时抛出，
 *   或者在使用SASL机制进行认证时发生的其他特定失败</li>
 *   <li>{@link UnsupportedSaslMechanismException} 当客户端请求的SASL机制
 *   在broker上不受支持时抛出</li>
 *   <li>{@link IllegalSaslStateException} 当在SASL握手过程中收到意外请求时抛出。
 *   这通常是由于安全协议配置错误导致的</li>
 *   <li>{@link SslAuthenticationException} 当SSL握手因任何{@link SSLException}失败时抛出。
 *   这可能是由于证书验证失败、协议不匹配等原因</li>
 * </ul>
 *
 * 应用场景：
 * 1. 客户端与Kafka broker建立连接时的认证失败处理
 * 2. 在安全性要求较高的环境中，用于识别和处理各种认证问题
 * 3. 作为Kafka安全认证机制中的核心异常处理类
 */
public class AuthenticationException extends ApiException {

    /**
     * 序列化版本号，用于类版本控制
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造认证异常
     * @param message 详细描述认证失败原因的错误消息
     */
    public AuthenticationException(String message) {
        super(message);
    }

    /**
     * 使用指定的原因构造认证异常
     * @param cause 导致认证失败的根本原因
     */
    public AuthenticationException(Throwable cause) {
        super(cause);
    }

    /**
     * 使用指定的错误消息和原因构造认证异常
     * @param message 详细描述认证失败原因的错误消息
     * @param cause 导致认证失败的根本原因
     */
    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
