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
 * 此异常表示SSL握手过程失败。可以通过{@link #getCause()}获取导致失败的
 * {@link SSLException}原始异常。
 * 
 * SSL握手失败的常见原因：
 * 1. 客户端认证失败
 *    - 当服务器配置要求客户端证书时，可能由于证书不受信任导致认证失败
 * 2. 安全配置错误
 *    - 协议版本不匹配（如服务器要求TLS 1.2，但客户端使用TLS 1.1）
 *    - 密码套件不兼容
 *    - 服务器证书验证失败
 *    - 服务器主机名验证失败
 * 
 * 解决方案：
 * - 确保客户端和服务器使用匹配的SSL/TLS版本
 * - 验证证书的有效性和信任链
 * - 检查密码套件配置
 * - 确保服务器证书的主机名与连接地址匹配
 */
public class SslAuthenticationException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造SSL认证异常
     * @param message 描述SSL认证失败原因的错误消息
     */
    public SslAuthenticationException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造SSL认证异常
     * @param message 描述SSL认证失败原因的错误消息
     * @param cause 导致认证失败的原始SSL异常
     */
    public SslAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
