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
 * 授权异常类
 * 当操作因为权限不足而失败时抛出此异常。
 * 
 * 应用场景：
 * 1. 访问控制：用户尝试执行未授权的操作
 * 2. 权限验证：验证用户操作权限失败
 * 3. 安全控制：实施安全策略时的权限检查
 * 4. 资源保护：保护敏感资源不被未授权访问
 *
 * 设计考虑：
 * 1. 继承性：继承自ApiException以统一异常处理
 * 2. 简单性：仅提供基本的异常信息传递
 * 3. 异常链：支持异常原因的传递
 * 4. 安全性：用于权限相关的错误处理
 */
public class AuthorizationException extends ApiException {

    /**
     * 使用错误消息构造授权异常
     *
     * @param message 描述授权失败原因的消息
     */
    public AuthorizationException(String message) {
        // 调用父类构造函数，传递错误消息
        super(message);
    }

    /**
     * 使用错误消息和原因构造授权异常
     *
     * @param message 描述授权失败原因的消息
     * @param cause 导致授权失败的原始异常
     */
    public AuthorizationException(String message, Throwable cause) {
        // 调用父类构造函数，传递错误消息和原因
        super(message, cause);
    }
}
