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
 * 委托令牌过期异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当客户端尝试使用已过期的委托令牌进行认证时
 * 2. 当代理服务器检测到接收到的委托令牌已超过其有效期时
 * 
 * 委托令牌是Kafka中用于身份验证的一种机制，它允许主体（用户）将其认证权限临时委托给其他主体。
 * 每个令牌都有一个过期时间，一旦超过这个时间，令牌就会失效，任何使用过期令牌的操作都会触发此异常。
 */
public class DelegationTokenExpiredException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     * 
     * @param message 异常消息，通常包含令牌过期的具体原因和相关信息
     */
    public DelegationTokenExpiredException(String message) {
        super(message);
    }

    /**
     * 构造函数
     * 
     * @param message 异常消息，通常包含令牌过期的具体原因和相关信息
     * @param cause 导致此异常的原始异常，可能包含更详细的错误信息
     */
    public DelegationTokenExpiredException(String message, Throwable cause) {
        super(message, cause);
    }

}
