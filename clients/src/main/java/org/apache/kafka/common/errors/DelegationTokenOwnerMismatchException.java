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
 * 委托令牌所有者不匹配异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当非令牌所有者尝试对令牌进行操作（如续期或删除）时
 * 2. 当用户尝试使用不属于自己的委托令牌进行认证时
 * 
 * 在Kafka的安全机制中，委托令牌的操作权限通常仅限于：
 * - 令牌的原始所有者
 * - 具有管理员权限的用户
 * 
 * 这个异常有助于防止未经授权的用户操作或使用他人的委托令牌，从而保护系统安全。
 */
public class DelegationTokenOwnerMismatchException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     * 
     * @param message 异常消息，通常包含令牌所有者不匹配的具体原因和相关信息
     */
    public DelegationTokenOwnerMismatchException(String message) {
        super(message);
    }

    /**
     * 构造函数
     * 
     * @param message 异常消息，通常包含令牌所有者不匹配的具体原因和相关信息
     * @param cause 导致此异常的原始异常，可能包含更详细的错误信息
     */
    public DelegationTokenOwnerMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

}
