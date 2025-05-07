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
 * 当委托令牌（Delegation Token）的授权操作失败时抛出此异常。
 * 
 * 应用场景：
 * 1. 当用户尝试创建、续期或删除委托令牌但没有足够权限时
 * 2. 当使用过期或无效的委托令牌进行认证时
 * 3. 当令牌的操作权限与请求的操作不匹配时
 * 
 * 设计考虑：
 * 1. 继承自AuthorizationException，表明这是一个授权相关的异常
 * 2. 用于处理Kafka安全机制中的令牌授权失败情况
 * 3. 支持自定义错误消息，便于提供详细的授权失败原因
 * 4. 允许包含原始异常，便于追踪授权失败的根本原因
 */
public class DelegationTokenAuthorizationException extends AuthorizationException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenAuthorizationException(String message) {
        super(message);
    }

    public DelegationTokenAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

}
