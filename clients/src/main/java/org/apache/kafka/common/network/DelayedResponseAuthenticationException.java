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
package org.apache.kafka.common.network;

import org.apache.kafka.common.errors.AuthenticationException;

/**
 * 延迟响应认证异常类，用于处理需要延迟响应的认证失败场景。
 * 这个异常通常在以下情况下使用：
 * 1. 异步认证处理过程中，当认证响应需要延迟返回时
 * 2. 认证服务器需要额外时间处理认证请求时
 * 3. 实现认证失败的优雅降级和重试机制时
 *
 * 该异常继承自AuthenticationException，提供了对认证失败原因的封装和传递机制。
 */
public class DelayedResponseAuthenticationException extends AuthenticationException {
    
    /**
     * 序列化版本ID，用于确保跨JVM的序列化兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 构造一个新的延迟响应认证异常
     * 
     * @param cause 导致此异常的原始异常。这个原因将被封装在异常链中，
     *             便于异常处理时进行根因分析
     */
    public DelayedResponseAuthenticationException(Throwable cause) {
        // 调用父类构造器，传递原始异常作为cause，构建异常链
        super(cause);
    }
}
