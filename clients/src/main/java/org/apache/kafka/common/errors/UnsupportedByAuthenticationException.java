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
 * 当认证机制不支持请求的功能时抛出此异常。
 * 
 * 应用场景：
 * 1. 客户端请求使用不被当前认证机制支持的安全特性
 * 2. 尝试在不支持的认证模式下执行特权操作
 * 3. 认证协议版本不兼容导致的功能限制
 * 
 * 设计考虑：
 * - 清晰区分认证机制的功能边界
 * - 帮助用户理解安全限制和认证要求
 * - 防止在不适当的认证上下文中执行敏感操作
 */
public class UnsupportedByAuthenticationException extends ApiException {
    private static final long serialVersionUID = 1L;

    public UnsupportedByAuthenticationException(String message) {
        super(message);
    }

    public UnsupportedByAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
