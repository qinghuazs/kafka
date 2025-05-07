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

import javax.security.sasl.SaslServer;

/**
 * SASL认证失败异常。当SASL认证过程失败时抛出此异常，异常消息中包含具体的失败原因。
 * 
 * 应用场景：
 * 1. 客户端提供了无效的认证凭证
 * 2. SASL机制配置不正确
 * 3. 认证服务器暂时不可用
 * 4. 认证协议版本不匹配
 * 
 * 异常特点：
 * 1. 表示SASL认证过程中的失败
 * 2. 包含详细的失败原因信息
 * 3. 继承自AuthenticationException
 * 
 * 触发条件：
 * 1. 用户名或密码错误
 * 2. 认证令牌过期
 * 3. SASL机制特定的认证失败
 * 
 * 安全考虑：
 * 1. 异常消息会发送给客户端，需注意不要包含敏感信息
 * 2. 自定义SaslServer实现时要谨慎处理错误消息
 * 3. 避免向未认证的客户端泄露安全关键信息
 * 
 * 处理机制：
 * 1. 客户端可以根据异常消息判断失败原因
 * 2. 支持重新尝试认证
 * 3. 可以实现自定义的错误处理逻辑
 * 
 * 注意事项：
 * 当{@link SaslServer#evaluateResponse(byte[])}在认证过程中抛出此异常时，
 * 异常消息将通过SaslAuthenticate响应发送给客户端。自定义的{@link SaslServer}
 * 实现可以抛出此异常来提供自定义错误消息，但需要确保消息中不包含任何不应泄露
 * 给未认证客户端的安全敏感信息。
 */
public class SaslAuthenticationException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public SaslAuthenticationException(String message) {
        super(message);
    }

    public SaslAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
