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
 * 当在SASL认证完成之前收到非预期的请求时抛出此异常。
 * 这通常是由于安全配置错误导致的，例如使用PLAINTEXT协议连接到需要SASL认证的端点。
 * 
 * 应用场景：
 * 1. 客户端使用了错误的安全协议配置
 * 2. 在完成SASL认证之前尝试执行需要认证的操作
 * 3. 检测到潜在的安全配置问题
 * 
 * 设计考虑：
 * 1. 继承自AuthenticationException以统一认证相关的异常处理
 * 2. 提供详细的错误信息以帮助诊断安全配置问题
 * 3. 作为SASL认证流程的状态检查机制
 * 4. 防止未经授权的操作访问受保护的资源
 */
public class IllegalSaslStateException extends AuthenticationException {

    /**
     * 序列化版本ID
     */
    private static final long serialVersionUID = 1L;

    /**
     * 带有错误信息的构造函数
     * @param message 异常描述信息，说明SASL状态错误的具体原因
     */
    public IllegalSaslStateException(String message) {
        super(message);
    }

    /**
     * 带有错误信息和原因的构造函数
     * @param message 异常描述信息，说明SASL状态错误的具体原因
     * @param cause 导致此异常的原始异常
     */
    public IllegalSaslStateException(String message, Throwable cause) {
        super(message, cause);
    }

}
