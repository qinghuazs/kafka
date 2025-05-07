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
 * 安全禁用异常。表示Broker上的安全功能已被禁用。
 * 
 * 应用场景：
 * 1. 客户端尝试使用安全功能，但Broker未启用安全配置
 * 2. 安全相关的API调用在非安全模式下执行
 * 3. 认证或授权功能在未配置安全机制时被调用
 * 
 * 异常特点：
 * 1. 表示系统配置问题，而非运行时错误
 * 2. 继承自ApiException，用于API层面的错误处理
 * 3. 通常需要管理员介入修改配置
 * 
 * 影响范围：
 * 1. 无法执行需要安全验证的操作
 * 2. 可能导致敏感操作被拒绝
 * 3. 影响系统的安全性保证
 * 
 * 处理建议：
 * 1. 检查Broker的安全配置
 * 2. 确认是否需要启用安全功能
 * 3. 根据需求配置适当的安全机制
 */
public class SecurityDisabledException extends ApiException {
    private static final long serialVersionUID = 1L;

    public SecurityDisabledException(String message) {
        super(message);
    }

    public SecurityDisabledException(String message, Throwable cause) {
        super(message, cause);
    }
}
