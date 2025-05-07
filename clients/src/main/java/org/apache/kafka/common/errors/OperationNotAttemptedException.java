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
 * 表示代理服务器未尝试执行此操作。这种情况通常发生在批处理RPC请求中，当批处理中的某些操作失败时，
 * 代理服务器会直接返回响应而不尝试执行剩余的操作。
 * 
 * 应用场景：
 * 1. 批量创建主题时部分主题创建失败
 * 2. 批量生产消息时部分消息发送失败
 * 3. 批量更新配置时部分配置更新失败
 * 
 * 设计考虑：
 * - 优化批处理操作的错误处理机制
 * - 快速失败策略，避免无谓的操作尝试
 * - 提供明确的错误信息，帮助定位批处理中的问题
 */
public class OperationNotAttemptedException extends ApiException {
    public OperationNotAttemptedException(final String message) {
        super(message);
    }
}
