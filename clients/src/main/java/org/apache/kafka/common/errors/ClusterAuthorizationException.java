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
 * 集群授权异常类
 * 当用户尝试执行需要集群级别权限的操作但未获得授权时抛出此异常。
 * 
 * 应用场景：
 * 1. 集群操作：需要集群级别权限的操作验证
 * 2. 管理任务：集群管理和配置更改
 * 3. 安全控制：集群级别的访问控制
 * 4. 权限验证：验证用户是否具有集群操作权限
 *
 * 设计考虑：
 * 1. 继承性：继承自AuthorizationException以复用通用授权异常逻辑
 * 2. 特化性：专门用于处理集群级别的授权问题
 * 3. 序列化：支持异常的序列化传输
 * 4. 异常链：支持异常原因的传递
 */
public class ClusterAuthorizationException extends AuthorizationException {

    /**
     * 序列化版本ID
     * 用于确保序列化的兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用错误消息构造集群授权异常
     * 
     * 实现说明：
     * - 调用父类构造函数传递错误消息
     * - 用于简单的授权失败场景
     *
     * @param message 描述集群授权失败原因的消息
     */
    public ClusterAuthorizationException(String message) {
        // 调用父类构造函数，传递错误消息
        super(message);
    }

    /**
     * 使用错误消息和原因构造集群授权异常
     * 
     * 实现说明：
     * - 调用父类构造函数传递错误消息和原因
     * - 用于需要保留原始异常信息的场景
     *
     * @param message 描述集群授权失败原因的消息
     * @param cause 导致授权失败的原始异常
     */
    public ClusterAuthorizationException(String message, Throwable cause) {
        // 调用父类构造函数，传递错误消息和原因
        super(message, cause);
    }
}
