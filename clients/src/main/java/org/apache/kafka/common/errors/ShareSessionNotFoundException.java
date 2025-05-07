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
 * 共享会话未找到异常。当尝试访问一个不存在的共享会话时抛出此异常。
 * 
 * 应用场景：
 * 1. 在分布式消费者组中，当尝试访问已过期或被清理的共享会话
 * 2. 在会话恢复过程中，原始会话信息已丢失
 * 3. 当多个消费者实例之间共享状态时，目标会话不可用
 * 
 * 触发条件：
 * 1. 使用无效或已过期的会话ID
 * 2. 系统重启后会话状态丢失
 * 3. 会话被其他进程或操作清理
 * 
 * 处理机制：
 * 1. 客户端需要重新创建共享会话
 * 2. 重新初始化会话状态
 * 
 * 设计考虑：
 * 1. 继承自RetriableException，表示这是一个可重试的异常
 * 2. 用于维护分布式环境下共享会话的状态一致性
 * 3. 支持故障恢复和会话重建机制
 */
public class ShareSessionNotFoundException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public ShareSessionNotFoundException(String message) {
        super(message);
    }
}
