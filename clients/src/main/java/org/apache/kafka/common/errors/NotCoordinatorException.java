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
 * 此异常在以下两种场景中会被抛出：
 * 
 * 1. 消费者组协调器（Group Coordinator）场景：
 *    - 当broker收到针对某个消费者组的偏移量获取或提交请求
 *    - 但该broker并不是这个消费者组的协调器时
 *    - 例如：消费者组成员向错误的broker发送了加入组或提交偏移量的请求
 * 
 * 2. 事务协调器（Transaction Coordinator）场景：
 *    - 当broker收到包含特定transactionalId的事务请求
 *    - 但该broker并不是负责这个transactionalId的协调器时
 *    - 例如：生产者向错误的broker发送了事务控制请求
 * 
 * 设计考虑：
 * 1. 继承自RetriableException表明这是一个可重试的异常
 * 2. 协调器的分配是基于一致性哈希，可能会因为broker的增减而改变
 * 3. 客户端应该通过重新获取元数据来找到正确的协调器
 */
public class NotCoordinatorException extends RetriableException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息创建异常实例
     * @param message 描述异常的详细信息，通常包含当前broker不是协调器的说明
     */
    public NotCoordinatorException(String message) {
        super(message);
    }

    /**
     * 使用错误消息和原始异常创建异常实例
     * @param message 描述异常的详细信息
     * @param cause 导致此异常的原始异常
     */
    public NotCoordinatorException(String message, Throwable cause) {
        super(message, cause);
    }

}
