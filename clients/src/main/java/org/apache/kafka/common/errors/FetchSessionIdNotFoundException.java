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
 * Fetch会话ID未找到异常。当消费者尝试使用不存在或已过期的Fetch会话ID进行消息获取时抛出此异常。
 * 
 * 应用场景：
 * 1. Kafka的消息获取优化机制中，使用会话来减少重复的元数据传输
 * 2. 消费者可以复用上一次Fetch请求的会话信息，提高效率
 * 
 * 触发条件：
 * 1. 使用已过期或无效的Fetch会话ID
 * 2. Broker重启后，之前的会话信息丢失
 * 
 * 处理机制：
 * 1. 消费者需要创建新的Fetch会话
 * 2. 重新发送完整的Fetch请求，包含所有必要的元数据
 * 
 * 设计考虑：
 * 1. 通过会话机制优化消息获取性能
 * 2. 异常可重试，允许消费者重新建立会话
 */
public class FetchSessionIdNotFoundException extends RetriableException {
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个新的Fetch会话ID未找到异常
     */
    public FetchSessionIdNotFoundException() {
    }

    /**
     * 创建一个新的Fetch会话ID未找到异常
     * @param message 异常描述信息
     */
    public FetchSessionIdNotFoundException(String message) {
        super(message);
    }
}
