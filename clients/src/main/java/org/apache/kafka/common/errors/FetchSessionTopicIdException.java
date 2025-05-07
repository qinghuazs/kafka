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
 * Fetch会话主题ID异常。当Fetch会话中使用的主题ID与Broker端记录的不匹配时抛出此异常。
 * 
 * 应用场景：
 * 1. Kafka 3.0引入了不可变的主题ID，用于唯一标识主题，替代主题名称
 * 2. 在增量式Fetch请求中，通过主题ID而不是主题名称来标识主题
 * 
 * 触发条件：
 * 1. Fetch会话中使用了错误或过期的主题ID
 * 2. 主题被删除后重新创建，导致主题ID发生变化
 * 
 * 处理机制：
 * 1. 消费者需要刷新主题元数据，获取最新的主题ID
 * 2. 重新创建Fetch会话，使用正确的主题ID
 * 
 * 设计考虑：
 * 1. 提供更可靠的主题标识机制，避免主题重命名带来的问题
 * 2. 作为可重试异常，允许客户端在获取新元数据后重试
 */
public class FetchSessionTopicIdException extends RetriableException {
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个新的Fetch会话主题ID异常
     * @param message 异常描述信息
     */
    public FetchSessionTopicIdException(String message) {
        super(message);
    }
}