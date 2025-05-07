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

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * 记录过大异常
 * 
 * 当单个记录的大小超过Kafka允许的最大限制时抛出此异常。
 * 记录大小限制的原因：
 * 1. 保证消息处理的效率
 * 2. 避免内存溢出风险
 * 3. 控制网络传输开销
 * 
 * 可能的触发场景：
 * - 生产者发送过大的单条消息
 * - 消息压缩后仍超过大小限制
 * - 消息头部过大
 * 
 * 处理建议：
 * - 检查消息大小配置（max.message.bytes）
 * - 考虑消息分片策略
 * - 优化消息格式和压缩方式
 */
public class RecordTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;
    
    /** 
     * 存储发生记录过大异常的主题分区及其对应的消息大小
     * key: 主题分区
     * value: 消息大小（字节数）
     */
    private Map<TopicPartition, Long> recordTooLargePartitions = null;

    /**
     * 默认构造函数
     */
    public RecordTooLargeException() {
        super();
    }

    /**
     * 构造函数
     *
     * @param message 异常消息
     * @param cause 导致此异常的原始异常
     */
    public RecordTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 构造函数
     *
     * @param message 异常消息
     */
    public RecordTooLargeException(String message) {
        super(message);
    }

    /**
     * 构造函数
     *
     * @param cause 导致此异常的原始异常
     */
    public RecordTooLargeException(Throwable cause) {
        super(cause);
    }

    /**
     * 构造函数
     *
     * @param message 异常消息
     * @param recordTooLargePartitions 发生记录过大异常的主题分区及其对应的消息大小映射
     */
    public RecordTooLargeException(String message, Map<TopicPartition, Long> recordTooLargePartitions) {
        super(message);
        this.recordTooLargePartitions = recordTooLargePartitions;
    }

    /**
     * 获取发生记录过大异常的主题分区信息
     *
     * @return 主题分区到消息大小的映射，如果不可用则返回null
     */
    public Map<TopicPartition, Long> recordTooLargePartitions() {
        return recordTooLargePartitions;
    }
}
