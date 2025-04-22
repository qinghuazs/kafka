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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * 当消费者请求的偏移量超出服务器为给定分区维护的偏移量范围，且没有配置重置策略时抛出此异常。
 * 这种情况通常发生在以下场景：
 * 1. 消费者请求的偏移量大于分区中最新的偏移量
 * 2. 消费者请求的偏移量小于分区中最早的偏移量（由于日志清理导致早期消息被删除）
 * 3. 消费者配置中未设置auto.offset.reset属性来处理越界情况
 */
public class OffsetOutOfRangeException extends InvalidOffsetException {

    // 序列化版本号，用于序列化和反序列化时的版本控制
    private static final long serialVersionUID = 1L;
    
    // 存储所有偏移量越界的分区及其对应的越界偏移量
    private final Map<TopicPartition, Long> offsetOutOfRangePartitions;

    /**
     * 构造函数，创建一个包含偏移量越界分区信息的异常
     * @param offsetOutOfRangePartitions 包含所有偏移量越界的分区及其对应的越界偏移量的映射
     */
    public OffsetOutOfRangeException(Map<TopicPartition, Long> offsetOutOfRangePartitions) {
        this("Offsets out of range with no configured reset policy for partitions: " +
            offsetOutOfRangePartitions, offsetOutOfRangePartitions);
    }

    /**
     * 构造函数，创建一个包含自定义错误消息和偏移量越界分区信息的异常
     * @param message 自定义错误消息
     * @param offsetOutOfRangePartitions 包含所有偏移量越界的分区及其对应的越界偏移量的映射
     */
    public OffsetOutOfRangeException(String message, Map<TopicPartition, Long> offsetOutOfRangePartitions) {
        super(message);
        this.offsetOutOfRangePartitions = offsetOutOfRangePartitions;
    }

    /**
     * 获取所有偏移量越界的分区及其对应的越界偏移量
     * @return 返回一个Map，key为主题分区，value为导致越界的偏移量值
     */
    public Map<TopicPartition, Long> offsetOutOfRangePartitions() {
        return offsetOutOfRangePartitions;
    }

    /**
     * 获取所有偏移量越界的分区集合
     * @return 返回所有发生偏移量越界的主题分区集合
     */
    @Override
    public Set<TopicPartition> partitions() {
        return offsetOutOfRangePartitions.keySet();
    }
}
