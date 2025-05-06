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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于根据时间戳查询分区偏移量的事件类。
 * 通过执行{@link org.apache.kafka.common.requests.ListOffsetsRequest ListOffsetsRequest}来获取偏移量信息。
 * 
 * 该事件的主要功能：
 * 1. 接收一个包含TopicPartition和目标时间戳的映射，用于搜索特定时间点的偏移量
 * 2. 返回一个TopicPartition到OffsetAndTimestamp的映射，包含第一条时间戳大于等于目标时间戳的消息的偏移量
 * 
 * 应用场景：
 * - 消费者需要从特定时间点开始消费消息
 * - 需要确定某个时间点对应的消息位置
 * - 查找特定时间范围内的消息
 */
public class ListOffsetsEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndTimestampInternal>> {
    /**
     * 存储待查询偏移量的主题分区和对应的目标时间戳
     * 使用不可变Map确保线程安全和数据一致性
     */
    private final Map<TopicPartition, Long> timestampsToSearch;

    /**
     * 标识是否要求返回消息的时间戳
     * true：要求返回时间戳信息
     * false：不需要返回时间戳信息
     */
    private final boolean requireTimestamps;

    /**
     * 创建一个ListOffsetsEvent实例
     * 
     * @param timestampToSearch 包含TopicPartition和目标时间戳的映射
     * @param deadlineMs 事件处理的截止时间（毫秒）
     * @param requireTimestamps 是否需要返回时间戳信息
     */
    public ListOffsetsEvent(Map<TopicPartition, Long> timestampToSearch,
                            long deadlineMs,
                            boolean requireTimestamps) {
        // 调用父类构造器，设置事件类型为LIST_OFFSETS和处理截止时间
        super(Type.LIST_OFFSETS, deadlineMs);
        // 将时间戳映射转换为不可修改的Map，确保线程安全
        this.timestampsToSearch = Collections.unmodifiableMap(timestampToSearch);
        this.requireTimestamps = requireTimestamps;
    }

    /**
     * 构建表示未找到任何偏移量的结果
     * 
     * 实现细节：
     * 1. 创建一个新的HashMap用于存储结果
     * 2. 遍历所有待查询的分区
     * 3. 将每个分区映射到null值，表示未找到对应的偏移量
     *
     * @return 包含所有待查询分区的映射，每个分区对应的值为null
     */
    public <T> Map<TopicPartition, T> emptyResults() {
        Map<TopicPartition, T> result = new HashMap<>();
        timestampsToSearch.keySet().forEach(tp -> result.put(tp, null));
        return result;
    }

    /**
     * 获取待查询的时间戳映射
     * 
     * @return 不可修改的TopicPartition到时间戳的映射
     */
    public Map<TopicPartition, Long> timestampsToSearch() {
        return timestampsToSearch;
    }

    /**
     * 检查是否需要返回时间戳信息
     * 
     * @return 如果需要返回时间戳信息则为true，否则为false
     */
    public boolean requireTimestamps() {
        return requireTimestamps;
    }

    /**
     * 指示该事件是否需要订阅元数据
     * 对于偏移量查询，总是需要最新的元数据来确保准确性
     * 
     * @return 始终返回true，表示需要订阅元数据
     */
    @Override
    public boolean requireSubscriptionMetadata() {
        return true;
    }

    /**
     * 生成事件的字符串表示
     * 
     * @return 包含事件基本信息、时间戳映射和时间戳要求的字符串
     */
    @Override
    public String toStringBase() {
        return super.toStringBase() +
                ", timestampsToSearch=" + timestampsToSearch +
                ", requireTimestamps=" + requireTimestamps;
    }
}