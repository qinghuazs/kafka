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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

/**
 * Fetch类表示从Kafka broker获取的一批消息记录。
 * 这个类是Kafka消费者的核心组件，用于管理从服务器拉取的消息及其相关元数据。
 * 
 * 主要功能：
 * 1. 管理消息记录：按主题分区组织和存储消息记录
 * 2. 跟踪消费位置：记录消费进度和位置更新
 * 3. 元数据管理：维护每个分区的偏移量和元数据信息
 * 
 * @param <K> 消息记录的键类型
 * @param <V> 消息记录的值类型
 */
public class Fetch<K, V> {
    // 存储按主题分区组织的消息记录
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    // 存储每个分区的下一个消费位置和元数据信息
    private final Map<TopicPartition, OffsetAndMetadata> nextOffsetAndMetadata;
    // 标记是否有分区的消费位置发生了前进
    private boolean positionAdvanced;
    // 记录总的消息数量
    private int numRecords;

    /**
     * 创建一个空的Fetch实例
     * 用于初始化或表示没有数据的情况
     * 
     * @param <K> 消息记录的键类型
     * @param <V> 消息记录的值类型
     * @return 返回一个不包含任何记录的Fetch实例
     */
    public static <K, V> Fetch<K, V> empty() {
        // 创建一个空的Fetch对象，所有字段都初始化为空或默认值
        return new Fetch<>(new HashMap<>(), false, 0, new HashMap<>());
    }

    /**
     * 为单个分区创建Fetch实例
     * 用于封装从特定分区获取的消息记录和相关元数据
     * 
     * @param partition 目标主题分区
     * @param records 从该分区获取的消息记录列表
     * @param positionAdvanced 是否更新了消费位置
     * @param nextOffsetAndMetadata 下一个要消费的偏移量和元数据
     * @return 返回包含指定分区数据的Fetch实例
     */
    public static <K, V> Fetch<K, V> forPartition(
            TopicPartition partition,
            List<ConsumerRecord<K, V>> records,
            boolean positionAdvanced,
            OffsetAndMetadata nextOffsetAndMetadata
    ) {
        // 如果记录列表为空，创建空的记录映射，否则创建包含该分区记录的映射
        Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsMap = records.isEmpty()
                ? new HashMap<>()
                : mkMap(mkEntry(partition, records));
        // 创建包含该分区下一个偏移量的映射
        Map<TopicPartition, OffsetAndMetadata> nextOffsetAndMetadataMap = mkMap(mkEntry(partition, nextOffsetAndMetadata));
        // 返回新的Fetch实例
        return new Fetch<>(recordsMap, positionAdvanced, records.size(), nextOffsetAndMetadataMap);
    }

    /**
     * Fetch类的私有构造函数
     * 用于创建一个新的Fetch实例，初始化所有必要的字段
     * 
     * @param records 按主题分区组织的消息记录映射
     * @param positionAdvanced 是否有分区的消费位置发生了前进
     * @param numRecords 消息记录的总数
     * @param nextOffsetAndMetadata 每个分区的下一个消费位置和元数据信息
     */
    private Fetch(
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records,
            boolean positionAdvanced,
            int numRecords,
            Map<TopicPartition, OffsetAndMetadata> nextOffsetAndMetadata
    ) {
        // 初始化所有字段
        this.records = records;
        this.positionAdvanced = positionAdvanced;
        this.numRecords = numRecords;
        this.nextOffsetAndMetadata = nextOffsetAndMetadata;
    }

    /**
     * Add another {@link Fetch} to this one; all of its records will be added to this fetch's
     * {@link #records() records}, and if the other fetch
     * {@link #positionAdvanced() advanced the consume position for any topic partition},
     * this fetch will be marked as having advanced the consume position as well.
     * @param fetch the other fetch to add; may not be null
     */
    /**
     * 将另一个Fetch对象的内容合并到当前对象中
     * 合并过程包括记录、位置状态和元数据的合并
     * 
     * @param fetch 要合并的Fetch对象，不能为null
     */
    public void add(Fetch<K, V> fetch) {
        // 确保传入的fetch对象不为null
        Objects.requireNonNull(fetch);
        // 合并消息记录
        addRecords(fetch.records);
        // 更新位置状态，如果任一fetch对象的位置前进了，则合并后的对象位置也前进
        this.positionAdvanced |= fetch.positionAdvanced;
        // 合并偏移量元数据
        this.nextOffsetAndMetadata.putAll(fetch.nextOffsetAndMetadata);
    }

    /**
     * @return all of the non-control messages for this fetch, grouped by partition
     */
    /**
     * 获取所有非控制消息记录
     * 返回按主题分区组织的消息记录的不可修改视图
     * 
     * @return 返回一个不可修改的Map，键为主题分区，值为该分区的消息记录列表
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> records() {
        // 返回记录映射的不可修改视图，防止外部修改
        return Collections.unmodifiableMap(records);
    }

    /**
     * @return whether the fetch caused the consumer's
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#position(TopicPartition) position} to advance for at
     * least one of the topic partitions in this fetch
     */
    /**
     * 检查是否有分区的消费位置发生了前进
     * 用于跟踪消费进度的变化
     * 
     * @return 如果至少有一个分区的消费位置前进了，返回true
     */
    public boolean positionAdvanced() {
        // 返回位置前进标志
        return positionAdvanced;
    }

    /**
     * @return the total number of non-control messages for this fetch, across all partitions
     */
    /**
     * 获取当前Fetch中的消息记录总数
     * 包括所有分区的非控制消息
     * 
     * @return 返回消息记录的总数
     */
    public int numRecords() {
        // 返回记录总数
        return numRecords;
    }

    /**
     * @return the next offsets and metadata that the consumer will consume (last epoch is included)
     */
    /**
     * 获取每个分区的下一个消费位置和元数据信息
     * 这些信息用于追踪消费进度和恢复点
     * 
     * @return 返回一个新的Map副本，包含每个分区的下一个偏移量和元数据
     */
    public Map<TopicPartition, OffsetAndMetadata> nextOffsets() {
        // 返回偏移量映射的不可变副本
        return Map.copyOf(nextOffsetAndMetadata);
    }

    /**
     * @return {@code true} if and only if this fetch did not return any user-visible (i.e., non-control) records, and
     * did not cause the consumer position to advance for any topic partitions
     */
    /**
     * 检查当前Fetch是否为空
     * 当没有用户可见的记录且消费位置没有前进时，认为是空的
     * 
     * @return 如果没有记录且位置没有前进，返回true
     */
    public boolean isEmpty() {
        // 检查是否既没有记录也没有位置前进
        return numRecords == 0 && !positionAdvanced;
    }

    /**
     * 将新的记录添加到现有的记录集合中
     * 这是一个内部方法，用于合并来自不同fetch的记录
     * 
     * @param records 要添加的新记录映射
     */
    private void addRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        records.forEach((partition, partRecords) -> {
            // 更新记录总数
            this.numRecords += partRecords.size();
            // 获取当前分区的记录列表
            List<ConsumerRecord<K, V>> currentRecords = this.records.get(partition);
            if (currentRecords == null) {
                // 如果当前分区没有记录，直接添加新记录
                this.records.put(partition, partRecords);
            } else {
                // 如果当前分区已有记录，需要合并记录列表
                // 这种情况在分区leader变更等特殊情况下可能发生
                // 创建新的可变列表，因为现有列表可能是不可变的
                List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(currentRecords.size() + partRecords.size());
                newRecords.addAll(currentRecords); // 添加现有记录
                newRecords.addAll(partRecords);    // 添加新记录
                this.records.put(partition, newRecords); // 更新记录映射
            }
        });
    }
}
