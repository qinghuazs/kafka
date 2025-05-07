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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ShareFetch类表示从broker获取的记录，这些记录将返回给消费者以满足
 * {@link KafkaShareConsumer#poll(Duration)}调用。这些记录可能来自多个主题分区。
 * 
 * 应用场景：
 * 1. 管理从broker获取的消息批次
 * 2. 处理多分区的消息获取
 * 3. 支持消息确认机制
 * 4. 维护消息的处理状态
 *
 * @param <K> 记录键的类型
 * @param <V> 记录值的类型
 */
public class ShareFetch<K, V> {
    /**
     * 存储主题分区ID到正在处理的消息批次的映射
     * 用于跟踪每个分区的消息处理状态
     */
    private final Map<TopicIdPartition, ShareInFlightBatch<K, V>> batches;

    /**
     * 创建一个空的ShareFetch实例
     * 用于初始化没有任何消息批次的情况
     *
     * @return 新的空ShareFetch实例
     */
    public static <K, V> ShareFetch<K, V> empty() {
        // 创建一个带有空HashMap的新实例
        return new ShareFetch<>(new HashMap<>());
    }

    /**
     * 私有构造函数
     * 用于创建ShareFetch实例
     *
     * @param batches 初始的消息批次映射
     */
    private ShareFetch(Map<TopicIdPartition, ShareInFlightBatch<K, V>> batches) {
        // 初始化批次映射
        this.batches = batches;
    }

    /**
     * 添加另一个ShareInFlightBatch到当前实例
     * 所有记录都将添加到此对象的records()中
     *
     * @param partition 主题分区
     * @param batch 要添加的批次，不能为null
     */
    public void add(TopicIdPartition partition, ShareInFlightBatch<K, V> batch) {
        // 检查批次是否为null
        Objects.requireNonNull(batch);
        // 获取当前分区的批次
        ShareInFlightBatch<K, V> currentBatch = this.batches.get(partition);
        if (currentBatch == null) {
            // 如果当前分区没有批次，直接添加新批次
            this.batches.put(partition, batch);
        } else {
            // 如果当前分区已有批次，合并新批次
            // 这种情况通常不会发生，因为我们每次只对每个分区发送一个获取请求
            // 但在某些罕见情况下可能发生（如分区leader变更）
            currentBatch.merge(batch);
        }
    }

    /**
     * 获取此获取操作的所有非控制消息，按分区分组
     *
     * @return 按分区分组的消息记录映射
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> records() {
        // 创建结果映射，使用LinkedHashMap保持顺序
        final LinkedHashMap<TopicPartition, List<ConsumerRecord<K, V>>> result = new LinkedHashMap<>();
        // 遍历所有批次，将记录添加到结果映射中
        batches.forEach((tip, batch) -> result.put(tip.topicPartition(), batch.getInFlightRecords()));
        // 返回不可修改的映射视图
        return Collections.unmodifiableMap(result);
    }

    /**
     * 获取此获取操作中所有分区的非控制消息总数
     *
     * @return 消息记录总数
     */
    public int numRecords() {
        int numRecords = 0;
        if (!batches.isEmpty()) {
            // 获取批次映射的迭代器
            Iterator<Map.Entry<TopicIdPartition, ShareInFlightBatch<K, V>>> iterator = batches.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TopicIdPartition, ShareInFlightBatch<K, V>> entry = iterator.next();
                ShareInFlightBatch<K, V> batch = entry.getValue();
                if (batch.isEmpty()) {
                    // 如果批次为空，移除该批次
                    iterator.remove();
                } else {
                    // 累加批次中的记录数
                    numRecords += batch.numRecords();
                }
            }
        }
        return numRecords;
    }

    /**
     * 检查此获取操作是否没有返回任何非控制记录
     *
     * @return 如果没有非控制记录则返回true
     */
    public boolean isEmpty() {
        // 通过检查记录总数来判断是否为空
        return numRecords() == 0;
    }

    /**
     * 确认当前批次中的单个记录
     *
     * @param record 要确认的记录
     * @param type 确认类型，表示处理是否成功
     */
    public void acknowledge(final ConsumerRecord<K, V> record, AcknowledgeType type) {
        // 遍历所有批次，查找匹配的主题和分区
        for (Map.Entry<TopicIdPartition, ShareInFlightBatch<K, V>> tipBatch : batches.entrySet()) {
            TopicIdPartition tip = tipBatch.getKey();
            if (tip.topic().equals(record.topic()) && (tip.partition() == record.partition())) {
                // 找到匹配的批次后确认记录
                tipBatch.getValue().acknowledge(record, type);
                return;
            }
        }
        // 如果没有找到匹配的批次，抛出异常
        throw new IllegalStateException("The record cannot be acknowledged.");
    }

    /**
     * 确认当前批次中的所有记录
     * 如果批次中的某些记录已经被确认，这些确认不会被覆盖
     *
     * @param type 确认类型，表示处理是否成功
     */
    public void acknowledgeAll(final AcknowledgeType type) {
        // 对所有批次执行全部确认操作
        batches.forEach((tip, batch) -> batch.acknowledgeAll(type));
    }

    /**
     * 移除所有已确认的记录并返回要发送的确认映射
     * 如果某些记录未被确认，则处理中的记录在此方法之后不会为空
     *
     * @return 要发送的确认映射
     */
    public Map<TopicIdPartition, Acknowledgements> takeAcknowledgedRecords() {
        // 创建确认映射
        Map<TopicIdPartition, Acknowledgements> acknowledgementMap = new LinkedHashMap<>();
        // 遍历所有批次，收集确认信息
        batches.forEach((tip, batch) -> {
            Acknowledgements acknowledgements = batch.takeAcknowledgedRecords();
            // 只添加非空的确认信息
            if (!acknowledgements.isEmpty())
                acknowledgementMap.put(tip, acknowledgements);
        });
        return acknowledgementMap;
    }
}
