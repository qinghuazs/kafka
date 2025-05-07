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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 共享处理中批次类
 * 用于管理正在处理中的消息批次及其确认状态。这个类跟踪单个分区中正在处理的记录，
 * 并维护这些记录的确认状态。
 *
 * 应用场景：
 * 1. 消息批量处理
 * 2. 消息确认管理
 * 3. 异常处理和缓存
 * 4. 批次合并操作
 *
 * @param <K> 消息键的类型
 * @param <V> 消息值的类型
 */
public class ShareInFlightBatch<K, V> {
    /**
     * 此批次所属的主题分区标识符
     */
    final TopicIdPartition partition;

    /**
     * 存储正在处理中的记录，键为偏移量，值为消费者记录
     */
    private final Map<Long, ConsumerRecord<K, V>> inFlightRecords;

    /**
     * 存储已确认记录的偏移量集合
     */
    private final Set<Long> acknowledgedRecords;

    /**
     * 存储记录确认信息
     */
    private Acknowledgements acknowledgements;

    /**
     * 存储处理过程中发生的Kafka异常
     */
    private KafkaException exception;

    /**
     * 标记是否有已缓存的异常
     */
    private boolean hasCachedException = false;

    /**
     * 构造函数
     * 创建一个新的共享处理中批次实例
     *
     * @param partition 主题分区标识符
     */
    public ShareInFlightBatch(TopicIdPartition partition) {
        // 初始化分区标识符
        this.partition = partition;
        // 使用TreeMap保证记录按偏移量排序
        inFlightRecords = new TreeMap<>();
        // 使用TreeSet保证已确认记录按偏移量排序
        acknowledgedRecords = new TreeSet<>();
        // 初始化空的确认信息
        acknowledgements = Acknowledgements.empty();
    }

    /**
     * 添加确认信息
     *
     * @param offset 记录偏移量
     * @param acknowledgeType 确认类型
     */
    public void addAcknowledgement(long offset, AcknowledgeType acknowledgeType) {
        // 将确认信息添加到确认集合中
        acknowledgements.add(offset, acknowledgeType);
    }

    /**
     * 确认单个记录
     *
     * @param record 要确认的记录
     * @param type 确认类型
     * @throws IllegalStateException 如果记录不存在于处理中记录集合
     */
    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        // 检查记录是否存在于处理中记录集合
        if (inFlightRecords.get(record.offset()) != null) {
            // 添加确认信息
            acknowledgements.add(record.offset(), type);
            // 将偏移量添加到已确认记录集合
            acknowledgedRecords.add(record.offset());
            return;
        }
        // 如果记录不存在，抛出异常
        throw new IllegalStateException("The record cannot be acknowledged.");
    }

    /**
     * 确认所有未确认的记录
     *
     * @param type 确认类型
     * @return 新确认的记录数量
     */
    public int acknowledgeAll(AcknowledgeType type) {
        int recordsAcknowledged = 0;
        // 遍历所有处理中的记录
        for (Map.Entry<Long, ConsumerRecord<K, V>> entry : inFlightRecords.entrySet()) {
            // 尝试添加确认信息，如果成功则增加计数
            if (acknowledgements.addIfAbsent(entry.getKey(), type)) {
                acknowledgedRecords.add(entry.getKey());
                recordsAcknowledged++;
            }
        }
        return recordsAcknowledged;
    }

    /**
     * 添加新记录到处理中记录集合
     *
     * @param record 要添加的记录
     */
    public void addRecord(ConsumerRecord<K, V> record) {
        // 将记录添加到处理中记录集合，使用偏移量作为键
        inFlightRecords.put(record.offset(), record);
    }

    /**
     * 添加偏移量间隙
     *
     * @param offset 间隙偏移量
     */
    public void addGap(long offset) {
        // 在确认信息中添加间隙标记
        acknowledgements.addGap(offset);
    }

    /**
     * 合并另一个批次的记录
     *
     * @param other 要合并的批次
     */
    public void merge(ShareInFlightBatch<K, V> other) {
        // 将其他批次的所有记录添加到当前批次
        inFlightRecords.putAll(other.inFlightRecords);
    }

    /**
     * 获取所有处理中的记录
     *
     * @return 处理中记录的列表副本
     */
    List<ConsumerRecord<K, V>> getInFlightRecords() {
        // 返回处理中记录的新列表副本
        return new ArrayList<>(inFlightRecords.values());
    }

    /**
     * 获取处理中记录的数量
     *
     * @return 记录数量
     */
    int numRecords() {
        // 返回处理中记录的数量
        return inFlightRecords.size();
    }

    /**
     * 获取并清除已确认的记录
     *
     * @return 当前的确认信息
     */
    Acknowledgements takeAcknowledgedRecords() {
        // 如果所有记录都已确认，直接清空处理中记录集合
        if (acknowledgedRecords.size() == inFlightRecords.size()) {
            inFlightRecords.clear();
        } else {
            // 否则只移除已确认的记录
            acknowledgedRecords.forEach(inFlightRecords::remove);
        }
        // 清空已确认记录集合
        acknowledgedRecords.clear();

        // 获取当前确认信息并重置
        Acknowledgements currentAcknowledgements = acknowledgements;
        acknowledgements = Acknowledgements.empty();
        return currentAcknowledgements;
    }

    /**
     * 获取当前的确认信息
     *
     * @return 确认信息
     */
    Acknowledgements getAcknowledgements() {
        return acknowledgements;
    }

    /**
     * 检查批次是否为空
     *
     * @return 如果没有处理中的记录且没有确认信息则返回true
     */
    public boolean isEmpty() {
        return inFlightRecords.isEmpty() && acknowledgements.isEmpty();
    }

    /**
     * 设置异常
     *
     * @param exception Kafka异常
     */
    public void setException(KafkaException exception) {
        this.exception = exception;
    }

    /**
     * 获取异常
     *
     * @return 存储的Kafka异常
     */
    public KafkaException getException() {
        return exception;
    }

    /**
     * 设置是否有已缓存的异常
     *
     * @param hasCachedException 是否有已缓存的异常
     */
    public void setHasCachedException(boolean hasCachedException) {
        this.hasCachedException = hasCachedException;
    }

    /**
     * 检查是否有已缓存的异常
     *
     * @return 如果有已缓存的异常则返回true
     */
    public boolean hasCachedException() {
        return hasCachedException;
    }
}
