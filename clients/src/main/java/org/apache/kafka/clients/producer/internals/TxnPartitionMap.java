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

package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * TxnPartitionMap类用于管理Kafka生产者的事务分区状态
 * 它维护了每个主题分区的事务状态信息，包括已确认的偏移量、序列号等
 * 主要用于确保事务消息的精确一次语义和幂等性
 */
class TxnPartitionMap {

    // 日志记录器，用于记录事务分区状态的变更和异常情况
    private final Logger log;

    // 存储主题分区与其对应的事务状态信息的映射关系
    // 每个TopicPartition都对应一个TxnPartitionEntry，包含该分区的详细事务状态
    private final Map<TopicPartition, TxnPartitionEntry> topicPartitions = new HashMap<>();

    /**
     * 构造函数
     * @param logContext 日志上下文，用于创建针对该类的日志记录器
     */
    TxnPartitionMap(LogContext logContext) {
        this.log = logContext.logger(TxnPartitionMap.class);
    }

    /**
     * 获取指定主题分区的事务状态信息
     * @param topicPartition 目标主题分区
     * @return 该分区的事务状态信息
     * @throws IllegalStateException 如果该分区的事务状态信息不存在
     */
    TxnPartitionEntry get(TopicPartition topicPartition) {
        // 从映射中获取分区的事务状态
        TxnPartitionEntry ent = topicPartitions.get(topicPartition);
        if (ent == null) {
            // 如果状态不存在，说明这个分区还未初始化事务状态，抛出异常
            throw new IllegalStateException("Trying to get txnPartitionEntry for " + topicPartition +
                ", but it was never set for this partition.");
        }
        return ent;
    }

    /**
     * 获取或创建指定主题分区的事务状态信息
     * @param topicPartition 目标主题分区
     * @return 该分区的事务状态信息，如果不存在则新建一个
     */
    TxnPartitionEntry getOrCreate(TopicPartition topicPartition) {
        // 如果分区的事务状态不存在，则创建一个新的TxnPartitionEntry
        return topicPartitions.computeIfAbsent(topicPartition, TxnPartitionEntry::new);
    }

    /**
     * 检查是否包含指定主题分区的事务状态信息
     * @param topicPartition 要检查的主题分区
     * @return 如果存在该分区的事务状态信息则返回true，否则返回false
     */
    boolean contains(TopicPartition topicPartition) {
        // 检查映射中是否包含指定分区的状态信息
        return topicPartitions.containsKey(topicPartition);
    }

    /**
     * 重置所有分区的事务状态信息
     * 通常在事务重启或发生严重错误需要重置状态时调用
     */
    void reset() {
        // 清空所有分区的事务状态信息
        topicPartitions.clear();
    }

    /**
     * 获取指定分区最后一个已确认消息的偏移量
     * @param topicPartition 目标主题分区
     * @return 最后确认的偏移量，如果没有则返回空
     */
    OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        // 获取分区的事务状态
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null)
            // 如果状态存在，返回最后确认的偏移量
            return entry.lastAckedOffset();
        // 如果状态不存在，返回空
        return OptionalLong.empty();
    }

    /**
     * 获取指定分区最后一个已确认消息的序列号
     * @param topicPartition 目标主题分区
     * @return 最后确认的序列号，如果没有则返回空
     */
    OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        // 获取分区的事务状态
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null)
            // 如果状态存在，返回最后确认的序列号
            return entry.lastAckedSequence();
        // 如果状态不存在，返回空
        return OptionalInt.empty();
    }

    /**
     * 重置指定分区的序列号，使用新的生产者ID和epoch开始新的序列
     * @param topicPartition 目标主题分区
     * @param newProducerIdAndEpoch 新的生产者ID和epoch信息
     */
    void startSequencesAtBeginning(TopicPartition topicPartition, ProducerIdAndEpoch newProducerIdAndEpoch) {
        // 获取分区的事务状态
        TxnPartitionEntry entry = get(topicPartition);
        if (entry != null)
            // 如果状态存在，使用新的生产者ID和epoch重置序列号
            entry.startSequencesAtBeginning(newProducerIdAndEpoch);
    }

    /**
     * 移除指定分区的事务状态信息
     * @param topicPartition 要移除的主题分区
     */
    void remove(TopicPartition topicPartition) {
        // 从映射中移除指定分区的事务状态
        topicPartitions.remove(topicPartition);
    }


    /**
     * 更新指定分区最后确认的偏移量
     * @param topicPartition 目标主题分区
     * @param isTransactional 是否是事务性消息
     * @param lastOffset 新的最后确认偏移量
     */
    void updateLastAckedOffset(TopicPartition topicPartition, boolean isTransactional, long lastOffset) {
        // 获取当前最后确认的偏移量
        OptionalLong lastAckedOffset = lastAckedOffset(topicPartition);
        // 如果是非事务性生产者且没有最后确认的偏移量记录
        // 这种情况可能发生在TransactionManager被重置而请求重新入队并得到有效响应时
        if (lastAckedOffset.isEmpty() && !isTransactional)
            // 创建新的事务状态记录
            getOrCreate(topicPartition);
        // 如果新的偏移量大于当前记录的最后偏移量
        if (lastOffset > lastAckedOffset.orElse(ProduceResponse.INVALID_OFFSET))
            // 更新最后确认的偏移量
            get(topicPartition).setLastAckedOffset(lastOffset);
        else
            // 如果新偏移量不大于当前记录的偏移量，记录跟踪日志
            log.trace("Partition {} keeps lastOffset at {}", topicPartition, lastOffset);
    }

    // If a batch is failed fatally, the sequence numbers for future batches bound for the partition must be adjusted
    // so that they don't fail with the OutOfOrderSequenceException.
    //
    // This method must only be called when we know that the batch is question has been unequivocally failed by the broker,
    // ie. it has received a confirmed fatal status code like 'Message Too Large' or something similar.
    /**
     * 调整因批次失败而需要更新的序列号
     * 当批次发生致命失败时，需要调整未来批次的序列号以避免OutOfOrderSequenceException
     * @param batch 失败的消息批次
     */
    void adjustSequencesDueToFailedBatch(ProducerBatch batch) {
        // 检查是否正在跟踪该分区的序列号
        if (!contains(batch.topicPartition))
            // 如果没有跟踪序列号（可能是因为生产者ID刚刚因为OutOfOrderSequenceException而重置）
            // 则直接返回
            return;
        // 记录调试日志，说明正在减少未来的序列号
        log.debug("producerId: {}, send to partition {} failed fatally. Reducing future sequence numbers by {}",
                batch.producerId(), batch.topicPartition, batch.recordCount);

        // 调整该分区的序列号
        get(batch.topicPartition).adjustSequencesDueToFailedBatch(batch.baseSequence(), batch.recordCount);
    }

    /**
     * 尝试更新最后确认的序列号
     * @param topicPartition 目标主题分区
     * @param sequence 新的序列号
     * @return 更新后的序列号，如果分区不存在则返回NO_LAST_ACKED_SEQUENCE_NUMBER
     */
    int maybeUpdateLastAckedSequence(TopicPartition topicPartition, int sequence) {
        // 获取分区的事务状态
        TxnPartitionEntry entry = topicPartitions.get(topicPartition);
        if (entry != null)
            // 如果状态存在，尝试更新最后确认的序列号
            return entry.maybeUpdateLastAckedSequence(sequence);
        // 如果状态不存在，返回特殊值表示没有最后确认的序列号
        return TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER;
    }

    /**
     * 获取指定分区按序列号排序的下一个待发送批次
     * @param topicPartition 目标主题分区
     * @return 下一个待发送的消息批次
     */
    ProducerBatch nextBatchBySequence(TopicPartition topicPartition) {
        // 获取分区的下一个待发送批次
        return get(topicPartition).nextBatchBySequence();
    }

    /**
     * 移除正在传输中的批次
     * 当批次发送完成或失败时调用此方法
     * @param batch 要移除的消息批次
     */
    void removeInFlightBatch(ProducerBatch batch) {
        // 从分区的事务状态中移除已完成传输的批次
        get(batch.topicPartition).removeInFlightBatch(batch);
    }
}
