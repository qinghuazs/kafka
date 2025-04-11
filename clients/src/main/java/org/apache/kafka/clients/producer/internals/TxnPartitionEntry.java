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
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import java.util.Comparator;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * TxnPartitionEntry类用于管理Kafka生产者中单个分区的事务状态
 * 它维护了分区级别的序列号、已确认的消息批次和偏移量等信息
 * 主要用于实现Kafka的精确一次语义和幂等性
 */
class TxnPartitionEntry {
    // 表示没有最后确认序列号的常量值
    static final int NO_LAST_ACKED_SEQUENCE_NUMBER = -1;

    // 当前事务分区条目所对应的主题分区
    private final TopicPartition topicPartition;

    // 当前分区使用的生产者ID和epoch
    // 用于唯一标识生产者会话，确保消息的幂等性
    private ProducerIdAndEpoch producerIdAndEpoch;

    // 下一个待发送批次的基础序列号
    // 用于确保消息按顺序发送和处理
    private int nextSequence;

    // 最后一个已确认批次中最后一条记录的序列号
    // 当分区没有正在传输的请求时，lastAckedSequence = nextSequence - 1
    private int lastAckedSequence;

    // 追踪当前正在传输的消息批次，按序列号排序
    // 这有助于在leader故障转移期间响应乱序返回时
    // 仍然能够按照序列号顺序处理批次
    // 批次在被消费时加入队列，在完成（成功或失败）时移除
    private SortedSet<ProducerBatch> inflightBatchesBySequence;

    // 记录每个分区最后确认的偏移量
    // 用于区分UnknownProducer响应是由于保留期过期导致的
    // 还是由于实际数据丢失导致的
    private long lastAckedOffset;

    // `inflightBatchesBySequence` should only have batches with the same producer id and producer
    // epoch, but there is an edge case where we may remove the wrong batch if the comparator
    // only takes `baseSequence` into account.
    // See https://github.com/apache/kafka/pull/12096#pullrequestreview-955554191 for details.
    private static final Comparator<ProducerBatch> PRODUCER_BATCH_COMPARATOR =
        Comparator.comparingLong(ProducerBatch::producerId)
            .thenComparingInt(ProducerBatch::producerEpoch)
            .thenComparingInt(ProducerBatch::baseSequence);

    /**
     * 构造函数，初始化事务分区条目
     * @param topicPartition 要管理的主题分区
     */
    TxnPartitionEntry(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
        this.nextSequence = 0;
        this.lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
        this.lastAckedOffset = ProduceResponse.INVALID_OFFSET;
        this.inflightBatchesBySequence = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
    }

    /**
     * 获取当前分区使用的生产者ID和epoch
     * @return 当前的ProducerIdAndEpoch对象
     */
    ProducerIdAndEpoch producerIdAndEpoch() {
        return producerIdAndEpoch;
    }

    /**
     * 获取下一个待发送批次的序列号
     * @return 下一个序列号
     */
    int nextSequence() {
        return nextSequence;
    }

    /**
     * 获取最后确认的偏移量
     * @return 如果有最后确认的偏移量则返回该值，否则返回空
     */
    OptionalLong lastAckedOffset() {
        if (lastAckedOffset != ProduceResponse.INVALID_OFFSET)
            return OptionalLong.of(lastAckedOffset);
        return OptionalLong.empty();
    }

    /**
     * 获取最后确认的序列号
     * @return 如果有最后确认的序列号则返回该值，否则返回空
     */
    OptionalInt lastAckedSequence() {
        if (lastAckedSequence != TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER)
            return OptionalInt.of(lastAckedSequence);
        return OptionalInt.empty();
    }

    /**
     * 检查是否有正在传输的批次
     * @return 如果有正在传输的批次返回true，否则返回false
     */
    boolean hasInflightBatches() {
        return !inflightBatchesBySequence.isEmpty();
    }

    /**
     * 获取序列号最小的正在传输的批次
     * @return 如果有正在传输的批次则返回第一个批次，否则返回null
     */
    ProducerBatch nextBatchBySequence() {
        return inflightBatchesBySequence.isEmpty() ? null : inflightBatchesBySequence.first();
    }

    /**
     * 增加序列号
     * @param increment 要增加的值
     */
    void incrementSequence(int increment) {
        this.nextSequence = DefaultRecordBatch.incrementSequence(this.nextSequence, increment);
    }

    /**
     * 添加一个正在传输的批次
     * @param batch 要添加的批次
     */
    void addInflightBatch(ProducerBatch batch) {
        inflightBatchesBySequence.add(batch);
    }

    /**
     * 设置最后确认的偏移量
     * @param lastAckedOffset 新的最后确认偏移量
     */
    void setLastAckedOffset(long lastAckedOffset) {
        this.lastAckedOffset = lastAckedOffset;
    }

    /**
     * 使用新的生产者ID和epoch重新开始序列号
     * 重置所有正在传输的批次的生产者状态
     * @param newProducerIdAndEpoch 新的生产者ID和epoch
     */
    void startSequencesAtBeginning(ProducerIdAndEpoch newProducerIdAndEpoch) {
        final PrimitiveRef.IntRef sequence = PrimitiveRef.ofInt(0);
        resetSequenceNumbers(inFlightBatch -> {
            inFlightBatch.resetProducerState(newProducerIdAndEpoch, sequence.value);
            sequence.value += inFlightBatch.recordCount;
        });
        producerIdAndEpoch = newProducerIdAndEpoch;
        nextSequence = sequence.value;
        lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
    }

    /**
     * 尝试更新最后确认的序列号
     * @param sequence 新的序列号
     * @return 更新后的最后确认序列号
     */
    int maybeUpdateLastAckedSequence(int sequence) {
        if (sequence > lastAckedSequence) {
            lastAckedSequence = sequence;
            return sequence;
        }
        return lastAckedSequence;
    }

    /**
     * 移除一个正在传输的批次
     * @param batch 要移除的批次
     */
    void removeInFlightBatch(ProducerBatch batch) {
        inflightBatchesBySequence.remove(batch);
    }

    /**
     * 因批次失败而调整序列号
     * 减少下一个序列号并重置所有受影响批次的序列号
     * @param baseSequence 失败批次的基础序列号
     * @param recordCount 失败批次中的记录数
     */
    void adjustSequencesDueToFailedBatch(long baseSequence, int recordCount) {
        decrementSequence(recordCount);
        resetSequenceNumbers(inFlightBatch -> {
            if (inFlightBatch.baseSequence() < baseSequence)
                return;

            int newSequence = inFlightBatch.baseSequence() - recordCount;
            if (newSequence < 0)
                throw new IllegalStateException("Sequence number for batch with sequence " + inFlightBatch.baseSequence()
                        + " for partition " + topicPartition + " is going to become negative: " + newSequence);

            inFlightBatch.resetProducerState(new ProducerIdAndEpoch(inFlightBatch.producerId(), inFlightBatch.producerEpoch()), newSequence);
        });
    }

    /**
     * 重置所有正在传输批次的序列号
     * @param resetSequence 用于重置每个批次序列号的函数
     */
    private void resetSequenceNumbers(Consumer<ProducerBatch> resetSequence) {
        TreeSet<ProducerBatch> newInflights = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
        for (ProducerBatch inflightBatch : inflightBatchesBySequence) {
            resetSequence.accept(inflightBatch);
            newInflights.add(inflightBatch);
        }
        inflightBatchesBySequence = newInflights;
    }

    /**
     * 减少序列号
     * @param decrement 要减少的值
     * @return 操作是否成功
     * @throws IllegalStateException 如果减少后的序列号小于0
     */
    private boolean decrementSequence(int decrement) {
        int updatedSequence = nextSequence;
        updatedSequence -= decrement;
        if (updatedSequence < 0) {
            throw new IllegalStateException(
                    "Sequence number for partition " + topicPartition + " is going to become negative: "
                            + updatedSequence);
        }
        this.nextSequence = updatedSequence;
        return true;
    }
}
