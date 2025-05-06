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
package org.apache.kafka.clients.admin;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * ProducerState类用于存储和管理Kafka生产者的状态信息。
 * 该类包含了生产者的唯一标识、生产者世代、最后发送的消息序列号等重要信息，
 * 这些信息对于实现Kafka的幂等性和事务特性至关重要。
 */
public class ProducerState {
    /** 生产者的唯一标识ID，用于在集群中唯一标识一个生产者实例 */
    private final long producerId;
    
    /** 生产者的世代编号，用于处理生产者故障恢复和重启场景，每次重启都会增加 */
    private final int producerEpoch;
    
    /** 该生产者发送的最后一条消息的序列号，用于消息去重和顺序保证 */
    private final int lastSequence;
    
    /** 最后一次更新状态的时间戳 */
    private final long lastTimestamp;
    
    /** 事务协调者的世代编号，用于事务状态的一致性维护 */
    private final OptionalInt coordinatorEpoch;
    
    /** 当前事务的起始偏移量，仅在事务处理时有效 */
    private final OptionalLong currentTransactionStartOffset;

    /**
     * 构造一个新的ProducerState实例
     * 
     * @param producerId 生产者的唯一标识ID
     * @param producerEpoch 生产者的世代编号
     * @param lastSequence 最后发送的消息序列号
     * @param lastTimestamp 最后更新状态的时间戳
     * @param coordinatorEpoch 事务协调者的世代编号
     * @param currentTransactionStartOffset 当前事务的起始偏移量
     */
    public ProducerState(
        long producerId,
        int producerEpoch,
        int lastSequence,
        long lastTimestamp,
        OptionalInt coordinatorEpoch,
        OptionalLong currentTransactionStartOffset
    ) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.lastSequence = lastSequence;
        this.lastTimestamp = lastTimestamp;
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTransactionStartOffset = currentTransactionStartOffset;
    }

    /**
     * 获取生产者的唯一标识ID
     * @return 生产者ID
     */
    public long producerId() {
        return producerId;
    }

    /**
     * 获取生产者的世代编号
     * @return 生产者世代编号
     */
    public int producerEpoch() {
        return producerEpoch;
    }

    /**
     * 获取最后发送的消息序列号
     * @return 最后的消息序列号
     */
    public int lastSequence() {
        return lastSequence;
    }

    /**
     * 获取最后更新状态的时间戳
     * @return 最后更新时间戳
     */
    public long lastTimestamp() {
        return lastTimestamp;
    }

    /**
     * 获取当前事务的起始偏移量
     * @return 事务起始偏移量，如果不在事务中则为空
     */
    public OptionalLong currentTransactionStartOffset() {
        return currentTransactionStartOffset;
    }

    /**
     * 获取事务协调者的世代编号
     * @return 协调者世代编号，如果不在事务中则为空
     */
    public OptionalInt coordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerState that = (ProducerState) o;
        return producerId == that.producerId &&
            producerEpoch == that.producerEpoch &&
            lastSequence == that.lastSequence &&
            lastTimestamp == that.lastTimestamp &&
            Objects.equals(coordinatorEpoch, that.coordinatorEpoch) &&
            Objects.equals(currentTransactionStartOffset, that.currentTransactionStartOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, producerEpoch, lastSequence, lastTimestamp,
            coordinatorEpoch, currentTransactionStartOffset);
    }

    @Override
    public String toString() {
        return "ProducerState(" +
            "producerId=" + producerId +
            ", producerEpoch=" + producerEpoch +
            ", lastSequence=" + lastSequence +
            ", lastTimestamp=" + lastTimestamp +
            ", coordinatorEpoch=" + coordinatorEpoch +
            ", currentTransactionStartOffset=" + currentTransactionStartOffset +
            ')';
    }
}
