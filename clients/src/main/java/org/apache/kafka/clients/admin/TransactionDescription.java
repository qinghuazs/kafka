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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Kafka事务描述类，用于提供事务的详细信息
 * 
 * 该类包含了Kafka事务的所有关键属性，包括：
 * - 事务协调器ID：用于标识负责协调该事务的broker
 * - 事务状态：表示当前事务的状态（如进行中、已提交、已中止等）
 * - 生产者信息：包括生产者ID和生产者纪元，用于唯一标识生产者实例
 * - 事务时间信息：包括超时时间和开始时间
 * - 涉及的主题分区：事务操作影响的所有主题分区集合
 *
 * 应用场景：
 * 1. 事务监控：用于监控和追踪事务的执行状态
 * 2. 故障恢复：在系统发生故障时，用于恢复事务状态
 * 3. 事务管理：帮助管理和维护分布式事务的一致性
 */
@InterfaceStability.Evolving
public class TransactionDescription {
    /**
     * 事务协调器的ID
     * 标识了负责协调该事务的特定broker
     */
    private final int coordinatorId;

    /**
     * 事务的当前状态
     * 表示事务是处于进行中、已提交还是已中止等状态
     */
    private final TransactionState state;

    /**
     * 生产者的唯一标识符
     * 用于识别执行事务的特定生产者
     */
    private final long producerId;

    /**
     * 生产者的纪元号
     * 用于处理生产者故障恢复，每次生产者重启都会增加
     */
    private final int producerEpoch;

    /**
     * 事务超时时间（毫秒）
     * 如果事务在该时间内未完成，将被自动中止
     */
    private final long transactionTimeoutMs;

    /**
     * 事务开始时间（毫秒）
     * 可选字段，记录事务的开始时间戳
     */
    private final OptionalLong transactionStartTimeMs;

    /**
     * 事务涉及的主题分区集合
     * 包含了该事务中所有被修改的主题分区
     */
    private final Set<TopicPartition> topicPartitions;

    /**
     * 构造函数，创建一个新的事务描述实例
     *
     * @param coordinatorId 事务协调器的ID
     * @param state 事务的当前状态
     * @param producerId 生产者的唯一标识符
     * @param producerEpoch 生产者的纪元号
     * @param transactionTimeoutMs 事务的超时时间（毫秒）
     * @param transactionStartTimeMs 事务的开始时间（毫秒）
     * @param topicPartitions 事务涉及的主题分区集合
     */
    public TransactionDescription(
        int coordinatorId,
        TransactionState state,
        long producerId,
        int producerEpoch,
        long transactionTimeoutMs,
        OptionalLong transactionStartTimeMs,
        Set<TopicPartition> topicPartitions
    ) {
        // 初始化所有字段
        this.coordinatorId = coordinatorId;
        this.state = state;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionStartTimeMs = transactionStartTimeMs;
        this.topicPartitions = topicPartitions;
    }

    /**
     * 获取事务协调器的ID
     * @return 返回负责协调该事务的broker的ID
     */
    public int coordinatorId() {
        return coordinatorId;
    }

    /**
     * 获取事务的当前状态
     * @return 返回表示事务当前状态的枚举值
     */
    public TransactionState state() {
        return state;
    }

    /**
     * 获取生产者ID
     * @return 返回执行该事务的生产者的唯一标识符
     */
    public long producerId() {
        return producerId;
    }

    /**
     * 获取生产者纪元
     * @return 返回当前生产者实例的纪元号
     */
    public int producerEpoch() {
        return producerEpoch;
    }

    /**
     * 获取事务超时时间
     * @return 返回事务的超时时间（毫秒）
     */
    public long transactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    /**
     * 获取事务开始时间
     * @return 返回事务的开始时间戳（毫秒）
     */
    public OptionalLong transactionStartTimeMs() {
        return transactionStartTimeMs;
    }

    /**
     * 获取事务涉及的主题分区集合
     * @return 返回该事务中被修改的所有主题分区
     */
    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionDescription that = (TransactionDescription) o;
        return coordinatorId == that.coordinatorId &&
            producerId == that.producerId &&
            producerEpoch == that.producerEpoch &&
            transactionTimeoutMs == that.transactionTimeoutMs &&
            state == that.state &&
            Objects.equals(transactionStartTimeMs, that.transactionStartTimeMs) &&
            Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinatorId, state, producerId, producerEpoch, transactionTimeoutMs, transactionStartTimeMs, topicPartitions);
    }

    @Override
    public String toString() {
        return "TransactionDescription(" +
            "coordinatorId=" + coordinatorId +
            ", state=" + state +
            ", producerId=" + producerId +
            ", producerEpoch=" + producerEpoch +
            ", transactionTimeoutMs=" + transactionTimeoutMs +
            ", transactionStartTimeMs=" + transactionStartTimeMs +
            ", topicPartitions=" + topicPartitions +
            ')';
    }
}
