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

/**
 * 事务中止操作的规范类
 *
 * 该类定义了中止Kafka事务所需的所有必要信息，包括：
 * - 事务涉及的主题分区
 * - 生产者ID和生产者纪元（用于识别特定的生产者实例）
 * - 事务协调器的纪元（用于确保事务操作的一致性）
 *
 * 注意：这个类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class AbortTransactionSpec {
    /**
     * 事务涉及的主题分区
     * 标识了需要中止事务的具体分区位置
     */
    private final TopicPartition topicPartition;

    /**
     * 生产者ID
     * 用于唯一标识执行事务的生产者
     */
    private final long producerId;

    /**
     * 生产者纪元
     * 用于处理生产者故障恢复，每次生产者重启都会增加
     */
    private final short producerEpoch;

    /**
     * 事务协调器的纪元
     * 用于确保事务操作的顺序性和一致性
     */
    private final int coordinatorEpoch;

    /**
     * 构造函数
     *
     * @param topicPartition 需要中止事务的主题分区
     * @param producerId 生产者的唯一标识符
     * @param producerEpoch 生产者的当前纪元
     * @param coordinatorEpoch 事务协调器的当前纪元
     */
    public AbortTransactionSpec(
        TopicPartition topicPartition,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch
    ) {
        // 初始化所有字段
        this.topicPartition = topicPartition;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    /**
     * 获取事务涉及的主题分区
     *
     * @return 返回需要中止事务的主题分区
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * 获取生产者ID
     *
     * @return 返回生产者的唯一标识符
     */
    public long producerId() {
        return producerId;
    }

    /**
     * 获取生产者纪元
     *
     * @return 返回生产者的当前纪元
     */
    public short producerEpoch() {
        return producerEpoch;
    }

    /**
     * 获取事务协调器的纪元
     *
     * @return 返回事务协调器的当前纪元
     */
    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbortTransactionSpec that = (AbortTransactionSpec) o;
        return producerId == that.producerId &&
            producerEpoch == that.producerEpoch &&
            coordinatorEpoch == that.coordinatorEpoch &&
            Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, producerId, producerEpoch, coordinatorEpoch);
    }

    @Override
    public String toString() {
        return "AbortTransactionSpec(" +
            "topicPartition=" + topicPartition +
            ", producerId=" + producerId +
            ", producerEpoch=" + producerEpoch +
            ", coordinatorEpoch=" + coordinatorEpoch +
            ')';
    }

}
