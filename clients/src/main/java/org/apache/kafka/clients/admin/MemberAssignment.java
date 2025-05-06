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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 描述Kafka消费者组中特定成员的分区分配信息。
 * 
 * 该类在消费者组的分区分配过程（Partition Assignment）中发挥关键作用：
 * 1. 在消费者组再平衡（Rebalance）时，用于记录每个消费者被分配的分区
 * 2. 通过Admin API查询消费者组信息时，用于展示成员的分区分配状态
 * 3. 支持消费者组的分区分配策略（如Range、RoundRobin等）的实现
 */
public class MemberAssignment {
    /**
     * 存储分配给该消费者组成员的主题分区集合
     * - 使用不可变集合（Set）确保线程安全
     * - TopicPartition包含topic和partition两个属性，用于唯一标识一个分区
     */
    private final Set<TopicPartition> topicPartitions;

    /**
     * 创建MemberAssignment实例
     * 
     * @param topicPartitions 要分配给消费者组成员的主题分区集合
     *                       如果为null，则创建一个空的不可变集合
     */
    public MemberAssignment(Set<TopicPartition> topicPartitions) {
        // 如果入参为null则创建空集合，否则创建入参的不可变副本
        // 使用Set.copyOf确保返回的集合是不可变的，防止外部修改
        this.topicPartitions = topicPartitions == null ? Collections.emptySet() : Set.copyOf(topicPartitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberAssignment that = (MemberAssignment) o;

        return Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return topicPartitions != null ? topicPartitions.hashCode() : 0;
    }

    /**
     * 获取分配给该消费者组成员的所有主题分区
     * 
     * 该方法主要用于以下场景：
     * 1. 消费者组成员获取自己的分区分配结果
     * 2. 监控和管理工具查询消费者组的分区分配状态
     * 3. 在再平衡过程中，分区分配策略需要了解当前的分配情况
     * 
     * @return 返回不可变的TopicPartition集合，包含分配给该成员的所有主题分区
     *         由于使用了不可变集合，调用方无法修改返回的集合内容
     */
    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public String toString() {
        return "(topicPartitions=" + topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")) + ")";
    }
}
