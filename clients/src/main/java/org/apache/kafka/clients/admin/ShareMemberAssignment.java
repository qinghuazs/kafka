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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 描述Kafka共享消费者组（Share Consumer Group）中特定成员的分区分配信息。
 * 
 * 该类在共享消费者组的管理中发挥关键作用：
 * 1. 用于记录和管理每个共享消费者组成员被分配的主题分区
 * 2. 在共享消费者组的再平衡过程中，用于跟踪和更新分区分配状态
 * 3. 支持Admin API查询共享消费者组成员的分区分配情况
 * 4. 与ShareMemberDescription配合使用，提供完整的共享消费者组成员信息
 */
@InterfaceStability.Evolving
public class ShareMemberAssignment {
    /**
     * 存储分配给共享消费者组成员的主题分区集合
     * - 使用不可变Set确保线程安全性
     * - TopicPartition对象包含topic和partition信息，用于唯一标识一个分区
     * - 通过final修饰确保引用不可变
     */
    private final Set<TopicPartition> topicPartitions;

    /**
     * 创建ShareMemberAssignment实例，用于初始化或更新成员的分区分配
     * 
     * @param topicPartitions 要分配给共享消费者组成员的主题分区集合
     *                       如果为null，则创建一个空的不可变集合
     */
    public ShareMemberAssignment(Set<TopicPartition> topicPartitions) {
        // 通过Set.copyOf创建一个不可变的副本，防止外部修改
        // 如果入参为null，则使用空集合作为默认值
        this.topicPartitions = topicPartitions == null ? Collections.emptySet() : Set.copyOf(topicPartitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShareMemberAssignment that = (ShareMemberAssignment) o;

        return Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return topicPartitions != null ? topicPartitions.hashCode() : 0;
    }

    /**
     * 获取分配给共享消费者组成员的所有主题分区
     * 
     * 该方法主要用于以下场景：
     * 1. 共享消费者组成员获取自己的分区分配结果
     * 2. Admin API查询特定成员的分区分配状态
     * 3. 在共享消费者组的再平衡过程中，用于确定当前的分配情况
     * 
     * @return 返回不可变的TopicPartition集合，包含分配给该成员的所有主题分区
     */
    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public String toString() {
        return "(topicPartitions=" + topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")) + ")";
    }
}
