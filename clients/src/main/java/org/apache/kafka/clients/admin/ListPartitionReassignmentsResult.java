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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * The result of {@link AdminClient#listPartitionReassignments(ListPartitionReassignmentsOptions)}.
 * 用于获取Kafka分区重分配操作的查询结果。
 *
 * The API of this class is evolving. See {@link AdminClient} for details.
 * 该类的API仍在演进中。更多详情请参见{@link AdminClient}。
 *
 * 应用场景：
 * 1. 监控分区重分配的进度和状态
 * 2. 获取当前正在进行的分区重分配信息
 * 3. 在分区迁移过程中跟踪副本的变化
 */
public class ListPartitionReassignmentsResult {
    /**
     * 存储分区重分配查询的异步结果
     * - Key: TopicPartition，表示特定主题的特定分区
     * - Value: PartitionReassignment，包含该分区的重分配详细信息
     */
    private final KafkaFuture<Map<TopicPartition, PartitionReassignment>> future;

    /**
     * 构造函数，初始化分区重分配查询结果
     * @param reassignments 包含所有分区重分配信息的Future对象
     */
    ListPartitionReassignmentsResult(KafkaFuture<Map<TopicPartition, PartitionReassignment>> reassignments) {
        this.future = reassignments;
    }

    /**
     * Return a future which yields a map containing each partition's reassignments
     * 返回一个Future对象，该对象包含了所有分区的重分配信息
     * 
     * @return KafkaFuture<Map<TopicPartition, PartitionReassignment>> 返回分区重分配信息的映射
     *         - Key: TopicPartition对象，标识具体的主题分区
     *         - Value: PartitionReassignment对象，包含该分区的重分配状态，如当前副本集合、添加的副本和移除的副本等信息
     */
    public KafkaFuture<Map<TopicPartition, PartitionReassignment>> reassignments() {
        return future;
    }
}
