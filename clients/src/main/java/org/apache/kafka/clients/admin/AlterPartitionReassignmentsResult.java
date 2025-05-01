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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * {@link AdminClient#alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}方法的执行结果。
 * 该类用于处理Kafka分区重分配操作的结果，包含了对每个分区的操作结果Future和聚合结果的方法。
 * 
 * 应用场景：
 * 1. 当需要动态调整Kafka分区的副本分配时，比如扩容、缩容或负载均衡
 * 2. 需要监控分区重分配操作的执行状态
 * 3. 需要批量处理多个分区的重分配结果
 * 
 * 该类的API仍在演进中。更多详情请参见{@link AdminClient}。
 */
@InterfaceStability.Evolving
public class AlterPartitionReassignmentsResult {
    /**
     * 存储每个主题分区的重分配操作结果
     * - Key: TopicPartition对象，表示特定主题的特定分区
     * - Value: KafkaFuture<Void>对象，表示该分区重分配操作的异步结果
     * 使用final修饰确保线程安全性
     */
    private final Map<TopicPartition, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化分区重分配结果映射
     * @param futures 包含所有分区重分配操作的Future结果映射
     */
    AlterPartitionReassignmentsResult(Map<TopicPartition, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * 返回一个映射，包含每个分区的重分配操作状态
     * 该方法允许用户分别检查每个分区的重分配结果
     *
     * 可能的错误码：
     * - INVALID_REPLICA_ASSIGNMENT (39): 指定的副本分配无效
     *   例如：包含负数、重复的编号，或指定了控制器未知的broker ID
     * - NO_REASSIGNMENT_IN_PROGRESS (85): 当尝试取消重分配但没有正在进行的重分配操作
     * - UNKNOWN (-1): 未知错误
     *
     * @return Map<TopicPartition, KafkaFuture<Void>> 返回分区到其重分配操作Future的映射
     */
    public Map<TopicPartition, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * 返回一个聚合的Future，只有当所有分区的重分配操作都成功启动时才会成功
     * 实现方式：使用KafkaFuture.allOf()方法将所有分区的Future合并为一个
     * 
     * @return KafkaFuture<Void> 表示所有重分配操作的聚合结果
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
