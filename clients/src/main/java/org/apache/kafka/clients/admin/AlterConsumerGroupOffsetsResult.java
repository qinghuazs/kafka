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
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 修改消费者组偏移量操作的结果类，用于处理{@link AdminClient#alterConsumerGroupOffsets(String, Map)}调用的返回结果。
 * 该类提供了检查单个分区修改结果和所有分区修改结果的方法。
 * 
 * 应用场景：
 * 1. 手动调整消费者组的消费偏移量，比如重新消费某些消息或跳过某些消息
 * 2. 在消费者组迁移或重组时，需要调整偏移量
 * 3. 修复消费者组的偏移量问题
 * 
 * 注意：该类的API仍在演进中，详见{@link AdminClient}。
 */
@InterfaceStability.Evolving
public class AlterConsumerGroupOffsetsResult {

    /**
     * 存储修改偏移量操作的Future结果
     * - Key: TopicPartition对象，表示主题分区
     * - Value: Errors对象，表示操作的错误状态
     */
    private final KafkaFuture<Map<TopicPartition, Errors>> future;

    /**
     * 构造函数，初始化修改偏移量操作的Future结果
     *
     * @param future 包含每个分区修改结果的Future对象
     */
    AlterConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, Errors>> future) {
        this.future = future;
    }

    /**
     * 获取指定分区的修改偏移量操作结果
     * 
     * 实现细节：
     * 1. 创建一个新的KafkaFutureImpl对象来存储单个分区的结果
     * 2. 当原始future完成时：
     *    - 如果发生异常，则将异常传递给结果future
     *    - 如果指定分区不在结果集中，抛出IllegalArgumentException
     *    - 如果分区操作成功(Errors.NONE)，完成future
     *    - 如果分区操作失败，使用对应的错误完成future
     *
     * @param partition 要检查结果的主题分区
     * @return 表示操作结果的Future，如果成功则完成，如果失败则包含异常
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        this.future.whenComplete((topicPartitions, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else if (!topicPartitions.containsKey(partition)) {
                result.completeExceptionally(new IllegalArgumentException(
                    "Alter offset for partition \"" + partition + "\" was not attempted"));
            } else {
                final Errors error = topicPartitions.get(partition);
                if (error == Errors.NONE) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(error.exception());
                }
            }
        });

        return result;
    }

    /**
     * 获取所有分区的修改偏移量操作的聚合结果
     * 
     * 实现细节：
     * 1. 使用thenApply转换原始future的结果
     * 2. 收集所有失败的分区到列表中
     * 3. 检查是否有任何错误：
     *    - 如果有错误，抛出异常，包含所有失败分区的信息
     *    - 如果全部成功，返回null表示操作完成
     *
     * @return 表示所有分区操作结果的Future，只有当所有分区都成功时才完成
     */
    public KafkaFuture<Void> all() {
        return this.future.thenApply(topicPartitionErrorsMap ->  {
            List<TopicPartition> partitionsFailed = topicPartitionErrorsMap.entrySet()
                .stream()
                .filter(e -> e.getValue() != Errors.NONE)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            for (Errors error : topicPartitionErrorsMap.values()) {
                if (error != Errors.NONE) {
                    throw error.exception(
                        "Failed altering consumer group offsets for the following partitions: " + partitionsFailed);
                }
            }
            return null;
        });
    }
}
