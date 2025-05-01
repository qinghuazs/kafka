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

import java.util.Map;
import java.util.Set;

/**
 * 用于处理删除消费者组位移操作的结果
 * 该类封装了{@link Admin#deleteConsumerGroupOffsets(String, Set)}调用的返回结果
 * 
 * 应用场景：
 * 1. 当需要删除消费者组在特定分区上的位移提交记录时使用
 * 2. 支持批量删除多个分区的位移，并异步获取每个分区的删除结果
 * 3. 适用于消费者组重置或清理场景
 * 
 * 注意：该API仍在演进中，详见{@link Admin}文档
 */
@InterfaceStability.Evolving
public class DeleteConsumerGroupOffsetsResult {
    /**
     * 存储删除操作的异步结果
     * Map的键为TopicPartition（主题分区），值为Errors（操作的错误信息）
     */
    private final KafkaFuture<Map<TopicPartition, Errors>> future;

    /**
     * 存储请求中包含的所有待删除位移的分区集合
     * 用于验证查询特定分区结果时，该分区是否在原始请求中
     */
    private final Set<TopicPartition> partitions;

    /**
     * 构造函数
     * @param future 异步操作的Future对象，包含每个分区的删除结果
     * @param partitions 请求中指定的待删除位移的分区集合
     */
    DeleteConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, Errors>> future, Set<TopicPartition> partitions) {
        this.future = future;
        this.partitions = partitions;
    }

    /**
     * 获取指定分区的删除操作结果
     * 
     * @param partition 要查询结果的目标分区
     * @return 返回一个KafkaFuture对象，用于异步获取指定分区的删除结果
     *         - 如果删除成功，Future完成时返回null
     *         - 如果删除失败，Future将抛出异常
     * @throws IllegalArgumentException 当查询的分区不在原始请求中时抛出此异常
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        // 检查分区是否在原始请求中
        if (!partitions.contains(partition)) {
            throw new IllegalArgumentException("Partition " + partition + " was not included in the original request");
        }
        // 创建新的Future用于返回单个分区的结果
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        // 注册回调处理原始Future完成时的逻辑
        this.future.whenComplete((topicPartitions, throwable) -> {
            if (throwable != null) {
                // 如果原始Future异常完成，传递异常
                result.completeExceptionally(throwable);
            } else if (!maybeCompleteExceptionally(topicPartitions, partition, result)) {
                // 如果没有异常发生，标记结果Future为成功完成
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * 获取所有分区的删除操作的聚合结果
     * 
     * @return 返回一个KafkaFuture对象，用于异步获取所有分区的删除结果
     *         - 只有当所有分区都删除成功时，Future才会成功完成
     *         - 如果任何分区删除失败，Future将抛出第一个遇到的异常
     */
    public KafkaFuture<Void> all() {
        // 创建新的Future用于返回聚合结果
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        // 注册回调处理原始Future完成时的逻辑
        this.future.whenComplete((topicPartitions, throwable) -> {
            if (throwable != null) {
                // 如果原始Future异常完成，传递异常
                result.completeExceptionally(throwable);
            } else {
                // 检查每个分区的删除结果
                for (TopicPartition partition : partitions) {
                    // 如果任何分区出现错误，立即返回错误结果
                    if (maybeCompleteExceptionally(topicPartitions, partition, result)) {
                        return;
                    }
                }
                // 所有分区都成功删除，标记结果Future为成功完成
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * 检查特定分区的错误并相应地完成Future
     * 
     * @param partitionLevelErrors 包含所有分区错误信息的映射
     * @param partition 要检查的目标分区
     * @param result 要设置结果的Future对象
     * @return 如果发现错误并完成Future则返回true，否则返回false
     */
    private boolean maybeCompleteExceptionally(Map<TopicPartition, Errors> partitionLevelErrors,
                                               TopicPartition partition,
                                               KafkaFutureImpl<Void> result) {
        // 获取分区级别的错误信息
        Throwable exception = KafkaAdminClient.getSubLevelError(partitionLevelErrors, partition,
            "Offset deletion result for partition \"" + partition + "\" was not included in the response");
        if (exception != null) {
            // 如果存在错误，使用该错误完成Future
            result.completeExceptionally(exception);
            return true;
        } else {
            // 没有错误发生
            return false;
        }
    }
}
