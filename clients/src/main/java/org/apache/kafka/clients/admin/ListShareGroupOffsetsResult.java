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

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)} 调用的结果类。
 * 该类用于处理共享消费者组偏移量查询的异步结果，支持批量查询多个消费者组的主题分区偏移量。
 * <p>
 * 注意：该API仍在演进中，详细信息请参考 {@link Admin}。
 * 
 * 应用场景：
 * 1. 监控多个消费者组的消费进度
 * 2. 分析消费者组的消费延迟情况
 * 3. 在消费者组迁移时获取源消费者组的偏移量信息
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsResult {

    /**
     * 存储每个消费者组的异步查询结果
     * - Key: 消费者组ID（String类型）
     * - Value: KafkaFuture对象，包含该消费者组的主题分区偏移量映射
     *   - 内层Map的Key为TopicPartition（主题分区）
     *   - 内层Map的Value为Long类型的偏移量
     */
    private final Map<String, KafkaFuture<Map<TopicPartition, Long>>> futures;

    /**
     * 构造函数，初始化查询结果对象
     * 
     * @param futures 协调者密钥到Future的映射，Future中包含主题分区的偏移量信息
     * 注：构造函数会将CoordinatorKey转换为消费者组ID作为Map的键
     */
    ListShareGroupOffsetsResult(final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, Long>>> futures) {
        // 将CoordinatorKey的idValue（即消费者组ID）作为新Map的键，保持Future对象作为值
        this.futures = futures.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().idValue, Map.Entry::getValue));
    }

    /**
     * 获取所有消费者组的查询结果
     * 该方法会等待所有异步查询完成，并将结果聚合到一个Map中
     *
     * @return 返回一个Future，其结果包含所有消费者组的主题分区偏移量信息
     *         - 外层Map的Key为消费者组ID
     *         - 外层Map的Value为该消费者组的主题分区偏移量映射
     */
    public KafkaFuture<Map<String, Map<TopicPartition, Long>>> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            nil -> {
                // 创建结果Map，用于存储所有消费者组的偏移量信息
                Map<String, Map<TopicPartition, Long>> offsets = new HashMap<>(futures.size());
                // 遍历每个消费者组的Future，获取其结果
                futures.forEach((groupId, future) -> {
                    try {
                        // 获取Future的结果并存入结果Map
                        offsets.put(groupId, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 由于KafkaFuture#allOf已确保所有Future成功完成，
                        // 理论上不会到达这个异常处理分支
                        throw new RuntimeException(e);
                    }
                });
                return offsets;
            });
    }

    /**
     * 获取指定消费者组的主题分区偏移量信息
     *
     * @param groupId 消费者组ID
     * @return 返回一个Future，其结果为指定消费者组的主题分区偏移量映射
     * @throws IllegalArgumentException 当指定的消费者组ID不存在时抛出此异常
     */
    public KafkaFuture<Map<TopicPartition, Long>> partitionsToOffset(String groupId) {
        // 检查消费者组ID是否存在
        if (!futures.containsKey(groupId)) {
            throw new IllegalArgumentException("Group ID not found: " + groupId);
        }
        // 返回对应的Future对象
        return futures.get(groupId);
    }
}
