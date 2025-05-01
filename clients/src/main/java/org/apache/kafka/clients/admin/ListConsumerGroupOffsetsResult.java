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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Admin#listConsumerGroupOffsets(Map)和Admin#listConsumerGroupOffsets(String)调用的结果类。
 * <p>
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsResult {

    /**
     * 存储消费者组ID到其对应的Future的映射
     * Future中包含了主题分区到偏移量和元数据的映射
     */
    final Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures;

    /**
     * 构造函数，初始化结果对象
     * 
     * @param futures 协调器键到Future的映射，Future中包含主题分区的偏移量信息
     */
    ListConsumerGroupOffsetsResult(final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        // 将CoordinatorKey映射转换为消费者组ID映射
        // 使用Stream API进行转换，提取CoordinatorKey的idValue作为新的key
        this.futures = futures.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().idValue, Entry::getValue));
    }

    /**
     * 返回一个Future，其结果为主题分区到偏移量和元数据的映射。
     * 如果消费者组没有为某个分区提交偏移量，则返回映射中对应的值为null。
     * 
     * @return 返回包含主题分区偏移量信息的Future对象
     * @throws IllegalStateException 如果请求了多个消费者组的偏移量
     */
    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata() {
        // 检查是否只请求了一个消费者组的偏移量
        if (futures.size() != 1) {
            // 如果请求了多个消费者组，抛出异常，建议使用带groupId参数的方法
            throw new IllegalStateException("Offsets from multiple consumer groups were requested. " +
                    "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.");
        }
        // 返回唯一的Future对象
        return futures.values().iterator().next();
    }

    /**
     * 返回指定消费者组的Future，其结果为主题分区到偏移量和元数据的映射。
     * 如果消费者组没有为某个分区提交偏移量，则返回映射中对应的值为null。
     * 
     * @param groupId 消费者组ID
     * @return 返回指定消费者组的主题分区偏移量信息的Future对象
     * @throws IllegalArgumentException 如果指定的消费者组不在请求列表中
     */
    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata(String groupId) {
        // 检查指定的消费者组是否在请求列表中
        if (!futures.containsKey(groupId))
            // 如果不在列表中，抛出异常
            throw new IllegalArgumentException("Offsets for consumer group '" + groupId + "' were not requested.");
        // 返回对应的Future对象
        return futures.get(groupId);
    }

    /**
     * 返回一个Future，其结果为所有消费者组的偏移量信息映射。
     * 只有当所有消费者组的请求都成功时才会返回结果。
     * 
     * @return 返回包含所有消费者组偏移量信息的Future对象
     */
    public KafkaFuture<Map<String, Map<TopicPartition, OffsetAndMetadata>>> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            nil -> {
                // 创建结果映射，用于存储所有消费者组的偏移量信息
                Map<String, Map<TopicPartition, OffsetAndMetadata>> listedConsumerGroupOffsets = new HashMap<>(futures.size());
                // 遍历所有Future，获取结果并存入映射
                futures.forEach((key, future) -> {
                    try {
                        // 获取Future的结果并存入映射
                        listedConsumerGroupOffsets.put(key, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况理论上不会发生，因为KafkaFuture#allOf已经确保所有Future都成功完成
                        throw new RuntimeException(e);
                    }
                });
                // 返回包含所有消费者组偏移量信息的映射
                return listedConsumerGroupOffsets;
            });
    }
}
