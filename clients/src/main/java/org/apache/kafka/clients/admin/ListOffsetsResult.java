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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * AdminClient#listOffsets(Map)调用的结果类。
 * 该类用于处理Kafka主题分区偏移量查询的异步结果，提供了获取单个分区和所有分区偏移量信息的方法。
 * 
 * 注意：该类的API仍在演进中，详细信息请参考{@link AdminClient}。
 */
@InterfaceStability.Evolving
public class ListOffsetsResult {

    /**
     * 存储每个主题分区对应的偏移量查询结果Future
     * Key: 主题分区信息
     * Value: 该分区的偏移量查询结果Future
     */
    private final Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures;

    /**
     * 构造函数，初始化偏移量查询结果映射
     * @param futures 包含所有分区查询结果的Future映射
     */
    public ListOffsetsResult(Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures) {
        this.futures = futures;
    }

    /**
     * 获取指定分区的偏移量查询结果Future
     * 
     * @param partition 目标分区
     * @return 该分区的偏移量信息Future
     * @throws IllegalArgumentException 如果指定的分区不在查询请求中
     */
    public KafkaFuture<ListOffsetsResultInfo> partitionResult(final TopicPartition partition) {
        // 从futures映射中获取指定分区的Future
        KafkaFuture<ListOffsetsResultInfo> future = futures.get(partition);
        // 如果Future为null，说明该分区不在原始请求中，抛出异常
        if (future == null) {
            throw new IllegalArgumentException(
                    "List Offsets for partition \"" + partition + "\" was not attempted");
        }
        return future;
    }

    /**
     * 获取所有分区的偏移量查询结果
     * 该方法返回的Future仅在所有分区的偏移量都成功获取时才会完成
     * 
     * @return 包含所有分区偏移量信息的Future
     */
    public KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> all() {
        // 使用KafkaFuture.allOf等待所有分区的Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
                .thenApply(v -> {
                    // 创建结果Map，用于存储所有分区的偏移量信息
                    Map<TopicPartition, ListOffsetsResultInfo> offsets = new HashMap<>(futures.size());
                    // 遍历所有分区的Future，获取结果并存入Map
                    for (Map.Entry<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> entry : futures.entrySet()) {
                        try {
                            offsets.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // 理论上不会到达这里，因为allOf确保了所有Future都成功完成
                            throw new RuntimeException(e);
                        }
                    }
                    return offsets;
                });
    }

    /**
     * 分区偏移量信息的内部类
     * 包含偏移量、时间戳和领导者纪元信息
     */
    public static class ListOffsetsResultInfo {

        /**
         * 消息的偏移量位置
         */
        private final long offset;

        /**
         * 消息的时间戳
         */
        private final long timestamp;

        /**
         * 领导者纪元信息，用于确保一致性
         */
        private final Optional<Integer> leaderEpoch;

        /**
         * 构造函数
         * @param offset 偏移量
         * @param timestamp 时间戳
         * @param leaderEpoch 领导者纪元
         */
        public ListOffsetsResultInfo(long offset, long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }

        /**
         * @return 消息的偏移量位置
         */
        public long offset() {
            return offset;
        }

        /**
         * @return 消息的时间戳
         */
        public long timestamp() {
            return timestamp;
        }

        /**
         * @return 领导者纪元信息
         */
        public Optional<Integer> leaderEpoch() {
            return leaderEpoch;
        }

        @Override
        public String toString() {
            return "ListOffsetsResultInfo(offset=" + offset + ", timestamp=" + timestamp + ", leaderEpoch="
                    + leaderEpoch + ")";
        }
    }
}
