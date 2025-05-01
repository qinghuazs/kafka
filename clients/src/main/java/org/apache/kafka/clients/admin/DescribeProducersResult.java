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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * {@link Admin#describeProducers(Collection)} 调用的结果类。
 * 该类用于获取Kafka主题分区上活跃生产者的信息。
 */
@InterfaceStability.Evolving
public class DescribeProducersResult {
    // 存储每个主题分区对应的生产者状态Future的映射
    // 使用KafkaFuture而不是CompletableFuture是为了提供更好的异常处理和类型安全
    private final Map<TopicPartition, KafkaFuture<PartitionProducerState>> futures;

    /**
     * 构造函数，初始化生产者描述结果
     * 
     * @param futures 包含每个主题分区对应的生产者状态Future的映射
     */
    DescribeProducersResult(Map<TopicPartition, KafkaFuture<PartitionProducerState>> futures) {
        // 初始化futures字段，存储每个分区的异步生产者状态结果
        this.futures = futures;
    }

    /**
     * 获取指定主题分区的生产者状态Future
     * 
     * @param partition 要查询的主题分区
     * @return 返回包含该分区生产者状态的Future
     * @throws IllegalArgumentException 如果请求中不包含指定的分区
     */
    public KafkaFuture<PartitionProducerState> partitionResult(final TopicPartition partition) {
        // 从futures映射中获取指定分区的Future
        KafkaFuture<PartitionProducerState> future = futures.get(partition);
        // 如果Future为null，说明请求中不包含该分区
        if (future == null) {
            throw new IllegalArgumentException("Topic partition " + partition +
                " was not included in the request");
        }
        // 返回该分区的Future结果
        return future;
    }

    /**
     * 获取所有分区的生产者状态
     * 
     * @return 返回包含所有分区生产者状态的Future映射
     */
    public KafkaFuture<Map<TopicPartition, PartitionProducerState>> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(nil -> {
                // 创建结果映射，用于存储所有分区的生产者状态
                Map<TopicPartition, PartitionProducerState> results = new HashMap<>(futures.size());
                // 遍历所有Future条目
                for (Map.Entry<TopicPartition, KafkaFuture<PartitionProducerState>> entry : futures.entrySet()) {
                    try {
                        // 获取每个Future的结果并存入results映射
                        results.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况理论上不会发生，因为KafkaFuture.allOf已经确保所有Future都成功完成
                        throw new KafkaException(e);
                    }
                }
                // 返回包含所有分区生产者状态的映射
                return results;
            });
    }

    /**
     * 表示分区上的生产者状态的内部类
     */
    public static class PartitionProducerState {
        // 存储分区上活跃的生产者列表
        private final List<ProducerState> activeProducers;

        /**
         * 构造函数
         * @param activeProducers 活跃的生产者列表
         */
        public PartitionProducerState(List<ProducerState> activeProducers) {
            // 初始化活跃生产者列表
            this.activeProducers = activeProducers;
        }

        /**
         * 获取活跃的生产者列表
         * @return 返回分区上当前活跃的生产者列表
         */
        public List<ProducerState> activeProducers() {
            // 返回活跃生产者列表
            return activeProducers;
        }

        @Override
        public String toString() {
            return "PartitionProducerState(" +
                "activeProducers=" + activeProducers +
                ')';
        }
    }
}
