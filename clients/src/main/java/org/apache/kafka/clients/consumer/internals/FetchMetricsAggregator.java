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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 获取度量指标聚合器
 * 由于我们对每个分区的消息数据进行延迟解析，因此需要在解析每个分区的消息时
 * 对获取级别的度量指标进行聚合。该类用于实现这种增量聚合。
 */
class FetchMetricsAggregator {

    /**
     * 度量指标管理器
     * 用于记录和管理各种度量指标
     */
    private final FetchMetricsManager metricsManager;

    /**
     * 未记录度量的分区集合
     * 跟踪尚未处理的分区
     */
    private final Set<TopicPartition> unrecordedPartitions;

    /**
     * 获取级别的度量指标
     * 用于聚合整个获取操作的度量数据
     */
    private final FetchMetrics fetchFetchMetrics = new FetchMetrics();

    /**
     * 每个主题的度量指标映射
     * 按主题维度聚合度量数据
     */
    private final Map<String, FetchMetrics> perTopicFetchMetrics = new HashMap<>();

    /**
     * 构造函数
     * 初始化度量指标聚合器
     *
     * @param metricsManager 度量指标管理器
     * @param partitions 需要记录度量的分区集合
     */
    FetchMetricsAggregator(FetchMetricsManager metricsManager, Set<TopicPartition> partitions) {
        // 设置度量指标管理器
        this.metricsManager = metricsManager;
        // 初始化未记录分区集合，创建一个新的HashSet以避免共享原始集合
        this.unrecordedPartitions = new HashSet<>(partitions);
    }

    /**
     * 记录单个分区的度量数据
     * 在解析完每个分区后，使用总字节数和记录数更新当前的度量总计。
     * 当所有分区都报告完成后，写入最终的度量数据。
     *
     * @param partition 主题分区
     * @param bytes 字节数
     * @param records 记录数
     */
    void record(TopicPartition partition, int bytes, int records) {
        // 在获取级别聚合度量指标
        fetchFetchMetrics.increment(bytes, records);

        // 在每个主题级别聚合度量指标
        // 如果主题不存在，则创建新的度量指标对象
        perTopicFetchMetrics.computeIfAbsent(partition.topic(), t -> new FetchMetrics())
                        .increment(bytes, records);

        // 尝试记录聚合的度量数据
        maybeRecordMetrics(partition);
    }

    /**
     * 尝试记录度量数据
     * 当检测到获取操作的所有分区都已处理完成时，
     * 记录聚合的度量值，包括获取级别和每个主题级别的度量数据。
     *
     * @param partition 已处理完成的分区
     */
    private void maybeRecordMetrics(TopicPartition partition) {
        // 从未记录分区集合中移除已处理的分区
        unrecordedPartitions.remove(partition);

        // 如果还有未处理的分区，则返回
        if (!unrecordedPartitions.isEmpty())
            return;

        // 记录获取级别的聚合度量数据
        metricsManager.recordBytesFetched(fetchFetchMetrics.bytes);
        metricsManager.recordRecordsFetched(fetchFetchMetrics.records);

        // 记录每个主题级别的聚合度量数据
        for (Map.Entry<String, FetchMetrics> entry: perTopicFetchMetrics.entrySet()) {
            String topic = entry.getKey();
            FetchMetrics fetchMetrics = entry.getValue();
            // 记录每个主题的字节数和记录数
            metricsManager.recordBytesFetched(topic, fetchMetrics.bytes);
            metricsManager.recordRecordsFetched(topic, fetchMetrics.records);
        }
    }

    /**
     * 获取度量指标内部类
     * 用于存储和累加字节数和记录数
     */
    private static class FetchMetrics {
        /**
         * 累计字节数
         */
        private int bytes;

        /**
         * 累计记录数
         */
        private int records;

        /**
         * 增加字节数和记录数
         *
         * @param bytes 要增加的字节数
         * @param records 要增加的记录数
         */
        private void increment(int bytes, int records) {
            // 累加字节数
            this.bytes += bytes;
            // 累加记录数
            this.records += records;
        }
    }
}