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

import java.util.Set;

/**
 * 共享获取指标聚合器
 * 用于聚合和管理共享消费者获取操作的性能指标
 * 
 * 应用场景：
 * 1. 跟踪分区级别的获取指标
 * 2. 聚合批量获取的字节数和记录数
 * 3. 管理未记录分区的状态
 * 4. 提供指标汇总功能
 */
public class ShareFetchMetricsAggregator {
    /**
     * 共享获取指标管理器
     * 用于管理和记录最终的聚合指标
     */
    private final ShareFetchMetricsManager shareFetchMetricsManager;

    /**
     * 获取指标实例
     * 用于临时存储和累加当前批次的指标数据
     */
    private final FetchMetrics fetchMetrics = new FetchMetrics();

    /**
     * 未记录指标的分区集合
     * 用于跟踪哪些分区的指标尚未被记录
     */
    private final Set<TopicPartition> unrecordedPartitions;

    /**
     * 构造函数
     * 创建一个新的共享获取指标聚合器实例
     *
     * @param shareFetchMetricsManager 共享获取指标管理器
     * @param partitions 需要记录指标的分区集合
     */
    public ShareFetchMetricsAggregator(ShareFetchMetricsManager shareFetchMetricsManager, Set<TopicPartition> partitions) {
        // 初始化指标管理器
        this.shareFetchMetricsManager = shareFetchMetricsManager;
        // 初始化未记录分区集合
        this.unrecordedPartitions = partitions;
    }

    /**
     * 记录单个分区的获取指标
     * 
     * @param partition 主题分区
     * @param bytes 获取的字节数
     * @param records 获取的记录数
     */
    public void record(TopicPartition partition, int bytes, int records) {
        // 在获取级别聚合指标
        fetchMetrics.increment(bytes, records);
        // 尝试记录指标（如果所有分区都已处理）
        maybeRecordMetrics(partition);
    }

    /**
     * 尝试记录聚合的指标
     * 只有当所有分区都已处理时才会实际记录指标
     *
     * @param partition 已处理的分区
     */
    private void maybeRecordMetrics(TopicPartition partition) {
        // 从未记录分区集合中移除已处理的分区
        unrecordedPartitions.remove(partition);

        // 如果还有未记录的分区，则返回
        if (!unrecordedPartitions.isEmpty())
            return;

        // 当所有分区都已处理时，记录聚合的指标
        shareFetchMetricsManager.recordRecordsFetched(fetchMetrics.records);
        shareFetchMetricsManager.recordBytesFetched(fetchMetrics.bytes);
    }

    /**
     * 获取指标内部类
     * 用于存储和累加单次获取操作的指标数据
     */
    private static class FetchMetrics {
        /**
         * 获取的总字节数
         */
        private int bytes;

        /**
         * 获取的总记录数
         */
        private int records;

        /**
         * 增加指标值
         * 累加字节数和记录数
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
