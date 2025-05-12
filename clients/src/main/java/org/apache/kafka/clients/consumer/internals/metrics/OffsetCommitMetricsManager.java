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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

/**
 * 管理偏移量提交相关的指标。
 *此类负责创建和更新与消费者偏移量提交操作相关的各种指标，
 *例如提交延迟、提交速率和提交总数。
 *它利用 Kafka Metrics 库来注册和记录这些指标。
 */
public class OffsetCommitMetricsManager {
    /**
     * 平均提交延迟指标名称。
     * 记录提交请求所花费的平均时间。
     */
    final MetricName commitLatencyAvg;
    /**
     * 最大提交延迟指标名称。
     * 记录提交请求所花费的最长时间。
     */
    final MetricName commitLatencyMax;
    /**
     * 提交速率指标名称。
     * 记录每秒提交调用的次数。
     */
    final MetricName commitRate;
    /**
     * 提交总数指标名称。
     * 记录提交调用的总次数。
     */
    final MetricName commitTotal;
    /**
     * 用于记录提交延迟的传感器。
     * Sensor 是 Kafka Metrics 中用于收集和聚合原始测量值（例如延迟）的组件。
     */
    private final Sensor commitSensor;

    /**
     * 构造一个 OffsetCommitMetricsManager 实例。
     * 此构造函数初始化所有与偏移量提交相关的指标。
     * 它会创建一个 Sensor 来跟踪提交延迟，并为平均延迟、最大延迟、提交速率和提交总数注册相应的 MetricName。
     *
     * 应用场景：在消费者客户端初始化时创建，用于监控偏移量提交的性能。
     * 实现细节：
     * - 定义指标组名称，结合了消费者指标组前缀和协调器指标后缀。
     * - 创建一个名为 "commit-latency" 的 Sensor。Sensor 用于收集原始数据点。
     * - 为每个指标（平均延迟、最大延迟、速率、总数）创建 MetricName，并将其与描述关联起来。
     * - 将相应的统计信息（Avg, Max, Meter）添加到 Sensor 中，以便在记录数据时自动计算这些指标。
     * 设计考虑：
     * - 将所有与偏移量提交相关的指标集中管理，便于维护和使用。
     * - 使用 Kafka Metrics 库提供的标准组件（Sensor, MetricName, Stats），确保与 Kafka 的监控生态系统兼容。
     * - 指标名称和描述清晰明了，方便用户理解其含义。
     *
     * @param metrics Metrics 实例，用于注册指标。
     */
    public OffsetCommitMetricsManager(Metrics metrics) {
        // 构造指标组名称，通常是 "consumer-coordinator-metrics"
        final String metricGroupName = CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX;
        // 创建一个名为 "commit-latency" 的 Sensor，用于收集提交延迟数据
        commitSensor = metrics.sensor("commit-latency");
        // 定义平均提交延迟指标
        commitLatencyAvg = metrics.metricName("commit-latency-avg", // 指标名称
            metricGroupName, // 指标组
            "提交请求所花费的平均时间"); // 指标描述
        // 将平均值统计信息添加到 commitSensor，当记录数据到 commitSensor 时，会自动计算平均值并更新 commitLatencyAvg 指标
        commitSensor.add(commitLatencyAvg, new Avg());
        // 定义最大提交延迟指标
        commitLatencyMax = metrics.metricName("commit-latency-max", // 指标名称
            metricGroupName, // 指标组
            "提交请求所花费的最长时间"); // 指标描述
        // 将最大值统计信息添加到 commitSensor
        commitSensor.add(commitLatencyMax, new Max());
        // 定义提交速率指标
        commitRate = metrics.metricName("commit-rate", // 指标名称
            metricGroupName, // 指标组
            "每秒提交调用的次数"); // 指标描述
        // 定义提交总数指标
        commitTotal = metrics.metricName("commit-total", // 指标名称
            metricGroupName, // 指标组
            "提交调用的总次数"); // 指标描述
        // 创建一个 Meter 统计信息，它包含一个 WindowedCount (用于计算总数)
        // 并将这个 Meter 同时关联到 commitRate 和 commitTotal 指标
        // 当记录数据到 commitSensor 时，Meter 会更新这两个指标
        commitSensor.add(new Meter(new WindowedCount(),
            commitRate,
            commitTotal));
    }

    /**
     * 记录一次提交请求的延迟。
     * 此方法用于在偏移量提交操作完成后，将操作所花费的时间（延迟）报告给 commitSensor。
     * Sensor 随后会更新所有关联的指标，如平均延迟、最大延迟、提交速率和总数。
     *
     * 应用场景：当消费者成功（或失败）完成一次偏移量提交后，调用此方法记录该次提交的耗时。
     * 实现细节：简单地调用 commitSensor 的 record 方法，传入测量的延迟值。
     * 设计考虑：提供一个简单直接的接口来记录数据点，将复杂的指标计算逻辑封装在 Sensor 和关联的统计信息中。
     *
     * @param responseLatencyMs 提交请求的响应延迟，单位为毫秒。
     */
    public void recordRequestLatency(long responseLatencyMs) {
        // 将响应延迟数据点记录到 commitSensor 中
        // Sensor 会根据配置的统计信息（Avg, Max, Meter）自动更新相关的指标
        this.commitSensor.record(responseLatencyMs);
    }
}
