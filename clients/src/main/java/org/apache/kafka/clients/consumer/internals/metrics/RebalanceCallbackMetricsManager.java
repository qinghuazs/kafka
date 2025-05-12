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

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX; // 导入消费者度量组前缀常量
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX; // 导入协调器度量后缀常量

/**
 * 管理与消费者重平衡回调相关的度量指标。
 * 这个类负责创建和更新在重平衡监听器回调（如分区撤销、分配和丢失）期间所花费时间的度量指标。
 * 应用场景：在Kafka消费者客户端中，当分区所有权发生变化时，会触发重平衡回调。此类用于监控这些回调的性能。
 * 实现细节：使用Kafka Metrics库来注册和记录传感器（Sensor）和度量名称（MetricName）。
 * 设计考虑：提供专门的管理器来封装回调相关的度量逻辑，使得代码更模块化，易于维护和测试。
 */
public class RebalanceCallbackMetricsManager {
    /**
     * 分区撤销回调平均延迟的度量名称。
     * 用于跟踪 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)} 回调的平均执行时间。
     */
    final MetricName partitionRevokeLatencyAvg;
    /**
     * 分区分配回调平均延迟的度量名称。
     * 用于跟踪 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)} 回调的平均执行时间。
     */
    final MetricName partitionAssignLatencyAvg;
    /**
     * 分区丢失回调平均延迟的度量名称。
     * 用于跟踪 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost(java.util.Collection)} 回调的平均执行时间。
     */
    final MetricName partitionLostLatencyAvg;
    /**
     * 分区撤销回调最大延迟的度量名称。
     * 用于跟踪 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)} 回调的最大执行时间。
     */
    final MetricName partitionRevokeLatencyMax;
    /**
     * 分区分配回调最大延迟的度量名称。
     * 用于跟踪 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)} 回调的最大执行时间。
     */
    final MetricName partitionAssignLatencyMax;
    /**
     * 分区丢失回调最大延迟的度量名称。
     * 用于跟踪 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost(java.util.Collection)} 回调的最大执行时间。
     */
    final MetricName partitionLostLatencyMax;
    /**
     * 用于记录分区撤销回调延迟的传感器。
     */
    private final Sensor partitionRevokeCallbackSensor;
    /**
     * 用于记录分区分配回调延迟的传感器。
     */
    private final Sensor partitionAssignCallbackSensor;
    /**
     * 用于记录分区丢失回调延迟的传感器。
     */
    private final Sensor partitionLostCallbackSensor;

    /**
     * 构造函数，使用默认的度量组前缀初始化 RebalanceCallbackMetricsManager。
     * @param metrics Metrics 实例，用于注册度量指标。
     */
    public RebalanceCallbackMetricsManager(Metrics metrics) {
        // 调用另一个构造函数，并传入默认的消费者度量组前缀。
        // 设计考虑：提供一个便捷的构造函数，使用户不必关心度量组前缀的细节。
        this(metrics, CONSUMER_METRIC_GROUP_PREFIX);
    }

    /**
     * 构造函数，使用指定的度量组前缀初始化 RebalanceCallbackMetricsManager。
     * 这个构造函数负责初始化所有与重平衡回调相关的传感器和度量名称。
     * @param metrics Metrics 实例，用于注册度量指标。
     * @param grpMetricsPrefix 度量组的前缀字符串。
     */
    public RebalanceCallbackMetricsManager(Metrics metrics, String grpMetricsPrefix) {
        // 构造度量组名称，格式为：前缀 + "-coordinator-metrics"
        final String metricGroupName = grpMetricsPrefix + COORDINATOR_METRICS_SUFFIX;

        // 初始化分区撤销回调延迟相关的度量
        // 创建名为 "partition-revoked-latency" 的传感器
        partitionRevokeCallbackSensor = metrics.sensor("partition-revoked-latency");
        // 创建分区撤销平均延迟的度量名称
        partitionRevokeLatencyAvg = metrics.metricName("partition-revoked-latency-avg",
            metricGroupName, // 度量组名称
            "分区撤销重平衡监听器回调所花费的平均时间"); // 度量描述
        // 将平均值统计信息添加到传感器
        partitionRevokeCallbackSensor.add(partitionRevokeLatencyAvg, new Avg());
        // 创建分区撤销最大延迟的度量名称
        partitionRevokeLatencyMax = metrics.metricName("partition-revoked-latency-max",
            metricGroupName, // 度量组名称
            "分区撤销重平衡监听器回调所花费的最大时间"); // 度量描述
        // 将最大值统计信息添加到传感器
        partitionRevokeCallbackSensor.add(partitionRevokeLatencyMax, new Max());

        // 初始化分区分配回调延迟相关的度量
        // 创建名为 "partition-assigned-latency" 的传感器
        partitionAssignCallbackSensor = metrics.sensor("partition-assigned-latency");
        // 创建分区分配平均延迟的度量名称
        partitionAssignLatencyAvg = metrics.metricName("partition-assigned-latency-avg",
            metricGroupName, // 度量组名称
            "分区分配重平衡监听器回调所花费的平均时间"); // 度量描述
        // 将平均值统计信息添加到传感器
        partitionAssignCallbackSensor.add(partitionAssignLatencyAvg, new Avg());
        // 创建分区分配最大延迟的度量名称
        partitionAssignLatencyMax = metrics.metricName("partition-assigned-latency-max",
            metricGroupName, // 度量组名称
            "分区分配重平衡监听器回调所花费的最大时间"); // 度量描述
        // 将最大值统计信息添加到传感器
        partitionAssignCallbackSensor.add(partitionAssignLatencyMax, new Max());

        // 初始化分区丢失回调延迟相关的度量
        // 创建名为 "partition-lost-latency" 的传感器
        partitionLostCallbackSensor = metrics.sensor("partition-lost-latency");
        // 创建分区丢失平均延迟的度量名称
        partitionLostLatencyAvg = metrics.metricName("partition-lost-latency-avg",
            metricGroupName, // 度量组名称
            "分区丢失重平衡监听器回调所花费的平均时间"); // 度量描述
        // 将平均值统计信息添加到传感器
        partitionLostCallbackSensor.add(partitionLostLatencyAvg, new Avg());
        // 创建分区丢失最大延迟的度量名称
        partitionLostLatencyMax = metrics.metricName("partition-lost-latency-max",
            metricGroupName, // 度量组名称
            "分区丢失重平衡监听器回调所花费的最大时间"); // 度量描述
        // 将最大值统计信息添加到传感器
        partitionLostCallbackSensor.add(partitionLostLatencyMax, new Max());
    }

    /**
     * 记录分区撤销回调所花费的延迟时间。
     * @param latencyMs 延迟时间，单位为毫秒。
     */
    public void recordPartitionsRevokedLatency(long latencyMs) {
        // 使用 partitionRevokeCallbackSensor 记录延迟时间
        // 这个传感器会自动更新相关的平均值和最大值度量
        partitionRevokeCallbackSensor.record(latencyMs);
    }

    /**
     * 记录分区分配回调所花费的延迟时间。
     * @param latencyMs 延迟时间，单位为毫秒。
     */
    public void recordPartitionsAssignedLatency(long latencyMs) {
        // 使用 partitionAssignCallbackSensor 记录延迟时间
        // 这个传感器会自动更新相关的平均值和最大值度量
        partitionAssignCallbackSensor.record(latencyMs);
    }

    /**
     * 记录分区丢失回调所花费的延迟时间。
     * @param latencyMs 延迟时间，单位为毫秒。
     */
    public void recordPartitionsLostLatency(long latencyMs) {
        // 使用 partitionLostCallbackSensor 记录延迟时间
        // 这个传感器会自动更新相关的平均值和最大值度量
        partitionLostCallbackSensor.record(latencyMs);
    }
}
