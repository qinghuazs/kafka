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
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

/**
 * HeartbeatMetricsManager 类负责管理与消费者心跳相关的指标。
 * 这些指标有助于监控消费者与协调器之间的心跳通信的健康状况和性能。
 * 应用场景：在 Kafka 消费者客户端中，用于跟踪心跳请求的频率、延迟以及自上次心跳以来的时间。
 * 实现细节：此类使用 Kafka Metrics 库来注册和更新各种心跳指标，例如最大响应时间、心跳速率和心跳总数。
 * 设计考虑：设计上旨在提供一组全面的心跳相关指标，同时保持较低的开销。
 * 它允许通过构造函数注入 Metrics 对象和度量组前缀，以实现灵活性和可测试性。
 */
public class HeartbeatMetricsManager {
    // MetricName 在测试中可见
    /**
     * 心跳响应时间最大值 MetricName。
     * 记录接收心跳请求响应所花费的最长时间。
     */
    final MetricName heartbeatResponseTimeMax;
    /**
     * 心跳速率 MetricName。
     * 记录每秒心跳数。
     */
    final MetricName heartbeatRate;
    /**
     * 心跳总数 MetricName。
     * 记录心跳总数。
     */
    final MetricName heartbeatTotal;
    /**
     * 最后一次心跳距今秒数 MetricName。
     * 记录自上次发送协调器心跳以来的秒数。
     */
    final MetricName lastHeartbeatSecondsAgo;
    /**
     * 用于记录心跳相关指标的 Sensor。
     * Sensor 是 Kafka Metrics 中用于收集和聚合原始测量值（如延迟）的组件。
     */
    private final Sensor heartbeatSensor;
    /**
     * 上一次心跳发送的时间戳（毫秒）。
     * 初始化为 -1L，表示尚未发送任何心跳。
     */
    private long lastHeartbeatMs = -1L;

    /**
     * HeartbeatMetricsManager 的构造函数。
     * 使用默认的消费者度量组前缀初始化指标。
     * 应用场景：当创建 HeartbeatMetricsManager 实例并使用标准的消费者指标分组时调用。
     * 实现细节：此构造函数委托给另一个构造函数，传入 Metrics 对象和默认的度量组前缀 {@link org.apache.kafka.clients.consumer.internals.ConsumerUtils#CONSUMER_METRIC_GROUP_PREFIX}。
     * 设计考虑：提供一个便捷的构造函数，简化在标准场景下的实例化过程。
     * @param metrics 用于注册指标的 Metrics 对象。
     */
    public HeartbeatMetricsManager(Metrics metrics) {
        // 调用另一个构造函数，使用提供的 Metrics 对象和默认的消费者度量组前缀
        this(metrics, CONSUMER_METRIC_GROUP_PREFIX);
    }

    /**
     * HeartbeatMetricsManager 的构造函数。
     * 使用指定的 Metrics 对象和度量组前缀初始化所有心跳相关的指标。
     * 应用场景：当需要为心跳指标指定自定义的度量组前缀时（例如，用于区分不同类型的消费者或共享组消费者）。
     * 实现细节：
     * 1. 构造度量组名称，由前缀和协调器后缀组成。
     * 2. 创建一个名为 "heartbeat-latency" 的 Sensor，用于收集心跳延迟数据。
     * 3. 定义并注册 "heartbeat-response-time-max" 指标，使用 Max 统计器记录最大心跳响应时间。
     * 4. 定义并注册 "heartbeat-rate" 和 "heartbeat-total" 指标，使用 Meter 和 WindowedCount 统计器记录每秒心跳数和总心跳数。
     * 5. 定义一个 Measurable (可测量) 对象 `lastHeartbeat`，用于计算自上次心跳以来的秒数。
     *    - 如果从未发送过心跳 (lastHeartbeatMs < 0)，则返回 -1。
     *    - 否则，计算当前时间与上次心跳时间之差，并转换为秒。
     * 6. 定义并注册 "last-heartbeat-seconds-ago" 指标，使用上面定义的 `lastHeartbeat` Measurable。
     * 设计考虑：此构造函数提供了配置度量组名称的灵活性，允许指标在 Metrics 注册表中更有条理地组织。
     * 它确保了所有必要的心跳指标都在实例化时被正确设置。
     * @param metrics 用于注册指标的 Metrics 对象。
     * @param metricGroupPrefix 用于构造度量组名称的前缀。
     */
    public HeartbeatMetricsManager(Metrics metrics, String metricGroupPrefix) {
        // 构造完整的度量组名称，例如 "consumer-coordinator-metrics" 或 "consumer-share-coordinator-metrics"
        final String metricGroupName = metricGroupPrefix + COORDINATOR_METRICS_SUFFIX;
        // 获取或创建一个名为 "heartbeat-latency" 的 Sensor 实例，用于记录心跳延迟
        heartbeatSensor = metrics.sensor("heartbeat-latency");
        // 定义 "heartbeat-response-time-max" 指标的 MetricName
        // 描述：接收心跳请求响应所花费的最长时间
        heartbeatResponseTimeMax = metrics.metricName("heartbeat-response-time-max",
            metricGroupName,
            "接收心跳请求响应所花费的最长时间");
        // 将 Max 统计器添加到 heartbeatSensor，用于计算 heartbeatResponseTimeMax 指标
        heartbeatSensor.add(heartbeatResponseTimeMax, new Max());

        // 窗口化计量器 (windowed meters)
        // 定义 "heartbeat-rate" 指标的 MetricName
        // 描述：每秒心跳数
        heartbeatRate = metrics.metricName("heartbeat-rate", metricGroupName, "每秒心跳数");
        // 定义 "heartbeat-total" 指标的 MetricName
        // 描述：心跳总数
        heartbeatTotal = metrics.metricName("heartbeat-total", metricGroupName, "心跳总数");
        // 将 Meter 统计器（使用 WindowedCount）添加到 heartbeatSensor，用于计算 heartbeatRate 和 heartbeatTotal 指标
        heartbeatSensor.add(new Meter(new WindowedCount(),
            heartbeatRate,
            heartbeatTotal));

        // 定义一个 Measurable 对象，用于动态计算 "last-heartbeat-seconds-ago" 指标的值
        Measurable lastHeartbeat = (config, now) -> {
            // 获取上次记录的心跳发送时间
            final long lastHeartbeatSend = lastHeartbeatMs;
            // 检查是否从未发送过心跳
            if (lastHeartbeatSend < 0L)
                // 如果从未触发过心跳，则返回 -1。
                return -1d;
            else
                // 计算当前时间与上次心跳发送时间之间的差值（毫秒），并将其转换为秒
                return TimeUnit.SECONDS.convert(now - lastHeartbeatSend, TimeUnit.MILLISECONDS);
        };
        // 定义 "last-heartbeat-seconds-ago" 指标的 MetricName
        // 描述：自上次发送协调器心跳以来的秒数
        lastHeartbeatSecondsAgo = metrics.metricName("last-heartbeat-seconds-ago",
            metricGroupName,
            "自上次发送协调器心跳以来的秒数");
        // 将 lastHeartbeat Measurable 添加到 Metrics 注册表，与 lastHeartbeatSecondsAgo MetricName 关联
        metrics.addMetric(lastHeartbeatSecondsAgo, lastHeartbeat);
    }

    /**
     * 记录心跳发送的时间戳。
     * 此方法在每次发送心跳请求时调用，以更新最后一次心跳的时间。
     * 应用场景：当消费者客户端成功发送心跳请求到协调器后，调用此方法。
     * 实现细节：将传入的时间戳 `timeMs` 赋值给 `lastHeartbeatMs` 字段。
     * 设计考虑：简单直接地更新时间戳，用于后续计算 "last-heartbeat-seconds-ago" 指标。
     * @param timeMs 心跳发送的时间戳，单位为毫秒。
     */
    public void recordHeartbeatSentMs(long timeMs) {
        // 更新 lastHeartbeatMs 字段为当前心跳发送的时间戳
        lastHeartbeatMs = timeMs;
    }

    /**
     * 记录心跳请求的延迟。
     * 此方法在收到心跳响应后调用，用于记录请求的往返时间。
     * 应用场景：当消费者客户端收到协调器对心跳请求的响应后，计算出请求延迟并调用此方法。
     * 实现细节：使用 `heartbeatSensor` 的 `record` 方法记录传入的请求延迟 `requestLatencyMs`。
     *           `heartbeatSensor` 会将此延迟值传递给其关联的统计器（如 Max, Meter）。
     * 设计考虑：通过 Sensor 记录延迟，可以利用 Kafka Metrics 库的强大功能自动计算各种聚合指标（如最大延迟、平均速率等）。
     * @param requestLatencyMs 心跳请求的延迟，单位为毫秒。
     */
    public void recordRequestLatency(long requestLatencyMs) {
        // 使用 heartbeatSensor 记录心跳请求的延迟
        // 这个记录会更新所有附加到此 sensor 的指标，例如 heartbeatResponseTimeMax, heartbeatRate, heartbeatTotal
        heartbeatSensor.record(requestLatencyMs);
    }
}
