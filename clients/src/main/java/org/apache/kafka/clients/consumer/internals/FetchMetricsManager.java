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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

/**
 * The {@link FetchMetricsManager} class provides wrapper methods to record lag, lead, latency, and fetch metrics.
 * It keeps an internal ID of the assigned set of partitions which is updated to ensure the set of metrics it
 * records matches up with the topic-partitions in use.
 */
public class FetchMetricsManager {

    private final Metrics metrics;
    private final FetchMetricsRegistry metricsRegistry;
    private final Sensor throttleTime;
    private final Sensor bytesFetched;
    private final Sensor recordsFetched;
    private final Sensor fetchLatency;
    private final Sensor recordsLag;
    private final Sensor recordsLead;

    private int assignmentId = 0;
    private Set<TopicPartition> assignedPartitions = Collections.emptySet();

    /**
     * FetchMetricsManager 的构造函数。
     * 初始化所有必要的传感器，用于记录各种获取相关的指标。
     *
     * @param metrics Kafka 指标收集器实例
     * @param metricsRegistry 获取指标注册表，包含指标定义
     * 应用场景: 在创建消费者实例时，会创建 FetchMetricsManager 来管理其获取指标。
     * 设计考虑: 构造函数负责初始化所有传感器，确保在消费者开始获取数据之前，所有指标都已准备好被记录。
     */
    public FetchMetricsManager(Metrics metrics, FetchMetricsRegistry metricsRegistry) {
        // 初始化 metrics 字段
        this.metrics = metrics;
        // 初始化 metricsRegistry 字段
        this.metricsRegistry = metricsRegistry;

        // 初始化节流时间传感器 (fetch-throttle-time)
        // 包括平均值和最大值指标
        this.throttleTime = new SensorBuilder(metrics, "fetch-throttle-time")
                .withAvg(metricsRegistry.fetchThrottleTimeAvg)
                .withMax(metricsRegistry.fetchThrottleTimeMax)
                .build();
        // 初始化获取字节数传感器 (bytes-fetched)
        // 包括平均值、最大值以及速率和总数计量器
        this.bytesFetched = new SensorBuilder(metrics, "bytes-fetched")
                .withAvg(metricsRegistry.fetchSizeAvg)
                .withMax(metricsRegistry.fetchSizeMax)
                .withMeter(metricsRegistry.bytesConsumedRate, metricsRegistry.bytesConsumedTotal)
                .build();
        // 初始化获取记录数传感器 (records-fetched)
        // 包括平均值以及速率和总数计量器
        this.recordsFetched = new SensorBuilder(metrics, "records-fetched")
                .withAvg(metricsRegistry.recordsPerRequestAvg)
                .withMeter(metricsRegistry.recordsConsumedRate, metricsRegistry.recordsConsumedTotal)
                .build();
        // 初始化获取延迟传感器 (fetch-latency)
        // 包括平均值、最大值以及请求速率和总数计量器
        this.fetchLatency = new SensorBuilder(metrics, "fetch-latency")
                .withAvg(metricsRegistry.fetchLatencyAvg)
                .withMax(metricsRegistry.fetchLatencyMax)
                .withMeter(new WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)
                .build();
        // 初始化记录延迟传感器 (records-lag)
        // 包括最大值指标
        this.recordsLag = new SensorBuilder(metrics, "records-lag")
                .withMax(metricsRegistry.recordsLagMax)
                .build();
        // 初始化记录领先传感器 (records-lead)
        // 包括最小值指标
        this.recordsLead = new SensorBuilder(metrics, "records-lead")
                .withMin(metricsRegistry.recordsLeadMin)
                .build();
    }

    /**
     * 获取节流时间传感器。
     *
     * @return 用于记录获取节流时间的 {@link Sensor} 实例。
     * 应用场景: 当消费者在获取数据时遇到节流，可以使用此传感器记录节流的持续时间。
     * 设计考虑: 提供一个公共方法来访问节流时间传感器，允许其他组件记录此指标。
     */
    public Sensor throttleTimeSensor() {
        // 返回 throttleTime 传感器实例
        return throttleTime;
    }

    /**
     * 记录指定节点的请求延迟。
     * 此方法用于追踪和记录从特定节点获取数据时产生的延迟。
     *
     * @param node 发生延迟的节点标识符。如果为空字符串，则只记录全局延迟。
     * @param requestLatencyMs 请求延迟时间，单位为毫秒。
     * 应用场景: 当消费者从 Kafka Broker 拉取消息后，调用此方法记录网络请求和处理所花费的时间。
     * 设计考虑: 区分节点延迟和全局延迟，有助于定位特定节点的性能瓶颈。
     *           如果节点名称为空，则只更新全局的 fetchLatency 指标；否则，还会更新特定节点的延迟指标。
     */
    void recordLatency(String node, long requestLatencyMs) {
        // 记录全局的获取延迟
        fetchLatency.record(requestLatencyMs);
        // 检查节点名称是否为空
        if (!node.isEmpty()) {
            // 构建特定节点的延迟指标名称，格式为 "node-<node>.latency"
            String nodeTimeName = "node-" + node + ".latency";
            // 从 metrics 对象中获取对应节点名称的 Sensor 对象
            Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
            // 如果找到了对应节点的 Sensor 对象
            if (nodeRequestTime != null)
                // 记录特定节点的请求延迟
                nodeRequestTime.record(requestLatencyMs);
        }
    }

    /**
     * 记录获取到的总字节数。
     * 此方法用于追踪消费者在一次或多次拉取操作中获取到的数据总量（字节为单位）。
     *
     * @param bytes 获取到的字节数。
     * 应用场景: 在每次成功从 Broker 拉取到数据后，调用此方法更新已获取字节数的指标。
     * 设计考虑: 这是一个全局指标，不区分主题或分区，用于监控整体的数据吞吐量。
     */
    void recordBytesFetched(int bytes) {
        // 使用全局的 bytesFetched 传感器记录获取到的字节数
        bytesFetched.record(bytes);
    }

    /**
     * 记录获取到的总记录数。
     * 此方法用于追踪消费者在一次或多次拉取操作中获取到的消息记录总数。
     *
     * @param records 获取到的记录数。
     * 应用场景: 在每次成功从 Broker 拉取到数据后，调用此方法更新已获取记录数的指标。
     * 设计考虑: 这是一个全局指标，不区分主题或分区，用于监控整体的消息处理速率。
     */
    void recordRecordsFetched(int records) {
        // 使用全局的 recordsFetched 传感器记录获取到的记录数
        recordsFetched.record(records);
    }

    /**
     * 记录从指定主题获取到的字节数。
     * 此方法用于追踪消费者从特定主题拉取数据时的数据量。
     *
     * @param topic 从中获取字节数的主题名称。
     * @param bytes 从该主题获取到的字节数。
     * 应用场景: 当消费者从特定主题拉取数据后，调用此方法记录该主题的数据吞吐量。
     * 设计考虑: 为每个主题创建独立的 Sensor 来记录字节数，有助于监控和分析各个主题的流量。
     *           同时会尝试记录一个已弃用的指标，以保持向后兼容性。
     */
    void recordBytesFetched(String topic, int bytes) {
        // 根据主题名称生成字节获取指标的名称
        String name = topicBytesFetchedMetricName(topic);
        // 尝试记录已弃用的按主题获取字节数的指标（如果需要）
        maybeRecordDeprecatedBytesFetched(name, topic, bytes);

        // 构建一个新的 Sensor 用于记录指定主题的字节获取情况
        // Sensor 名称为生成的 name，并包含 "topic" 标签
        Sensor bytesFetched = new SensorBuilder(metrics, name, () -> Map.of("topic", topic))
            // 配置平均值指标
            .withAvg(metricsRegistry.topicFetchSizeAvg)
            // 配置最大值指标
            .withMax(metricsRegistry.topicFetchSizeMax)
            // 配置速率和总数计量器
            .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal)
            // 构建 Sensor 实例
            .build();
        // 使用新创建的 Sensor 记录从该主题获取的字节数
        bytesFetched.record(bytes);
    }

    /**
     * 记录从指定主题获取到的记录数。
     * 此方法用于追踪消费者从特定主题拉取消息的记录数量。
     *
     * @param topic 从中获取记录数的主题名称。
     * @param records 从该主题获取到的记录数。
     * 应用场景: 当消费者从特定主题拉取消息后，调用此方法记录该主题的消息处理速率。
     * 设计考虑: 为每个主题创建独立的 Sensor 来记录记录数，有助于监控和分析各个主题的消息量。
     *           同时会尝试记录一个已弃用的指标，以保持向后兼容性。
     */
    void recordRecordsFetched(String topic, int records) {
        // 根据主题名称生成记录获取指标的名称
        String name = topicRecordsFetchedMetricName(topic);
        // 尝试记录已弃用的按主题获取记录数的指标（如果需要）
        maybeRecordDeprecatedRecordsFetched(name, topic, records);

        // 构建一个新的 Sensor 用于记录指定主题的记录获取情况
        // Sensor 名称为生成的 name，并包含 "topic" 标签
        Sensor recordsFetched = new SensorBuilder(metrics, name, () -> Map.of("topic", topic))
            // 配置平均值指标 (每个请求的记录数)
            .withAvg(metricsRegistry.topicRecordsPerRequestAvg)
            // 配置速率和总数计量器 (记录消耗速率和总消耗记录数)
            .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal)
            // 构建 Sensor 实例
            .build();
        // 使用新创建的 Sensor 记录从该主题获取的记录数
        recordsFetched.record(records);
    }

    /**
     * 记录指定主题分区的记录延迟（lag）。
     * Lag 是指消费者当前消费位点与分区最新消息位点之间的差距。
     *
     * @param tp 要记录延迟的主题分区对象。
     * @param lag 该分区的记录延迟值。
     * 应用场景: 消费者在处理来自特定分区的消息时，定期计算并记录其消费延迟，以监控消费进度。
     * 设计考虑: 同时更新全局的 recordsLag 指标和特定分区的 lag 指标。
     *           为每个分区创建独立的 Sensor，包含 "topic" 和 "partition" 标签，以便细粒度监控。
     *           还会尝试记录一个已弃用的指标。
     */
    void recordPartitionLag(TopicPartition tp, long lag) {
        // 记录全局的记录延迟
        this.recordsLag.record(lag);

        // 根据主题分区生成分区记录延迟指标的名称
        String name = partitionRecordsLagMetricName(tp);
        // 尝试记录已弃用的按分区记录延迟的指标（如果需要）
        maybeRecordDeprecatedPartitionLag(name, tp, lag);

        // 构建一个新的 Sensor 用于记录指定分区的记录延迟
        // Sensor 名称为生成的 name，并包含 "topic" 和 "partition" 标签
        Sensor recordsLag = new SensorBuilder(metrics, name, () -> mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))
            // 配置当前值指标
            .withValue(metricsRegistry.partitionRecordsLag)
            // 配置最大值指标
            .withMax(metricsRegistry.partitionRecordsLagMax)
            // 配置平均值指标
            .withAvg(metricsRegistry.partitionRecordsLagAvg)
            // 构建 Sensor 实例
            .build();

        // 使用新创建的 Sensor 记录该分区的记录延迟
        recordsLag.record(lag);
    }

    /**
     * 记录指定主题分区的记录领先（lead）。
     * Lead 通常用于衡量消费者相对于分区起始位点的进度，或者在某些场景下表示预读的程度。
     *
     * @param tp 要记录领先值的主题分区对象。
     * @param lead 该分区的记录领先值。
     * 应用场景: 在某些特定的消费策略或监控需求下，需要追踪消费者相对于分区起始或其他基准的领先程度。
     * 设计考虑: 同时更新全局的 recordsLead 指标和特定分区的 lead 指标。
     *           为每个分区创建独立的 Sensor，包含 "topic" 和 "partition" 标签。
     *           还会尝试记录一个已弃用的指标。
     */
    void recordPartitionLead(TopicPartition tp, long lead) {
        // 记录全局的记录领先值
        this.recordsLead.record(lead);

        // 根据主题分区生成分区记录领先指标的名称
        String name = partitionRecordsLeadMetricName(tp);
        // 尝试记录已弃用的按分区记录领先的指标（如果需要）
        maybeRecordDeprecatedPartitionLead(name, tp, lead);

        // 构建一个新的 Sensor 用于记录指定分区的记录领先情况
        // Sensor 名称为生成的 name，并包含 "topic" 和 "partition" 标签
        Sensor recordsLead = new SensorBuilder(metrics, name, () -> mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))
            // 配置当前值指标
            .withValue(metricsRegistry.partitionRecordsLead)
            // 配置最小值指标
            .withMin(metricsRegistry.partitionRecordsLeadMin)
            // 配置平均值指标
            .withAvg(metricsRegistry.partitionRecordsLeadAvg)
            // 构建 Sensor 实例
            .build();

        // 使用新创建的 Sensor 记录该分区的记录领先值
        recordsLead.record(lead);
    }

    /**
     * 此方法由 {@link Fetch fetch} 逻辑在请求获取之前调用，以更新内部跟踪的指标集。
     *
     * @param subscription {@link SubscriptionState} 包含已分配分区集合的订阅状态对象
     * @see SubscriptionState#assignmentId()
     * @return void 无返回值
     * 
     * 应用场景: 当消费者的分区分配发生变化时（例如，由于再均衡），此方法被调用以更新与这些分区相关的度量指标。
     * 设计考虑: 通过比较当前的 assignmentId 和新的 assignmentId，可以有效地检测分区分配是否已更改。
     *           如果已更改，则会移除旧分配中不再存在的分区的指标，并为新分配中新增的分区添加指标。
     *           这确保了度量指标始终准确反映当前活动的分区。
     */
    void maybeUpdateAssignment(SubscriptionState subscription) { // 方法定义：可能更新分配状态
        // 获取订阅状态中的新分配ID
        int newAssignmentId = subscription.assignmentId();

        // 检查当前分配ID是否与新分配ID不同，如果不同，则表示分配已更改
        if (this.assignmentId != newAssignmentId) {
            // 获取新的已分配分区集合
            Set<TopicPartition> newAssignedPartitions = subscription.assignedPartitions();

            // 遍历当前已分配的分区
            for (TopicPartition tp : this.assignedPartitions) {
                // 如果新的已分配分区集合中不包含当前分区，则表示该分区已被取消分配
                if (!newAssignedPartitions.contains(tp)) {
                    // 移除该分区的记录延迟传感器
                    metrics.removeSensor(partitionRecordsLagMetricName(tp));
                    // 移除该分区的记录领先传感器
                    metrics.removeSensor(partitionRecordsLeadMetricName(tp));
                    // 移除该分区的首选读取副本指标
                    metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp));
                    // 移除已弃用的指标
                    // 移除已弃用的分区记录延迟传感器
                    metrics.removeSensor(deprecatedMetricName(partitionRecordsLagMetricName(tp)));
                    // 移除已弃用的分区记录领先传感器
                    metrics.removeSensor(deprecatedMetricName(partitionRecordsLeadMetricName(tp)));
                    // 移除已弃用的分区首选读取副本指标
                    metrics.removeMetric(deprecatedPartitionPreferredReadReplicaMetricName(tp));
                }
            }

            // 遍历新的已分配分区
            for (TopicPartition tp : newAssignedPartitions) {
                // 如果当前已分配的分区集合中不包含新的分区，则表示该分区是新分配的
                if (!this.assignedPartitions.contains(tp)) {
                    // 尝试记录已弃用的首选读取副本指标
                    maybeRecordDeprecatedPreferredReadReplica(tp, subscription);

                    // 获取分区首选读取副本的指标名称
                    MetricName metricName = partitionPreferredReadReplicaMetricName(tp);
                    // 如果指标不存在，则添加该分区的首选读取副本指标
                    // 该指标的值通过 Gauge 函数动态获取，返回首选读取副本的 ID，如果不存在则返回 -1
                    metrics.addMetricIfAbsent(
                        metricName, // 指标名称
                        null, // 指标描述 (可选)
                        (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1) // 获取指标值的函数
                    );
                }
            }

            // 更新当前已分配的分区集合为新的集合
            this.assignedPartitions = newAssignedPartitions;
            // 更新当前分配ID为新的ID
            this.assignmentId = newAssignmentId;
        }
    }

    /**
     * @deprecated 此方法将在 Kafka 5.0 版本中移除。
     * 尝试记录已弃用的按主题获取字节数的指标（如果需要）。
     * 
     * @param name 指标名称
     * @param topic 主题名称
     * @param bytes 获取的字节数
     * @return void 无返回值
     * 
     * 应用场景: 用于在过渡期间保持对旧版本指标的兼容性。
     * 设计考虑: 通过 shouldReportDeprecatedMetric 方法判断是否需要报告已弃用的指标，避免不必要的指标创建和记录。
     */
    @Deprecated // 标记此方法已弃用，并将在 Kafka 5.0 版本中移除
    private void maybeRecordDeprecatedBytesFetched(String name, String topic, int bytes) { // 方法定义：可能记录已弃用的获取字节数
        // 检查是否应该报告此主题的已弃用指标
        if (shouldReportDeprecatedMetric(topic)) {
            // 创建一个用于记录已弃用字节获取指标的 Sensor
            // Sensor 名称通过 deprecatedMetricName(name) 生成，并包含主题标签
            Sensor deprecatedBytesFetched = new SensorBuilder(metrics, deprecatedMetricName(name), () -> topicTags(topic))
                .withAvg(metricsRegistry.topicFetchSizeAvg) // 配置平均值指标 (主题获取大小平均值)
                .withMax(metricsRegistry.topicFetchSizeMax) // 配置最大值指标 (主题获取大小最大值)
                .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal) // 配置速率和总数计量器 (主题字节消耗速率和总消耗字节数)
                .build(); // 构建 Sensor 实例
            // 使用创建的 Sensor 记录获取的字节数
            deprecatedBytesFetched.record(bytes);
        }
    }

    /**
     * @deprecated 此方法将在 Kafka 5.0 版本中移除。
     * 尝试记录已弃用的按主题获取记录数的指标（如果需要）。
     * 
     * @param name 指标名称
     * @param topic 主题名称
     * @param records 获取的记录数
     * @return void 无返回值
     * 
     * 应用场景: 用于在过渡期间保持对旧版本指标的兼容性。
     * 设计考虑: 通过 shouldReportDeprecatedMetric 方法判断是否需要报告已弃用的指标。
     */
    @Deprecated // 标记此方法已弃用，并将在 Kafka 5.0 版本中移除
    private void maybeRecordDeprecatedRecordsFetched(String name, String topic, int records) { // 方法定义：可能记录已弃用的获取记录数
        // 检查是否应该报告此主题的已弃用指标
        if (shouldReportDeprecatedMetric(topic)) {
            // 创建一个用于记录已弃用记录获取指标的 Sensor
            // Sensor 名称通过 deprecatedMetricName(name) 生成，并包含主题标签
            Sensor deprecatedRecordsFetched = new SensorBuilder(metrics, deprecatedMetricName(name), () -> topicTags(topic))
                .withAvg(metricsRegistry.topicRecordsPerRequestAvg) // 配置平均值指标 (主题每请求记录数平均值)
                .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal) // 配置速率和总数计量器 (主题记录消耗速率和总消耗记录数)
                .build(); // 构建 Sensor 实例
            // 使用创建的 Sensor 记录获取的记录数
            deprecatedRecordsFetched.record(records);
        }
    }

    /**
     * @deprecated 此方法将在 Kafka 5.0 版本中移除。
     * 尝试记录已弃用的分区记录延迟指标（如果需要）。
     * 
     * @param name 指标名称
     * @param tp 主题分区对象
     * @param lag 记录延迟值
     * @return void 无返回值
     * 
     * 应用场景: 用于在过渡期间保持对旧版本指标的兼容性。
     * 设计考虑: 通过 shouldReportDeprecatedMetric 方法判断是否需要报告已弃用的指标。
     */
    @Deprecated // 标记此方法已弃用，并将在 Kafka 5.0 版本中移除
    private void maybeRecordDeprecatedPartitionLag(String name, TopicPartition tp, long lag) { // 方法定义：可能记录已弃用的分区延迟
        // 检查是否应该报告此主题分区的已弃用指标
        if (shouldReportDeprecatedMetric(tp.topic())) {
            // 创建一个用于记录已弃用分区记录延迟指标的 Sensor
            // Sensor 名称通过 deprecatedMetricName(name) 生成，并包含主题分区标签
            Sensor deprecatedRecordsLag = new SensorBuilder(metrics, deprecatedMetricName(name), () -> topicPartitionTags(tp))
                .withValue(metricsRegistry.partitionRecordsLag) // 配置当前值指标 (分区记录延迟)
                .withMax(metricsRegistry.partitionRecordsLagMax) // 配置最大值指标 (分区记录延迟最大值)
                .withAvg(metricsRegistry.partitionRecordsLagAvg) // 配置平均值指标 (分区记录延迟平均值)
                .build(); // 构建 Sensor 实例

            // 使用创建的 Sensor 记录延迟值
            deprecatedRecordsLag.record(lag);
        }
    }

    /**
     * @deprecated 此方法将在 Kafka 5.0 版本中移除。
     * 尝试记录已弃用的分区记录领先指标（如果需要）。
     * 
     * @param name 指标名称
     * @param tp 主题分区对象
     * @param lead 记录领先值
     * @return void 无返回值
     * 
     * 应用场景: 用于在过渡期间保持对旧版本指标的兼容性。
     * 设计考虑: 通过 shouldReportDeprecatedMetric 方法判断是否需要报告已弃用的指标。
     */
    @Deprecated // 标记此方法已弃用，并将在 Kafka 5.0 版本中移除
    private void maybeRecordDeprecatedPartitionLead(String name, TopicPartition tp, double lead) { // 方法定义：可能记录已弃用的分区领先
        // 检查是否应该报告此主题分区的已弃用指标
        if (shouldReportDeprecatedMetric(tp.topic())) {
            // 创建一个用于记录已弃用分区记录领先指标的 Sensor
            // Sensor 名称通过 deprecatedMetricName(name) 生成，并包含主题分区标签
            Sensor deprecatedRecordsLead = new SensorBuilder(metrics, deprecatedMetricName(name), () -> topicPartitionTags(tp))
                .withValue(metricsRegistry.partitionRecordsLead) // 配置当前值指标 (分区记录领先)
                .withMin(metricsRegistry.partitionRecordsLeadMin) // 配置最小值指标 (分区记录领先最小值)
                .withAvg(metricsRegistry.partitionRecordsLeadAvg) // 配置平均值指标 (分区记录领先平均值)
                .build(); // 构建 Sensor 实例

            // 使用创建的 Sensor 记录领先值
            deprecatedRecordsLead.record(lead);
        }
    }

    /**
     * @deprecated 此方法将在 Kafka 5.0 版本中移除。
     * 尝试记录已弃用的首选读取副本指标（如果需要）。
     * 
     * @param tp 主题分区对象
     * @param subscription 订阅状态对象
     * @return void 无返回值
     * 
     * 应用场景: 用于在过渡期间保持对旧版本指标的兼容性。
     * 设计考虑: 通过 shouldReportDeprecatedMetric 方法判断是否需要报告已弃用的指标。
     */
    @Deprecated // 标记此方法已弃用，并将在 Kafka 5.0 版本中移除
    private void maybeRecordDeprecatedPreferredReadReplica(TopicPartition tp, SubscriptionState subscription) { // 方法定义：可能记录已弃用的首选读取副本
        // 检查是否应该报告此主题分区的已弃用指标
        if (shouldReportDeprecatedMetric(tp.topic())) {
            // 获取已弃用的分区首选读取副本的指标名称
            MetricName metricName = deprecatedPartitionPreferredReadReplicaMetricName(tp);
            // 如果指标不存在，则添加该分区的已弃用首选读取副本指标
            // 该指标的值通过 Gauge 函数动态获取，返回首选读取副本的 ID，如果不存在则返回 -1
            metrics.addMetricIfAbsent(
                metricName, // 指标名称
                null, // 指标描述 (可选)
                (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1) // 获取指标值的函数
            );
        }
    }

    /**
     * 生成主题获取字节数指标的名称。
     * 
     * @param topic 主题名称
     * @return String 指标名称，格式为 "topic.{topic}.bytes-fetched"
     * 
     * 应用场景: 用于为特定主题的字节获取指标生成唯一的名称。
     * 设计考虑: 静态方法，方便在类内部和外部使用，确保指标名称的一致性。
     */
    private static String topicBytesFetchedMetricName(String topic) { // 方法定义：获取主题字节获取指标名称
        // 返回格式化的指标名称字符串
        return "topic." + topic + ".bytes-fetched";
    }

    /**
     * 生成主题获取记录数指标的名称。
     * 
     * @param topic 主题名称
     * @return String 指标名称，格式为 "topic.{topic}.records-fetched"
     * 
     * 应用场景: 用于为特定主题的记录获取指标生成唯一的名称。
     * 设计考虑: 静态方法，确保指标名称的一致性。
     */
    private static String topicRecordsFetchedMetricName(String topic) { // 方法定义：获取主题记录获取指标名称
        // 返回格式化的指标名称字符串
        return "topic." + topic + ".records-fetched";
    }

    /**
     * 生成分区记录领先指标的名称。
     * 
     * @param tp 主题分区对象
     * @return String 指标名称，格式为 "{topicPartition}.records-lead"
     * 
     * 应用场景: 用于为特定主题分区的记录领先指标生成唯一的名称。
     * 设计考虑: 静态方法，确保指标名称的一致性。
     */
    private static String partitionRecordsLeadMetricName(TopicPartition tp) { // 方法定义：获取分区记录领先指标名称
        // 返回格式化的指标名称字符串，tp.toString() 会给出 "topic-partition" 格式
        return tp + ".records-lead";
    }

    /**
     * 生成分区记录延迟指标的名称。
     * 
     * @param tp 主题分区对象
     * @return String 指标名称，格式为 "{topicPartition}.records-lag"
     * 
     * 应用场景: 用于为特定主题分区的记录延迟指标生成唯一的名称。
     * 设计考虑: 静态方法，确保指标名称的一致性。
     */
    private static String partitionRecordsLagMetricName(TopicPartition tp) { // 方法定义：获取分区记录延迟指标名称
        // 返回格式化的指标名称字符串
        return tp + ".records-lag";
    }

    /**
     * 生成已弃用指标的名称。
     * 
     * @param name 原始指标名称
     * @return String 已弃用指标的名称，格式为 "{name}.deprecated"
     * 
     * 应用场景: 用于为已弃用的指标生成统一的后缀，方便识别和管理。
     * 设计考虑: 静态方法，确保已弃用指标名称格式的一致性。
     */
    private static String deprecatedMetricName(String name) { // 方法定义：获取已弃用指标名称
        // 在原始指标名称后附加 ".deprecated"
        return name + ".deprecated";
    }

    /**
     * 判断是否应该报告指定主题的已弃用指标。
     * 当前的逻辑是，如果主题名称中包含 "."，则报告已弃用的指标。
     * 这通常用于区分内部主题（如 __consumer_offsets）和用户创建的主题。
     * 
     * @param topic 主题名称
     * @return boolean 如果应该报告已弃用指标，则返回 true；否则返回 false。
     * 
     * 应用场景: 在记录指标前，调用此方法判断是否需要记录相应的已弃用指标，以支持向后兼容。
     * 设计考虑: 这个判断条件可能基于特定的命名约定或配置，用于控制哪些主题的旧版指标需要继续报告。
     */
    private static boolean shouldReportDeprecatedMetric(String topic) { // 方法定义：判断是否应报告已弃用指标
        // 如果主题名称中包含点号 (".")，则返回 true，表示应该报告已弃用指标
        return topic.contains(".");
    }

    /**
     * 为指定的主题分区生成首选读取副本的指标名称。
     * 此指标用于追踪特定分区是否正在从其首选的读取副本读取数据。
     *
     * @param tp 主题分区对象，包含主题名称和分区号。
     * @return {@link MetricName} 指标名称对象，包含了指标的名称和标签。
     * 应用场景: 当需要监控特定分区是否从其指定的首选读取副本消费数据时，会使用此方法生成的指标名称。
     * 设计考虑: 指标标签中包含了主题和分区信息，方便按主题和分区进行聚合和筛选。
     */
    private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp) {
        // 创建一个包含主题和分区标签的Map
        Map<String, String> metricTags = mkMap(
            // 添加主题标签，值为主题名称
            mkEntry("topic", tp.topic()), 
            // 添加分区标签，值为分区号的字符串表示
            mkEntry("partition", String.valueOf(tp.partition()))
        );
        // 使用 metricsRegistry 中定义的 partitionPreferredReadReplica 模板和上面创建的标签，获取或创建指标实例
        return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
    }

    /**
     * (已弃用) 为指定的主题分区生成首选读取副本的指标名称。
     * 此方法已被弃用，因为它使用了已弃用的 {@link #topicPartitionTags(TopicPartition)} 方法来生成标签，
     * 该方法会将主题名称中的 '.' 替换为 '_'。
     *
     * @param tp 主题分区对象，包含主题名称和分区号。
     * @return {@link MetricName} 指标名称对象，包含了指标的名称和标签。
     * 应用场景: 旧版本代码中用于生成首选读取副本指标名称，新代码应使用 {@link #partitionPreferredReadReplicaMetricName(TopicPartition)}。
     * 设计考虑: 标记为 @Deprecated 以提示开发者此方法已过时，并引导使用新的实现。
     */
    @Deprecated
    private MetricName deprecatedPartitionPreferredReadReplicaMetricName(TopicPartition tp) {
        // 调用已弃用的 topicPartitionTags 方法生成指标标签
        Map<String, String> metricTags = topicPartitionTags(tp);
        // 使用 metricsRegistry 中定义的 partitionPreferredReadReplica 模板和上面创建的标签，获取或创建指标实例
        return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
    }

    /**
     * (已弃用) 为指定的主题生成指标标签。
     * 此方法已弃用，因为它会将主题名称中的 '.' 替换为 '_'，这可能导致指标名称不一致或难以解析。
     *
     * @param topic 主题名称。
     * @return 包含主题标签的Map，其中键为 "topic"，值为处理过的主题名称。
     * 应用场景: 旧版本代码中用于为仅包含主题信息的指标生成标签。
     * 设计考虑: 标记为 @Deprecated，因为替换 '.' 的行为是不推荐的，新的实现应直接使用原始主题名称。
     */
    @Deprecated
    static Map<String, String> topicTags(String topic) {
        // 创建一个只包含主题标签的Map，主题名称中的 '.' 被替换为 '_'
        return Map.of("topic", topic.replace('.', '_'));
    }

    /**
     * (已弃用) 为指定的主题分区生成指标标签。
     * 此方法已弃用，因为它会将主题名称中的 '.' 替换为 '_'，这可能导致指标名称不一致或难以解析。
     *
     * @param tp 主题分区对象，包含主题名称和分区号。
     * @return 包含主题和分区标签的Map。
     * 应用场景: 旧版本代码中用于为包含主题和分区信息的指标生成标签。
     * 设计考虑: 标记为 @Deprecated，因为替换 '.' 的行为是不推荐的，新的实现应直接使用原始主题名称。
     */
    @Deprecated
    static Map<String, String> topicPartitionTags(TopicPartition tp) {
        // 创建一个包含主题和分区标签的Map
        return mkMap(
            // 添加主题标签，值为主题名称（'.' 被替换为 '_'）
            mkEntry("topic", tp.topic().replace('.', '_')), 
            // 添加分区标签，值为分区号的字符串表示
            mkEntry("partition", String.valueOf(tp.partition()))
        );
    }

}