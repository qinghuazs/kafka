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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.WindowedCount;

/**
 * 共享获取指标管理器
 * 用于管理和记录Kafka共享消费者的获取操作相关的性能指标
 * 
 * 应用场景：
 * 1. 监控消息获取性能
 * 2. 跟踪消息确认状态
 * 3. 记录节点延迟情况
 * 4. 分析吞吐量指标
 */
public class ShareFetchMetricsManager {
    /**
     * 指标注册表，用于管理所有指标
     */
    private final Metrics metrics;

    /**
     * 限流时间传感器，用于监控获取操作的限流情况
     */
    private final Sensor throttleTime;

    /**
     * 获取字节数传感器，用于监控数据传输量
     */
    private final Sensor bytesFetched;

    /**
     * 获取记录数传感器，用于监控消息数量
     */
    private final Sensor recordsFetched;

    /**
     * 获取延迟传感器，用于监控操作延迟
     */
    private final Sensor fetchLatency;

    /**
     * 已发送确认传感器，用于监控确认发送情况
     */
    private final Sensor sentAcknowledgements;

    /**
     * 失败确认传感器，用于监控确认失败情况
     */
    private final Sensor failedAcknowledgements;

    /**
     * 构造函数
     * 初始化所有性能指标传感器
     *
     * @param metrics 指标注册表
     * @param metricsRegistry 指标注册器，包含所有预定义的指标模板
     */
    public ShareFetchMetricsManager(Metrics metrics, ShareFetchMetricsRegistry metricsRegistry) {
        // 保存指标注册表引用
        this.metrics = metrics;

        // 初始化获取字节数传感器
        this.bytesFetched = new SensorBuilder(metrics, "bytes-fetched")
                .withAvg(metricsRegistry.fetchSizeAvg)    // 添加平均值统计
                .withMax(metricsRegistry.fetchSizeMax)    // 添加最大值统计
                .withMeter(metricsRegistry.bytesFetchedRate, metricsRegistry.bytesFetchedTotal)  // 添加速率和总量统计
                .build();

        // 初始化获取记录数传感器
        this.recordsFetched = new SensorBuilder(metrics, "records-fetched")
                .withAvg(metricsRegistry.recordsPerRequestAvg)  // 添加平均值统计
                .withMax(metricsRegistry.recordsPerRequestMax)  // 添加最大值统计
                .withMeter(metricsRegistry.recordsFetchedRate, metricsRegistry.recordsFetchedTotal)  // 添加速率和总量统计
                .build();

        // 初始化已发送确认传感器
        this.sentAcknowledgements = new SensorBuilder(metrics, "sent-acknowledgements")
                .withMeter(metricsRegistry.acknowledgementSendRate, metricsRegistry.acknowledgementSendTotal)  // 添加速率和总量统计
                .build();

        // 初始化失败确认传感器
        this.failedAcknowledgements = new SensorBuilder(metrics, "failed-acknowledgements")
                .withMeter(metricsRegistry.acknowledgementErrorRate, metricsRegistry.acknowledgementErrorTotal)  // 添加速率和总量统计
                .build();

        // 初始化获取延迟传感器
        this.fetchLatency = new SensorBuilder(metrics, "fetch-latency")
                .withAvg(metricsRegistry.fetchLatencyAvg)  // 添加平均值统计
                .withMax(metricsRegistry.fetchLatencyMax)  // 添加最大值统计
                .withMeter(new WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)  // 添加窗口计数统计
                .build();

        // 初始化限流时间传感器
        this.throttleTime = new SensorBuilder(metrics, "fetch-throttle-time")
                .withAvg(metricsRegistry.fetchThrottleTimeAvg)  // 添加平均值统计
                .withMax(metricsRegistry.fetchThrottleTimeMax)  // 添加最大值统计
                .build();
    }

    /**
     * 获取限流时间传感器
     * 用于外部访问和记录限流时间指标
     *
     * @return 限流时间传感器实例
     */
    public Sensor throttleTimeSensor() {
        return throttleTime;
    }

    /**
     * 记录延迟指标
     * 同时记录全局延迟和节点特定延迟
     *
     * @param node 节点标识符
     * @param requestLatencyMs 请求延迟（毫秒）
     */
    void recordLatency(String node, long requestLatencyMs) {
        // 记录全局获取延迟
        fetchLatency.record(requestLatencyMs);
        
        // 如果节点标识符不为空，记录节点特定延迟
        if (!node.isEmpty()) {
            // 构造节点时间传感器名称
            String nodeTimeName = "node-" + node + ".latency";
            // 获取节点特定的延迟传感器
            Sensor nodeRequestTime = metrics.getSensor(nodeTimeName);
            // 如果传感器存在，记录延迟
            if (nodeRequestTime != null)
                nodeRequestTime.record(requestLatencyMs);
        }
    }

    /**
     * 记录获取的字节数
     *
     * @param bytes 获取的字节数
     */
    void recordBytesFetched(int bytes) {
        // 记录获取的字节数
        bytesFetched.record(bytes);
    }

    /**
     * 记录获取的记录数
     *
     * @param records 获取的记录数
     */
    void recordRecordsFetched(int records) {
        // 记录获取的记录数
        recordsFetched.record(records);
    }

    /**
     * 记录已发送的确认数
     *
     * @param acknowledgements 发送的确认数
     */
    void recordAcknowledgementSent(int acknowledgements) {
        // 记录发送的确认数
        sentAcknowledgements.record(acknowledgements);
    }

    /**
     * 记录失败的确认数
     *
     * @param acknowledgements 失败的确认数
     */
    void recordFailedAcknowledgements(int acknowledgements) {
        // 记录失败的确认数
        failedAcknowledgements.record(acknowledgements);
    }
}
