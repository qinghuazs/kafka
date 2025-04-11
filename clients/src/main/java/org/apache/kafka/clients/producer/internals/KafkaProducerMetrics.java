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

package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

import java.util.Map;

/**
 * Kafka生产者指标监控类，用于收集和记录生产者的各种性能指标
 * 主要监控以下几类指标：
 * 1. 事务相关操作的耗时（初始化、开始、提交、中止等）
 * 2. 刷新操作的耗时
 * 3. 元数据等待时间
 * 所有时间指标均以纳秒为单位
 */
public class KafkaProducerMetrics implements AutoCloseable {

    // 指标组名称，用于标识这是生产者的指标
    public static final String GROUP = "producer-metrics";
    // 刷新操作的指标名称
    private static final String FLUSH = "flush";
    // 事务初始化的指标名称
    private static final String TXN_INIT = "txn-init";
    // 事务开始的指标名称
    private static final String TXN_BEGIN = "txn-begin";
    // 发送事务中偏移量的指标名称
    private static final String TXN_SEND_OFFSETS = "txn-send-offsets";
    // 事务提交的指标名称
    private static final String TXN_COMMIT = "txn-commit";
    // 事务中止的指标名称
    private static final String TXN_ABORT = "txn-abort";
    // 时间指标后缀，表示以纳秒为单位的总时间
    private static final String TOTAL_TIME_SUFFIX = "-time-ns-total";
    // 等待元数据的指标名称
    private static final String METADATA_WAIT = "metadata-wait";

    // 指标标签，用于附加额外的标识信息
    private final Map<String, String> tags;
    // 指标管理器，用于创建和管理所有指标
    private final Metrics metrics;
    // 事务初始化时间传感器
    private final Sensor initTimeSensor;
    // 事务开始时间传感器
    private final Sensor beginTxnTimeSensor;
    // 刷新操作时间传感器
    private final Sensor flushTimeSensor;
    // 发送偏移量时间传感器
    private final Sensor sendOffsetsSensor;
    // 事务提交时间传感器
    private final Sensor commitTxnSensor;
    // 事务中止时间传感器
    private final Sensor abortTxnSensor;
    // 元数据等待时间传感器
    private final Sensor metadataWaitSensor;

    /**
     * 构造函数，初始化所有指标传感器
     * @param metrics 指标管理器实例
     */
    public KafkaProducerMetrics(Metrics metrics) {
        this.metrics = metrics;
        // 获取配置的指标标签
        tags = this.metrics.config().tags();
        // 初始化刷新操作时间传感器
        flushTimeSensor = newLatencySensor(
            FLUSH,
            "Total time producer has spent in flush in nanoseconds."
        );
        // 初始化事务初始化时间传感器
        initTimeSensor = newLatencySensor(
            TXN_INIT,
            "Total time producer has spent in initTransactions in nanoseconds."
        );
        // 初始化事务开始时间传感器
        beginTxnTimeSensor = newLatencySensor(
            TXN_BEGIN,
            "Total time producer has spent in beginTransaction in nanoseconds."
        );
        // 初始化发送偏移量时间传感器
        sendOffsetsSensor = newLatencySensor(
            TXN_SEND_OFFSETS,
            "Total time producer has spent in sendOffsetsToTransaction in nanoseconds."
        );
        // 初始化事务提交时间传感器
        commitTxnSensor = newLatencySensor(
            TXN_COMMIT,
            "Total time producer has spent in commitTransaction in nanoseconds."
        );
        // 初始化事务中止时间传感器
        abortTxnSensor = newLatencySensor(
            TXN_ABORT,
            "Total time producer has spent in abortTransaction in nanoseconds."
        );
        // 初始化元数据等待时间传感器
        metadataWaitSensor = newLatencySensor(
            METADATA_WAIT,
            "Total time producer has spent waiting on topic metadata in nanoseconds."
        );
    }

    /**
     * 关闭并清理所有指标
     * 实现AutoCloseable接口，在生产者关闭时自动调用
     */
    @Override
    public void close() {
        // 移除所有注册的指标传感器
        removeMetric(FLUSH);
        removeMetric(TXN_INIT);
        removeMetric(TXN_BEGIN);
        removeMetric(TXN_SEND_OFFSETS);
        removeMetric(TXN_COMMIT);
        removeMetric(TXN_ABORT);
        removeMetric(METADATA_WAIT);
    }

    /**
     * 记录刷新操作的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordFlush(long duration) {
        flushTimeSensor.record(duration);
    }

    /**
     * 记录事务初始化的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordInit(long duration) {
        initTimeSensor.record(duration);
    }

    /**
     * 记录开始事务的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordBeginTxn(long duration) {
        beginTxnTimeSensor.record(duration);
    }

    /**
     * 记录发送事务中偏移量的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordSendOffsets(long duration) {
        sendOffsetsSensor.record(duration);
    }

    /**
     * 记录提交事务的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordCommitTxn(long duration) {
        commitTxnSensor.record(duration);
    }

    /**
     * 记录中止事务的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordAbortTxn(long duration) {
        abortTxnSensor.record(duration);
    }

    /**
     * 记录等待元数据的耗时
     * @param duration 耗时（纳秒）
     */
    public void recordMetadataWait(long duration) {
        metadataWaitSensor.record(duration);
    }

    /**
     * 创建新的延迟时间传感器
     * @param name 传感器名称
     * @param description 传感器描述
     * @return 创建的传感器实例
     */
    private Sensor newLatencySensor(String name, String description) {
        // 创建传感器，名称加上时间后缀
        Sensor sensor = metrics.sensor(name + TOTAL_TIME_SUFFIX);
        // 添加累计总和统计
        sensor.add(metricName(name, description), new CumulativeSum());
        return sensor;
    }

    /**
     * 创建指标名称
     * @param name 基础名称
     * @param description 指标描述
     * @return 完整的指标名称
     */
    private MetricName metricName(final String name, final String description) {
        // 创建指标名称，包含时间后缀、组名、描述和标签
        return metrics.metricName(name + TOTAL_TIME_SUFFIX, GROUP, description, tags);
    }

    /**
     * 移除指定的指标传感器
     * @param name 要移除的传感器名称
     */
    private void removeMetric(final String name) {
        // 移除指定名称（加上时间后缀）的传感器
        metrics.removeSensor(name + TOTAL_TIME_SUFFIX);
    }
}
