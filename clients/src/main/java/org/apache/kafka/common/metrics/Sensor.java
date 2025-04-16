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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * Sensor(传感器)类用于将一系列连续的数值应用到一组相关的度量指标上。
 * 例如，一个消息大小的传感器会使用{@link #record(double)}方法记录一系列的消息大小，
 * 并维护一组关于请求大小的度量指标，如平均值或最大值。
 * 
 * 该类是Kafka指标系统的核心组件，负责：
 * 1. 收集和处理数值型指标数据
 * 2. 管理多个相关的度量指标(metrics)
 * 3. 支持分层的传感器结构(父子关系)
 * 4. 提供不同级别的记录控制(INFO/DEBUG/TRACE)
 * 5. 实现指标配额管理和检查
 */
public final class Sensor {

    // 指标注册表，用于管理所有的度量指标
    private final Metrics registry;
    // 传感器的唯一名称
    private final String name;
    // 父传感器数组，支持传感器的层级结构
    private final Sensor[] parents;
    // 统计配置列表，包含每个统计指标及其配置
    private final List<StatAndConfig> stats;
    // 度量指标映射表，key为指标名称，value为具体的Kafka指标对象
    private final Map<MetricName, KafkaMetric> metrics;
    // 指标配置对象，包含记录级别、配额等设置
    private final MetricConfig config;
    // 时间对象，用于获取当前时间
    private final Time time;
    // 最后一次记录数据的时间戳
    private volatile long lastRecordTime;
    // 传感器不活动过期时间(毫秒)
    private final long inactiveSensorExpirationTimeMs;
    // 度量指标的同步锁对象
    private final Object metricLock;

    /**
     * StatAndConfig内部类用于将统计对象(Stat)和其配置(MetricConfig)绑定在一起
     * 通过Supplier模式支持动态配置更新
     */
    private static class StatAndConfig {
        private final Stat stat;
        private final Supplier<MetricConfig> configSupplier;

        StatAndConfig(Stat stat, Supplier<MetricConfig> configSupplier) {
            this.stat = stat;
            this.configSupplier = configSupplier;
        }

        public Stat stat() {
            return stat;
        }

        public MetricConfig config() {
            return configSupplier.get();
        }

        @Override
        public String toString() {
            return "StatAndConfig(stat=" + stat + ')';
        }
    }

    /**
     * RecordingLevel枚举定义了传感器的记录级别
     * - INFO: 基本信息级别，只记录关键指标
     * - DEBUG: 调试级别，记录更详细的指标信息
     * - TRACE: 跟踪级别，记录最详细的指标信息
     */
    public enum RecordingLevel {
        INFO(0, "INFO"), DEBUG(1, "DEBUG"), TRACE(2, "TRACE");

        private static final RecordingLevel[] ID_TO_TYPE;
        private static final int MIN_RECORDING_LEVEL_KEY = 0;
        public static final int MAX_RECORDING_LEVEL_KEY;

        static {
            int maxRL = -1;
            for (RecordingLevel level : RecordingLevel.values()) {
                maxRL = Math.max(maxRL, level.id);
            }
            RecordingLevel[] idToName = new RecordingLevel[maxRL + 1];
            for (RecordingLevel level : RecordingLevel.values()) {
                idToName[level.id] = level;
            }
            ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final short id;

        RecordingLevel(int id, String name) {
            this.id = (short) id;
            this.name = name;
        }

        public static RecordingLevel forId(int id) {
            if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
                throw new IllegalArgumentException(String.format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
                    "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
            return ID_TO_TYPE[id];
        }

        /** Case insensitive lookup by protocol name */
        public static RecordingLevel forName(String name) {
            return RecordingLevel.valueOf(name.toUpperCase(Locale.ROOT));
        }

        public boolean shouldRecord(final int configId) {
            if (configId == INFO.id) {
                return this.id == INFO.id;
            } else if (configId == DEBUG.id) {
                return this.id == INFO.id || this.id == DEBUG.id;
            } else if (configId == TRACE.id) {
                return true;
            } else {
                throw new IllegalStateException("Did not recognize recording level " + configId);
            }
        }
    }

    private final RecordingLevel recordingLevel;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time,
           long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel) {
        super();
        this.registry = registry;
        this.name = Objects.requireNonNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new LinkedHashMap<>();
        this.stats = new ArrayList<>();
        this.config = config;
        this.time = time;
        this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
        this.lastRecordTime = time.milliseconds();
        this.recordingLevel = recordingLevel;
        this.metricLock = new Object();
        checkForest(new HashSet<>());
    }

    /* Validate that this sensor doesn't end up referencing itself */
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (Sensor parent : parents)
            parent.checkForest(sensors);
    }

    /**
     * 获取传感器的注册名称
     * 该名称在所有已注册的传感器中是唯一的，用于标识和查找特定的传感器
     * 
     * @return 传感器的唯一名称
     */
    public String name() {
        return this.name;
    }

    /**
     * 获取该传感器的所有父传感器
     * 父传感器用于构建传感器的层级结构，当前传感器的记录会传播到父传感器
     * 
     * @return 父传感器列表的不可修改视图
     */
    List<Sensor> parents() {
        return unmodifiableList(asList(parents));
    }

    /**
     * 检查当前传感器是否应该记录度量值
     * 基于传感器的记录级别(INFO/DEBUG/TRACE)和配置的记录级别判断
     * 
     * @return 如果应该记录返回true，否则返回false
     */
    public boolean shouldRecord() {
        return this.recordingLevel.shouldRecord(config.recordLevel().id);
    }

    /**
     * 记录一次发生，这是{@link #record(double) record(1.0)}的简写方式
     * 通常用于记录事件计数，每次调用相当于记录值1.0
     */
    public void record() {
        if (shouldRecord()) {
            recordInternal(1.0d, time.milliseconds(), true);
        }
    }

    /**
     * 使用传感器记录一个值
     * 该值将被应用到所有关联的度量指标中，并可能触发配额检查
     * 
     * @param value 要记录的数值
     * @throws QuotaViolationException 如果记录的值超出了配置的最大或最小边界值
     */
    public void record(double value) {
        if (shouldRecord()) {
            recordInternal(value, time.milliseconds(), true);
        }
    }

    /**
     * 在指定时间点记录一个值
     * 该方法比{@link #record(double)}稍快，因为它重用了提供的时间戳而不是获取当前时间
     * 
     * @param value 要记录的数值
     * @param timeMs POSIX时间戳(毫秒)
     * @throws QuotaViolationException 如果记录的值超出了配置的最大或最小边界值
     */
    public void record(double value, long timeMs) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, true);
        }
    }

    /**
     * 在指定时间点记录一个值，并可选择是否进行配额检查
     * 
     * @param value 要记录的数值
     * @param timeMs POSIX时间戳(毫秒)
     * @param checkQuotas 是否执行配额检查，true表示检查，false表示不检查
     * @throws QuotaViolationException 当checkQuotas为true且记录的值超出配置的边界值时抛出
     */
    public void record(double value, long timeMs, boolean checkQuotas) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, checkQuotas);
        }
    }

    /**
     * 内部记录方法，实现了值的实际记录逻辑
     * 
     * @param value 要记录的数值
     * @param timeMs 记录时间戳
     * @param checkQuotas 是否检查配额
     */
    private void recordInternal(double value, long timeMs, boolean checkQuotas) {
        // 更新最后记录时间
        this.lastRecordTime = timeMs;
        synchronized (this) {
            synchronized (metricLock()) {
                // 更新所有统计指标的值
                for (StatAndConfig statAndConfig : this.stats) {
                    statAndConfig.stat.record(statAndConfig.config(), value, timeMs);
                }
            }
            // 如果需要，执行配额检查
            if (checkQuotas)
                checkQuotas(timeMs);
        }
        // 将记录传播到所有父传感器
        for (Sensor parent : parents)
            parent.record(value, timeMs, checkQuotas);
    }

    /**
     * 检查所有配置了配额的度量指标是否违反了配额限制
     * 这是一个便捷方法，使用当前时间执行配额检查
     */
    public void checkQuotas() {
        checkQuotas(time.milliseconds());
    }

    /**
     * 在指定时间点检查所有度量指标的配额
     * 遍历所有度量指标，对配置了配额的指标进行检查：
     * 1. 对于TokenBucket类型的指标，检查值是否小于0
     * 2. 对于其他类型的指标，检查值是否在可接受范围内
     * 
     * @param timeMs 检查时的时间戳
     * @throws QuotaViolationException 当任何指标违反其配额限制时抛出
     */
    public void checkQuotas(long timeMs) {
        for (KafkaMetric metric : this.metrics.values()) {
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota();
                if (quota != null) {
                    double value = metric.measurableValue(timeMs);
                    if (metric.measurable() instanceof TokenBucket) {
                        // 令牌桶类型特殊处理：检查是否有可用令牌
                        if (value < 0) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    } else {
                        // 其他类型：检查值是否在配额限制范围内
                        if (!quota.acceptable(value)) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    }
                }
            }
        }
    }

    /**
     * 注册一个复合统计指标，使用传感器默认配置
     * 
     * @param stat 要注册的统计指标
     * @return 如果统计指标添加成功返回true，如果传感器已过期返回false
     */
    public boolean add(CompoundStat stat) {
        return add(stat, null);
    }

    /**
     * 注册一个复合统计指标，该指标可以产生多个可测量的量（如直方图）
     * 
     * @param stat 要注册的统计指标
     * @param config 该统计指标的配置。如果为null则使用传感器的默认配置
     * @return 如果统计指标添加成功返回true，如果传感器已过期返回false
     */
    public synchronized boolean add(CompoundStat stat, MetricConfig config) {
        if (hasExpired())
            return false;

        // 确定使用的配置：优先使用传入的配置，否则使用传感器默认配置
        final MetricConfig statConfig = config == null ? this.config : config;
        stats.add(new StatAndConfig(Objects.requireNonNull(stat), () -> statConfig));
        Object lock = metricLock();
        // 注册复合统计指标中的所有可测量指标
        for (NamedMeasurable m : stat.stats()) {
            final KafkaMetric metric = new KafkaMetric(lock, m.name(), m.stat(), statConfig, time);
            if (!metrics.containsKey(metric.metricName())) {
                KafkaMetric existingMetric = registry.registerMetric(metric);
                if (existingMetric != null) {
                    throw new IllegalArgumentException("A metric named '" + metric.metricName() + "' already exists, can't register another one.");
                }
                metrics.put(metric.metricName(), metric);
            }
        }
        return true;
    }

    /**
     * 注册一个简单的度量指标，使用传感器默认配置
     * 
     * @param metricName 度量指标的名称
     * @param stat 要维护的统计指标
     * @return 如果度量指标添加成功返回true，如果传感器已过期返回false
     */
    public boolean add(MetricName metricName, MeasurableStat stat) {
        return add(metricName, stat, null);
    }

    /**
     * Register a metric with this sensor
     *
     * @param metricName The name of the metric
     * @param stat       The statistic to keep
     * @param config     A special configuration for this metric. If null use the sensor default configuration.
     * @return true if metric is added to sensor, false if sensor is expired
     */
    public synchronized boolean add(final MetricName metricName, final MeasurableStat stat, final MetricConfig config) {
        if (hasExpired()) {
            return false;
        } else if (metrics.containsKey(metricName)) {
            return true;
        } else {
            final MetricConfig statConfig = config == null ? this.config : config;
            final KafkaMetric metric = new KafkaMetric(
                metricLock(),
                Objects.requireNonNull(metricName),
                Objects.requireNonNull(stat),
                statConfig,
                time
            );
            KafkaMetric existingMetric = registry.registerMetric(metric);
            if (existingMetric != null) {
                throw new IllegalArgumentException("A metric named '" + metricName + "' already exists, can't register another one.");
            }
            metrics.put(metric.metricName(), metric);
            stats.add(new StatAndConfig(Objects.requireNonNull(stat), metric::config));
            return true;
        }
    }

    /**
     * Return if metrics were registered with this sensor.
     *
     * @return true if metrics were registered, false otherwise
     */
    public synchronized boolean hasMetrics() {
        return !metrics.isEmpty();
    }

    /**
     * Return true if the Sensor is eligible for removal due to inactivity.
     *        false otherwise
     */
    public boolean hasExpired() {
        return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
    }

    synchronized List<KafkaMetric> metrics() {
        return List.copyOf(this.metrics.values());
    }

    /**
     * KafkaMetrics of sensors which use SampledStat should be synchronized on the same lock
     * for sensor record and metric value read to allow concurrent reads and updates. For simplicity,
     * all sensors are synchronized on this object.
     * <p>
     * Sensor object is not used as a lock for reading metric value since metrics reporter is
     * invoked while holding Sensor and Metrics locks to report addition and removal of metrics
     * and synchronized reporters may deadlock if Sensor lock is used for reading metrics values.
     * Note that Sensor object itself is used as a lock to protect the access to stats and metrics
     * while recording metric values, adding and deleting sensors.
     * </p><p>
     * Locking order (assume all MetricsReporter methods may be synchronized):
     * <ul>
     *   <li>Sensor#add: Sensor -> Metrics -> MetricsReporter</li>
     *   <li>Metrics#removeSensor: Sensor -> Metrics -> MetricsReporter</li>
     *   <li>KafkaMetric#metricValue: MetricsReporter -> Sensor#metricLock</li>
     *   <li>Sensor#record: Sensor -> Sensor#metricLock</li>
     * </ul>
     * </p>
     */
    private Object metricLock() {
        return metricLock;
    }
}
