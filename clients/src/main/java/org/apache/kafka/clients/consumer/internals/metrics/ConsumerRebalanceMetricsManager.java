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
// ... existing code ...

// 导入 Kafka 指标名称类
import org.apache.kafka.common.MetricName;
// 导入 Kafka 可度量指标接口
import org.apache.kafka.common.metrics.Measurable;
// 导入 Kafka 指标注册和管理类
import org.apache.kafka.common.metrics.Metrics;
// 导入 Kafka 指标传感器类，用于记录和聚合指标数据
import org.apache.kafka.common.metrics.Sensor;
// 导入 Kafka 平均值统计类
import org.apache.kafka.common.metrics.stats.Avg;
// 导入 Kafka 累积计数统计类
import org.apache.kafka.common.metrics.stats.CumulativeCount;
// 导入 Kafka 累积和统计类
import org.apache.kafka.common.metrics.stats.CumulativeSum;
// 导入 Kafka 最大值统计类
import org.apache.kafka.common.metrics.stats.Max;
// 导入 Kafka 速率统计类
import org.apache.kafka.common.metrics.stats.Rate;
// 导入 Kafka 窗口计数统计类
import org.apache.kafka.common.metrics.stats.WindowedCount;

// 导入 Java 时间单位枚举类
import java.util.concurrent.TimeUnit;

// 静态导入消费者工具类中的消费者指标组前缀常量
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
// 静态导入消费者工具类中的协调器指标后缀常量
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

/**
 * 消费者重平衡指标管理器，用于跟踪和记录与消费者重平衡相关的指标。
 * 继承自 {@link RebalanceMetricsManager}。
 * 这个类是 final 的，意味着它不能被继承。
 * 应用场景：在消费者客户端中，当发生分区重分配（重平衡）时，此类负责收集和报告相关的性能指标，
 * 例如重平衡的延迟、频率、成功次数和失败次数等。这些指标有助于监控消费者组的健康状况和性能。
 * 设计考虑：通过专门的类来管理重平衡指标，可以使代码结构更清晰，职责更明确。
 * 使用 {@link Sensor} 来记录原始数据，并通过不同的统计类型（如 {@link Avg}, {@link Max}, {@link CumulativeCount}）来计算和暴露指标。
 */
public final class ConsumerRebalanceMetricsManager extends RebalanceMetricsManager {
    /**
     * 用于记录成功重平衡事件的传感器。
     * Sensor 用于收集原始数据点，然后可以将这些数据点传递给一个或多个指标进行聚合。
     */
    private final Sensor successfulRebalanceSensor;
    /**
     * 用于记录失败重平衡事件的传感器。
     */
    private final Sensor failedRebalanceSensor;

    /**
     * 重平衡平均延迟指标名称。
     * 表示消费者组完成一次重平衡所花费的平均时间（毫秒）。
     */
    public final MetricName rebalanceLatencyAvg;
    /**
     * 重平衡最大延迟指标名称。
     * 表示消费者组完成一次重平衡所花费的最长时间（毫秒）。
     */
    public final MetricName rebalanceLatencyMax;
    /**
     * 重平衡总延迟指标名称。
     * 表示在重平衡上花费的总毫秒数。
     */
    public final MetricName rebalanceLatencyTotal;
    /**
     * 重平衡总次数指标名称。
     * 表示发生的重平衡事件的总数。
     */
    public final MetricName rebalanceTotal;
    /**
     * 每小时重平衡速率指标名称。
     * 表示每小时发生的重平衡事件的数量。
     */
    public final MetricName rebalanceRatePerHour;
    /**
     * 距离上次重平衡发生时间的秒数指标名称。
     */
    public final MetricName lastRebalanceSecondsAgo;
    /**
     * 失败重平衡总次数指标名称。
     * 表示失败的重平衡事件的总数。
     */
    public final MetricName failedRebalanceTotal;
    /**
     * 每小时失败重平衡速率指标名称。
     * 表示每小时失败的重平衡事件的数量。
     */
    public final MetricName failedRebalanceRate;
    /**
     * 上次重平衡结束的时间戳（毫秒）。
     * 初始化为 -1L，表示还没有发生过重平衡或上次重平衡未成功结束。
     */
    private long lastRebalanceEndMs = -1L;
    /**
     * 上次重平衡开始的时间戳（毫秒）。
     * 初始化为 -1L，表示还没有开始过重平衡。
     */
    private long lastRebalanceStartMs = -1L;

    /**
     * 构造函数，初始化消费者重平衡指标管理器。
     *
     * @param metrics Kafka 指标注册和管理对象，用于创建和注册指标。
     *                设计考虑：传入 Metrics 对象使得指标的创建和管理与 Kafka 的整体指标体系保持一致。
     */
    public ConsumerRebalanceMetricsManager(Metrics metrics) {
        // 调用父类 RebalanceMetricsManager 的构造函数，设置指标组名称。
        // 指标组名称由消费者指标组前缀和协调器指标后缀拼接而成，用于组织和区分相关的指标。
        super(CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX);

        // 创建重平衡平均延迟指标。
        // "rebalance-latency-avg" 是指标的名称。
        // "The average time in ms taken for a group to complete a rebalance" 是指标的描述，翻译为：“消费者组完成一次重平衡所花费的平均时间（毫秒）”。
        rebalanceLatencyAvg = createMetric(metrics, "rebalance-latency-avg",
                "消费者组完成一次重平衡所花费的平均时间（毫秒）");
        // 创建重平衡最大延迟指标。
        // "rebalance-latency-max" 是指标的名称。
        // "The max time in ms taken for a group to complete a rebalance" 是指标的描述，翻译为：“消费者组完成一次重平衡所花费的最长时间（毫秒）”。
        rebalanceLatencyMax = createMetric(metrics, "rebalance-latency-max",
                "消费者组完成一次重平衡所花费的最长时间（毫秒）");
        // 创建重平衡总延迟指标。
        // "rebalance-latency-total" 是指标的名称。
        // "The total number of milliseconds spent in rebalances" 是指标的描述，翻译为：“在重平衡上花费的总毫秒数”。
        rebalanceLatencyTotal = createMetric(metrics, "rebalance-latency-total",
                "在重平衡上花费的总毫秒数");
        // 创建重平衡总次数指标。
        // "rebalance-total" 是指标的名称。
        // "The total number of rebalance events" 是指标的描述，翻译为：“发生的重平衡事件的总数”。
        rebalanceTotal = createMetric(metrics, "rebalance-total",
                "发生的重平衡事件的总数");
        // 创建每小时重平衡速率指标。
        // "rebalance-rate-per-hour" 是指标的名称。
        // "The number of rebalance events per hour" 是指标的描述，翻译为：“每小时发生的重平衡事件的数量”。
        rebalanceRatePerHour = createMetric(metrics, "rebalance-rate-per-hour",
                "每小时发生的重平衡事件的数量");
        // 创建失败重平衡总次数指标。
        // "failed-rebalance-total" 是指标的名称。
        // "The total number of failed rebalance events" 是指标的描述，翻译为：“失败的重平衡事件的总数”。
        failedRebalanceTotal = createMetric(metrics, "failed-rebalance-total",
                "失败的重平衡事件的总数");
        // 创建每小时失败重平衡速率指标。
        // "failed-rebalance-rate-per-hour" 是指标的名称。
        // "The number of failed rebalance events per hour" 是指标的描述，翻译为：“每小时失败的重平衡事件的数量”。
        failedRebalanceRate = createMetric(metrics, "failed-rebalance-rate-per-hour",
                "每小时失败的重平衡事件的数量");

        // 获取或创建名为 "rebalance-latency" 的传感器，用于记录成功的重平衡延迟。
        // Sensor 是 Kafka 指标系统中的一个核心概念，它用于收集原始的测量值。
        successfulRebalanceSensor = metrics.sensor("rebalance-latency");
        // 将重平衡平均延迟指标 (rebalanceLatencyAvg) 添加到成功重平衡传感器，并使用 Avg 统计类型计算平均值。
        successfulRebalanceSensor.add(rebalanceLatencyAvg, new Avg());
        // 将重平衡最大延迟指标 (rebalanceLatencyMax) 添加到成功重平衡传感器，并使用 Max 统计类型计算最大值。
        successfulRebalanceSensor.add(rebalanceLatencyMax, new Max());
        // 将重平衡总延迟指标 (rebalanceLatencyTotal) 添加到成功重平衡传感器，并使用 CumulativeSum 统计类型计算累积和。
        successfulRebalanceSensor.add(rebalanceLatencyTotal, new CumulativeSum());
        // 将重平衡总次数指标 (rebalanceTotal) 添加到成功重平衡传感器，并使用 CumulativeCount 统计类型计算累积次数。
        successfulRebalanceSensor.add(rebalanceTotal, new CumulativeCount());
        // 将每小时重平衡速率指标 (rebalanceRatePerHour) 添加到成功重平衡传感器。
        // 使用 Rate 统计类型计算速率，时间单位为小时 (TimeUnit.HOURS)，并基于窗口计数 (WindowedCount)。
        successfulRebalanceSensor.add(rebalanceRatePerHour, new Rate(TimeUnit.HOURS, new WindowedCount()));

        // 获取或创建名为 "failed-rebalance" 的传感器，用于记录失败的重平衡事件。
        failedRebalanceSensor = metrics.sensor("failed-rebalance");
        // 将失败重平衡总次数指标 (failedRebalanceTotal) 添加到失败重平衡传感器，并使用 CumulativeSum 统计类型计算累积和。
        // 注意：这里用 CumulativeSum 记录失败次数，通常失败次数用 CumulativeCount 更直观，但这里可能是为了保持与成功重平衡总延迟的统计方式一致，或者表示每次失败事件的权重为1。
        failedRebalanceSensor.add(failedRebalanceTotal, new CumulativeSum());
        // 将每小时失败重平衡速率指标 (failedRebalanceRate) 添加到失败重平衡传感器。
        // 使用 Rate 统计类型计算速率，时间单位为小时 (TimeUnit.HOURS)，并基于窗口计数 (WindowedCount)。
        failedRebalanceSensor.add(failedRebalanceRate, new Rate(TimeUnit.HOURS, new WindowedCount()));

        // 定义一个可度量的对象 (Measurable)，用于计算距离上次重平衡结束的时间。
        // 这是一个 lambda 表达式，实现了 Measurable 接口的 measure 方法。
        Measurable lastRebalance = (config, now) -> {
            // 检查上次重平衡结束时间戳 (lastRebalanceEndMs) 是否为初始值 -1L。
            if (lastRebalanceEndMs == -1L)
                // 如果是初始值，表示还没有成功的重平衡，或者上次重平衡未结束，返回 -1.0。
                return -1d;
            // 否则，计算当前时间 (now) 与上次重平衡结束时间 (lastRebalanceEndMs) 的差值。
            else
                // 将时间差从毫秒转换为秒，并返回。
                return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
        };
        // 创建距离上次重平衡发生时间的秒数指标。
        // "last-rebalance-seconds-ago" 是指标的名称。
        // "The number of seconds since the last rebalance event" 是指标的描述，翻译为：“距离上次重平衡事件发生的秒数”。
        lastRebalanceSecondsAgo = createMetric(metrics,
                "last-rebalance-seconds-ago",
                "距离上次重平衡事件发生的秒数");
        // 将上面定义的 lastRebalance (Measurable 对象) 添加到指标注册表中，与 lastRebalanceSecondsAgo 指标名称关联。
        // 这样，当查询 lastRebalanceSecondsAgo 指标时，就会调用 lastRebalance 的 measure 方法来获取值。
        metrics.addMetric(lastRebalanceSecondsAgo, lastRebalance);
    }

    /**
     * 记录重平衡开始的事件。
     * 这个方法应该在消费者开始进行重平衡操作时调用。
     *
     * @param nowMs 当前时间戳（毫秒），用于标记重平衡开始的精确时间。
     *              设计考虑：传入当前时间戳而不是在方法内部获取，可以确保时间的一致性，特别是在分布式或多线程环境中。
     */
    public void recordRebalanceStarted(long nowMs) {
        // 将传入的当前时间戳赋值给 lastRebalanceStartMs 字段，记录重平衡的开始时间。
        lastRebalanceStartMs = nowMs;
    }

    /**
     * 记录重平衡成功结束的事件。
     * 这个方法应该在消费者成功完成重平衡操作后调用。
     *
     * @param nowMs 当前时间戳（毫秒），用于标记重平衡结束的精确时间。
     */
    public void recordRebalanceEnded(long nowMs) {
        // 将传入的当前时间戳赋值给 lastRebalanceEndMs 字段，记录重平衡的结束时间。
        lastRebalanceEndMs = nowMs;
        // 计算本次重平衡的持续时间（当前时间 - 开始时间）。
        // 然后，使用 successfulRebalanceSensor 记录这个持续时间。
        // 这个记录会触发与该传感器关联的各个指标（如平均延迟、最大延迟、总延迟、总次数、每小时速率）的更新。
        successfulRebalanceSensor.record(nowMs - lastRebalanceStartMs);
    }

    /**
     * 尝试记录一次失败的重平衡事件。
     * 这个方法用于在检测到重平衡可能失败时调用。
     * 设计考虑：方法命名为 "maybeRecord" 暗示了记录行为是条件性的。
     * 它会检查重平衡是否真的已经开始但尚未成功结束，以避免错误地记录失败。
     */
    public void maybeRecordRebalanceFailed() {
        // 检查重平衡是否已经成功结束或从未开始。
        // 如果重平衡开始时间 (lastRebalanceStartMs) 小于或等于结束时间 (lastRebalanceEndMs)，
        // 这意味着：
        // 1. 如果 lastRebalanceStartMs 和 lastRebalanceEndMs 都大于 -1L，且 lastRebalanceStartMs <= lastRebalanceEndMs，说明上一次重平衡已经成功结束。
        // 2. 如果 lastRebalanceStartMs 是 -1L (初始状态)，而 lastRebalanceEndMs 也是 -1L 或某个值，说明重平衡从未开始，或者开始后未正确记录结束。
        // 3. 如果 lastRebalanceStartMs > -1L 但 lastRebalanceEndMs 是 -1L，这表示重平衡已开始但未结束，此时不应执行 return。
        // 因此，这个条件主要用于判断重平衡是否已经明确地成功结束了，或者是否处于一个不应记录为失败的状态（例如，从未开始）。
        // 更准确地说，如果重平衡已经开始 (lastRebalanceStartMs > lastRebalanceEndMs, 且 lastRebalanceEndMs 可能是初始值 -1L, 或者上一次成功结束的时间)，那么就应该记录失败。
        // 所以，当 lastRebalanceStartMs <= lastRebalanceEndMs 时，表示重平衡要么没开始，要么已经成功结束了，此时不应该记录失败。
        if (lastRebalanceStartMs <= lastRebalanceEndMs)
            // 如果条件成立，则直接返回，不记录失败事件。
            return;
        // 如果重平衡已经开始 (lastRebalanceStartMs > lastRebalanceEndMs)，但尚未记录结束，则认为重平衡失败。
        // 使用 failedRebalanceSensor 记录一次失败事件。
        // 这会触发与该传感器关联的失败指标（如失败总次数、每小时失败速率）的更新。
        // 注意：这里只记录事件本身，没有记录失败的持续时间，因为失败可能没有明确的结束点，或者失败的原因导致无法准确测量持续时间。
        failedRebalanceSensor.record();
    }

    /**
     * 检查当前是否正在进行重平衡。
     * 应用场景：其他组件可能需要查询当前是否处于重平衡过程中，以便采取相应的行为，例如暂停某些操作。
     *
     * @return 如果重平衡已经开始但尚未结束，则返回 true；否则返回 false。
     */
    public boolean rebalanceStarted() {
        // 判断重平衡是否已经开始但尚未结束。
        // 如果上次重平衡开始的时间戳 (lastRebalanceStartMs) 大于上次重平衡结束的时间戳 (lastRebalanceEndMs)，
        // 则意味着一次新的重平衡已经启动，并且还没有记录其结束。
        // 初始状态下，lastRebalanceStartMs 和 lastRebalanceEndMs 都是 -1L，此时 -1L > -1L 为 false。
        // 当 recordRebalanceStarted 被调用后，lastRebalanceStartMs 会被更新为一个正值，此时 lastRebalanceStartMs > lastRebalanceEndMs (-1L) 为 true。
        // 当 recordRebalanceEnded 被调用后，lastRebalanceEndMs 会被更新，如果成功，则 lastRebalanceStartMs <= lastRebalanceEndMs，此时返回 false。
        return lastRebalanceStartMs > lastRebalanceEndMs;
    }
}