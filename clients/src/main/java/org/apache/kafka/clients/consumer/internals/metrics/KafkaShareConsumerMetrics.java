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
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;

/**
 * 用于 Kafka 共享消费者的指标收集类。
 * <p>
 * 它实现了 {@link AutoCloseable} 接口，以便在消费者关闭时可以清理和移除相关的指标。
 * 这个类主要用于跟踪与 {@code poll()} 方法相关的性能数据，例如两次 {@code poll()} 之间的间隔时间以及 {@code poll()} 方法的空闲时间比例。
 * 这些指标对于理解共享消费者的行为和性能至关重要，尤其是在多个消费者实例共享连接和资源的场景下。
 * </p>
 */
public class KafkaShareConsumerMetrics implements AutoCloseable {
    /**
     * Metrics 实例，用于注册和管理所有指标。
     * 这是 Kafka 客户端库中用于收集和报告指标的核心组件。
     */
    private final Metrics metrics;
    /**
     * 上一次 {@code poll()} 调用指标的名称。
     * 用于唯一标识 "last-poll-seconds-ago" 指标。
     */
    private final MetricName lastPollMetricName;
    /**
     * 用于记录两次 {@code poll()} 调用之间时间的传感器。
     * Sensor 是 Kafka 指标系统中的一个概念，用于记录原始测量值，并可以附加一个或多个统计信息（如平均值、最大值）。
     */
    private final Sensor timeBetweenPollSensor;
    /**
     * 用于记录 {@code poll()} 方法空闲时间的传感器。
     * 这个传感器帮助计算消费者在 {@code poll()} 调用期间的空闲时间与等待用户代码处理记录的时间的比例。
     */
    private final Sensor pollIdleSensor;
    /**
     * 上一次 {@code poll()} 调用的时间戳（毫秒）。
     * 用于计算两次 {@code poll} 调用之间的时间间隔以及自上次 {@code poll} 以来的时间。
     */
    private long lastPollMs;
    /**
     * 当前 {@code poll()} 调用开始的时间戳（毫秒）。
     * 用于计算当前 {@code poll} 调用的持续时间。
     */
    private long pollStartMs;
    /**
     * 自上次 {@code poll()} 调用以来经过的时间（毫秒）。
     * 这个值用于计算 {@code poll} 的空闲比例。
     */
    private long timeSinceLastPollMs;

    /**
     * KafkaShareConsumerMetrics 的构造函数。
     * <p>
     * 此构造函数负责初始化所有必要的指标和传感器，这些指标专用于共享消费者场景。它会：
     * <ol>
     *     <li>基于提供的 {@code metricGrpPrefix} 和 {@code CONSUMER_METRICS_SUFFIX} 构建指标组名称。</li>
     *     <li>创建一个 {@link org.apache.kafka.common.metrics.Measurable} 实例来计算自上次 {@code poll()} 调用以来的秒数，并将其注册为一个指标 ('last-poll-seconds-ago')。这个指标对于监控共享消费者是否活跃非常有用。</li>
     *     <li>初始化 {@code timeBetweenPollSensor}，并为其添加平均值和最大值指标，用于跟踪两次 {@code poll()} 调用之间的平均和最大延迟。这有助于识别共享消费者 {@code poll} 循环中的潜在瓶颈。</li>
     *     <li>初始化 {@code pollIdleSensor}，并为其添加平均值指标，用于跟踪共享消费者 {@code poll()} 方法的平均空闲时间比例。这个比例表示 {@code poll()} 方法在等待用户代码处理记录与实际空闲时间的对比。</li>
     * </ol>
     * 这些指标共同提供了一个关于共享消费者性能和行为的视图。
     * </p>
     * @param metrics Metrics 对象，用于注册和管理所有指标。
     * @param metricGrpPrefix 指标组的前缀，通常用于区分不同消费者或消费者组的指标。
     */
    public KafkaShareConsumerMetrics(Metrics metrics, String metricGrpPrefix) {
        // 将传入的 Metrics 对象赋值给当前类的 metrics 字段
        this.metrics = metrics;
        // 构建指标组的名称，通过将传入的前缀和 CONSUMER_METRICS_SUFFIX（通常是 "-metrics"）连接起来
        final String metricGroupName = metricGrpPrefix + CONSUMER_METRICS_SUFFIX;
        // 定义一个 Measurable 对象，用于动态计算“距离上次poll调用过去了多少秒”这个指标的值
        Measurable lastPoll = (mConfig, now) -> {
            // 检查 lastPollMs 是否为 0，这表示 poll() 方法从未被调用过
            if (lastPollMs == 0L)
                // 如果 poll() 从未被触发，则返回 -1。
                return -1d; // 如果 poll() 从未被触发，则返回 -1.0，表示无效或未初始化状态
            else
                // 如果 poll() 至少被调用过一次，则计算当前时间 'now' 与上次 poll 时间 'lastPollMs' 之间的差值，并将其从毫秒转换为秒
                return TimeUnit.SECONDS.convert(now - lastPollMs, TimeUnit.MILLISECONDS);
        };
        // 创建并注册 "last-poll-seconds-ago" 指标
        // metrics.metricName() 用于创建一个 MetricName 对象，它包含了指标的名称、组名和描述
        // "The number of seconds since the last poll() invocation." // 自上次 poll() 调用以来的秒数。
        this.lastPollMetricName = metrics.metricName("last-poll-seconds-ago",
                metricGroupName, "自上次 poll() 调用以来的秒数。");
        // 将上面创建的 MetricName 和 Measurable 对象添加到 Metrics 实例中，使其成为一个可监控的指标
        metrics.addMetric(lastPollMetricName, lastPoll);

        // 初始化 "time-between-poll" 传感器，用于记录两次 poll() 调用之间的时间间隔
        this.timeBetweenPollSensor = metrics.sensor("time-between-poll");
        // 为 "time-between-poll" 传感器添加一个平均值指标
        // "The average delay between invocations of poll() in milliseconds." // poll() 调用之间的平均延迟（毫秒）。
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-avg",
                        metricGroupName,
                        "poll() 调用之间的平均延迟（毫秒）。"),
                new Avg()); // 使用 Avg 统计类型来计算平均值
        // 为 "time-between-poll" 传感器添加一个最大值指标
        // "The max delay between invocations of poll() in milliseconds." // poll() 调用之间的最大延迟（毫秒）。
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-max",
                        metricGroupName,
                        "poll() 调用之间的最大延迟（毫秒）。"),
                new Max()); // 使用 Max 统计类型来计算最大值

        // 初始化 "poll-idle-ratio-avg" 传感器，用于记录 poll() 方法的空闲时间比例
        this.pollIdleSensor = metrics.sensor("poll-idle-ratio-avg");
        // 为 "poll-idle-ratio-avg" 传感器添加一个平均值指标
        // "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records." // 消费者 poll() 方法空闲时间与等待用户代码处理记录时间的平均比例。
        this.pollIdleSensor.add(metrics.metricName("poll-idle-ratio-avg",
                        metricGroupName,
                        "消费者 poll() 方法空闲时间与等待用户代码处理记录时间的平均比例。"),
                new Avg()); // 使用 Avg 统计类型来计算平均值
    }

    /**
     * 记录 {@code poll()} 方法开始调用的时间点。
     * <p>
     * 此方法在每次调用 {@code Consumer#poll(Duration)} 方法的开始时被调用。
     * 它会更新 {@code pollStartMs} 为当前 {@code poll} 的开始时间，
     * 计算自上次 {@code poll} 以来的时间 ({@code timeSinceLastPollMs})，
     * 并使用 {@code timeBetweenPollSensor} 记录这个时间间隔。
     * 最后，更新 {@code lastPollMs} 为当前 {@code poll} 的开始时间，供下次计算使用。
     * </p>
     * <p>
     * 应用场景：用于精确测量两次 {@code poll} 调用之间的间隔，以及 {@code poll} 操作本身的耗时。
     * 设计考虑：通过在 {@code poll} 开始时记录时间，可以确保时间戳的准确性，并为后续计算 {@code poll} 耗时和空闲比例提供基准。
     * </p>
     * @param pollStartMs 当前 {@code poll()} 调用开始的时间戳（毫秒）。
     */
    public void recordPollStart(long pollStartMs) {
        // 将传入的 pollStartMs（当前 poll 开始时间）赋值给成员变量 pollStartMs
        this.pollStartMs = pollStartMs;
        // 计算自上次 poll 调用到当前 poll 开始的时间差。如果 lastPollMs 为0（即第一次 poll），则时间差为0
        this.timeSinceLastPollMs = lastPollMs != 0L ? pollStartMs - lastPollMs : 0;
        // 使用 timeBetweenPollSensor 记录两次 poll 调用之间的时间间隔 (timeSinceLastPollMs)
        this.timeBetweenPollSensor.record(timeSinceLastPollMs);
        // 更新 lastPollMs 为当前 poll 的开始时间，为下一次计算 poll 间隔做准备
        this.lastPollMs = pollStartMs;
    }

    /**
     * 记录 {@code poll()} 方法结束调用的时间点，并计算相关的指标。
     * <p>
     * 此方法在 {@code Consumer#poll(Duration)} 方法即将返回时被调用。
     * 它会计算本次 {@code poll} 操作的持续时间 ({@code pollTimeMs})，
     * 然后基于 {@code pollTimeMs} 和 {@code timeSinceLastPollMs}（两次 {@code poll} 之间的空闲时间）计算 {@code poll} 的空闲比例。
     * 最后，使用 {@code pollIdleSensor} 记录这个空闲比例。
     * </p>
     * <p>
     * 应用场景：用于评估消费者在 {@code poll} 周期中的繁忙程度。高空闲比例可能表示消息处理速度快或消息量少，低空闲比例可能表示消息处理耗时长或消息积压。
     * 设计考虑：空闲比例的计算方式 {@code pollTimeMs / (pollTimeMs + timeSinceLastPollMs)} 确保了分母始终大于0（除非 {@code pollTimeMs} 和 {@code timeSinceLastPollMs} 都是0，这在实际中不太可能），避免了除零错误。
     * </p>
     * @param pollEndMs 当前 {@code poll()} 调用结束的时间戳（毫秒）。
     */
    public void recordPollEnd(long pollEndMs) {
        // 计算当前 poll 调用的总耗时，即 poll 结束时间减去 poll 开始时间
        long pollTimeMs = pollEndMs - pollStartMs;
        // 计算 poll 的空闲时间比例。公式为：(poll调用耗时) / (poll调用耗时 + 两次poll之间的间隔时间)。乘以1.0是为了确保浮点数除法。
        // 如果 pollTimeMs + timeSinceLastPollMs 为0 (例如第一次poll且poll瞬间完成)，则比例为0，避免NaN
        double pollIdleRatio = (pollTimeMs + timeSinceLastPollMs == 0) ? 0.0 : pollTimeMs * 1.0 / (pollTimeMs + timeSinceLastPollMs);
        // 使用 pollIdleSensor 记录计算出的 poll 空闲比例
        this.pollIdleSensor.record(pollIdleRatio);
    }

    /**
     * 关闭 KafkaShareConsumerMetrics 实例并清理所有相关的指标和传感器。
     * <p>
     * 此方法实现了 {@link AutoCloseable#close()} 接口，通常在消费者关闭时调用。
     * 它会从 {@link org.apache.kafka.common.metrics.Metrics} 实例中移除此类注册的所有指标和传感器，以释放资源并防止内存泄漏。
     * </p>
     * <p>
     * 应用场景：在消费者生命周期结束时，确保所有监控资源得到妥善清理。
     * 设计考虑：移除指标和传感器是必要的，以避免在 Metrics 实例中保留不再使用的对象，特别是在消费者实例频繁创建和销毁的场景中。
     * </p>
     */
    @Override
    public void close() {
        // 从 Metrics 实例中移除 "last-poll-seconds-ago" 指标
        metrics.removeMetric(lastPollMetricName);
        // 从 Metrics 实例中移除 "time-between-poll" 传感器及其关联的指标
        metrics.removeSensor(timeBetweenPollSensor.name());
        // 从 Metrics 实例中移除 "poll-idle-ratio-avg" 传感器及其关联的指标
        metrics.removeSensor(pollIdleSensor.name());
    }
}
