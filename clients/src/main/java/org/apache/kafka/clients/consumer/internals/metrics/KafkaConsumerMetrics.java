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
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;

/**
 * Kafka 消费者指标类，用于收集和报告与消费者操作相关的各种指标。
 * <p>
 * 该类实现了 {@link AutoCloseable} 接口，以便在不再需要时可以正确关闭和清理资源，
 * 例如移除已注册的指标和传感器。
 * </p>
 * <p>
 * 主要功能包括：
 * <ul>
 *     <li>记录两次 poll() 调用之间的时间。</li>
 *     <li>记录 poll() 方法的空闲时间比例。</li>
 *     <li>记录同步提交 (commitSync) 和异步提交 (committed) 操作所花费的时间。</li>
 *     <li>提供一个指标，显示自上次 poll() 调用以来经过的秒数。</li>
 * </ul>
 * 这些指标对于监控消费者的健康状况、性能瓶颈以及与 Kafka 集群的交互效率至关重要。
 * 例如，通过观察 "time-between-poll" 指标，可以了解 poll 循环的频率，过高的值可能表示消费者处理消息的速度跟不上或者 poll() 调用间隔太长。
 * "poll-idle-ratio-avg" 指标则可以帮助判断消费者在 poll() 期间是忙于处理用户逻辑还是在等待新消息。
 * </p>
 */
public class KafkaConsumerMetrics implements AutoCloseable {
    /**
     * Metrics 实例，用于注册和管理所有指标。
     * 这是 Kafka 客户端库中用于收集和报告指标的核心组件。
     */
    private final Metrics metrics;
    /**
     * 上一次 poll() 调用指标的名称。
     * 用于唯一标识 "last-poll-seconds-ago" 指标。
     */
    private final MetricName lastPollMetricName;
    /**
     * 用于记录两次 poll() 调用之间时间的传感器。
     * Sensor 是 Kafka 指标系统中的一个概念，用于记录原始测量值，并可以附加一个或多个统计信息（如平均值、最大值）。
     */
    private final Sensor timeBetweenPollSensor;
    /**
     * 用于记录 poll() 方法空闲时间的传感器。
     * 这个传感器帮助计算消费者在 poll() 调用期间的空闲时间与等待用户代码处理记录的时间的比例。
     */
    private final Sensor pollIdleSensor;
    /**
     * 用于记录异步提交 (committed) 操作所花费时间的传感器。
     */
    private final Sensor committedSensor;
    /**
     * 用于记录同步提交 (commitSync) 操作所花费时间的传感器。
     */
    private final Sensor commitSyncSensor;
    /**
     * 上一次 poll() 调用的时间戳（毫秒）。
     * 用于计算两次 poll 调用之间的时间间隔以及自上次 poll 以来的时间。
     */
    private long lastPollMs;
    /**
     * 当前 poll() 调用开始的时间戳（毫秒）。
     * 用于计算当前 poll 调用的持续时间。
     */
    private long pollStartMs;
    /**
     * 自上次 poll() 调用以来经过的时间（毫秒）。
     * 这个值用于计算 poll 的空闲比例。
     */
    private long timeSinceLastPollMs;

    /**
     * KafkaConsumerMetrics 的构造函数。
     * <p>
     * 此构造函数负责初始化所有必要的指标和传感器。它会：
     * <ol>
     *     <li>基于提供的 {@code metricGrpPrefix} 和 {@code CONSUMER_METRICS_SUFFIX} 构建指标组名称。</li>
     *     <li>创建一个 {@code Measurable} 实例来计算自上次 poll() 调用以来的秒数，并将其注册为一个指标。
     *         这个指标 ("last-poll-seconds-ago") 对于监控消费者是否活跃非常有用。如果长时间没有 poll，可能意味着消费者卡顿或停止工作。</li>
     *     <li>初始化 {@code timeBetweenPollSensor}，并为其添加平均值和最大值指标，用于跟踪两次 poll() 调用之间的平均和最大延迟。
     *         这有助于识别 poll 循环中的潜在瓶颈或不规则性。</li>
     *     <li>初始化 {@code pollIdleSensor}，并为其添加平均值指标，用于跟踪消费者 poll() 方法的平均空闲时间比例。
     *         这个比例表示 poll() 方法在等待用户代码处理记录与实际空闲时间的对比，有助于了解消费者是CPU密集型还是IO密集型。</li>
     *     <li>初始化 {@code commitSyncSensor}，并为其添加累积和指标，用于跟踪消费者在同步提交 (commitSync) 中花费的总时间。</li>
     *     <li>初始化 {@code committedSensor}，并为其添加累积和指标，用于跟踪消费者在异步提交 (committed) 中花费的总时间。</li>
     * </ol>
     * 这些指标共同提供了一个关于消费者性能和行为的全面视图。
     * </p>
     * @param metrics Metrics 对象，用于注册和管理所有指标。
     * @param metricGrpPrefix 指标组的前缀，通常用于区分不同消费者的指标。
     */
    public KafkaConsumerMetrics(Metrics metrics, String metricGrpPrefix) {
        // 将传入的 Metrics 对象赋值给当前类的 metrics 字段
        this.metrics = metrics;
        // 构建指标组的名称，通过将传入的前缀和 CONSUMER_METRICS_SUFFIX（通常是 "-metrics"）连接起来
        final String metricGroupName = metricGrpPrefix + CONSUMER_METRICS_SUFFIX;
        // 定义一个 Measurable 对象，用于动态计算“距离上次poll调用过去了多少秒”这个指标的值
        Measurable lastPoll = (mConfig, now) -> {
            // 检查 lastPollMs 是否为 0，这表示 poll() 方法从未被调用过
            if (lastPollMs == 0L)
                // 如果 poll() 从未被触发，则返回 -1.0，表示无效或未初始化状态
                // if no poll is ever triggered, just return -1. // 如果 poll() 从未被触发，则返回 -1。
                return -1d;
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

        // 初始化 "commit-sync-time-ns-total" 传感器，用于记录同步提交所花费的总时间（纳秒）
        this.commitSyncSensor = metrics.sensor("commit-sync-time-ns-total");
        // 为 "commit-sync-time-ns-total" 传感器添加一个累积和指标
        this.commitSyncSensor.add(
            metrics.metricName(
                "commit-sync-time-ns-total", // 指标名称
                metricGroupName, // 指标组名称
                // "The total time the consumer has spent in commitSync in nanoseconds" // 消费者在 commitSync 中花费的总时间（纳秒）
                "消费者在 commitSync 中花费的总时间（纳秒）"
            ),
            new CumulativeSum() // 使用 CumulativeSum 统计类型来计算累积总和
        );

        // 初始化 "committed-time-ns-total" 传感器，用于记录异步提交（或广义上的提交操作）所花费的总时间（纳秒）
        this.committedSensor = metrics.sensor("committed-time-ns-total");
        // 为 "committed-time-ns-total" 传感器添加一个累积和指标
        this.committedSensor.add(
            metrics.metricName(
                "committed-time-ns-total", // 指标名称
                metricGroupName, // 指标组名称
                // "The total time the consumer has spent in committed in nanoseconds" // 消费者在 committed 中花费的总时间（纳秒）
                "消费者在 committed 中花费的总时间（纳秒）"
            ),
            new CumulativeSum() // 使用 CumulativeSum 统计类型来计算累积总和
        );
    }

    /**
     * 记录 poll() 方法开始调用的时间点。
     * <p>
     * 此方法在每次调用 {@code Consumer#poll(Duration)} 方法的开始时被调用。
     * 它会更新 {@code pollStartMs} 为当前 poll 的开始时间，
     * 计算自上次 poll 以来的时间 ({@code timeSinceLastPollMs})，
     * 并使用 {@code timeBetweenPollSensor} 记录这个时间间隔。
     * 最后，更新 {@code lastPollMs} 为当前 poll 的开始时间，供下次计算使用。
     * </p>
     * <p>
     * 应用场景：用于精确测量两次 poll 调用之间的间隔，以及 poll 操作本身的耗时。
     * 设计考虑：通过在 poll 开始时记录时间，可以更准确地分离出 poll 内部的耗时和 poll 之间的空闲/处理时间。
     * </p>
     * @param pollStartMs poll() 方法开始调用的时间戳（毫秒）。
     */
    public void recordPollStart(long pollStartMs) {
        // 将传入的 pollStartMs（当前 poll 开始的时间戳）赋值给成员变量 this.pollStartMs
        this.pollStartMs = pollStartMs;
        // 计算自上次 poll 调用到本次 poll 开始的时间差
        // 如果 lastPollMs 不为 0 (即之前有过 poll 调用)，则时间差为 pollStartMs - lastPollMs
        // 否则 (首次 poll)，时间差为 0
        this.timeSinceLastPollMs = lastPollMs != 0L ? pollStartMs - lastPollMs : 0;
        // 使用 timeBetweenPollSensor 记录本次计算出的 timeSinceLastPollMs 值
        // 这个传感器会收集所有 poll 之间的时间间隔，用于计算平均值、最大值等统计数据
        this.timeBetweenPollSensor.record(timeSinceLastPollMs);
        // 更新 lastPollMs 为当前 poll 的开始时间，为下一次 recordPollStart 调用做准备
        this.lastPollMs = pollStartMs;
    }

    /**
     * 记录 poll() 方法结束调用的时间点。
     * <p>
     * 此方法在每次调用 {@code Consumer#poll(Duration)} 方法的结束时被调用。
     * 它计算本次 poll() 调用的持续时间 ({@code pollTimeMs})，
     * 然后根据 poll 持续时间和自上次 poll 以来的时间 ({@code timeSinceLastPollMs}) 计算 poll 的空闲比例。
     * 这个空闲比例会被 {@code pollIdleSensor} 记录下来。
     * </p>
     * <p>
     * 应用场景：用于评估消费者在 poll 周期中的繁忙程度。高空闲比例可能意味着消费者处理能力有富余，或者没有足够的消息可供处理。
     * 设计考虑：空闲比例的计算方式 {@code pollTimeMs / (pollTimeMs + timeSinceLastPollMs)} 确保了当 {@code timeSinceLastPollMs} 很大（即两次 poll 间隔长）时，
     * 即使 {@code pollTimeMs} 较小，空闲比例也会较低，反之亦然。这反映了 poll 操作本身相对于整个 poll 周期的占比。
     * </p>
     * @param pollEndMs poll() 方法结束调用的时间戳（毫秒）。
     */
    public void recordPollEnd(long pollEndMs) {
        // 计算本次 poll 操作的持续时间，即 poll 结束时间减去 poll 开始时间
        long pollTimeMs = pollEndMs - pollStartMs;
        // 计算 poll 的空闲比例
        // pollIdleRatio = poll 持续时间 / (poll 持续时间 + 上次 poll 到本次 poll 开始的时间间隔)
        // 乘以 1.0 是为了确保进行浮点数除法
        // 这个比例表示 poll 方法本身执行时间占整个“两次 poll 间隔 + 本次 poll 执行时间”的比例。
        double pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeSinceLastPollMs);
        // 使用 pollIdleSensor 记录计算出的 pollIdleRatio 值
        // 这个传感器会收集 poll 空闲比例，用于计算平均值等统计数据
        this.pollIdleSensor.record(pollIdleRatio);
    }

    /**
     * 记录同步提交 (commitSync) 操作所花费的时间。
     * <p>
     * 此方法在消费者执行同步位移提交操作后被调用，用于记录该操作的耗时。
     * </p>
     * <p>
     * 应用场景：监控同步提交的性能。长时间的同步提交可能会阻塞消费者线程，影响整体吞吐量。
     * 设计考虑：直接记录耗时，由 {@code commitSyncSensor} (通常配置为 {@code CumulativeSum}) 进行累加，
     * 从而可以观察到总的同步提交耗时以及通过其他方式（如速率）间接了解平均耗时。
     * </p>
     * @param duration 同步提交操作所花费的时间（通常是纳秒）。
     */
    public void recordCommitSync(long duration) {
        // 使用 commitSyncSensor 记录同步提交操作的持续时间
        this.commitSyncSensor.record(duration);
    }

    /**
     * 记录异步提交 (committed) 操作所花费的时间。
     * <p>
     * 此方法在消费者执行异步位移提交操作后被调用（通常在回调中），用于记录该操作的耗时。
     * </p>
     * <p>
     * 应用场景：监控异步提交的性能。虽然异步提交不直接阻塞主处理流程，但其耗时仍然是衡量系统健康度的指标之一。
     * 设计考虑：与 {@code recordCommitSync} 类似，直接记录耗时，由 {@code committedSensor} 进行累加。
     * </p>
     * @param duration 异步提交操作所花费的时间（通常是纳秒）。
     */
    public void recordCommitted(long duration) {
        // 使用 committedSensor 记录异步提交操作的持续时间
        this.committedSensor.record(duration);
    }

    /**
     * 关闭 KafkaConsumerMetrics 实例并清理所有相关的指标和传感器。
     * <p>
     * 实现 {@link AutoCloseable#close()} 接口方法。当消费者关闭或不再需要指标收集时调用此方法。
     * 它会从 {@link Metrics} 实例中移除此类注册的所有指标和传感器，以释放资源并防止内存泄漏。
     * </p>
     * <p>
     * 应用场景：在消费者生命周期结束时，确保所有监控资源得到妥善清理。
     * 设计考虑：移除操作是幂等的，即使尝试移除不存在的指标或传感器也不会抛出异常。
     * 移除顺序并不重要。
     * </p>
     */
    @Override
    public void close() {
        // 从 Metrics 实例中移除 "last-poll-seconds-ago" 指标
        metrics.removeMetric(lastPollMetricName);
        // 从 Metrics 实例中移除 "time-between-poll" 传感器及其关联的所有指标
        metrics.removeSensor(timeBetweenPollSensor.name());
        // 从 Metrics 实例中移除 "poll-idle-ratio-avg" 传感器及其关联的所有指标
        metrics.removeSensor(pollIdleSensor.name());
        // 从 Metrics 实例中移除 "commit-sync-time-ns-total" 传感器及其关联的所有指标
        metrics.removeSensor(commitSyncSensor.name());
        // 从 Metrics 实例中移除 "committed-time-ns-total" 传感器及其关联的所有指标
        metrics.removeSensor(committedSensor.name());
    }
}
