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
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount; // 导入 WindowedCount 类，用于窗口计数统计

import java.util.concurrent.TimeUnit; // 导入 TimeUnit 类，用于时间单位转换

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX; // 静态导入消费者共享指标组前缀
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX; // 静态导入协调器指标后缀

/**
 * 共享消费者 Rebalance 指标管理器。
 * <p>
 * 该类负责管理与共享消费者 Rebalance 相关的指标，例如 Rebalance 的总次数、每小时 Rebalance 速率等。
 * 它继承自 {@link RebalanceMetricsManager}，并提供了针对共享消费场景的特定实现。
 * 应用场景：在 Kafka 共享消费模式下，用于监控和度量消费者组的 Rebalance 活动。
 * 设计考虑：通过继承 {@link RebalanceMetricsManager} 复用通用的 Rebalance 指标管理逻辑，同时针对共享消费的特点进行定制。
 * 使用 `final` 关键字确保该类不可被继承，保证其行为的确定性。
 */
public final class ShareRebalanceMetricsManager extends RebalanceMetricsManager {
    /**
     * Rebalance 传感器。
     * <p>
     * 用于记录 Rebalance 事件的原始数据，例如 Rebalance 的耗时。
     * Sensor 是 Kafka Metrics 库中的核心组件，用于收集和处理指标数据。
     */
    private final Sensor rebalanceSensor;
    /**
     * Rebalance 总次数指标名称。
     * <p>
     * 代表 Rebalance 事件发生的总次数。
     * {@link MetricName} 用于唯一标识一个指标。
     */
    public final MetricName rebalanceTotal;
    /**
     * 每小时 Rebalance 速率指标名称。
     * <p>
     * 代表平均每小时发生 Rebalance 事件的次数。
     * {@link MetricName} 用于唯一标识一个指标。
     */
    public final MetricName rebalanceRatePerHour;
    /**
     * 上一次 Rebalance 结束的时间戳（毫秒）。
     * <p>
     * 初始化为 -1，表示尚未发生过 Rebalance 或初始状态。
     * 用于计算两次 Rebalance 之间的时间间隔或判断 Rebalance 是否正在进行。
     */
    private long lastRebalanceEndMs = -1L;
    /**
     * 上一次 Rebalance 开始的时间戳（毫秒）。
     * <p>
     * 初始化为 -1，表示尚未发生过 Rebalance 或初始状态。
     * 用于计算 Rebalance 的持续时间。
     */
    private long lastRebalanceStartMs = -1L;

    /**
     * 构造函数。
     *
     * @param metrics Metrics 对象，用于创建和注册指标。
     *                设计考虑：通过构造函数注入 Metrics 对象，使得该管理器可以与 Kafka 的 Metrics 系统集成，
     *                方便统一管理和暴露指标数据。
     */
    public ShareRebalanceMetricsManager(Metrics metrics) {
        // 调用父类 RebalanceMetricsManager 的构造函数，并指定指标组名称
        // 指标组名称由 CONSUMER_SHARE_METRIC_GROUP_PREFIX（消费者共享指标组前缀）和 COORDINATOR_METRICS_SUFFIX（协调器指标后缀）拼接而成
        // 这样做有助于将共享消费者的 Rebalance 指标归类到特定的指标组下，方便监控和管理
        super(CONSUMER_SHARE_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX);

        // 创建 Rebalance 总次数指标
        // 使用父类的 createMetric 方法创建 MetricName 对象
        // 指标名称为 "rebalance-total"
        // 指标描述为 "The total number of rebalance events" (Rebalance 事件的总数)
        rebalanceTotal = createMetric(metrics, "rebalance-total",
                "Rebalance 事件的总数");
        // 创建每小时 Rebalance 速率指标
        // 使用父类的 createMetric 方法创建 MetricName 对象
        // 指标名称为 "rebalance-rate-per-hour"
        // 指标描述为 "The number of rebalance events per hour" (每小时 Rebalance 事件的数量)
        rebalanceRatePerHour = createMetric(metrics, "rebalance-rate-per-hour",
                "每小时 Rebalance 事件的数量");

        // 获取或创建名为 "rebalance-latency" 的传感器
        // Sensor 用于记录原始的 Rebalance 延迟数据
        rebalanceSensor = metrics.sensor("rebalance-latency");
        // 为 rebalanceSensor 添加一个指标，用于统计 Rebalance 的总次数
        // 使用 rebalanceTotal 这个 MetricName
        // 使用 CumulativeCount 类型的统计器，它会累积记录的次数
        rebalanceSensor.add(rebalanceTotal, new CumulativeCount());
        // 为 rebalanceSensor 添加另一个指标，用于计算每小时的 Rebalance 速率
        // 使用 rebalanceRatePerHour 这个 MetricName
        // 使用 Rate 类型的统计器，它会计算在指定时间窗口内事件发生的速率
        // TimeUnit.HOURS 指定时间单位为小时
        // WindowedCount 用于在时间窗口内计数
        rebalanceSensor.add(rebalanceRatePerHour, new Rate(TimeUnit.HOURS, new WindowedCount()));
    }

    /**
     * 记录 Rebalance 开始的事件。
     * <p>
     * 当消费者开始进行 Rebalance 操作时，应调用此方法。
     * 此方法会更新 {@link #lastRebalanceStartMs} 为当前时间戳。
     * 应用场景：在 Rebalance 流程的起始点调用，用于后续计算 Rebalance 耗时等指标。
     *
     * @param nowMs 当前时间戳（毫秒）。
     */
    @Override
    public void recordRebalanceStarted(long nowMs) {
        // 将上一次 Rebalance 开始的时间戳更新为当前时间戳
        lastRebalanceStartMs = nowMs;
    }

    /**
     * 记录 Rebalance 结束的事件。
     * <p>
     * 当消费者完成 Rebalance 操作时，应调用此方法。
     * 此方法会更新 {@link #lastRebalanceEndMs} 为当前时间戳，并记录 Rebalance 的耗时到 {@link #rebalanceSensor}。
     * Rebalance 的耗时是通过当前时间戳减去 {@link #lastRebalanceStartMs} 计算得到的。
     * 应用场景：在 Rebalance 流程的结束点调用，用于计算 Rebalance 耗时、更新成功次数等指标。
     *
     * @param nowMs 当前时间戳（毫秒）。
     */
    @Override
    public void recordRebalanceEnded(long nowMs) {
        // 将上一次 Rebalance 结束的时间戳更新为当前时间戳
        lastRebalanceEndMs = nowMs;
        // 计算 Rebalance 的持续时间（当前时间 - 开始时间）
        // 并将该持续时间记录到 rebalanceSensor 中
        // rebalanceSensor 会根据配置的统计器（CumulativeCount 和 Rate）更新相应的指标
        rebalanceSensor.record(nowMs - lastRebalanceStartMs);
    }

    /**
     * 检查 Rebalance 是否已经开始但尚未结束。
     * <p>
     * 通过比较 {@link #lastRebalanceStartMs} 和 {@link #lastRebalanceEndMs} 来判断。
     * 如果 {@link #lastRebalanceStartMs} 大于 {@link #lastRebalanceEndMs}，则表示 Rebalance 已经开始但尚未结束。
     * 应用场景：用于判断消费者是否正在进行 Rebalance，以便在某些操作前进行检查，
     * 例如，避免在 Rebalance 过程中提交位移。
     *
     * @return 如果 Rebalance 已开始且未结束，则返回 true；否则返回 false。
     */
    @Override
    public boolean rebalanceStarted() {
        // 如果上一次 Rebalance 开始的时间戳大于上一次 Rebalance 结束的时间戳，
        // 则说明 Rebalance 已经开始但尚未结束，返回 true
        // 否则，返回 false
        // 设计考虑：这种判断方式基于 Rebalance 开始和结束事件的记录顺序，
        // 确保了在 Rebalance 过程中，lastRebalanceStartMs 会先被更新，然后才是 lastRebalanceEndMs。
        return lastRebalanceStartMs > lastRebalanceEndMs;
    }
}
