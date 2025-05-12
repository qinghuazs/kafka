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

/**
 * Rebalance 指标管理器抽象类。
 * <p>
 * 该类定义了管理消费者 Rebalance 过程中相关指标的基础接口和通用功能。
 * 子类需要实现具体的指标记录逻辑。
 * Rebalance 是指消费者组内分区分配发生变化的过程，例如新消费者加入、现有消费者离开或主题分区数量变更等。
 * 监控 Rebalance 相关的指标有助于了解 Rebalance 的频率、耗时以及可能存在的问题，从而优化消费者的性能和稳定性。
 */
public abstract class RebalanceMetricsManager {
    /**
     * 指标组名称。
     * <p>
     * 用于在 Metrics系统中唯一标识该组指标，方便查询和归类。
     * 例如，可以是 "consumer-fetch-manager-metrics" 或 "consumer-coordinator-metrics"。
     */
    protected final String metricGroupName;

    /**
     * 构造函数。
     *
     * @param metricGroupName 指标组名称，用于创建指标时指定其所属的组。
     *                        设计考虑：通过构造函数传入 metricGroupName，使得子类可以定义自己的指标组，
     *                        增强了灵活性和可扩展性。
     */
    RebalanceMetricsManager(String metricGroupName) {
        // 将传入的指标组名称赋值给成员变量 metricGroupName
        this.metricGroupName = metricGroupName;
    }

    /**
     * 创建一个指标名称对象 (MetricName)。
     * <p>
     * 此方法封装了 MetricName 的创建逻辑，确保所有由此管理器创建的指标都使用相同的指标组名称。
     * 应用场景：当子类需要定义新的指标时，可以调用此方法来创建 MetricName。
     *
     * @param metrics     Metrics 对象，用于创建 MetricName 的工厂类。
     * @param name        指标的名称，例如 "rebalance-latency-avg"。
     * @param description 指标的描述信息，例如 "The average time taken for a rebalance operation"。
     * @return 创建的 MetricName 对象。
     *         设计考虑：将 MetricName 的创建逻辑集中在此处，可以统一管理指标的命名规范和分组，
     *         便于后续的指标监控和分析。
     */
    protected MetricName createMetric(Metrics metrics, String name, String description) {
        // 调用 Metrics 对象的 metricName 方法创建 MetricName
        // 参数包括：指标名称 (name)，指标组名称 (metricGroupName)，以及指标描述 (description)
        return metrics.metricName(name, metricGroupName, description);
    }

    /**
     * 记录 Rebalance 开始的事件。
     * <p>
     * 当消费者开始进行 Rebalance 操作时，应调用此方法。
     * 子类需要实现此方法来记录 Rebalance 开始的时间戳或其他相关信息。
     * 应用场景：在 Rebalance 流程的起始点调用，用于后续计算 Rebalance 耗时等指标。
     *
     * @param nowMs 当前时间戳（毫秒）。
     */
    public abstract void recordRebalanceStarted(long nowMs);

    /**
     * 记录 Rebalance 结束的事件。
     * <p>
     * 当消费者完成 Rebalance 操作时，应调用此方法。
     * 子类需要实现此方法来记录 Rebalance 结束的时间戳，并可能计算和更新 Rebalance 相关的指标，如 Rebalance 耗时。
     * 应用场景：在 Rebalance 流程的结束点调用，用于计算 Rebalance 耗时、更新成功次数等指标。
     *
     * @param nowMs 当前时间戳（毫秒）。
     */
    public abstract void recordRebalanceEnded(long nowMs);

    /**
     * 可能会记录 Rebalance 失败的事件。
     * <p>
     * 默认实现为空操作。子类可以覆盖此方法，以在 Rebalance 失败时记录相关指标。
     * 应用场景：当 Rebalance 过程中发生异常或失败时调用，用于追踪 Rebalance 失败的频率和原因。
     * 设计考虑：提供一个可选的钩子方法，允许子类根据需要实现失败记录逻辑，而不需要强制所有实现都处理失败情况。
     */
    public void maybeRecordRebalanceFailed() {
        // 默认实现为空，子类可以根据需要覆盖此方法来记录 Rebalance 失败的指标。
        // 例如，可以增加一个计数器来统计 Rebalance 失败的次数。
    }

    /**
     * 检查 Rebalance 是否已经开始但尚未结束。
     * <p>
     * 子类需要实现此方法来判断当前是否处于 Rebalance 过程中。
     * 应用场景：用于判断消费者是否正在进行 Rebalance，以便在某些操作前进行检查，
     * 例如，避免在 Rebalance 过程中提交位移。
     *
     * @return 如果 Rebalance 已开始且未结束，则返回 true；否则返回 false。
     */
    public abstract boolean rebalanceStarted();
}