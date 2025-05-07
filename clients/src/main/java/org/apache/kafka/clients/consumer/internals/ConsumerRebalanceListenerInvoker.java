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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * 消费者重平衡监听器调用器
 * 该类封装了{@link ConsumerRebalanceListener}接口中定义的回调方法的调用。
 * 当消费者组分区分配发生变化时，这些方法会被调用。该类为这些回调调用提供了日志记录、
 * 可选的{@link Sensor}更新等包装功能。
 */
public class ConsumerRebalanceListenerInvoker {

    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 订阅状态管理器
     * 用于管理消费者的主题订阅和分区分配状态
     */
    private final SubscriptionState subscriptions;

    /**
     * 时间服务
     * 用于记录回调执行时间和延迟计算
     */
    private final Time time;

    /**
     * 重平衡回调度量管理器
     * 用于记录重平衡相关的度量指标
     */
    private final RebalanceCallbackMetricsManager metricsManager;

    /**
     * 构造函数
     *
     * @param logContext 日志上下文
     * @param subscriptions 订阅状态管理器
     * @param time 时间服务
     * @param metricsManager 重平衡回调度量管理器
     */
    ConsumerRebalanceListenerInvoker(LogContext logContext,
                                     SubscriptionState subscriptions,
                                     Time time,
                                     RebalanceCallbackMetricsManager metricsManager) {
        // 初始化日志记录器
        this.log = logContext.logger(getClass());
        // 初始化订阅状态管理器
        this.subscriptions = subscriptions;
        // 初始化时间服务
        this.time = time;
        // 初始化度量管理器
        this.metricsManager = metricsManager;
    }

    /**
     * 调用分区分配回调方法
     * 当新的分区被分配给消费者时调用
     *
     * @param assignedPartitions 新分配的分区集合
     * @return 如果回调执行过程中发生异常则返回该异常，否则返回null
     */
    public Exception invokePartitionsAssigned(final SortedSet<TopicPartition> assignedPartitions) {
        // 记录新分配的分区信息
        log.info("Adding newly assigned partitions: {}", assignedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        // 获取重平衡监听器
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();

        // 如果存在监听器，执行回调
        if (listener.isPresent()) {
            try {
                // 记录开始时间
                final long startMs = time.milliseconds();
                // 调用监听器的分区分配回调方法
                listener.get().onPartitionsAssigned(assignedPartitions);
                // 记录分区分配延迟
                metricsManager.recordPartitionsAssignedLatency(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                // 对于唤醒和中断异常直接抛出
                throw e;
            } catch (Exception e) {
                // 记录其他异常并返回
                log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                        listener.get().getClass().getName(), assignedPartitions, e);
                return e;
            }
        }

        return null;
    }

    /**
     * 调用分区撤销回调方法
     * 当分区从消费者撤销时调用
     *
     * @param revokedPartitions 被撤销的分区集合
     * @return 如果回调执行过程中发生异常则返回该异常，否则返回null
     */
    public Exception invokePartitionsRevoked(final SortedSet<TopicPartition> revokedPartitions) {
        // 记录被撤销的分区信息
        log.info("Revoke previously assigned partitions {}", revokedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
        // 获取被撤销的已暂停分区
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        // 保留被撤销的分区中的已暂停分区
        revokePausedPartitions.retainAll(revokedPartitions);
        // 如果存在被撤销的已暂停分区，记录日志
        if (!revokePausedPartitions.isEmpty())
                log.info("The pause flag in partitions [{}] will be removed due to revocation.", revokePausedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        // 获取重平衡监听器
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();

        // 如果存在监听器，执行回调
        if (listener.isPresent()) {
            try {
                // 记录开始时间
                final long startMs = time.milliseconds();
                // 调用监听器的分区撤销回调方法
                listener.get().onPartitionsRevoked(revokedPartitions);
                // 记录分区撤销延迟
                metricsManager.recordPartitionsRevokedLatency(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                // 对于唤醒和中断异常直接抛出
                throw e;
            } catch (Exception e) {
                // 记录其他异常并返回
                log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                        listener.get().getClass().getName(), revokedPartitions, e);
                return e;
            }
        }

        return null;
    }

    /**
     * 调用分区丢失回调方法
     * 当分区因为某些原因（如会话超时）丢失时调用
     *
     * @param lostPartitions 丢失的分区集合
     * @return 如果回调执行过程中发生异常则返回该异常，否则返回null
     */
    public Exception invokePartitionsLost(final SortedSet<TopicPartition> lostPartitions) {
        // 记录丢失的分区信息
        log.info("Lost previously assigned partitions {}", lostPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
        // 获取丢失的已暂停分区
        Set<TopicPartition> lostPausedPartitions = subscriptions.pausedPartitions();
        // 保留丢失的分区中的已暂停分区
        lostPausedPartitions.retainAll(lostPartitions);
        // 如果存在丢失的已暂停分区，记录日志
        if (!lostPausedPartitions.isEmpty())
            log.info("The pause flag in partitions [{}] will be removed due to partition lost.", lostPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        // 获取重平衡监听器
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();

        // 如果存在监听器，执行回调
        if (listener.isPresent()) {
            try {
                // 记录开始时间
                final long startMs = time.milliseconds();
                // 调用监听器的分区丢失回调方法
                listener.get().onPartitionsLost(lostPartitions);
                // 记录分区丢失延迟
                metricsManager.recordPartitionsLostLatency(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                // 对于唤醒和中断异常直接抛出
                throw e;
            } catch (Exception e) {
                // 记录其他异常并返回
                log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                        listener.get().getClass().getName(), lostPartitions, e);
                return e;
            }
        }

        return null;
    }
}
