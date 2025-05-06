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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * 后台事件处理器，负责从{@link ConsumerNetworkThread 网络线程}接收{@link BackgroundEvent 后台事件}，
 * 并通过{@link EventProcessor 事件处理器}将这些事件提供给应用线程。
 * 
 * 该处理器在Kafka消费者的异步处理模型中扮演着关键角色：
 * 1. 作为网络线程和应用线程之间的桥梁，实现了事件的异步传递
 * 2. 管理事件队列，确保事件按照接收顺序被处理
 * 3. 提供事件的计时和监控功能，有助于性能分析和问题诊断
 */

public class BackgroundEventHandler {

    /**
     * 存储后台事件的阻塞队列
     * 使用BlockingQueue实现线程安全的事件传递，确保网络线程和应用线程之间的正确通信
     */
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    /**
     * 时间工具类实例
     * 用于记录事件的时间戳，便于追踪事件处理的时间线
     */
    private final Time time;

    /**
     * 异步消费者指标收集器
     * 用于监控和记录事件队列的大小等指标，帮助分析系统性能
     */
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    /**
     * 创建一个新的后台事件处理器实例
     * 
     * @param backgroundEventQueue 用于存储后台事件的阻塞队列，确保线程安全的事件传递
     * @param time 时间工具类实例，用于记录事件时间戳
     * @param asyncConsumerMetrics 异步消费者指标收集器，用于性能监控
     */
    public BackgroundEventHandler(final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                  final Time time,
                                  final AsyncConsumerMetrics asyncConsumerMetrics) {
        this.backgroundEventQueue = backgroundEventQueue;
        this.time = time;
        this.asyncConsumerMetrics = asyncConsumerMetrics;
    }

    /**
     * 将一个后台事件添加到处理器中
     * 
     * @param event 由{@link ConsumerNetworkThread 网络线程}创建的{@link BackgroundEvent 后台事件}
     * 
     * 实现细节：
     * 1. 首先检查事件对象是否为null，确保事件有效性
     * 2. 记录事件入队时间戳，用于后续的时间追踪
     * 3. 更新队列大小指标，用于监控队列状态
     * 4. 将事件添加到阻塞队列中等待处理
     */
    public void add(BackgroundEvent event) {
        Objects.requireNonNull(event, "BackgroundEvent provided to add must be non-null");
        event.setEnqueuedMs(time.milliseconds());
        asyncConsumerMetrics.recordBackgroundEventQueueSize(backgroundEventQueue.size() + 1);
        backgroundEventQueue.add(event);
    }

    /**
     * 从处理器中排空所有后台事件
     * 
     * @return 包含所有被排空事件的列表
     * 
     * 实现细节：
     * 1. 创建一个新的ArrayList用于存储排空的事件
     * 2. 使用drainTo方法原子性地将队列中的所有事件转移到列表中
     * 3. 更新队列大小指标为0，表示队列已清空
     * 4. 返回包含所有排空事件的列表
     */
    public List<BackgroundEvent> drainEvents() {
        List<BackgroundEvent> events = new ArrayList<>();
        backgroundEventQueue.drainTo(events);
        asyncConsumerMetrics.recordBackgroundEventQueueSize(0);
        return events;
    }
}
