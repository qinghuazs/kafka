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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;

/**
 * 该事件表示网络I/O线程需要调用{@link ConsumerRebalanceListener}上的回调方法之一。
 * 当用户执行下一次{@link Consumer#poll(Duration)}调用时，该事件将由应用线程处理。
 * 处理时，应用线程应根据{@link #methodName()}调用适当的回调方法，并传入给定的分区集合。
 *
 * 应用场景：
 * 1. 在消费者组再平衡过程中，当分区分配发生变化时触发
 * 2. 用于在网络I/O线程和应用线程之间进行事件通信
 * 3. 确保再平衡回调在正确的线程上下文中执行
 *
 * 设计考虑：
 * 1. 继承自CompletableBackgroundEvent，支持异步操作完成状态跟踪
 * 2. 使用不可变集合存储分区信息，保证线程安全
 * 3. 通过methodName字段区分不同类型的回调（如分区分配、撤销等）
 */
public class ConsumerRebalanceListenerCallbackNeededEvent extends CompletableBackgroundEvent<Void> {

    /**
     * 需要调用的ConsumerRebalanceListener回调方法名称
     * 可能是onPartitionsAssigned、onPartitionsRevoked或onPartitionsLost
     */
    private final ConsumerRebalanceListenerMethodName methodName;

    /**
     * 与回调方法相关的主题分区集合
     * 使用SortedSet保证分区顺序一致性，通过Collections.unmodifiableSortedSet确保不可变
     */
    private final SortedSet<TopicPartition> partitions;

    /**
     * 创建一个新的ConsumerRebalanceListenerCallbackNeededEvent实例
     *
     * 实现细节：
     * 1. 调用父类构造函数，设置事件类型和超时时间（使用Long.MAX_VALUE表示永不超时）
     * 2. 确保methodName参数不为null
     * 3. 创建分区集合的不可变副本
     *
     * @param methodName 要调用的回调方法名称，不能为null
     * @param partitions 相关的主题分区集合
     */
    public ConsumerRebalanceListenerCallbackNeededEvent(final ConsumerRebalanceListenerMethodName methodName,
                                                        final SortedSet<TopicPartition> partitions) {
        super(Type.CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED, Long.MAX_VALUE);
        this.methodName = Objects.requireNonNull(methodName);
        this.partitions = Collections.unmodifiableSortedSet(partitions);
    }

    /**
     * 获取需要调用的回调方法名称
     * @return 回调方法名称
     */
    public ConsumerRebalanceListenerMethodName methodName() {
        return methodName;
    }

    /**
     * 获取与回调相关的主题分区集合
     * @return 不可变的已排序主题分区集合
     */
    public SortedSet<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", methodName=" + methodName +
                ", partitions=" + partitions;
    }
}
