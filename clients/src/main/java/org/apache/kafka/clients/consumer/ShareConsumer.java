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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Kafka共享消费者接口，提供了一种新的消费模式，允许多个消费者实例共享消费进度
 * 相比传统的Consumer接口，ShareConsumer具有以下特点：
 * 1. 支持消息级别的确认机制(acknowledge)，可以更细粒度地控制消息的消费状态
 * 2. 提供同步和异步的提交方式，并支持提交回调
 * 3. 集成了消费者度量指标管理，方便监控消费者状态
 * 4. 支持优雅关闭和唤醒机制
 *
 * 应用场景：
 * - 需要共享消费进度的分布式系统
 * - 要求消息处理可靠性的关键业务
 * - 需要细粒度控制消息确认的场景
 * 
 * @see KafkaShareConsumer 共享消费者的具体实现类
 * @see MockShareConsumer 用于测试的模拟实现
 */
@InterfaceStability.Evolving
public interface ShareConsumer<K, V> extends Closeable {

    /**
     * 获取当前消费者已订阅的主题集合
     * 此方法返回通过subscribe()方法订阅的主题列表
     * 
     * @return 已订阅的主题名称集合
     * @see KafkaShareConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * 订阅指定的主题集合
     * 订阅操作会触发重平衡，可能改变分区的分配
     * 
     * @param topics 要订阅的主题集合
     * @throws IllegalArgumentException 如果主题集合为null或包含null/空字符串
     * @see KafkaShareConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * 取消当前消费者的所有订阅
     * 这将触发一次重平衡，释放所有已分配的分区
     * 
     * @see KafkaShareConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * 从已订阅的主题中拉取消息记录
     * 如果在超时时间内没有可用的消息，将返回空记录集
     * 
     * @param timeout 等待新消息的最大时间
     * @return 消息记录集合，可能为空
     * @throws IllegalStateException 如果消费者未订阅任何主题
     * @see KafkaShareConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * 确认单条消息已被成功处理
     * 使用默认的确认类型进行消息确认
     * 
     * @param record 要确认的消息记录
     * @throws IllegalArgumentException 如果记录为null
     * @see KafkaShareConsumer#acknowledge(ConsumerRecord)
     */
    void acknowledge(ConsumerRecord<K, V> record);

    /**
     * 使用指定的确认类型确认消息
     * 支持不同级别的消息确认策略
     * 
     * @param record 要确认的消息记录
     * @param type 确认类型，定义了消息如何被确认
     * @throws IllegalArgumentException 如果记录为null或确认类型为null
     * @see KafkaShareConsumer#acknowledge(ConsumerRecord, AcknowledgeType)
     */
    void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type);

    /**
     * 同步提交已确认的消息偏移量
     * 此方法会阻塞直到提交完成或发生错误
     * 
     * @return 提交结果映射，键为主题分区，值为可能的异常
     * @see KafkaShareConsumer#commitSync()
     */
    Map<TopicIdPartition, Optional<KafkaException>> commitSync();

    /**
     * 在指定超时时间内同步提交已确认的消息偏移量
     * 
     * @param timeout 提交操作的超时时间
     * @return 提交结果映射，键为主题分区，值为可能的异常
     * @throws IllegalArgumentException 如果超时时间为负
     * @see KafkaShareConsumer#commitSync(Duration)
     */
    Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout);

    /**
     * 异步提交已确认的消息偏移量
     * 此方法立即返回，不等待提交完成
     * 
     * @see KafkaShareConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * 设置确认提交回调
     * 当异步提交完成时会调用此回调
     * 
     * @param callback 提交完成时要执行的回调函数
     * @see KafkaShareConsumer#setAcknowledgementCommitCallback(AcknowledgementCommitCallback)
     */
    void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback);

    /**
     * 获取消费者实例的唯一标识符
     * 
     * @param timeout 获取操作的超时时间
     * @return 消费者实例的UUID
     * @throws IllegalArgumentException 如果超时时间为负
     * @see KafkaShareConsumer#clientInstanceId(Duration)
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * 获取消费者的所有度量指标
     * 返回当前消费者实例的性能和状态指标
     * 
     * @return 度量指标映射，键为指标名称，值为指标对象
     * @see KafkaShareConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * 为订阅注册新的度量指标
     * 
     * @param metric 要注册的度量指标
     * @throws IllegalArgumentException 如果指标为null
     * @see KafkaShareConsumer#registerMetricForSubscription(KafkaMetric)
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * 取消订阅的度量指标注册
     * 
     * @param metric 要取消注册的度量指标
     * @throws IllegalArgumentException 如果指标为null
     * @see KafkaShareConsumer#unregisterMetricFromSubscription(KafkaMetric)
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * 关闭消费者
     * 释放所有资源，提交最后的偏移量
     * 
     * @see KafkaShareConsumer#close()
     */
    void close();

    /**
     * 在指定超时时间内关闭消费者
     * 
     * @param timeout 关闭操作的超时时间
     * @throws IllegalArgumentException 如果超时时间为负
     * @see KafkaShareConsumer#close(Duration)
     */
    void close(Duration timeout);

    /**
     * 从阻塞的轮询操作中唤醒消费者
     * 用于在其他线程中安全地中断消费者的轮询
     * 
     * @see KafkaShareConsumer#wakeup()
     */
    void wakeup();

}
