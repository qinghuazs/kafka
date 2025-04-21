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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Kafka消费者接口，定义了消费者的所有核心操作
 * 包括订阅主题、消费消息、提交偏移量、查询分区信息等功能
 * @see KafkaConsumer 具体实现类
 * @see MockConsumer 用于测试的模拟实现
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * 获取当前消费者被分配的所有分区
     * @see KafkaConsumer#assignment()
     */
    Set<TopicPartition> assignment();

    /**
     * 获取当前消费者订阅的所有主题
     * @see KafkaConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * 订阅指定的主题集合
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * 订阅指定的主题集合，并注册再均衡监听器
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * 手动指定要消费的分区
     * @see KafkaConsumer#assign(Collection)
     */
    void assign(Collection<TopicPartition> partitions);

    /**
     * 使用正则表达式订阅主题，并注册再均衡监听器
     * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * 使用正则表达式订阅主题
     * @see KafkaConsumer#subscribe(Pattern)
     */
    void subscribe(Pattern pattern);

    /**
     * 使用订阅模式订阅主题，并注册再均衡监听器
     * @see KafkaConsumer#subscribe(SubscriptionPattern, ConsumerRebalanceListener)
     */
    void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback);

    /**
     * 使用订阅模式订阅主题
     * @see KafkaConsumer#subscribe(SubscriptionPattern)
     */
    void subscribe(SubscriptionPattern pattern);

    /**
     * 取消所有订阅
     * @see KafkaConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * 拉取消息，如果在超时时间内没有新消息，则返回空记录
     * @see KafkaConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * 同步提交当前消费的所有分区的偏移量
     * @see KafkaConsumer#commitSync()
     */
    void commitSync();

    /**
     * 在指定超时时间内同步提交偏移量
     * @see KafkaConsumer#commitSync(Duration)
     */
    void commitSync(Duration timeout);

    /**
     * 同步提交指定分区的偏移量
     * @see KafkaConsumer#commitSync(Map)
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * 在指定超时时间内同步提交指定分区的偏移量
     * @see KafkaConsumer#commitSync(Map, Duration)
     */
    void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);

    /**
     * 异步提交当前消费的所有分区的偏移量
     * @see KafkaConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * 异步提交当前偏移量，并在完成时执行回调
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * 异步提交指定分区的偏移量，并在完成时执行回调
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * 为订阅注册度量指标
     * @see KafkaConsumer#registerMetricForSubscription(KafkaMetric)
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * 取消订阅的度量指标注册
     * @see KafkaConsumer#unregisterMetricFromSubscription(KafkaMetric)
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * 将消费者的位置重置到指定的偏移量
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    void seek(TopicPartition partition, long offset);

    /**
     * 将消费者的位置重置到指定的偏移量和元数据
     * @see KafkaConsumer#seek(TopicPartition, OffsetAndMetadata)
     */
    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * 将指定分区的消费位置重置到最早的偏移量
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * 将指定分区的消费位置重置到最新的偏移量
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * 获取指定分区的当前消费位置
     * @see KafkaConsumer#position(TopicPartition)
     */
    long position(TopicPartition partition);
    
    /**
     * 在指定超时时间内获取分区的当前消费位置
     * @see KafkaConsumer#position(TopicPartition, Duration)
     */
    long position(TopicPartition partition, final Duration timeout);

    /**
     * 获取指定分区已提交的偏移量
     * @see KafkaConsumer#committed(Set)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions);

    /**
     * 在指定超时时间内获取指定分区已提交的偏移量
     * @see KafkaConsumer#committed(Set, Duration)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, final Duration timeout);

    /**
     * 获取消费者实例ID
     * See {@link KafkaConsumer#clientInstanceId(Duration)}}
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * 获取消费者的所有度量指标
     * @see KafkaConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * 获取指定主题的所有分区信息
     * @see KafkaConsumer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * 在指定超时时间内获取主题的所有分区信息
     * @see KafkaConsumer#partitionsFor(String, Duration)
     */
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);

    /**
     * 列出所有可用的主题及其分区信息
     * @see KafkaConsumer#listTopics()
     */
    Map<String, List<PartitionInfo>> listTopics();

    /**
     * 在指定超时时间内列出所有可用的主题及其分区信息
     * @see KafkaConsumer#listTopics(Duration)
     */
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);

    /**
     * 获取当前已暂停的分区集合
     * @see KafkaConsumer#paused()
     */
    Set<TopicPartition> paused();

    /**
     * 暂停指定分区的消息消费
     * @see KafkaConsumer#pause(Collection)
     */
    void pause(Collection<TopicPartition> partitions);

    /**
     * 恢复指定分区的消息消费
     * @see KafkaConsumer#resume(Collection)
     */
    void resume(Collection<TopicPartition> partitions);

    /**
     * 查找大于等于指定时间戳的第一个记录的偏移量
     * @see KafkaConsumer#offsetsForTimes(Map)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * 在指定超时时间内查找大于等于指定时间戳的第一个记录的偏移量
     * @see KafkaConsumer#offsetsForTimes(Map, Duration)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

    /**
     * 获取指定分区的最早偏移量
     * @see KafkaConsumer#beginningOffsets(Collection)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * 在指定超时时间内获取指定分区的最早偏移量
     * @see KafkaConsumer#beginningOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * 获取指定分区的最新偏移量
     * @see KafkaConsumer#endOffsets(Collection)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * 在指定超时时间内获取指定分区的最新偏移量
     * @see KafkaConsumer#endOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * 获取指定分区的当前消费滞后量（消费者位置与最新偏移量之间的差距）
     * @see KafkaConsumer#currentLag(TopicPartition)
     */
    OptionalLong currentLag(TopicPartition topicPartition);

    /**
     * 获取消费者组的元数据信息
     * @see KafkaConsumer#groupMetadata()
     */
    ConsumerGroupMetadata groupMetadata();

    /**
     * 强制触发一次消费者组的再均衡
     * @see KafkaConsumer#enforceRebalance()
     */
    void enforceRebalance();

    /**
     * 强制触发一次消费者组的再均衡，并指定原因
     * @see KafkaConsumer#enforceRebalance(String)
     */
    void enforceRebalance(final String reason);

    /**
     * 关闭消费者
     * @see KafkaConsumer#close()
     */
    void close();

    /**
     * 在指定超时时间内关闭消费者
     * @see KafkaConsumer#close(Duration)
     */
    void close(Duration timeout);

    /**
     * 唤醒可能阻塞的消费者操作（比如poll方法）
     * @see KafkaConsumer#wakeup()
     */
    void wakeup();

}
