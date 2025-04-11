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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Kafka生产者的核心接口，定义了消息发送、事务操作和资源管理等基本功能。
 * 该接口有两个主要实现：
 * 1. KafkaProducer: 实际的生产者实现，用于向Kafka集群发送消息
 * 2. MockProducer: 用于测试的模拟实现
 * 
 * @param <K> 消息键的类型
 * @param <V> 消息值的类型
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer<K, V> extends Closeable {

    /**
     * 初始化事务型生产者。
     * 如果要使用事务功能，必须在发送任何消息之前调用此方法。
     * 该方法将：
     * 1. 确保事务协调器已准备就绪
     * 2. 获取生产者ID
     * 3. 恢复任何正在进行的事务（如果有）
     * 
     * @throws IllegalStateException 如果生产者已经初始化过事务
     * @throws UnsupportedVersionException 如果broker不支持事务
     * @throws AuthorizationException 如果生产者未被授权执行此操作
     * @see KafkaProducer#initTransactions()
     */
    void initTransactions();

    /**
     * 开始一个新的事务。
     * 在调用此方法后，所有的发送操作将被添加到事务中，直到调用commitTransaction()或abortTransaction()。
     * 
     * @throws ProducerFencedException 如果生产者被另一个具有相同transactionalId的生产者实例取代
     * @throws IllegalStateException 如果生产者未调用initTransactions()，或者已经有一个正在进行的事务
     * @see KafkaProducer#beginTransaction()
     */
    void beginTransaction() throws ProducerFencedException;

    /**
     * 在当前事务中提交指定的消费者组位移。
     * 这个方法通常用于实现消费-转换-生产模式的事务性，确保消息的消费和生产是原子的。
     * 
     * @param offsets 要提交的分区位移映射
     * @param groupMetadata 消费者组的元数据信息
     * @throws ProducerFencedException 如果生产者被另一个具有相同transactionalId的生产者实例取代
     * @throws IllegalStateException 如果没有正在进行的事务
     * @see KafkaProducer#sendOffsetsToTransaction(Map, ConsumerGroupMetadata)
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  ConsumerGroupMetadata groupMetadata) throws ProducerFencedException;

    /**
     * 提交当前事务。
     * 这将提交在beginTransaction()之后执行的所有发送操作和位移提交操作。
     * 
     * @throws ProducerFencedException 如果生产者被另一个具有相同transactionalId的生产者实例取代
     * @throws IllegalStateException 如果没有正在进行的事务
     * @see KafkaProducer#commitTransaction()
     */
    void commitTransaction() throws ProducerFencedException;

    /**
     * 中止当前事务。
     * 这将回滚在beginTransaction()之后执行的所有发送操作和位移提交操作。
     * 
     * @throws ProducerFencedException 如果生产者被另一个具有相同transactionalId的生产者实例取代
     * @throws IllegalStateException 如果没有正在进行的事务
     * @see KafkaProducer#abortTransaction()
     */
    void abortTransaction() throws ProducerFencedException;

    /**
     * 为生产者注册一个指标监控。
     * 这个方法用于监控生产者的性能和行为。
     * 
     * @param metric 要注册的Kafka指标对象
     * @see KafkaProducer#registerMetricForSubscription(KafkaMetric)
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * 取消注册之前添加的指标监控。
     * 
     * @param metric 要取消注册的Kafka指标对象
     * @see KafkaProducer#unregisterMetricFromSubscription(KafkaMetric)
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * 异步发送一条消息到指定的主题。
     * 发送是异步的，此方法将立即返回一个Future对象，可用于获取发送结果。
     * 
     * @param record 要发送的消息记录，包含主题、分区（可选）、时间戳（可选）、键（可选）和值
     * @return 一个Future对象，完成时可获取消息的元数据（主题、分区、位移等）
     * @throws SerializationException 如果无法序列化消息的键或值
     * @throws BufferExhaustedException 如果发送缓冲区已满
     * @see KafkaProducer#send(ProducerRecord)
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * 异步发送一条消息，并在发送完成时执行回调。
     * 回调函数将在消息被确认或发送失败时执行。
     * 
     * @param record 要发送的消息记录
     * @param callback 发送完成时要执行的回调函数
     * @return 一个Future对象，完成时可获取消息的元数据
     * @throws SerializationException 如果无法序列化消息的键或值
     * @throws BufferExhaustedException 如果发送缓冲区已满
     * @see KafkaProducer#send(ProducerRecord, Callback)
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * 刷新任何缓存的消息。
     * 此方法会阻塞，直到所有之前发送的消息都已完成发送（即已收到服务器确认）。
     * 
     * @see KafkaProducer#flush()
     */
    void flush();

    /**
     * 获取指定主题的分区信息。
     * 
     * @param topic 主题名称
     * @return 主题的分区信息列表，包含分区ID、leader副本、所有副本等信息
     * @throws AuthorizationException 如果生产者未被授权访问该主题
     * @see KafkaProducer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * 获取生产者的所有度量指标。
     * 返回的指标包括请求率、响应率、请求延迟、I/O等待时间等。
     * 
     * @return 度量指标的映射，键为指标名称，值为指标对象
     * @see KafkaProducer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * 获取此生产者实例的唯一标识符。
     * 此ID在生产者的整个生命周期内保持不变。
     * 
     * @param timeout 等待获取ID的超时时间
     * @return 生产者实例的唯一ID
     * @throws TimeoutException 如果在指定时间内未能获取到ID
     * @see KafkaProducer#clientInstanceId(Duration)}}
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * 关闭生产者，释放所有资源。
     * 此方法会阻塞等待所有发送中的请求完成。
     * 
     * @see KafkaProducer#close()
     */
    void close();

    /**
     * 在指定的超时时间内关闭生产者。
     * 
     * @param timeout 等待发送中请求完成的最大时间
     * @throws InterruptedException 如果线程在等待时被中断
     * @see KafkaProducer#close(Duration)
     */
    void close(Duration timeout);
}
