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
package org.apache.kafka.common;

/**
 * 一个回调接口，用户可以实现该接口以获取集群元数据变更的通知。
 * <p>
 * 在拦截器、指标报告器、序列化器和反序列化器中需要访问集群元数据的用户
 * 可以实现此接口。下面描述了这些不同类型组件的方法调用顺序。
 * <p>
 * <h4>客户端</h4>
 * 每次收到元数据响应后，都会调用一次{@link ClusterResourceListener#onUpdate(ClusterResource)}。
 * 注意：当Kafka broker版本低于0.10.1.0时，集群ID可能为null。如果收到null集群ID，
 * 除非集群中有多个broker版本（这种情况可能发生在集群升级过程中），否则它将一直为null。
 * <p>
 * {@link org.apache.kafka.clients.producer.ProducerInterceptor}：
 * {@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在
 * {@link org.apache.kafka.clients.producer.ProducerInterceptor#onSend(org.apache.kafka.clients.producer.ProducerRecord)}之后，
 * {@link org.apache.kafka.clients.producer.ProducerInterceptor#onAcknowledgement(org.apache.kafka.clients.producer.RecordMetadata, Exception)}之前调用。
 * <p>
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor}：
 * {@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor#onConsume(org.apache.kafka.clients.consumer.ConsumerRecords)}之前调用。
 * <p>
 * {@link org.apache.kafka.common.serialization.Serializer}：
 * {@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在
 * {@link org.apache.kafka.common.serialization.Serializer#serialize(String, Object)}之前调用。
 * <p>
 * {@link org.apache.kafka.common.serialization.Deserializer}：
 * {@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在
 * {@link org.apache.kafka.common.serialization.Deserializer#deserialize(String, byte[])}之前调用。
 * <p>
 * {@link org.apache.kafka.common.metrics.MetricsReporter}：
 * 对于生产者指标报告器，{@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在首次调用
 * {@link org.apache.kafka.clients.producer.KafkaProducer#send(org.apache.kafka.clients.producer.ProducerRecord)}后调用；
 * 对于消费者指标报告器，将在首次调用{@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)}后调用。
 * 报告器可能在此方法调用之前就收到来自网络层的指标事件。
 * <h4>Broker</h4>
 * 在broker启动时会调用一次{@link ClusterResourceListener#onUpdate(ClusterResource)}，之后集群元数据不会再改变。
 * <p>
 * KafkaMetricsReporter：{@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在Kafka broker启动过程中调用。
 * 报告器可能在此方法调用之前就收到来自网络层的指标事件。
 * <p>
 * {@link org.apache.kafka.common.metrics.MetricsReporter}：
 * {@link ClusterResourceListener#onUpdate(ClusterResource)}方法将在Kafka broker启动过程中调用。
 * 报告器可能在此方法调用之前就收到来自网络层的指标事件。
 */
public interface ClusterResourceListener {
    /**
     * A callback method that a user can implement to get updates for {@link ClusterResource}.
     * @param clusterResource cluster metadata
     */
    void onUpdate(ClusterResource clusterResource);
}
