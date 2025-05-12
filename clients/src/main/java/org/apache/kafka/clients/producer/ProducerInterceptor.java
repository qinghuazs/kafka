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

import org.apache.kafka.common.Configurable;

/**
 * 生产者拦截器接口，允许在消息发送到Kafka集群之前拦截(并可能修改)生产者收到的记录。
 * 这个接口主要用于第三方组件对生产者应用进行自定义监控、日志记录等操作。
 * <p>
 * 该类通过configure()方法获取生产者配置属性，包括KafkaProducer分配的clientId(如果生产者配置中未指定)。
 * 拦截器实现需要注意它将与其他拦截器和序列化器共享生产者配置命名空间，并确保不存在冲突。
 * <p>
 * ProducerInterceptor方法抛出的异常将被捕获并记录日志，但不会进一步传播。因此，如果用户配置了错误的key和value类型参数，
 * 生产者不会抛出异常，只会记录错误。
 * <p>
 * ProducerInterceptor的回调可能会从多个线程调用。如果需要，拦截器实现必须确保线程安全。
 * <p>
 * 实现{@link org.apache.kafka.common.ClusterResourceListener}接口可以在集群元数据可用时接收通知。
 * 更多信息请参见ClusterResourceListener的类文档。
 * 实现{@link org.apache.kafka.common.metrics.Monitorable}接口可以使拦截器注册度量指标。
 * 所有注册的度量指标会自动添加以下标签：config设置为interceptor.classes，class设置为ProducerInterceptor类名。
 */
public interface ProducerInterceptor<K, V> extends Configurable, AutoCloseable {
    /**
     * 该方法在{@link org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord)}和
     * {@link org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord, Callback)}方法中调用，
     * 在key和value被序列化以及分区被分配之前(如果ProducerRecord中未指定分区)。
     * <p>
     * 该方法允许修改记录，这种情况下，将返回新的记录。修改key/value的含义是分区分配(如果ProducerRecord中未指定)
     * 将基于修改后的key/value进行，而不是客户端的原始key/value。因此，onSend()中进行的key和value转换需要保持一致：
     * 相同的key和value应该转换为相同的(修改后的)key和value。否则，日志压缩可能无法按预期工作。
     * <p>
     * 同样，拦截器实现需要确保在ProducerRecord中返回正确的主题/分区。
     * 通常情况下，应该与'record'中的主题/分区保持一致。
     * <p>
     * 该方法抛出的任何异常都将被调用者捕获并记录日志，但不会进一步传播。
     * <p>
     * 由于生产者可能运行多个拦截器，特定拦截器的onSend()回调将按照
     * {@link org.apache.kafka.clients.producer.ProducerConfig#INTERCEPTOR_CLASSES_CONFIG}中指定的顺序调用。
     * 列表中的第一个拦截器获取来自客户端的记录，后续拦截器将获取前一个拦截器返回的记录，依此类推。
     * 由于拦截器可以修改记录，因此拦截器可能会获取到已被其他拦截器修改的记录。
     * 但是不建议构建依赖于前一个拦截器输出的可变拦截器管道，因为拦截器可能无法修改记录并抛出异常，这可能会产生副作用。
     * 如果列表中的某个拦截器从onSend()抛出异常，异常将被捕获并记录日志，下一个拦截器将使用列表中最后一个成功的拦截器返回的记录，
     * 或者使用客户端的原始记录。
     *
     * 如果onSend返回null，生产者会直接忽略该消息。此时，消息不会被序列化、分配分区，也不会进入发送队列（RecordAccumulator），更不会被传输到Broker。
     * 相当于Kafka丢弃了该消息。
     * 如果存在多个拦截器，当某个拦截器返回null时，后续拦截器的onSend方法将不再执行，最终结果仍然是消息被丢弃。
     * @param record 来自客户端的记录或拦截器链中前一个拦截器返回的记录
     * @return 要发送到主题/分区的生产者记录
     */
    ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);

    /**
     * 当记录被服务器确认，或在记录发送到服务器之前发送失败时调用此方法。
     * <p>
     * 此方法通常在用户回调之前调用，以及在<code>KafkaProducer.send()</code>抛出异常的其他情况下调用。
     * <p>
     * 此方法抛出的任何异常都将被调用者忽略。
     * <p>
     * 此方法通常在后台I/O线程中执行，因此实现应该合理快速。
     * 否则，可能会延迟其他线程的消息发送。
     *
     * @param metadata 已发送记录的元数据(即分区和偏移量)。
     *                 如果发生错误，元数据将只包含有效的主题，可能还包含分区。
     *                 如果ProducerRecord中未给出分区，并且在分配分区之前发生错误，
     *                 则分区将设置为RecordMetadata.NO_PARTITION。
     *                 如果客户端向{@link org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord)}传递了null记录，
     *                 则元数据可能为null。
     * @param exception 处理此记录期间抛出的异常。如果没有发生错误则为null。
     */
    void onAcknowledgement(RecordMetadata metadata, Exception exception);

    /**
     * 当拦截器关闭时调用此方法
     */
    void close();
}
