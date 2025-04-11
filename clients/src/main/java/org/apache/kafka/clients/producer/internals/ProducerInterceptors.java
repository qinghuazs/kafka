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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * 生产者拦截器容器类，用于管理和调用自定义拦截器链。
 * 该类持有一个{@link org.apache.kafka.clients.producer.ProducerInterceptor}列表，
 * 并封装了对这些拦截器的链式调用。
 * 
 * 拦截器链的主要功能：
 * 1. 在消息发送前对消息进行拦截和处理（onSend）
 * 2. 在消息发送完成或失败时进行回调处理（onAcknowledgement）
 * 3. 在发送出错时进行错误处理（onSendError）
 * 
 * 拦截器链的执行特点：
 * 1. 链式调用：前一个拦截器的输出作为下一个拦截器的输入
 * 2. 异常隔离：单个拦截器的异常不会影响其他拦截器的执行
 * 3. 有序执行：按照配置顺序依次调用各个拦截器
 */
public class ProducerInterceptors<K, V> implements Closeable {
    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    // 拦截器插件列表，使用Plugin包装以支持度量指标收集
    private final List<Plugin<ProducerInterceptor<K, V>>> interceptorPlugins;

    /**
     * 构造函数，初始化拦截器容器
     * 
     * @param interceptors 拦截器列表
     * @param metrics 度量指标收集器，用于监控拦截器的性能和行为
     */
    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors, Metrics metrics) {
        // 使用Plugin.wrapInstances包装拦截器实例，支持度量指标收集
        this.interceptorPlugins = Plugin.wrapInstances(interceptors, metrics, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

    /**
     * This is called when client sends the record to KafkaProducer, before key and value gets serialized.
     * The method calls {@link ProducerInterceptor#onSend(ProducerRecord)} method. ProducerRecord
     * returned from the first interceptor's onSend() is passed to the second interceptor onSend(), and so on in the
     * interceptor chain. The record returned from the last interceptor is returned from this method.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * If an interceptor in the middle of the chain, that normally modifies the record, throws an exception,
     * the next interceptor in the chain will be called with a record returned by the previous interceptor that did not
     * throw an exception.
     *
     * @param record the record from client
     * @return producer record to send to topic/partition
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        // 初始化拦截记录，初始值为原始记录
        ProducerRecord<K, V> interceptRecord = record;
        // 遍历所有拦截器插件
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用当前拦截器的onSend方法，处理记录
                // 处理后的记录作为下一个拦截器的输入
                interceptRecord = interceptorPlugin.get().onSend(interceptRecord);
            } catch (Exception e) {
                // 捕获并记录异常，但不向上传播
                // 继续调用链中的其他拦截器
                if (record != null)
                    // 如果原始记录不为空，记录主题和分区信息
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(), record.partition(), e);
                else
                    // 原始记录为空时的异常日志
                    log.warn("Error executing interceptor onSend callback", e);
            }
        }
        // 返回经过所有拦截器处理后的记录
        return interceptRecord;
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
     * it gets sent to the server. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)}
     * method for each interceptor.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     *                 If an error occurred, metadata will only contain valid topic and maybe partition.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 遍历所有拦截器插件
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用拦截器的onAcknowledgement方法进行确认回调
                // 传入消息元数据和可能的异常信息
                interceptorPlugin.get().onAcknowledgement(metadata, exception);
            } catch (Exception e) {
                // 捕获并记录拦截器执行过程中的异常，但不向上传播
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * This method is called when sending the record fails in {@link ProducerInterceptor#onSend
     * (ProducerRecord)} method. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)}
     * method for each interceptor
     *
     * @param record The record from client
     * @param interceptTopicPartition  The topic/partition for the record if an error occurred
     *        after partition gets assigned; the topic part of interceptTopicPartition is the same as in record.
     * @param exception The exception thrown during processing of this record.
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        // 遍历所有拦截器插件
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                if (record == null && interceptTopicPartition == null) {
                    // 如果记录和主题分区都为空，直接调用onAcknowledgement，传入null元数据
                    interceptorPlugin.get().onAcknowledgement(null, exception);
                } else {
                    if (interceptTopicPartition == null) {
                        // 如果主题分区为空但记录不为空，从记录中提取主题分区信息
                        interceptTopicPartition = extractTopicPartition(record);
                    }
                    // 创建一个带有错误标记的元数据对象（使用-1表示无效值）
                    // 并调用拦截器的onAcknowledgement方法
                    interceptorPlugin.get().onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                                    RecordBatch.NO_TIMESTAMP, -1, -1), exception);
                }
            } catch (Exception e) {
                // 捕获并记录拦截器执行过程中的异常，但不向上传播
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * 从生产者记录中提取主题分区信息
     * 
     * @param record 生产者记录
     * @return 主题分区对象
     */
    public static <K, V> TopicPartition extractTopicPartition(ProducerRecord<K, V> record) {
        // 创建TopicPartition对象，如果分区为null则使用未知分区标记
        return new TopicPartition(record.topic(), record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
    }

    /**
     * Closes every interceptor in a container.
     */
    /**
     * 关闭所有拦截器
     * 实现Closeable接口，在容器关闭时调用
     */
    @Override
    public void close() {
        // 遍历并关闭所有拦截器插件
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用插件的close方法进行资源清理
                interceptorPlugin.close();
            } catch (Exception e) {
                // 记录关闭过程中的错误，但不中断其他拦截器的关闭
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
