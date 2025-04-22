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


import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * 消费者拦截器接口
 * 
 * 这是一个插件接口，允许你拦截（并可能修改）消费者接收到的记录。主要用于让第三方组件
 * 接入消费者应用程序，实现自定义的监控、日志记录等功能。
 * 
 * <p>
 * 拦截器的主要特点和使用说明：
 * 1. 配置：通过<code>configure()</code>方法获取消费者配置属性，包括KafkaConsumer分配的clientId
 *    （如果消费者配置中未指定）。实现时需注意与其他拦截器和序列化器共享配置命名空间，避免冲突。
 * 
 * 2. 异常处理：拦截器方法抛出的异常会被捕获并记录日志，但不会向上传播。这意味着即使用户配置了
 *    错误的键值类型参数，消费者也不会抛出异常，只会记录错误日志。
 * 
 * 3. 线程安全：拦截器的回调方法与调用{@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)}
 *    的线程是同一个线程，确保了操作的线程安全性。
 * 
 * 4. 扩展功能：
 *    - 实现{@link org.apache.kafka.common.ClusterResourceListener}接口可以在集群元数据可用时收到通知
 *    - 实现{@link org.apache.kafka.common.metrics.Monitorable}接口可以注册度量指标
 *      所有注册的度量指标会自动添加以下标签：
 *      - <code>config</code>设置为<code>interceptor.classes</code>
 *      - <code>class</code>设置为ConsumerInterceptor类名
 */
public interface ConsumerInterceptor<K, V> extends Configurable, AutoCloseable {

    /**
     * 消息消费拦截方法
     * 
     * 此方法在{@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)}
     * 返回记录之前被调用。
     * 
     * 功能特点：
     * 1. 记录修改：可以修改消费者记录，修改后的记录将被返回给消费者
     * 2. 记录过滤：可以过滤掉不需要的记录
     * 3. 记录生成：可以生成新的记录，返回记录数量没有限制
     * 
     * 多拦截器处理机制：
     * 1. 执行顺序：按照{@link org.apache.kafka.clients.consumer.ConsumerConfig#INTERCEPTOR_CLASSES_CONFIG}
     *    中指定的顺序依次调用各个拦截器
     * 2. 数据流转：第一个拦截器获取原始记录，后续拦截器依次处理前一个拦截器返回的记录
     * 3. 异常处理：如果某个拦截器抛出异常，异常会被捕获并记录日志，然后使用最后一个成功的
     *    拦截器返回的记录（或原始记录）继续调用下一个拦截器
     * 
     * 注意事项：
     * 不建议构建依赖于前一个拦截器输出的可变拦截器管道，因为拦截器可能会修改失败并抛出异常，
     * 导致不可预期的副作用。
     *
     * @param records 待消费的记录或前一个拦截器返回的记录
     * @return 经过拦截器处理后的记录（可能是修改后的记录，也可能与输入记录相同）
     */
    ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records);

    /**
     * 偏移量提交拦截方法
     * 
     * 当消费者提交偏移量时调用此方法。可以用于：
     * 1. 监控偏移量提交情况
     * 2. 记录提交日志
     * 3. 执行自定义的偏移量处理逻辑
     * 
     * 注意：此方法抛出的任何异常都会被调用者忽略
     *
     * @param offsets 包含每个分区偏移量及其元数据的映射
     */
    void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * 拦截器关闭方法
     * 
     * 当拦截器被关闭时调用此方法，用于：
     * 1. 释放资源
     * 2. 关闭连接
     * 3. 执行清理工作
     */
    void close();
}
