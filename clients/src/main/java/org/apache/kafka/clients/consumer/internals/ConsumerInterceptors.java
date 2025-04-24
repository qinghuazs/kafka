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


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * 消费者拦截器容器类，用于管理和维护一组自定义的消费者拦截器。
 * 该类持有一个{@link org.apache.kafka.clients.consumer.ConsumerInterceptor}列表，
 * 并封装了对拦截器链的调用逻辑。
 * <p>
 * 拦截器链的主要作用：
 * 1. 在消息被返回给用户之前进行拦截处理
 * 2. 在偏移量提交成功后执行回调操作
 * 3. 支持优雅关闭所有拦截器
 */
public class ConsumerInterceptors<K, V> implements Closeable {
    // 日志记录器实例
    private static final Logger log = LoggerFactory.getLogger(ConsumerInterceptors.class);
    
    // 存储所有消费者拦截器的列表，使用Plugin包装以支持度量指标收集
    private final List<Plugin<ConsumerInterceptor<K, V>>> interceptorPlugins;

    /**
     * 构造函数，初始化拦截器容器
     * @param interceptors 用户配置的拦截器列表
     * @param metrics 度量指标收集器
     */
    public ConsumerInterceptors(List<ConsumerInterceptor<K, V>> interceptors, Metrics metrics) {
        // 将拦截器实例包装成Plugin对象，支持度量指标收集
        this.interceptorPlugins = Plugin.wrapInstances(interceptors, metrics, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

    /** 
     * 检查拦截器列表是否为空
     * @return 如果没有配置任何拦截器返回true，此时所有方法都将是空操作
     */
    public boolean isEmpty() {
        return interceptorPlugins.isEmpty();
    }

    /**
     * 在消息返回给用户之前调用拦截器链进行处理
     * <p>
     * 该方法会依次调用每个拦截器的{@link ConsumerInterceptor#onConsume(ConsumerRecords)}方法。
     * 每个拦截器处理完的结果会传递给链中的下一个拦截器继续处理。
     * <p>
     * 该方法采用容错设计，不会抛出异常。如果链中某个拦截器抛出异常：
     * 1. 异常会被捕获并记录日志
     * 2. 继续调用链中的下一个拦截器
     * 3. 使用上一个成功执行的拦截器返回的结果作为输入
     *
     * @param records 即将被客户端消费的消息记录
     * @return 经过拦截器链处理后的消息记录，可能是修改后的记录，也可能与输入的记录相同
     */
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        // 保存当前处理的消息记录，初始值为输入的records
        ConsumerRecords<K, V> interceptRecords = records;
        // 遍历所有拦截器插件
        for (Plugin<ConsumerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用当前拦截器的onConsume方法处理消息
                interceptRecords = interceptorPlugin.get().onConsume(interceptRecords);
            } catch (Exception e) {
                // 捕获异常但不传播，只记录日志并继续调用其他拦截器
                log.warn("Error executing interceptor onConsume callback", e);
            }
        }
        return interceptRecords;
    }

    /**
     * 当偏移量提交请求从broker成功返回时调用此方法
     * <p>
     * 该方法会依次调用每个拦截器的{@link ConsumerInterceptor#onCommit(Map)}方法，
     * 使拦截器能够在偏移量成功提交后执行自定义的处理逻辑。
     * <p>
     * 该方法采用容错设计，不会抛出异常。如果某个拦截器抛出异常：
     * 1. 异常会被捕获并记录日志
     * 2. 异常不会向上传播
     * 3. 继续执行链中的其他拦截器
     *
     * @param offsets 包含分区偏移量和相关元数据的映射
     */
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 遍历所有拦截器插件
        for (Plugin<ConsumerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用当前拦截器的onCommit方法
                interceptorPlugin.get().onCommit(offsets);
            } catch (Exception e) {
                // 捕获异常但不传播，只记录日志
                log.warn("Error executing interceptor onCommit callback", e);
            }
        }
    }

    /**
     * 关闭容器中的所有拦截器
     * <p>
     * 在消费者关闭时调用此方法，确保所有拦截器能够正常释放资源。
     * 如果某个拦截器关闭时发生异常，会记录错误日志但不影响其他拦截器的关闭。
     */
    @Override
    public void close() {
        // 遍历关闭所有拦截器插件
        for (Plugin<ConsumerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用插件的close方法关闭拦截器
                interceptorPlugin.close();
            } catch (Exception e) {
                // 记录关闭失败的错误日志
                log.error("Failed to close consumer interceptor ", e);
            }
        }
    }
}
