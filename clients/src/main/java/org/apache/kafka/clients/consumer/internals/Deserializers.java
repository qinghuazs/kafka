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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 反序列化器管理类
 * 该类管理Kafka消费者的键值反序列化器，提供插件化的反序列化功能
 * 实现AutoCloseable接口以支持资源的自动关闭
 */
public class Deserializers<K, V> implements AutoCloseable {

    /**
     * 键反序列化器插件
     * 用于处理消息键的反序列化操作
     */
    private final Plugin<Deserializer<K>> keyDeserializerPlugin;

    /**
     * 值反序列化器插件
     * 用于处理消息值的反序列化操作
     */
    private final Plugin<Deserializer<V>> valueDeserializerPlugin;

    /**
     * 构造函数
     * 使用已存在的反序列化器实例初始化
     *
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param metrics 度量指标对象
     * @throws NullPointerException 如果任一反序列化器为null
     */
    public Deserializers(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Metrics metrics) {
        // 包装键反序列化器，确保非空
        this.keyDeserializerPlugin = Plugin.wrapInstance(
                Objects.requireNonNull(keyDeserializer, "Key deserializer provided to Deserializers should not be null"),
                metrics,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        // 包装值反序列化器，确保非空
        this.valueDeserializerPlugin = Plugin.wrapInstance(
                Objects.requireNonNull(valueDeserializer, "Value deserializer provided to Deserializers should not be null"),
                metrics,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }

    /**
     * 构造函数
     * 使用配置创建反序列化器实例
     *
     * @param config 消费者配置
     * @param keyDeserializer 可选的键反序列化器
     * @param valueDeserializer 可选的值反序列化器
     * @param metrics 度量指标对象
     */
    @SuppressWarnings("unchecked")
    public Deserializers(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Metrics metrics) {
        // 获取客户端ID
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        // 处理键反序列化器
        if (keyDeserializer == null) {
            // 如果未提供，从配置创建实例
            keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            // 配置反序列化器，传入客户端ID
            keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            // 如果已提供，忽略配置中的反序列化器设置
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }
        // 包装键反序列化器
        this.keyDeserializerPlugin = Plugin.wrapInstance(keyDeserializer, metrics, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);

        // 处理值反序列化器
        if (valueDeserializer == null) {
            // 如果未提供，从配置创建实例
            valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            // 配置反序列化器，传入客户端ID
            valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            // 如果已提供，忽略配置中的反序列化器设置
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }
        // 包装值反序列化器
        this.valueDeserializerPlugin = Plugin.wrapInstance(valueDeserializer, metrics, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }

    /**
     * 获取键反序列化器
     *
     * @return 键反序列化器实例
     */
    public Deserializer<K> keyDeserializer() {
        // 返回键反序列化器插件中的实例
        return keyDeserializerPlugin.get();
    }

    /**
     * 获取值反序列化器
     *
     * @return 值反序列化器实例
     */
    public Deserializer<V> valueDeserializer() {
        // 返回值反序列化器插件中的实例
        return valueDeserializerPlugin.get();
    }

    /**
     * 关闭反序列化器资源
     * 实现AutoCloseable接口的close方法
     *
     * @throws InterruptException 如果关闭过程被中断
     * @throws KafkaException 如果关闭过程发生其他异常
     */
    @Override
    public void close() {
        // 创建原子引用存储第一个发生的异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        // 安静地关闭键反序列化器
        Utils.closeQuietly(keyDeserializerPlugin, "key deserializer", firstException);
        // 安静地关闭值反序列化器
        Utils.closeQuietly(valueDeserializerPlugin, "value deserializer", firstException);
        // 获取可能发生的异常
        Throwable exception = firstException.get();

        // 处理异常
        if (exception != null) {
            if (exception instanceof InterruptException) {
                // 如果是中断异常，直接抛出
                throw (InterruptException) exception;
            }
            // 将其他异常包装为KafkaException并抛出
            throw new KafkaException("Failed to close deserializers", exception);
        }
    }

    @Override
    public String toString() {
        return "Deserializers{" +
                "keyDeserializer=" + keyDeserializerPlugin.get() +
                ", valueDeserializer=" + valueDeserializerPlugin.get() +
                '}';
    }
}
