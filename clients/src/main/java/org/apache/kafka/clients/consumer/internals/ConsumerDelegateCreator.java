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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Locale;

/**
 * ConsumerDelegateCreator实现了一个准工厂模式，使调用者无需了解所创建的底层{@link Consumer}实现
 * 这提供了一种机制，使{@link KafkaConsumer}可以保持作为实现的顶层外观，同时允许不同的实现在底层共存
 *
 * <p/>
 *
 * 当前的{@code ConsumerCreator}逻辑检查传入的配置，并确定是使用新的消费者组协议(KIP-848)
 * 还是回退到现有的传统组协议。这基于{@link ConsumerConfig#GROUP_PROTOCOL_CONFIG group.protocol}
 * 配置的存在和值。如果该值存在且等于&quot;{@code consumer}&quot;，将返回{@link AsyncKafkaConsumer}
 * 否则，将返回{@link ClassicKafkaConsumer}
 *
 * <p/>
 *
 * <em>注意</em>：这仅供内部使用，不适用于最终用户。内部用户不应尝试确定底层实现
 * 以避免依赖不稳定的接口。相反，应该使用{@link Consumer} API契约作为调用者的接口
 */
public class ConsumerDelegateCreator {

    /**
     * 创建消费者委托的简化版本
     * 仅使用基本配置和序列化器创建消费者
     *
     * @param config 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @return 创建的消费者委托实例
     * @throws KafkaException 如果创建消费者失败
     */
    public <K, V> ConsumerDelegate<K, V> create(ConsumerConfig config,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer) {
        try {
            // 从配置中获取组协议类型并转换为大写
            GroupProtocol groupProtocol = GroupProtocol.valueOf(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG).toUpperCase(Locale.ROOT));

            // 根据协议类型创建对应的消费者实现
            if (groupProtocol == GroupProtocol.CONSUMER)
                // 创建异步Kafka消费者
                return new AsyncKafkaConsumer<>(config, keyDeserializer, valueDeserializer);
            else
                // 创建经典Kafka消费者
                return new ClassicKafkaConsumer<>(config, keyDeserializer, valueDeserializer);
        } catch (KafkaException e) {
            // 直接抛出Kafka异常
            throw e;
        } catch (Throwable t) {
            // 将其他异常包装为Kafka异常
            throw new KafkaException("Failed to construct Kafka consumer", t);
        }
    }

    /**
     * 创建消费者委托的完整版本
     * 使用所有必要的组件创建消费者
     *
     * @param logContext 日志上下文
     * @param time 时间服务
     * @param config 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param client Kafka客户端
     * @param subscriptions 订阅状态
     * @param metadata 消费者元数据
     * @param assignors 分区分配器列表
     * @return 创建的消费者委托实例
     * @throws KafkaException 如果创建消费者失败
     */
    public <K, V> ConsumerDelegate<K, V> create(LogContext logContext,
                                                Time time,
                                                ConsumerConfig config,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer,
                                                KafkaClient client,
                                                SubscriptionState subscriptions,
                                                ConsumerMetadata metadata,
                                                List<ConsumerPartitionAssignor> assignors) {
        try {
            // 从配置中获取组协议类型并转换为大写
            GroupProtocol groupProtocol = GroupProtocol.valueOf(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG).toUpperCase(Locale.ROOT));

            // 根据协议类型创建对应的消费者实现
            if (groupProtocol == GroupProtocol.CONSUMER)
                // 创建异步Kafka消费者，使用完整参数集
                return new AsyncKafkaConsumer<>(
                    logContext,
                    time,
                    config,
                    keyDeserializer,
                    valueDeserializer,
                    client,
                    subscriptions,
                    metadata
                );
            else
                // 创建经典Kafka消费者，使用完整参数集
                return new ClassicKafkaConsumer<>(
                    logContext,
                    time,
                    config,
                    keyDeserializer,
                    valueDeserializer,
                    client,
                    subscriptions,
                    metadata,
                    assignors
                );
        } catch (KafkaException e) {
            // 直接抛出Kafka异常
            throw e;
        } catch (Throwable t) {
            // 将其他异常包装为Kafka异常
            throw new KafkaException("Failed to construct Kafka consumer", t);
        }
    }
}
