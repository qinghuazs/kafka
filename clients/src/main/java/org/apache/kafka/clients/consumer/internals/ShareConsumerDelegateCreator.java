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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

/**
 * 共享消费者委托创建器
 * 实现了一个准工厂模式，使调用者无需了解所创建的{@link ShareConsumer}底层实现。
 * 这提供了一种机制，使{@link KafkaShareConsumer}可以保持作为实现的顶层外观，
 * 同时允许不同的实现在底层共存。
 *
 * 应用场景：
 * 1. 创建共享消费者实例
 * 2. 隔离实现细节
 * 3. 统一实例创建接口
 * 4. 支持多种实现共存
 *
 * 注意：此类仅供内部使用，不适用于最终用户。内部用户不应尝试确定底层实现，
 * 以避免依赖不稳定的接口。相反，应该使用{@link ShareConsumer} API契约
 * 作为调用者的接口。
 */
public class ShareConsumerDelegateCreator {
    
    /**
     * 创建共享消费者委托实例的简化方法
     * 使用最基本的配置创建一个新的共享消费者实例
     *
     * @param config 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @return 新创建的共享消费者委托实例
     * @throws KafkaException 如果创建过程中发生错误
     */
    public <K, V> ShareConsumerDelegate<K, V> create(final ConsumerConfig config,
                                                     final Deserializer<K> keyDeserializer,
                                                     final Deserializer<V> valueDeserializer) {
        try {
            // 创建新的日志上下文
            LogContext logContext = new LogContext();
            // 获取当前类的日志记录器
            Logger log = logContext.logger(getClass());
            // 记录警告信息，提醒这是早期访问特性
            log.warn("Share groups and KafkaShareConsumer are part of the early access of KIP-932 and MUST NOT be used in production.");
            // 创建并返回新的共享消费者实现实例
            return new ShareConsumerImpl<>(config, keyDeserializer, valueDeserializer);
        } catch (KafkaException e) {
            // 直接抛出Kafka异常
            throw e;
        } catch (Throwable t) {
            // 将其他异常包装为KafkaException
            throw new KafkaException("Failed to construct Kafka share consumer", t);
        }
    }

    /**
     * 创建共享消费者委托实例的完整方法
     * 使用所有可配置参数创建一个新的共享消费者实例
     *
     * @param logContext 日志上下文
     * @param clientId 客户端ID
     * @param groupId 消费者组ID
     * @param config 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param time 时间实例
     * @param client Kafka客户端实例
     * @param subscriptions 订阅状态
     * @param metadata 消费者元数据
     * @return 新创建的共享消费者委托实例
     * @throws KafkaException 如果创建过程中发生错误
     */
    public <K, V> ShareConsumerDelegate<K, V> create(final LogContext logContext,
                                                     final String clientId,
                                                     final String groupId,
                                                     final ConsumerConfig config,
                                                     final Deserializer<K> keyDeserializer,
                                                     final Deserializer<V> valueDeserializer,
                                                     final Time time,
                                                     final KafkaClient client,
                                                     final SubscriptionState subscriptions,
                                                     final ConsumerMetadata metadata) {
        try {
            // 获取日志记录器
            Logger log = logContext.logger(getClass());
            // 记录警告信息，提醒这是早期访问特性
            log.warn("Share groups and KafkaShareConsumer are part of the early access of KIP-932 and MUST NOT be used in production.");
            // 创建并返回新的共享消费者实现实例，使用所有提供的参数
            return new ShareConsumerImpl<>(
                    logContext,
                    clientId,
                    groupId,
                    config,
                    keyDeserializer,
                    valueDeserializer,
                    time,
                    client,
                    subscriptions,
                    metadata
            );
        } catch (KafkaException e) {
            // 直接抛出Kafka异常
            throw e;
        } catch (Throwable t) {
            // 将其他异常包装为KafkaException
            throw new KafkaException("Failed to construct Kafka share consumer", t);
        }
    }
}