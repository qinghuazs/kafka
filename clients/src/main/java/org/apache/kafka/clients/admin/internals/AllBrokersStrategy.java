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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is used for use cases which require requests to be sent to all
 * brokers in the cluster.
 * 该类用于需要向集群中所有broker发送请求的场景。
 *
 * This is a slightly degenerate case of a lookup strategy in the sense that
 * the broker IDs are used as both the keys and values. Also, unlike
 * {@link CoordinatorStrategy} and {@link PartitionLeaderStrategy}, we do not
 * know the set of keys ahead of time: we require the initial lookup in order
 * to discover what the broker IDs are. This is represented with a more complex
 * type {@code Future<Map<Integer, Future<V>>} in the admin API result type.
 * For example, see {@link org.apache.kafka.clients.admin.ListTransactionsResult}.
 * 这是一个特殊的查找策略实现，其中broker ID同时作为键和值使用。与{@link CoordinatorStrategy}和
 * {@link PartitionLeaderStrategy}不同，我们事先并不知道完整的键集合：需要先进行初始查找才能发现所有的broker ID。
 * 这种特性在admin API的结果类型中表现为一个更复杂的类型{@code Future<Map<Integer, Future<V>>}。
 * 例如，请参见{@link org.apache.kafka.clients.admin.ListTransactionsResult}。
 */
public class AllBrokersStrategy implements AdminApiLookupStrategy<AllBrokersStrategy.BrokerKey> {
    /**
     * 表示任意broker的特殊键值，用于初始元数据请求
     * 使用OptionalInt.empty()构造，表示不指定具体的broker ID
     */
    public static final BrokerKey ANY_BROKER = new BrokerKey(OptionalInt.empty());
    
    /**
     * 查找键集合，仅包含ANY_BROKER一个元素
     * 用于初始化查找过程，获取集群中所有broker的信息
     */
    public static final Set<BrokerKey> LOOKUP_KEYS = Collections.singleton(ANY_BROKER);
    
    /**
     * 单一请求作用域，用于标识所有请求共享同一个上下文
     * 因为元数据请求可以获取所有broker的信息，所以不需要多个请求作用域
     */
    private static final ApiRequestScope SINGLE_REQUEST_SCOPE = new ApiRequestScope() {
    };

    /**
     * 日志记录器实例，用于记录策略执行过程中的重要信息
     */
    private final Logger log;

    /**
     * 构造函数
     * @param logContext 日志上下文，用于创建特定于该策略的日志记录器
     */
    public AllBrokersStrategy(
        LogContext logContext
    ) {
        this.log = logContext.logger(AllBrokersStrategy.class);
    }

    /**
     * 获取给定键的请求作用域
     * @param key broker键
     * @return 统一的请求作用域实例，因为所有请求共享同一个上下文
     */
    @Override
    public ApiRequestScope lookupScope(BrokerKey key) {
        return SINGLE_REQUEST_SCOPE;
    }

    /**
     * 构建元数据请求
     * @param keys broker键集合
     * @return 空的元数据请求构建器，因为我们只需要响应中的broker信息
     */
    @Override
    public MetadataRequest.Builder buildRequest(Set<BrokerKey> keys) {
        // 验证查找键集合的有效性
        validateLookupKeys(keys);
        // 发送空的元数据请求，我们只关注响应中的broker信息
        return new MetadataRequest.Builder(new MetadataRequestData());
    }

    /**
     * 处理元数据响应
     * @param keys 查找键集合
     * @param abstractResponse 服务器响应
     * @return 包含所有broker映射关系的查找结果
     */
    @Override
    public LookupResult<BrokerKey> handleResponse(Set<BrokerKey> keys, AbstractResponse abstractResponse) {
        // 验证查找键集合的有效性
        validateLookupKeys(keys);

        // 将抽象响应转换为元数据响应并获取broker列表
        MetadataResponse response = (MetadataResponse) abstractResponse;
        MetadataResponseData.MetadataResponseBrokerCollection brokers = response.data().brokers();

        // 如果没有找到broker，返回空结果并在稍后重试
        if (brokers.isEmpty()) {
            log.debug("Metadata response contained no brokers. Will backoff and retry");
            return LookupResult.empty();
        } else {
            log.debug("Discovered all brokers {} to send requests to", brokers);
        }

        // 将broker信息转换为BrokerKey到broker ID的映射
        Map<BrokerKey, Integer> brokerKeys = brokers.stream().collect(Collectors.toMap(
            broker -> new BrokerKey(OptionalInt.of(broker.nodeId())),
            MetadataResponseData.MetadataResponseBroker::nodeId
        ));

        // 返回查找结果，包含已完成的查找键、空的错误映射和broker映射关系
        return new LookupResult<>(
            Collections.singletonList(ANY_BROKER),
            Collections.emptyMap(),
            brokerKeys
        );
    }

    /**
     * 验证查找键集合的有效性
     * @param keys 要验证的键集合
     * @throws IllegalArgumentException 如果键集合大小不为1或者不包含ANY_BROKER
     */
    private void validateLookupKeys(Set<BrokerKey> keys) {
        // 确保键集合只包含一个元素
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected key set: " + keys);
        }
        // 确保该元素是ANY_BROKER
        BrokerKey key = keys.iterator().next();
        if (key != ANY_BROKER) {
            throw new IllegalArgumentException("Unexpected key set: " + keys);
        }
    }

    /**
     * Broker键类，用于标识和查找集群中的broker
     * 主要用途：
     * 1. 作为查找策略的键类型
     * 2. 在元数据请求中标识特定的broker
     * 3. 支持使用ANY_BROKER表示不指定具体broker的场景
     */
    public static class BrokerKey {
        /**
         * broker的ID，使用OptionalInt表示
         * - 如果为empty，表示ANY_BROKER场景
         * - 如果有值，表示特定的broker ID
         */
        public final OptionalInt brokerId;

        /**
         * 构造函数
         * @param brokerId broker ID，可以是empty表示ANY_BROKER
         */
        public BrokerKey(OptionalInt brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BrokerKey that = (BrokerKey) o;
            return Objects.equals(brokerId, that.brokerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerId);
        }

        @Override
        public String toString() {
            return "BrokerKey(" +
                "brokerId=" + brokerId +
                ')';
        }
    }

    /**
     * 所有Broker的Future实现类，用于处理针对所有broker的异步操作
     * 主要功能：
     * 1. 管理针对每个broker的异步操作结果
     * 2. 提供查找完成和异常处理机制
     * 3. 支持单个broker操作的完成和异常处理
     *
     * @param <V> Future完成时的结果类型
     */
    public static class AllBrokersFuture<V> implements AdminApiFuture<BrokerKey, V> {
        /**
         * 整体操作的Future，其结果是一个映射，包含每个broker ID对应的单独Future
         */
        private final KafkaFutureImpl<Map<Integer, KafkaFutureImpl<V>>> future = new KafkaFutureImpl<>();

        /**
         * 存储每个broker对应的Future实例
         * key: broker ID
         * value: 该broker操作的Future
         */
        private final Map<Integer, KafkaFutureImpl<V>> brokerFutures = new HashMap<>();

        /**
         * 获取查找键集合，用于初始化查找过程
         * @return 包含ANY_BROKER的单例集合
         */
        @Override
        public Set<BrokerKey> lookupKeys() {
            return LOOKUP_KEYS;
        }

        /**
         * 完成查找操作，为每个发现的broker创建对应的Future
         * @param brokerMapping broker键到ID的映射关系
         * @throws IllegalArgumentException 如果映射关系无效
         */
        @Override
        public void completeLookup(Map<BrokerKey, Integer> brokerMapping) {
            // 遍历每个broker映射，创建对应的Future
            brokerMapping.forEach((brokerKey, brokerId) -> {
                // 验证映射的broker ID与键中的ID匹配
                if (brokerId != brokerKey.brokerId.orElse(-1)) {
                    throw new IllegalArgumentException("Invalid lookup mapping " + brokerKey + " -> " + brokerId);
                }
                // 为每个broker创建新的Future实例
                brokerFutures.put(brokerId, new KafkaFutureImpl<>());
            });
            // 完成整体Future，结果是所有broker的Future映射
            future.complete(brokerFutures);
        }

        /**
         * 处理查找过程中的异常情况
         * @param lookupErrors 查找过程中发生的错误映射
         * @throws IllegalArgumentException 如果错误键集合与预期不符
         */
        @Override
        public void completeLookupExceptionally(Map<BrokerKey, Throwable> lookupErrors) {
            // 验证错误键集合是否符合预期
            if (!LOOKUP_KEYS.equals(lookupErrors.keySet())) {
                throw new IllegalArgumentException("Unexpected keys among lookup errors: " + lookupErrors);
            }
            // 使用ANY_BROKER对应的异常完成整体Future
            future.completeExceptionally(lookupErrors.get(ANY_BROKER));
        }

        /**
         * 完成多个broker的操作
         * @param values broker键到结果值的映射
         */
        @Override
        public void complete(Map<BrokerKey, V> values) {
            // 遍历完成每个broker的Future
            values.forEach(this::complete);
        }

        /**
         * 完成单个broker的操作
         * @param key broker键
         * @param value 操作结果
         * @throws IllegalArgumentException 如果尝试使用ANY_BROKER完成操作
         */
        private void complete(AllBrokersStrategy.BrokerKey key, V value) {
            // 禁止使用ANY_BROKER完成操作
            if (key == ANY_BROKER) {
                throw new IllegalArgumentException("Invalid attempt to complete with lookup key sentinel");
            } else {
                // 完成对应broker的Future
                futureOrThrow(key).complete(value);
            }
        }

        /**
         * 处理多个broker操作的异常情况
         * @param errors broker键到异常的映射
         */
        @Override
        public void completeExceptionally(Map<BrokerKey, Throwable> errors) {
            // 遍历处理每个broker的异常
            errors.forEach(this::completeExceptionally);
        }

        /**
         * 处理单个broker操作的异常情况
         * @param key broker键
         * @param t 异常实例
         */
        private void completeExceptionally(AllBrokersStrategy.BrokerKey key, Throwable t) {
            // 如果是ANY_BROKER，则完成整体Future的异常
            if (key == ANY_BROKER) {
                future.completeExceptionally(t);
            } else {
                // 完成对应broker Future的异常
                futureOrThrow(key).completeExceptionally(t);
            }
        }

        /**
         * 获取整体操作的Future
         * @return 包含所有broker Future映射的Future实例
         */
        public KafkaFutureImpl<Map<Integer, KafkaFutureImpl<V>>> all() {
            return future;
        }

        /**
         * 获取指定broker的Future，如果无效则抛出异常
         * @param key broker键
         * @return broker对应的Future实例
         * @throws IllegalArgumentException 如果broker键无效或未知
         */
        private KafkaFutureImpl<V> futureOrThrow(BrokerKey key) {
            // 检查broker ID是否存在
            if (key.brokerId.isEmpty()) {
                throw new IllegalArgumentException("Attempt to complete with invalid key: " + key);
            } else {
                // 获取broker ID并查找对应的Future
                int brokerId = key.brokerId.getAsInt();
                KafkaFutureImpl<V> future = brokerFutures.get(brokerId);
                if (future == null) {
                    throw new IllegalArgumentException("Attempt to complete with unknown broker id: " + brokerId);
                } else {
                    return future;
                }
            }
        }

    }

}
