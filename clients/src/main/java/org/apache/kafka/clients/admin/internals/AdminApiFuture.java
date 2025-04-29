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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Kafka Admin API的异步操作核心接口
 * 该接口处理Admin API请求的两个主要阶段：
 * 1. 查找(Lookup)阶段：确定每个请求key应该发送到哪个Broker
 * 2. 执行(Fulfillment)阶段：向目标Broker发送实际请求并处理响应
 *
 * @param <K> 请求的key类型
 * @param <V> 响应的value类型
 */
public interface AdminApiFuture<K, V> {

    /**
     * 获取初始的查找key集合
     * The initial set of lookup keys. Although this will usually match the fulfillment
     * keys, it does not necessarily have to. For example, in the case of
     * {@link AllBrokersStrategy.AllBrokersFuture},
     * we use the lookup phase in order to discover the set of keys that will be searched
     * during the fulfillment phase.
     *
     * 通常情况下，这些key与执行阶段的key相匹配，但并非必须如此。
     * 例如在{@link AllBrokersStrategy.AllBrokersFuture}的情况下，
     * 我们使用查找阶段来发现在执行阶段将要搜索的key集合。
     *
     * @return 非空的初始查找key集合
     */
    Set<K> lookupKeys();

    /**
     * 获取没有缓存的broker id映射的请求key集合
     * The set of request keys that do not have cached key-broker id mappings. If there
     * is no cached key mapping, this will be the same as the lookup keys.
     * Can be empty, but only if the cached key mapping is not empty.
     *
     * 如果没有缓存的key映射，这将与lookupKeys()返回的集合相同。
     * 只有当缓存的key映射非空时，此方法才可能返回空集合。
     */
    default Set<K> uncachedLookupKeys() {
        return lookupKeys();
    }

    /**
     * 获取缓存的key到broker id的映射
     * The cached key-broker id mapping. For lookup strategies that do not make use of a
     * cache of metadata, this will be empty.
     *
     * 对于不使用元数据缓存的查找策略，此方法将返回空映射。
     * 
     * @return key到broker id的映射关系
     */
    default Map<K, Integer> cachedKeyBrokerIdMapping() {
        return Collections.emptyMap();
    }

    /**
     * 完成与给定key关联的futures
     * Complete the futures associated with the given keys.
     *
     * @param values 已完成的key及其对应的值的映射
     */
    void complete(Map<K, V> values);

    /**
     * 当一组key的查找成功时调用
     * Invoked when lookup of a set of keys succeeds.
     *
     * @param brokerIdMapping 发现的key到将处理执行请求的brokerId的映射
     */
    default void completeLookup(Map<K, Integer> brokerIdMapping) {
    }

    /**
     * 当一组key的查找因致命错误而失败时调用
     * Invoked when lookup fails with a fatal error on a set of keys.
     *
     * @param lookupErrors 查找失败的key及其对应的错误的映射
     */
    default void completeLookupExceptionally(Map<K, Throwable> lookupErrors) {
        completeExceptionally(lookupErrors);
    }

    /**
     * 以异常方式完成与给定key关联的futures
     * Complete the futures associated with the given keys exceptionally.
     *
     * @param errors 失败的key及其对应的错误的映射
     */
    void completeExceptionally(Map<K, Throwable> errors);

    /**
     * 为给定的key集合创建一个SimpleAdminApiFuture实例
     * 
     * @param keys 需要处理的key集合
     * @return 新创建的SimpleAdminApiFuture实例
     */
    static <K, V> SimpleAdminApiFuture<K, V> forKeys(Set<K> keys) {
        return new SimpleAdminApiFuture<>(keys);
    }

    /**
     * 当key集合在创建时就已知的情况下使用的简单实现类
     * This class can be used when the set of keys is known ahead of time.
     * 
     * 该类维护了一个key到KafkaFuture的映射，用于跟踪每个key的异步操作状态
     */
    class SimpleAdminApiFuture<K, V> implements AdminApiFuture<K, V> {
        /**
         * 存储每个key对应的Future对象的映射
         * key: 请求的key
         * value: 对应的KafkaFuture实例，用于异步获取结果
         */
        private final Map<K, KafkaFuture<V>> futures;

        /**
         * 构造函数，为每个key创建对应的KafkaFutureImpl实例
         * 
         * @param keys 需要处理的key集合
         */
        public SimpleAdminApiFuture(Set<K> keys) {
            // 使用Stream API将key集合转换为key到Future的映射
            this.futures = keys.stream().collect(Collectors.toMap(
                Function.identity(), // key保持不变
                k -> new KafkaFutureImpl<>() // 为每个key创建新的Future实例
            ));
        }

        @Override
        public Set<K> lookupKeys() {
            // 返回所有key的集合
            return futures.keySet();
        }

        @Override
        public void complete(Map<K, V> values) {
            // 遍历完成的key-value对，逐个完成对应的Future
            values.forEach(this::complete);
        }

        /**
         * 完成单个key的Future
         * 
         * @param key 要完成的key
         * @param value 对应的结果值
         */
        private void complete(K key, V value) {
            futureOrThrow(key).complete(value);
        }

        @Override
        public void completeExceptionally(Map<K, Throwable> errors) {
            // 遍历错误的key-异常对，逐个完成对应的Future(异常完成)
            errors.forEach(this::completeExceptionally);
        }

        /**
         * 以异常方式完成单个key的Future
         * 
         * @param key 要完成的key
         * @param t 对应的异常
         */
        private void completeExceptionally(K key, Throwable t) {
            futureOrThrow(key).completeExceptionally(t);
        }

        /**
         * 获取指定key的Future，如果key不存在则抛出异常
         * 
         * @param key 要获取Future的key
         * @return key对应的KafkaFutureImpl实例
         * @throws IllegalArgumentException 如果key不在初始化时提供的集合中
         */
        private KafkaFutureImpl<V> futureOrThrow(K key) {
            // 类型转换是安全的，因为我们初始化时只使用KafkaFutureImpl
            KafkaFutureImpl<V> future = (KafkaFutureImpl<V>) futures.get(key);
            if (future == null) {
                throw new IllegalArgumentException("尝试完成未请求的key: " + key);
            } else {
                return future;
            }
        }

        /**
         * 获取所有key的Future映射
         * 
         * @return key到Future的映射关系
         */
        public Map<K, KafkaFuture<V>> all() {
            return futures;
        }

        /**
         * 获取指定key的Future
         * 
         * @param key 要获取Future的key
         * @return key对应的Future，如果key不存在则返回null
         */
        public KafkaFuture<V> get(K key) {
            return futures.get(key);
        }
    }
}
