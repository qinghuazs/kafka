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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kafka管理API的核心处理接口，负责处理各种管理操作请求。
 * 该接口采用泛型设计：
 * @param <K> 请求的键类型，例如主题名称、事务ID等
 * @param <V> 响应的值类型，取决于具体的管理操作
 */
public interface AdminApiHandler<K, V> {

    /**
     * 获取该处理器实现的API的用户友好名称。
     * Get a user-friendly name for the API this handler is implementing.
     */
    String apiName();

    /**
     * 为给定的键集合构建必要的请求。
     * 在查找阶段，{@link AdminApiDriver}会将映射到同一目标broker的键分组。
     * 处理器可以选择：
     * 1. 为所有键发出单个批量请求（参见{@link Batched}）
     * 2. 为每个键发出单独的请求（参见{@link Unbatched}）
     * 3. 实现自定义的分组逻辑
     * 
     * Build the requests necessary for the given keys. The set of keys is derived by
     * {@link AdminApiDriver} during the lookup stage as the set of keys which all map
     * to the same destination broker. Handlers can choose to issue a single request for
     * all of the provided keys (see {@link Batched}), issue one request per key (see
     * {@link Unbatched}), or implement their own custom grouping logic if necessary.
     *
     * @param brokerId 目标broker的ID
     * @param keys 需要处理的键集合
     * @return 包含请求和对应键的{@link RequestAndKeys}集合
     */
    Collection<RequestAndKeys<K>> buildRequest(int brokerId, Set<K> keys);

    /**
     * 请求成功返回时的回调处理方法。
     * 该方法需要：
     * 1. 解析响应内容
     * 2. 检查错误信息
     * 3. 返回处理结果，包括：
     *    - 已完成的键
     *    - 遇到不可恢复错误的键
     *    - 需要重新映射的键
     * 
     * 特殊情况处理：
     * 1. 如果响应表明目标brokerId不正确（例如NotLeader错误），相关键将被取消映射并重试查找
     * 2. 遇到可重试错误的键应从结果中排除，系统会自动重试这些键
     * 
     * Callback that is invoked when a request returns successfully.
     * The handler should parse the response, check for errors, and return a
     * result which indicates which keys (if any) have either been completed or
     * failed with an unrecoverable error.
     *
     * It is also possible that the response indicates an incorrect target brokerId
     * (e.g. in the case of a NotLeader error when the request is bound for a partition
     * leader). In this case the key will be "unmapped" from the target brokerId
     * and lookup will be retried.
     *
     * Note that keys which received a retriable error should be left out of the
     * result. They will be retried automatically.
     *
     * @param broker 接收请求的broker节点
     * @param keys 关联请求中的键集合
     * @param response broker返回的响应
     * @return 包含键完成状态、失败信息和重映射信息的结果
     */
    ApiResult<K, V> handleResponse(Node broker, Set<K> keys, AbstractResponse response);

    /**
     * 当请求遇到UnsupportedVersionException时的回调处理方法。
     * 处理流程：
     * 1. 对于无法处理且不应重试的键，将其映射到错误并返回
     * 2. 对于其余的键，系统将重试请求
     * 
     * Callback that is invoked when a fulfillment request hits an UnsupportedVersionException.
     * Keys for which the exception cannot be handled and the request shouldn't be retried must be mapped
     * to an error and returned. The request will then be retried for the remainder of the keys.
     *
     * @param brokerId 目标broker的ID
     * @param exception 不支持版本的异常
     * @param keys 请求中的键集合
     * @return 无法处理的键到错误的映射。如果异常完全无法处理，将包含所有初始键
     */
    default Map<K, Throwable> handleUnsupportedVersionException(
        int brokerId,
        UnsupportedVersionException exception,
        Set<K> keys
    ) {
        return keys.stream().collect(Collectors.toMap(k -> k, k -> exception));
    }

    /**
     * 获取负责查找处理每个键的brokerId的查找策略。
     * 该策略用于确定每个管理请求应该发送到哪个broker节点。
     * 
     * Get the lookup strategy that is responsible for finding the brokerId
     * which will handle each respective key.
     *
     * @return 非空的查找策略实例
     */
    AdminApiLookupStrategy<K> lookupStrategy();

    /**
     * API请求的结果类，包含三种状态的键：
     * 1. 完成的键（completedKeys）：请求成功完成
     * 2. 失败的键（failedKeys）：遇到不可恢复的错误
     * 3. 未映射的键（unmappedKeys）：需要重新查找目标broker
     */
    class ApiResult<K, V> {
        public final Map<K, V> completedKeys;
        public final Map<K, Throwable> failedKeys;
        public final List<K> unmappedKeys;

        public ApiResult(
            Map<K, V> completedKeys,
            Map<K, Throwable> failedKeys,
            List<K> unmappedKeys
        ) {
            this.completedKeys = Collections.unmodifiableMap(completedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
            this.unmappedKeys = Collections.unmodifiableList(unmappedKeys);
        }

        public static <K, V> ApiResult<K, V> completed(K key, V value) {
            return new ApiResult<>(
                Collections.singletonMap(key, value),
                Collections.emptyMap(),
                Collections.emptyList()
            );
        }

        public static <K, V> ApiResult<K, V> failed(K key, Throwable t) {
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.singletonMap(key, t),
                Collections.emptyList()
            );
        }

        public static <K, V> ApiResult<K, V> unmapped(List<K> keys) {
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                keys
            );
        }

        public static <K, V> ApiResult<K, V> empty() {
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList()
            );
        }
    }

    /**
     * 请求和相关键的封装类，用于将请求与其处理的键关联起来
     */
    class RequestAndKeys<K> {
        public final AbstractRequest.Builder<?> request;
        public final Set<K> keys;

        public RequestAndKeys(AbstractRequest.Builder<?> request, Set<K> keys) {
            this.request = request;
            this.keys = keys;
        }
    }

    /**
     * 批处理请求处理器，用于支持将多个键组合到单个请求中的场景。
     * 应用场景：
     * - 当多个键的目标是同一个broker时，将它们分组处理
     * - 适用于支持批量操作的broker API，如描述或列出事务
     * 
     * An {@link AdminApiHandler} that will group multiple keys into a single request when possible.
     * Keys will be grouped together whenever they target the same broker. This type of handler
     * should be used when interacting with broker APIs that can act on multiple keys at once, such
     * as describing or listing transactions.
     */
    abstract class Batched<K, V> implements AdminApiHandler<K, V> {
        abstract AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<K> keys);

        @Override
        public final Collection<RequestAndKeys<K>> buildRequest(int brokerId, Set<K> keys) {
            return Collections.singleton(new RequestAndKeys<>(buildBatchedRequest(brokerId, keys), keys));
        }
    }

    /**
     * 单键请求处理器，为每个键创建独立的请求。
     * 应用场景：
     * - 不进行基于目标broker的分组
     * - 适用于不支持批量操作的broker API，如初始化事务生产者
     * 
     * An {@link AdminApiHandler} that will create one request per key, not performing any grouping based
     * on the targeted broker. This type of handler should only be used for broker APIs that do not accept
     * multiple keys at once, such as initializing a transactional producer.
     */
    abstract class Unbatched<K, V> implements AdminApiHandler<K, V> {
        abstract AbstractRequest.Builder<?> buildSingleRequest(int brokerId, K key);
        abstract ApiResult<K, V> handleSingleResponse(Node broker, K key, AbstractResponse response);

        @Override
        public final Collection<RequestAndKeys<K>> buildRequest(int brokerId, Set<K> keys) {
            return keys.stream()
                .map(key -> new RequestAndKeys<>(buildSingleRequest(brokerId, key), Collections.singleton(key)))
                .collect(Collectors.toSet());
        }

        @Override
        public final ApiResult<K, V> handleResponse(Node broker, Set<K> keys, AbstractResponse response) {
            if (keys.size() != 1) {
                throw new IllegalArgumentException("Unbatched admin handler should only be required to handle responses for a single key at a time");
            }
            K key = keys.iterator().next();
            return handleSingleResponse(broker, key, response);
        }
    }
}
