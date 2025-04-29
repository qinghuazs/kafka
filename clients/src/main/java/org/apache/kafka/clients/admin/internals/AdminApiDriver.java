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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException;
import org.apache.kafka.common.requests.OffsetFetchRequest.NoBatchedOffsetFetchRequestException;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The `KafkaAdminClient`'s internal `Call` primitive is not a good fit for multi-stage
 * request workflows such as we see with the group coordinator APIs or any request which
 * needs to be sent to a partition leader. Typically these APIs have two concrete stages:
 *
 * 1. Lookup: Find the broker that can fulfill the request (e.g. partition leader or group
 *            coordinator)
 * 2. Fulfillment: Send the request to the broker found in the first step
 *
 * This is complicated by the fact that `Admin` APIs are typically batched, which
 * means the Lookup stage may result in a set of brokers. For example, take a `ListOffsets`
 * request for a set of topic partitions. In the Lookup stage, we will find the partition
 * leaders for this set of partitions; in the Fulfillment stage, we will group together
 * partition according to the IDs of the discovered leaders.
 *
 * Additionally, the flow between these two stages is bi-directional. We may find after
 * sending a `ListOffsets` request to an expected leader that there was a leader change.
 * This would result in a topic partition being sent back to the Lookup stage.
 *
 * Managing this complexity by chaining together `Call` implementations is challenging
 * and messy, so instead we use this class to do the bookkeeping. It handles both the
 * batching aspect as well as the transitions between the Lookup and Fulfillment stages.
 *
 * Note that the interpretation of the `retries` configuration becomes ambiguous
 * for this kind of pipeline. We could treat it as an overall limit on the number
 * of requests that can be sent, but that is not very useful because each pipeline
 * has a minimum number of requests that need to be sent in order to satisfy the request.
 * Instead, we treat this number of retries independently at each stage so that each
 * stage has at least one opportunity to complete. So if a user sets `retries=1`, then
 * the full pipeline can still complete as long as there are no request failures.
 *
 * 中文说明：
 * KafkaAdminClient的内部Call原语不适合处理多阶段请求工作流，比如组协调器API或需要发送到分区leader的请求。
 * 这些API通常有两个具体阶段：
 *
 * 1. 查找(Lookup)：找到可以处理请求的broker(如分区leader或组协调器)
 * 2. 执行(Fulfillment)：将请求发送给在第一步中找到的broker
 *
 * 这个过程因Admin API通常是批处理的而变得复杂，意味着Lookup阶段可能会得到一组brokers。
 * 例如，对于一组主题分区的ListOffsets请求，在Lookup阶段我们会找到这些分区的leader，
 * 在Fulfillment阶段，我们会根据发现的leader的ID将分区分组。
 *
 * 此外，这两个阶段之间的流程是双向的。我们可能在向预期的leader发送ListOffsets请求后
 * 发现leader发生了变更，这会导致主题分区被送回Lookup阶段。
 *
 * 通过链接Call实现来管理这种复杂性是具有挑战性且混乱的，因此我们使用这个类来进行簿记。
 * 它同时处理批处理方面以及Lookup和Fulfillment阶段之间的转换。
 *
 * 注意，对于这种管道，retries配置的解释变得模糊。我们可以将其视为可以发送的请求总数的限制，
 * 但这不是很有用，因为每个管道都需要发送最小数量的请求才能满足请求。相反，我们在每个阶段
 * 独立处理重试次数，以便每个阶段至少有一次完成的机会。因此，如果用户设置retries=1，
 * 只要没有请求失败，完整的管道仍然可以完成。
 *
 * @param <K> 键类型，也是请求路由的粒度(例如，对于发送到分区leader的请求，可以是TopicPartition，
 *            对于发送到组协调器的消费者组请求，可以是GroupId)
 * @param <V> 每个键的完成类型(例如，当键类型是消费者GroupId时，可以是消费者组状态)
 */
public class AdminApiDriver<K, V> {
    // 日志记录器
    private final Logger log;
    // 指数退避重试机制
    private final ExponentialBackoff retryBackoff;
    // 请求截止时间(毫秒)
    private final long deadlineMs;
    // API处理器，负责处理具体的请求逻辑
    private final AdminApiHandler<K, V> handler;
    // 异步结果Future
    private final AdminApiFuture<K, V> future;

    // 查找阶段的映射，存储需要查找broker的键
    private final BiMultimap<ApiRequestScope, K> lookupMap = new BiMultimap<>();
    // 执行阶段的映射，存储已知broker的键
    private final BiMultimap<FulfillmentScope, K> fulfillmentMap = new BiMultimap<>();
    // 每个请求范围的状态，包含重试和进行中的请求信息
    private final Map<ApiRequestScope, RequestState> requestStates = new HashMap<>();

    /**
     * 创建AdminApiDriver实例
     * 
     * @param handler API处理器，负责处理具体的请求逻辑
     * @param future 异步结果Future，用于存储请求的结果
     * @param deadlineMs 请求截止时间(毫秒)
     * @param retryBackoffMs 重试初始退避时间(毫秒)
     * @param retryBackoffMaxMs 重试最大退避时间(毫秒)
     * @param logContext 日志上下文
     */
    public AdminApiDriver(
        AdminApiHandler<K, V> handler,
        AdminApiFuture<K, V> future,
        long deadlineMs,
        long retryBackoffMs,
        long retryBackoffMaxMs,
        LogContext logContext
    ) {
        // 初始化基本组件
        this.handler = handler;
        this.future = future;
        this.deadlineMs = deadlineMs;
        // 创建指数退避重试机制，用于控制重试间隔
        this.retryBackoff = new ExponentialBackoff(
            retryBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            retryBackoffMaxMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.log = logContext.logger(AdminApiDriver.class);

        // 对于没有缓存信息的查找键，需要查找元数据
        // 对于所有已缓存的键，可以直接进入执行阶段
        // 注意：缓存仅用于初始调用，任何导致额外查找的错误都会使用完整的查找键集
        retryLookup(future.uncachedLookupKeys());
        // 将已缓存的键-broker映射直接添加到执行映射中
        future.cachedKeyBrokerIdMapping().forEach((key, brokerId) -> fulfillmentMap.put(new FulfillmentScope(brokerId), key));
    }

    /**
     * 将键与brokerId关联。这在Lookup阶段的响应揭示映射关系时调用
     * (例如，当FindCoordinator告诉我们特定消费者组的组协调器时)
     *
     * @param key 需要映射的键
     * @param brokerId 目标broker的ID
     */
    private void map(K key, Integer brokerId) {
        // 从查找映射中移除键，因为已经找到了对应的broker
        lookupMap.remove(key);
        // 将键添加到执行映射中，与目标broker关联
        fulfillmentMap.put(new FulfillmentScope(brokerId), key);
    }

    /**
     * 解除键与当前映射的brokerId的关联。这会将键发送回Lookup阶段，
     * 允许我们重新尝试查找。
     *
     * @param key 需要解除映射的键
     */
    private void unmap(K key) {
        // 从执行映射中移除键
        fulfillmentMap.remove(key);

        // 获取键的查找范围和目标broker ID
        ApiRequestScope lookupScope = handler.lookupStrategy().lookupScope(key);
        OptionalInt destinationBrokerId = lookupScope.destinationBrokerId();

        // 如果查找范围指定了目标broker，直接添加到执行映射
        // 否则，将键添加到查找映射中等待重新查找
        if (destinationBrokerId.isPresent()) {
            fulfillmentMap.put(new FulfillmentScope(destinationBrokerId.getAsInt()), key);
        } else {
            lookupMap.put(handler.lookupStrategy().lookupScope(key), key);
        }
    }

    /**
     * 清除一组键的所有映射关系
     *
     * @param keys 需要清除映射的键集合
     */
    private void clear(Collection<K> keys) {
        // 从查找映射和执行映射中移除所有指定的键
        keys.forEach(key -> {
            lookupMap.remove(key);
            fulfillmentMap.remove(key);
        });
    }

    /**
     * 获取键对应的broker ID
     *
     * @param key 需要查找broker ID的键
     * @return 如果键已映射到broker则返回broker ID，否则返回空
     */
    OptionalInt keyToBrokerId(K key) {
        // 从执行映射中获取键的范围
        Optional<FulfillmentScope> scope = fulfillmentMap.getKey(key);
        // 如果找到范围，返回对应的broker ID，否则返回空
        return scope
            .map(fulfillmentScope -> OptionalInt.of(fulfillmentScope.destinationBrokerId))
            .orElseGet(OptionalInt::empty);
    }

    /**
     * 异常完成与给定键关联的future。调用后，键将从Lookup和Fulfillment阶段中移除，
     * 以避免请求重试。
     *
     * @param errors 键到异常的映射
     */
    private void completeExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            // 使用异常完成future
            future.completeExceptionally(errors);
            // 清除这些键的所有映射关系
            clear(errors.keySet());
        }
    }

    /**
     * 异常完成Lookup阶段的future。调用后，键将从Lookup和Fulfillment阶段中移除，
     * 以避免请求重试。
     *
     * @param errors 键到异常的映射
     */
    private void completeLookupExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            // 使用异常完成Lookup阶段的future
            future.completeLookupExceptionally(errors);
            // 清除这些键的所有映射关系
            clear(errors.keySet());
        }
    }

    /**
     * 重试查找指定的键集合
     *
     * @param keys 需要重试查找的键集合
     */
    private void retryLookup(Collection<K> keys) {
        // 解除每个键的当前映射，使其重新进入查找阶段
        keys.forEach(this::unmap);
    }

    /**
     * 完成与给定键关联的future。调用后，所有键将从Lookup和Fulfillment阶段中移除，
     * 以避免请求重试。
     *
     * @param values 键到值的映射
     */
    private void complete(Map<K, V> values) {
        if (!values.isEmpty()) {
            // 完成future
            future.complete(values);
            // 清除这些键的所有映射关系
            clear(values.keySet());
        }
    }

    /**
     * 完成Lookup阶段，将找到的broker ID映射到相应的键
     *
     * @param brokerIdMapping 键到broker ID的映射
     */
    private void completeLookup(Map<K, Integer> brokerIdMapping) {
        if (!brokerIdMapping.isEmpty()) {
            // 完成Lookup阶段的future
            future.completeLookup(brokerIdMapping);
            // 将每个键映射到对应的broker
            brokerIdMapping.forEach(this::map);
        }
    }

    /**
     * Check whether any requests need to be sent. This should be called immediately
     * after the driver is constructed and then again after each request returns
     * (i.e. after {@link #onFailure(long, RequestSpec, Throwable)} or
     * {@link #onResponse(long, RequestSpec, AbstractResponse, Node)}).
     *
     * @return A list of requests that need to be sent
     * 
     * 检查是否有需要发送的请求。这个方法应该在驱动器构造后立即调用，并在每个请求返回后再次调用
     * (即在{@link #onFailure(long, RequestSpec, Throwable)}或
     * {@link #onResponse(long, RequestSpec, AbstractResponse, Node)}之后)。
     *
     * @return 需要发送的请求列表
     */
    public List<RequestSpec<K>> poll() {
        // 创建一个新的请求列表用于存储待发送的请求
        List<RequestSpec<K>> requests = new ArrayList<>();
        // 收集查找阶段(Lookup)的请求，例如查找组协调器或分区leader
        collectLookupRequests(requests);
        // 收集执行阶段(Fulfillment)的请求，即向已知的broker发送实际操作请求
        collectFulfillmentRequests(requests);
        return requests;
    }

    /**
     * Callback that is invoked when a `Call` returns a response successfully.
     * 
     * 当请求调用成功返回响应时调用的回调方法。
     * 该方法处理两种类型的响应：
     * 1. 执行阶段(Fulfillment)的响应：处理实际操作的结果
     * 2. 查找阶段(Lookup)的响应：处理broker查找的结果
     */
    public void onResponse(
        long currentTimeMs,      // 当前时间戳(毫秒)
        RequestSpec<K> spec,     // 请求规范，包含请求的详细信息
        AbstractResponse response,// 服务器的响应
        Node node               // 响应来自的节点
    ) {
        // 清除正在处理的请求状态，更新重试计时器
        clearInflightRequest(currentTimeMs, spec);

        // 判断是执行阶段还是查找阶段的响应
        if (spec.scope instanceof FulfillmentScope) {
            // 处理执行阶段的响应
            AdminApiHandler.ApiResult<K, V> result = handler.handleResponse(
                node,
                spec.keys,
                response
            );
            // 完成成功处理的键
            complete(result.completedKeys);
            // 处理失败的键，使用异常完成
            completeExceptionally(result.failedKeys);
            // 对于需要重新映射的键（如leader变更），重新进行查找
            retryLookup(result.unmappedKeys);
        } else {
            // 处理查找阶段的响应
            AdminApiLookupStrategy.LookupResult<K> result = handler.lookupStrategy().handleResponse(
                spec.keys,
                response
            );

            // 从查找映射中移除已完成的键
            result.completedKeys.forEach(lookupMap::remove);
            // 完成broker映射，将键移动到执行阶段
            completeLookup(result.mappedKeys);
            // 处理查找失败的键
            completeLookupExceptionally(result.failedKeys);
        }
    }

    /**
     * Callback that is invoked when a `Call` is failed.
     * 
     * 当请求调用失败时调用的回调方法。
     * 该方法处理各种类型的失败情况：
     * 1. 连接断开：重新查找以找到新的coordinator或leader
     * 2. 批处理不支持：禁用批处理并重试
     * 3. 版本不支持：处理不可恢复的失败
     * 4. 其他错误：完成异常处理
     */
    public void onFailure(
        long currentTimeMs,      // 当前时间戳(毫秒)
        RequestSpec<K> spec,     // 失败的请求规范
        Throwable t              // 失败的异常
    ) {
        // 清除正在处理的请求状态，更新重试计时器
        clearInflightRequest(currentTimeMs, spec);
        
        if (t instanceof DisconnectException) {
            // 处理连接断开异常
            log.debug("Node disconnected before response could be received for request {}. " +
                "Will attempt retry", spec.request);

            // 连接断开后，让驱动器重新查找键
            // 这给我们一个机会找到新的coordinator或分区leader
            Set<K> keysToUnmap = spec.keys.stream()
                .filter(future.lookupKeys()::contains)
                .collect(Collectors.toSet());
            retryLookup(keysToUnmap);

        } else if (t instanceof NoBatchedFindCoordinatorsException || t instanceof NoBatchedOffsetFetchRequestException) {
            // 处理批处理不支持的异常
            // 禁用批处理功能并重试查找
            ((CoordinatorStrategy) handler.lookupStrategy()).disableBatch();
            Set<K> keysToUnmap = spec.keys.stream()
                .filter(future.lookupKeys()::contains)
                .collect(Collectors.toSet());
            retryLookup(keysToUnmap);
        } else if (t instanceof UnsupportedVersionException) {
            // 处理版本不支持的异常
            if (spec.scope instanceof FulfillmentScope) {
                // 执行阶段的版本不支持
                int brokerId = ((FulfillmentScope) spec.scope).destinationBrokerId;
                Map<K, Throwable> unrecoverableFailures =
                    handler.handleUnsupportedVersionException(
                        brokerId,
                        (UnsupportedVersionException) t,
                        spec.keys);
                completeExceptionally(unrecoverableFailures);
            } else {
                // 查找阶段的版本不支持
                Map<K, Throwable> unrecoverableLookupFailures =
                    handler.lookupStrategy().handleUnsupportedVersionException(
                        (UnsupportedVersionException) t,
                        spec.keys);
                completeLookupExceptionally(unrecoverableLookupFailures);
                // 对于可以重试的键继续进行查找
                Set<K> keysToUnmap = spec.keys.stream()
                    .filter(k -> !unrecoverableLookupFailures.containsKey(k))
                    .collect(Collectors.toSet());
                retryLookup(keysToUnmap);
            }
        } else {
            // 处理其他类型的异常
            Map<K, Throwable> errors = spec.keys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> t
            ));
            // 根据请求阶段选择不同的异常完成方式
            if (spec.scope instanceof FulfillmentScope) {
                completeExceptionally(errors);
            } else {
                completeLookupExceptionally(errors);
            }
        }
    }

    /**
     * 清除正在处理的请求状态，并根据请求类型更新重试计时器
     * 
     * @param currentTimeMs 当前时间戳(毫秒)
     * @param spec 请求规范，包含请求的详细信息
     */
    private void clearInflightRequest(long currentTimeMs, RequestSpec<K> spec) {
        // 获取请求范围对应的状态
        RequestState requestState = requestStates.get(spec.scope);
        if (requestState != null) {
            // 只有在执行阶段(非查找请求)才应用退避策略
            if (spec.scope instanceof FulfillmentScope) {
                // 清除正在处理的请求状态并更新退避时间
                requestState.clearInflightAndBackoff(currentTimeMs);
            } else {
                // 对于查找请求，只清除正在处理的状态
                requestState.clearInflight(currentTimeMs);
            }
        }
    }

    /**
     * 收集需要发送的请求，支持批处理和请求状态管理
     * 
     * @param requests 存储待发送请求的列表
     * @param multimap 键到请求范围的映射
     * @param buildRequest 构建请求的函数，接收键集合和范围，返回请求和键的集合
     * @param <T> 请求范围类型，必须是ApiRequestScope的子类
     */
    private <T extends ApiRequestScope> void collectRequests(
        List<RequestSpec<K>> requests,
        BiMultimap<T, K> multimap,
        BiFunction<Set<K>, T, Collection<AdminApiHandler.RequestAndKeys<K>>> buildRequest
    ) {
        // 遍历每个请求范围和对应的键集合
        for (Map.Entry<T, Set<K>> entry : multimap.entrySet()) {
            T scope = entry.getKey();

            // 获取当前范围的所有键
            Set<K> keys = entry.getValue();
            if (keys.isEmpty()) {
                continue;
            }

            // 获取或创建请求状态，用于跟踪重试和进行中的请求
            RequestState requestState = requestStates.computeIfAbsent(scope, c -> new RequestState());
            // 如果当前范围已有正在处理的请求，跳过
            if (requestState.hasInflight()) {
                continue;
            }

            // 复制键集合以避免暴露底层的可变集合
            Set<K> copyKeys = Set.copyOf(keys);

            // 使用提供的函数构建新的请求
            Collection<AdminApiHandler.RequestAndKeys<K>> newRequests = buildRequest.apply(copyKeys, scope);
            if (newRequests.isEmpty()) {
                return;
            }

            // 只处理第一个请求，因为所有请求都会发送到同一个broker
            // 我们不希望同时向一个broker发送多个执行请求
            AdminApiHandler.RequestAndKeys<K> newRequest = newRequests.iterator().next();
            // 创建请求规范，包含请求的所有必要信息
            RequestSpec<K> spec = new RequestSpec<>(
                handler.apiName() + "(api=" + newRequest.request.apiKey() + ")",
                scope,
                newRequest.keys,
                newRequest.request,
                requestState.nextAllowedRetryMs,
                deadlineMs,
                requestState.tries
            );

            // 设置请求状态为正在处理
            requestState.setInflight(spec);
            // 添加到待发送请求列表
            requests.add(spec);
        }
    }

    /**
     * 收集查找阶段(Lookup)的请求，用于查找broker
     * 例如：查找消费者组的协调器或分区的leader
     * 
     * @param requests 存储待发送请求的列表
     */
    private void collectLookupRequests(List<RequestSpec<K>> requests) {
        // 调用通用的请求收集方法，处理查找阶段的请求
        // 使用lookupMap中的键构建FindCoordinator或元数据请求
        collectRequests(
            requests,
            lookupMap,
            // 为每组键构建单个查找请求
            (keys, scope) -> Collections.singletonList(new AdminApiHandler.RequestAndKeys<>(handler.lookupStrategy().buildRequest(keys), keys))
        );
    }

    /**
     * 收集执行阶段(Fulfillment)的请求，用于向已知的broker发送实际操作请求
     * 例如：向分区leader发送ListOffsets请求，或向组协调器发送OffsetFetch请求
     * 
     * @param requests 存储待发送请求的列表
     */
    private void collectFulfillmentRequests(List<RequestSpec<K>> requests) {
        // 调用通用的请求收集方法，处理执行阶段的请求
        // 使用fulfillmentMap中的键和对应的broker构建实际的操作请求
        collectRequests(
            requests,
            fulfillmentMap,
            // 为每组键构建发送到特定broker的请求
            (keys, scope) -> handler.buildRequest(scope.destinationBrokerId, keys)
        );
    }

    /**
     * 请求规范类，用于将需要发送的请求映射到KafkaAdminClient内部使用的Call实现
     * 这个类封装了请求的所有必要信息，包括请求名称、范围、键集合、请求构建器等
     * 
     * @param <K> 键类型，与外部类的键类型相同
     */
    public static class RequestSpec<K> {
        // 请求名称，通常是API名称和类型的组合
        public final String name;
        // 请求范围，可以是查找范围或执行范围
        public final ApiRequestScope scope;
        // 与请求关联的键集合
        public final Set<K> keys;
        // 请求构建器，用于创建实际的请求对象
        public final AbstractRequest.Builder<?> request;
        // 下次允许重试的时间戳
        public final long nextAllowedTryMs;
        // 请求的截止时间
        public final long deadlineMs;
        // 已尝试的次数
        public final int tries;

        public RequestSpec(
            String name,
            ApiRequestScope scope,
            Set<K> keys,
            AbstractRequest.Builder<?> request,
            long nextAllowedTryMs,
            long deadlineMs,
            int tries
        ) {
            this.name = name;
            this.scope = scope;
            this.keys = keys;
            this.request = request;
            this.nextAllowedTryMs = nextAllowedTryMs;
            this.deadlineMs = deadlineMs;
            this.tries = tries;
        }

        @Override
        public String toString() {
            return "RequestSpec(" +
                "name=" + name +
                ", scope=" + scope +
                ", keys=" + keys +
                ", request=" + request +
                ", nextAllowedTryMs=" + nextAllowedTryMs +
                ", deadlineMs=" + deadlineMs +
                ", tries=" + tries +
                ')';
        }
    }

    /**
     * Helper class used to track the request state within each request scope.
     * This class enforces a maximum number of inflight request and keeps track
     * of backoff/retry state.
     * 
     * 辅助类，用于在每个请求范围内跟踪请求状态。
     * 该类强制执行最大进行中请求数量的限制，并跟踪退避/重试状态。
     */
    private class RequestState {
        // 当前正在进行的请求，使用Optional包装以表示可能不存在的情况
        private Optional<RequestSpec<K>> inflightRequest = Optional.empty();
        // 当前请求的尝试次数
        private int tries = 0;
        // 下一次允许重试的时间戳(毫秒)
        private long nextAllowedRetryMs = 0;

        /**
         * 检查是否有正在进行的请求
         * 
         * @return 如果有正在进行的请求则返回true，否则返回false
         */
        boolean hasInflight() {
            // 通过检查Optional是否有值来判断是否有正在进行的请求
            return inflightRequest.isPresent();
        }

        /**
         * 清除当前正在进行的请求状态，并设置下一次允许重试的时间
         * 
         * @param currentTimeMs 当前时间戳(毫秒)
         */
        public void clearInflight(long currentTimeMs) {
            // 清除正在进行的请求
            this.inflightRequest = Optional.empty();
            // 设置下一次允许重试的时间为当前时间
            this.nextAllowedRetryMs = currentTimeMs;
        }

        /**
         * 清除当前正在进行的请求状态，并应用退避策略设置下一次允许重试的时间
         * 
         * @param currentTimeMs 当前时间戳(毫秒)
         */
        public void clearInflightAndBackoff(long currentTimeMs) {
            // 计算退避时间并清除请求状态
            // 注意：tries-1是为了在第一次失败时不增加退避时间
            clearInflight(currentTimeMs + retryBackoff.backoff(tries >= 1 ? tries - 1 : 0));
        }

        /**
         * 设置新的正在进行的请求，并增加尝试次数
         * 
         * @param spec 新的请求规范
         */
        public void setInflight(RequestSpec<K> spec) {
            // 设置新的正在进行的请求
            this.inflightRequest = Optional.of(spec);
            // 增加尝试次数
            this.tries++;
        }
    }

    /**
     * Completion of the Lookup stage results in a destination broker to send the
     * fulfillment request to. Each destination broker in the Fulfillment stage
     * gets its own request scope.
     * 
     * 查找阶段完成后，会得到一个目标broker用于发送执行请求。
     * 在执行阶段，每个目标broker都有自己的请求范围。
     */
    private static class FulfillmentScope implements ApiRequestScope {
        // 目标broker的ID，用于标识请求应该发送到哪个broker
        public final int destinationBrokerId;

        /**
         * 创建一个新的执行阶段请求范围
         * 
         * @param destinationBrokerId 目标broker的ID
         */
        private FulfillmentScope(int destinationBrokerId) {
            this.destinationBrokerId = destinationBrokerId;
        }

        /**
         * 获取目标broker的ID
         * 
         * @return 包装在OptionalInt中的目标broker ID
         */
        @Override
        public OptionalInt destinationBrokerId() {
            // 将broker ID包装在OptionalInt中返回
            return OptionalInt.of(destinationBrokerId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FulfillmentScope that = (FulfillmentScope) o;
            return destinationBrokerId == that.destinationBrokerId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(destinationBrokerId);
        }
    }

    /**
     * Helper class which maintains a bi-directional mapping from a key to a set of values.
     * Each value can map to one and only one key, but many values can be associated with
     * a single key.
     * 
     * 辅助类，维护从键到值集合的双向映射关系。
     * 每个值只能映射到一个键，但一个键可以关联多个值。
     *
     * @param <K> The key type (键类型)
     * @param <V> The value type (值类型)
     */
    private static class BiMultimap<K, V> {
        // 值到键的反向映射，用于快速查找值对应的键
        private final Map<V, K> reverseMap = new HashMap<>();
        // 键到值集合的映射，一个键可以对应多个值
        private final Map<K, Set<V>> map = new HashMap<>();

        /**
         * 添加键值对映射关系
         * 
         * @param key 键
         * @param value 值
         */
        void put(K key, V value) {
            // 先移除值的现有映射（如果存在）
            remove(value);
            // 在反向映射中添加值到键的映射
            reverseMap.put(value, key);
            // 在正向映射中添加键到值的映射
            // 如果键不存在，先创建一个新的值集合
            map.computeIfAbsent(key, k -> new HashSet<>()).add(value);
        }

        /**
         * 移除指定值的映射关系
         * 
         * @param value 要移除的值
         */
        void remove(V value) {
            // 从反向映射中移除值并获取对应的键
            K key = reverseMap.remove(value);
            if (key != null) {
                // 如果找到了键，从正向映射中移除值
                Set<V> set = map.get(key);
                if (set != null) {
                    set.remove(value);
                    // 如果键对应的值集合为空，移除整个键的映射
                    if (set.isEmpty()) {
                        map.remove(key);
                    }
                }
            }
        }

        /**
         * 获取值对应的键
         * 
         * @param value 要查找的值
         * @return 包装在Optional中的键，如果值不存在映射则返回空Optional
         */
        Optional<K> getKey(V value) {
            // 从反向映射中查找值对应的键，并包装在Optional中
            return Optional.ofNullable(reverseMap.get(value));
        }

        /**
         * 获取所有键值对映射关系
         * 
         * @return 键到值集合的映射条目集合
         */
        Set<Map.Entry<K, Set<V>>> entrySet() {
            // 返回正向映射的所有条目
            return map.entrySet();
        }
    }

}
