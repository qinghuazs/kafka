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

import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 事务列表处理器类，用于处理获取Kafka事务列表的请求
 * 该处理器继承自AdminApiHandler.Batched，专门处理批量事务列表请求
 * 主要功能包括：
 * 1. 构建事务列表请求，支持按生产者ID、事务状态和持续时间进行过滤
 * 2. 处理事务列表响应，包括处理各种错误情况
 * 3. 与事务协调器进行交互，确保请求的正确路由
 */
public class ListTransactionsHandler extends AdminApiHandler.Batched<AllBrokersStrategy.BrokerKey, Collection<TransactionListing>> {
    // 日志记录器，用于记录处理过程中的重要信息
    private final Logger log;
    // 列表事务的选项配置，包含过滤条件等
    private final ListTransactionsOptions options;
    // 查找策略，用于定位所有broker节点
    private final AllBrokersStrategy lookupStrategy;

    /**
     * 构造函数
     * @param options 列表事务的选项配置，用于指定过滤条件
     * @param logContext 日志上下文，用于创建日志记录器
     */
    public ListTransactionsHandler(
        ListTransactionsOptions options,
        LogContext logContext
    ) {
        // 初始化配置选项
        this.options = options;
        // 创建日志记录器实例
        this.log = logContext.logger(ListTransactionsHandler.class);
        // 创建所有broker的查找策略实例
        this.lookupStrategy = new AllBrokersStrategy(logContext);
    }

    /**
     * 创建新的AllBrokersFuture实例，用于异步获取事务列表结果
     * @return 返回一个新的AllBrokersFuture实例
     */
    public static AllBrokersStrategy.AllBrokersFuture<Collection<TransactionListing>> newFuture() {
        return new AllBrokersStrategy.AllBrokersFuture<>();
    }

    /**
     * 获取API名称
     * @return 返回API名称"listTransactions"
     */
    @Override
    public String apiName() {
        return "listTransactions";
    }

    /**
     * 获取查找策略
     * @return 返回用于查找broker的策略实例
     */
    @Override
    public AdminApiLookupStrategy<AllBrokersStrategy.BrokerKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建批量事务列表请求
     * @param brokerId 目标broker的ID
     * @param keys 要查询的broker键集合
     * @return 返回事务列表请求构建器
     */
    @Override
    public ListTransactionsRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<AllBrokersStrategy.BrokerKey> keys
    ) {
        // 创建请求数据对象
        ListTransactionsRequestData request = new ListTransactionsRequestData();
        // 设置生产者ID过滤器
        request.setProducerIdFilters(new ArrayList<>(options.filteredProducerIds()));
        // 设置事务状态过滤器，将TransactionState枚举转换为字符串
        request.setStateFilters(options.filteredStates().stream()
            .map(TransactionState::toString)
            .collect(Collectors.toList()));
        // 设置持续时间过滤器
        request.setDurationFilter(options.filteredDuration());
        return new ListTransactionsRequest.Builder(request);
    }

    /**
     * 处理事务列表响应
     * @param broker 响应来源的broker节点
     * @param keys 请求的broker键集合
     * @param abstractResponse 原始响应对象
     * @return 返回API处理结果，包含成功、失败和未映射的事务信息
     */
    @Override
    public ApiResult<AllBrokersStrategy.BrokerKey, Collection<TransactionListing>> handleResponse(
        Node broker,
        Set<AllBrokersStrategy.BrokerKey> keys,
        AbstractResponse abstractResponse
    ) {
        // 获取broker ID
        int brokerId = broker.id();
        // 验证并获取单个broker键
        AllBrokersStrategy.BrokerKey key = requireSingleton(keys, brokerId);

        // 转换响应类型并获取错误码
        ListTransactionsResponse response = (ListTransactionsResponse) abstractResponse;
        Errors error = Errors.forCode(response.data().errorCode());

        // 处理各种错误情况
        if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
            // 协调器正在加载状态，需要稍后重试
            log.debug("The `ListTransactions` request sent to broker {} failed because the " +
                "coordinator is still loading state. Will try again after backing off", brokerId);
            return ApiResult.empty();
        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE) {
            // 协调器不可用，可能正在关闭
            log.debug("The `ListTransactions` request sent to broker {} failed because the " +
                "coordinator is shutting down", brokerId);
            return ApiResult.failed(key, new CoordinatorNotAvailableException("ListTransactions " +
                "request sent to broker " + brokerId + " failed because the coordinator is shutting down"));
        } else if (error != Errors.NONE) {
            // 其他未预期的错误
            log.error("The `ListTransactions` request sent to broker {} failed because of an " +
                "unexpected error {}", brokerId, error);
            return ApiResult.failed(key, error.exception("ListTransactions request " +
                "sent to broker " + brokerId + " failed with an unexpected exception"));
        } else {
            // 成功处理响应，转换事务状态为TransactionListing对象
            List<TransactionListing> listings = response.data().transactionStates().stream()
                .map(transactionState -> new TransactionListing(
                    transactionState.transactionalId(),
                    transactionState.producerId(),
                    TransactionState.parse(transactionState.transactionState())))
                .collect(Collectors.toList());
            return ApiResult.completed(key, listings);
        }
    }

    /**
     * 验证broker键集合中只包含单个键，并且该键与指定的broker ID匹配
     * @param keys broker键集合
     * @param brokerId 期望的broker ID
     * @return 返回验证通过的broker键
     * @throws IllegalArgumentException 当验证失败时抛出异常
     */
    private AllBrokersStrategy.BrokerKey requireSingleton(
        Set<AllBrokersStrategy.BrokerKey> keys,
        int brokerId
    ) {
        // 验证集合大小为1
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected key set: " + keys);
        }

        // 获取并验证broker键
        AllBrokersStrategy.BrokerKey key = keys.iterator().next();
        if (key.brokerId.isEmpty() || key.brokerId.getAsInt() != brokerId) {
            throw new IllegalArgumentException("Unexpected broker key: " + key);
        }

        return key;
    }

}
