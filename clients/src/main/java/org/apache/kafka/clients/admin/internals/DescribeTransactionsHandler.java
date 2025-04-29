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

import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 事务描述处理器类，用于处理描述Kafka事务状态的请求
 * 该处理器继承自AdminApiHandler.Batched，专门处理批量事务描述请求
 * 主要功能包括：
 * 1. 构建事务描述请求
 * 2. 处理事务描述响应
 * 3. 处理各种错误情况
 */
public class DescribeTransactionsHandler extends AdminApiHandler.Batched<CoordinatorKey, TransactionDescription> {
    // 日志记录器
    private final Logger log;
    // 事务协调器查找策略
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    /**
     * 构造函数
     * @param logContext 日志上下文，用于创建日志记录器
     */
    public DescribeTransactionsHandler(
        LogContext logContext
    ) {
        // 初始化日志记录器
        this.log = logContext.logger(DescribeTransactionsHandler.class);
        // 创建事务协调器查找策略实例
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.TRANSACTION, logContext);
    }

    /**
     * 创建新的AdminApiFuture实例
     * @param transactionalIds 要查询的事务ID集合
     * @return 返回一个新的AdminApiFuture实例，用于异步获取事务描述结果
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, TransactionDescription> newFuture(
        Collection<String> transactionalIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(transactionalIds));
    }

    /**
     * 将事务ID集合转换为CoordinatorKey集合
     * @param transactionalIds 事务ID集合
     * @return 返回对应的CoordinatorKey集合
     */
    private static Set<CoordinatorKey> buildKeySet(Collection<String> transactionalIds) {
        return transactionalIds.stream()
            .map(CoordinatorKey::byTransactionalId)
            .collect(Collectors.toSet());
    }

    @Override
    public String apiName() {
        return "describeTransactions";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建批量事务描述请求
     * @param brokerId 目标broker的ID
     * @param keys 要查询的事务协调器键集合
     * @return 返回事务描述请求构建器
     */
    @Override
    public DescribeTransactionsRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<CoordinatorKey> keys
    ) {
        // 创建请求数据对象
        DescribeTransactionsRequestData request = new DescribeTransactionsRequestData();
        // 将CoordinatorKey集合转换为事务ID列表
        List<String> transactionalIds = keys.stream().map(key -> {
            // 验证key类型是否为TRANSACTION
            if (key.type != FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeTransaction` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        // 设置事务ID列表到请求中
        request.setTransactionalIds(transactionalIds);
        return new DescribeTransactionsRequest.Builder(request);
    }

    /**
     * 处理事务描述响应
     * @param broker 响应来源的broker节点
     * @param keys 请求的事务协调器键集合
     * @param abstractResponse 原始响应对象
     * @return 返回API处理结果，包含成功、失败和未映射的事务信息
     */
    @Override
    public ApiResult<CoordinatorKey, TransactionDescription> handleResponse(
        Node broker,
        Set<CoordinatorKey> keys,
        AbstractResponse abstractResponse
    ) {
        // 转换响应类型
        DescribeTransactionsResponse response = (DescribeTransactionsResponse) abstractResponse;
        // 初始化结果容器
        Map<CoordinatorKey, TransactionDescription> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();

        // 遍历响应中的所有事务状态
        for (DescribeTransactionsResponseData.TransactionState transactionState : response.data().transactionStates()) {
            // 根据事务ID创建协调器键
            CoordinatorKey transactionalIdKey = CoordinatorKey.byTransactionalId(
                transactionState.transactionalId());
            // 验证是否是请求的事务ID
            if (!keys.contains(transactionalIdKey)) {
                log.warn("Response included transactionalId `{}`, which was not requested",
                    transactionState.transactionalId());
                continue;
            }

            // 获取错误码
            Errors error = Errors.forCode(transactionState.errorCode());
            if (error != Errors.NONE) {
                // 处理错误情况
                handleError(transactionalIdKey, error, failed, unmapped);
                continue;
            }

            // 处理事务开始时间
            OptionalLong transactionStartTimeMs = transactionState.transactionStartTimeMs() < 0 ?
                OptionalLong.empty() :
                OptionalLong.of(transactionState.transactionStartTimeMs());

            // 创建事务描述对象并添加到完成列表
            completed.put(transactionalIdKey, new TransactionDescription(
                broker.id(),
                TransactionState.parse(transactionState.transactionState()),
                transactionState.producerId(),
                transactionState.producerEpoch(),
                transactionState.transactionTimeoutMs(),
                transactionStartTimeMs,
                collectTopicPartitions(transactionState)
            ));
        }

        return new ApiResult<>(completed, failed, unmapped);
    }

    /**
     * 收集事务涉及的主题分区信息
     * @param transactionState 事务状态数据
     * @return 返回事务涉及的主题分区集合
     */
    private Set<TopicPartition> collectTopicPartitions(
        DescribeTransactionsResponseData.TransactionState transactionState
    ) {
        Set<TopicPartition> res = new HashSet<>();
        // 遍历事务涉及的所有主题
        for (DescribeTransactionsResponseData.TopicData topicData : transactionState.topics()) {
            String topic = topicData.topic();
            // 遍历主题下的所有分区
            for (Integer partitionId : topicData.partitions()) {
                res.add(new TopicPartition(topic, partitionId));
            }
        }
        return res;
    }

    /**
     * 处理事务描述请求中的各种错误情况
     * @param transactionalIdKey 事务ID对应的协调器键
     * @param error 错误类型
     * @param failed 失败事务的映射
     * @param unmapped 需要重新查找协调器的事务列表
     */
    private void handleError(
        CoordinatorKey transactionalIdKey,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> unmapped
    ) {
        switch (error) {
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                // 处理事务ID授权失败错误
                failed.put(transactionalIdKey, new TransactionalIdAuthorizationException(
                    "DescribeTransactions request for transactionalId `" + transactionalIdKey.idValue + "` " +
                        "failed due to authorization failure"));
                break;

            case TRANSACTIONAL_ID_NOT_FOUND:
                // 处理事务ID不存在错误
                failed.put(transactionalIdKey, new TransactionalIdNotFoundException(
                    "DescribeTransactions request for transactionalId `" + transactionalIdKey.idValue + "` " +
                        "failed because the ID could not be found"));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // 处理协调器正在加载状态的情况，需要重试
                log.debug("DescribeTransactions request for transactionalId `{}` failed because the " +
                        "coordinator is still in the process of loading state. Will retry",
                    transactionalIdKey.idValue);
                break;

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                // 处理协调器不可用或发生变更的情况，需要重新查找协调器
                unmapped.add(transactionalIdKey);
                log.debug("DescribeTransactions request for transactionalId `{}` returned error {}. Will attempt " +
                        "to find the coordinator again and retry", transactionalIdKey.idValue, error);
                break;

            default:
                // 处理其他未预期的错误
                failed.put(transactionalIdKey, error.exception("DescribeTransactions request for " +
                    "transactionalId `" + transactionalIdKey.idValue + "` failed due to unexpected error"));
        }
    }

}
