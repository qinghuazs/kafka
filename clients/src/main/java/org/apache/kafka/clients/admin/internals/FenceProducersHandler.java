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

import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * FenceProducersHandler类用于处理隔离生产者的请求。
 * 该类继承自AdminApiHandler.Unbatched，专门处理CoordinatorKey到ProducerIdAndEpoch的非批量操作。
 * 
 * 应用场景：
 * 1. 事务管理 - 通过隔离生产者来防止事务冲突和数据不一致
 * 2. 故障恢复 - 在生产者崩溃或网络分区时隔离旧的生产者实例
 * 3. 生产者迁移 - 在需要迁移生产者时安全地停止旧实例
 */
public class FenceProducersHandler extends AdminApiHandler.Unbatched<CoordinatorKey, ProducerIdAndEpoch> {
    // 日志记录器
    private final Logger log;
    // 用于查找事务协调器的策略
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;
    // 事务超时时间(毫秒)
    private final int txnTimeoutMs;

    /**
     * 构造函数
     * @param options 隔离生产者的配置选项，可以指定事务超时时间
     * @param logContext 日志上下文
     * @param requestTimeoutMs 请求超时时间，当options中未指定事务超时时间时使用该值
     */
    public FenceProducersHandler(
        FenceProducersOptions options,
        LogContext logContext,
        int requestTimeoutMs
    ) {
        // 初始化日志记录器
        this.log = logContext.logger(FenceProducersHandler.class);
        // 创建事务协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(FindCoordinatorRequest.CoordinatorType.TRANSACTION, logContext);
        // 设置事务超时时间，优先使用options中的设置
        this.txnTimeoutMs = options.timeoutMs() != null ? options.timeoutMs() : requestTimeoutMs;
    }

    /**
     * 创建新的AdminApiFuture实例
     * @param transactionalIds 需要隔离的事务ID集合
     * @return 返回一个新的SimpleAdminApiFuture实例
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ProducerIdAndEpoch> newFuture(
        Collection<String> transactionalIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(transactionalIds));
    }

    /**
     * 构建事务ID对应的CoordinatorKey集合
     * @param transactionalIds 事务ID集合
     * @return 返回CoordinatorKey集合
     */
    private static Set<CoordinatorKey> buildKeySet(Collection<String> transactionalIds) {
        // 将事务ID转换为CoordinatorKey
        return transactionalIds.stream()
            .map(CoordinatorKey::byTransactionalId)
            .collect(Collectors.toSet());
    }

    /**
     * 获取API名称
     * @return 返回"fenceProducer"作为API名称
     */
    @Override
    public String apiName() {
        return "fenceProducer";
    }

    /**
     * 获取协调器查找策略
     * @return 返回用于查找事务协调器的策略
     */
    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建单个InitProducerId请求
     * 该请求用于隔离指定事务ID的生产者，通过不带producerId和epoch的请求触发协调器生成新的epoch
     *
     * @param brokerId broker节点ID
     * @param key 包含事务ID的协调器键
     * @return 返回InitProducerId请求的构建器
     * @throws IllegalArgumentException 当key类型不是TRANSACTION时抛出
     */
    @Override
    InitProducerIdRequest.Builder buildSingleRequest(int brokerId, CoordinatorKey key) {
        // 验证key类型必须是TRANSACTION
        if (key.type != FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `InitProducerId` request");
        }

        // 创建请求数据对象
        InitProducerIdRequestData data = new InitProducerIdRequestData()
            // 不设置producer epoch和ID，这样可以触发协调器生成新的epoch
            // 这意味着某些错误（如PRODUCER_FENCED）永远不会在broker响应中返回
            // 如果将来修改此逻辑以包含epoch或producer ID，需要更新错误处理逻辑
            .setProducerEpoch(ProducerIdAndEpoch.NONE.epoch)
            .setProducerId(ProducerIdAndEpoch.NONE.producerId)
            .setTransactionalId(key.idValue)
            // 设置事务超时时间，协调器使用此超时时间将新的producer epoch记录追加到事务日志
            .setTransactionTimeoutMs(txnTimeoutMs);

        return new InitProducerIdRequest.Builder(data);
    }

    /**
     * 处理单个InitProducerId请求的响应
     * 解析响应中的producerId和epoch，或处理可能的错误
     *
     * @param broker 响应来自的broker节点
     * @param key 请求使用的协调器键
     * @param abstractResponse broker的响应
     * @return 返回API调用结果，包含成功完成的请求、失败的请求和需要重新映射的请求
     */
    @Override
    public ApiResult<CoordinatorKey, ProducerIdAndEpoch> handleSingleResponse(
        Node broker,
        CoordinatorKey key,
        AbstractResponse abstractResponse
    ) {
        // 将抽象响应转换为具体的InitProducerId响应
        InitProducerIdResponse response = (InitProducerIdResponse) abstractResponse;

        // 获取错误码并检查是否有错误
        Errors error = Errors.forCode(response.data().errorCode());
        if (error != Errors.NONE) {
            return handleError(key, error);
        }

        // 创建成功结果，包含新的producerId和epoch
        Map<CoordinatorKey, ProducerIdAndEpoch> completed = Collections.singletonMap(key, new ProducerIdAndEpoch(
            response.data().producerId(),
            response.data().producerEpoch()
        ));

        return new ApiResult<>(completed, Collections.emptyMap(), Collections.emptyList());
    }

    /**
     * 处理InitProducerId请求的错误响应
     * 根据不同的错误类型采取相应的处理策略：
     * 1. 权限错误 - 返回失败结果
     * 2. 临时错误 - 返回空结果以便重试
     * 3. 协调器错误 - 重新查找协调器
     * 4. 其他错误 - 作为意外错误处理
     *
     * @param transactionalIdKey 事务ID对应的协调器键
     * @param error 错误类型
     * @return 返回相应的API调用结果
     */
    private ApiResult<CoordinatorKey, ProducerIdAndEpoch> handleError(
        CoordinatorKey transactionalIdKey,
        Errors error
    ) {
        switch (error) {
            case CLUSTER_AUTHORIZATION_FAILED:
                // 集群级别的授权失败，表示客户端没有操作集群的权限
                return ApiResult.failed(transactionalIdKey, new ClusterAuthorizationException(
                        "InitProducerId request for transactionalId `" + transactionalIdKey.idValue + "` " +
                                "failed due to cluster authorization failure"));

            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                // 事务ID级别的授权失败，表示客户端没有操作该事务ID的权限
                return ApiResult.failed(transactionalIdKey, new TransactionalIdAuthorizationException(
                        "InitProducerId request for transactionalId `" + transactionalIdKey.idValue + "` " +
                                "failed due to transactional ID authorization failure"));

            case COORDINATOR_LOAD_IN_PROGRESS:
                // 协调器正在加载状态，这是临时错误，需要重试
                log.debug("InitProducerId request for transactionalId `{}` failed because the " +
                                "coordinator is still in the process of loading state. Will retry",
                        transactionalIdKey.idValue);
                return ApiResult.empty();

            case CONCURRENT_TRANSACTIONS:
                // 存在并发的事务操作，这是临时错误，需要重试
                log.debug("InitProducerId request for transactionalId `{}` failed because of " +
                                "a concurrent transaction. Will retry", transactionalIdKey.idValue);
                return ApiResult.empty();

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                // 协调器不可用或发生了协调器变更
                // 需要重新查找协调器并重试请求
                log.debug("InitProducerId request for transactionalId `{}` returned error {}. Will attempt " +
                        "to find the coordinator again and retry", transactionalIdKey.idValue, error);
                return ApiResult.unmapped(Collections.singletonList(transactionalIdKey));

            // 我们故意省略了PRODUCER_FENCED、TRANSACTIONAL_ID_NOT_FOUND和INVALID_PRODUCER_EPOCH的处理
            // 因为当InitProducerIdRequest不包含producer epoch或ID时，这些错误永远不应该发生
            // 如果发生，应该作为意外错误处理

            default:
                // 处理所有其他意外错误
                return ApiResult.failed(transactionalIdKey, error.exception("InitProducerId request for " +
                        "transactionalId `" + transactionalIdKey.idValue + "` failed due to unexpected error"));
        }
    }
}
