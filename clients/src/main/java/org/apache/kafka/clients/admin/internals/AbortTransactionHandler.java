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

import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

/**
 * 事务中止处理器，用于处理Kafka事务的中止操作
 * 继承自AdminApiHandler.Batched类，专门处理TopicPartition类型的批量请求，无返回值(Void)
 */
public class AbortTransactionHandler extends AdminApiHandler.Batched<TopicPartition, Void> {
    // 日志记录器
    private final Logger log;
    // 事务中止操作的规格说明，包含必要的事务信息（如producerId, epoch等）
    private final AbortTransactionSpec abortSpec;
    // 分区leader查找策略，用于定位分区的leader broker
    private final PartitionLeaderStrategy lookupStrategy;

    /**
     * 构造函数
     * @param abortSpec 事务中止操作的规格说明
     * @param logContext 日志上下文
     */
    public AbortTransactionHandler(
        AbortTransactionSpec abortSpec,
        LogContext logContext
    ) {
        // 初始化事务中止规格说明
        this.abortSpec = abortSpec;
        // 创建该类的日志记录器
        this.log = logContext.logger(AbortTransactionHandler.class);
        // 初始化分区leader查找策略
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
    }

    /**
     * 创建新的分区leader查找Future
     * @param topicPartitions 主题分区集合
     * @param partitionLeaderCache 分区leader缓存映射
     * @return 分区leader查找Future对象
     */
    public static PartitionLeaderStrategy.PartitionLeaderFuture<Void> newFuture(
        Set<TopicPartition> topicPartitions,
        Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(topicPartitions, partitionLeaderCache);
    }

    /**
     * 获取API名称
     * @return 返回"abortTransaction"作为API名称
     */
    @Override
    public String apiName() {
        return "abortTransaction";
    }

    /**
     * 获取分区leader查找策略
     * @return 返回当前使用的查找策略实例
     */
    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建批量事务标记请求
     * @param brokerId broker节点ID
     * @param topicPartitions 主题分区集合
     * @return 事务标记请求构建器
     */
    @Override
    public WriteTxnMarkersRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<TopicPartition> topicPartitions
    ) {
        // 验证主题分区集合是否符合预期
        validateTopicPartitions(topicPartitions);

        // 创建事务标记，设置协调器epoch、生产者epoch、生产者ID等信息
        // 设置transactionResult为false表示这是一个中止事务的标记
        WriteTxnMarkersRequestData.WritableTxnMarker marker = new WriteTxnMarkersRequestData.WritableTxnMarker()
            .setCoordinatorEpoch(abortSpec.coordinatorEpoch())
            .setProducerEpoch(abortSpec.producerEpoch())
            .setProducerId(abortSpec.producerId())
            .setTransactionResult(false);

        // 添加主题和分区信息到标记中
        marker.topics().add(new WriteTxnMarkersRequestData.WritableTxnMarkerTopic()
            .setName(abortSpec.topicPartition().topic())
            .setPartitionIndexes(singletonList(abortSpec.topicPartition().partition()))
        );

        // 创建请求数据对象并添加标记
        WriteTxnMarkersRequestData request = new WriteTxnMarkersRequestData();
        request.markers().add(marker);

        // 返回请求构建器
        return new WriteTxnMarkersRequest.Builder(request);
    }

    /**
     * 处理事务标记请求的响应
     * @param broker broker节点
     * @param topicPartitions 主题分区集合
     * @param abstractResponse 抽象响应对象
     * @return API调用结果
     */
    @Override
    public ApiResult<TopicPartition, Void> handleResponse(
        Node broker,
        Set<TopicPartition> topicPartitions,
        AbstractResponse abstractResponse
    ) {
        // 验证主题分区集合是否符合预期
        validateTopicPartitions(topicPartitions);

        // 转换响应类型并获取标记响应列表
        WriteTxnMarkersResponse response = (WriteTxnMarkersResponse) abstractResponse;
        List<WriteTxnMarkersResponseData.WritableTxnMarkerResult> markerResponses = response.data().markers();

        // 验证标记响应数量和生产者ID是否匹配
        if (markerResponses.size() != 1 || markerResponses.get(0).producerId() != abortSpec.producerId()) {
            return ApiResult.failed(abortSpec.topicPartition(), new KafkaException("WriteTxnMarkers response " +
                "included unexpected marker entries: " + markerResponses + "(expected to find exactly one " +
                "entry with producerId " + abortSpec.producerId() + ")"));
        }

        // 获取标记响应和主题响应列表
        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse = markerResponses.get(0);
        List<WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult> topicResponses = markerResponse.topics();

        // 验证主题响应数量和主题名称是否匹配
        if (topicResponses.size() != 1 || !topicResponses.get(0).name().equals(abortSpec.topicPartition().topic())) {
            return ApiResult.failed(abortSpec.topicPartition(), new KafkaException("WriteTxnMarkers response " +
                "included unexpected topic entries: " + markerResponses + "(expected to find exactly one " +
                "entry with topic partition " + abortSpec.topicPartition() + ")"));
        }

        // 获取主题响应和分区响应列表
        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse = topicResponses.get(0);
        List<WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult> partitionResponses =
            topicResponse.partitions();

        // 验证分区响应数量和分区索引是否匹配
        if (partitionResponses.size() != 1 || partitionResponses.get(0).partitionIndex() != abortSpec.topicPartition().partition()) {
            return ApiResult.failed(abortSpec.topicPartition(), new KafkaException("WriteTxnMarkers response " +
                "included unexpected partition entries for topic " + abortSpec.topicPartition().topic() +
                ": " + markerResponses + "(expected to find exactly one entry with partition " +
                abortSpec.topicPartition().partition() + ")"));
        }

        // 获取分区响应和错误码
        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse = partitionResponses.get(0);
        Errors error = Errors.forCode(partitionResponse.errorCode());

        // 根据错误码处理响应结果
        if (error != Errors.NONE) {
            return handleError(error);
        } else {
            return ApiResult.completed(abortSpec.topicPartition(), null);
        }
    }

    /**
     * 处理事务标记请求的错误响应
     * @param error 错误类型
     * @return API调用结果
     */
    private ApiResult<TopicPartition, Void> handleError(Errors error) {
        switch (error) {
            case CLUSTER_AUTHORIZATION_FAILED:
                // 集群授权失败，表示客户端没有足够的权限执行操作
                log.error("WriteTxnMarkers request for abort spec {} failed cluster authorization", abortSpec);
                return ApiResult.failed(abortSpec.topicPartition(), new ClusterAuthorizationException(
                    "WriteTxnMarkers request with " + abortSpec + " failed due to cluster " +
                        "authorization error"));

            case INVALID_PRODUCER_EPOCH:
                // 生产者epoch无效，可能是由于生产者已被关闭或重启
                log.error("WriteTxnMarkers request for abort spec {} failed due to an invalid producer epoch",
                    abortSpec);
                return ApiResult.failed(abortSpec.topicPartition(), new InvalidProducerEpochException(
                    "WriteTxnMarkers request with " + abortSpec + " failed due an invalid producer epoch"));

            case TRANSACTION_COORDINATOR_FENCED:
                // 事务协调器被篱笆隔离，表示有更新的协调器接管了该事务
                log.error("WriteTxnMarkers request for abort spec {} failed because the coordinator epoch is fenced",
                    abortSpec);
                return ApiResult.failed(abortSpec.topicPartition(), new TransactionCoordinatorFencedException(
                    "WriteTxnMarkers request with " + abortSpec + " failed since the provided " +
                        "coordinator epoch " + abortSpec.coordinatorEpoch() + " has been fenced " +
                        "by the active coordinator"));

            case NOT_LEADER_OR_FOLLOWER:
            case REPLICA_NOT_AVAILABLE:
            case BROKER_NOT_AVAILABLE:
            case UNKNOWN_TOPIC_OR_PARTITION:
                // 这些错误都是可重试的，通常是由于broker临时不可用或主题分区配置变更导致
                log.debug("WriteTxnMarkers request for abort spec {} failed due to {}. Will retry after attempting to " +
                        "find the leader again", abortSpec, error);
                return ApiResult.unmapped(singletonList(abortSpec.topicPartition()));

            default:
                // 处理未预期的错误
                log.error("WriteTxnMarkers request for abort spec {} failed due to an unexpected error {}",
                    abortSpec, error);
                return ApiResult.failed(abortSpec.topicPartition(), error.exception(
                    "WriteTxnMarkers request with " + abortSpec + " failed due to unexpected error: " + error.message()));
        }
    }

    /**
     * 验证主题分区集合是否符合预期
     * 由于每个事务中止操作只能处理一个主题分区，
     * 所以需要确保传入的主题分区集合只包含abortSpec中指定的分区
     * 
     * @param topicPartitions 待验证的主题分区集合
     * @throws IllegalArgumentException 如果主题分区集合不符合预期
     */
    private void validateTopicPartitions(Set<TopicPartition> topicPartitions) {
        if (!topicPartitions.equals(singleton(abortSpec.topicPartition()))) {
            throw new IllegalArgumentException("Received unexpected topic partitions " + topicPartitions +
                " (expected only " + singleton(abortSpec.topicPartition()) + ")");
        }
    }

}
