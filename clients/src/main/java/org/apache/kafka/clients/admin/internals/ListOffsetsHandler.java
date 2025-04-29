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

import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ListOffsetsHandler类用于处理获取主题分区偏移量的请求。
 * 该类继承自Batched类，支持批量处理TopicPartition的偏移量查询，并返回ListOffsetsResultInfo结果。
 * 主要用于Kafka管理客户端中查询分区的最早、最新或指定时间戳的偏移量。
 */
public final class ListOffsetsHandler extends Batched<TopicPartition, ListOffsetsResultInfo> {

    /**
     * 存储每个主题分区及其对应的时间戳映射
     * 时间戳可以是具体的时间点，也可以是特殊值如LATEST_TIMESTAMP(最新)、EARLIEST_TIMESTAMP(最早)等
     */
    private final Map<TopicPartition, Long> offsetTimestampsByPartition;
    
    /**
     * 偏移量查询的配置选项，包含隔离级别等设置
     */
    private final ListOffsetsOptions options;
    
    /**
     * 日志记录器
     */
    private final Logger log;
    
    /**
     * 分区leader查找策略，用于定位每个分区的leader broker
     */
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;
    
    /**
     * 默认的API超时时间(毫秒)
     */
    private final int defaultApiTimeoutMs;

    /**
     * 构造函数
     * @param offsetTimestampsByPartition 主题分区到时间戳的映射，用于指定要查询的偏移量位置
     * @param options 偏移量查询的配置选项
     * @param logContext 日志上下文
     * @param defaultApiTimeoutMs 默认的API超时时间
     */
    public ListOffsetsHandler(
        Map<TopicPartition, Long> offsetTimestampsByPartition,
        ListOffsetsOptions options,
        LogContext logContext,
        int defaultApiTimeoutMs
    ) {
        // 初始化各个字段
        this.offsetTimestampsByPartition = offsetTimestampsByPartition;
        this.options = options;
        this.log = logContext.logger(ListOffsetsHandler.class);
        // 创建分区leader查找策略，false表示不启用批处理
        this.lookupStrategy = new PartitionLeaderStrategy(logContext, false);
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
    }

    /**
     * 返回API的名称
     * @return 返回"listOffsets"，表示这是一个获取偏移量的API
     */
    @Override
    public String apiName() {
        return "listOffsets";
    }

    /**
     * 返回用于查找分区leader的策略
     * @return 返回配置的分区leader查找策略实例
     */
    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return this.lookupStrategy;
    }

    /**
     * 构建批量请求的ListOffsets请求
     * 
     * @param brokerId 目标broker的ID
     * @param keys 需要查询偏移量的主题分区集合
     * @return 返回构建好的ListOffsetsRequest.Builder实例
     */
    @Override
    ListOffsetsRequest.Builder buildBatchedRequest(int brokerId, Set<TopicPartition> keys) {
        // 按主题名称对分区进行分组，构建ListOffsetsTopic对象
        Map<String, ListOffsetsTopic> topicsByName = CollectionUtils.groupPartitionsByTopic(
            keys,
            // 为每个主题创建一个新的ListOffsetsTopic对象
            topicName -> new ListOffsetsTopic().setName(topicName),
            // 为每个分区添加对应的时间戳信息
            (listOffsetsTopic, partitionId) -> {
                TopicPartition topicPartition = new TopicPartition(listOffsetsTopic.name(), partitionId);
                long offsetTimestamp = offsetTimestampsByPartition.get(topicPartition);
                listOffsetsTopic.partitions().add(
                    new ListOffsetsPartition()
                        .setPartitionIndex(partitionId)
                        .setTimestamp(offsetTimestamp));
            });

        // 检查是否有分区需要查询最大时间戳的偏移量
        boolean supportsMaxTimestamp = keys
            .stream()
            .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.MAX_TIMESTAMP);

        // 检查是否有分区需要查询最早的本地时间戳
        boolean requireEarliestLocalTimestamp = keys
                .stream()
                .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP);

        // 检查是否有分区需要查询分层存储的最新时间戳
        boolean requireTieredStorageTimestamp = keys
            .stream()
            .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP);

        // 获取超时时间，如果未指定则使用默认值
        int timeoutMs = options.timeoutMs() != null ? options.timeoutMs() : defaultApiTimeoutMs;
        
        // 构建ListOffsetsRequest请求
        return ListOffsetsRequest.Builder.forConsumer(true,
                        options.isolationLevel(),
                        supportsMaxTimestamp,
                        requireEarliestLocalTimestamp,
                        requireTieredStorageTimestamp)
                .setTargetTimes(new ArrayList<>(topicsByName.values()))
                .setTimeoutMs(timeoutMs);
    }

    /**
     * 处理ListOffsets请求的响应
     * 
     * @param broker 发送请求的broker节点
     * @param keys 请求中包含的主题分区集合
     * @param abstractResponse 从broker收到的原始响应
     * @return 返回ApiResult，包含成功完成的、失败的和需要重新映射的分区信息
     */
    @Override
    public ApiResult<TopicPartition, ListOffsetsResultInfo> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        // 将抽象响应转换为具体的ListOffsetsResponse类型
        ListOffsetsResponse response = (ListOffsetsResponse) abstractResponse;
        // 存储成功获取偏移量的分区结果
        Map<TopicPartition, ListOffsetsResultInfo> completed = new HashMap<>();
        // 存储处理失败的分区及其异常
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        // 存储需要重新查找leader的分区
        List<TopicPartition> unmapped = new ArrayList<>();
        // 存储可以重试的分区
        Set<TopicPartition> retriable = new HashSet<>();

        // 遍历响应中的每个主题和分区
        for (ListOffsetsTopicResponse topic : response.topics()) {
            for (ListOffsetsPartitionResponse partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());
                // 检查响应中的分区是否在请求中
                if (!offsetTimestampsByPartition.containsKey(topicPartition)) {
                    log.warn("ListOffsets response includes unknown topic partition {}", topicPartition);
                } else if (error == Errors.NONE) {
                    // 如果没有错误，处理leader epoch信息
                    Optional<Integer> leaderEpoch = (partition.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                        ? Optional.empty()
                        : Optional.of(partition.leaderEpoch());
                    // 将成功的结果添加到completed map中
                    completed.put(
                        topicPartition,
                        new ListOffsetsResultInfo(partition.offset(), partition.timestamp(), leaderEpoch));
                } else {
                    // 处理分区级别的错误
                    handlePartitionError(topicPartition, error, failed, unmapped, retriable);
                }
            }
        }

        // 对未返回结果的分区进行完整性检查
        for (TopicPartition topicPartition : keys) {
            // 如果分区既不在完成列表中，也不在失败列表中，也不在重试列表中，且没有未映射的分区
            if (unmapped.isEmpty()
                && !completed.containsKey(topicPartition)
                && !failed.containsKey(topicPartition)
                && !retriable.contains(topicPartition)
            ) {
                // 创建完整性检查异常
                ApiException sanityCheckException = new ApiException(
                    "The response from broker " + broker.id() +
                        " did not contain a result for topic partition " + topicPartition);
                log.error(
                    "ListOffsets request for topic partition {} failed sanity check",
                    topicPartition,
                    sanityCheckException);
                failed.put(topicPartition, sanityCheckException);
            }
        }

        // 返回包含所有处理结果的ApiResult
        return new ApiResult<>(completed, failed, unmapped);
    }

    /**
     * 处理分区级别的错误
     * 
     * @param topicPartition 发生错误的主题分区
     * @param error 错误类型
     * @param failed 存储失败的分区及其异常
     * @param unmapped 存储需要重新查找leader的分区
     * @param retriable 存储可以重试的分区
     */
    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors error,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped,
        Set<TopicPartition> retriable
    ) {
        // 处理leader相关的错误，这些错误需要重新查找分区leader
        if (error == Errors.NOT_LEADER_OR_FOLLOWER || error == Errors.LEADER_NOT_AVAILABLE) {
            log.debug(
                "ListOffsets lookup request for topic partition {} will be retried due to invalid leader metadata {}",
                topicPartition,
                error);
            unmapped.add(topicPartition);
        } 
        // 处理可重试的错误
        else if (error.exception() instanceof RetriableException) {
            log.debug(
                "ListOffsets fulfillment request for topic partition {} will be retried due to {}",
                topicPartition,
                error);
            retriable.add(topicPartition);
        } 
        // 处理不可恢复的错误
        else {
            log.error(
                "ListOffsets request for topic partition {} failed due to an unexpected error {}",
                topicPartition,
                error);
            failed.put(topicPartition, error.exception());
        }
    }

    /**
     * 处理不支持的API版本异常
     * 主要处理broker不支持MAX_TIMESTAMP偏移量规范的情况
     * 
     * @param brokerId 不支持该功能的broker ID
     * @param exception 不支持版本的异常
     * @param keys 请求中的主题分区集合
     * @return 返回需要失败处理的分区及其对应的异常映射
     */
    @Override
    public Map<TopicPartition, Throwable> handleUnsupportedVersionException(
        int brokerId, UnsupportedVersionException exception, Set<TopicPartition> keys
    ) {
        // 记录警告日志
        log.warn("Broker " + brokerId + " does not support MAX_TIMESTAMP offset specs");
        
        // 收集使用了MAX_TIMESTAMP的分区
        Map<TopicPartition, Throwable> maxTimestampPartitions = new HashMap<>();
        for (TopicPartition topicPartition : keys) {
            Long offsetTimestamp = offsetTimestampsByPartition.get(topicPartition);
            if (offsetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                maxTimestampPartitions.put(topicPartition, exception);
            }
        }

        // 如果没有使用MAX_TIMESTAMP的分区，则所有分区都应该失败
        // 否则，只有使用了MAX_TIMESTAMP的分区需要失败，其他分区可以在后续阶段重试
        if (maxTimestampPartitions.isEmpty()) {
            return keys.stream().collect(Collectors.toMap(k -> k, k -> exception));
        } else {
            return maxTimestampPartitions;
        }
    }

    /**
     * 创建一个新的PartitionLeaderFuture实例
     * 用于异步处理分区leader的查找和偏移量获取
     * 
     * @param topicPartitions 需要处理的主题分区集合
     * @param partitionLeaderCache 分区leader的缓存映射
     * @return 返回一个新的PartitionLeaderFuture实例
     */
    public static PartitionLeaderStrategy.PartitionLeaderFuture<ListOffsetsResultInfo> newFuture(
        Collection<TopicPartition> topicPartitions,
        Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(new HashSet<>(topicPartitions), partitionLeaderCache);
    }
}
