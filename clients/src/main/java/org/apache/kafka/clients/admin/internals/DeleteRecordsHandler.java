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

import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DeleteRecordsHandler类负责处理删除记录的请求，继承自Batched抽象类
 * 该类实现了批量删除Kafka主题分区中的记录，支持多分区并发操作
 * 应用场景：
 * 1. 清理过期数据
 * 2. 按时间或偏移量删除历史消息
 * 3. 管理磁盘空间使用
 */
public final class DeleteRecordsHandler extends Batched<TopicPartition, DeletedRecords> {

    /**
     * 存储每个主题分区及其对应的待删除记录信息
     * key: 主题分区
     * value: 删除记录的具体要求(如偏移量范围)
     */
    private final Map<TopicPartition, RecordsToDelete> recordsToDelete;
    
    /**
     * 日志记录器，用于记录操作和错误信息
     */
    private final Logger log;
    
    /**
     * 分区leader查找策略，用于定位每个分区的leader broker
     */
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    /**
     * 请求超时时间(毫秒)
     */
    private final int timeout;

    /**
     * 构造函数
     * @param recordsToDelete 待删除记录的映射表
     * @param logContext 日志上下文
     * @param timeout 超时时间(毫秒)
     */
    public DeleteRecordsHandler(
            Map<TopicPartition, RecordsToDelete> recordsToDelete,
            LogContext logContext, int timeout
    ) {
        // 初始化待删除记录映射表
        this.recordsToDelete = recordsToDelete;
        // 创建日志记录器
        this.log = logContext.logger(DeleteRecordsHandler.class);
        // 初始化分区leader查找策略
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
        // 设置超时时间
        this.timeout = timeout;
    }

    /**
     * 返回API名称
     * @return 返回"deleteRecords"，表示这是删除记录的API
     */
    @Override
    public String apiName() {
        return "deleteRecords";
    }

    /**
     * 返回分区leader查找策略
     * @return 返回用于查找分区leader的策略对象
     */
    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return this.lookupStrategy;
    }

    /**
     * 创建新的分区leader Future对象
     * @param topicPartitions 主题分区集合
     * @param partitionLeaderCache 分区leader缓存映射表
     * @return 返回处理删除记录请求的Future对象
     */
    public static PartitionLeaderStrategy.PartitionLeaderFuture<DeletedRecords> newFuture(
            Collection<TopicPartition> topicPartitions,
            Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(new HashSet<>(topicPartitions), partitionLeaderCache);
    }

    /**
     * 构建批量删除记录请求
     * @param brokerId broker节点ID
     * @param keys 要处理的主题分区集合
     * @return 返回删除记录请求的构建器
     */
    @Override
    public DeleteRecordsRequest.Builder buildBatchedRequest(int brokerId, Set<TopicPartition> keys) {
        // 创建每个主题的删除请求数据映射
        Map<String, DeleteRecordsRequestData.DeleteRecordsTopic> deletionsForTopic = new HashMap<>();
        
        // 遍历所有要处理的主题分区
        for (TopicPartition topicPartition : keys) {
            // 获取该分区的删除记录要求
            RecordsToDelete toDelete = recordsToDelete.get(topicPartition);
            // 获取或创建主题的删除请求数据
            DeleteRecordsRequestData.DeleteRecordsTopic deleteRecords = deletionsForTopic.computeIfAbsent(
                    topicPartition.topic(),
                    key -> new DeleteRecordsRequestData.DeleteRecordsTopic().setName(topicPartition.topic())
            );
            // 添加分区的删除请求数据
            deleteRecords.partitions().add(new DeleteRecordsRequestData.DeleteRecordsPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setOffset(toDelete.beforeOffset()));
        }

        // 创建删除记录请求数据对象
        DeleteRecordsRequestData data = new DeleteRecordsRequestData()
                .setTopics(new ArrayList<>(deletionsForTopic.values()))
                .setTimeoutMs(timeout);
        return new DeleteRecordsRequest.Builder(data);
    }


    /**
     * 处理删除记录请求的响应
     * @param broker 处理请求的broker节点
     * @param keys 请求中包含的主题分区集合
     * @param abstractResponse broker返回的原始响应
     * @return 返回API处理结果，包含成功、失败和需要重新映射的分区信息
     */
    @Override
    public ApiResult<TopicPartition, DeletedRecords> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        // 将抽象响应转换为具体的删除记录响应
        DeleteRecordsResponse response = (DeleteRecordsResponse) abstractResponse;
        // 存储处理成功的分区及其删除结果
        Map<TopicPartition, DeletedRecords> completed = new HashMap<>();
        // 存储处理失败的分区及其异常信息
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        // 存储需要重新查找leader的分区
        List<TopicPartition> unmapped = new ArrayList<>();
        // 存储可以重试的分区
        Set<TopicPartition> retriable = new HashSet<>();

        // 遍历响应中的每个主题的结果
        for (DeleteRecordsResponseData.DeleteRecordsTopicResult topicResult: response.data().topics()) {
            // 遍历每个主题中的分区结果
            for (DeleteRecordsResponseData.DeleteRecordsPartitionResult partitionResult : topicResult.partitions()) {
                // 获取错误码对应的错误类型
                Errors error = Errors.forCode(partitionResult.errorCode());
                // 构建主题分区对象
                TopicPartition topicPartition = new TopicPartition(topicResult.name(), partitionResult.partitionIndex());
                if (error == Errors.NONE) {
                    // 如果没有错误，将分区添加到成功列表，并记录新的低水位标记
                    completed.put(topicPartition, new DeletedRecords(partitionResult.lowWatermark()));
                } else {
                    // 如果有错误，交给错误处理方法处理
                    handlePartitionError(topicPartition, error, failed, unmapped, retriable);
                }
            }
        }

        // 完整性检查：确保所有请求的分区都收到了响应
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
                // 记录错误日志
                log.error(
                        "DeleteRecords request for topic partition {} failed sanity check",
                        topicPartition,
                        sanityCheckException);
                // 将分区添加到失败列表
                failed.put(topicPartition, sanityCheckException);
            }
        }

        // 返回API结果，包含成功、失败和需要重新映射的分区信息
        return new ApiResult<>(completed, failed, unmapped);
    }

    /**
     * 处理分区级别的错误
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
        if (error.exception() instanceof InvalidMetadataException) {
            // 如果是元数据无效错误(如leader发生变更)，将分区加入未映射列表，等待重新查找leader
            log.debug(
                "DeleteRecords lookup request for topic partition {} will be retried due to invalid leader metadata {}",
                 topicPartition,
                 error);
            unmapped.add(topicPartition);
        } else if (error.exception() instanceof RetriableException) {
            // 如果是可重试的错误(如临时网络问题)，将分区加入重试列表
            log.debug(
                "DeleteRecords fulfillment request for topic partition {} will be retried due to {}",
                topicPartition,
                error);
            retriable.add(topicPartition);
        } else if (error.exception() instanceof TopicAuthorizationException) {
            // 如果是权限错误，记录错误并将分区加入失败列表
            log.error(
                "DeleteRecords request for topic partition {} failed due to an error {}",
                topicPartition,
                error);
            failed.put(topicPartition, error.exception());
        } else {
            // 对于其他未预期的错误，记录错误并将分区加入失败列表
            log.error(
                "DeleteRecords request for topic partition {} failed due to an unexpected error {}",
                topicPartition,
                error);
            failed.put(topicPartition, error.exception());
        }
    }
}
