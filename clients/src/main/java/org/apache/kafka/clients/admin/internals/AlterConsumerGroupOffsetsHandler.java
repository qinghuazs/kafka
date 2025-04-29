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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * 消费者组偏移量修改处理器
 * 该类负责处理修改消费者组偏移量的请求，通过与Kafka协调器(Coordinator)交互来完成偏移量的提交操作
 * 继承自AdminApiHandler.Batched，支持批量处理请求，使用CoordinatorKey作为键，返回每个分区的错误信息
 */
public class AlterConsumerGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, Errors>> {

    /**
     * 消费者组ID的协调器键
     * 用于标识和定位目标消费者组
     */
    private final CoordinatorKey groupId;

    /**
     * 待修改的主题分区偏移量信息映射
     * 键为主题分区，值为包含偏移量和元数据的对象
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 协调器查找策略
     * 用于查找和管理消费者组的协调器节点
     */
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    /**
     * 构造函数
     * @param groupId 消费者组ID
     * @param offsets 待修改的主题分区偏移量映射
     * @param logContext 日志上下文
     */
    public AlterConsumerGroupOffsetsHandler(
        String groupId,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        LogContext logContext
    ) {
        // 创建消费者组的协调器键
        this.groupId = CoordinatorKey.byGroupId(groupId);
        // 保存待修改的偏移量信息
        this.offsets = offsets;
        // 初始化日志记录器
        this.log = logContext.logger(AlterConsumerGroupOffsetsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 获取API名称
     * @return 返回"offsetCommit"，表示这是一个偏移量提交操作
     */
    @Override
    public String apiName() {
        return "offsetCommit";
    }

    /**
     * 获取协调器查找策略
     * @return 返回用于查找消费者组协调器的策略对象
     */
    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 创建新的Future对象
     * 用于异步处理偏移量修改请求
     * @param groupId 消费者组ID
     * @return 返回一个AdminApiFuture对象，用于跟踪请求的执行结果
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> newFuture(
        String groupId
    ) {
        // 为指定的消费者组ID创建一个Future对象
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    /**
     * 验证协调器键集合
     * 确保请求中只包含当前处理器负责的消费者组ID
     * @param groupIds 待验证的协调器键集合
     * @throws IllegalArgumentException 如果包含非预期的消费者组ID则抛出异常
     */
    private void validateKeys(Set<CoordinatorKey> groupIds) {
        // 检查groupIds是否只包含当前处理器的groupId
        if (!groupIds.equals(singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + singleton(groupId) + ")");
        }
    }

    /**
     * 构建批量偏移量提交请求
     * @param coordinatorId 协调器节点ID
     * @param groupIds 消费者组ID集合
     * @return 返回OffsetCommitRequest.Builder对象
     */
    @Override
    public OffsetCommitRequest.Builder buildBatchedRequest(
        int coordinatorId,
        Set<CoordinatorKey> groupIds
    ) {
        // 验证消费者组ID
        validateKeys(groupIds);

        // 创建主题偏移量数据映射
        Map<String, OffsetCommitRequestTopic> offsetData = new HashMap<>();
        // 遍历所有待提交的偏移量
        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            // 获取或创建主题的提交请求对象
            OffsetCommitRequestTopic topic = offsetData.computeIfAbsent(
                topicPartition.topic(),
                key -> new OffsetCommitRequestTopic().setName(topicPartition.topic())
            );

            // 添加分区的偏移量信息
            topic.partitions().add(new OffsetCommitRequestPartition()
                // 设置待提交的偏移量
                .setCommittedOffset(offsetAndMetadata.offset())
                // 设置领导者epoch，如果不存在则设为-1
                .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(-1))
                // 设置偏移量的元数据信息
                .setCommittedMetadata(offsetAndMetadata.metadata())
                // 设置分区索引
                .setPartitionIndex(topicPartition.partition()));
        });

        // 创建偏移量提交请求数据
        OffsetCommitRequestData data = new OffsetCommitRequestData()
            // 设置消费者组ID
            .setGroupId(groupId.idValue)
            // 设置主题列表
            .setTopics(new ArrayList<>(offsetData.values()));

        // 构建并返回请求构建器
        return new OffsetCommitRequest.Builder(data);
    }

    /**
     * 处理偏移量提交响应
     * @param coordinator 协调器节点
     * @param groupIds 消费者组ID集合
     * @param abstractResponse 服务端响应
     * @return 返回API处理结果
     */
    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        // 验证消费者组ID
        validateKeys(groupIds);

        // 转换响应类型
        final OffsetCommitResponse response = (OffsetCommitResponse) abstractResponse;
        // 需要重新查找协调器的消费者组集合
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
        // 需要重试的消费者组集合
        final Set<CoordinatorKey> groupsToRetry = new HashSet<>();
        // 存储每个分区的处理结果
        final Map<TopicPartition, Errors> partitionResults = new HashMap<>();

        // 遍历响应中的所有主题
        for (OffsetCommitResponseTopic topic : response.data().topics()) {
            // 遍历主题中的所有分区
            for (OffsetCommitResponsePartition partition : topic.partitions()) {
                // 创建主题分区对象
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                // 获取错误码
                Errors error = Errors.forCode(partition.errorCode());

                // 如果存在错误，则进行错误处理
                if (error != Errors.NONE) {
                    handleError(
                        groupId,
                        topicPartition,
                        error,
                        partitionResults,
                        groupsToUnmap,
                        groupsToRetry
                    );
                } else {
                    // 如果没有错误，将结果添加到分区结果映射中
                    partitionResults.put(topicPartition, error);
                }
            }
        }

        // 根据处理结果返回相应的ApiResult
        if (groupsToUnmap.isEmpty() && groupsToRetry.isEmpty()) {
            // 如果不需要重新查找协调器也不需要重试，则返回完成状态
            return ApiResult.completed(groupId, partitionResults);
        } else {
            // 如果需要重新查找协调器，则返回未映射状态
            return ApiResult.unmapped(new ArrayList<>(groupsToUnmap));
        }
    }

    /**
     * 处理偏移量提交过程中的错误
     * @param groupId 消费者组ID
     * @param topicPartition 主题分区
     * @param error 错误类型
     * @param partitionResults 分区结果映射
     * @param groupsToUnmap 需要重新查找协调器的消费者组集合
     * @param groupsToRetry 需要重试的消费者组集合
     */
    private void handleError(
        CoordinatorKey groupId,
        TopicPartition topicPartition,
        Errors error,
        Map<TopicPartition, Errors> partitionResults,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            // 如果协调器正在加载中，或者消费者组正在重平衡，则需要重试
            case COORDINATOR_LOAD_IN_PROGRESS:
            case REBALANCE_IN_PROGRESS:
                log.debug("OffsetCommit request for group id {} returned error {}. Will retry.",
                    groupId.idValue, error);
                groupsToRetry.add(groupId);
                break;

            // 如果协调器不可用，则需要重新查找协调器并重试
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                log.debug("OffsetCommit request for group id {} returned error {}. Will rediscover the coordinator and retry.",
                    groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            // 消费者组级别的错误
            case INVALID_GROUP_ID:            // 无效的消费者组ID
            case INVALID_COMMIT_OFFSET_SIZE:  // 无效的提交偏移量大小
            case GROUP_AUTHORIZATION_FAILED:   // 消费者组授权失败
            case GROUP_ID_NOT_FOUND:          // 未找到消费者组ID
            // 成员级别的错误
            case UNKNOWN_MEMBER_ID:           // 未知的成员ID
            case STALE_MEMBER_EPOCH:          // 过期的成员epoch
                log.debug("OffsetCommit request for group id {} failed due to error {}.",
                    groupId.idValue, error);
                partitionResults.put(topicPartition, error);
                break;

            // 主题分区级别的错误
            case UNKNOWN_TOPIC_OR_PARTITION:    // 未知的主题或分区
            case OFFSET_METADATA_TOO_LARGE:     // 偏移量元数据太大
            case TOPIC_AUTHORIZATION_FAILED:     // 主题授权失败
                log.debug("OffsetCommit request for group id {} and partition {} failed due" +
                    " to error {}.", groupId.idValue, topicPartition, error);
                partitionResults.put(topicPartition, error);
                break;

            // 未预期的错误
            default:
                log.error("OffsetCommit request for group id {} and partition {} failed due" +
                    " to unexpected error {}.", groupId.idValue, topicPartition, error);
                partitionResults.put(topicPartition, error);
        }
    }
}
