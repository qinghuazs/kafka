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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 消费者组偏移量删除处理器
 * 该类负责处理删除消费者组特定主题分区的偏移量的请求
 * 继承自AdminApiHandler.Batched，支持批量处理请求，使用CoordinatorKey作为键，返回每个分区的错误信息
 */
public class DeleteConsumerGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, Errors>> {

    /**
     * 消费者组ID的协调器键
     * 用于标识和定位目标消费者组
     */
    private final CoordinatorKey groupId;

    /**
     * 需要删除偏移量的主题分区集合
     */
    private final Set<TopicPartition> partitions;

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
     * @param partitions 需要删除偏移量的主题分区集合
     * @param logContext 日志上下文
     */
    public DeleteConsumerGroupOffsetsHandler(
        String groupId,
        Set<TopicPartition> partitions,
        LogContext logContext
    ) {
        // 创建消费者组的协调器键
        this.groupId = CoordinatorKey.byGroupId(groupId);
        // 保存需要删除偏移量的分区集合
        this.partitions = partitions;
        // 初始化日志记录器
        this.log = logContext.logger(DeleteConsumerGroupOffsetsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 获取API名称
     * @return 返回"offsetDelete"，表示这是一个偏移量删除操作
     */
    @Override
    public String apiName() {
        return "offsetDelete";
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
     * 用于异步处理偏移量删除请求
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
        if (!groupIds.equals(Collections.singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + Collections.singleton(groupId) + ")");
        }
    }

    /**
     * 构建批量偏移量删除请求
     * @param coordinatorId 协调器节点ID
     * @param groupIds 消费者组ID集合
     * @return 返回OffsetDeleteRequest.Builder对象
     */
    @Override
    public OffsetDeleteRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> groupIds) {
        // 验证消费者组ID
        validateKeys(groupIds);

        // 创建主题集合对象
        final OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();
        // 按主题分组处理分区，并构建删除请求
        partitions.stream().collect(Collectors.groupingBy(TopicPartition::topic)).forEach((topic, topicPartitions) -> topics.add(
            new OffsetDeleteRequestTopic()
            .setName(topic)
            .setPartitions(topicPartitions.stream()
                .map(tp -> new OffsetDeleteRequestPartition().setPartitionIndex(tp.partition()))
                .collect(Collectors.toList())
            )
        ));

        // 构建并返回删除请求
        return new OffsetDeleteRequest.Builder(
            new OffsetDeleteRequestData()
                .setGroupId(groupId.idValue)
                .setTopics(topics)
        );
    }

    /**
     * 处理偏移量删除响应
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

        // 转换响应类型并获取错误码
        final OffsetDeleteResponse response = (OffsetDeleteResponse) abstractResponse;
        final Errors error = Errors.forCode(response.data().errorCode());

        // 处理响应中的错误
        if (error != Errors.NONE) {
            // 创建失败结果和需要重新映射的组ID集合
            final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
            final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

            // 处理组级别错误
            handleGroupError(groupId, error, failed, groupsToUnmap);

            // 返回错误结果
            return new ApiResult<>(Collections.emptyMap(), failed, new ArrayList<>(groupsToUnmap));
        } else {
            // 处理成功响应，收集每个分区的处理结果
            final Map<TopicPartition, Errors> partitionResults = new HashMap<>();
            response.data().topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    partitionResults.put(
                        new TopicPartition(topic.name(), partition.partitionIndex()),
                        Errors.forCode(partition.errorCode())
                    )
                )
            );

            // 返回成功结果
            return ApiResult.completed(groupId, partitionResults);
        }
    }

    /**
     * 处理组级别的错误
     * @param groupId 消费者组ID
     * @param error 错误类型
     * @param failed 失败结果映射
     * @param groupsToUnmap 需要重新映射的组ID集合
     */
    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            // 处理权限、组ID相关错误
            case GROUP_AUTHORIZATION_FAILED:
            case GROUP_ID_NOT_FOUND:
            case INVALID_GROUP_ID:
            case NON_EMPTY_GROUP:
                log.debug("`OffsetDelete` request for group id {} failed due to error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;

            // 处理协调器加载中的错误，需要重试
            case COORDINATOR_LOAD_IN_PROGRESS:
                log.debug("`OffsetDelete` request for group id {} failed because the coordinator" +
                    " is still in the process of loading state. Will retry.", groupId.idValue);
                break;

            // 处理协调器不可用或变更的错误，需要重新查找协调器
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                log.debug("`OffsetDelete` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            // 处理其他未预期的错误
            default:
                log.error("`OffsetDelete` request for group id {} failed due to unexpected error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;
        }
    }

}
