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

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 消费者组偏移量列表处理器
 * 用于处理获取消费者组提交偏移量的请求，实现了AdminApiHandler接口
 * 主要功能：
 * 1. 构建批量获取消费者组偏移量的请求
 * 2. 处理偏移量获取响应
 * 3. 处理各种错误情况
 */
public class ListConsumerGroupOffsetsHandler implements AdminApiHandler<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> {

    /**
     * 是否要求返回稳定的偏移量
     * 当设置为true时，只返回已经完成事务的偏移量
     */
    private final boolean requireStable;

    /**
     * 消费者组规格映射，key为消费者组ID，value为对应的规格信息
     * 包含了每个消费者组需要查询的主题分区列表
     */
    private final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs;

    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 协调器查找策略，用于查找消费者组的协调器节点
     */
    private final CoordinatorStrategy lookupStrategy;

    /**
     * 构造函数
     * @param groupSpecs 消费者组规格映射，指定每个组要查询的分区
     * @param requireStable 是否要求稳定的偏移量
     * @param logContext 日志上下文
     */
    public ListConsumerGroupOffsetsHandler(
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
        boolean requireStable,
        LogContext logContext
    ) {
        // 初始化日志记录器
        this.log = logContext.logger(ListConsumerGroupOffsetsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        this.groupSpecs = groupSpecs;
        this.requireStable = requireStable;
    }

    /**
     * 创建新的AdminApiFuture实例
     * @param groupIds 要查询偏移量的消费者组ID集合
     * @return 返回一个新的Future实例，用于异步处理偏移量查询请求
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> newFuture(Collection<String> groupIds) {
        // 将消费者组ID转换为协调器键，并创建对应的Future
        return AdminApiFuture.forKeys(coordinatorKeys(groupIds));
    }

    /**
     * 获取API名称
     * @return 返回"offsetFetch"作为API标识
     */
    @Override
    public String apiName() {
        return "offsetFetch";
    }

    /**
     * 获取协调器查找策略
     * @return 返回用于查找消费者组协调器的策略实例
     */
    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 验证请求的消费者组ID是否有效
     * @param groupIds 要验证的消费者组ID集合
     * @throws IllegalArgumentException 如果包含未知的消费者组ID
     */
    private void validateKeys(Set<CoordinatorKey> groupIds) {
        // 获取所有已知的消费者组ID
        Set<CoordinatorKey> keys = coordinatorKeys(groupSpecs.keySet());
        // 检查是否所有请求的组ID都是已知的
        if (!keys.containsAll(groupIds)) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                    " (expected one of " + keys + ")");
        }
    }

    /**
     * 将消费者组ID集合转换为协调器键集合
     * @param groupIds 消费者组ID集合
     * @return 返回对应的协调器键集合
     */
    private static Set<CoordinatorKey> coordinatorKeys(Collection<String> groupIds) {
        // 将每个消费者组ID转换为对应的协调器键
        return groupIds.stream()
           .map(CoordinatorKey::byGroupId)
           .collect(Collectors.toSet());
    }

    /**
     * 构建批量获取偏移量的请求
     * @param groupIds 要查询的消费者组ID集合
     * @return 返回OffsetFetchRequest.Builder实例
     */
    public OffsetFetchRequest.Builder buildBatchedRequest(Set<CoordinatorKey> groupIds) {
        // 创建一个映射，只包含由协调器管理的消费者组
        Map<String, List<TopicPartition>> coordinatorGroupIdToTopicPartitions = new HashMap<>(groupIds.size());
        // 遍历每个消费者组ID
        groupIds.forEach(g -> {
            // 获取该消费者组的查询规格
            ListConsumerGroupOffsetsSpec spec = groupSpecs.get(g.idValue);
            // 获取需要查询的主题分区列表，如果未指定则为null（表示查询所有分区）
            List<TopicPartition> partitions = spec.topicPartitions() != null ? new ArrayList<>(spec.topicPartitions()) : null;
            // 将消费者组ID和其对应的分区列表添加到映射中
            coordinatorGroupIdToTopicPartitions.put(g.idValue, partitions);
        });

        // 创建OffsetFetchRequest.Builder实例
        // requireStable参数用于指定是否只返回已完成事务的偏移量
        // 最后一个参数false表示不需要等待所有副本同步完成
        return new OffsetFetchRequest.Builder(coordinatorGroupIdToTopicPartitions, requireStable, false);
    }

    /**
     * 构建请求
     * @param brokerId broker节点ID
     * @param groupIds 要查询的消费者组ID集合
     * @return 返回请求和对应键的集合
     */
    @Override
    public Collection<RequestAndKeys<CoordinatorKey>> buildRequest(int brokerId, Set<CoordinatorKey> groupIds) {
        // 验证消费者组ID的有效性
        validateKeys(groupIds);

        // 当OffsetFetchRequest因NoBatchedOffsetFetchRequestException失败时
        // 我们会完全禁用批处理，包括FindCoordinatorRequest
        if (lookupStrategy.batch()) {
            // 如果支持批处理，则创建一个包含所有组ID的批量请求
            return Collections.singletonList(new RequestAndKeys<>(buildBatchedRequest(groupIds), groupIds));
        } else {
            // 如果不支持批处理，则为每个组ID创建单独的请求
            return groupIds.stream().map(groupId -> {
                Set<CoordinatorKey> keys = Collections.singleton(groupId);
                return new RequestAndKeys<>(buildBatchedRequest(keys), keys);
            }).collect(Collectors.toList());
        }
    }

    /**
     * 处理偏移量获取响应
     * @param coordinator 协调器节点
     * @param groupIds 请求的消费者组ID集合
     * @param abstractResponse 服务器的响应
     * @return 返回API处理结果，包含成功、失败和需要重新映射的请求
     */
    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        // 验证消费者组ID的有效性
        validateKeys(groupIds);

        // 将响应转换为OffsetFetchResponse类型
        final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;

        // 存储处理结果的容器
        // completed: 成功获取偏移量的消费者组
        // failed: 处理失败的消费者组及其异常
        // unmapped: 需要重新查找协调器的消费者组
        Map<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();

        // 处理每个消费者组的响应
        for (CoordinatorKey coordinatorKey : groupIds) {
            String group = coordinatorKey.idValue;
            // 检查消费者组级别是否有错误
            if (response.groupHasError(group)) {
                // 如果有错误，交给错误处理方法处理
                handleGroupError(CoordinatorKey.byGroupId(group), response.groupLevelError(group), failed, unmapped);
            } else {
                // 如果没有错误，处理该组的分区偏移量数据
                final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();
                Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = response.partitionDataMap(group);
                
                // 处理每个分区的偏移量数据
                for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> partitionEntry : responseData.entrySet()) {
                    final TopicPartition topicPartition = partitionEntry.getKey();
                    OffsetFetchResponse.PartitionData partitionData = partitionEntry.getValue();
                    final Errors error = partitionData.error;

                    if (error == Errors.NONE) {
                        // 如果分区没有错误，获取偏移量信息
                        final long offset = partitionData.offset;
                        final String metadata = partitionData.metadata;
                        final Optional<Integer> leaderEpoch = partitionData.leaderEpoch;
                        
                        // 负值偏移量表示该分区没有提交的偏移量
                        if (offset < 0) {
                            groupOffsetsListing.put(topicPartition, null);
                        } else {
                            // 创建偏移量元数据对象并存储
                            groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, leaderEpoch, metadata));
                        }
                    } else {
                        // 如果分区有错误，记录警告日志
                        log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                    }
                }
                // 将该消费者组的偏移量信息添加到完成列表
                completed.put(CoordinatorKey.byGroupId(group), groupOffsetsListing);
            }
        }
        // 返回处理结果
        return new ApiResult<>(completed, failed, unmapped);
    }

    /**
     * 处理消费者组级别的错误
     * @param groupId 消费者组ID
     * @param error 错误类型
     * @param failed 存储失败操作的Map
     * @param groupsToUnmap 存储需要重新查找协调器的消费者组列表
     */
    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED: // 组授权失败
            case UNKNOWN_MEMBER_ID: // 未知的成员ID
            case STALE_MEMBER_EPOCH: // 过期的成员epoch
                // 这些错误表示请求本身有问题，记录日志并标记为失败
                log.debug("`OffsetFetch` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;

            case COORDINATOR_LOAD_IN_PROGRESS: // 协调器正在加载中
                // 如果协调器正在加载状态，需要重试请求
                log.debug("`OffsetFetch` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE: // 协调器不可用
            case NOT_COORDINATOR: // 不是正确的协调器
                // 如果协调器不可用或发生了协调器变更，需要重新查找协调器
                log.debug("`OffsetFetch` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default: // 其他未预期的错误
                // 记录错误日志并标记为失败
                log.error("`OffsetFetch` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }
}
