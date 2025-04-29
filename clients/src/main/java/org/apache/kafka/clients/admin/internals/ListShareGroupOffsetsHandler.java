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

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 该类是{@link KafkaAdminClient#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}调用的处理器
 * 用于处理查询共享消费者组的偏移量信息的请求
 * 继承自AdminApiHandler.Batched，支持批量处理请求
 * 使用CoordinatorKey作为键，返回每个主题分区的偏移量信息
 * 
 * 应用场景：
 * 1. 查询共享消费者组的消费进度
 * 2. 监控消费者组的消费状态
 * 3. 诊断消费延迟问题
 */
public class ListShareGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, Long>> {

    /**
     * 存储共享消费者组的规格信息
     * 键为消费者组ID，值为包含要查询的主题分区信息的规格对象
     */
    private final Map<String, ListShareGroupOffsetsSpec> groupSpecs;

    /**
     * 日志记录器
     * 用于记录处理过程中的重要信息和错误
     */
    private final Logger log;

    /**
     * 协调器查找策略
     * 用于查找和管理共享消费者组的协调器节点
     */
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    /**
     * 构造函数
     * @param groupSpecs 共享消费者组规格映射，包含要查询的主题分区信息
     * @param logContext 日志上下文对象，用于创建日志记录器
     */
    public ListShareGroupOffsetsHandler(
        Map<String, ListShareGroupOffsetsSpec> groupSpecs,
        LogContext logContext) {
        // 初始化共享消费者组规格映射
        this.groupSpecs = groupSpecs;
        // 创建日志记录器
        this.log = logContext.logger(ListShareGroupOffsetsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 创建新的Future对象用于处理异步请求
     * @param groupIds 要查询的共享消费者组ID集合
     * @return 包含请求结果的Future对象
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Long>> newFuture(Collection<String> groupIds) {
        // 将消费者组ID转换为协调器键并创建Future对象
        return AdminApiFuture.forKeys(coordinatorKeys(groupIds));
    }

    /**
     * 获取API名称
     * @return 返回"describeShareGroupOffsets"，表示这是一个查询共享消费者组偏移量的操作
     */
    @Override
    public String apiName() {
        return "describeShareGroupOffsets";
    }

    /**
     * 获取协调器查找策略
     * @return 返回用于查找共享消费者组协调器的策略对象
     */
    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建批量查询共享消费者组偏移量的请求
     * @param coordinatorId 协调器节点ID
     * @param keys 协调器键集合
     * @return 共享消费者组偏移量查询请求的构建器
     */
    @Override
    public DescribeShareGroupOffsetsRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        // 将协调器键转换为消费者组ID列表
        List<String> groupIds = keys.stream().map(key -> {
            // 验证协调器键类型是否为GROUP
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeShareGroupOffsets` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        
        // 注意：当前DescribeShareGroupOffsetsRequest只支持单个消费者组ID
        // 这可能是一个需要在后续PR中修复的问题
        String groupId = groupIds.isEmpty() ? null : groupIds.get(0);
        if (groupId == null) {
            throw new IllegalArgumentException("Missing group id in request");
        }
        
        // 获取消费者组的规格信息
        ListShareGroupOffsetsSpec spec = groupSpecs.get(groupId);
        
        // 构建主题分区请求列表
        List<DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic> topics =
            spec.topicPartitions().stream().map(
                topicPartition -> new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                    .setTopicName(topicPartition.topic())
                    .setPartitions(List.of(topicPartition.partition()))
            ).collect(Collectors.toList());
        
        // 创建请求数据对象
        DescribeShareGroupOffsetsRequestData data = new DescribeShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(topics);
        
        // 返回请求构建器
        return new DescribeShareGroupOffsetsRequest.Builder(data, true);
    }

    /**
     * 处理查询共享消费者组偏移量请求的响应
     * @param coordinator 协调器节点
     * @param groupIds 请求的共享消费者组ID集合
     * @param abstractResponse 服务端响应
     * @return API处理结果，包含成功和失败的处理结果
     */
    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Long>> handleResponse(Node coordinator,
                                                                               Set<CoordinatorKey> groupIds,
                                                                               AbstractResponse abstractResponse) {
        // 转换响应类型为DescribeShareGroupOffsetsResponse
        final DescribeShareGroupOffsetsResponse response = (DescribeShareGroupOffsetsResponse) abstractResponse;
        // 初始化成功完成的结果映射
        final Map<CoordinatorKey, Map<TopicPartition, Long>> completed = new HashMap<>();
        // 初始化失败的结果映射
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();

        // 处理每个消费者组的响应
        for (CoordinatorKey groupId : groupIds) {
            // 创建存储主题分区偏移量的映射
            Map<TopicPartition, Long> data = new HashMap<>();
            // 处理响应中的每个主题
            response.data().responses().stream().map(
                describedTopic ->
                    // 处理主题中的每个分区
                    describedTopic.partitions().stream().map(
                        partition -> {
                            // 如果没有错误，记录分区的偏移量
                            if (partition.errorCode() == Errors.NONE.code())
                                data.put(new TopicPartition(describedTopic.topicName(), partition.partitionIndex()), partition.startOffset());
                            else
                                // 如果有错误，记录错误日志
                                log.error("Skipping return offset for topic {} partition {} due to error {}.", describedTopic.topicName(), partition.partitionIndex(), Errors.forCode(partition.errorCode()));
                            return data;
                        }
                    ).collect(Collectors.toList())
            ).collect(Collectors.toList());
            // 将处理结果添加到完成映射中
            completed.put(groupId, data);
        }
        // 返回API结果，包含成功和失败的处理结果
        return new ApiResult<>(completed, failed, Collections.emptyList());
    }

    /**
     * 将消费者组ID集合转换为协调器键集合
     * @param groupIds 消费者组ID集合
     * @return 协调器键集合
     */
    private static Set<CoordinatorKey> coordinatorKeys(Collection<String> groupIds) {
        // 将每个消费者组ID转换为对应的协调器键
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }
}
