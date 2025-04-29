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

import org.apache.kafka.clients.admin.ClassicGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.internals.AdminUtils.validAclOperations;

/**
 * 经典消费者组描述处理器，用于处理对经典消费者组的描述请求
 * 该处理器继承自AdminApiHandler.Batched，专门处理批量的消费者组描述请求
 */
public class DescribeClassicGroupsHandler extends AdminApiHandler.Batched<CoordinatorKey, ClassicGroupDescription> {

    /**
     * 是否包含已授权的操作信息
     */
    private final boolean includeAuthorizedOperations;
    
    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 协调器查找策略，用于定位消费者组的协调器节点
     */
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    /**
     * 构造函数
     * @param includeAuthorizedOperations 是否在响应中包含已授权的操作信息
     * @param logContext 日志上下文对象，用于创建日志记录器
     */
    public DescribeClassicGroupsHandler(
        boolean includeAuthorizedOperations,
        LogContext logContext
    ) {
        // 初始化是否包含授权操作的标志
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        // 使用日志上下文创建日志记录器
        this.log = logContext.logger(DescribeConsumerGroupsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 将消费者组ID集合转换为协调器键集合
     * @param groupIds 消费者组ID集合
     * @return 协调器键集合
     */
    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        // 将每个组ID转换为对应的协调器键并收集为Set
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    /**
     * 创建新的AdminApiFuture实例用于处理异步请求
     * @param groupIds 要描述的消费者组ID集合
     * @return 包含请求结果的Future对象
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ClassicGroupDescription> newFuture(Collection<String> groupIds) {
        // 使用协调器键集合创建Future对象
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    /**
     * 获取API名称
     * @return API名称字符串
     */
    @Override
    public String apiName() {
        return "describeClassicGroups";
    }

    /**
     * 获取协调器查找策略
     * @return 协调器查找策略实例
     */
    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建批量描述消费者组的请求
     * @param coordinatorId 协调器节点ID
     * @param keys 协调器键集合
     * @return 描述组请求的构建器
     */
    @Override
    public DescribeGroupsRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        // 将协调器键转换为组ID列表
        List<String> groupIds = keys.stream().map(key -> {
            // 验证协调器键类型是否为GROUP
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeGroups` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        
        // 创建请求数据对象，设置组ID列表和是否包含授权操作信息
        DescribeGroupsRequestData data = new DescribeGroupsRequestData()
            .setGroups(groupIds)
            .setIncludeAuthorizedOperations(includeAuthorizedOperations);
        
        // 返回请求构建器
        return new DescribeGroupsRequest.Builder(data);
    }

    /**
     * 处理描述消费者组请求的响应
     * @param coordinator 协调器节点
     * @param groupIds 请求的消费者组ID集合
     * @param abstractResponse 服务端响应
     * @return API处理结果，包含成功、失败和需要重新查找协调器的组信息
     */
    @Override
    public ApiResult<CoordinatorKey, ClassicGroupDescription> handleResponse(
            Node coordinator,
            Set<CoordinatorKey> groupIds,
            AbstractResponse abstractResponse) {
        // 转换响应类型
        final DescribeGroupsResponse response = (DescribeGroupsResponse) abstractResponse;
        // 初始化结果容器
        final Map<CoordinatorKey, ClassicGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        // 处理每个描述的组
        for (DescribeGroupsResponseData.DescribedGroup describedGroup : response.data().groups()) {
            // 创建组ID对应的协调器键
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            // 获取错误码
            Errors error = Errors.forCode(describedGroup.errorCode());
            // 如果存在错误，交给错误处理器处理
            if (error != Errors.NONE) {
                handleError(groupIdKey, error, error.message(), failed, groupsToUnmap);
                continue;
            }

            // 创建成员描述列表
            final List<MemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());
            // 获取已授权的操作集合
            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());

            // 获取协议类型并判断是否为消费者组
            final String protocolType = describedGroup.protocolType();
            final boolean isConsumerGroup = protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty();
            
            // 处理每个组成员
            describedGroup.members().forEach(groupMember -> {
                // 默认分区集合为空
                Set<TopicPartition> partitions = Collections.emptySet();
                // 如果是消费者组且存在分配信息，则反序列化分配信息
                if (isConsumerGroup && groupMember.memberAssignment().length > 0) {
                    final Assignment assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
                    partitions = new HashSet<>(assignment.partitions());
                }
                // 创建成员描述对象并添加到列表
                memberDescriptions.add(new MemberDescription(
                    groupMember.memberId(),
                    Optional.ofNullable(groupMember.groupInstanceId()),
                    groupMember.clientId(),
                    groupMember.clientHost(),
                    new MemberAssignment(partitions),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
            });

            // 创建经典消费者组描述对象
            final ClassicGroupDescription classicGroupDescription =
                new ClassicGroupDescription(
                    groupIdKey.idValue,
                    protocolType,
                    describedGroup.protocolData(),
                    memberDescriptions,
                    ClassicGroupState.parse(describedGroup.groupState()),
                    coordinator,
                    authorizedOperations);
            // 将结果添加到完成映射中
            completed.put(groupIdKey, classicGroupDescription);
        }

        // 返回API结果
        return new ApiResult<>(completed, failed, List.copyOf(groupsToUnmap));
    }

    /**
     * 处理描述消费者组请求过程中的错误
     * @param groupId 消费者组的协调器键
     * @param error 错误类型
     * @param errorMsg 错误消息
     * @param failed 失败的请求映射
     * @param groupsToUnmap 需要重新查找协调器的组集合
     */
    private void handleError(
            CoordinatorKey groupId,
            Errors error,
            String errorMsg,
            Map<CoordinatorKey, Throwable> failed,
            Set<CoordinatorKey> groupsToUnmap) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                // 处理授权失败错误
                log.debug("`DescribeGroups` request for group id {} failed due to error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // 处理协调器正在加载状态的错误，这种情况下需要重试
                log.debug("`DescribeGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry.", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // 处理协调器不可用或不是协调器的错误
                // 这种情况下需要重新查找协调器并重试请求
                log.debug("`DescribeGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                // 处理其他未预期的错误
                log.error("`DescribeGroups` request for group id {} failed due to unexpected error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
