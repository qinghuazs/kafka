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

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeResponse;
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
 * 消费者组描述处理器，用于处理新版和经典版消费者组的描述请求
 * 该处理器实现了AdminApiHandler接口，支持两种API:
 * 1. 新版ConsumerGroupDescribe API - 用于新版消费者组
 * 2. 经典DescribeGroups API - 用于经典消费者组
 * 处理器会优先尝试使用新版API，如果失败则回退到经典API
 */
public class DescribeConsumerGroupsHandler implements AdminApiHandler<CoordinatorKey, ConsumerGroupDescription> {

    /**
     * 是否在响应中包含已授权的操作信息
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
     * 需要使用经典API的消费者组ID集合
     * 当新版API调用失败时，将相应的groupId添加到此集合中
     */
    private final Set<String> useClassicGroupApi;

    /**
     * 存储GROUP_ID_NOT_FOUND错误的详细信息
     * key: groupId
     * value: 新版API返回的错误信息(通常比经典API的错误信息更详细)
     */
    private final Map<String, String> groupIdNotFoundErrorMessages;

    /**
     * 构造函数
     * @param includeAuthorizedOperations 是否在响应中包含已授权的操作信息
     * @param logContext 日志上下文对象，用于创建日志记录器
     */
    public DescribeConsumerGroupsHandler(
        boolean includeAuthorizedOperations,
        LogContext logContext
    ) {
        // 初始化是否包含授权操作的标志
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        // 使用日志上下文创建日志记录器
        this.log = logContext.logger(DescribeConsumerGroupsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        // 初始化需要使用经典API的消费者组ID集合
        this.useClassicGroupApi = new HashSet<>();
        // 初始化存储GROUP_ID_NOT_FOUND错误信息的映射
        this.groupIdNotFoundErrorMessages = new HashMap<>();
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
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> newFuture(
        Collection<String> groupIds
    ) {
        // 使用协调器键集合创建Future对象
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    /**
     * 获取API名称
     * @return API名称字符串
     */
    @Override
    public String apiName() {
        return "describeConsumerGroups";
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
     * 构建描述消费者组的请求
     * 该方法会根据消费者组的类型(新版或经典)构建不同的请求:
     * 1. 对于新版消费者组，使用ConsumerGroupDescribe API
     * 2. 对于经典消费者组，使用DescribeGroups API
     * 
     * @param coordinatorId 协调器节点ID
     * @param keys 协调器键集合
     * @return 请求和对应键的集合
     */
    @Override
    public Collection<RequestAndKeys<CoordinatorKey>> buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        // 初始化新版和经典API的请求参数容器
        Set<CoordinatorKey> newConsumerGroupKeys = new HashSet<>();
        Set<CoordinatorKey> oldConsumerGroupKeys = new HashSet<>();
        List<String> newConsumerGroupIds = new ArrayList<>();
        List<String> oldConsumerGroupIds = new ArrayList<>();

        // 遍历所有协调器键，将其分类到新版或经典API的容器中
        keys.forEach(key -> {
            // 验证协调器键类型是否为GROUP
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeGroups` request");
            }

            // 默认使用新版API，如果之前失败过则使用经典API
            if (useClassicGroupApi.contains(key.idValue)) {
                oldConsumerGroupKeys.add(key);
                oldConsumerGroupIds.add(key.idValue);
            } else {
                newConsumerGroupKeys.add(key);
                newConsumerGroupIds.add(key.idValue);
            }
        });

        // 构建请求列表
        List<RequestAndKeys<CoordinatorKey>> requests = new ArrayList<>();
        
        // 如果有新版消费者组，构建ConsumerGroupDescribe请求
        if (!newConsumerGroupKeys.isEmpty()) {
            ConsumerGroupDescribeRequestData data = new ConsumerGroupDescribeRequestData()
                .setGroupIds(newConsumerGroupIds)
                .setIncludeAuthorizedOperations(includeAuthorizedOperations);
            requests.add(new RequestAndKeys<>(new ConsumerGroupDescribeRequest.Builder(data), newConsumerGroupKeys));
        }

        // 如果有经典消费者组，构建DescribeGroups请求
        if (!oldConsumerGroupKeys.isEmpty()) {
            DescribeGroupsRequestData data = new DescribeGroupsRequestData()
                .setGroups(oldConsumerGroupIds)
                .setIncludeAuthorizedOperations(includeAuthorizedOperations);
            requests.add(new RequestAndKeys<>(new DescribeGroupsRequest.Builder(data), oldConsumerGroupKeys));
        }

        return requests;
    }

    /**
     * 处理描述消费者组请求的响应
     * 根据响应类型分别处理新版和经典API的响应
     * 
     * @param coordinator 协调器节点
     * @param groupIds 请求的消费者组ID集合
     * @param abstractResponse 服务端响应
     * @return API处理结果，包含成功、失败和需要重新查找协调器的组信息
     */
    @Override
    public ApiResult<CoordinatorKey, ConsumerGroupDescription> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        // 初始化结果容器
        final Map<CoordinatorKey, ConsumerGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        // 根据响应类型调用对应的处理方法
        if (abstractResponse instanceof DescribeGroupsResponse) {
            // 处理经典API响应
            return handledClassicGroupResponse(
                coordinator,
                completed,
                failed,
                groupsToUnmap,
                (DescribeGroupsResponse) abstractResponse
            );
        } else if (abstractResponse instanceof ConsumerGroupDescribeResponse) {
            // 处理新版API响应
            return handledConsumerGroupResponse(
                coordinator,
                completed,
                failed,
                groupsToUnmap,
                (ConsumerGroupDescribeResponse) abstractResponse
            );
        } else {
            throw new IllegalArgumentException("Received an unexpected response type.");
        }
    }

    /**
     * 处理不支持版本异常
     * 当新版API不支持时，尝试切换到经典API
     * 
     * @param coordinator 协调器节点ID
     * @param exception 不支持版本异常
     * @param keys 请求的协调器键集合
     * @return 无法处理的错误映射
     */
    @Override
    public Map<CoordinatorKey, Throwable> handleUnsupportedVersionException(
        int coordinator,
        UnsupportedVersionException exception,
        Set<CoordinatorKey> keys
    ) {
        // 初始化错误映射
        Map<CoordinatorKey, Throwable> errors = new HashMap<>();

        // 遍历所有键，尝试切换到经典API
        keys.forEach(key -> {
            // 如果已经使用过经典API，则添加到错误映射中
            if (!useClassicGroupApi.add(key.idValue)) {
                // 已经尝试过经典API，此时必须失败
                errors.put(key, exception);
            }
        });

        return errors;
    }

    /**
     * 处理新版消费者组API的响应
     * 
     * @param coordinator 协调器节点
     * @param completed 成功处理的消费者组描述映射
     * @param failed 处理失败的错误映射
     * @param groupsToUnmap 需要重新查找协调器的组集合
     * @param response 新版API的响应
     * @return API处理结果
     */
    private ApiResult<CoordinatorKey, ConsumerGroupDescription> handledConsumerGroupResponse(
        Node coordinator,
        Map<CoordinatorKey, ConsumerGroupDescription> completed,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        ConsumerGroupDescribeResponse response
    ) {
        // 遍历响应中的每个消费者组
        for (ConsumerGroupDescribeResponseData.DescribedGroup describedGroup : response.data().groups()) {
            // 创建组ID对应的协调器键
            final CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            // 获取错误码
            final Errors error = Errors.forCode(describedGroup.errorCode());
            // 如果存在错误，交给错误处理器处理
            if (error != Errors.NONE) {
                handleError(
                    groupIdKey,
                    error,
                    describedGroup.errorMessage(),
                    failed,
                    groupsToUnmap,
                    true
                );
                continue;
            }

            // 获取已授权的操作集合
            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
            // 创建成员描述列表
            final List<MemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());

            // 处理每个组成员
            describedGroup.members().forEach(groupMember ->
                memberDescriptions.add(new MemberDescription(
                    groupMember.memberId(), // 成员ID
                    Optional.ofNullable(groupMember.instanceId()), // 实例ID(静态成员)
                    groupMember.clientId(), // 客户端ID
                    groupMember.clientHost(), // 客户端主机
                    new MemberAssignment(convertAssignment(groupMember.assignment())), // 当前分配
                    Optional.of(new MemberAssignment(convertAssignment(groupMember.targetAssignment()))), // 目标分配
                    Optional.of(groupMember.memberEpoch()), // 成员轮次
                    groupMember.memberType() == -1 ? Optional.empty() : Optional.of(groupMember.memberType() == 1) // 成员类型
                ))
            );

            // 创建消费者组描述对象
            final ConsumerGroupDescription consumerGroupDescription =
                new ConsumerGroupDescription(
                    groupIdKey.idValue, // 组ID
                    false, // 不是简单消费者组
                    memberDescriptions, // 成员描述列表
                    describedGroup.assignorName(), // 分配器名称
                    GroupType.CONSUMER, // 组类型为消费者
                    GroupState.parse(describedGroup.groupState()), // 组状态
                    coordinator, // 协调器节点
                    authorizedOperations, // 已授权操作
                    Optional.of(describedGroup.groupEpoch()), // 组轮次
                    Optional.of(describedGroup.assignmentEpoch()) // 分配轮次
                );
            // 添加到成功处理的映射中
            completed.put(groupIdKey, consumerGroupDescription);
        }

        // 返回处理结果
        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    /**
     * 处理经典消费者组API的响应
     * 该方法处理使用经典DescribeGroups API获取的消费者组信息
     * 
     * @param coordinator 协调器节点
     * @param completed 成功处理的消费者组描述映射
     * @param failed 处理失败的错误映射
     * @param groupsToUnmap 需要重新查找协调器的组集合
     * @param response 经典API的响应
     * @return API处理结果
     */
    private ApiResult<CoordinatorKey, ConsumerGroupDescription> handledClassicGroupResponse(
        Node coordinator,
        Map<CoordinatorKey, ConsumerGroupDescription> completed,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        DescribeGroupsResponse response
    ) {
        // 遍历响应中的每个消费者组
        for (DescribedGroup describedGroup : response.data().groups()) {
            // 根据组ID创建协调器键
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            // 获取错误码并检查是否有错误
            Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                // 如果有错误，交给错误处理器处理
                handleError(
                    groupIdKey,
                    error,
                    describedGroup.errorMessage(),
                    failed,
                    groupsToUnmap,
                    false // 标记这是经典API的响应
                );
                continue;
            }

            // 获取协议类型并验证是否为消费者组
            final String protocolType = describedGroup.protocolType();
            if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                // 获取组成员列表
                final List<DescribedGroupMember> members = describedGroup.members();
                final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
                // 获取已授权的操作集合
                final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());

                // 处理每个组成员
                for (DescribedGroupMember groupMember : members) {
                    // 默认分区集合为空
                    Set<TopicPartition> partitions = Collections.emptySet();
                    // 如果成员有分配的分区，则解析分配信息
                    if (groupMember.memberAssignment().length > 0) {
                        final Assignment assignment = ConsumerProtocol.
                            deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
                        partitions = new HashSet<>(assignment.partitions());
                    }
                    // 创建成员描述对象
                    memberDescriptions.add(new MemberDescription(
                        groupMember.memberId(), // 成员ID
                        Optional.ofNullable(groupMember.groupInstanceId()), // 实例ID(静态成员)
                        groupMember.clientId(), // 客户端ID
                        groupMember.clientHost(), // 客户端主机
                        new MemberAssignment(partitions), // 分区分配信息
                        Optional.empty(), // 经典API不支持目标分配
                        Optional.empty(), // 经典API不支持成员轮次
                        Optional.empty())); // 经典API不支持成员类型
                }

                // 创建消费者组描述对象
                final ConsumerGroupDescription consumerGroupDescription =
                    new ConsumerGroupDescription(
                        groupIdKey.idValue, // 组ID
                        protocolType.isEmpty(), // 是否为简单消费者组
                        memberDescriptions, // 成员描述列表
                        describedGroup.protocolData(), // 协议数据
                        GroupType.CLASSIC, // 组类型为经典
                        GroupState.parse(describedGroup.groupState()), // 组状态
                        coordinator, // 协调器节点
                        authorizedOperations, // 已授权操作
                        Optional.empty(), // 经典API不支持组轮次
                        Optional.empty()); // 经典API不支持分配轮次
                // 添加到成功处理的映射中
                completed.put(groupIdKey, consumerGroupDescription);
            } else {
                // 如果不是消费者组，添加到失败映射中
                failed.put(groupIdKey, new IllegalArgumentException(
                    String.format("GroupId %s is not a consumer group (%s).",
                        groupIdKey.idValue, protocolType)));
            }
        }

        // 返回处理结果
        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    /**
     * 转换分区分配信息
     * 将新版API的分配信息格式转换为TopicPartition集合
     * 
     * @param assignment 新版API的分配信息对象
     * @return 主题分区集合
     */
    private Set<TopicPartition> convertAssignment(ConsumerGroupDescribeResponseData.Assignment assignment) {
        // 将每个主题的分区列表转换为TopicPartition对象，并收集为Set
        return assignment.topicPartitions().stream().flatMap(topic ->
            // 对每个主题的分区列表进行处理
            topic.partitions().stream().map(partition ->
                // 创建TopicPartition对象表示主题分区
                new TopicPartition(topic.topicName(), partition)
            )
        ).collect(Collectors.toSet());
    }

    /**
     * 处理错误情况
     * 根据不同的错误类型采取相应的处理策略
     * 
     * @param groupId 消费者组的协调器键
     * @param error 错误类型
     * @param errorMsg 错误信息
     * @param failed 处理失败的错误映射
     * @param groupsToUnmap 需要重新查找协调器的组集合
     * @param isConsumerGroupResponse 是否为新版API的响应
     */
    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        String errorMsg,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        boolean isConsumerGroupResponse
    ) {
        // 根据API类型设置名称
        String apiName = isConsumerGroupResponse ? "ConsumerGroupDescribe" : "DescribeGroups";

        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                // 授权失败：记录日志并添加到失败映射
                log.debug("`{}` request for group id {} failed due to error {}.", apiName, groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // 协调器正在加载：记录日志并等待重试
                log.debug("`{}` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry.", apiName, groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // 协调器不可用或不是正确的协调器：记录日志并重新查找协调器
                log.debug("`{}` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", apiName, groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            case UNSUPPORTED_VERSION:
                if (isConsumerGroupResponse) {
                    // 新版API不支持：记录日志并切换到经典API
                    log.debug("`{}` request for group id {} failed because the API is not " +
                        "supported. Will retry with `DescribeGroups` API.", apiName, groupId.idValue);
                    useClassicGroupApi.add(groupId.idValue);
                } else {
                    // 经典API也不支持：记录错误并添加到失败映射
                    log.error("`{}` request for group id {} failed because the `ConsumerGroupDescribe` API is not supported.",
                        apiName, groupId.idValue);
                    failed.put(groupId, error.exception(errorMsg));
                }
                break;

            case GROUP_ID_NOT_FOUND:
                if (isConsumerGroupResponse) {
                    // 新版API找不到组：记录日志并切换到经典API
                    log.debug("`{}` request for group id {} failed because the group is not " +
                        "a new consumer group. Will retry with `DescribeGroups` API. {}",
                        apiName, groupId.idValue, errorMsg != null ? errorMsg : "");
                    useClassicGroupApi.add(groupId.idValue);

                    // 保存新版API的错误信息，因为它通常比经典API的错误信息更详细
                    groupIdNotFoundErrorMessages.put(groupId.idValue, errorMsg);
                } else {
                    // 经典API也找不到组：记录日志并使用之前保存的详细错误信息
                    log.debug("`{}` request for group id {} failed because the group does not exist. {}",
                        apiName, groupId.idValue, errorMsg != null ? errorMsg : "");
                    failed.put(groupId, error.exception(groupIdNotFoundErrorMessages.getOrDefault(groupId.idValue, errorMsg)));
                }
                break;

            default:
                // 其他未预期的错误：记录错误并添加到失败映射
                log.error("`{}` request for group id {} failed due to unexpected error {}.", apiName, groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
