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

import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.ShareGroupDescribeResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.internals.AdminUtils.validAclOperations;

/**
 * 共享组描述处理器，用于处理对共享组的描述请求
 * 该处理器继承自AdminApiHandler.Batched，专门处理批量的共享组描述请求
 * 应用场景：
 * 1. 用于获取共享组的详细信息，包括组成员、分配的分区等
 * 2. 支持批量查询多个共享组的信息
 * 3. 处理共享组描述请求的错误和重试逻辑
 */
public class DescribeShareGroupsHandler extends AdminApiHandler.Batched<CoordinatorKey, ShareGroupDescription> {

    /**
     * 是否在响应中包含已授权的操作信息
     */
    private final boolean includeAuthorizedOperations;
    
    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 协调器查找策略，用于定位共享组的协调器节点
     */
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    /**
     * 构造函数
     * @param includeAuthorizedOperations 是否在响应中包含已授权的操作信息
     * @param logContext 日志上下文对象，用于创建日志记录器
     */
    public DescribeShareGroupsHandler(
          boolean includeAuthorizedOperations,
          LogContext logContext) {
        // 初始化是否包含授权操作的标志
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        // 使用日志上下文创建日志记录器
        this.log = logContext.logger(DescribeShareGroupsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 将共享组ID集合转换为协调器键集合
     * @param groupIds 共享组ID集合
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
     * @param groupIds 要描述的共享组ID集合
     * @return 包含请求结果的Future对象
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ShareGroupDescription> newFuture(Collection<String> groupIds) {
        // 使用协调器键集合创建Future对象
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    /**
     * 获取API名称
     * @return API名称字符串
     */
    @Override
    public String apiName() {
        return "describeShareGroups";
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
     * 构建批量描述共享组的请求
     * @param coordinatorId 协调器节点ID
     * @param keys 协调器键集合
     * @return 共享组描述请求的构建器
     */
    @Override
    public ShareGroupDescribeRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        // 将协调器键转换为组ID列表
        List<String> groupIds = keys.stream().map(key -> {
            // 验证协调器键类型是否为GROUP
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeShareGroups` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        
        // 创建请求数据对象，设置组ID列表和是否包含授权操作信息
        ShareGroupDescribeRequestData data = new ShareGroupDescribeRequestData()
            .setGroupIds(groupIds)
            .setIncludeAuthorizedOperations(includeAuthorizedOperations);
        
        // 返回请求构建器
        return new ShareGroupDescribeRequest.Builder(data, true);
    }

    /**
     * 处理描述共享组请求的响应
     * @param coordinator 协调器节点
     * @param groupIds 请求的共享组ID集合
     * @param abstractResponse 服务端响应
     * @return API处理结果，包含成功、失败和需要重新查找协调器的组信息
     */
    @Override
    public ApiResult<CoordinatorKey, ShareGroupDescription> handleResponse(
            Node coordinator,
            Set<CoordinatorKey> groupIds,
            AbstractResponse abstractResponse) {
        // 转换响应类型
        final ShareGroupDescribeResponse response = (ShareGroupDescribeResponse) abstractResponse;
        // 初始化结果容器
        final Map<CoordinatorKey, ShareGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        // 处理每个描述的组
        for (ShareGroupDescribeResponseData.DescribedGroup describedGroup : response.data().groups()) {
            // 创建组ID对应的协调器键
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            // 获取错误码
            Errors error = Errors.forCode(describedGroup.errorCode());
            // 如果存在错误，交给错误处理器处理
            if (error != Errors.NONE) {
                handleError(groupIdKey, describedGroup, coordinator, error, describedGroup.errorMessage(), completed, failed, groupsToUnmap);
                continue;
            }

            // 创建成员描述列表和获取已授权的操作集合
            final List<ShareMemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());
            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());

            // 处理每个组成员
            describedGroup.members().forEach(groupMember ->
                memberDescriptions.add(new ShareMemberDescription(
                    groupMember.memberId(),
                    groupMember.clientId(),
                    groupMember.clientHost(),
                    new ShareMemberAssignment(convertAssignment(groupMember.assignment())),
                    groupMember.memberEpoch()
                ))
            );

            // 创建共享组描述对象并添加到完成列表
            final ShareGroupDescription shareGroupDescription =
                new ShareGroupDescription(groupIdKey.idValue,
                    memberDescriptions,
                    GroupState.parse(describedGroup.groupState()),
                    coordinator,
                    describedGroup.groupEpoch(),
                    describedGroup.assignmentEpoch(),
                    authorizedOperations);
            completed.put(groupIdKey, shareGroupDescription);
        }

        // 返回API结果
        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    /**
     * 转换分配信息为TopicPartition集合
     * @param assignment 分配信息
     * @return 主题分区集合
     */
    private Set<TopicPartition> convertAssignment(ShareGroupDescribeResponseData.Assignment assignment) {
        // 将分配信息中的主题分区列表转换为TopicPartition对象集合
        return assignment.topicPartitions().stream().flatMap(topic ->
            topic.partitions().stream().map(partition ->
                new TopicPartition(topic.topicName(), partition)
            )
        ).collect(Collectors.toSet());
    }

    /**
     * 处理描述共享组请求过程中的错误
     * @param groupId 共享组ID对应的协调器键
     * @param describedGroup 描述的组信息
     * @param coordinator 协调器节点
     * @param error 错误类型
     * @param errorMsg 错误信息
     * @param completed 完成的请求结果映射
     * @param failed 失败的请求结果映射
     * @param groupsToUnmap 需要重新查找协调器的组集合
     */
    private void handleError(
            CoordinatorKey groupId,
            ShareGroupDescribeResponseData.DescribedGroup describedGroup,
            Node coordinator,
            Errors error,
            String errorMsg,
            Map<CoordinatorKey, ShareGroupDescription> completed,
            Map<CoordinatorKey, Throwable> failed,
            Set<CoordinatorKey> groupsToUnmap) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                // 处理授权失败错误
                log.debug("`DescribeShareGroups` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // 处理协调器正在加载状态的错误，需要重试
                log.debug("`DescribeShareGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // 处理协调器不可用或协调器变更的错误，需要重新查找协调器
                log.debug("`DescribeShareGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            case GROUP_ID_NOT_FOUND:
                // 处理组不存在的错误
                log.debug("`DescribeShareGroups` request for group id {} failed because the group does not exist. {}",
                    groupId.idValue, errorMsg != null ? errorMsg : "");
                failed.put(groupId, error.exception(errorMsg));
                break;

            default:
                // 处理未预期的错误
                log.error("`DescribeShareGroups` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
