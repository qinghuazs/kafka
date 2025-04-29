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
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 从消费者组中移除成员的处理器
 * 继承自AdminApiHandler.Batched，用于处理批量移除消费者组成员的请求
 * 主要功能：
 * 1. 构建LeaveGroup请求以移除指定的消费者组成员
 * 2. 处理移除成员的响应结果
 * 3. 处理各种错误情况（如授权失败、协调器不可用等）
 * 4. 支持批量操作以提高效率
 */
public class RemoveMembersFromConsumerGroupHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<MemberIdentity, Errors>> {

    /**
     * 要操作的消费者组的协调器键
     */
    private final CoordinatorKey groupId;
    
    /**
     * 要从消费者组中移除的成员列表
     */
    private final List<MemberIdentity> members;
    
    /**
     * 日志记录器，用于记录处理过程中的日志信息
     */
    private final Logger log;
    
    /**
     * 协调器查找策略，用于查找消费者组的协调器节点
     */
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    /**
     * 构造函数
     * @param groupId 消费者组ID
     * @param members 要移除的成员列表
     * @param logContext 日志上下文
     */
    public RemoveMembersFromConsumerGroupHandler(
        String groupId,
        List<MemberIdentity> members,
        LogContext logContext
    ) {
        // 将消费者组ID转换为协调器键
        this.groupId = CoordinatorKey.byGroupId(groupId);
        // 保存要移除的成员列表
        this.members = members;
        // 初始化日志记录器
        this.log = logContext.logger(RemoveMembersFromConsumerGroupHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 获取API名称
     * @return 返回"leaveGroup"作为API标识
     */
    @Override
    public String apiName() {
        return "leaveGroup";
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
     * 创建新的AdminApiFuture实例
     * @param groupId 要操作的消费者组ID
     * @return 返回一个新的Future实例，用于异步处理成员移除请求
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<MemberIdentity, Errors>> newFuture(
        String groupId
    ) {
        // 创建只包含一个消费者组ID的Future
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    /**
     * 验证请求的消费者组ID是否有效
     * @param groupIds 要验证的消费者组ID集合
     * @throws IllegalArgumentException 如果包含非预期的消费者组ID
     */
    private void validateKeys(
        Set<CoordinatorKey> groupIds
    ) {
        // 确保groupIds集合只包含当前处理器要操作的消费者组ID
        if (!groupIds.equals(Collections.singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + Collections.singleton(groupId) + ")");
        }
    }

    /**
     * 构建批量移除成员的请求
     * @param coordinatorId 协调器ID
     * @param groupIds 要操作的消费者组ID集合
     * @return 返回LeaveGroupRequest.Builder实例
     */
    @Override
    public LeaveGroupRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> groupIds) {
        // 验证消费者组ID的有效性
        validateKeys(groupIds);
        // 创建LeaveGroup请求，包含组ID和要移除的成员列表
        return new LeaveGroupRequest.Builder(groupId.idValue, members);
    }

    /**
     * 处理移除成员的响应
     * @param coordinator 协调器节点
     * @param groupIds 请求的消费者组ID集合
     * @param abstractResponse 服务器的响应
     * @return 返回API处理结果
     */
    @Override
    public ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        // 验证消费者组ID的有效性
        validateKeys(groupIds);
        // 转换为LeaveGroupResponse类型
        final LeaveGroupResponse response = (LeaveGroupResponse) abstractResponse;

        // 获取顶层错误码
        final Errors error = response.topLevelError();
        if (error != Errors.NONE) {
            // 如果存在顶层错误，创建失败结果集合
            final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
            final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

            // 处理组级别的错误
            handleGroupError(groupId, error, failed, groupsToUnmap);

            // 返回错误结果
            return new ApiResult<>(Collections.emptyMap(), failed, new ArrayList<>(groupsToUnmap));
        } else {
            // 如果没有顶层错误，处理每个成员的响应
            final Map<MemberIdentity, Errors> memberErrors = new HashMap<>();
            for (MemberResponse memberResponse : response.memberResponses()) {
                // 将每个成员的响应结果添加到映射中
                memberErrors.put(new MemberIdentity()
                                     .setMemberId(memberResponse.memberId())
                                     .setGroupInstanceId(memberResponse.groupInstanceId()),
                                 Errors.forCode(memberResponse.errorCode()));
            }

            // 返回成功结果
            return ApiResult.completed(groupId, memberErrors);
        }
    }

    /**
     * 处理组级别的错误
     * @param groupId 消费者组ID
     * @param error 错误类型
     * @param failed 存储失败操作的Map
     * @param groupsToUnmap 存储需要重新查找协调器的消费者组集合
     */
    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                // 组授权失败，记录错误并标记为失败
                log.debug("`LeaveGroup` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
                // 如果协调器正在加载状态，需要重试
                log.debug("`LeaveGroup` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // 如果协调器不可用或发生协调器变更，需要重新查找协调器
                log.debug("`LeaveGroup` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                // 处理未预期的错误
                log.error("`LeaveGroup` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }

}
