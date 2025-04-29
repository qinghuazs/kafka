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
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
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

/**
 * 删除消费者组的处理器类
 * 继承自AdminApiHandler.Batched，用于处理批量删除消费者组的请求
 * 主要功能：
 * 1. 构建删除消费者组的请求
 * 2. 处理删除响应
 * 3. 处理各种错误情况
 */
public class DeleteConsumerGroupsHandler extends AdminApiHandler.Batched<CoordinatorKey, Void> {

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
     * @param logContext 日志上下文，用于创建日志记录器
     */
    public DeleteConsumerGroupsHandler(
        LogContext logContext
    ) {
        // 初始化日志记录器
        this.log = logContext.logger(DeleteConsumerGroupsHandler.class);
        // 创建GROUP类型的协调器查找策略
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    /**
     * 获取API名称
     * @return 返回"deleteConsumerGroups"作为API标识
     */
    @Override
    public String apiName() {
        return "deleteConsumerGroups";
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
     * @param groupIds 要删除的消费者组ID集合
     * @return 返回一个新的Future实例，用于异步处理删除请求
     */
    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Void> newFuture(
        Collection<String> groupIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    /**
     * 构建协调器键集合
     * @param groupIds 消费者组ID集合
     * @return 返回由消费者组ID转换而来的CoordinatorKey集合
     */
    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        // 将每个消费者组ID转换为对应的CoordinatorKey
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    /**
     * 构建批量删除请求
     * @param coordinatorId 协调器ID
     * @param keys 要删除的消费者组的协调器键集合
     * @return 返回DeleteGroupsRequest.Builder实例
     */
    @Override
    public DeleteGroupsRequest.Builder buildBatchedRequest(
        int coordinatorId,
        Set<CoordinatorKey> keys
    ) {
        // 提取所有消费者组ID
        List<String> groupIds = keys.stream().map(key -> key.idValue).collect(Collectors.toList());
        // 创建删除请求数据
        DeleteGroupsRequestData data = new DeleteGroupsRequestData()
            .setGroupsNames(groupIds);
        return new DeleteGroupsRequest.Builder(data);
    }

    /**
     * 处理删除响应
     * @param coordinator 协调器节点
     * @param groupIds 请求的消费者组ID集合
     * @param abstractResponse 服务器的响应
     * @return 返回API处理结果
     */
    @Override
    public ApiResult<CoordinatorKey, Void> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        // 转换为DeleteGroupsResponse类型
        final DeleteGroupsResponse response = (DeleteGroupsResponse) abstractResponse;
        // 存储成功完成的删除操作
        final Map<CoordinatorKey, Void> completed = new HashMap<>();
        // 存储失败的删除操作及其异常
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        // 存储需要重新查找协调器的消费者组
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        // 处理每个消费者组的删除结果
        for (DeletableGroupResult deletedGroup : response.data().results()) {
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(deletedGroup.groupId());
            Errors error = Errors.forCode(deletedGroup.errorCode());
            if (error != Errors.NONE) {
                // 处理错误情况
                handleError(groupIdKey, error, failed, groupsToUnmap);
                continue;
            }

            // 记录成功删除的消费者组
            completed.put(groupIdKey, null);
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    /**
     * 处理删除操作中的错误
     * @param groupId 消费者组ID
     * @param error 错误类型
     * @param failed 存储失败操作的Map
     * @param groupsToUnmap 存储需要重新查找协调器的消费者组集合
     */
    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED: // 组授权失败
            case INVALID_GROUP_ID: // 无效的组ID
            case NON_EMPTY_GROUP: // 组不为空
            case GROUP_ID_NOT_FOUND: // 未找到组ID
                log.debug("`DeleteConsumerGroups` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;

            case COORDINATOR_LOAD_IN_PROGRESS: // 协调器正在加载中
                // 如果协调器正在加载状态，需要重试
                log.debug("`DeleteConsumerGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE: // 协调器不可用
            case NOT_COORDINATOR: // 不是协调器
                // 如果协调器不可用或发生了协调器变更，需要重新查找协调器
                log.debug("`DeleteConsumerGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default: // 其他未预期的错误
                log.error("`DeleteConsumerGroups` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }

}
