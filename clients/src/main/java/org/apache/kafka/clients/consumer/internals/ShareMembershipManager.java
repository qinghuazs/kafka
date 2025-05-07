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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * 共享消费者组成员管理器
 * 用于管理单个消费者的组成员身份，该消费者在配置中定义了组ID
 * {@link ConsumerConfig#GROUP_ID_CONFIG}，并使用共享组协议在调用subscribe API时
 * 自动分配分区。
 *
 * 状态管理：
 * 1. 当subscribe API未被调用（或消费者调用了unsubscribe）时，此管理器
 *    仅负责将成员保持在{@link MemberState#UNSUBSCRIBED}状态，不加入组。
 *
 * 2. 当调用消费者subscribe API时，此管理器将使用{@link #groupId()}加入
 *    共享组，并基于共享组协议心跳，处理成员的完整生命周期，包括：
 *    - 加入组
 *    - 协调分配
 *    - 处理致命错误
 *    - 离开组
 *
 * 协调过程：
 * 成员接受从broker接收的所有分配，从元数据解析主题名称，协调已解析的分配，
 * 并保持未解析的分配等待元数据更新时进行协调。已解析分配的协调按顺序执行，
 * 并在完成时向服务器确认。由于协调过程涉及多个异步操作，成员将继续发送心跳，
 * 以确保在协调期间保持在组中。
 *
 * 协调步骤：
 * 1. 解析目标分配中收到的所有主题ID的主题名称。在元数据中找到的主题名称
 *    随后可以进行协调。未找到的主题ID保持未解析状态，成员请求元数据更新
 *    直到解析它们（或broker将其从目标分配中移除）。
 *
 * 2. 当上述步骤完成时，成员确认已协调的分配，这是目标分配的子集，已从
 *    元数据中解析并实际协调。确认通过向broker发送心跳请求来执行。
 */
public class ShareMembershipManager extends AbstractMembershipManager<ShareGroupHeartbeatResponse> {

    /**
     * 成员的机架ID（如果指定）
     * 用于支持机架感知的分区分配
     */
    protected final String rackId;

    /**
     * 构造函数
     * 创建一个新的共享成员管理器实例
     *
     * @param logContext 日志上下文
     * @param groupId 消费者组ID
     * @param rackId 机架ID
     * @param subscriptions 订阅状态
     * @param metadata 消费者元数据
     * @param time 时间实例
     * @param metrics 指标收集器
     */
    public ShareMembershipManager(LogContext logContext,
                                  String groupId,
                                  String rackId,
                                  SubscriptionState subscriptions,
                                  ConsumerMetadata metadata,
                                  Time time,
                                  Metrics metrics) {
        // 调用内部构造函数，创建新的重平衡指标管理器
        this(logContext,
                groupId,
                rackId,
                subscriptions,
                metadata,
                time,
                new ShareRebalanceMetricsManager(metrics));
    }

    /**
     * 内部构造函数（用于测试）
     * 允许注入自定义的重平衡指标管理器
     *
     * @param logContext 日志上下文
     * @param groupId 消费者组ID
     * @param rackId 机架ID
     * @param subscriptions 订阅状态
     * @param metadata 消费者元数据
     * @param time 时间实例
     * @param metricsManager 重平衡指标管理器
     */
    ShareMembershipManager(LogContext logContext,
                           String groupId,
                           String rackId,
                           SubscriptionState subscriptions,
                           ConsumerMetadata metadata,
                           Time time,
                           ShareRebalanceMetricsManager metricsManager) {
        // 调用父类构造函数初始化基本成员管理功能
        super(groupId,
                subscriptions,
                metadata,
                logContext.logger(ShareMembershipManager.class),
                time,
                metricsManager);
        // 初始化机架ID
        this.rackId = rackId;
    }

    /**
     * 获取成员的机架ID
     *
     * @return 机架ID（如果已指定）
     */
    public String rackId() {
        return rackId;
    }

    /**
     * 处理心跳成功响应
     * 处理从broker接收到的心跳响应，更新成员状态和分配
     *
     * @param response 心跳响应
     */
    @Override
    public void onHeartbeatSuccess(ShareGroupHeartbeatResponse response) {
        // 获取响应数据
        ShareGroupHeartbeatResponseData responseData = response.data();
        
        // 检查响应中是否有错误
        if (responseData.errorCode() != Errors.NONE.code()) {
            // 如果有错误，构造错误消息并抛出异常
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(responseData.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }

        // 获取当前成员状态
        MemberState state = state();

        // 如果成员正在离开组，忽略心跳响应
        if (state == MemberState.LEAVING) {
            log.debug("Ignoring heartbeat response received from broker. Member {} with epoch {} is " +
                    "already leaving the group.", memberId, memberEpoch);
            return;
        }

        // 如果成员已取消订阅且正在完成离开操作
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.debug("Member {} with epoch {} received a successful response to the heartbeat " +
                    "to leave the group and completed the leave operation. ", memberId, memberEpoch);
            return;
        }

        // 如果成员不在组中，忽略心跳响应
        if (isNotInGroup()) {
            log.debug("Ignoring heartbeat response received from broker. Member {} is in {} state" +
                    " so it's not a member of the group. ", memberId, state);
            return;
        }

        // 更新成员世代
        updateMemberEpoch(responseData.memberEpoch());

        // 获取新的分配信息
        ShareGroupHeartbeatResponseData.Assignment assignment = responseData.assignment();

        // 如果有新的分配
        if (assignment != null) {
            // 检查成员当前状态是否可以处理新分配
            if (!state.canHandleNewAssignment()) {
                // 如果不能处理新分配（例如，正准备离开组），记录日志并返回
                log.debug("Ignoring new assignment {} received from server because member is in {} state.",
                        assignment, state);
                return;
            }

            // 创建新的分配映射，将主题分区信息转换为内部格式
            Map<Uuid, SortedSet<Integer>> newAssignment = new HashMap<>();
            // 遍历所有主题分区，构建分配映射
            assignment.topicPartitions().forEach(topicPartition -> 
                newAssignment.put(topicPartition.topicId(), new TreeSet<>(topicPartition.partitions())));
            // 处理接收到的分配
            processAssignmentReceived(newAssignment);
        }
    }

    /**
     * 获取加入组时使用的世代值
     *
     * @return 加入组的世代值
     */
    @Override
    public int joinGroupEpoch() {
        return ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH;
    }

    /**
     * 获取离开组时使用的世代值
     *
     * @return 离开组的世代值
     */
    @Override
    public int leaveGroupEpoch() {
        return ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
    }
}
