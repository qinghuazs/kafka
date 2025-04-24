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

import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 跟踪单个成员与消费者组之间的关系状态
 * <p/>
 * 主要职责:
 * <ul>
 *   <li>维护成员状态</li>
 *   <li>维护成员的任务分配</li>
 *   <li>协调任务分配，例如在分配新任务之前需要先撤销某些任务</li>
 *   <li>调用Streams客户端的分配和撤销回调</li>
 * </ul>
 */
public class StreamsMembershipManager implements RequestManager {

    /**
     * 表示streams组中成员的当前任务分配和目标任务分配的数据结构
     * <p/>
     * 除了分配的任务外，还包含一个本地epoch，每当分配发生变化时都会递增，
     * 这确保了即使两个分配具有相同的任务，只要epoch不同就不会被认为是相等的。
     * 这种设计有助于追踪任务分配的变更历史和确保分配的一致性。
     */
    private static class LocalAssignment {
        /**
         * 表示无效epoch的常量值
         */
        public static final long NONE_EPOCH = -1;

        /**
         * 表示空任务分配的常量实例，用于初始化或重置任务分配
         */
        public static final LocalAssignment NONE = new LocalAssignment(
            NONE_EPOCH,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        /**
         * 本地epoch，用于追踪任务分配的版本
         */
        public final long localEpoch;

        /**
         * 活动任务映射，key为子拓扑ID，value为该子拓扑下的分区集合
         */
        public final Map<String, SortedSet<Integer>> activeTasks;

        /**
         * 备用任务映射，用于故障转移场景
         */
        public final Map<String, SortedSet<Integer>> standbyTasks;

        /**
         * 预热任务映射，用于任务迁移时的预热阶段
         */
        public final Map<String, SortedSet<Integer>> warmupTasks;

        /**
         * 构造函数
         * @param localEpoch 本地epoch值
         * @param activeTasks 活动任务映射
         * @param standbyTasks 备用任务映射
         * @param warmupTasks 预热任务映射
         * @throws IllegalArgumentException 当epoch为NONE但存在任务分配时抛出
         */
        public LocalAssignment(final long localEpoch,
                               final Map<String, SortedSet<Integer>> activeTasks,
                               final Map<String, SortedSet<Integer>> standbyTasks,
                               final Map<String, SortedSet<Integer>> warmupTasks) {
            // 初始化各个字段
            this.localEpoch = localEpoch;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.warmupTasks = warmupTasks;
            
            // 如果epoch为NONE但任务集合不为空，则抛出异常
            if (localEpoch == NONE_EPOCH &&
                    (!activeTasks.isEmpty() || !standbyTasks.isEmpty() || !warmupTasks.isEmpty())) {
                throw new IllegalArgumentException("Local epoch must be set if tasks are assigned.");
            }
        }

        /**
         * 使用新的任务分配更新当前实例
         * @param activeTasks 新的活动任务映射
         * @param standbyTasks 新的备用任务映射
         * @param warmupTasks 新的预热任务映射
         * @return 如果任务分配有变化，返回包含新分配的Optional；否则返回空Optional
         */
        Optional<LocalAssignment> updateWith(final Map<String, SortedSet<Integer>> activeTasks,
                                             final Map<String, SortedSet<Integer>> standbyTasks,
                                             final Map<String, SortedSet<Integer>> warmupTasks) {
            // 检查是否需要更新：当前epoch不为NONE且所有任务集合都相同时，不需要更新
            if (localEpoch != NONE_EPOCH &&
                    activeTasks.equals(this.activeTasks) &&
                    standbyTasks.equals(this.standbyTasks) &&
                    warmupTasks.equals(this.warmupTasks)) {
                return Optional.empty();
            }

            // 创建新的分配实例，epoch加1
            long nextLocalEpoch = localEpoch + 1;
            return Optional.of(new LocalAssignment(nextLocalEpoch, activeTasks, standbyTasks, warmupTasks));
        }

        @Override
        public String toString() {
            return "LocalAssignment{" +
                "localEpoch=" + localEpoch +
                ", activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                ", warmupTasks=" + warmupTasks +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LocalAssignment that = (LocalAssignment) o;
            return localEpoch == that.localEpoch &&
                Objects.equals(activeTasks, that.activeTasks) &&
                Objects.equals(standbyTasks, that.standbyTasks) &&
                Objects.equals(warmupTasks, that.warmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(localEpoch, activeTasks, standbyTasks, warmupTasks);
        }
    }

    /**
     * 基于主题名称和分区的TopicPartition比较器
     * 用于对TopicPartition对象进行排序和比较
     */
    static final Utils.TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new Utils.TopicPartitionComparator();

    private final Logger log;

    /**
     * Streams重平衡协议事件处理器
     * 处理Streams重平衡协议中的事件，例如处理任务分配/撤销回调的调用请求
     * 在重平衡过程中负责协调任务的分配和撤销操作
     */
    private final StreamsRebalanceEventsProcessor streamsRebalanceEventsProcessor;

    /**
     * Streams重平衡协议所需的数据
     * 包含参与Streams重平衡协议所需的各种数据和状态信息
     * 用于维护重平衡过程中的协议状态
     */
    private final StreamsRebalanceData streamsRebalanceData;

    /**
     * 订阅状态对象
     * 保存成员当前在Streams应用程序拓扑中的任务分配信息
     * 用于跟踪和管理成员的订阅状态和分配的任务
     */
    private final SubscriptionState subscriptionState;

    /**
     * 成员当前状态
     * 表示该成员在消费者组中的当前状态，由{@link MemberState}定义
     * 用于追踪成员在组内的生命周期状态
     */
    private MemberState state;

    /**
     * Streams组ID
     * 创建当前成员管理器时提供的Streams组的标识符
     * 用于标识该成员所属的消费者组
     */
    private final String groupId;

    /**
     * 成员ID
     * 由消费者在启动时生成，在组内唯一且在进程生命周期内保持一致
     * 作为消费者进程的化身标识符，即使成员离开并重新加入组也不会重置或更改
     * 直到进程完全停止或终止前都保持不变
     */
    private final String memberId = Uuid.randomUuid().toString();

    /**
     * 组实例ID
     * 用于静态成员的组实例ID，创建成员管理器时提供
     * 静态成员可以在重启后重用相同的组实例ID来恢复之前的分配
     */
    private final Optional<String> groupInstanceId = Optional.empty();

    /**
     * 成员当前的epoch值
     * 初始设置为0，用于加入组时的心跳请求
     * 由服务器维护，当成员确认收到的分配时递增
     * 如果成员被隔离(fenced)则重置为0
     */
    private int memberEpoch = 0;

    /**
     * 离组操作进度
     * 当成员调用{@link #leaveGroup()}或{@link #leaveGroupOnClose()}离开组时
     * 此Future在离组操作完成时(回调执行完毕且发送离组心跳请求)完成
     * 如果成员不在离开过程中则为空
     */
    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    /**
     * 过期成员释放分配完成的Future
     * 当成员因轮询计时器过期而离开组后，用于等待其完成释放分配
     * 确保只有在完成释放分配后，成员才能在计时器重置时重新加入组
     */
    private CompletableFuture<Void> staleMemberAssignmentRelease;

    /**
     * 协调进度标志
     * 表示是否有正在进行的协调操作(回调)
     * 当在收到心跳响应或元数据更新后触发{@link #maybeReconcile()}时设置为true
     */
    private boolean reconciliationInProgress;

    /**
     * 协调中重新加入标志
     * 如果在协调过程中成员重新加入了组则为true
     * 用于判断正在进行的协调是否应该被中断而不应用
     */
    private boolean rejoinedWhileReconciliationInProgress;

    /**
     * 状态更新监听器列表
     * 注册的监听器会在成员epoch更新时收到通知
     * 包括从broker收到有效值，或因成员离开组、被隔离或失败而清除值的情况
     */
    private final List<MemberStateListener> stateUpdatesListeners = new ArrayList<>();

    /**
     * 目标任务分配
     * 最近一次从服务器收到的任务分配，包含其本地epoch
     * 每次收到新的分配时都会更新此变量
     * 当不在组中时等于LocalAssignment.NONE
     */
    private LocalAssignment targetAssignment = LocalAssignment.NONE;

    /**
     * 当前任务分配
     * 成员从服务器收到并成功处理的分配，包含其本地epoch
     * 当不在组中或尚未协调任何分配时等于LocalAssignment.NONE
     */
    private LocalAssignment currentAssignment = LocalAssignment.NONE;

    /**
     * 订阅更新标志
     * 用于追踪订阅是否已更新的原子布尔值
     * 如果为true且订阅状态为UNSUBSCRIBED，下一次{@link #onConsumerPoll()}将使成员状态变为JOINING
     */
    private final AtomicBoolean subscriptionUpdated = new AtomicBoolean(false);

    /**
     * 重平衡指标管理器
     * 用于测量成功重平衡的延迟和失败重平衡的次数
     */
    private final RebalanceMetricsManager metricsManager;

    private final Time time;

    /**
     * 轮询计时器过期标志
     * 当调用{@link #transitionToSendingLeaveGroup(boolean)}时dueToExpiredPollTimer参数为true时设置
     * 用于判断成员在离开组后是否应该转换到STALE状态
     * 在该状态下释放其分配并等待计时器重置
     */
    private boolean isPollTimerExpired;

    /**
     * 构造Streams成员管理器
     * <p>
     * 该管理器负责维护Kafka Streams应用程序中单个成员的状态和任务分配。它处理成员的生命周期事件，
     * 包括加入组、接收任务分配、协调任务转移以及离开组等操作。
     *
     * @param groupId                           消费者组的ID，用于标识成员所属的组
     * @param streamsRebalanceEventsProcessor   Streams重平衡事件处理器，负责处理任务分配/撤销等回调请求
     * @param streamsRebalanceData              参与Streams重平衡协议所需的数据
     * @param subscriptionState                 成员的订阅状态，维护当前分配的任务信息
     * @param logContext                        日志上下文，用于创建日志记录器
     * @param time                              时间服务，用于时间相关的操作
     * @param metrics                           度量指标管理器，用于监控重平衡性能
     */
    public StreamsMembershipManager(final String groupId,
                                    final StreamsRebalanceEventsProcessor streamsRebalanceEventsProcessor,
                                    final StreamsRebalanceData streamsRebalanceData,
                                    final SubscriptionState subscriptionState,
                                    final LogContext logContext,
                                    final Time time,
                                    final Metrics metrics) {
        // 初始化日志记录器
        log = logContext.logger(StreamsMembershipManager.class);
        // 设置初始状态为未订阅
        this.state = MemberState.UNSUBSCRIBED;
        // 保存组ID
        this.groupId = groupId;
        // 设置重平衡事件处理器
        this.streamsRebalanceEventsProcessor = streamsRebalanceEventsProcessor;
        // 设置重平衡数据
        this.streamsRebalanceData = streamsRebalanceData;
        // 设置订阅状态
        this.subscriptionState = subscriptionState;
        // 创建重平衡指标管理器
        metricsManager = new ConsumerRebalanceMetricsManager(metrics);
        // 设置时间服务
        this.time = time;
    }

    /**
     * 获取成员所属的消费者组ID
     * 
     * @return 当前成员所属(或打算加入)的消费者组ID
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 获取成员ID
     * <p>
     * 成员ID在进程启动时生成，在整个进程生命周期内保持不变。即使成员离开并重新加入组，
     * ID也不会改变，直到进程完全停止。
     * 
     * @return 成员的唯一标识符
     */
    public String memberId() {
        return memberId;
    }

    /**
     * 获取组实例ID
     * <p>
     * 组实例ID用于支持静态成员身份。静态成员可以在重启后使用相同的实例ID重新加入组，
     * 从而恢复之前的分配状态。
     * 
     * @return 如果是静态成员则返回组实例ID，否则返回空
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * 获取成员当前的epoch值
     * <p>
     * epoch由服务器维护，用于追踪成员的状态变更。当成员确认收到新的分配时会递增，
     * 如果成员被隔离(fenced)则重置为0。
     * 
     * @return 当前的成员epoch值
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * 获取成员当前状态
     * <p>
     * 返回成员在消费者组中的当前状态，状态定义在{@link MemberState}中。
     * 状态反映了成员的生命周期阶段，如未订阅、加入中、稳定运行等。
     * 
     * @return 当前的成员状态
     */
    public MemberState state() {
        return state;
    }

    /**
     * 检查成员是否正在离开组
     * <p>
     * 当成员处于PREPARE_LEAVING(等待回调完成)或LEAVING(发送最后的心跳)状态时，
     * 表示正在进行离组操作。这用于在轮询计时器过期时避免重复触发离组操作。
     * 
     * @return 如果成员正在离开组则返回true
     */
    public boolean isLeavingGroup() {
        return state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING;
    }

    /**
     * 检查成员是否不在组中
     * <p>
     * 当成员处于以下状态时被认为不在组中：
     * - UNSUBSCRIBED: 未订阅任何主题
     * - FENCED: 被服务器隔离
     * - FATAL: 发生了致命错误
     * - STALE: 因轮询超时而过期
     * 
     * @return 如果成员不在组中则返回true
     */
    private boolean isNotInGroup() {
        return state == MemberState.UNSUBSCRIBED ||
            state == MemberState.FENCED ||
            state == MemberState.FATAL ||
            state == MemberState.STALE;
    }

    /**
     * 注册状态变更监听器
     * <p>
     * 监听器会在以下情况被调用：
     * - 成员状态发生变化
     * - 收到新的成员ID
     * - 收到新的epoch值
     * 
     * @param listener 要注册的监听器
     * @throws NullPointerException 如果监听器为null
     * @throws IllegalArgumentException 如果监听器已经注册
     */
    public void registerStateListener(MemberStateListener listener) {
        // 检查监听器是否为null
        Objects.requireNonNull(listener, "State updates listener cannot be null");
        // 检查监听器是否已注册
        for (MemberStateListener registeredListener : stateUpdatesListeners) {
            if (registeredListener == listener) {
                throw new IllegalArgumentException("Listener is already registered.");
            }
        }
        // 添加监听器到列表
        stateUpdatesListeners.add(listener);
    }

    /**
     * 当成员的epoch值更新时通知所有已注册的监听器
     * <p>
     * 通知内容包括成员ID。如果成员失败或离开组，将使用空epoch值调用此方法。
     * 此方法在以下情况下被调用：
     * <ul>
     *   <li>成员成功加入组并收到新的epoch值</li>
     *   <li>成员被隔离或发生错误导致epoch重置</li>
     *   <li>成员主动离开组时清除epoch</li>
     * </ul>
     *
     * @param epoch 更新后的epoch值，如果成员离开组则为空
     */
    private void notifyEpochChange(Optional<Integer> epoch) {
        // 遍历所有状态更新监听器，调用其onMemberEpochUpdated方法
        stateUpdatesListeners.forEach(stateListener -> stateListener.onMemberEpochUpdated(epoch, memberId));
    }

    /**
     * 当分配的分区集合发生变化时通知所有监听器
     * <p>
     * 在以下情况下会触发通知：
     * <ul>
     *   <li>收到新的分区分配</li>
     *   <li>取消订阅时清除分配</li>
     *   <li>离开组时释放分配</li>
     * </ul>
     *
     * @param partitions 更新后的分区集合
     */
    void notifyAssignmentChange(Set<TopicPartition> partitions) {
        // 遍历所有状态更新监听器，调用其onGroupAssignmentUpdated方法
        stateUpdatesListeners.forEach(stateListener -> stateListener.onGroupAssignmentUpdated(partitions));
    }

    /**
     * 将成员状态转换为JOINING，表示成员将在下一次心跳请求时尝试加入组
     * <p>
     * 此方法在以下情况下被调用：
     * <ul>
     *   <li>用户调用subscribe API时</li>
     *   <li>成员被隔离后希望重新加入组时</li>
     * </ul>
     * 方法可见性为包私有，用于测试。
     */
    private void transitionToJoining() {
        // 如果成员处于FATAL状态，记录警告并返回
        if (state == MemberState.FATAL) {
            log.warn("No action taken to join the group with the updated subscription because " +
                "the member is in FATAL state");
            return;
        }
        // 如果正在进行协调，设置标志表示在协调过程中重新加入
        if (reconciliationInProgress) {
            rejoinedWhileReconciliationInProgress = true;
        }
        // 重置epoch值
        resetEpoch();
        // 转换到JOINING状态
        transitionTo(MemberState.JOINING);
        // 清除当前的任务分配
        clearCurrentTaskAssignment();
    }

    /**
     * 重置成员epoch值为离开组心跳请求所需的值，并转换到LEAVING状态
     * <p>
     * 状态转换后将发送包含新epoch值的心跳请求。此方法处理两种离组场景：
     * <ul>
     *   <li>正常离组：完成后直接离开组</li>
     *   <li>因轮询超时离组：完成后转入STALE状态等待计时器重置</li>
     * </ul>
     *
     * @param dueToExpiredPollTimer 如果为true，表示离组是由于轮询计时器过期导致。
     *                              这种情况下成员在离开组后将保持STALE状态，
     *                              直到释放其分配且计时器被重置。
     */
    private void transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer) {
        // 如果成员处于FATAL状态，记录警告并返回
        if (state == MemberState.FATAL) {
            log.warn("Member {} with epoch {} won't send leave group request because it is in " +
                "FATAL state", memberId, memberEpoch);
            return;
        }
        // 如果成员已经不在组中，记录警告并返回
        if (state == MemberState.UNSUBSCRIBED) {
            log.warn("Member {} won't send leave group request because it is already out of the group.",
                memberId);
            return;
        }

        // 如果是因轮询超时导致的离组
        if (dueToExpiredPollTimer) {
            // 设置轮询计时器过期标志
            isPollTimerExpired = true;
            // 短暂转换到PREPARE_LEAVING状态
            // 由于成员已过期，不需要在发送离组请求前释放分配
            // 将在STALE状态下发送离组请求后调用onAllTasksLost
            transitionTo(MemberState.PREPARE_LEAVING);
        }
        // 完成离组准备工作
        finalizeLeaving();
        // 转换到LEAVING状态
        transitionTo(MemberState.LEAVING);
    }

    /**
     * 完成离组前的准备工作
     * <p>
     * 主要执行两个操作：
     * <ul>
     *   <li>更新成员epoch为离组专用值</li>
     *   <li>清除当前的任务分配</li>
     * </ul>
     */
    private void finalizeLeaving() {
        // 将成员epoch更新为离组专用值
        updateMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH);
        // 清除当前的任务分配
        clearCurrentTaskAssignment();
    }

    /**
     * 因轮询计时器过期而离组后，转换到STALE状态以释放分配
     * <p>
     * 此方法的主要职责：
     * <ul>
     *   <li>转换到STALE状态</li>
     *   <li>触发onAllTasksLost回调</li>
     *   <li>等待回调完成后清除任务和分区分配</li>
     *   <li>保持STALE状态直到应用程序轮询事件重置计时器</li>
     * </ul>
     * 
     * 成员将保持STALE状态直到{@link #maybeRejoinStaleMember()}被调用。
     */
    private void transitionToStale() {
        // 转换到STALE状态
        transitionTo(MemberState.STALE);

        // 请求执行onAllTasksLost回调
        final CompletableFuture<Void> onAllTasksLostCallbackExecution =
            streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
        // 设置回调完成后的处理
        staleMemberAssignmentRelease = onAllTasksLostCallbackExecution.whenComplete((result, error) -> {
            // 如果回调执行失败，记录错误
            if (error != null) {
                log.error("Task revocation callback invocation failed " +
                    "after member left group due to expired poll timer.", error);
            }
            // 清除任务和分区分配
            clearTaskAndPartitionAssignment();
            // 记录调试信息
            log.debug("Member {} sent leave group heartbeat and released its assignment. It will remain " +
                    "in {} state until the poll timer is reset, and it will then rejoin the group",
                memberId, MemberState.STALE);
        });
    }

    /**
     * 将成员状态转换为FATAL状态并更新成员信息
     * <p>
     * 当发生不可恢复的错误时调用此方法，例如心跳请求返回不可重试的错误。
     * 在FATAL状态下，成员将：
     * <ul>
     *   <li>清除其epoch值</li>
     *   <li>释放所有任务分配</li>
     *   <li>通知所有监听器状态变更</li>
     * </ul>
     */
    public void transitionToFatal() {
        // 保存当前状态用于后续判断
        MemberState previousState = state;
        // 转换到FATAL状态
        transitionTo(MemberState.FATAL);
        // 记录错误日志
        log.error("Member {} with epoch {} transitioned to fatal state", memberId, memberEpoch);
        // 清除epoch并通知监听器
        notifyEpochChange(Optional.empty());

        // 如果之前是UNSUBSCRIBED状态，说明已经离开组，不需要触发onAllTasksLost回调
        if (previousState == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                "left the group, so onAllTasksLost callback won't be triggered.", memberId, memberEpoch);
            return;
        }

        // 如果之前正在离开组，放弃离开过程，保持在FATAL状态
        if (previousState == MemberState.LEAVING || previousState == MemberState.PREPARE_LEAVING) {
            log.info("Member {} with epoch {} was leaving the group with state {} when it got a " +
                "fatal error from the broker. It will discard the ongoing leave and remain in " +
                "fatal state.", memberId, memberEpoch, previousState);
            maybeCompleteLeaveInProgress();
            return;
        }

        // 请求执行onAllTasksLost回调，通知应用程序所有任务都已丢失
        CompletableFuture<Void> onAllTasksLostCallbackExecuted = streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
        // 回调完成后，无论成功失败都清除任务分配
        onAllTasksLostCallbackExecuted.whenComplete((result, error) -> {
            if (error != null) {
                log.error("onAllTasksLost callback invocation failed while releasing assignment " +
                    "after member failed with fatal error.", error);
            }
            clearTaskAndPartitionAssignment();
        });
    }

    /**
     * 处理心跳请求被跳过的情况
     * <p>
     * 即使心跳请求未能发送，也要确保成员能从{@link MemberState#LEAVING}状态转出。
     * 这是一个尽力而为的机制，即使无法发送离组请求，也要避免成员被阻塞在LEAVING状态。
     * <p>
     * 应用场景：
     * <ul>
     *   <li>协调器不可用或未知时</li>
     *   <li>网络问题导致无法发送请求时</li>
     * </ul>
     */
    public void onHeartbeatRequestSkipped() {
        // 只有在LEAVING状态时才需要处理
        if (state == MemberState.LEAVING) {
            // 记录警告日志
            log.warn("Heartbeat to leave group cannot be sent (most probably due to coordinator " +
                    "not known/available). Member {} with epoch {} will transition to {}.",
                memberId, memberEpoch, MemberState.UNSUBSCRIBED);
            // 直接转换到UNSUBSCRIBED状态
            transitionTo(MemberState.UNSUBSCRIBED);
            // 完成离组操作
            maybeCompleteLeaveInProgress();
        }
    }

    /**
     * 更新成员状态，仅在状态转换有效时才设置为新状态
     * <p>
     * 状态转换的有效性由{@link MemberState}中定义的状态机规则决定。
     * 每次状态转换时都会：
     * <ul>
     *   <li>验证转换的合法性</li>
     *   <li>记录重平衡相关的指标</li>
     *   <li>记录状态变更日志</li>
     * </ul>
     *
     * @param nextState 目标状态
     * @throws IllegalStateException 如果从当前状态到目标状态的转换不被允许
     */
    private void transitionTo(MemberState nextState) {
        // 验证状态转换的合法性：如果状态发生变化，则新状态必须在当前状态的有效后继状态集合中
        if (!state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            throw new IllegalStateException(String.format("Invalid state transition from %s to %s",
                state, nextState));
        }

        // 如果是完成重平衡，记录重平衡结束时间
        if (isCompletingRebalance(state, nextState)) {
            metricsManager.recordRebalanceEnded(time.milliseconds());
        }
        // 如果是开始重平衡，记录重平衡开始时间
        if (isStartingRebalance(state, nextState)) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
        }

        // 记录状态转换日志
        log.info("Member {} with epoch {} transitioned from {} to {}.", memberId, memberEpoch, state, nextState);
        // 更新状态
        this.state = nextState;
    }

    /**
     * 判断是否正在完成重平衡过程
     * <p>
     * 当成员从RECONCILING状态转换到STABLE或ACKNOWLEDGING状态时，
     * 表示重平衡过程即将完成。这时需要记录重平衡的结束时间。
     *
     * @param currentState 当前状态
     * @param nextState 目标状态
     * @return 如果是完成重平衡则返回true
     */
    private static boolean isCompletingRebalance(MemberState currentState, MemberState nextState) {
        return currentState == MemberState.RECONCILING &&
            (nextState == MemberState.STABLE || nextState == MemberState.ACKNOWLEDGING);
    }

    /**
     * 判断是否正在开始重平衡过程
     * <p>
     * 当成员从非RECONCILING状态转换到RECONCILING状态时，
     * 表示开始了新的重平衡过程。这时需要记录重平衡的开始时间。
     *
     * @param currentState 当前状态
     * @param nextState 目标状态
     * @return 如果是开始重平衡则返回true
     */
    private static boolean isStartingRebalance(MemberState currentState, MemberState nextState) {
        return currentState != MemberState.RECONCILING && nextState == MemberState.RECONCILING;
    }

    /**
     * 重置成员的epoch值
     * <p>
     * 将epoch重置为加入组时的初始值。这通常发生在：
     * <ul>
     *   <li>成员被隔离时</li>
     *   <li>成员重新加入组时</li>
     *   <li>发生致命错误时</li>
     * </ul>
     */
    private void resetEpoch() {
        updateMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH);
    }

    /**
     * 更新成员的epoch值
     * <p>
     * epoch是由服务器维护的单调递增的值，用于：
     * <ul>
     *   <li>追踪成员的状态变更</li>
     *   <li>检测成员是否被隔离</li>
     *   <li>确保消息的顺序性</li>
     * </ul>
     *
     * @param newEpoch 新的epoch值
     */
    private void updateMemberEpoch(int newEpoch) {
        // 检查是否收到新的epoch值
        boolean newEpochReceived = this.memberEpoch != newEpoch;
        // 更新epoch
        this.memberEpoch = newEpoch;
        // 如果收到新的epoch，通知所有监听器
        if (newEpochReceived) {
            if (memberEpoch > 0) {
                // 正常的epoch更新
                notifyEpochChange(Optional.of(memberEpoch));
            } else {
                // epoch被清除，说明成员已离开组或被隔离
                notifyEpochChange(Optional.empty());
            }
        }
    }

    /**
     * 丢弃尚未协调的任务分配(等待元数据或下一个协调循环)
     * <p>
     * 此方法用于清除当前的任务分配状态，通常在以下情况下调用：
     * <ul>
     *   <li>成员离开组时</li>
     *   <li>发生错误需要重置状态时</li>
     *   <li>准备接收新的任务分配时</li>
     * </ul>
     */
    private void clearCurrentTaskAssignment() {
        // 将当前任务分配重置为空
        currentAssignment = LocalAssignment.NONE;
    }

    /**
     * 清除成员订阅中的分区分配、待处理的分配和元数据缓存
     * <p>
     * 此方法执行完整的任务和分区分配清理，包括：
     * <ul>
     *   <li>清除订阅状态中的分区分配</li>
     *   <li>通知监听器分配变更</li>
     *   <li>重置当前和目标任务分配</li>
     * </ul>
     */
    private void clearTaskAndPartitionAssignment() {
        // 将订阅状态中的分区分配设置为空集合
        subscriptionState.assignFromSubscribed(Collections.emptySet());
        // 通知所有监听器分配已变更为空
        notifyAssignmentChange(Collections.emptySet());
        // 重置当前和目标任务分配
        currentAssignment = LocalAssignment.NONE;
        targetAssignment = LocalAssignment.NONE;
    }

    /**
     * 判断成员是否应该跳过发送心跳
     * <p>
     * 当成员不是组的活跃成员时(处于UNSUBSCRIBED、FENCED、FATAL或STALE状态)，
     * 应该跳过发送心跳请求。
     *
     * @return 如果成员不是组的活跃成员则返回true
     */
    public boolean shouldSkipHeartbeat() {
        // 检查成员是否不在组中
        return isNotInGroup();
    }

    /**
     * 判断成员是否应该立即发送心跳
     * <p>
     * 在以下状态下需要立即发送心跳，而不是等待心跳间隔：
     * <ul>
     *   <li>ACKNOWLEDGING - 确认已收到并处理了新的任务分配</li>
     *   <li>LEAVING - 正在离开组</li>
     *   <li>JOINING - 正在加入组</li>
     * </ul>
     *
     * @return 如果成员需要立即发送心跳则返回true
     */
    public boolean shouldHeartbeatNow() {
        // 检查成员是否处于需要立即发送心跳的状态
        return state == MemberState.ACKNOWLEDGING || state == MemberState.LEAVING || state == MemberState.JOINING;
    }

    /**
     * 标记订阅已更新
     * <p>
     * 将{@link #subscriptionUpdated}设置为true，表示订阅已更新。这将触发以下行为：
     * <ul>
     *   <li>如果成员尚未加入组，下一次{@link #onConsumerPoll()}将使其加入组</li>
     *   <li>如果成员已在组中，确保在下一次心跳请求中包含更新后的订阅信息</li>
     * </ul>
     * <p>
     * 注意：订阅中的主题列表来自共享的订阅状态。
     */
    public void onSubscriptionUpdated() {
        // 原子操作：将subscriptionUpdated从false设置为true
        subscriptionUpdated.compareAndSet(false, true);
    }

    /**
     * 在消费者轮询时处理组加入
     * <p>
     * 此方法将{@link #transitionToJoining}与{@link #onSubscriptionUpdated}分开，
     * 以满足"重平衡只能在调用{@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)}期间发生"的要求。
     * <p>
     * 只有当订阅已更新且成员处于UNSUBSCRIBED状态时，才会触发加入组的操作。
     */
    public void onConsumerPoll() {
        // 原子操作：如果subscriptionUpdated为true则设置为false，并检查成员状态
        if (subscriptionUpdated.compareAndSet(true, false) && state == MemberState.UNSUBSCRIBED) {
            // 转换到JOINING状态，准备加入组
            transitionToJoining();
        }
    }

    /**
     * 在生成心跳请求时更新状态
     * <p>
     * 此方法处理那些在发送心跳请求后就结束的状态转换，无需等待响应：
     * <ul>
     *   <li>ACKNOWLEDGING状态：确认已处理分配</li>
     *   <li>LEAVING状态：完成离开组的操作</li>
     * </ul>
     */
    public void onHeartbeatRequestGenerated() {
        if (state == MemberState.ACKNOWLEDGING) {
            // 如果目标分配已完全协调
            if (targetAssignmentReconciled()) {
                // 转换到STABLE状态
                transitionTo(MemberState.STABLE);
            } else {
                // 如果还有新的分配需要协调，转换到RECONCILING状态
                log.debug("Member {} with epoch {} transitioned to {} after a heartbeat was sent " +
                    "to ack a previous reconciliation. New assignments are ready to " +
                    "be reconciled.", memberId, memberEpoch, MemberState.RECONCILING);
                transitionTo(MemberState.RECONCILING);
            }
        } else if (state == MemberState.LEAVING) {
            // 如果是因为轮询计时器过期而离开
            if (isPollTimerExpired) {
                log.debug("Member {} with epoch {} generated the heartbeat to leave due to expired poll timer. It will " +
                    "remain stale (no heartbeat) until it rejoins the group on the next consumer " +
                    "poll.", memberId, memberEpoch);
                // 转换到STALE状态
                transitionToStale();
            } else {
                // 正常离开组
                log.debug("Member {} with epoch {} generated the heartbeat to leave the group.", memberId, memberEpoch);
                // 转换到UNSUBSCRIBED状态
                transitionTo(MemberState.UNSUBSCRIBED);
            }
        }
    }

    /**
     * 处理成功的心跳响应
     * <p>
     * 该方法处理从broker接收到的心跳响应，主要职责包括：
     * <ul>
     *   <li>检查响应中是否存在意外错误</li>
     *   <li>根据成员当前状态处理响应</li>
     *   <li>更新成员epoch</li>
     *   <li>处理新的任务分配（如果有）</li>
     * </ul>
     *
     * @param response 包含成员信息和错误的心跳响应
     */
    public void onHeartbeatSuccess(StreamsGroupHeartbeatResponse response) {
        // 获取响应数据
        StreamsGroupHeartbeatResponseData responseData = response.data();
        // 检查响应中是否有意外错误
        throwIfUnexpectedError(responseData);

        // 如果成员正在离开组，忽略响应
        if (state == MemberState.LEAVING) {
            log.debug("Ignoring heartbeat response received from broker. Member {} with epoch {} is " +
                "already leaving the group.", memberId, memberEpoch);
            return;
        }

        // 如果成员已取消订阅且正在完成离组操作，处理离组完成
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.debug("Member {} with epoch {} received a successful response to the heartbeat " +
                "to leave the group and completed the leave operation. ", memberId, memberEpoch);
            return;
        }

        // 如果成员不在组中，忽略响应
        if (isNotInGroup()) {
            log.debug("Ignoring heartbeat response received from broker. Member {} is in {} state" +
                " so it's not a member of the group. ", memberId, state);
            return;
        }
        
        // 使用响应中的epoch更新成员epoch
        updateMemberEpoch(responseData.memberEpoch());

        // 获取新的任务分配信息
        final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks = responseData.activeTasks();
        final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks = responseData.standbyTasks();
        final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks = responseData.warmupTasks();

        // 如果所有任务列表都不为null，处理新的分配
        if (activeTasks != null && standbyTasks != null && warmupTasks != null) {
            // 检查成员当前状态是否可以处理新的分配
            if (!state.canHandleNewAssignment()) {
                log.debug("Ignoring new assignment: active tasks {}, standby tasks {}, and warm-up tasks {} received " +
                        "from server because member is in {} state.",
                    activeTasks, standbyTasks, warmupTasks, state);
                return;
            }

            // 处理收到的任务分配
            processAssignmentReceived(
                toTasksAssignment(activeTasks),
                toTasksAssignment(standbyTasks),
                toTasksAssignment(warmupTasks)
            );
        } else {
            // 检查任务列表的一致性：要么全为null，要么全不为null
            if (responseData.activeTasks() != null ||
                responseData.standbyTasks() != null ||
                responseData.warmupTasks() != null) {

                throw new IllegalStateException("Invalid response data, task collections must be all null or all non-null: "
                    + responseData);
            }
        }
    }

    /**
     * 处理心跳响应失败的情况
     * <p>
     * 该方法在心跳请求失败时被调用，主要职责包括：
     * <ul>
     *   <li>记录不可重试的重平衡失败指标</li>
     *   <li>如果是离组请求失败，完成离组操作</li>
     * </ul>
     *
     * @param retriable 如果错误是可重试的则为true
     */
    public void onHeartbeatFailure(boolean retriable) {
        // 如果错误不可重试，记录重平衡失败指标
        if (!retriable) {
            metricsManager.maybeRecordRebalanceFailed();
        }

        // 离组请求只发送一次（不重试），所以无论响应如何，
        // 一旦请求完成就应该完成离组操作
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.warn("Member {} with epoch {} received a failed response to the heartbeat to " +
                "leave the group and completed the leave operation. ", memberId, memberEpoch);
        }
    }

    /**
     * 处理轮询计时器过期事件
     * <p>
     * 当成员的轮询计时器过期时调用此方法。这表示成员在指定时间内未能完成轮询操作，
     * 可能是由于处理时间过长或应用程序暂停。此时需要将成员从组中移除，以维护组的健康状态。
     */
    public void onPollTimerExpired() {
        // 将成员状态转换为发送离组请求的状态，并标记是由于轮询计时器过期导致的
        transitionToSendingLeaveGroup(true);
    }

    /**
     * Notify when member is fenced.
     * 
     * 当成员被隔离(fenced)时的处理方法
     * <p>
     * 在Kafka Streams中，成员被隔离通常发生在以下情况：
     * <ul>
     *   <li>成员的epoch值与服务器不匹配</li>
     *   <li>成员被检测为僵尸实例</li>
     *   <li>成员的静态成员ID被其他实例使用</li>
     * </ul>
     * 
     * 该方法根据成员当前状态执行不同的处理逻辑：
     * <ul>
     *   <li>如果成员正在准备离开或已经离开组，则完成离开流程</li>
     *   <li>如果成员处于活跃状态，则释放任务分配并尝试重新加入组</li>
     * </ul>
     */
    public void onFenced() {
        // 如果成员正在准备离开组，则完成离开流程而不尝试重新加入
        if (state == MemberState.PREPARE_LEAVING) {
            log.debug("Member {} with epoch {} got fenced but it is already preparing to leave " +
                "the group, so it will stop sending heartbeat and won't attempt to send the " +
                "leave request or rejoin.", memberId, memberEpoch);
            finalizeLeaving();
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
            return;
        }

        // 如果成员正在发送离开组的心跳，则直接转换到未订阅状态
        if (state == MemberState.LEAVING) {
            log.debug("Member {} with epoch {} got fenced before sending leave group heartbeat. " +
                "It will not send the leave request and won't attempt to rejoin.", memberId, memberEpoch);
            transitionTo(MemberState.UNSUBSCRIBED);
            maybeCompleteLeaveInProgress();
            return;
        }

        // 如果成员已经处于未订阅状态，则不需要任何操作
        if (state == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fenced but it already left the group, so it " +
                "won't attempt to rejoin.", memberId, memberEpoch);
            return;
        }

        // 将成员状态转换为被隔离状态，并重置epoch值
        transitionTo(MemberState.FENCED);
        resetEpoch();
        log.debug("Member {} with epoch {} transitioned to {} state. It will release its " +
            "assignment and rejoin the group.", memberId, memberEpoch, MemberState.FENCED);

        // 请求执行onAllTasksLost回调，释放所有任务分配
        CompletableFuture<Void> callbackResult = streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
        callbackResult.whenComplete((result, error) -> {
            // 即使回调执行失败，也继续进行重新加入组的流程
            if (error != null) {
                log.error("onAllTasksLost callback invocation failed while releasing assignment" +
                    " after member got fenced. Member will rejoin the group anyways.", error);
            }
            // 清除任务和分区分配
            clearTaskAndPartitionAssignment();
            // 如果成员仍处于被隔离状态，则尝试重新加入组
            if (state == MemberState.FENCED) {
                transitionToJoining();
            } else {
                log.debug("Fenced member onAllTasksLost callback completed but the state has " +
                    "already changed to {}, so the member won't rejoin the group", state);
            }
        });
    }

    /**
     * 检查心跳响应中是否存在非预期错误
     * <p>
     * 在Streams组心跳协议中，某些错误是预期的且可以处理的（如成员被隔离），
     * 而其他错误则表示系统处于不一致状态。此方法用于检测和报告这些非预期错误。
     * 
     * @param responseData 心跳响应数据
     * @throws IllegalArgumentException 当响应中包含非预期错误时抛出
     */
    private void throwIfUnexpectedError(StreamsGroupHeartbeatResponseData responseData) {
        // 检查响应中的错误码是否不为NONE
        if (responseData.errorCode() != Errors.NONE.code()) {
            // 构造错误消息，包含错误码和错误描述
            String errorMessage = String.format(
                "Unexpected error in Heartbeat response. Expected no error, but received: %s with message: '%s'",
                Errors.forCode(responseData.errorCode()), responseData.errorMessage()
            );
            // 抛出异常，表示遇到了非预期的错误状态
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Transition a {@link MemberState#STALE} member to {@link MemberState#JOINING} when it completes
     * releasing its assignment. This is expected to be used when the poll timer is reset.
     * 
     * 当轮询计时器重置时，将过期(STALE)状态的成员转换为加入(JOINING)状态
     * <p>
     * 在Kafka Streams中，如果成员长时间未调用poll方法，会被标记为过期状态。
     * 当轮询计时器重置（例如应用程序恢复正常轮询）时，此方法允许过期成员
     * 在完成释放其之前的任务分配后重新加入组。
     * 
     * 这种设计确保了：
     * <ul>
     *   <li>任务分配的安全释放，避免出现任务重复分配</li>
     *   <li>成员可以在应用程序恢复正常后重新加入组</li>
     *   <li>维护了组成员状态的一致性</li>
     * </ul>
     */
    public void maybeRejoinStaleMember() {
        // 重置轮询计时器过期标志
        isPollTimerExpired = false;
        // 只有当成员处于过期状态时才执行重新加入
        if (state == MemberState.STALE) {
            log.debug("Expired poll timer has been reset so stale member {} will rejoin the group " +
                "when it completes releasing its previous assignment.", memberId);
            // 等待释放任务分配完成后，转换到加入状态
            staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining());
        }
    }

    /**
     * 完成正在进行的离组操作(如果有的话)
     * 当成员收到离组心跳请求的响应时，预期会调用此方法来完成离组操作
     * 
     * @return 如果存在离组操作并成功完成则返回true，否则返回false
     */
    private boolean maybeCompleteLeaveInProgress() {
        // 检查是否有正在进行的离组操作
        if (leaveGroupInProgress.isPresent()) {
            // 完成离组Future并清空引用
            leaveGroupInProgress.get().complete(null);
            leaveGroupInProgress = Optional.empty();
            return true;
        }
        return false;
    }

    /**
     * 将任务映射转换为TaskId集合
     * 用于将子拓扑ID和分区号的映射转换为TaskId对象的有序集合
     * 
     * @param tasks 任务映射，key为子拓扑ID，value为该子拓扑下的分区集合
     * @return 包含所有任务ID的有序集合
     */
    private static SortedSet<StreamsRebalanceData.TaskId> toTaskIdSet(final Map<String, SortedSet<Integer>> tasks) {
        // 创建有序集合用于存储TaskId
        SortedSet<StreamsRebalanceData.TaskId> taskIdSet = new TreeSet<>();
        // 遍历每个子拓扑的任务映射
        for (final Map.Entry<String, SortedSet<Integer>> task : tasks.entrySet()) {
            final String subtopologyId = task.getKey();
            final SortedSet<Integer> partitions = task.getValue();
            // 为每个分区创建TaskId并添加到结果集合
            for (final int partition : partitions) {
                taskIdSet.add(new StreamsRebalanceData.TaskId(subtopologyId, partition));
            }
        }
        return taskIdSet;
    }

    /**
     * 将心跳响应中的任务ID列表转换为任务分配映射
     * 
     * @param taskIds 心跳响应中的任务ID列表
     * @return 转换后的任务分配映射，key为子拓扑ID，value为该子拓扑下的分区有序集合
     */
    private static Map<String, SortedSet<Integer>> toTasksAssignment(final List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds) {
        // 使用流式处理将任务ID列表转换为映射
        return taskIds.stream()
            .collect(Collectors.toMap(StreamsGroupHeartbeatResponseData.TaskIds::subtopologyId, taskId -> new TreeSet<>(taskId.partitions())));
    }

    /**
     * 在成员关闭时离开组
     * 
     * <p>
     * 此方法执行以下操作:
     * <ol>
     *     <li>将成员状态转换为{@link MemberState#PREPARE_LEAVING}</li>
     *     <li>跳过调用撤销回调或丢失回调</li>
     *     <li>清除当前和目标分配，取消订阅所有主题，并将成员状态转换为{@link MemberState#LEAVING}</li>
     * </ol>
     * 状态{@link MemberState#PREPARE_LEAVING}和{@link MemberState#LEAVING}会导致心跳请求管理器
     * 发送离组心跳请求
     * </p>
     *
     * @return 当发送离组心跳请求后完成的Future
     */
    public CompletableFuture<Void> leaveGroupOnClose() {
        // 调用leaveGroup方法并传入true表示是由于关闭而离组
        return leaveGroup(true);
    }

    /**
     * 主动离开组
     * 
     * <p>
     * 此方法执行以下操作:
     * <ol>
     *     <li>将成员状态转换为{@link MemberState#PREPARE_LEAVING}</li>
     *     <li>请求调用撤销回调或丢失回调</li>
     *     <li>当回调完成后，清除当前和目标分配，取消订阅所有主题，
     *     并将成员状态转换为{@link MemberState#LEAVING}</li>
     * </ol>
     * 状态{@link MemberState#PREPARE_LEAVING}和{@link MemberState#LEAVING}会导致心跳请求管理器
     * 发送离组心跳请求
     * </p>
     *
     * @return 当撤销回调执行完成且发送离组心跳请求后完成的Future
     */
    public CompletableFuture<Void> leaveGroup() {
        // 调用leaveGroup方法并传入false表示是主动离组
        return leaveGroup(false);
    }

    /**
     * 离组操作的核心实现
     * 
     * @param isOnClose 是否是由于关闭而离组
     * @return 离组操作完成的Future
     */
    private CompletableFuture<Void> leaveGroup(final boolean isOnClose) {
        // 如果成员不在组中
        if (isNotInGroup()) {
            // 如果成员被隔离，清除任务分配并转换到未订阅状态
            if (state == MemberState.FENCED) {
                clearTaskAndPartitionAssignment();
                transitionTo(MemberState.UNSUBSCRIBED);
            }
            // 取消订阅并通知分配变更
            subscriptionState.unsubscribe();
            notifyAssignmentChange(Collections.emptySet());
            return CompletableFuture.completedFuture(null);
        }

        // 如果已经在离组过程中，返回现有的离组Future
        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            log.debug("Leave group operation already in progress for member {}", memberId);
            return leaveGroupInProgress.get();
        }

        // 转换到准备离组状态
        transitionTo(MemberState.PREPARE_LEAVING);
        CompletableFuture<Void> onGroupLeft = new CompletableFuture<>();
        leaveGroupInProgress = Optional.of(onGroupLeft);
        
        // 根据是否是关闭离组选择不同的处理路径
        if (isOnClose) {
            // 关闭时直接进入leaving状态
            leaving();
        } else {
            // 主动离组时先释放活动任务
            CompletableFuture<Void> onAllActiveTasksReleasedCallbackExecuted = releaseActiveTasks();
            onAllActiveTasksReleasedCallbackExecuted
                .whenComplete((__, callbackError) -> leavingAfterReleasingActiveTasks(callbackError));
        }

        return onGroupLeft;
    }

    /**
     * 释放当前分配的活动任务
     * 
     * @return 释放任务操作完成的Future
     */
    private CompletableFuture<Void> releaseActiveTasks() {
        // 如果成员epoch大于0，说明成员正常运行，调用撤销任务
        if (memberEpoch > 0) {
            return revokeActiveTasks(toTaskIdSet(currentAssignment.activeTasks));
        } else {
            // 否则调用释放丢失任务
            return releaseLostActiveTasks();
        }
    }

    /**
     * 在释放活动任务后处理离组操作
     * <p>
     * 当成员完成活动任务的释放后，无论是成功还是失败，都会调用此方法来处理离组流程。
     * 方法会记录相应的日志信息，然后继续执行离组操作。
     *
     * @param callbackError 回调执行过程中的错误，如果回调成功执行则为null
     */
    private void leavingAfterReleasingActiveTasks(Throwable callbackError) {
        // 如果回调执行失败，记录错误日志
        if (callbackError != null) {
            log.error("Member {} callback to revoke task assignment failed. It will proceed " +
                    "to clear its assignment and send a leave group heartbeat",
                memberId, callbackError);
        } else {
            // 如果回调执行成功，记录信息日志
            log.info("Member {} completed callback to revoke task assignment. It will proceed " +
                    "to clear its assignment and send a leave group heartbeat",
                memberId);
        }
        // 执行离组操作
        leaving();
    }

    /**
     * 执行离组操作的核心逻辑
     * <p>
     * 该方法执行以下步骤：
     * 1. 清除当前成员的任务和分区分配
     * 2. 取消订阅所有主题
     * 3. 转换状态为发送离组心跳
     */
    private void leaving() {
        // 清除所有任务和分区分配
        clearTaskAndPartitionAssignment();
        // 取消所有主题的订阅
        subscriptionState.unsubscribe();
        // 转换到发送离组心跳的状态
        transitionToSendingLeaveGroup(false);
    }

    /**
     * 处理从服务器接收到的任务分配
     * <p>
     * 如果接收到的分配与当前分配不同，此方法会确保在下一次调用poll时尝试进行协调。
     * 如果当前已有协调操作在进行中，则新的协调会在当前协调完成后的第一次poll时触发。
     *
     * @param activeTasks 从broker接收到的目标活动任务分配
     * @param standbyTasks 从broker接收到的目标备用任务分配
     * @param warmupTasks 从broker接收到的目标预热任务分配
     */
    private void processAssignmentReceived(Map<String, SortedSet<Integer>> activeTasks,
                                           Map<String, SortedSet<Integer>> standbyTasks,
                                           Map<String, SortedSet<Integer>> warmupTasks) {
        // 使用新的分配更新目标分配
        replaceTargetAssignmentWithNewAssignment(activeTasks, standbyTasks, warmupTasks);
        // 检查是否需要协调
        if (!targetAssignmentReconciled()) {
            // 如果目标分配与当前分配不同，转换到协调状态
            transitionTo(MemberState.RECONCILING);
        } else {
            // 如果分配相同，记录日志
            log.debug("Target assignment {} received from the broker is equals to the member " +
                    "current assignment {}. Nothing to reconcile.",
                targetAssignment, currentAssignment);
            // 如果当前处于协调或加入状态，转换到稳定状态
            if (state == MemberState.RECONCILING || state == MemberState.JOINING) {
                transitionTo(MemberState.STABLE);
            }
        }
    }

    /**
     * 检查目标分配是否已经与当前分配协调一致
     * 
     * @return 如果目标分配等于当前分配则返回true
     */
    private boolean targetAssignmentReconciled() {
        return currentAssignment.equals(targetAssignment);
    }

    /**
     * 使用新的任务分配更新目标分配
     * <p>
     * 如果新的分配与当前目标分配不同，会创建一个新的LocalAssignment实例，
     * 并增加本地epoch值。
     *
     * @param activeTasks 新的活动任务分配
     * @param standbyTasks 新的备用任务分配
     * @param warmupTasks 新的预热任务分配
     */
    private void replaceTargetAssignmentWithNewAssignment(Map<String, SortedSet<Integer>> activeTasks,
                                                          Map<String, SortedSet<Integer>> standbyTasks,
                                                          Map<String, SortedSet<Integer>> warmupTasks) {
        // 尝试更新目标分配
        targetAssignment.updateWith(activeTasks, standbyTasks, warmupTasks)
            .ifPresent(updatedAssignment -> {
                // 如果分配发生变化，记录日志并更新目标分配
                log.debug("Target assignment updated from {} to {}. Member will reconcile it on the next poll.",
                    targetAssignment, updatedAssignment);
                targetAssignment = updatedAssignment;
            });
    }

    /**
     * 由网络线程调用，用于协调当前分配和目标分配
     * <p>
     * 当成员处于RECONCILING状态时，尝试进行协调操作。
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        // 如果处于协调状态，尝试进行协调
        if (state == MemberState.RECONCILING) {
            maybeReconcile();
        }
        return NetworkClientDelegate.PollResult.EMPTY;
    }

    /**
     * 尝试协调从服务器接收到的分配
     * <p>
     * 协调过程会触发回调并更新订阅状态。在以下两种情况下不会触发协调：
     * 1. 已经完成协调（目标分配与当前分配相同）
     * 2. 另一个协调操作正在进行中
     * <p>
     * 协调过程包括：
     * 1. 计算需要撤销和分配的任务
     * 2. 验证分区分配的一致性
     * 3. 执行任务撤销和分配的回调
     * 4. 完成后更新当前分配并转换状态
     */
    private void maybeReconcile() {
        // 检查是否需要协调
        if (targetAssignmentReconciled()) {
            log.trace("Ignoring reconciliation attempt. Target assignment is equal to the " +
                "current assignment.");
            return;
        }
        // 检查是否有协调正在进行
        if (reconciliationInProgress) {
            log.trace("Ignoring reconciliation attempt. Another reconciliation is already in progress. Assignment " +
                targetAssignment + " will be handled in the next reconciliation loop.");
            return;
        }

        // 标记协调开始
        markReconciliationInProgress();

        // 计算各类任务的分配情况
        SortedSet<StreamsRebalanceData.TaskId> assignedActiveTasks = toTaskIdSet(targetAssignment.activeTasks);
        SortedSet<StreamsRebalanceData.TaskId> ownedActiveTasks = toTaskIdSet(currentAssignment.activeTasks);
        SortedSet<StreamsRebalanceData.TaskId> activeTasksToRevoke = new TreeSet<>(ownedActiveTasks);
        activeTasksToRevoke.removeAll(assignedActiveTasks);
        SortedSet<StreamsRebalanceData.TaskId> assignedStandbyTasks = toTaskIdSet(targetAssignment.standbyTasks);
        SortedSet<StreamsRebalanceData.TaskId> ownedStandbyTasks = toTaskIdSet(currentAssignment.standbyTasks);
        SortedSet<StreamsRebalanceData.TaskId> assignedWarmupTasks = toTaskIdSet(targetAssignment.warmupTasks);
        SortedSet<StreamsRebalanceData.TaskId> ownedWarmupTasks = toTaskIdSet(currentAssignment.warmupTasks);

        // 记录详细的任务分配日志
        log.info("Assigned tasks with local epoch {}\n" +
                "\tMember:                        {}\n" +
                "\tAssigned active tasks:         {}\n" +
                "\tOwned active tasks:            {}\n" +
                "\tActive tasks to revoke:        {}\n" +
                "\tAssigned standby tasks:        {}\n" +
                "\tOwned standby tasks:           {}\n" +
                "\tAssigned warm-up tasks:        {}\n" +
                "\tOwned warm-up tasks:           {}\n",
            targetAssignment.localEpoch,
            memberId,
            assignedActiveTasks,
            ownedActiveTasks,
            activeTasksToRevoke,
            assignedStandbyTasks,
            ownedStandbyTasks,
            assignedWarmupTasks,
            ownedWarmupTasks
        );

        // 验证分区分配的一致性
        SortedSet<TopicPartition> ownedTopicPartitionsFromSubscriptionState = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        ownedTopicPartitionsFromSubscriptionState.addAll(subscriptionState.assignedPartitions());
        SortedSet<TopicPartition> ownedTopicPartitionsFromAssignedTasks =
            topicPartitionsForActiveTasks(currentAssignment.activeTasks);
        if (!ownedTopicPartitionsFromAssignedTasks.equals(ownedTopicPartitionsFromSubscriptionState)) {
            throw new IllegalStateException("Owned partitions from subscription state and owned partitions from " +
                "assigned active tasks are not equal. " +
                "Owned partitions from subscription state: " + ownedTopicPartitionsFromSubscriptionState + ", " +
                "Owned partitions from assigned active tasks: " + ownedTopicPartitionsFromAssignedTasks);
        }
        
        // 计算需要撤销的分区
        SortedSet<TopicPartition> assignedTopicPartitions = topicPartitionsForActiveTasks(targetAssignment.activeTasks);
        SortedSet<TopicPartition> partitionsToRevoke = new TreeSet<>(ownedTopicPartitionsFromSubscriptionState);
        partitionsToRevoke.removeAll(assignedTopicPartitions);

        // 执行任务撤销
        final CompletableFuture<Void> tasksRevoked = revokeActiveTasks(activeTasksToRevoke);

        // 在任务撤销完成后执行任务分配
        final CompletableFuture<Void> tasksRevokedAndAssigned = tasksRevoked.thenCompose(__ -> {
            if (!maybeAbortReconciliation()) {
                return assignTasks(assignedActiveTasks, ownedActiveTasks, assignedStandbyTasks, assignedWarmupTasks);
            }
            return CompletableFuture.completedFuture(null);
        });

        // 捕获当前目标分配，确保在协调完成时使用正确的分配版本
        LocalAssignment currentTargetAssignment = targetAssignment;
        tasksRevokedAndAssigned.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                // 如果回调执行失败，记录错误并标记协调完成
                log.error("Reconciliation failed: callback invocation failed for tasks {}",
                    currentTargetAssignment, callbackError);
                markReconciliationCompleted();
            } else {
                // 如果回调执行成功且协调未被中止，更新当前分配并转换状态
                if (reconciliationInProgress && !maybeAbortReconciliation()) {
                    currentAssignment = currentTargetAssignment;
                    transitionTo(MemberState.ACKNOWLEDGING);
                    markReconciliationCompleted();
                }
            }
        });
    }

    /**
     * 撤销活动任务的方法
     * <p>
     * 当需要撤销某些活动任务时(例如在重平衡过程中)，此方法负责:
     * 1. 将待撤销的任务标记为待撤销状态
     * 2. 调用用户定义的onTasksRevoked回调
     * 3. 等待回调执行完成
     *
     * @param activeTasksToRevoke 需要撤销的活动任务集合
     * @return 表示撤销操作完成的Future
     */
    private CompletableFuture<Void> revokeActiveTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        // 如果没有需要撤销的任务，直接返回完成的Future
        if (activeTasksToRevoke.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // 记录待撤销的任务信息
        log.info("Revoking previously assigned active tasks {}", activeTasksToRevoke.stream()
            .map(StreamsRebalanceData.TaskId::toString)
            .collect(Collectors.joining(", ")));

        // 获取待撤销任务对应的分区集合
        final SortedSet<TopicPartition> partitionsToRevoke = topicPartitionsForActiveTasks(activeTasksToRevoke);
        log.debug("Marking partitions pending for revocation: {}", partitionsToRevoke);
        // 将分区标记为待撤销状态
        subscriptionState.markPendingRevocation(partitionsToRevoke);

        // 创建表示任务撤销完成的Future
        CompletableFuture<Void> tasksRevoked = new CompletableFuture<>();
        // 请求执行onTasksRevoked回调
        CompletableFuture<Void> onTasksRevokedCallbackExecuted =
            streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksToRevoke);
        // 处理回调执行结果
        onTasksRevokedCallbackExecuted.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                // 如果回调执行失败，记录错误并完成Future(异常)
                log.error("onTasksRevoked callback invocation failed for tasks {}",
                    activeTasksToRevoke, callbackError);
                tasksRevoked.completeExceptionally(callbackError);
            } else {
                // 回调执行成功，完成Future
                tasksRevoked.complete(null);
            }
        });
        return tasksRevoked;
    }

    /**
     * 分配任务的方法
     * <p>
     * 当需要分配新任务时(例如在重平衡或初始分配时)，此方法负责:
     * 1. 更新订阅状态，标记新分配的分区
     * 2. 通知分配变更
     * 3. 调用用户定义的onTasksAssigned回调
     * 4. 等待回调执行完成
     *
     * @param activeTasksToAssign 要分配的活动任务集合
     * @param ownedActiveTasks 当前已拥有的活动任务集合
     * @param standbyTasksToAssign 要分配的备用任务集合
     * @param warmupTasksToAssign 要分配的预热任务集合
     * @return 表示分配操作完成的Future
     */
    private CompletableFuture<Void> assignTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasksToAssign,
                                                final SortedSet<StreamsRebalanceData.TaskId> ownedActiveTasks,
                                                final SortedSet<StreamsRebalanceData.TaskId> standbyTasksToAssign,
                                                final SortedSet<StreamsRebalanceData.TaskId> warmupTasksToAssign) {
        // 记录待分配的任务信息
        log.info("Assigning active tasks {{}}, standby tasks {{}}, and warm-up tasks {{}} to the member.",
            activeTasksToAssign.stream()
                .map(StreamsRebalanceData.TaskId::toString)
                .collect(Collectors.joining(", ")),
            standbyTasksToAssign.stream()
                .map(StreamsRebalanceData.TaskId::toString)
                .collect(Collectors.joining(", ")),
            warmupTasksToAssign.stream()
                .map(StreamsRebalanceData.TaskId::toString)
                .collect(Collectors.joining(", "))
        );

        // 获取待分配的活动任务对应的分区集合
        final SortedSet<TopicPartition> partitionsToAssign = topicPartitionsForActiveTasks(activeTasksToAssign);
        // 计算新分配的分区(之前未拥有的分区)
        final SortedSet<TopicPartition> partitionsToAssigneNotPreviouslyOwned =
            partitionsToAssignNotPreviouslyOwned(partitionsToAssign, topicPartitionsForActiveTasks(ownedActiveTasks));

        // 更新订阅状态，标记等待回调的分区
        subscriptionState.assignFromSubscribedAwaitingCallback(
            partitionsToAssign,
            partitionsToAssigneNotPreviouslyOwned
        );
        // 通知分配变更
        notifyAssignmentChange(partitionsToAssign);

        // 请求执行onTasksAssigned回调
        CompletableFuture<Void> onTasksAssignedCallbackExecuted =
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                new StreamsRebalanceData.Assignment(
                    activeTasksToAssign,
                    standbyTasksToAssign,
                    warmupTasksToAssign
                )
            );
        // 处理回调执行结果
        onTasksAssignedCallbackExecuted.whenComplete((__, callbackError) -> {
            if (callbackError == null) {
                // 回调执行成功，启用等待回调的分区
                subscriptionState.enablePartitionsAwaitingCallback(partitionsToAssign);
            } else {
                // 回调执行失败，对于新分配的分区，保持其不可获取状态
                if (!partitionsToAssigneNotPreviouslyOwned.isEmpty()) {
                    log.warn("Leaving newly assigned partitions {} marked as non-fetchable and not " +
                            "requiring initializing positions after onTasksAssigned callback failed.",
                        partitionsToAssigneNotPreviouslyOwned, callbackError);
                }
            }
        });

        return onTasksAssignedCallbackExecuted;
    }

    /**
     * 释放丢失的活动任务的方法
     * <p>
     * 当成员失去某些活动任务时(例如被隔离或发生错误)，此方法负责:
     * 1. 将丢失的任务标记为待撤销状态
     * 2. 调用用户定义的onAllTasksLost回调
     * 3. 等待回调执行完成
     *
     * @return 表示释放操作完成的Future
     */
    private CompletableFuture<Void> releaseLostActiveTasks() {
        // 获取当前分配中的活动任务集合
        final SortedSet<StreamsRebalanceData.TaskId> activeTasksToRelease = toTaskIdSet(currentAssignment.activeTasks);
        // 记录待释放的任务信息
        log.info("Revoking previously assigned and now lost active tasks {}", activeTasksToRelease.stream()
            .map(StreamsRebalanceData.TaskId::toString)
            .collect(Collectors.joining(", ")));

        // 获取待释放任务对应的分区集合
        final SortedSet<TopicPartition> partitionsToRelease = topicPartitionsForActiveTasks(activeTasksToRelease);
        log.debug("Marking lost partitions pending for revocation: {}", partitionsToRelease);
        // 将分区标记为待撤销状态
        subscriptionState.markPendingRevocation(partitionsToRelease);

        // 请求执行onAllTasksLost回调并返回
        return streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation();
    }

    /**
     * 获取新分配的分区集合的方法
     * <p>
     * 此方法用于计算新分配中哪些分区是之前未拥有的，这些分区可能需要特殊处理
     * (例如初始化位置或延迟启用)。
     *
     * @param assignedTopicPartitions 新分配的所有分区集合
     * @param ownedTopicPartitions 当前已拥有的分区集合
     * @return 新分配中之前未拥有的分区集合
     */
    private SortedSet<TopicPartition> partitionsToAssignNotPreviouslyOwned(final SortedSet<TopicPartition> assignedTopicPartitions,
                                                                           final SortedSet<TopicPartition> ownedTopicPartitions) {
        // 创建新的TreeSet用于存储结果，使用TopicPartition比较器确保顺序一致性
        SortedSet<TopicPartition> assignedPartitionsNotPreviouslyOwned = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 添加所有新分配的分区
        assignedPartitionsNotPreviouslyOwned.addAll(assignedTopicPartitions);
        // 移除已拥有的分区，剩余的就是新分配的分区
        assignedPartitionsNotPreviouslyOwned.removeAll(ownedTopicPartitions);
        return assignedPartitionsNotPreviouslyOwned;
    }

    /**
     * 根据活动任务映射获取对应的主题分区集合
     * <p>
     * 该方法遍历活动任务映射，对每个子拓扑ID和其对应的分区集合：
     * 1. 获取该子拓扑的源主题和重分区源主题
     * 2. 为每个主题的每个分区创建TopicPartition对象
     * 3. 将所有TopicPartition对象添加到结果集合中
     *
     * @param activeTasks 活动任务映射，key为子拓扑ID，value为分区ID集合
     * @return 按主题名称和分区号排序的TopicPartition集合
     */
    private SortedSet<TopicPartition> topicPartitionsForActiveTasks(final Map<String, SortedSet<Integer>> activeTasks) {
        // 创建一个有序集合用于存储TopicPartition对象，使用TOPIC_PARTITION_COMPARATOR进行排序
        final SortedSet<TopicPartition> topicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 遍历每个子拓扑及其分区集合
        activeTasks.forEach((subtopologyId, partitionIds) ->
            // 合并子拓扑的源主题和重分区源主题流
            Stream.concat(
                streamsRebalanceData.subtopologies().get(subtopologyId).sourceTopics().stream(),
                streamsRebalanceData.subtopologies().get(subtopologyId).repartitionSourceTopics().keySet().stream()
            ).forEach(topic -> {
                // 为每个主题的每个分区创建TopicPartition对象并添加到结果集合
                for (final int partitionId : partitionIds) {
                    topicPartitions.add(new TopicPartition(topic, partitionId));
                }
            })
        );
        return topicPartitions;
    }

    /**
     * 根据活动任务ID集合获取对应的主题分区集合
     * <p>
     * 该方法遍历活动任务ID集合，对每个任务：
     * 1. 获取其子拓扑的源主题和重分区源主题
     * 2. 为每个主题创建一个带有任务分区ID的TopicPartition对象
     * 3. 将所有TopicPartition对象添加到结果集合中
     *
     * @param activeTasks 活动任务ID集合
     * @return 按主题名称和分区号排序的TopicPartition集合
     */
    private SortedSet<TopicPartition> topicPartitionsForActiveTasks(final SortedSet<StreamsRebalanceData.TaskId> activeTasks) {
        // 创建一个有序集合用于存储TopicPartition对象，使用TOPIC_PARTITION_COMPARATOR进行排序
        final SortedSet<TopicPartition> topicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 遍历每个任务ID
        activeTasks.forEach(task ->
            // 合并任务子拓扑的源主题和重分区源主题流
            Stream.concat(
                streamsRebalanceData.subtopologies().get(task.subtopologyId()).sourceTopics().stream(),
                streamsRebalanceData.subtopologies().get(task.subtopologyId()).repartitionSourceTopics().keySet().stream()
            ).forEach(topic -> {
                // 为每个主题创建一个TopicPartition对象并添加到结果集合
                topicPartitions.add(new TopicPartition(topic, task.partitionId()));
            })
        );
        return topicPartitions;
    }

    /**
     * 标记协调过程完成
     * <p>
     * 重置协调状态标志：
     * 1. 将协调进行中标志设置为false
     * 2. 将协调过程中重新加入标志设置为false
     */
    private void markReconciliationCompleted() {
        reconciliationInProgress = false;
        rejoinedWhileReconciliationInProgress = false;
    }

    /**
     * 检查是否需要中止当前的协调过程
     * <p>
     * 在以下情况下会中止协调：
     * 1. 成员状态不再是RECONCILING
     * 2. 成员在协调过程中重新加入了组
     *
     * @return 如果需要中止协调则返回true
     */
    private boolean maybeAbortReconciliation() {
        // 检查是否需要中止：状态不是RECONCILING或在协调过程中重新加入
        boolean shouldAbort = state != MemberState.RECONCILING || rejoinedWhileReconciliationInProgress;
        if (shouldAbort) {
            // 根据中止原因构造日志消息
            String reason = rejoinedWhileReconciliationInProgress ?
                "the member has re-joined the group" :
                "the member already transitioned out of the reconciling state into " + state;
            log.info("Interrupting reconciliation that is not relevant anymore because " + reason);
            // 标记协调完成
            markReconciliationCompleted();
        }
        return shouldAbort;
    }

    /**
     * 标记开始新的协调过程
     * <p>
     * 设置协调状态标志：
     * 1. 将协调进行中标志设置为true
     * 2. 将协调过程中重新加入标志设置为false
     */
    private void markReconciliationInProgress() {
        reconciliationInProgress = true;
        rejoinedWhileReconciliationInProgress = false;
    }

    /**
     * 完成任务撤销回调的执行
     * <p>
     * 处理从应用线程发送到网络线程的回调完成事件：
     * 1. 如果回调执行出错，记录警告日志并使用异常完成Future
     * 2. 如果回调执行成功，记录调试日志并正常完成Future
     *
     * @param event 包含Future的回调完成事件，用于确认回调的执行结果
     */
    public void onTasksRevokedCallbackCompleted(final StreamsOnTasksRevokedCallbackCompletedEvent event) {
        // 获取错误和Future对象
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            // 如果有错误，记录警告并使用异常完成Future
            Exception e = error.get();
            log.warn("The onTasksRevoked callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            // 如果成功，记录调试信息并正常完成Future
            log.debug("The onTasksRevoked callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }

    /**
     * 完成任务分配回调的执行
     * <p>
     * 处理从应用线程发送到网络线程的回调完成事件：
     * 1. 如果回调执行出错，记录警告日志并使用异常完成Future
     * 2. 如果回调执行成功，记录调试日志并正常完成Future
     *
     * @param event 包含Future的回调完成事件，用于确认回调的执行结果
     */
    public void onTasksAssignedCallbackCompleted(final StreamsOnTasksAssignedCallbackCompletedEvent event) {
        // 获取错误和Future对象
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            // 如果有错误，记录警告并使用异常完成Future
            Exception e = error.get();
            log.warn("The onTasksAssigned callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            // 如果成功，记录调试信息并正常完成Future
            log.debug("The onTasksAssigned callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }

    /**
     * 完成所有任务丢失回调的执行
     * <p>
     * 处理从应用线程发送到网络线程的回调完成事件：
     * 1. 如果回调执行出错，记录警告日志并使用异常完成Future
     * 2. 如果回调执行成功，记录调试日志并正常完成Future
     *
     * @param event 包含Future的回调完成事件，用于确认回调的执行结果
     */
    public void onAllTasksLostCallbackCompleted(final StreamsOnAllTasksLostCallbackCompletedEvent event) {
        // 获取错误和Future对象
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            // 如果有错误，记录警告并使用异常完成Future
            Exception e = error.get();
            log.warn("The onAllTasksLost callback completed with an error ({}); " +
                "signaling to continue to the next phase of rebalance", e.getMessage());
            future.completeExceptionally(e);
        } else {
            // 如果成功，记录调试信息并正常完成Future
            log.debug("The onAllTasksLost callback completed successfully; signaling to continue to the next phase of rebalance");
            future.complete(null);
        }
    }
}
