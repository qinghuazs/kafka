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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;

/**
 * Group manager for a single consumer that has a group id defined in the config
 * {@link ConsumerConfig#GROUP_ID_CONFIG}, to use the Kafka-based offset management capability,
 * and the consumer group protocol to get automatically assigned partitions when calling the
 * subscribe API.
 *
 * <p/>
 *
 * While the subscribe API hasn't been called (or if the consumer called unsubscribe), this manager
 * will only be responsible for keeping the member in the {@link MemberState#UNSUBSCRIBED} state,
 * where it can commit offsets to the group identified by the {@link #groupId()}, without joining
 * the group.
 *
 * <p/>
 *
 * If the consumer subscribe API is called, this manager will use the {@link #groupId()} to join the
 * consumer group, and based on the consumer group protocol heartbeats, will handle the full
 * lifecycle of the member as it joins the group, reconciles assignments, handles fencing and
 * fatal errors, and leaves the group.
 *
 * <p/>
 *
 * Reconciliation process:<p/>
 * The member accepts all assignments received from the broker, resolves topic names from
 * metadata, reconciles the resolved assignments, and keeps the unresolved to be reconciled when
 * discovered with a metadata update. Reconciliations of resolved assignments are executed
 * sequentially and acknowledged to the server as they complete. The reconciliation process
 * involves multiple async operations, so the member will continue to heartbeat while these
 * operations complete, to make sure that the member stays in the group while reconciling.
 *
 * <p/>
 *
 * Reconciliation steps:
 * <ol>
 *     <li>Resolve topic names for all topic IDs received in the target assignment. Topic names
 *     found in metadata are then ready to be reconciled. Topic IDs not found are kept as
 *     unresolved, and the member request metadata updates until it resolves them (or the broker
 *     removes it from the target assignment.</li>
 *     <li>Commit offsets if auto-commit is enabled.</li>
 *     <li>Invoke the user-defined onPartitionsRevoked listener.</li>
 *     <li>Invoke the user-defined onPartitionsAssigned listener.</li>
 *     <li>When the above steps complete, the member acknowledges the reconciled assignment,
 *     which is the subset of the target that was resolved from metadata and actually reconciled.
 *     The ack is performed by sending a heartbeat request back to the broker, including the
 *     reconciled assignment.</li>
 * </ol>
 *
 * Note that user-defined callbacks are triggered from this manager that runs in the
 * BackgroundThread, but executed in the Application Thread, where a failure will be returned to
 * the user if the callbacks fail. This manager is only concerned about the callbacks completion to
 * know that it can proceed with the reconciliation.
 */
/**
 * @class ConsumerMembershipManager
 * @brief 消费者成员资格管理器，用于管理单个消费者的组成员资格。
 * 
 * 该类继承自 {@link AbstractMembershipManager}，专门处理具有在配置 {@link ConsumerConfig#GROUP_ID_CONFIG} 中定义的组ID的消费者。
 * 它利用 Kafka 提供的偏移量管理能力，并通过消费者组协议在调用 subscribe API 时自动分配分区。
 * 
 * 应用场景:
 * - 当消费者使用组管理功能时，此类负责处理成员加入组、离开组、分区分配和再均衡等逻辑。
 * - 支持动态成员和静态成员（通过 group.instance.id 配置）。
 * 
 * 实现细节:
 * - 未调用 subscribe API 或调用了 unsubscribe 时，管理器将成员保持在 {@link MemberState#UNSUBSCRIBED} 状态，此时可以提交偏移量到 {@link #groupId()} 标识的组，而无需加入该组。
 * - 调用 subscribe API 后，管理器使用 {@link #groupId()} 加入消费者组，并根据消费者组协议心跳处理成员的完整生命周期，包括加入组、协调分配、处理隔离和致命错误以及离开组。
 * 
 * 再均衡过程:
 * 成员接受从 broker 收到的所有分配，从元数据解析主题名称，协调已解析的分配，并保留未解析的分配，以便在元数据更新时发现并协调。
 * 已解析分配的协调按顺序执行，并在完成后向服务器确认。
 * 由于协调过程涉及多个异步操作，成员将在这些操作完成期间继续发送心跳，以确保在协调期间成员保持在组内。
 * 
 * 再均衡步骤:
 * <ol>
 *     <li>解析目标分配中收到的所有主题ID的主题名称。在元数据中找到的主题名称已准备好进行协调。未找到的主题ID将保持未解析状态，成员请求元数据更新，直到解析它们（或 broker 将其从目标分配中移除）。</li>
 *     <li>如果启用了自动提交，则提交偏移量。</li>
 *     <li>调用用户定义的 onPartitionsRevoked 监听器。</li>
 *     <li>调用用户定义的 onPartitionsAssigned 监听器。</li>
 *     <li>上述步骤完成后，成员确认已协调的分配，这是从元数据解析并实际协调的目标子集。确认操作通过向 broker 发送心跳请求来执行，请求中包含已协调的分配。</li>
 * </ol>
 *
 * 设计考虑:
 * - 用户定义的回调从此管理器（在后台线程中运行）触发，但在应用程序线程中执行。如果回调失败，将向用户返回故障。
 * - 此管理器仅关注回调的完成情况，以确定可以继续进行协调。
 */
public class ConsumerMembershipManager extends AbstractMembershipManager<ConsumerGroupHeartbeatResponse> {

    /**
     * @field groupInstanceId
     * @brief 成员使用的组实例ID，在创建当前成员资格管理器时提供。
     * 如果非空，表示这是一个静态成员，有助于在重启后快速恢复之前的分区分配，减少再均衡的发生。
     */
    protected final Optional<String> groupInstanceId;

    /**
     * @field rebalanceTimeoutMs
     * @brief 再均衡超时时间（毫秒）。
     * 用于在收到新分配时发出的提交请求的时间限制。该提交请求会重试，直到成功、因不可重试错误失败或超时。
     * 这个超时确保了在再均衡过程中，提交操作不会无限期阻塞。
     */
    private final int rebalanceTimeoutMs;

    /**
     * @field serverAssignor
     * @brief 此成员配置使用的服务器端分配器的名称。
     * 它将被发送到 {@link ConsumerGroupHeartbeatRequest} 中的服务器。如果未定义，服务器将选择要使用的分配器实现。
     * 允许用户指定特定的分区分配策略，例如 "range", "roundrobin", "sticky", "cooperative"。
     */
    private final Optional<String> serverAssignor;

    /**
     * @field commitRequestManager
     * @brief 用于执行在撤销分区前所需的提交请求的管理器（如果启用了自动提交）。
     * 确保在分区被撤销之前，相关的偏移量能够被正确提交，防止消息丢失或重复处理。
     */
    private final CommitRequestManager commitRequestManager;

    /**
     * @field backgroundEventHandler
     * @brief 作为向应用程序线程报告事件的通道。
     * 这是必需的，因为我们会向应用程序线程发送 {@link ConsumerRebalanceListenerCallbackNeededEvent 回调} 以及在需要时发送 {@link ErrorEvent 错误}。
     * 这种设计将网络I/O和协议处理与应用程序逻辑分离开，提高了系统的响应性和健壮性。
     */
    private final BackgroundEventHandler backgroundEventHandler;

    /**
     * @constructor ConsumerMembershipManager
     * @brief 构造函数，用于创建一个 ConsumerMembershipManager 实例。
     * @param groupId 消费者组ID。
     * @param groupInstanceId 可选的组实例ID，用于静态成员资格。
     * @param rebalanceTimeoutMs 再均衡超时时间（毫秒）。
     * @param serverAssignor 可选的服务器端分配器名称。
     * @param subscriptions 订阅状态，跟踪消费者订阅的主题和分区。
     * @param commitRequestManager 提交请求管理器，用于处理偏移量提交。
     * @param metadata 消费者元数据，用于获取集群和主题信息。
     * @param logContext 日志上下文，用于记录日志。
     * @param backgroundEventHandler 后台事件处理器，用于与应用程序线程通信。
     * @param time 时间工具，用于获取当前时间。
     * @param metrics 指标收集器，用于收集性能指标。
     */
    public ConsumerMembershipManager(String groupId,
                                     Optional<String> groupInstanceId,
                                     int rebalanceTimeoutMs,
                                     Optional<String> serverAssignor,
                                     SubscriptionState subscriptions,
                                     CommitRequestManager commitRequestManager,
                                     ConsumerMetadata metadata,
                                     LogContext logContext,
                                     BackgroundEventHandler backgroundEventHandler,
                                     Time time,
                                     Metrics metrics) {
        // 调用另一个构造函数，并创建一个新的 ConsumerRebalanceMetricsManager 实例
        this(groupId, // 消费者组ID
            groupInstanceId, // 组实例ID
            rebalanceTimeoutMs, // 再均衡超时时间
            serverAssignor, // 服务器端分配器
            subscriptions, // 订阅状态
            commitRequestManager, // 提交请求管理器
            metadata, // 消费者元数据
            logContext, // 日志上下文
            backgroundEventHandler, // 后台事件处理器
            time, // 时间工具
            new ConsumerRebalanceMetricsManager(metrics)); // 创建并传入再均衡指标管理器
    }

    // 可见性仅用于测试
    /**
     * @constructor ConsumerMembershipManager
     * @brief 构造函数（主要用于测试），用于创建一个 ConsumerMembershipManager 实例。
     * @param groupId 消费者组ID。
     * @param groupInstanceId 可选的组实例ID，用于静态成员资格。
     * @param rebalanceTimeoutMs 再均衡超时时间（毫秒）。
     * @param serverAssignor 可选的服务器端分配器名称。
     * @param subscriptions 订阅状态，跟踪消费者订阅的主题和分区。
     * @param commitRequestManager 提交请求管理器，用于处理偏移量提交。
     * @param metadata 消费者元数据，用于获取集群和主题信息。
     * @param logContext 日志上下文，用于记录日志。
     * @param backgroundEventHandler 后台事件处理器，用于与应用程序线程通信。
     * @param time 时间工具，用于获取当前时间。
     * @param metricsManager 再均衡指标管理器。
     */
    ConsumerMembershipManager(String groupId,
                              Optional<String> groupInstanceId,
                              int rebalanceTimeoutMs,
                              Optional<String> serverAssignor,
                              SubscriptionState subscriptions,
                              CommitRequestManager commitRequestManager,
                              ConsumerMetadata metadata,
                              LogContext logContext,
                              BackgroundEventHandler backgroundEventHandler,
                              Time time,
                              RebalanceMetricsManager metricsManager) {
        // 调用父类的构造函数，初始化基本信息
        super(groupId, // 消费者组ID
            subscriptions, // 订阅状态
            metadata, // 消费者元数据
            logContext.logger(ConsumerMembershipManager.class), // 获取特定于此类的日志记录器
            time, // 时间工具
            metricsManager); // 再均衡指标管理器
        // 初始化 ConsumerMembershipManager 特有的字段
        this.groupInstanceId = groupInstanceId; // 设置组实例ID
        this.rebalanceTimeoutMs = rebalanceTimeoutMs; // 设置再均衡超时时间
        this.serverAssignor = serverAssignor; // 设置服务器端分配器名称
        this.commitRequestManager = commitRequestManager; // 设置提交请求管理器
        this.backgroundEventHandler = backgroundEventHandler; // 设置后台事件处理器
    }

    /**
     * @method groupInstanceId
     * @brief 获取成员加入组时使用的实例ID。
     * @return 如果非空，则表示这是一个静态成员。静态成员资格允许消费者在重启后保留其成员ID和分区分配，从而避免不必要的再均衡。
     */
    public Optional<String> groupInstanceId() {
        // 返回存储的组实例ID
        return groupInstanceId;
    }

    /**
     * {@inheritDoc}
     * @method onHeartbeatSuccess
     * @brief 当心跳请求成功时调用此方法。
     * @param response 消费者组心跳响应。
     * 
     * 实现细节:
     * - 从响应中提取响应数据。
     * - 调用父类的 onHeartbeatSuccess 方法处理通用的心跳成功逻辑，例如更新成员epoch和状态。
     * - 检查响应中是否包含新的分区分配 (assignment)。
     * - 如果存在新的分配，则调用 {@link #onAssignmentReceived(ConsumerGroupHeartbeatResponseData.Assignment)} 方法处理该分配。
     * - 如果响应中指示成员ID已被释放 (memberIdWasReleased)，则将成员状态转换为 {@link MemberState#LEAVING}，表示成员正在离开组。
     * - 如果响应中指示成员epoch已丢失 (memberEpochWasLost)，则将成员状态转换为 {@link MemberState#JOINING}，表示成员需要重新加入组。
     * - 如果响应中指示需要停止心跳 (shouldStopHeartbeating)，则将成员状态转换为 {@link MemberState#STOPPING}，表示成员正在停止心跳。
     * - 如果响应中指示成员处于FENCED状态 (isFenced)，则将成员状态转换为 {@link MemberState#FENCED}，并记录错误信息。
     * - 如果响应中指示成员处于FATAL状态 (isFatal)，则将成员状态转换为 {@link MemberState#FATAL}，并记录错误信息。
     * 
     * 应用场景:
     * - 这是消费者组成员与协调器之间通信的核心部分，用于接收协调器的指令和更新成员状态。
     */
    @Override
    public void onHeartbeatSuccess(ConsumerGroupHeartbeatResponse response) {
        // 从响应对象中获取具体的数据部分
        // 从响应中获取响应数据
        ConsumerGroupHeartbeatResponseData responseData = response.data();
        // 检查响应数据中是否存在错误码
        if (responseData.errorCode() != Errors.NONE.code()) {
            // 如果存在错误码，格式化错误信息
            String errorMessage = String.format(
                    "心跳响应中出现意外错误。预期没有错误，但收到：%s",
                    Errors.forCode(responseData.errorCode())
            );
            // 抛出非法参数异常，指示心跳响应中存在错误
            throw new IllegalArgumentException(errorMessage);
        }
        // 获取当前成员状态
        MemberState state = state();
        // 如果成员状态为正在离开
        if (state == MemberState.LEAVING) {
            // 记录调试日志，忽略从代理接收到的心跳响应，因为成员已在离开组
            log.debug("忽略从代理接收到的心跳响应。成员 {}（epoch {}）已在离开组。", memberId, memberEpoch);
            // 直接返回，不处理此心跳响应
            return;
        }
        // 如果成员状态为未订阅，并且可能正在完成离开过程
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            // 记录调试日志，成员收到了离开组心跳的成功响应并完成了离开操作
            log.debug("成员 {}（epoch {}）收到了离开组心跳的成功响应并完成了离开操作。", memberId, memberEpoch);
            // 直接返回，不处理此心跳响应
            return;
        }
        // 如果成员不在组内（例如，状态为致命错误或未加入）
        if (isNotInGroup()) {
            // 记录调试日志，忽略从代理接收到的心跳响应，因为成员当前状态不属于组成员
            log.debug("忽略从代理接收到的心跳响应。成员 {} 处于 {} 状态，因此不是该组的成员。", memberId, state);
            // 直接返回，不处理此心跳响应
            return;
        }

        // 使用响应数据中的成员 epoch 更新本地成员 epoch
        // 设计考虑：保持成员 epoch 与服务器同步对于正确的组成员资格管理至关重要。
        updateMemberEpoch(responseData.memberEpoch());

        // 从响应数据中获取分配信息
        ConsumerGroupHeartbeatResponseData.Assignment assignment = responseData.assignment();

        // 如果分配信息不为空，表示收到了新的分区分配
        if (assignment != null) {
            // 检查当前成员状态是否可以处理新的分配
            if (!state.canHandleNewAssignment()) {
                // 如果成员处于无法接受新分配的状态（例如，正在准备离开组）
                // 记录调试日志，忽略从服务器接收到的新分配
                log.debug("忽略从服务器接收到的新分配 {}，因为成员处于 {} 状态。",
                        assignment, state);
                // 直接返回，不处理此新分配
                return;
            }

            // 创建一个新的映射来存储新的分配信息，键为主题ID (Uuid)，值为分区号的有序集合
            Map<Uuid, SortedSet<Integer>> newAssignment = new HashMap<>();
            // 遍历分配中的主题分区信息
            assignment.topicPartitions().forEach(topicPartition ->
                // 将主题ID和对应的分区集合（转换为TreeSet以保持有序）放入新分配映射中
                newAssignment.put(topicPartition.topicId(), new TreeSet<>(topicPartition.partitions())));
            // 处理接收到的新分配
            // 应用场景：当消费者收到新的分区分配时，需要更新其本地状态并触发相应的回调。
            processAssignmentReceived(newAssignment);
        }
    }

    /**
     * {@inheritDoc}
     * @brief 标志协调过程开始。
     * @details
     * 应用场景：在成员收到新的分区分配并开始协调过程时调用。
     * 实现细节：发出一个提交请求，该请求将重试直到成功、因不可重试错误失败或超时。
     *          在过期的成员 epoch 错误时重试，尽力提交偏移量，以防在当前协调过程中 epoch 可能已更改。
     *          注意，这里使用再均衡超时作为代理强制完成协调过程的限制。
     * 设计考虑：确保在分区被撤销之前，如果启用了自动提交，偏移量会被同步提交。
     *          重试机制增强了提交的可靠性，特别是在 epoch 可能发生变化的动态环境中。
     * @return 一个 CompletableFuture<Void>，表示提交操作的完成。
     */
    @Override
    protected CompletableFuture<Void> signalReconciliationStarted() {
        // 发出一个提交请求，该请求将重试直到成功、因不可重试错误失败或超时。
        // 在过期的成员 epoch 错误时重试，尽力提交偏移量，以防在当前协调过程中 epoch 可能已更改。
        // 注意，这里使用再均衡超时作为代理强制完成协调过程的限制。
        // commitRequestManager 负责管理提交请求的逻辑，包括重试和超时处理。
        // getDeadlineMsForTimeout 方法根据 rebalanceTimeoutMs 计算提交的截止时间。
        return commitRequestManager.maybeAutoCommitSyncBeforeRevocation(getDeadlineMsForTimeout(rebalanceTimeoutMs));
    }

    /**
     * {@inheritDoc}
     * @brief 标志协调过程即将完成。
     * @details
     * 应用场景：在成员成功协调了新的分区分配，并且相关的回调（如 onPartitionsAssigned）已经执行完毕后调用。
     * 实现细节：重置自动提交计时器，以便从现在开始，成员有了新的分配后，可以按计划进行自动提交。
     * 设计考虑：确保在新的分配生效后，自动提交机制能够正确地重新启动，避免因协调过程导致提交中断或延迟。
     */
    @Override
    protected void signalReconciliationCompleting() {
        // 从现在开始，成员有了新的分配，重新安排自动提交。
        // commitRequestManager 负责管理自动提交的计时和执行。
        commitRequestManager.resetAutoCommitTimer();
    }

    /**
     * {@inheritDoc}
     * @brief 标志成员正在离开组。
     * @details
     * 应用场景：当成员主动调用 unsubscribe() 或遇到不可恢复的错误导致需要离开组时调用。
     * 实现细节：调用 invokeOnPartitionsRevokedOrLostToReleaseAssignment() 方法来释放当前分配的分区，并触发 onPartitionsRevoked 或 onPartitionsLost 回调。
     * 设计考虑：确保在成员离开组之前，其占有的资源（分区）能够被正确释放，并且相关的用户逻辑（通过回调）能够被执行。
     * @return 一个 CompletableFuture<Void>，表示释放分配操作的完成。
     */
    @Override
    protected CompletableFuture<Void> signalMemberLeavingGroup() {
        // 调用内部方法来处理分区撤销或丢失的逻辑，并释放分配
        return invokeOnPartitionsRevokedOrLostToReleaseAssignment();
    }

    /**
     * {@inheritDoc}
     * @brief 标志分区已丢失。
     * @param partitionsLost 已丢失的分区集合。
     * @details
     * 应用场景：当协调器通知成员其之前分配的某些分区已丢失（例如，由于主题删除或分区再分配给其他成员）时调用。
     * 实现细节：调用 invokeOnPartitionsLostCallback(partitionsLost) 方法，触发用户定义的 onPartitionsLost 回调。
     * 设计考虑：允许用户在分区丢失时执行自定义的清理或状态更新逻辑。
     * @return 一个 CompletableFuture<Void>，表示 onPartitionsLost 回调执行的完成。
     */
    @Override
    protected CompletableFuture<Void> signalPartitionsLost(Set<TopicPartition> partitionsLost) {
        // 调用内部方法来处理分区丢失的回调逻辑
        return invokeOnPartitionsLostCallback(partitionsLost);
    }

    /**
     * @method invokeOnPartitionsRevokedOrLostToReleaseAssignment
     * @brief 通过调用用户定义的 onPartitionsRevoked 或 onPartitionsLost 回调来释放成员分配。
     * <ul>
     *     <li>如果成员是组的一部分 (epoch > 0)，则将调用 onPartitionsRevoked。
     *     这适用于成员因有意离开组（在调用 unsubscribe 后）而释放分配的情况。</li>
     *
     *     <li>如果成员不再是组的一部分 (epoch <= 0)，则将调用 onPartitionsLost。
     *     这适用于成员在被隔离 (fenced) 后释放分配的情况。</li>
     * </ul>
     *
     * 应用场景:
     * - 当消费者主动取消订阅或被协调器踢出组时，需要释放其当前持有的分区分配。
     * - 此方法确保在释放分区前，能够正确调用用户注册的监听器，以便应用程序可以执行清理工作。
     *
     * 实现细节:
     * - 首先获取当前成员已分配的所有分区。
     * - 如果没有已分配的分区，则直接返回一个已完成的 Future。
     * - 如果有已分配的分区，则根据成员的 epoch 判断是调用 onPartitionsRevoked (成员仍在组内) 还是 onPartitionsLost (成员已不在组内)。
     *
     * 设计考虑:
     * - 使用 CompletableFuture 异步执行回调，避免阻塞主协调线程。
     * - 区分 onPartitionsRevoked 和 onPartitionsLost 是为了让用户能够根据不同的场景执行不同的逻辑。
     *   例如，onPartitionsRevoked 通常表示计划内的分区释放，而 onPartitionsLost 可能表示意外的分区丢失。
     *
     * @return 当回调执行完成时将完成的 Future。
     */
    private CompletableFuture<Void> invokeOnPartitionsRevokedOrLostToReleaseAssignment() {
        // 创建一个有序集合来存储需要丢弃的分区，使用 TOPIC_PARTITION_COMPARATOR 进行排序
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 获取当前订阅中已分配的分区，并添加到 droppedPartitions 集合中
        droppedPartitions.addAll(subscriptions.assignedPartitions());

        // 记录日志，表明成员正在触发回调以释放分配并离开组
        log.info("成员 {} 正在触发回调以释放分配 {} 并离开组",
                memberId, droppedPartitions);

        // 用于存储回调结果的 CompletableFuture
        CompletableFuture<Void> callbackResult;
        // 检查是否有需要释放的分区
        if (droppedPartitions.isEmpty()) {
            // 没有需要释放的分配。
            // 如果没有分区需要释放，则直接返回一个已完成的 CompletableFuture
            callbackResult = CompletableFuture.completedFuture(null);
        } else {
            // 释放分配。
            // 如果成员的 epoch 大于 0，表示成员仍然是组的一部分
            if (memberEpoch > 0) {
                // 成员是组的一部分。调用 onPartitionsRevoked。
                // 调用 revokePartitions 方法，该方法内部会触发 onPartitionsRevoked 回调
                callbackResult = revokePartitions(droppedPartitions);
            } else {
                // 成员不再是组的一部分。调用 onPartitionsLost。
                // 调用 invokeOnPartitionsLostCallback 方法，触发 onPartitionsLost 回调
                callbackResult = invokeOnPartitionsLostCallback(droppedPartitions);
            }
        }
        // 返回回调执行的结果
        return callbackResult;
    }

    /**
     * @method serverAssignor
     * @brief 获取为成员配置的服务器端分配器实现。
     * 此分配器名称将被发送到服务器以供使用。如果为空，则服务器将选择默认的分配器。
     *
     * 应用场景:
     * - 当消费者希望使用特定的分区分配策略（如 range, roundrobin, sticky, cooperative）时，可以通过此方法获取配置的分配器名称。
     * - 该信息会包含在发送给 GroupCoordinator 的 ConsumerGroupHeartbeatRequest 中。
     *
     * 实现细节:
     * - 直接返回成员变量 `this.serverAssignor`，它是一个 `Optional<String>` 类型。
     *
     * 设计考虑:
     * - 使用 `Optional` 表示分配器名称可能未被配置，这种情况下由服务器决定使用何种分配策略。
     * - 允许客户端指定分配器，提供了灵活性，但也需要客户端和服务端都支持该分配器。
     *
     * @return 为成员配置的服务器端分配器实现，如果为空，则服务器将选择分配器。
     */
    public Optional<String> serverAssignor() {
        // 返回存储在成员变量中的服务器端分配器名称
        return this.serverAssignor;
    }

    /**
     * @method invokeOnPartitionsRevokedCallback
     * @brief 调用 onPartitionsRevoked 回调。
     * 
     * 应用场景:
     * - 在分区被撤销之前调用，允许用户执行必要的清理操作，例如提交偏移量、关闭资源等。
     *
     * 实现细节:
     * - 检查 `partitionsRevoked` 集合是否为空以及是否存在 `ConsumerRebalanceListener`。
     * - 如果两者都满足，则将 `ON_PARTITIONS_REVOKED` 事件和被撤销的分区集合加入到后台事件队列中，等待应用程序线程处理。
     * - 否则，返回一个已完成的 `CompletableFuture`。
     *
     * 设计考虑:
     * - 遵循现有行为，如果 `partitionsRevoked` 为空，则不应触发回调。
     * - 通过 `enqueueConsumerRebalanceListenerCallback` 将回调的执行委托给应用程序线程，避免阻塞网络线程。
     *
     * @param partitionsRevoked 被撤销的分区集合。
     * @return 当回调执行完成时将完成的 Future。
     */
    private CompletableFuture<Void> invokeOnPartitionsRevokedCallback(Set<TopicPartition> partitionsRevoked) {
        // 遵循现有行为，如果 partitionsRevoked 为空，则不应触发回调。
        // 获取订阅中注册的 ConsumerRebalanceListener
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        // 如果被撤销的分区集合不为空，并且监听器存在
        if (!partitionsRevoked.isEmpty() && listener.isPresent()) {
            // 将 ON_PARTITIONS_REVOKED 事件和被撤销的分区入队，等待应用程序线程处理回调
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_REVOKED, partitionsRevoked);
        } else {
            // 否则，返回一个已完成的 CompletableFuture
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * @method invokeOnPartitionsAssignedCallback
     * @brief 调用 onPartitionsAssigned 回调。
     *
     * 应用场景:
     * - 在新的分区分配给消费者后调用，允许用户执行初始化操作，例如开始从新的分区消费数据、重置状态等。
     *
     * 实现细节:
     * - 检查是否存在 `ConsumerRebalanceListener`。
     * - 如果存在，则将 `ON_PARTITIONS_ASSIGNED` 事件和新分配的分区集合加入到后台事件队列中，等待应用程序线程处理。
     * - 即使 `partitionsAssigned` 为空，也应触发回调，以保持当前行为。
     * - 否则，返回一个已完成的 `CompletableFuture`。
     *
     * 设计考虑:
     * - 遵循现有行为，即使 `partitionsAssigned` 为空，也应始终触发回调。
     * - 通过 `enqueueConsumerRebalanceListenerCallback` 将回调的执行委托给应用程序线程。
     *
     * @param partitionsAssigned 新分配的分区集合。
     * @return 当回调执行完成时将完成的 Future。
     */
    private CompletableFuture<Void> invokeOnPartitionsAssignedCallback(Set<TopicPartition> partitionsAssigned) {
        // 遵循现有行为，即使 partitionsAssigned 为空，也应始终触发回调。
        // 获取订阅中注册的 ConsumerRebalanceListener
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        // 如果监听器存在
        if (listener.isPresent()) {
            // 将 ON_PARTITIONS_ASSIGNED 事件和新分配的分区入队，等待应用程序线程处理回调
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_ASSIGNED, partitionsAssigned);
        } else {
            // 否则，返回一个已完成的 CompletableFuture
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * @method invokeOnPartitionsLostCallback
     * @brief 调用 onPartitionsLost 回调。
     *
     * 应用场景:
     * - 当消费者持有的分区被意外丢失时调用（例如，由于消费者被踢出组，或者协调器故障导致分区重新分配）。
     * - 允许用户执行必要的清理或恢复操作。
     *
     * 实现细节:
     * - 检查 `partitionsLost` 集合是否为空以及是否存在 `ConsumerRebalanceListener`。
     * - 如果两者都满足，则将 `ON_PARTITIONS_LOST` 事件和丢失的分区集合加入到后台事件队列中，等待应用程序线程处理。
     * - 否则，返回一个已完成的 `CompletableFuture`。
     *
     * 设计考虑:
     * - 遵循现有行为，如果 `partitionsLost` 为空，则不应触发回调。
     * - `onPartitionsLost` 与 `onPartitionsRevoked` 的区别在于前者通常表示非预期的分区丢失。
     * - 通过 `enqueueConsumerRebalanceListenerCallback` 将回调的执行委托给应用程序线程。
     *
     * @param partitionsLost 丢失的分区集合。
     * @return 当回调执行完成时将完成的 Future。
     */
    private CompletableFuture<Void> invokeOnPartitionsLostCallback(Set<TopicPartition> partitionsLost) {
        // 遵循现有行为，如果 partitionsLost 为空，则不应触发回调。
        // 获取订阅中注册的 ConsumerRebalanceListener
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        // 如果丢失的分区集合不为空，并且监听器存在
        if (!partitionsLost.isEmpty() && listener.isPresent()) {
            // 将 ON_PARTITIONS_LOST 事件和丢失的分区入队，等待应用程序线程处理回调
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_LOST, partitionsLost);
        } else {
            // 否则，返回一个已完成的 CompletableFuture
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * {@inheritDoc}
     * @method signalPartitionsAssigned
     * @brief 标志分区已分配，并触发相应的回调。
     * 这是 {@link AssignmentReconciler.ReconciliationCallbacks#signalPartitionsAssigned(Set)} 接口的实现。
     *
     * 应用场景:
     * - 在协调过程(reconciliation process)中，当新的分区分配确定后，此方法被调用以通知消费者。
     *
     * 实现细节:
     * - 直接调用 {@link #invokeOnPartitionsAssignedCallback(Set)} 方法来处理分区分配的回调逻辑。
     *
     * @param partitionsAssigned 新分配给此成员的分区集合。
     * @return 一个 CompletableFuture，在 onPartitionsAssigned 回调完成后完成。
     */
    @Override
    public CompletableFuture<Void> signalPartitionsAssigned(Set<TopicPartition> partitionsAssigned) {
        // 调用内部方法来触发 onPartitionsAssigned 回调
        return invokeOnPartitionsAssignedCallback(partitionsAssigned);
    }

    /**
     * {@inheritDoc}
     * @method signalPartitionsBeingRevoked
     * @brief 标志分区即将被撤销。
     * 这是 {@link AssignmentReconciler.ReconciliationCallbacks#signalPartitionsBeingRevoked(Set)} 接口的实现。
     *
     * 应用场景:
     * - 在协调过程中，当确定某些分区将要从当前成员撤销时，此方法被调用。
     * - 主要用于记录日志，特别是记录那些即将被撤销但当前处于暂停状态的分区。
     *
     * 实现细节:
     * - 调用 {@code logPausedPartitionsBeingRevoked} 方法记录相关日志信息。
     * - 此方法本身不直接触发用户回调，实际的撤销回调 (onPartitionsRevoked) 会在后续步骤中通过 {@link #signalPartitionsRevoked(Set)} 触发。
     *
     * @param partitionsToRevoke 即将被撤销的分区集合。
     */
    @Override
    public void signalPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) {
        // 记录即将被撤销的暂停分区日志
        logPausedPartitionsBeingRevoked(partitionsToRevoke);
    }

    /**
     * {@inheritDoc}
     * @method signalPartitionsRevoked
     * @brief 标志分区已被撤销，并触发相应的回调。
     * 这是 {@link AssignmentReconciler.ReconciliationCallbacks#signalPartitionsRevoked(Set)} 接口的实现。
     *
     * 应用场景:
     * - 在协调过程中，当分区实际被撤销后，此方法被调用以通知消费者。
     *
     * 实现细节:
     * - 直接调用 {@link #invokeOnPartitionsRevokedCallback(Set)} 方法来处理分区撤销的回调逻辑。
     *
     * @param partitionsRevoked 已被撤销的分区集合。
     * @return 一个 CompletableFuture，在 onPartitionsRevoked 回调完成后完成。
     */
    @Override
    public CompletableFuture<Void> signalPartitionsRevoked(Set<TopicPartition> partitionsRevoked) {
        // 调用内部方法来触发 onPartitionsRevoked 回调
        return invokeOnPartitionsRevokedCallback(partitionsRevoked);
    }

    /**
     * 记录那些已经被暂停但即将被撤销的分区，因为暂停标志实际上会丢失。
     * 应用场景：当分区被撤销时，如果这些分区之前被用户手动暂停了，需要记录这个信息，因为撤销后暂停状态会失效。
     * 实现细节：获取当前所有暂停的分区，然后与待撤销的分区取交集，如果交集不为空，则记录日志。
     * 设计考虑：确保用户了解暂停状态的变更，避免因分区撤销导致暂停状态意外丢失而产生困惑。
     */
    private void logPausedPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) {
        // 获取当前所有已暂停的分区集合
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        // 保留 revokePausedPartitions 中也存在于 partitionsToRevoke 中的元素，即找出那些既被暂停又要被撤销的分区
        revokePausedPartitions.retainAll(partitionsToRevoke);
        // 如果存在这样的分区
        if (!revokePausedPartitions.isEmpty()) {
            // 记录info级别日志，通知用户这些分区的暂停标志将因撤销而被移除
            log.info("分区 [{}] 中的暂停标志将因撤销而被移除。", revokePausedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
        }
    }

    /**
     * 将一个 {@link ConsumerRebalanceListenerCallbackNeededEvent} 事件入队，以触发在应用程序线程上执行
     * 合适的 {@link ConsumerRebalanceListener} 的 {@link ConsumerRebalanceListenerMethodName 方法}。
     *
     * <p/>
     *
     * 因为协调过程（在后台线程中运行）将被应用程序线程阻塞，直到它完成此操作，
     * 我们需要提供一个 {@link CompletableFuture} 来记住我们中断的地方。
     * 应用场景：在再均衡过程中，需要在应用程序线程执行用户定义的 `onPartitionsAssigned` 或 `onPartitionsRevoked` 回调。
     * 实现细节：创建一个事件，将其添加到后台事件处理器的队列中，并返回一个 Future，以便再均衡逻辑可以等待回调完成。
     * 设计考虑：通过事件队列和 Future 实现后台线程与应用程序线程之间的异步协调，避免直接阻塞后台线程，同时确保回调按预期执行。
     *
     * @param methodName 需要在应用程序线程上执行的回调方法名称
     * @param partitions 提供给回调方法的分区集合
     * @return 将在其余协调逻辑中链接的 Future
     */
    private CompletableFuture<Void> enqueueConsumerRebalanceListenerCallback(ConsumerRebalanceListenerMethodName methodName,
                                                                             Set<TopicPartition> partitions) {
        // 创建一个有序的分区集合，使用 TOPIC_PARTITION_COMPARATOR 进行排序
        SortedSet<TopicPartition> sortedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 将传入的分区添加到有序集合中
        sortedPartitions.addAll(partitions);

        // 创建一个 ConsumerRebalanceListenerCallbackNeededEvent 事件实例
        CompletableBackgroundEvent<Void> event = new ConsumerRebalanceListenerCallbackNeededEvent(methodName, sortedPartitions);
        // 将事件添加到后台事件处理器的队列中
        backgroundEventHandler.add(event);
        // 记录debug级别日志，表示触发方法执行的事件已成功入队
        log.debug("触发 {} 方法执行的事件已成功入队", methodName.fullyQualifiedMethodName());
        // 返回事件关联的 Future，用于后续的协调逻辑
        return event.future();
    }

    /**
     * 表示一个 {@link ConsumerRebalanceListener} 回调已完成。
     * 当应用程序线程完成回调并将 {@link ConsumerRebalanceListenerCallbackCompletedEvent} 提交到网络I/O线程时，
     * 将调用此方法。此时，我们通知状态机它已完成，以便它可以进入再均衡过程的下一个适当步骤。
     * 应用场景：当应用程序线程执行完用户回调后，通过此方法通知后台线程回调已完成，可以继续再均衡流程。
     * 实现细节：从事件中获取方法名、错误信息和 Future。如果存在错误，则以异常方式完成 Future；否则，正常完成 Future。
     * 设计考虑：这是后台线程与应用程序线程之间回调完成信号的同步点。通过 Future 的完成状态来传递回调的成功或失败。
     *
     * @param event 包含已执行回调详细信息的事件
     */
    public void consumerRebalanceListenerCallbackCompleted(ConsumerRebalanceListenerCallbackCompletedEvent event) {
        // 从事件中获取回调方法的名称
        ConsumerRebalanceListenerMethodName methodName = event.methodName();
        // 从事件中获取可选的 KafkaException 错误信息
        Optional<KafkaException> error = event.error();
        // 从事件中获取与回调关联的 CompletableFuture
        CompletableFuture<Void> future = event.future();

        // 检查回调是否出错
        if (error.isPresent()) {
            // 获取异常实例
            Exception e = error.get();
            // 记录warn级别日志，表明方法执行出错，但仍将继续到再均衡的下一阶段
            log.warn(
                    "{} 方法执行出错 ({})；正在通知继续到再均衡的下一阶段",
                    methodName.fullyQualifiedMethodName(),
                    e.getMessage()
            );

            // 以异常方式完成 Future，并将异常传递给等待方
            future.completeExceptionally(e);
        } else {
            // 记录debug级别日志，表明方法成功完成，将继续到再均衡的下一阶段
            log.debug(
                    "{} 方法成功完成；正在通知继续到再均衡的下一阶段",
                    methodName.fullyQualifiedMethodName()
            );

            // 正常完成 Future，通知等待方回调已成功执行
            future.complete(null);
        }
    }

    /**
     * {@inheritDoc}
     * 获取加入组的成员 epoch。
     * 应用场景：在发送加入组请求 (ConsumerGroupHeartbeatRequest) 时，需要指定成员的 epoch。
     * 实现细节：返回预定义的 JOIN_GROUP_MEMBER_EPOCH 常量。
     * 设计考虑：这是消费者组协议的一部分，用于标识成员加入组的意图。
     */
    @Override
    public int joinGroupEpoch() {
        // 返回表示加入组请求的成员 epoch 值
        return ConsumerGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH;
    }

    /**
     * {@inheritDoc}
     * 获取离开组的成员 epoch。
     * 应用场景：在发送离开组请求 (ConsumerGroupHeartbeatRequest) 时，需要指定成员的 epoch。
     * 实现细节：如果存在组实例ID (groupInstanceId)，则表示是静态成员，返回 LEAVE_GROUP_STATIC_MEMBER_EPOCH；
     *          否则，表示是动态成员，返回 LEAVE_GROUP_MEMBER_EPOCH。
     * 设计考虑：区分静态成员和动态成员的离开组行为。静态成员离开组时使用特定的 epoch，
     *          这有助于 broker 更好地管理静态成员的状态。
     */
    @Override
    public int leaveGroupEpoch() {
        // 检查是否存在 groupInstanceId
        return groupInstanceId.isPresent() ?
                // 如果存在，说明是静态成员，返回静态成员离开组的 epoch
                ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH :
                // 如果不存在，说明是动态成员，返回普通成员离开组的 epoch
                ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
    }
}
