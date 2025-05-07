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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.MemberIdRequiredException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCoordinator类实现了单个组成员的组管理功能，通过与指定的Kafka broker(协调器)进行交互
 * 组语义通过扩展此类来提供。参见{@link ConsumerCoordinator}作为使用示例
 *
 * 从高层次来看，Kafka的组管理协议包含以下操作序列：
 *
 * <ol>
 *     <li>组注册：组成员向协调器注册，提供自己的元数据（如感兴趣的主题集合）</li>
 *     <li>组/领导者选举：协调器选择组的成员并选择一个成员作为领导者</li>
 *     <li>状态分配：领导者收集组内所有成员的元数据并分配状态</li>
 *     <li>组稳定：每个成员接收领导者分配的状态并开始处理</li>
 * </ol>
 *
 * 要使用此协议，实现必须定义：
 * 1. 在{@link #metadata()}中提供每个成员用于组注册的元数据格式
 * 2. 在{@link #onLeaderElected(String, String, List, boolean)}中提供领导者的状态分配格式
 * 3. 在{@link #onJoinComplete(int, String, String, ByteBuffer)}中使状态对成员可用
 *
 * 关于锁定的说明：此类在调用者和后台线程之间共享状态，后台线程用于在客户端加入组后发送心跳
 * 所有可变状态和状态转换都受类监视器保护。这通常意味着在读取或写入组的状态（如generation, memberId）之前
 * 获取锁，并在发送影响组状态的请求（如JoinGroup, LeaveGroup）时持有锁
 */
public abstract class AbstractCoordinator implements Closeable {
    /**
     * 心跳线程名称前缀
     */
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    
    /**
     * 加入组超时时间间隔（毫秒）
     */
    public static final int JOIN_GROUP_TIMEOUT_LAPSE = 5000;

    /**
     * 成员状态枚举
     * 描述消费者在消费者组中的当前状态
     */
    protected enum MemberState {
        UNJOINED,             // 客户端不是组的一部分
        PREPARING_REBALANCE,  // 客户端已发送加入组请求，但尚未收到响应
        COMPLETING_REBALANCE, // 客户端已收到加入组响应，但尚未收到分配
        STABLE;               // 客户端已加入并正在发送心跳

        /**
         * 检查成员是否尚未加入组
         * @return 如果状态为UNJOINED或PREPARING_REBALANCE则返回true
         */
        public boolean hasNotJoinedGroup() {
            return equals(UNJOINED) || equals(PREPARING_REBALANCE);
        }
    }

    // 日志记录器
    private final Logger log;
    // 心跳管理器
    private final Heartbeat heartbeat;
    // 组协调器指标
    private final GroupCoordinatorMetrics sensors;
    // 重平衡配置
    private final GroupRebalanceConfig rebalanceConfig;
    // 客户端遥测报告器（可选）
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    // 时间管理器
    protected final Time time;
    // 消费者网络客户端
    protected final ConsumerNetworkClient client;
    // 指数退避重试机制
    protected final ExponentialBackoff retryBackoff;

    // 协调器节点
    private Node coordinator = null;
    // 重新加入原因
    private String rejoinReason = "";
    // 是否需要重新加入
    private boolean rejoinNeeded = true;
    // 是否需要准备加入
    private boolean needsJoinPrepare = true;
    // 心跳线程
    private HeartbeatThread heartbeatThread = null;
    // 加入组的Future
    private RequestFuture<ByteBuffer> joinFuture = null;
    // 查找协调器的Future
    private RequestFuture<Void> findCoordinatorFuture = null;
    // 查找协调器时的致命异常
    private volatile RuntimeException fatalFindCoordinatorException = null;
    // 当前代（世代）信息
    private Generation generation = Generation.NO_GENERATION;
    // 最后一次重平衡开始时间
    private long lastRebalanceStartMs = -1L;
    // 最后一次重平衡结束时间
    private long lastRebalanceEndMs = -1L;
    // 最后一次连接时间（仅在长时间无法连接时开始记录警告）
    private long lastTimeOfConnectionMs = -1L;

    // 当前成员状态
    protected MemberState state = MemberState.UNJOINED;


    /**
     * 初始化协调管理器
     * 这是一个便捷的构造函数，不包含遥测报告器
     */
    public AbstractCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time) {
        // 调用完整的构造函数，传入空的遥测报告器
        this(rebalanceConfig, logContext, client, metrics, metricGrpPrefix, time, Optional.empty());
    }

    /**
     * 初始化协调管理器的完整构造函数
     *
     * @param rebalanceConfig 重平衡配置，包含组ID等重要参数
     * @param logContext 日志上下文，用于创建日志记录器
     * @param client 消费者网络客户端，用于网络通信
     * @param metrics 指标收集器，用于性能监控
     * @param metricGrpPrefix 指标组前缀，用于指标分类
     * @param time 时间管理器，用于时间相关操作
     * @param clientTelemetryReporter 可选的客户端遥测报告器
     */
    public AbstractCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        // 确保组ID不为空
        Objects.requireNonNull(rebalanceConfig.groupId,
                               "Expected a non-null group id for coordinator construction");
        // 初始化重平衡配置
        this.rebalanceConfig = rebalanceConfig;
        // 创建日志记录器
        this.log = logContext.logger(this.getClass());
        // 设置网络客户端
        this.client = client;
        // 设置时间管理器
        this.time = time;
        // 创建指数退避重试机制
        this.retryBackoff = new ExponentialBackoff(
                rebalanceConfig.retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                rebalanceConfig.retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
        // 创建心跳管理器
        this.heartbeat = new Heartbeat(rebalanceConfig, time);
        // 创建组协调器指标
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        // 设置遥测报告器
        this.clientTelemetryReporter = clientTelemetryReporter;
    }

    /**
     * 获取支持的协议类型的唯一标识符
     * 例如"consumer"或"connect"
     *
     * @return 非空的协议类型名称
     */
    protected abstract String protocolType();

    /**
     * 获取本地成员支持的协议列表及其关联的元数据
     * 列表中协议的顺序表示偏好（第一个条目最优先）
     * 协调器在选择生成协议时会考虑这个偏好
     * 通常会选择更优先的协议，只要所有成员都支持且在偏好上没有分歧
     *
     * @return 非空的支持协议和元数据映射
     */
    protected abstract JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata();

    /**
     * 在每次加入或重新加入组之前调用
     * 通常用于执行前一代的清理工作（例如提交消费者的偏移量）
     *
     * @param timer 限制方法阻塞时间的计时器
     * @param generation 前一代的代数，如果没有则为-1
     * @param memberId 成员在前一个组中的标识符，如果没有则为空字符串
     * @return 如果异步提交成功则返回true，否则返回false
     */
    protected abstract boolean onJoinPrepare(Timer timer, int generation, String memberId);

    /**
     * 当选出领导者时调用
     * 领导者使用此方法执行必要的分配并将状态推送给组的所有成员
     * 例如在新消费者的情况下推送分区分配
     *
     * @param leaderId 领导者的ID（即当前成员）
     * @param protocol 协调器选择的协议
     * @param allMemberMetadata 组中所有成员的元数据
     * @param skipAssignment 如果为true，领导者必须跳过运行分配器
     * @return 从每个成员到其状态分配的映射
     */
    protected abstract Map<String, ByteBuffer> onLeaderElected(String leaderId,
                                                               String protocol,
                                                               List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata,
                                                               boolean skipAssignment);

    /**
     * 当组成员成功加入组时调用
     * 如果此调用失败并抛出异常，则在下次调用ensureActiveGroup()时
     * 将使用相同的分配状态重试
     *
     * @param generation 加入的代数
     * @param memberId 本地成员在组中的标识符
     * @param protocol 协调器选择的协议
     * @param memberAssignment 从组领导者传播的分配
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * 在每次离开组事件之前调用
     * 通常用于清理已分配的分区
     * 注意：这是由消费者的API调用者线程触发的
     * （即使后台心跳线程在心跳会话过期时尝试强制离开组，也不会触发此方法）
     */
    protected void onLeavePrepare() {}

    /**
     * Ensure that the coordinator is ready to receive requests.
     *
     * @param timer Timer bounding how long this method can block
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    /**
     * 确保协调器已准备好接收请求
     * 这是一个同步方法，会阻塞直到协调器准备就绪或超时
     *
     * @param timer 限制方法阻塞时间的计时器
     * @return 如果协调器发现和初始连接成功则返回true，否则返回false
     */
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        return ensureCoordinatorReady(timer, false);
    }

    /**
     * Ensure that the coordinator is ready to receive requests. This will return
     * immediately without blocking. It is intended to be called in an asynchronous
     * context when wakeups are not expected.
     *
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    /**
     * 确保协调器已准备好接收请求的异步版本
     * 此方法会立即返回而不阻塞，适用于不期望唤醒的异步上下文
     *
     * @return 如果协调器发现和初始连接成功则返回true，否则返回false
     */
    protected synchronized boolean ensureCoordinatorReadyAsync() {
        return ensureCoordinatorReady(time.timer(0), true);
    }

    /**
     * 确保协调器已准备好接收请求的内部实现方法
     *
     * @param timer 限制方法阻塞时间的计时器
     * @param disableWakeup 是否禁用唤醒机制
     * @return 如果协调器已就绪则返回true，否则返回false
     */
    private synchronized boolean ensureCoordinatorReady(final Timer timer, boolean disableWakeup) {
        // 如果协调器已知，直接返回true
        if (!coordinatorUnknown())
            return true;

        // 记录尝试次数，用于退避计算
        long attempts = 0L;
        do {
            // 检查是否存在致命异常
            if (fatalFindCoordinatorException != null) {
                final RuntimeException fatalException = fatalFindCoordinatorException;
                fatalFindCoordinatorException = null;
                throw fatalException;
            }
            // 查找协调器
            final RequestFuture<Void> future = lookupCoordinator();
            // 轮询等待查找结果
            client.poll(future, timer, disableWakeup);

            // 如果请求未完成，说明超时，跳出循环
            if (!future.isDone()) {
                break;
            }

            RuntimeException fatalException = null;

            // 处理查找失败的情况
            if (future.failed()) {
                if (future.isRetriable()) {
                    // 可重试错误，记录日志并进行退避重试
                    log.debug("Coordinator discovery failed, refreshing metadata", future.exception());
                    timer.sleep(retryBackoff.backoff(attempts++));
                    client.awaitMetadataUpdate(timer);
                } else {
                    // 不可重试的致命错误
                    fatalException = future.exception();
                    log.info("FindCoordinator request hit fatal exception", fatalException);
                }
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // 协调器已找到但连接失败，标记为未知并进行退避重试
                markCoordinatorUnknown("coordinator unavailable");
                timer.sleep(retryBackoff.backoff(attempts++));
            }

            // 清理查找Future
            clearFindCoordinatorFuture();
            // 如果存在致命异常，抛出
            if (fatalException != null)
                throw fatalException;
        } while (coordinatorUnknown() && timer.notExpired());

        // 返回协调器是否已知
        return !coordinatorUnknown();
    }

    /**
     * 查找协调器的方法
     * 使用异步Future模式实现
     *
     * @return 包含查找结果的Future对象
     */
    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // 查找负载最小的节点
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                // 没有可用的broker节点
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else {
                // 向选中的节点发送查找协调器请求
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
            }
        }
        return findCoordinatorFuture;
    }

    /**
     * 清理查找协调器的Future对象
     * 用于重置查找状态
     */
    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes) or whether a
     * rejoin request is already in flight and needs to be completed.
     *
     * @return true if it should, false otherwise
     */
    /**
     * 检查是否需要重新加入组或是否有未完成的重新加入请求
     * 例如在元数据变更时需要重新加入
     *
     * @return 如果需要重新加入或有待处理的加入请求则返回true，否则返回false
     */
    protected synchronized boolean rejoinNeededOrPending() {
        // 如果需要重新加入或存在未完成的加入请求，返回true
        return rejoinNeeded || joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    /**
     * 检查心跳线程的状态（如果活跃）并指示客户端的活跃性
     * 在通过{@link #ensureActiveGroup()}加入组后必须定期调用此方法
     * 以确保成员留在组中。如果在超过重平衡超时时间的间隔内没有调用此方法
     * 则客户端将主动离开组
     *
     * @param now 当前时间（毫秒）
     * @throws RuntimeException 当心跳线程抛出意外错误时
     */
    protected synchronized void pollHeartbeat(long now) {
        // 检查心跳线程是否存在
        if (heartbeatThread != null) {
            // 检查心跳线程是否失败
            if (heartbeatThread.hasFailed()) {
                // 设置心跳线程为null并抛出异常
                // 如果用户捕获异常，下次调用ensureActiveGroup()将创建新的心跳线程
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // 如果需要发送心跳，唤醒心跳线程
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            // 执行心跳轮询
            heartbeat.poll(now);
        }
    }

    /**
     * 计算到下一次心跳的时间间隔
     *
     * @param now 当前时间（毫秒）
     * @return 到下一次心跳的时间间隔（毫秒）
     */
    protected synchronized long timeToNextHeartbeat(long now) {
        // 如果尚未加入组或正在准备重平衡，不需要发送心跳
        if (state.hasNotJoinedGroup())
            return Long.MAX_VALUE;
        // 检查心跳线程是否失败
        if (heartbeatThread != null && heartbeatThread.hasFailed()) {
            // 如果心跳线程发生异常，抛出异常
            throw heartbeatThread.failureCause();
        }
        // 计算到下一次心跳的时间
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    /**
     * 确保组处于活跃状态（即已加入并同步）
     */
    public void ensureActiveGroup() {
        // 循环直到确保组处于活跃状态
        while (!ensureActiveGroup(time.timer(Long.MAX_VALUE))) {
            log.warn("still waiting to ensure active group");
        }
    }

    /**
     * Ensure the group is active (i.e., joined and synced)
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the group is active
     */
    /**
     * 确保组处于活跃状态（即已加入并同步）
     *
     * @param timer 限制方法阻塞时间的计时器
     * @throws KafkaException 如果回调抛出异常
     * @return 如果组处于活跃状态则返回true
     */
    boolean ensureActiveGroup(final Timer timer) {
        // 始终确保协调器就绪，因为在发送心跳时可能已断开连接
        // 这不一定需要我们重新加入组
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }

        // 如果需要，启动心跳线程
        startHeartbeatThreadIfNeeded();
        // 如果需要，加入组
        return joinGroupIfNeeded(timer);
    }

    /**
     * 如果需要，启动心跳线程
     * 此方法是同步的，以确保线程安全
     */
    private synchronized void startHeartbeatThreadIfNeeded() {
        // 如果心跳线程不存在，创建并启动新的心跳线程
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    /**
     * 关闭心跳线程
     * 此方法会等待心跳线程完全关闭
     */
    private void closeHeartbeatThread() {
        HeartbeatThread thread;
        synchronized (this) {
            // 如果心跳线程不存在，直接返回
            if (heartbeatThread == null)
                return;
            // 关闭心跳线程
            heartbeatThread.close();
            thread = heartbeatThread;
            heartbeatThread = null;
        }
        try {
            // 等待心跳线程完全关闭
            thread.join();
        } catch (InterruptedException e) {
            // 如果等待被中断，记录警告并抛出中断异常
            log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            throw new InterruptException(e);
        }
    }

    /**
     * Joins the group without starting the heartbeat thread.
     *
     * If this function returns true, the state must always be in STABLE and heartbeat enabled.
     * If this function returns false, the state can be in one of the following:
     *  * UNJOINED: got error response but times out before being able to re-join, heartbeat disabled
     *  * PREPARING_REBALANCE: not yet received join-group response before timeout, heartbeat disabled
     *  * COMPLETING_REBALANCE: not yet received sync-group response before timeout, heartbeat enabled
     *
     * Visible for testing.
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the operation succeeded
     */
    boolean joinGroupIfNeeded(final Timer timer) {
        while (rejoinNeededOrPending()) {
            if (!ensureCoordinatorReady(timer)) {
                return false;
            }

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            if (needsJoinPrepare) {
                // need to set the flag before calling onJoinPrepare since the user callback may throw
                // exception, in which case upon retry we should not retry onJoinPrepare either.
                needsJoinPrepare = false;
                // return false when onJoinPrepare is waiting for committing offset
                if (!onJoinPrepare(timer, generation.generationId, generation.memberId)) {
                    needsJoinPrepare = true;
                    //should not initiateJoinGroup if needsJoinPrepare still is true
                    return false;
                }
            }

            final RequestFuture<ByteBuffer> future = initiateJoinGroup();
            client.poll(future, timer);
            if (!future.isDone()) {
                // we ran out of time
                return false;
            }

            if (future.succeeded()) {
                Generation generationSnapshot;
                MemberState stateSnapshot;

                // Generation data maybe concurrently cleared by Heartbeat thread.
                // Can't use synchronized for {@code onJoinComplete}, because it can be long enough
                // and shouldn't block heartbeat thread.
                // See {@link PlaintextConsumerTest#testMaxPollIntervalMsDelayInAssignment}
                synchronized (AbstractCoordinator.this) {
                    generationSnapshot = this.generation;
                    stateSnapshot = this.state;
                }

                if (!hasGenerationReset(generationSnapshot) && stateSnapshot == MemberState.STABLE) {
                    // Duplicate the buffer in case `onJoinComplete` does not complete and needs to be retried.
                    ByteBuffer memberAssignment = future.value().duplicate();

                    onJoinComplete(generationSnapshot.generationId, generationSnapshot.memberId, generationSnapshot.protocolName, memberAssignment);

                    // Generally speaking we should always resetJoinGroupFuture once the future is done, but here
                    // we can only reset the join group future after the completion callback returns. This ensures
                    // that if the callback is woken up, we will retry it on the next joinGroupIfNeeded.
                    // And because of that we should explicitly trigger resetJoinGroupFuture in other conditions below.
                    resetJoinGroupFuture();
                    needsJoinPrepare = true;
                } else {
                    final String reason = String.format("rebalance failed since the generation/state was " +
                            "modified by heartbeat thread to %s/%s before the rebalance callback triggered",
                            generationSnapshot, stateSnapshot);

                    resetStateAndRejoin(reason, true);
                    resetJoinGroupFuture();
                }
            } else {
                final RuntimeException exception = future.exception();

                resetJoinGroupFuture();
                synchronized (AbstractCoordinator.this) {
                    final String simpleName = exception.getClass().getSimpleName();
                    final String shortReason = String.format("rebalance failed due to %s", simpleName);
                    final String fullReason = String.format("rebalance failed due to '%s' (%s)",
                        exception.getMessage(),
                        simpleName);
                    // Don't need to request rejoin again for MemberIdRequiredException since we've done that in JoinGroupResponseHandler
                    if (!(exception instanceof MemberIdRequiredException)) {
                        requestRejoin(shortReason, fullReason);
                    }
                }

                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof IllegalGenerationException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof MemberIdRequiredException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;

                // We need to return upon expired timer, in case if the client.poll returns immediately and the time
                // has elapsed.
                if (timer.isExpired()) {
                    return false;
                }

                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        }
        return true;
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        if (joinFuture == null) {
            state = MemberState.PREPARING_REBALANCE;
            // a rebalance can be triggered consecutively if the previous one failed,
            // in this case we would not update the start time.
            if (lastRebalanceStartMs == -1L)
                lastRebalanceStartMs = time.milliseconds();
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // do nothing since all the handler logic are in SyncGroupResponseHandler already
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin;
                    // this can be triggered when either join or sync request failed
                    synchronized (AbstractCoordinator.this) {
                        sensors.failedRebalanceSensor.record();
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #onLeaderElected(String, String, List, boolean)} if
     * elected leader by the coordinator.
     *
     * NOTE: This is visible only for testing
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group");
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                new JoinGroupRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setProtocolType(protocolType())
                        .setProtocols(metadata())
                        .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
                        .setReason(JoinGroupRequest.maybeTruncateReason(this.rejoinReason))
        );

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);

        // Note that we override the request timeout using the rebalance timeout since that is the
        // maximum time that it may block on the coordinator. We add an extra 5 seconds for small delays.
        int joinGroupTimeoutMs = Math.max(
            client.defaultRequestTimeoutMs(),
            Math.max(
                rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE,
                rebalanceConfig.rebalanceTimeoutMs) // guard against overflow since rebalance timeout can be MAX_VALUE
            );
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                .compose(new JoinGroupResponseHandler(generation));
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        private JoinGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(joinResponse.data().protocolType())) {
                    log.error("JoinGroup failed: Inconsistent Protocol Type, received {} but expected {}",
                        joinResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful JoinGroup response: {}", joinResponse);
                    sensors.joinSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        if (state != MemberState.PREPARING_REBALANCE) {
                            // if the consumer was woken up before a rebalance completes, we may have already left
                            // the group. In this case, we do not want to continue with the sync group.
                            future.raise(new UnjoinedGroupException());
                        } else {
                            state = MemberState.COMPLETING_REBALANCE;

                            // we only need to enable heartbeat thread whenever we transit to
                            // COMPLETING_REBALANCE state since we always transit from this state to STABLE
                            if (heartbeatThread != null)
                                heartbeatThread.enable();

                            AbstractCoordinator.this.generation = new Generation(
                                joinResponse.data().generationId(),
                                joinResponse.data().memberId(), joinResponse.data().protocolName());

                            log.info("Successfully joined group with generation {}", AbstractCoordinator.this.generation);
                            clientTelemetryReporter.ifPresent(reporter -> reporter.updateMetricsLabels(
                                Collections.singletonMap(ClientTelemetryProvider.GROUP_MEMBER_ID, joinResponse.data().memberId())));

                            if (joinResponse.isLeader()) {
                                onLeaderElected(joinResponse).chain(future);
                            } else {
                                onJoinFollower().chain(future);
                            }
                        }
                    }
                }
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                log.info("JoinGroup failed: Coordinator {} is loading the group.", coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.info("JoinGroup failed: {} Need to re-join the group. Sent generation was {}",
                         error.message(), sentGeneration);
                // only need to reset the member id if generation has not been changed,
                // then retry immediately
                if (generationUnchanged())
                    resetStateOnResponseError(ApiKeys.JOIN_GROUP, error, true);

                future.raise(error);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry with backoff
                markCoordinatorUnknown(error);
                log.info("JoinGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                          error.message(), sentGeneration);
                future.raise(error);
            } else if (error == Errors.FENCED_INSTANCE_ID) {
                // for join-group request, even if the generation has changed we would not expect the instance id
                // gets fenced, and hence we always treat this as a fatal error
                log.error("JoinGroup failed: The group instance id {} has been fenced by another instance. " +
                              "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID
                    || error == Errors.GROUP_AUTHORIZATION_FAILED
                    || error == Errors.GROUP_MAX_SIZE_REACHED) {
                // log the error and re-throw the exception
                log.error("JoinGroup failed due to fatal error: {}", error.message());
                if (error == Errors.GROUP_MAX_SIZE_REACHED) {
                    future.raise(new GroupMaxSizeReachedException("Consumer group " + rebalanceConfig.groupId +
                            " already has the configured maximum number of members."));
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(error);
                }
            } else if (error == Errors.UNSUPPORTED_VERSION) {
                log.error("JoinGroup failed due to unsupported version error. Please unset field group.instance.id " +
                          "and retry to see if the problem resolves");
                future.raise(error);
            } else if (error == Errors.MEMBER_ID_REQUIRED) {
                // Broker requires a concrete member id to be allowed to join the group. Update member id
                // and send another join group request in next cycle.
                String memberId = joinResponse.data().memberId();
                log.debug("JoinGroup failed due to non-fatal error: {}. Will set the member id as {} and then rejoin. " +
                              "Sent generation was {}", error, memberId, sentGeneration);
                synchronized (AbstractCoordinator.this) {
                    AbstractCoordinator.this.generation = new Generation(OffsetCommitRequest.DEFAULT_GENERATION_ID, memberId, null);
                }
                requestRejoin("need to re-join with the given member-id: " + memberId);

                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                log.info("JoinGroup failed due to non-fatal error: REBALANCE_IN_PROGRESS, " +
                    "which could indicate a replication timeout on the broker. Will retry.");
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                log.error("JoinGroup failed due to unexpected error: {}", error.message());
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(
                        new SyncGroupRequestData()
                                .setGroupId(rebalanceConfig.groupId)
                                .setMemberId(generation.memberId)
                                .setProtocolType(protocolType())
                                .setProtocolName(generation.protocolName)
                                .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                .setGenerationId(generation.generationId)
                                .setAssignments(Collections.emptyList())
                );
        log.debug("Sending follower SyncGroup to coordinator {}: {}", this.coordinator, requestBuilder);
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onLeaderElected(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            Map<String, ByteBuffer> groupAssignment = onLeaderElected(
                joinResponse.data().leader(),
                joinResponse.data().protocolName(),
                joinResponse.data().members(),
                joinResponse.data().skipAssignment()
            );

            List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
            for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
                groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                        .setMemberId(assignment.getKey())
                        .setAssignment(Utils.toArray(assignment.getValue()))
                );
            }

            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(
                            new SyncGroupRequestData()
                                    .setGroupId(rebalanceConfig.groupId)
                                    .setMemberId(generation.memberId)
                                    .setProtocolType(protocolType())
                                    .setProtocolName(generation.protocolName)
                                    .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                    .setGenerationId(generation.generationId)
                                    .setAssignments(groupAssignmentList)
                    );
            log.debug("Sending leader SyncGroup to coordinator {}: {}", this.coordinator, requestBuilder);
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, requestBuilder)
                .compose(new SyncGroupResponseHandler(generation));
    }

    private boolean hasGenerationReset(Generation gen) {
        // the member ID might not be reset for ILLEGAL_GENERATION error, so only check generationID and protocol name here
        return gen.generationId == Generation.NO_GENERATION.generationId && gen.protocolName == null;
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        private SyncGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = syncResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(syncResponse.data().protocolType())) {
                    log.error("SyncGroup failed due to inconsistent Protocol Type, received {} but expected {}",
                        syncResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful SyncGroup response: {}", syncResponse);
                    sensors.syncSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        if (!hasGenerationReset(generation) && state == MemberState.COMPLETING_REBALANCE) {
                            // check protocol name only if the generation is not reset
                            final String protocolName = syncResponse.data().protocolName();
                            final boolean protocolNameInconsistent = protocolName != null &&
                                !protocolName.equals(generation.protocolName);

                            if (protocolNameInconsistent) {
                                log.error("SyncGroup failed due to inconsistent Protocol Name, received {} but expected {}",
                                    protocolName, generation.protocolName);

                                future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                            } else {
                                log.info("Successfully synced group in generation {}", generation);
                                state = MemberState.STABLE;
                                rejoinReason = "";
                                rejoinNeeded = false;
                                // record rebalance latency
                                lastRebalanceEndMs = time.milliseconds();
                                sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
                                lastRebalanceStartMs = -1L;

                                future.complete(ByteBuffer.wrap(syncResponse.data().assignment()));
                            }
                        } else {
                            log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                                "receiving SyncGroup response, marking this rebalance as failed and retry",
                                generation, state);
                            // use ILLEGAL_GENERATION error code to let it retry immediately
                            future.raise(Errors.ILLEGAL_GENERATION);
                        }
                    }
                }
            } else {
                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.info("SyncGroup failed: The group began another rebalance. Need to re-join the group. " +
                                 "Sent generation was {}", sentGeneration);
                    future.raise(error);
                } else if (error == Errors.FENCED_INSTANCE_ID) {
                    // for sync-group request, even if the generation has changed we would not expect the instance id
                    // gets fenced, and hence we always treat this as a fatal error
                    log.error("SyncGroup failed: The group instance id {} has been fenced by another instance. " +
                        "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.info("SyncGroup failed: {} Need to re-join the group. Sent generation was {}",
                            error.message(), sentGeneration);
                    if (generationUnchanged()) {
                        // don't reset generation member ID when ILLEGAL_GENERATION, since the member ID might still be valid
                        resetStateOnResponseError(ApiKeys.SYNC_GROUP, error, error != Errors.ILLEGAL_GENERATION);
                    }

                    future.raise(error);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR) {
                    log.info("SyncGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                             error.message(), sentGeneration);
                    markCoordinatorUnknown(error);
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a FindCoordinator request to
     * the given broker node. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        log.debug("Sending FindCoordinator request to broker {}", node);
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(CoordinatorType.GROUP.id())
                .setKey(this.rebalanceConfig.groupId);
        FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
        return client.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler());
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);

            List<Coordinator> coordinators = ((FindCoordinatorResponse) resp.responseBody()).coordinators();
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                future.raise(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            Coordinator coordinatorData = coordinators.get(0);
            Errors error = Errors.forCode(coordinatorData.errorCode());
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    int coordinatorConnectionId = Integer.MAX_VALUE - coordinatorData.nodeId();

                    AbstractCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,
                            coordinatorData.host(),
                            coordinatorData.port());
                    log.info("Discovered group coordinator {}", coordinator);
                    client.tryConnect(coordinator);
                    heartbeat.resetSessionTimeout();
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            log.debug("FindCoordinator request failed due to {}", e.toString());

            if (!(e instanceof RetriableException)) {
                // Remember the exception if fatal so we can ensure it gets thrown by the main thread
                fatalFindCoordinatorException = e;
            }

            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node checkAndGetCoordinator() {
        if (coordinator != null && client.isUnavailable(coordinator)) {
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    private synchronized Node coordinator() {
        return this.coordinator;
    }


    protected synchronized void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected synchronized void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected synchronized void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                    + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                    cause, isDisconnected);
            Node oldCoordinator = this.coordinator;

            // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
            // requests may attempt to do likewise. This also prevents new requests from being sent to the
            // coordinator while the disconnect is in progress.
            this.coordinator = null;

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
            if (!isDisconnected) {
                log.info("Requesting disconnect from last known coordinator {}", oldCoordinator);
                client.disconnectAsync(oldCoordinator);
            }

            lastTimeOfConnectionMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs;
            if (durationOfOngoingDisconnect > rebalanceConfig.rebalanceTimeoutMs)
                log.warn("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnect);
        }
    }

    /**
     * Get the current generation state, regardless of whether it is currently stable.
     * Note that the generation information can be updated while we are still in the middle
     * of a rebalance, after the join-group response is received.
     *
     * @return the current generation
     */
    protected synchronized Generation generation() {
        return generation;
    }

    /**
     * Get the current generation state if the group is stable, otherwise return null
     *
     * @return the current generation or null
     */
    protected synchronized Generation generationIfStable() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    protected synchronized boolean rebalanceInProgress() {
        return this.state == MemberState.PREPARING_REBALANCE || this.state == MemberState.COMPLETING_REBALANCE;
    }

    protected synchronized String memberId() {
        return generation.memberId;
    }

    private synchronized void resetStateAndGeneration(final String reason, final boolean shouldResetMemberId) {
        log.info("Resetting generation {}due to: {}", shouldResetMemberId ? "and member id " : "", reason);

        state = MemberState.UNJOINED;
        if (shouldResetMemberId) {
            generation = Generation.NO_GENERATION;
        } else {
            // keep member id since it might be still valid, to avoid to wait for the old member id leaving group
            // until rebalance timeout in next rebalance
            generation = new Generation(Generation.NO_GENERATION.generationId, generation.memberId, null);
        }
    }

    private synchronized void resetStateAndRejoin(final String reason, final boolean shouldResetMemberId) {
        resetStateAndGeneration(reason, shouldResetMemberId);
        requestRejoin(reason);
        needsJoinPrepare = true;
    }

    synchronized void resetStateOnResponseError(ApiKeys api, Errors error, boolean shouldResetMemberId) {
        final String reason = String.format("encountered %s from %s response", error, api);
        resetStateAndRejoin(reason, shouldResetMemberId);
    }

    synchronized void resetGenerationOnLeaveGroup() {
        resetStateAndRejoin("consumer pro-actively leaving the group", true);
    }

    public synchronized void requestRejoinIfNecessary(final String shortReason,
                                                      final String fullReason) {
        if (!this.rejoinNeeded) {
            requestRejoin(shortReason, fullReason);
        }
    }

    public synchronized void requestRejoin(final String shortReason) {
        requestRejoin(shortReason, shortReason);
    }

    /**
     * Request to rejoin the group.
     *
     * @param shortReason This is the reason passed up to the group coordinator. It must be
     *                    reasonably small.
     * @param fullReason This is the reason logged locally.
     */
    public synchronized void requestRejoin(final String shortReason,
                                           final String fullReason) {
        log.info("Request joining group due to: {}", fullReason);
        this.rejoinReason = shortReason;
        this.rejoinNeeded = true;
    }

    private boolean isProtocolTypeInconsistent(String protocolType) {
        return protocolType != null && !protocolType.equals(protocolType());
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public final void close() {
        close(time.timer(0));
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected void close(Timer timer) {
        try {
            closeHeartbeatThread();
        } finally {
            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized (this) {
                if (rebalanceConfig.leaveGroupOnClose) {
                    onLeavePrepare();
                    maybeLeaveGroup("the consumer is being closed");
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.
                Node coordinator = checkAndGetCoordinator();
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timer))
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections",
                            client.pendingRequestCount(coordinator));
            }
        }
    }

    protected void handlePollTimeoutExpiry() {
        log.warn("consumer poll timeout has expired. This means the time between subsequent calls to poll() " +
            "was longer than the configured max.poll.interval.ms, which typically implies that " +
            "the poll loop is spending too much time processing messages. You can address this " +
            "either by increasing max.poll.interval.ms or by reducing the maximum size of batches " +
            "returned in poll() with max.poll.records.");

        maybeLeaveGroup("consumer poll timeout has expired.");
    }

    /**
     * Sends LeaveGroupRequest and logs the {@code leaveReason}, unless this member is using static membership or is already
     * not part of the group (ie does not have a valid member id, is in the UNJOINED state, or the coordinator is unknown).
     *
     * @param leaveReason the reason to leave the group for logging
     * @throws KafkaException if the rebalance callback throws exception
     */
    public synchronized RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
        RequestFuture<Void> future = null;

        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
        // and the membership expiration is only controlled by session timeout.
        if (isDynamicMember() && !coordinatorUnknown() &&
            state != MemberState.UNJOINED && generation.hasMemberId()) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            log.info("Member {} sending LeaveGroup request to coordinator {} due to {}",
                generation.memberId, coordinator, leaveReason);
            LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(
                rebalanceConfig.groupId,
                Collections.singletonList(new MemberIdentity().setMemberId(generation.memberId).setReason(JoinGroupRequest.maybeTruncateReason(leaveReason)))
            );

            future = client.send(coordinator, request).compose(new LeaveGroupResponseHandler(generation));
            client.pollNoWakeup();
        }

        resetGenerationOnLeaveGroup();

        return future;
    }

    protected boolean isDynamicMember() {
        return rebalanceConfig.groupInstanceId.isEmpty();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        private LeaveGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            final List<MemberResponse> members = leaveResponse.memberResponses();
            if (members.size() > 1) {
                future.raise(new IllegalStateException("The expected leave group response " +
                                                           "should only contain no more than one member info, however get " + members));
            }

            final Errors error = leaveResponse.error();
            if (error == Errors.NONE) {
                log.debug("LeaveGroup response with {} returned successfully: {}", sentGeneration, response);
                future.complete(null);
            } else {
                log.error("LeaveGroup request with {} failed with error: {}", sentGeneration, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request with generation {} and member id {} to coordinator {}",
            generation.generationId, generation.memberId, coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler(generation));
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        private HeartbeatResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatSensor.record(response.requestLatencyMs());
            Errors error = heartbeatResponse.error();

            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                future.complete(null);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                        coordinator());
                markCoordinatorUnknown(error);
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // since we may be sending the request during rebalance, we should check
                // this case and ignore the REBALANCE_IN_PROGRESS error
                synchronized (AbstractCoordinator.this) {
                    if (state == MemberState.STABLE) {
                        requestRejoin("group is already rebalancing");
                        future.raise(error);
                    } else {
                        log.debug("Ignoring heartbeat response with error {} during {} state", error, state);
                        future.complete(null);
                    }
                }
            } else if (error == Errors.ILLEGAL_GENERATION ||
                       error == Errors.UNKNOWN_MEMBER_ID ||
                       error == Errors.FENCED_INSTANCE_ID) {
                if (generationUnchanged()) {
                    log.info("Attempt to heartbeat with {} and group instance id {} failed due to {}, resetting generation",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    // don't reset generation member ID when ILLEGAL_GENERATION, since the member ID is still valid
                    resetStateOnResponseError(ApiKeys.HEARTBEAT, error, error != Errors.ILLEGAL_GENERATION);
                    future.raise(error);
                } else {
                    // if the generation has changed, then ignore this error
                    log.info("Attempt to heartbeat with stale {} and group instance id {} failed due to {}, ignoring the error",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    future.complete(null);
                }
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        CoordinatorResponseHandler(final Generation generation) {
            this.sentGeneration = generation;
        }

        final Generation sentGeneration;
        ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                markCoordinatorUnknown(true, e.getMessage());
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

        boolean generationUnchanged() {
            synchronized (AbstractCoordinator.this) {
                return generation.equals(sentGeneration);
            }
        }
    }

    protected final Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatSensor;
        public final Sensor joinSensor;
        public final Sensor syncSensor;
        public final Sensor successfulRebalanceSensor;
        public final Sensor failedRebalanceSensor;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatSensor = metrics.sensor("heartbeat-latency");
            this.heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatSensor.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinSensor = metrics.sensor("join-latency");
            this.joinSensor.add(metrics.metricName("join-time-avg",
                this.metricGrpName,
                "The average time taken for a group rejoin"), new Avg());
            this.joinSensor.add(metrics.metricName("join-time-max",
                this.metricGrpName,
                "The max time taken for a group rejoin"), new Max());
            this.joinSensor.add(createMeter(metrics, metricGrpName, "join", "group joins"));

            this.syncSensor = metrics.sensor("sync-latency");
            this.syncSensor.add(metrics.metricName("sync-time-avg",
                this.metricGrpName,
                "The average time taken for a group sync"), new Avg());
            this.syncSensor.add(metrics.metricName("sync-time-max",
                this.metricGrpName,
                "The max time taken for a group sync"), new Max());
            this.syncSensor.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
                this.metricGrpName,
                "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Avg());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
                this.metricGrpName,
                "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Max());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                this.metricGrpName,
                "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
                new CumulativeSum());
            this.successfulRebalanceSensor.add(
                metrics.metricName("rebalance-total",
                    this.metricGrpName,
                    "The total number of successful rebalance events, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new CumulativeCount()
            );
            this.successfulRebalanceSensor.add(
                metrics.metricName(
                    "rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of successful rebalance events per hour, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
            this.failedRebalanceSensor.add(
                metrics.metricName("failed-rebalance-total",
                    this.metricGrpName,
                    "The total number of failed rebalance events"),
                new CumulativeCount()
            );
            this.failedRebalanceSensor.add(
                metrics.metricName(
                    "failed-rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of failed rebalance events per hour"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            Measurable lastRebalance = (config, now) -> {
                if (lastRebalanceEndMs == -1L)
                    // if no rebalance is ever triggered, we just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-rebalance-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last successful rebalance event"),
                lastRebalance);

            Measurable lastHeartbeat = (config, now) -> {
                if (heartbeat.lastHeartbeatSend() == 0L)
                    // if no heartbeat is ever triggered, just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last coordinator heartbeat was sent"),
                lastHeartbeat);
        }
    }

    private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        private boolean enabled = false;
        private boolean closed = false;
        private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        // we do not need to heartbeat we are not part of a group yet;
                        // also if we already have fatal error, the client will be
                        // crashed soon, hence we do not need to continue heartbeating either
                        if (state.hasNotJoinedGroup() || hasFailed()) {
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null) {
                                // clear the future so that after the backoff, if the hb still sees coordinator unknown in
                                // the next iteration it will try to re-discover the coordinator in case the main thread cannot
                                clearFindCoordinatorFuture();
                            } else {
                                lookupCoordinator();
                            }
                            // backoff properly
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            markCoordinatorUnknown("session timed out without receiving a "
                                    + "heartbeat response");
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll().
                            handlePollTimeoutExpiry();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected. Note that the heartbeat timing takes account of
                            // exponential backoff.
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);
                            final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                            heartbeatFuture.addListener(new RequestFutureListener<>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat();
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat();
                                        } else if (e instanceof FencedInstanceIdException) {
                                            log.error("Caught fenced group.instance.id {} error in heartbeat thread", rebalanceConfig.groupInstanceId);
                                            heartbeatThread.failed.set(e);
                                        } else {
                                            heartbeat.failHeartbeat();
                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread", e);
                this.failed.set(new RuntimeException(e));
            } catch (Throwable e) {
                log.error("Heartbeat thread failed due to unexpected error", e);
                if (e instanceof RuntimeException)
                    this.failed.set((RuntimeException) e);
                else
                    this.failed.set(new RuntimeException(e));
            } finally {
                log.debug("Heartbeat thread has closed");
                this.closed = true;
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocolName;

        public Generation(int generationId, String memberId, String protocolName) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocolName = protocolName;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                    Objects.equals(memberId, that.memberId) &&
                    Objects.equals(protocolName, that.protocolName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocolName);
        }

        @Override
        public String toString() {
            return "Generation{" +
                    "generationId=" + generationId +
                    ", memberId='" + memberId + '\'' +
                    ", protocol='" + protocolName + '\'' +
                    '}';
        }
    }

    private static class UnjoinedGroupException extends RetriableException {

    }

    // For testing only below
    final Heartbeat heartbeat() {
        return heartbeat;
    }

    final String rejoinReason() {
        return rejoinReason;
    }

    final synchronized void setLastRebalanceTime(final long timestamp) {
        lastRebalanceEndMs = timestamp;
    }

    /**
     * Check whether given generation id is matching the record within current generation.
     *
     * @param generationId generation id
     * @return true if the two ids are matching.
     */
    final boolean hasMatchingGenerationId(int generationId) {
        return !generation.equals(Generation.NO_GENERATION) && generation.generationId == generationId;
    }

    final boolean hasUnknownGeneration() {
        return generation.equals(Generation.NO_GENERATION);
    }

    /**
     * @return true if the current generation's member ID is valid, false otherwise
     */
    final boolean hasValidMemberId() {
        return !hasUnknownGeneration() && generation.hasMemberId();
    }

    final synchronized void setNewGeneration(final Generation generation) {
        this.generation = generation;
    }

    final synchronized void setNewState(final MemberState state) {
        this.state = state;
    }
}
