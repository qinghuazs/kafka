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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * {@code ShareConsumeRequestManager} 负责生成 {@link ShareFetchRequest} 和
 * {@link ShareAcknowledgeRequest} 来获取和确认为共享组中的消费者投递的记录。
 */
@SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
public class ShareConsumeRequestManager implements RequestManager, MemberStateListener, Closeable {
    // 时间工具，用于获取当前时间和计算超时
    private final Time time;
    // 日志记录器
    private final Logger log;
    // 日志上下文，用于创建日志记录器
    private final LogContext logContext;
    // 消费者组ID
    private final String groupId;
    // 消费者元数据，包含主题分区信息和集群信息
    private final ConsumerMetadata metadata;
    // 订阅状态，管理消费者的主题订阅信息
    private final SubscriptionState subscriptions;
    // 获取配置，包含获取请求的各种参数设置
    private final FetchConfig fetchConfig;
    // 共享获取缓冲区，用于存储从服务器获取的记录
    protected final ShareFetchBuffer shareFetchBuffer;
    // 后台事件处理器，用于处理异步事件
    private final BackgroundEventHandler backgroundEventHandler;
    // 会话处理器映射，按节点ID索引，管理与不同节点的会话
    private final Map<Integer, ShareSessionHandler> sessionHandlers;
    // 有待处理请求的节点集合，用于跟踪哪些节点有未完成的请求
    private final Set<Integer> nodesWithPendingRequests;
    // 共享获取指标管理器，用于记录和监控指标
    private final ShareFetchMetricsManager metricsManager;
    // 幂等关闭器，确保关闭操作只执行一次
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    // 成员ID，标识共享组中的消费者
    private Uuid memberId;
    // 是否需要获取更多记录的标志
    private boolean fetchMoreRecords = false;
    // 待发送的获取确认映射，按主题分区ID索引
    private final Map<TopicIdPartition, Acknowledgements> fetchAcknowledgementsToSend;
    // 正在传输中的获取确认映射，按主题分区ID索引
    private final Map<TopicIdPartition, Acknowledgements> fetchAcknowledgementsInFlight;
    // 确认请求状态映射，按节点ID索引
    private final Map<Integer, Tuple<AcknowledgeRequestState>> acknowledgeRequestStates;
    // 重试退避时间（毫秒）
    private final long retryBackoffMs;
    // 最大重试退避时间（毫秒）
    private final long retryBackoffMaxMs;
    // 是否正在关闭的标志
    private boolean closing = false;
    // 关闭完成的Future对象
    private final CompletableFuture<Void> closeFuture;
    // 是否已注册确认提交回调的标志
    private boolean isAcknowledgementCommitCallbackRegistered = false;
    // 主题名称映射，用于将主题ID和分区映射到主题名称
    private final Map<IdAndPartition, String> topicNamesMap = new HashMap<>();

    /**
     * 构造函数，初始化共享消费请求管理器
     * 
     * @param time 时间工具，用于获取当前时间和计算超时
     * @param logContext 日志上下文，用于创建日志记录器
     * @param groupId 消费者组ID
     * @param metadata 消费者元数据，包含主题分区信息和集群信息
     * @param subscriptions 订阅状态，管理消费者的主题订阅信息
     * @param fetchConfig 获取配置，包含获取请求的各种参数设置
     * @param shareFetchBuffer 共享获取缓冲区，用于存储从服务器获取的记录
     * @param backgroundEventHandler 后台事件处理器，用于处理异步事件
     * @param metricsManager 共享获取指标管理器，用于记录和监控指标
     * @param retryBackoffMs 重试退避时间（毫秒）
     * @param retryBackoffMaxMs 最大重试退避时间（毫秒）
     */
    ShareConsumeRequestManager(final Time time,
                               final LogContext logContext,
                               final String groupId,
                               final ConsumerMetadata metadata,
                               final SubscriptionState subscriptions,
                               final FetchConfig fetchConfig,
                               final ShareFetchBuffer shareFetchBuffer,
                               final BackgroundEventHandler backgroundEventHandler,
                               final ShareFetchMetricsManager metricsManager,
                               final long retryBackoffMs,
                               final long retryBackoffMaxMs) {
        // 初始化时间工具
        this.time = time;
        // 从日志上下文创建日志记录器
        this.log = logContext.logger(ShareConsumeRequestManager.class);
        // 保存日志上下文
        this.logContext = logContext;
        // 保存消费者组ID
        this.groupId = groupId;
        // 保存消费者元数据
        this.metadata = metadata;
        // 保存订阅状态
        this.subscriptions = subscriptions;
        // 保存获取配置
        this.fetchConfig = fetchConfig;
        // 保存共享获取缓冲区
        this.shareFetchBuffer = shareFetchBuffer;
        // 保存后台事件处理器
        this.backgroundEventHandler = backgroundEventHandler;
        // 保存共享获取指标管理器
        this.metricsManager = metricsManager;
        // 保存重试退避时间
        this.retryBackoffMs = retryBackoffMs;
        // 保存最大重试退避时间
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        // 初始化会话处理器映射
        this.sessionHandlers = new HashMap<>();
        // 初始化有待处理请求的节点集合
        this.nodesWithPendingRequests = new HashSet<>();
        // 初始化确认请求状态映射
        this.acknowledgeRequestStates = new HashMap<>();
        // 初始化待发送的获取确认映射
        this.fetchAcknowledgementsToSend = new HashMap<>();
        // 初始化正在传输中的获取确认映射
        this.fetchAcknowledgementsInFlight = new HashMap<>();
        // 初始化关闭完成的Future对象
        this.closeFuture = new CompletableFuture<>();
    }

    /**
     * 轮询方法，生成并返回需要发送的请求
     * 实现了RequestManager接口的poll方法
     * 
     * @param currentTimeMs 当前时间（毫秒）
     * @return 包含待发送请求的轮询结果
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        // 如果成员ID为空，表示消费者尚未加入共享组，返回空结果
        if (memberId == null) {
            return PollResult.EMPTY;
        }

        // 在获取更多记录之前，先处理待发送的确认
        PollResult pollResult = processAcknowledgements(currentTimeMs);
        // 如果有确认需要发送，直接返回确认请求
        if (pollResult != null) {
            return pollResult;
        }

        // 如果不需要获取更多记录，返回空结果
        if (!fetchMoreRecords) {
            return PollResult.EMPTY;
        }

        // 创建节点到会话处理器的映射
        Map<Node, ShareSessionHandler> handlerMap = new HashMap<>();
        // 获取主题名称到主题ID的映射
        Map<String, Uuid> topicIds = metadata.topicIds();
        // 创建已获取分区的集合，用于跟踪
        Set<TopicIdPartition> fetchedPartitions = new HashSet<>();
        // 遍历需要获取的分区
        for (TopicPartition partition : partitionsToFetch()) {
            // 获取分区的领导节点
            Optional<Node> leaderOpt = metadata.currentLeader(partition).leader;

            // 如果领导节点不存在，请求更新元数据并继续下一个分区
            if (leaderOpt.isEmpty()) {
                log.debug("Requesting metadata update for partition {} since current leader node is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            // 获取主题ID
            Uuid topicId = topicIds.get(partition.topic());
            // 如果主题ID不存在，请求更新元数据并继续下一个分区
            if (topicId == null) {
                log.debug("Requesting metadata update for partition {} since topic ID is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            // 获取领导节点
            Node node = leaderOpt.get();
            // 如果节点有待处理的请求，跳过此分区
            if (nodesWithPendingRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous fetch request to {} has not been processed", partition, node.id());
            } else {
                // 如果有领导节点且没有正在处理的请求，发起新的获取请求
                // 获取或创建会话处理器
                ShareSessionHandler handler = handlerMap.computeIfAbsent(node,
                        k -> sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n, memberId)));

                // 创建主题ID分区对象
                TopicIdPartition tip = new TopicIdPartition(topicId, partition);
                // 获取并移除待发送的确认
                Acknowledgements acknowledgementsToSend = fetchAcknowledgementsToSend.remove(tip);
                // 如果有确认需要发送
                if (acknowledgementsToSend != null) {
                    // 记录已发送的确认数量
                    metricsManager.recordAcknowledgementSent(acknowledgementsToSend.size());
                    // 将确认添加到正在传输的映射中
                    fetchAcknowledgementsInFlight.put(tip, acknowledgementsToSend);
                }
                // 向会话处理器添加要获取的分区
                handler.addPartitionToFetch(tip, acknowledgementsToSend);
                // 将分区添加到已获取分区集合
                fetchedPartitions.add(tip);
                // 更新主题名称映射
                topicNamesMap.putIfAbsent(new IdAndPartition(tip.topicId(), tip.partition()), tip.topic());

                log.debug("Added fetch request for partition {} to node {}", tip, node.id());
            }
        }

        // 创建存储即将忘记的分区列表的映射
        Map<Node, List<TopicIdPartition>> partitionsToForgetMap = new HashMap<>();
        // 获取集群信息
        Cluster cluster = metadata.fetch();
        // 遍历会话处理器，查看是否有需要为不再是当前订阅一部分的分区发送确认
        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            // 获取节点
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                // 如果节点有待处理的请求，跳过
                if (nodesWithPendingRequests.contains(node.id())) {
                    log.trace("Skipping fetch because previous fetch request to {} has not been processed", node.id());
                } else {
                    // 遍历会话中的分区
                    for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                        // 如果分区不在已获取分区集合中
                        if (!fetchedPartitions.contains(tip)) {
                            // 获取并移除待发送的确认
                            Acknowledgements acknowledgementsToSend = fetchAcknowledgementsToSend.remove(tip);

                            // 如果有确认需要发送
                            if (acknowledgementsToSend != null) {
                                // 记录已发送的确认数量
                                metricsManager.recordAcknowledgementSent(acknowledgementsToSend.size());
                                // 将确认添加到正在传输的映射中
                                fetchAcknowledgementsInFlight.put(tip, acknowledgementsToSend);

                                // 向会话处理器添加要获取的分区
                                sessionHandler.addPartitionToFetch(tip, acknowledgementsToSend);
                                // 将处理器添加到处理器映射
                                handlerMap.put(node, sessionHandler);

                                // 初始化并添加到要忘记的分区映射
                                partitionsToForgetMap.putIfAbsent(node, new ArrayList<>());
                                partitionsToForgetMap.get(node).add(tip);

                                // 更新主题名称映射
                                topicNamesMap.putIfAbsent(new IdAndPartition(tip.topicId(), tip.partition()), tip.topic());
                                // 将分区添加到已获取分区集合
                                fetchedPartitions.add(tip);
                                log.debug("Added fetch request for previously subscribed partition {} to node {}", tip, node.id());
                            }
                        }
                    }
                }
            }
        });

        // 创建节点到请求构建器的映射
        Map<Node, ShareFetchRequest.Builder> builderMap = new LinkedHashMap<>();
        // 遍历处理器映射，为每个节点创建请求构建器
        for (Map.Entry<Node, ShareSessionHandler> entry : handlerMap.entrySet()) {
            // 创建共享获取请求构建器
            ShareFetchRequest.Builder builder = entry.getValue().newShareFetchBuilder(groupId, fetchConfig);
            Node node = entry.getKey();

            // 如果有要忘记的分区，更新请求构建器
            if (partitionsToForgetMap.containsKey(node)) {
                // 如果忘记的主题数据为空，初始化它
                if (builder.data().forgottenTopicsData() == null) {
                    builder.data().setForgottenTopicsData(new ArrayList<>());
                }
                // 更新要忘记的数据
                builder.updateForgottenData(partitionsToForgetMap.get(node));
            }

            // 将构建器添加到映射
            builderMap.put(node, builder);
        }

        // 创建未发送请求列表
        List<UnsentRequest> requests = builderMap.entrySet().stream().map(entry -> {
            // 获取目标节点
            Node target = entry.getKey();
            log.trace("Building ShareFetch request to send to node {}", target.id());
            // 获取请求构建器
            ShareFetchRequest.Builder requestBuilder = entry.getValue();

            // 将节点添加到有待处理请求的节点集合
            nodesWithPendingRequests.add(target.id());

            // 创建响应处理器
            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                // 如果有错误，处理获取失败
                if (error != null) {
                    handleShareFetchFailure(target, requestBuilder.data(), error);
                } else {
                    // 否则处理获取成功
                    handleShareFetchSuccess(target, requestBuilder.data(), clientResponse);
                }
            };
            // 创建并返回未发送请求，设置完成回调
            return new UnsentRequest(requestBuilder, Optional.of(target)).whenComplete(responseHandler);
        }).collect(Collectors.toList());

        // 返回包含请求的轮询结果
        return new PollResult(requests);
    }

    /**
     * 设置获取标志并存储确认信息
     * 
     * @param acknowledgementsMap 主题分区ID到确认的映射
     */
    public void fetch(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        // 如果当前不需要获取更多记录，设置标志为true
        if (!fetchMoreRecords) {
            log.debug("Fetch more data");
            fetchMoreRecords = true;
        }

        // 将通过ShareFetch发送的确认存储在此映射中
        // 遍历确认映射，合并到待发送的确认映射中
        acknowledgementsMap.forEach((tip, acks) -> fetchAcknowledgementsToSend.merge(tip, acks, Acknowledgements::merge));
    }

    /**
     * 处理确认请求状态并准备在poll()中发送的确认列表
     *
     * @param currentTimeMs 当前时间（毫秒）
     *
     * @return 包含零个或多个确认的轮询结果
     */
    private PollResult processAcknowledgements(long currentTimeMs) {
        // 创建未发送请求列表，用于存储需要发送的确认请求
        List<UnsentRequest> unsentRequests = new ArrayList<>();
        // 创建原子布尔值，用于标记异步请求是否已发送
        AtomicBoolean isAsyncSent = new AtomicBoolean();
        // 遍历所有节点的确认请求状态
        for (Map.Entry<Integer, Tuple<AcknowledgeRequestState>> requestStates : acknowledgeRequestStates.entrySet()) {
            // 获取节点ID
            int nodeId = requestStates.getKey();
            // 检查节点是否空闲，如果不空闲则跳过确认请求
            if (!isNodeFree(nodeId)) {
                log.trace("Skipping acknowledge request because previous request to {} has not been processed, so acks are not sent", nodeId);
            } else {
                isAsyncSent.set(false);
                // 首先发送来自commitAsync的确认请求
                maybeBuildRequest(requestStates.getValue().getAsyncRequest(), currentTimeMs, true, isAsyncSent).ifPresent(unsentRequests::add);
            
                // 确保只有在没有commitAsync请求需要处理时才开始处理commitSync/close
                if (isAsyncSent.get()) {
                    if (!isNodeFree(nodeId)) {
                        log.trace("Skipping acknowledge request because previous request to {} has not been processed, so acks are not sent", nodeId);
                        continue;
                    }
            
                    // 只有在处理完异步和同步请求后才尝试处理关闭请求
                    if (requestStates.getValue().getSyncRequestQueue() == null) {
                        AcknowledgeRequestState closeRequestState = requestStates.getValue().getCloseRequest();
            
                        maybeBuildRequest(closeRequestState, currentTimeMs, false, isAsyncSent).ifPresent(unsentRequests::add);
                    } else {
                        // 处理来自commitSync的确认请求
                        for (AcknowledgeRequestState acknowledgeRequestState : requestStates.getValue().getSyncRequestQueue()) {
                            maybeBuildRequest(acknowledgeRequestState, currentTimeMs, false, isAsyncSent).ifPresent(unsentRequests::add);
                        }
                    }
                }
            }

        }

        // 初始化轮询结果为null
        PollResult pollResult = null;
        // 如果有未发送的请求
        if (!unsentRequests.isEmpty()) {
            // 创建新的轮询结果对象，包含未发送的请求
            pollResult = new PollResult(unsentRequests);
        } else if (checkAndRemoveCompletedAcknowledgements()) {
            // 检查并移除已完成的确认请求状态
            // 返回空结果，直到所有确认请求状态被处理完毕
            pollResult = PollResult.EMPTY;
        } else if (closing) {
            // 如果正在关闭
            if (!closeFuture.isDone()) {
                // 完成关闭操作
                closeFuture.complete(null);
            }
            // 返回空结果
            pollResult = PollResult.EMPTY;
        }
        // 返回轮询结果
        return pollResult;
    }

    // 检查节点是否空闲的方法
// 如果节点没有待处理请求，则认为节点是空闲的
private boolean isNodeFree(int nodeId) {
    return !nodesWithPendingRequests.contains(nodeId);
}

    // 设置是否已注册确认提交回调的方法
// 用于标记确认提交回调是否已注册
public void setAcknowledgementCommitCallbackRegistered(boolean isAcknowledgementCommitCallbackRegistered) {
    this.isAcknowledgementCommitCallbackRegistered = isAcknowledgementCommitCallbackRegistered;
}

    // 可能发送共享确认提交回调事件的方法
// 如果确认提交回调已注册，则创建事件并添加到后台事件处理器
private void maybeSendShareAcknowledgeCommitCallbackEvent(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
    if (isAcknowledgementCommitCallbackRegistered) {
        ShareAcknowledgementCommitCallbackEvent event = new ShareAcknowledgementCommitCallbackEvent(acknowledgementsMap);
        backgroundEventHandler.add(event);
    }
}

    /**
     *
     * @param acknowledgeRequestState Contains the acknowledgements to be sent.
     * @param currentTimeMs The current time in ms.
     * @param onCommitAsync Boolean to denote if the acknowledgements came from a commitAsync or not.
     * @param isAsyncSent Boolean to indicate if the async request has been sent.
     *
     * @return Returns the request if it was built.
     */
    // 可能构建请求的方法
// 根据确认请求状态和当前时间决定是否构建请求
// 如果请求状态为空或已过期，则返回空
// 如果请求可以发送，则构建请求并返回
private Optional<UnsentRequest> maybeBuildRequest(AcknowledgeRequestState acknowledgeRequestState,
                                                  long currentTimeMs,
                                                  boolean onCommitAsync,
                                                  AtomicBoolean isAsyncSent) {
    boolean asyncSent = true;
    try {
        if (acknowledgeRequestState == null || (!acknowledgeRequestState.onClose() && acknowledgeRequestState.isEmpty())) {
            return Optional.empty();
        }

        if (acknowledgeRequestState.maybeExpire()) {
            // 处理超时的确认请求
            for (TopicIdPartition tip : acknowledgeRequestState.incompleteAcknowledgements.keySet()) {
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getIncompleteAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeTimedOut(tip);
            }
            acknowledgeRequestState.incompleteAcknowledgements.clear();
            return Optional.empty();
        }

        if (!acknowledgeRequestState.canSendRequest(currentTimeMs)) {
            // 等待退避时间后才能发送请求
            asyncSent = false;
            return Optional.empty();
        }

        UnsentRequest request = acknowledgeRequestState.buildRequest();
        if (request == null) {
            asyncSent = false;
            return Optional.empty();
        }

        acknowledgeRequestState.onSendAttempt(currentTimeMs);
        return Optional.of(request);
    } finally {
        if (onCommitAsync) {
            isAsyncSent.set(asyncSent);
        }
    }
}

    /**
     * Prunes the empty acknowledgementRequestStates in {@link #acknowledgeRequestStates}
     *
     * @return Returns true if there are still any acknowledgements left to be processed.
     */
    // 检查并移除已完成的确认请求的方法
// 遍历确认请求状态映射，移除已完成的请求
// 如果请求状态为空或已完成，则从映射中移除
private boolean checkAndRemoveCompletedAcknowledgements() {
    boolean areAnyAcksLeft = false;
    Iterator<Map.Entry<Integer, Tuple<AcknowledgeRequestState>>> iterator = acknowledgeRequestStates.entrySet().iterator();

    while (iterator.hasNext()) {
        Map.Entry<Integer, Tuple<AcknowledgeRequestState>> acknowledgeRequestStatePair = iterator.next();
        boolean areAsyncAcksLeft = true, areSyncAcksLeft = true;
        if (!isRequestStateInProgress(acknowledgeRequestStatePair.getValue().getAsyncRequest())) {
            acknowledgeRequestStatePair.getValue().setAsyncRequest(null);
            areAsyncAcksLeft = false;
        }

        if (!areRequestStatesInProgress(acknowledgeRequestStatePair.getValue().getSyncRequestQueue())) {
            acknowledgeRequestStatePair.getValue().nullifySyncRequestQueue();
            areSyncAcksLeft = false;
        }

        if (!isRequestStateInProgress(acknowledgeRequestStatePair.getValue().getCloseRequest())) {
            acknowledgeRequestStatePair.getValue().setCloseRequest(null);
        }

        if (areAsyncAcksLeft || areSyncAcksLeft) {
            areAnyAcksLeft = true;
        } else if (acknowledgeRequestStatePair.getValue().getCloseRequest() == null) {
            iterator.remove();
        }
    }

    if (!acknowledgeRequestStates.isEmpty()) areAnyAcksLeft = true;
    return areAnyAcksLeft;
}

    private boolean isRequestStateInProgress(AcknowledgeRequestState acknowledgeRequestState) {
        if (acknowledgeRequestState == null) {
            return false;
        } else if (acknowledgeRequestState.onClose()) {
            return !acknowledgeRequestState.isProcessed;
        } else {
            return !(acknowledgeRequestState.isEmpty());
        }
    }

    private boolean areRequestStatesInProgress(Queue<AcknowledgeRequestState> acknowledgeRequestStates) {
        if (acknowledgeRequestStates == null) return false;
        for (AcknowledgeRequestState acknowledgeRequestState : acknowledgeRequestStates) {
            if (isRequestStateInProgress(acknowledgeRequestState)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Enqueue an AcknowledgeRequestState to be picked up on the next poll
     *
     * @param acknowledgementsMap The acknowledgements to commit
     * @param deadlineMs          Time until which the request will be retried if it fails with
     *                            an expected retriable error.
     *
     * @return The future which completes when the acknowledgements finished
     */
    public CompletableFuture<Map<TopicIdPartition, Acknowledgements>> commitSync(
            final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
            final long deadlineMs) {
        final AtomicInteger resultCount = new AtomicInteger();
        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.of(future));

        final Cluster cluster = metadata.fetch();

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

                // Add the incoming commitSync() request to the queue.
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();
                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added sync acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        resultCount.incrementAndGet();
                    }
                }

                acknowledgeRequestStates.get(nodeId).addSyncRequest(new AcknowledgeRequestState(logContext,
                        ShareConsumeRequestManager.class.getSimpleName() + ":1",
                        deadlineMs,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        sessionHandler,
                        nodeId,
                        acknowledgementsMapForNode,
                        resultHandler,
                        AcknowledgeRequestType.COMMIT_SYNC
                ));
            }

        });

        resultHandler.completeIfEmpty();
        return future;
    }

    /**
     * Enqueue an AcknowledgeRequestState to be picked up on the next poll.
     *
     * @param acknowledgementsMap The acknowledgements to commit
     */
    public void commitAsync(final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        final Cluster cluster = metadata.fetch();
        final ResultHandler resultHandler = new ResultHandler(Optional.empty());

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();

                acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added async acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        AcknowledgeRequestState asyncRequestState = acknowledgeRequestStates.get(nodeId).getAsyncRequest();
                        if (asyncRequestState == null) {
                            acknowledgeRequestStates.get(nodeId).setAsyncRequest(new AcknowledgeRequestState(logContext,
                                    ShareConsumeRequestManager.class.getSimpleName() + ":2",
                                    Long.MAX_VALUE,
                                    retryBackoffMs,
                                    retryBackoffMaxMs,
                                    sessionHandler,
                                    nodeId,
                                    acknowledgementsMapForNode,
                                    resultHandler,
                                    AcknowledgeRequestType.COMMIT_ASYNC
                            ));
                        } else {
                            Acknowledgements prevAcks = asyncRequestState.acknowledgementsToSend.putIfAbsent(tip, acknowledgements);
                            if (prevAcks != null) {
                                asyncRequestState.acknowledgementsToSend.get(tip).merge(acknowledgements);
                            }
                        }
                    }
                }
            }
        });

        resultHandler.completeIfEmpty();
    }

    /**
     * Enqueue the final AcknowledgeRequestState used to commit the final acknowledgements and
     * close the share sessions.
     *
     * @param acknowledgementsMap The acknowledgements to commit
     * @param deadlineMs          Time until which the request will be retried if it fails with
     *                            an expected retriable error.
     *
     * @return The future which completes when the acknowledgements finished
     */
    public CompletableFuture<Void> acknowledgeOnClose(final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                                      final long deadlineMs) {
        final Cluster cluster = metadata.fetch();
        final AtomicInteger resultCount = new AtomicInteger();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.empty());

        closing = true;

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();
                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.getOrDefault(tip, Acknowledgements.empty());

                    Acknowledgements acksFromShareFetch = fetchAcknowledgementsToSend.remove(tip);

                    if (acksFromShareFetch != null) {
                        acknowledgements.merge(acksFromShareFetch);
                    }

                    if (acknowledgements != null && !acknowledgements.isEmpty()) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added closing acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        resultCount.incrementAndGet();
                    }
                }

                acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

                // Ensure there is no close() request already present as they are blocking calls
                // and only one request can be active at a time.
                if (acknowledgeRequestStates.get(nodeId).getCloseRequest() != null && !acknowledgeRequestStates.get(nodeId).getCloseRequest().isEmpty()) {
                    log.error("Attempt to call close() when there is an existing close request for node {}-{}", node.id(), acknowledgeRequestStates.get(nodeId).getSyncRequestQueue());
                    closeFuture.completeExceptionally(
                            new IllegalStateException("Attempt to call close() when there is an existing close request for node : " + node.id()));
                } else {
                    // There can only be one close() happening at a time. So per node, there will be one acknowledge request state.
                    acknowledgeRequestStates.get(nodeId).setCloseRequest(new AcknowledgeRequestState(logContext,
                            ShareConsumeRequestManager.class.getSimpleName() + ":3",
                            deadlineMs,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            sessionHandler,
                            nodeId,
                            acknowledgementsMapForNode,
                            resultHandler,
                            AcknowledgeRequestType.CLOSE
                    ));

                }
            }
        });

        resultHandler.completeIfEmpty();
        return closeFuture;
    }

    private void handleShareFetchSuccess(Node fetchTarget,
                                         @SuppressWarnings("unused") ShareFetchRequestData requestData,
                                         ClientResponse resp) {
        try {
            log.debug("Completed ShareFetch request from node {} successfully", fetchTarget.id());
            final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find ShareSessionHandler for node {}. Ignoring ShareFetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.UNKNOWN_TOPIC_ID) {
                    metadata.requestUpdate(false);
                }
                return;
            }

            final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();

            response.data().responses().forEach(topicResponse ->
                    topicResponse.partitions().forEach(partition ->
                            responseData.put(new TopicIdPartition(topicResponse.topicId(),
                                    partition.partitionIndex(),
                                    metadata.topicNames().getOrDefault(topicResponse.topicId(),
                                            topicNamesMap.remove(new IdAndPartition(topicResponse.topicId(), partition.partitionIndex())))), partition))
            );

            final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
            final ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(metricsManager, partitions);

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition tip = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("ShareFetch for partition {} returned fetch data {}", tip, partitionData);

                Acknowledgements acks = fetchAcknowledgementsInFlight.remove(tip);
                if (acks != null) {
                    if (partitionData.acknowledgeErrorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acks.size());
                    }
                    acks.setAcknowledgeErrorCode(Errors.forCode(partitionData.acknowledgeErrorCode()));
                    Map<TopicIdPartition, Acknowledgements> acksMap = Collections.singletonMap(tip, acks);
                    maybeSendShareAcknowledgeCommitCallbackEvent(acksMap);
                }

                Errors partitionError = Errors.forCode(partitionData.errorCode());
                if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", tip, partitionError, partitionData.currentLeader());
                    if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                        partitionsWithUpdatedLeaderInfo.put(tip.topicPartition(), new Metadata.LeaderIdAndEpoch(
                            Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                    }
                }

                ShareCompletedFetch completedFetch = new ShareCompletedFetch(
                        logContext,
                        BufferSupplier.create(),
                        tip,
                        partitionData,
                        shareFetchMetricsAggregator,
                        requestVersion);
                shareFetchBuffer.add(completedFetch);

                if (!partitionData.acquiredRecords().isEmpty()) {
                    fetchMoreRecords = false;
                }
            }

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
            }

            metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareFetchFailure(Node fetchTarget,
                                         ShareFetchRequestData requestData,
                                         Throwable error) {
        try {
            log.debug("Completed ShareFetch request from node {} unsuccessfully {}", fetchTarget.id(), Errors.forException(error));
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
            if (handler != null) {
                handler.handleError(error);
            }

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));

                Acknowledgements acks = fetchAcknowledgementsInFlight.remove(tip);
                if (acks != null) {
                    metricsManager.recordFailedAcknowledgements(acks.size());
                    acks.setAcknowledgeErrorCode(Errors.forException(error));
                    Map<TopicIdPartition, Acknowledgements> acksMap = Collections.singletonMap(tip, acks);
                    maybeSendShareAcknowledgeCommitCallbackEvent(acksMap);
                }
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeSuccess(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               ClientResponse resp,
                                               long responseCompletionTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge request from node {} successfully", fetchTarget.id());
            ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();

            if (acknowledgeRequestState.onClose()) {
                response.data().responses().forEach(topic -> topic.partitions().forEach(partition -> {
                    TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                            partition.partitionIndex(),
                            metadata.topicNames().get(topic.topicId()));
                    if (partition.errorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                    }
                    acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forCode(partition.errorCode()));
                }));

                acknowledgeRequestState.onSuccessfulAttempt(responseCompletionTimeMs);
                acknowledgeRequestState.processingComplete();

            } else {
                if (!acknowledgeRequestState.sessionHandler.handleResponse(response, resp.requestHeader().apiVersion())) {
                    // Received a response-level error code.
                    acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);

                    if (response.error().exception() instanceof RetriableException) {
                        // We retry the request until the timer expires, unless we are closing.
                        acknowledgeRequestState.moveAllToIncompleteAcks();
                    } else {
                        response.data().responses().forEach(shareAcknowledgeTopicResponse -> shareAcknowledgeTopicResponse.partitions().forEach(partitionData -> {
                            TopicIdPartition tip = new TopicIdPartition(shareAcknowledgeTopicResponse.topicId(),
                                    partitionData.partitionIndex(),
                                    metadata.topicNames().get(shareAcknowledgeTopicResponse.topicId()));

                            acknowledgeRequestState.handleAcknowledgeErrorCode(tip, response.error());
                        }));
                        acknowledgeRequestState.processingComplete();
                    }
                } else {
                    AtomicBoolean shouldRetry = new AtomicBoolean(false);
                    // Check all partition level error codes
                    response.data().responses().forEach(shareAcknowledgeTopicResponse -> shareAcknowledgeTopicResponse.partitions().forEach(partitionData -> {
                        Errors partitionError = Errors.forCode(partitionData.errorCode());
                        TopicIdPartition tip = new TopicIdPartition(shareAcknowledgeTopicResponse.topicId(),
                                partitionData.partitionIndex(),
                                metadata.topicNames().get(shareAcknowledgeTopicResponse.topicId()));
                        if (partitionError.exception() != null) {
                            boolean retry = false;

                            if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                                // If the leader has changed, there's no point in retrying the operation because the acquisition locks
                                // will have been released.
                                TopicPartition tp = new TopicPartition(metadata.topicNames().get(shareAcknowledgeTopicResponse.topicId()), partitionData.partitionIndex());

                                log.debug("For {}, received error {}, with leaderIdAndEpoch {}", tp, partitionError, partitionData.currentLeader());
                                if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                                    partitionsWithUpdatedLeaderInfo.put(tp, new Metadata.LeaderIdAndEpoch(
                                        Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                                }
                            } else if (partitionError.exception() instanceof RetriableException) {
                                retry = true;
                            }

                            if (retry) {
                                // Move to incomplete acknowledgements to retry
                                acknowledgeRequestState.moveToIncompleteAcks(tip);
                                shouldRetry.set(true);
                            } else {
                                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, partitionError);
                            }
                        } else {
                            acknowledgeRequestState.handleAcknowledgeErrorCode(tip, partitionError);
                        }
                    }));

                    if (shouldRetry.get()) {
                        acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);
                    } else {
                        acknowledgeRequestState.onSuccessfulAttempt(responseCompletionTimeMs);
                        acknowledgeRequestState.processingComplete();
                    }
                }
            }

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
            }

            if (acknowledgeRequestState.isProcessed) {
                metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
            }
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());

            if (acknowledgeRequestState.onClose()) {
                log.debug("Removing node from ShareSession {}", fetchTarget.id());
                sessionHandlers.remove(fetchTarget.id());
            }
        }
    }

    private void handleShareAcknowledgeFailure(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               Throwable error,
                                               long responseCompletionTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge request from node {} unsuccessfully {}", fetchTarget.id(), Errors.forException(error));
            acknowledgeRequestState.sessionHandler().handleError(error);
            acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error));
            }));

            acknowledgeRequestState.processingComplete();
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());

            if (acknowledgeRequestState.onClose()) {
                log.debug("Removing node from ShareSession {}", fetchTarget.id());
                sessionHandlers.remove(fetchTarget.id());
            }
        }
    }

    private List<TopicPartition> partitionsToFetch() {
        return subscriptions.fetchablePartitions(tp -> true);
    }

    public ShareSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    boolean hasCompletedFetches() {
        return !shareFetchBuffer.isEmpty();
    }

    protected void closeInternal() {
        Utils.closeQuietly(shareFetchBuffer, "shareFetchBuffer");
    }

    public void close() {
        idempotentCloser.close(this::closeInternal);
    }

    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpochOpt, String memberId) {
        this.memberId = Uuid.fromString(memberId);
    }

    /**
     * Represents a request to acknowledge delivery that can be retried or aborted.
     */
    public class AcknowledgeRequestState extends TimedRequestState {

        /**
         * The share session handler.
         */
        private final ShareSessionHandler sessionHandler;

        /**
         * The node to send the request to.
         */
        private final int nodeId;

        /**
         * The map of acknowledgements to send
         */
        private final Map<TopicIdPartition, Acknowledgements> acknowledgementsToSend;

        /**
         * The map of acknowledgements to be retried in the next attempt.
         */
        private final Map<TopicIdPartition, Acknowledgements> incompleteAcknowledgements;

        /**
         * The in-flight acknowledgements
         */
        private final Map<TopicIdPartition, Acknowledgements> inFlightAcknowledgements;

        /**
         * This handles completing a future when all results are known.
         */
        private final ResultHandler resultHandler;

        /**
         * Indicates whether this was part of commitAsync, commitSync or close operation.
         */
        private final AcknowledgeRequestType requestType;

        /**
         * Boolean to indicate if the request has been processed.
         * <p>
         * Set to true once we process the response and do not retry the request.
         * <p>
         * Initialized to false every time we build a request.
         */
        private boolean isProcessed;

        AcknowledgeRequestState(LogContext logContext,
                                String owner,
                                long deadlineMs,
                                long retryBackoffMs,
                                long retryBackoffMaxMs,
                                ShareSessionHandler sessionHandler,
                                int nodeId,
                                Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                ResultHandler resultHandler,
                                AcknowledgeRequestType acknowledgeRequestType) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs, deadlineTimer(time, deadlineMs));
            this.sessionHandler = sessionHandler;
            this.nodeId = nodeId;
            this.acknowledgementsToSend = acknowledgementsMap;
            this.resultHandler = resultHandler;
            this.inFlightAcknowledgements = new HashMap<>();
            this.incompleteAcknowledgements = new HashMap<>();
            this.requestType = acknowledgeRequestType;
            this.isProcessed = false;
        }

        UnsentRequest buildRequest() {
            // If this is the closing request, close the share session by setting the final epoch
            if (onClose()) {
                sessionHandler.notifyClose();
            }

            Map<TopicIdPartition, Acknowledgements> finalAcknowledgementsToSend = new HashMap<>(
                    incompleteAcknowledgements.isEmpty() ? acknowledgementsToSend : incompleteAcknowledgements);

            for (Map.Entry<TopicIdPartition, Acknowledgements> entry : finalAcknowledgementsToSend.entrySet()) {
                sessionHandler.addPartitionToFetch(entry.getKey(), entry.getValue());
            }

            ShareAcknowledgeRequest.Builder requestBuilder = sessionHandler.newShareAcknowledgeBuilder(groupId, fetchConfig);

            isProcessed = false;
            Node nodeToSend = metadata.fetch().nodeById(nodeId);

            if (requestBuilder == null) {
                handleSessionErrorCode(Errors.SHARE_SESSION_NOT_FOUND);
                return null;
            } else if (nodeToSend != null) {
                nodesWithPendingRequests.add(nodeId);

                log.trace("Building acknowledgements to send : {}", finalAcknowledgementsToSend);

                inFlightAcknowledgements.putAll(finalAcknowledgementsToSend);
                if (incompleteAcknowledgements.isEmpty()) {
                    acknowledgementsToSend.clear();
                } else {
                    incompleteAcknowledgements.clear();
                }

                UnsentRequest unsentRequest = new UnsentRequest(requestBuilder, Optional.of(nodeToSend));
                BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                    if (error != null) {
                        handleShareAcknowledgeFailure(nodeToSend, requestBuilder.data(), this, error, unsentRequest.handler().completionTimeMs());
                    } else {
                        handleShareAcknowledgeSuccess(nodeToSend, requestBuilder.data(), this, clientResponse, unsentRequest.handler().completionTimeMs());
                    }
                };
                return unsentRequest.whenComplete(responseHandler);
            }

            return null;
        }

        int getInFlightAcknowledgementsCount(TopicIdPartition tip) {
            Acknowledgements acks = inFlightAcknowledgements.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        int getIncompleteAcknowledgementsCount(TopicIdPartition tip) {
            Acknowledgements acks = incompleteAcknowledgements.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        int getAcknowledgementsToSendCount(TopicIdPartition tip) {
            Acknowledgements acks = acknowledgementsToSend.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        boolean isEmpty() {
            return acknowledgementsToSend.isEmpty() &&
                    incompleteAcknowledgements.isEmpty() &&
                    inFlightAcknowledgements.isEmpty();
        }

        /**
         * Sets the error code in the acknowledgements and sends the response
         * through a background event.
         */
        void handleAcknowledgeErrorCode(TopicIdPartition tip, Errors acknowledgeErrorCode) {
            Acknowledgements acks = inFlightAcknowledgements.get(tip);
            if (acks != null) {
                acks.setAcknowledgeErrorCode(acknowledgeErrorCode);
            }
            resultHandler.complete(tip, acks, onCommitAsync());
        }

        /**
         * Sets the error code for the acknowledgements which were timed out
         * after some retries.
         */
        void handleAcknowledgeTimedOut(TopicIdPartition tip) {
            Acknowledgements acks = incompleteAcknowledgements.get(tip);
            if (acks != null) {
                acks.setAcknowledgeErrorCode(Errors.REQUEST_TIMED_OUT);
            }
            resultHandler.complete(tip, acks, onCommitAsync());
        }

        /**
         * Set the error code for all remaining acknowledgements in the event
         * of a session error which prevents the remains acknowledgements from
         * being sent.
         */
        void handleSessionErrorCode(Errors errorCode) {
            Map<TopicIdPartition, Acknowledgements> acknowledgementsMapToClear =
                    incompleteAcknowledgements.isEmpty() ? acknowledgementsToSend : incompleteAcknowledgements;

            acknowledgementsMapToClear.forEach((tip, acks) -> {
                if (acks != null) {
                    acks.setAcknowledgeErrorCode(errorCode);
                }
                resultHandler.complete(tip, acks, onCommitAsync());
            });
            acknowledgementsMapToClear.clear();
            processingComplete();
        }

        ShareSessionHandler sessionHandler() {
            return sessionHandler;
        }

        void processingComplete() {
            inFlightAcknowledgements.clear();
            resultHandler.completeIfEmpty();
            isProcessed = true;
        }

        /**
         * Moves all the in-flight acknowledgements to incomplete acknowledgements to retry
         * in the next request.
         */
        void moveAllToIncompleteAcks() {
            incompleteAcknowledgements.putAll(inFlightAcknowledgements);
            inFlightAcknowledgements.clear();
        }

        boolean maybeExpire() {
            return numAttempts > 0 && isExpired();
        }

        /**
         * Moves the in-flight acknowledgements for a given partition to incomplete acknowledgements to retry
         * in the next request.
         */
        public void moveToIncompleteAcks(TopicIdPartition tip) {
            Acknowledgements acks = inFlightAcknowledgements.remove(tip);
            if (acks != null) {
                incompleteAcknowledgements.put(tip, acks);
            }
        }

        public boolean onClose() {
            return requestType == AcknowledgeRequestType.CLOSE;
        }

        public boolean onCommitAsync() {
            return requestType == AcknowledgeRequestType.COMMIT_ASYNC;
        }
    }

    /**
     * Sends a ShareAcknowledgeCommitCallback event to the application when it is done
     * processing all the remaining acknowledgement request states.
     * Also manages completing the future for synchronous acknowledgement commit by counting
     * down the results as they are known and completing the future at the end.
     */
    class ResultHandler {
        private final Map<TopicIdPartition, Acknowledgements> result;
        private final AtomicInteger remainingResults;
        private final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future;

        ResultHandler(final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future) {
            this(null, future);
        }

        ResultHandler(final AtomicInteger remainingResults,
                      final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future) {
            result = new HashMap<>();
            this.remainingResults = remainingResults;
            this.future = future;
        }

        /**
         * Handle the result of a ShareAcknowledge request sent to one or more nodes and
         * signal the completion when all results are known.
         */
        public void complete(TopicIdPartition partition, Acknowledgements acknowledgements, boolean isCommitAsync) {
            if (!isCommitAsync && acknowledgements != null) {
                result.put(partition, acknowledgements);
            }
            // For commitAsync, we do not wait for other results to complete, we prepare a background event
            // for every ShareAcknowledgeResponse.
            // For commitAsync, we send out a background event for every TopicIdPartition, so we use a singletonMap each time.
            if (isCommitAsync) {
                if (acknowledgements != null) {
                    maybeSendShareAcknowledgeCommitCallbackEvent(Collections.singletonMap(partition, acknowledgements));
                }
            } else if (remainingResults != null && remainingResults.decrementAndGet() == 0) {
                maybeSendShareAcknowledgeCommitCallbackEvent(result);
                future.ifPresent(future -> future.complete(result));
            }
        }

        /**
         * Handles the case where there are no results pending after initialization.
         */
        public void completeIfEmpty() {
            if (remainingResults != null && remainingResults.get() == 0) {
                future.ifPresent(future -> future.complete(result));
            }
        }
    }

    static class Tuple<V> {
        private V asyncRequest;
        private Queue<V> syncRequestQueue;
        private V closeRequest;

        public Tuple(V asyncRequest, Queue<V> syncRequestQueue, V closeRequest) {
            this.asyncRequest = asyncRequest;
            this.syncRequestQueue = syncRequestQueue;
            this.closeRequest = closeRequest;
        }

        public void setAsyncRequest(V asyncRequest) {
            this.asyncRequest = asyncRequest;
        }

        public void nullifySyncRequestQueue() {
            this.syncRequestQueue = null;
        }

        public void addSyncRequest(V syncRequest) {
            if (syncRequestQueue == null) {
                syncRequestQueue = new LinkedList<>();
            }
            this.syncRequestQueue.add(syncRequest);
        }

        public void setCloseRequest(V closeRequest) {
            this.closeRequest = closeRequest;
        }

        public V getAsyncRequest() {
            return asyncRequest;
        }

        public Queue<V> getSyncRequestQueue() {
            return syncRequestQueue;
        }

        public V getCloseRequest() {
            return closeRequest;
        }
    }

    Tuple<AcknowledgeRequestState> requestStates(int nodeId) {
        return acknowledgeRequestStates.get(nodeId);
    }

    static class IdAndPartition {
        private final Uuid topicId;
        private final int partitionIndex;

        IdAndPartition(Uuid topicId, int partitionIndex) {
            this.topicId = topicId;
            this.partitionIndex = partitionIndex;
        }

        int getPartitionIndex() {
            return partitionIndex;
        }

        Uuid getTopicId() {
            return topicId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, partitionIndex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdAndPartition that = (IdAndPartition) o;
            return Objects.equals(topicId, that.topicId) &&
                    partitionIndex == that.partitionIndex;
        }
    }

    public enum AcknowledgeRequestType {
        COMMIT_ASYNC((byte) 0),
        COMMIT_SYNC((byte) 1),
        CLOSE((byte) 2);

        public final byte id;

        AcknowledgeRequestType(byte id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

    }
}
