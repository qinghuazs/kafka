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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code AbstractFetch} 代表了记录获取处理的基本状态和逻辑。
 * 该类封装了从 Kafka broker 获取数据的通用机制，包括会话管理、错误处理和数据缓冲。
 * 子类需要实现与特定网络客户端交互的细节。
 */
public abstract class AbstractFetch implements Closeable {

    /**
     * 用于记录日志的 Logger 实例。
     */
    private final Logger log;
    /**
     * 用于确保 close 方法只被执行一次的幂等关闭器。
     */
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    /**
     * 日志上下文，用于创建特定于组件的 logger。
     */
    protected final LogContext logContext;
    /**
     * 消费者元数据，维护集群和主题分区的信息。
     */
    protected final ConsumerMetadata metadata;
    /**
     * 消费者的订阅状态，跟踪消费者订阅的主题和分区。
     */
    protected final SubscriptionState subscriptions;
    /**
     * Fetch 请求的配置信息，例如获取消息的最大字节数、超时时间等。
     */
    protected final FetchConfig fetchConfig;
    /**
     * 时间工具类，用于获取当前时间等操作。
     */
    protected final Time time;
    /**
     * Fetch 指标管理器，用于收集和报告与 Fetch 请求相关的指标。
     */
    protected final FetchMetricsManager metricsManager;
    /**
     * Fetch 缓冲区，用于存储从 broker 获取到的数据。
     */
    protected final FetchBuffer fetchBuffer;
    /**
     * 解压缩缓冲区供应器，用于提供解压缩数据时所需的缓冲区。
     */
    protected final BufferSupplier decompressionBufferSupplier;
    /**
     * 存储有待处理 Fetch 请求的节点 ID 集合。
     */
    protected final Set<Integer> nodesWithPendingFetchRequests;

    /**
     * 存储每个节点 ID 对应的 Fetch 会话处理器。
     * Fetch 会话用于优化连续的 Fetch 请求，减少网络开销。
     */
    private final Map<Integer, FetchSessionHandler> sessionHandlers;

    /**
     * API 版本信息，用于确定与 broker 通信时使用的 API 版本。
     */
    private final ApiVersions apiVersions;

    /**
     * 构造一个 AbstractFetch 实例。
     *
     * @param logContext 日志上下文
     * @param metadata 消费者元数据
     * @param subscriptions 消费者订阅状态
     * @param fetchConfig Fetch 配置
     * @param fetchBuffer Fetch 缓冲区
     * @param metricsManager Fetch 指标管理器
     * @param time 时间工具
     * @param apiVersions API 版本信息
     */
    public AbstractFetch(final LogContext logContext,
                         final ConsumerMetadata metadata,
                         final SubscriptionState subscriptions,
                         final FetchConfig fetchConfig,
                         final FetchBuffer fetchBuffer,
                         final FetchMetricsManager metricsManager,
                         final Time time,
                         final ApiVersions apiVersions) {
        // 初始化 logger
        this.log = logContext.logger(AbstractFetch.class);
        // 初始化日志上下文
        this.logContext = logContext;
        // 初始化消费者元数据
        this.metadata = metadata;
        // 初始化订阅状态
        this.subscriptions = subscriptions;
        // 初始化 Fetch 配置
        this.fetchConfig = fetchConfig;
        // 初始化 Fetch 缓冲区
        this.fetchBuffer = fetchBuffer;
        // 创建解压缩缓冲区供应器
        this.decompressionBufferSupplier = BufferSupplier.create();
        // 初始化会话处理器映射
        this.sessionHandlers = new HashMap<>();
        // 初始化有待处理 Fetch 请求的节点集合
        this.nodesWithPendingFetchRequests = new HashSet<>();
        // 初始化 Fetch 指标管理器
        this.metricsManager = metricsManager;
        // 初始化时间工具
        this.time = time;
        // 初始化 API 版本信息
        this.apiVersions = apiVersions;
    }

    /**
     * 检查节点是否已断开连接并且无法立即重新连接（即，如果它在断开连接后的重新连接退避窗口中）。
     * 这个方法是抽象的，需要子类根据具体的网络客户端实现来判断节点的可用性。
     * 例如，可以检查与节点的连接状态以及是否处于退避期。
     *
     * @param node 要检查可用性的 {@link Node} 节点
     * @return 如果节点不可用则返回 true，否则返回 false
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    protected abstract boolean isUnavailable(Node node);

    /**
     * 检查给定节点上是否存在身份验证错误，如果存在则抛出异常。
     * 这个方法是抽象的，子类需要实现来检查特定节点的身份验证状态。
     * 如果在与该节点之前的交互中发生了身份验证失败，则应抛出 {@link AuthenticationException}。
     *
     * @param node 要检查先前 {@link AuthenticationException} 的 {@link Node} 节点；如果找到则抛出
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    protected abstract void maybeThrowAuthFailure(Node node);

    /**
     * 返回我们是否有任何已完成的获取请求等待返回给用户。此方法是线程安全的。具有测试可见性。
     * 这个方法用于检查 {@link FetchBuffer} 中是否有已经完成但尚未被用户消费的 Fetch 数据。
     * “已完成”意味着数据已经从 broker 成功获取并存入缓冲区。
     *
     * @return 如果有已完成的获取则返回 true，否则返回 false
     */
    boolean hasCompletedFetches() {
        // 检查 fetchBuffer 是否为空，不为空则表示有已完成的获取
        return !fetchBuffer.isEmpty();
    }

    /**
     * 返回我们是否有任何可获取的已完成的获取请求。此方法是线程安全的。
     * “可获取”不仅意味着 Fetch 数据已完成，还意味着对应的分区是当前消费者可以拉取数据的分区。
     * 例如，如果分区被暂停，即使有数据，也不认为是可获取的。
     *
     * @return 如果有可以返回的已完成获取则返回 true，否则返回 false
     */
    public boolean hasAvailableFetches() {
        // 检查 fetchBuffer 中是否有已完成的获取，并且这些获取对应的分区是可拉取的
        return fetchBuffer.hasCompletedFetches(fetch -> subscriptions.isFetchable(fetch.partition));
    }

    /**
     * 实现成功获取响应的核心逻辑。
     * 当从 broker 成功收到 Fetch 响应时，此方法被调用。
     * 它负责解析响应，处理会话信息，更新元数据，并将获取到的数据添加到 {@link FetchBuffer} 中。
     *
     * @param fetchTarget 发起 Fetch 请求的目标 {@link Node} 节点
     * @param data 代表会话数据的 {@link FetchSessionHandler.FetchRequestData} 对象
     * @param resp 将从中检索 {@link FetchResponse} 的 {@link ClientResponse} 客户端响应
     */
    protected void handleFetchSuccess(final Node fetchTarget,
                                      final FetchSessionHandler.FetchRequestData data,
                                      final ClientResponse resp) {
        try {
            // 从客户端响应中获取 FetchResponse 对象
            final FetchResponse response = (FetchResponse) resp.responseBody();
            // 根据目标节点 ID 获取对应的 FetchSessionHandler
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());

            // 如果找不到对应节点的会话处理器，则记录错误并忽略此响应
            // 这种情况可能发生在会话已关闭或节点信息已过时
            if (handler == null) {
                log.error("无法找到节点 {} 的 FetchSessionHandler。正在忽略获取响应。",
                        fetchTarget.id());
                return;
            }

            // 获取请求头中的 API 版本号
            final short requestVersion = resp.requestHeader().apiVersion();

            // 使用会话处理器处理响应，如果处理失败（例如会话已过期或出现错误）
            if (!handler.handleResponse(response, requestVersion)) {
                // 如果错误是 FETCH_SESSION_TOPIC_ID_ERROR，说明 topic ID 可能已更改，请求元数据更新
                if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
                    metadata.requestUpdate(false);
                }
                // 处理失败，直接返回，不进一步处理数据
                return;
            }

            // 获取响应中的分区数据，使用会话处理器提供的会话主题名称和请求版本进行解析
            final Map<TopicPartition, FetchResponseData.PartitionData> responseData = response.responseData(handler.sessionTopicNames(), requestVersion);
            // 从响应数据中提取所有涉及的分区
            final Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
            // 创建 Fetch 指标聚合器，用于收集这些分区的 Fetch 指标
            final FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metricsManager, partitions);

            // 用于存储需要更新 Leader 信息的 TopicPartition 及其新的 LeaderIdAndEpoch
            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
            // 遍历响应中的每个分区数据
            for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry : responseData.entrySet()) {
                // 获取当前处理的分区
                TopicPartition partition = entry.getKey();
                // 从原始请求数据中获取该分区的请求信息
                FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);

                // 如果在原始请求数据中找不到该分区的信息，这是一个异常情况
                if (requestData == null) {
                    String message;
                    // 根据会话元数据是否为 full 来构造不同的错误消息
                    // full 元数据表示这是一个不使用会话的 Fetch 请求，或者会话刚刚建立
                    if (data.metadata().isFull()) {
                        message = MessageFormatter.arrayFormat(
                                "完整请求分区丢失的响应：分区={}；元数据={}",
                                new Object[]{partition, data.metadata()}).getMessage();
                    } else {
                        // 非 full 元数据表示这是一个增量 Fetch 请求
                        message = MessageFormatter.arrayFormat(
                                "会话请求分区丢失的响应：分区={}；元数据={}；待发送={}；待忘记={}；待替换={}",
                                new Object[]{partition, data.metadata(), data.toSend(), data.toForget(), data.toReplace()}).getMessage();
                    }

                    // 收到丢失会话分区的获取响应，抛出非法状态异常
                    throw new IllegalStateException(message);
                }

                // 获取该分区的 Fetch Offset
                long fetchOffset = requestData.fetchOffset;
                // 获取该分区的响应数据
                FetchResponseData.PartitionData partitionData = entry.getValue();

                // 记录调试日志，包含隔离级别、fetch offset、分区和返回的分区数据
                log.debug("在偏移量 {} 获取分区 {} 的隔离级别 {} 返回了获取数据 {}",
                        fetchConfig.isolationLevel, fetchOffset, partition, partitionData);

                // 将分区数据的错误码转换为 Errors 枚举
                Errors partitionError = Errors.forCode(partitionData.errorCode());
                // 如果错误是 NOT_LEADER_OR_FOLLOWER 或 FENCED_LEADER_EPOCH，表示当前 broker 不是该分区的 leader 或 leader epoch 已过时
                if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                    // 记录调试日志，说明收到的错误以及当前的 leaderId 和 leaderEpoch
                    log.debug("对于分区 {}，收到错误 {}，leaderIdAndEpoch 为 {}", partition, partitionError, partitionData.currentLeader());
                    // 如果响应中包含了有效的 currentLeader 信息（leaderId 和 leaderEpoch 都不是 -1）
                    if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                        // 将该分区及其新的 leader 信息添加到待更新列表中
                        partitionsWithUpdatedLeaderInfo.put(partition, new Metadata.LeaderIdAndEpoch(
                            Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                    }
                }

                // 创建一个 CompletedFetch 对象，封装了获取到的数据和相关信息
                CompletedFetch completedFetch = new CompletedFetch(
                        logContext,                     // 日志上下文
                        subscriptions,                  // 订阅状态
                        decompressionBufferSupplier,    // 解压缩缓冲区供应器
                        partition,                      // 当前分区
                        partitionData,                  // 分区数据
                        metricAggregator,               // 指标聚合器
                        fetchOffset,                    // Fetch Offset
                        requestVersion);                // 请求版本号
                // 将 CompletedFetch 对象添加到 FetchBuffer 中，供后续处理
                fetchBuffer.add(completedFetch);
            }

            // 如果有需要更新 leader 信息的分区
            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                // 从响应数据中提取节点端点信息，并转换为 Node 对象列表
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack())) // 将端点信息映射为 Node 对象
                    .filter(e -> !e.equals(Node.noNode())) // 过滤掉无效的 Node.noNode()
                    .collect(Collectors.toList()); // 收集为列表
                // 更新元数据中的分区 leader 信息
                Set<TopicPartition> updatedPartitions = metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
                // 对于每个成功更新了 leader 的分区
                updatedPartitions.forEach(
                    tp -> {
                        // 记录调试日志，说明 leader 已更新，将验证位置信息
                        log.debug("对于分区 {}，由于 leader 已更新，将验证其位置。", tp);
                        // 验证当前 leader 的位置信息，这可能涉及到重置消费者的 offset
                        subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, tp, metadata.currentLeader(tp));
                    }
                );
            }

            // 记录 Fetch 请求的延迟指标
            metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
        } finally {
            // 无论成功还是失败，最终都要从待处理请求列表中移除当前请求
            // 这是为了确保不会因为异常导致请求永远停留在待处理状态
            removePendingFetchRequest(fetchTarget, data.metadata().sessionId());
        }
    }

    /**
     * 实现处理失败的 fetch 响应的核心逻辑。
     * 应用场景：当一个 fetch 请求失败时，此方法被调用以执行必要的清理和错误处理。
     * 实现细节：它会尝试获取与目标节点关联的 FetchSessionHandler，如果存在，则调用其 handleError 方法，
     * 并清除与该会话相关的 TopicPartition 的首选读取副本。无论成功与否，最终都会移除挂起的 fetch 请求。
     * 设计考虑：使用 try-finally 确保即使在处理过程中发生异常，挂起的请求也会被移除，防止资源泄漏。
     *
     * @param fetchTarget 从其请求 fetch 数据的 {@link org.apache.kafka.common.Node} 节点
     * @param data        来自请求的 {@link org.apache.kafka.clients.FetchSessionHandler.FetchRequestData}
     * @param t           代表导致失败的错误的 {@link java.lang.Throwable}
     */
    protected void handleFetchFailure(final Node fetchTarget,
                                      final FetchSessionHandler.FetchRequestData data,
                                      final Throwable t) {
        try { // 开始 try 块，用于异常处理和资源清理
            // 根据 fetchTarget 的 ID 获取对应的 FetchSessionHandler 实例
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler != null) { // 检查 FetchSessionHandler 是否成功获取
                // 调用 handler 的 handleError 方法来处理具体的 fetch 错误
                handler.handleError(t);
                // 对于此会话处理的所有主题分区，清除其首选读取副本的设置
                // 这是因为 fetch 失败可能意味着副本状态已更改，需要重新评估
                handler.sessionTopicPartitions().forEach(subscriptions::clearPreferredReadReplica);
            }
        } finally { // finally 块确保无论 try 块中是否发生异常，以下代码都会执行
            // 移除与此 fetchTarget 和会话 ID 关联的挂起 fetch 请求记录
            // 这是必要的清理步骤，以避免将此请求视为仍在进行中，并释放相关资源
            removePendingFetchRequest(fetchTarget, data.metadata().sessionId());
        }
    }

    /**
     * 处理成功关闭 Fetch 会话的响应。
     * 应用场景：当客户端成功向 broker 发送关闭 Fetch 会话的请求并收到确认后调用此方法。
     * 实现细节：从待处理的 Fetch 请求中移除该会话，并记录成功关闭的调试信息。
     * 设计考虑：确保在会话成功关闭后，及时清理客户端侧的会话状态，避免不必要的资源占用。
     *
     * @param fetchTarget 发送关闭会话请求的目标 {@link org.apache.kafka.common.Node} 节点
     * @param data        包含会话元数据的 {@link org.apache.kafka.clients.FetchSessionHandler.FetchRequestData}
     * @param ignored     来自 broker 的 {@link org.apache.kafka.clients.ClientResponse}，此处未使用
     */
    protected void handleCloseFetchSessionSuccess(final Node fetchTarget,
                                                  final FetchSessionHandler.FetchRequestData data,
                                                  final ClientResponse ignored) {
        // 从请求数据中获取会话ID
        int sessionId = data.metadata().sessionId();
        // 移除与此 fetchTarget 和会话 ID 关联的挂起 fetch 请求记录
        removePendingFetchRequest(fetchTarget, sessionId);
        // 记录调试信息，表明已成功发送关闭 fetch 会话的消息
        log.debug("Successfully sent a close message for fetch session: {} to node: {}", sessionId, fetchTarget);
    }

    /**
     * 处理关闭 Fetch 会话失败的情况。
     * 应用场景：当客户端尝试向 broker 发送关闭 Fetch 会话的请求失败时调用此方法。
     * 实现细节：尽管发送关闭请求失败，仍然从待处理的 Fetch 请求中移除该会话，并记录失败的调试信息及潜在影响。
     * 设计考虑：即使关闭请求失败，客户端也应假定会话可能未在 broker 端正确关闭，但仍需清理本地状态以继续操作。
     *           记录日志有助于诊断问题，并提示用户 broker 端可能存在不必要的会话。
     *
     * @param fetchTarget 发送关闭会话请求的目标 {@link org.apache.kafka.common.Node} 节点
     * @param data        包含会话元数据的 {@link org.apache.kafka.clients.FetchSessionHandler.FetchRequestData}
     * @param t           导致关闭请求失败的 {@link java.lang.Throwable}
     */
    public void handleCloseFetchSessionFailure(final Node fetchTarget,
                                               final FetchSessionHandler.FetchRequestData data,
                                               final Throwable t) {
        // 从请求数据中获取会话ID
        int sessionId = data.metadata().sessionId();
        // 即使关闭消息发送失败，也移除与此 fetchTarget 和会话 ID 关联的挂起 fetch 请求记录
        // 这是因为客户端无法确定 broker 的状态，继续跟踪此请求没有意义
        removePendingFetchRequest(fetchTarget, sessionId);
        // 记录调试信息，表明无法发送关闭 fetch 会话的消息，并指出这可能导致 broker 上出现不必要的 fetch 会话
        log.debug("Unable to send a close message for fetch session: {} to node: {}. " +
                "This may result in unnecessary fetch sessions at the broker.", sessionId, fetchTarget, t);
    }

    /**
     * 移除指定节点和会话ID的待处理 Fetch 请求。
     * 应用场景：在 Fetch 请求完成（成功或失败）或关闭 Fetch 会话后，调用此方法清理内部状态。
     * 实现细节：从 `nodesWithPendingFetchRequests` 集合中移除目标节点的 ID。
     * 设计考虑：此方法是内部辅助方法，用于统一管理待处理请求的状态，确保 `nodesWithPendingFetchRequests` 准确反映当前情况。
     *
     * @param fetchTarget 目标 {@link org.apache.kafka.common.Node} 节点
     * @param sessionId   要移除的 Fetch 会话的 ID
     */
    private void removePendingFetchRequest(Node fetchTarget, int sessionId) {
        // 记录调试信息，表明正在移除指定节点和会话ID的挂起请求
        log.debug("Removing pending request for fetch session: {} for node: {}", sessionId, fetchTarget);
        // 从记录有待处理 Fetch 请求的节点 ID 集合中，移除该 fetchTarget 节点的 ID
        // 这表示对于该节点，不再有与此特定会话相关的挂起请求（或不再有任何挂起请求，取决于具体实现如何使用此集合）
        nodesWithPendingFetchRequests.remove(fetchTarget.id());
    }

    /**
     * 创建一个新的 {@link org.apache.kafka.common.requests.FetchRequest FetchRequest}，准备发送到 Kafka 集群。
     * 应用场景：当消费者需要从 broker 获取数据时，此方法被调用来构建实际的 Fetch 请求。
     * 实现细节：根据会话数据和 Fetch 配置构建 FetchRequest.Builder。\它会确定合适的 API 版本，
     * 设置消费者的相关参数（如最大等待时间、最小字节数、隔离级别、最大字节数等），并包含会话元数据、
     * 要移除或替换的主题分区信息，以及客户端的 rack ID。
     * 设计考虑：此方法封装了 FetchRequest 的构建逻辑，使其更易于管理和修改。
     *           在将节点添加到待处理请求集合之前记录日志，有助于跟踪请求的生命周期。
     *           版本选择逻辑确保了与不同版本 broker 的兼容性。
     *
     * @param fetchTarget 将从其请求 fetch 数据的 {@link org.apache.kafka.common.Node} 节点
     * @param requestData 代表会话数据的 {@link org.apache.kafka.clients.FetchSessionHandler.FetchRequestData}
     * @return 可提交给 broker 的 {@link org.apache.kafka.common.requests.FetchRequest.Builder}
     */
    protected FetchRequest.Builder createFetchRequest(final Node fetchTarget,
                                                      final FetchSessionHandler.FetchRequestData requestData) {
        // 版本12是不使用主题ID所能使用的最大版本。有关模式更改日志，请参阅FetchRequest.json。
        // 根据请求数据是否可以使用主题ID来确定最大的Fetch API版本
        // 如果可以使用主题ID，则使用最新的Fetch API版本；否则，使用版本12（这是不使用主题ID的最高版本）
        final short maxVersion = requestData.canUseTopicIds() ? ApiKeys.FETCH.latestVersion() : (short) 12;

        // 使用 FetchRequest.Builder 构建 Fetch 请求
        final FetchRequest.Builder request = FetchRequest.Builder
                // 为消费者构建请求，指定API版本、最大等待时间、最小字节数以及要发送的分区数据
                .forConsumer(maxVersion, fetchConfig.maxWaitMs, fetchConfig.minBytes, requestData.toSend())
                // 设置隔离级别 (例如，read_committed 或 read_uncommitted)
                .isolationLevel(fetchConfig.isolationLevel)
                // 设置Fetch请求响应的最大字节数
                .setMaxBytes(fetchConfig.maxBytes)
                // 设置Fetch会话的元数据 (例如，会话ID和纪元)
                .metadata(requestData.metadata())
                // 设置在此Fetch请求中要从会话中移除的主题分区
                .removed(requestData.toForget())
                // 设置在此Fetch请求中要替换其元数据的主题分区
                .replaced(requestData.toReplace())
                // 设置客户端的 rack ID，用于 broker 的副本选择优化
                .rackId(fetchConfig.clientRackId);

        // 记录调试信息，表明将要发送的 Fetch 请求的隔离级别、请求数据和目标 broker
        log.debug("Sending {} {} to broker {}", fetchConfig.isolationLevel, requestData, fetchTarget);

        // 我们在添加监听器之前将节点添加到具有挂起获取请求的节点集中
        // 因为 future 可能已经在另一个线程上被满足（例如，在心跳线程处理断开连接期间）
        // 这将意味着监听器将被同步调用。
        // 记录调试信息，表明正在为目标节点添加一个挂起的请求
        log.debug("Adding pending request for node {}", fetchTarget);
        // 将目标节点的 ID 添加到 `nodesWithPendingFetchRequests` 集合中，标记该节点有待处理的 Fetch 请求
        nodesWithPendingFetchRequests.add(fetchTarget.id());

        // 返回构建好的 FetchRequest.Builder 实例
        return request;
    }

    /**
     * 返回<em>可获取</em>分区列表，这些分区是我们已订阅的分区集合，
     * 但<em>排除</em>了我们仍有缓冲数据的任何分区。其思想是，由于用户
     * 尚未处理已获取的分区数据，我们不应在先前获取的数据被处理之前
     * 发送更多数据请求。
     * 应用场景：在准备构建 Fetch 请求之前，调用此方法来确定哪些分区当前适合获取数据。
     * 实现细节：首先获取当前 FetchBuffer 中已缓冲数据的分区集合。然后定义一个谓词，用于判断分区是否未被缓冲。
     *           最后，调用 `subscriptions.fetchablePartitions()` 方法，传入此谓词，以获取所有处于可获取状态且没有缓冲数据的分区。
     * 设计考虑：这种机制避免了对同一分区重复获取数据，如果之前获取的数据尚未被消费，可以减少不必要的网络流量和处理开销，
     *           同时也有助于控制内存使用，防止缓冲区无限增长。
     *
     * @return 我们应该为其获取数据的 {@link java.util.Set} 集合，元素为 {@link org.apache.kafka.common.TopicPartition topic partitions}
     */
    private Set<TopicPartition> fetchablePartitions() {
        // 这是我们缓冲区中已存在数据的分区集合
        // 获取当前 fetchBuffer 中所有已缓冲数据的分区
        Set<TopicPartition> buffered = fetchBuffer.bufferedPartitions();

        // 这是一个测试，如果分区*未*被缓冲，则返回 true
        // 定义一个 Predicate (断言函数)，用于检查给定的 TopicPartition 是否不在 buffered 集合中
        Predicate<TopicPartition> isNotBuffered = tp -> !buffered.contains(tp);

        // 返回所有处于可获取状态*并且*我们缓冲区中尚无消息的分区。
        // 调用 `subscriptions.fetchablePartitions` 方法，传入 `isNotBuffered` 断言
        // 该方法会返回所有已订阅、可获取，并且满足 `isNotBuffered` 条件 (即没有缓冲数据) 的分区
        // 使用 HashSet 包装结果以确保返回的是一个 Set 集合
        return new HashSet<>(subscriptions.fetchablePartitions(isNotBuffered));
    }

    /**
     * 决定从哪个副本读取数据：<i>首选副本</i>或<i>领导者副本</i>。仅当满足以下所有条件时，才使用首选副本：
     *
     * <ul>
     *     <li>之前已设置首选副本</li>
     *     <li>我们仍处于首选副本的租约时间内</li>
     *     <li>该副本仍处于在线/可用状态</li>
     * </ul>
     *
     * 如果上述任何条件未满足，则返回领导者节点。
     * 应用场景：在为特定分区构建 Fetch 请求时，此方法用于确定最佳的读取源节点，以支持从就近副本读取（Follower Fetching）的功能。
     * 实现细节：首先尝试从 `subscriptions` 获取当前时间下该分区的首选读取副本ID。如果存在，则进一步检查该节点是否在线且存在于元数据中。
     *           如果首选副本有效，则返回该节点。否则，如果首选副本不存在、不在线或元数据陈旧，则记录追踪信息，
     *           清除该分区的首选副本设置，请求元数据更新，并返回领导者副本作为读取目标。
     * 设计考虑：此逻辑旨在平衡从就近副本读取带来的低延迟优势和数据一致性（通过租约机制）。
     *           当首选副本不可用时，回退到领导者副本确保了可用性。请求元数据更新有助于纠正潜在的陈旧信息。
     *
     * @param partition 我们要为其获取数据的 {@link org.apache.kafka.common.TopicPartition}
     * @param leaderReplica 给定分区的领导者副本的 {@link org.apache.kafka.common.Node}
     * @param currentTimeMs 当前时间（毫秒）；用于确定我们是否在可选的租约窗口内
     * @return 从其请求数据的副本 {@link org.apache.kafka.common.Node node}
     * @see SubscriptionState#preferredReadReplica(TopicPartition, long)
     * @see SubscriptionState#updatePreferredReadReplica(TopicPartition, int, long)
     */
    Node selectReadReplica(final TopicPartition partition, final Node leaderReplica, final long currentTimeMs) {
        // 尝试从订阅状态中获取指定分区在当前时间下的首选读取副本的节点ID
        Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);

        if (nodeId.isPresent()) { // 如果存在首选读取副本的节点ID
            // 尝试根据节点ID从元数据中获取在线的节点信息
            // flatMap 用于安全地处理 Optional<Integer> 到 Optional<Node> 的转换
            Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
            if (node.isPresent()) { // 如果成功获取到在线的首选节点
                // 返回该首选节点作为读取副本
                return node.get();
            } else { // 如果首选节点不在线或在元数据中找不到
                // 记录追踪日志，说明由于首选副本离线或元数据缺失，将改用领导者副本
                log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata," +
                        " using the leader instead.", nodeId, partition);
                // 注意：这种情况可能是由于元数据陈旧导致的，因此我们清除首选副本并刷新元数据。
                // 清除该分区的首选读取副本设置
                // 请求元数据更新，以获取最新的集群状态，这可能包括副本的在线状态或领导者变更
                requestMetadataUpdate(metadata, subscriptions, partition);
                // 返回领导者副本作为读取目标
                return leaderReplica;
            }
        } else { // 如果没有设置首选读取副本，或者租约已过期
            // 直接返回领导者副本作为读取目标
            return leaderReplica;
        }
    }

    /**
     * 准备关闭 Fetch 会话的请求。
     * <p>
     * 应用场景：当消费者准备关闭或不再需要某个 Fetch 会话时，会调用此方法来构建关闭会话的请求。
     * 实现细节：
     * 1. 获取最新的集群元数据。
     * 2. 遍历当前所有活动的会话处理器 (sessionHandlers)。
     * 3. 对每个会话处理器，调用 {@link FetchSessionHandler#notifyClose()} 方法，标记该会话将在下一次元数据请求中发送关闭消息。
     * 4. 检查会话的目标节点 (FetchTargetNode) 是否仍然可达。如果节点不可达（例如已断开连接），则跳过向该节点发送关闭请求。
     * 5. 如果节点可达，则将该节点和对应的会话处理器的构建器 (builder) 存入 fetchable 映射中。
     * 6. 最后，将 fetchable 映射转换为 {@code Map<Node, FetchSessionHandler.FetchRequestData>}，其中键是目标节点，值是构建好的关闭会话请求数据。
     * 设计考虑：
     * - 通过 {@link FetchSessionHandler#notifyClose()} 异步通知关闭，而不是立即发送，这样可以将关闭操作与其他元数据操作合并，减少网络请求。
     * - 增加了对目标节点可达性的检查，避免向不可达节点发送请求，从而减少不必要的错误和等待。
     *
     * @return 一个映射，键是目标节点 {@link Node}，值是对应的 {@link FetchSessionHandler.FetchRequestData}，包含了关闭会话所需的信息。
     */
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareCloseFetchSessionRequests() {
        // 获取最新的集群元数据
        final Cluster cluster = metadata.fetch();
        // 创建一个映射，用于存储可发送关闭请求的节点及其对应的会话处理器构建器
        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();

        // 遍历所有的会话处理器
        sessionHandlers.forEach((fetchTargetNodeId, sessionHandler) -> {
            // 设置会话处理器以通知关闭。这将设置下一个元数据请求以发送关闭消息。
            sessionHandler.notifyClose();

            // FetchTargetNode 可能不可用，因为它可能已经断开了连接。在这种情况下，我们将跳过发送关闭请求。
            // 根据节点ID从集群元数据中获取目标节点信息
            final Node fetchTarget = cluster.nodeById(fetchTargetNodeId);

            // 如果目标节点为null（即在集群中找不到）或者节点不可用
            if (fetchTarget == null || isUnavailable(fetchTarget)) {
                // 记录调试信息，跳过向该broker发送关闭会话请求，因为它不可达
                log.debug("Skip sending close session request to broker {} since it is not reachable", fetchTarget);
                // 返回，处理下一个会话
                return;
            }

            // 将可达的目标节点及其会话处理器的构建器放入fetchable映射中
            fetchable.put(fetchTarget, sessionHandler.newBuilder());
        });

        // 将fetchable映射转换为最终的请求数据映射并返回
        // 使用流式操作将Map<Node, FetchSessionHandler.Builder> 转换为 Map<Node, FetchSessionHandler.FetchRequestData>
        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * 为所有已分配且当前没有正在处理的 Fetch 请求的分区创建 Fetch 请求。
     * <p>
     * 应用场景：消费者客户端定期调用此方法来构建新的 Fetch 请求，以从 broker 获取消息数据。
     * 实现细节：
     * 1. 如果分区分配发生变化，则更新相关的度量指标。
     * 2. 初始化一个空的 `fetchable` 映射，用于存储目标节点和对应的 Fetch 会话构建器。
     * 3. 获取当前时间戳和主题ID映射。
     * 4. 遍历所有可 Fetch 的分区 (通过 {@link #fetchablePartitions()} 获取)。
     *    a. 获取分区的当前 Fetch 位置 (offset, leader epoch 等)。如果位置信息缺失，则抛出 {@link IllegalStateException}。
     *    b. 获取分区的当前 leader 节点。如果 leader 信息缺失，则请求元数据更新并跳过此分区。
     *    c. 根据配置选择读取副本（优先副本或 leader）。
     *    d. 检查选定的节点是否可用：
     *        i. 如果节点不可用（例如处于重连退避期），则记录日志并跳过此分区。可能会抛出认证失败异常。
     *        ii. 如果该节点已有正在处理的 Fetch 请求 (nodesWithPendingFetchRequests 包含该节点ID)，则记录日志并跳过此分区，避免发送重复请求。
     *    e. 如果节点可用且没有正在处理的请求：
     *        i. 获取或创建一个针对该节点的 {@link FetchSessionHandler.Builder}。
     *        ii. 获取分区对应的主题ID。
     *        iii. 构建 {@link FetchRequest.PartitionData}，包含主题ID、Fetch偏移量、最大获取字节数、leader epoch 等信息。
     *        iv. 将分区数据添加到会话构建器中。
     *        v. 记录调试信息，表明已为该分区添加了 Fetch 请求。
     * 5. 将 `fetchable` 映射转换为 {@code Map<Node, FetchSessionHandler.FetchRequestData>} 并返回。
     * 设计考虑：
     * - 批量构建请求：一次性为多个分区和节点准备 Fetch 请求，提高效率。
     * - 会话管理：利用 {@link FetchSessionHandler} 来管理 Fetch 会话，优化连续的 Fetch 操作。
     * - 节点状态检查：在发送请求前检查节点可用性和是否有正在处理的请求，避免不必要的网络开销和错误。
     * - 元数据依赖：依赖 {@link ConsumerMetadata} 和 {@link SubscriptionState} 提供最新的集群和订阅信息。
     * - 副本选择：支持从优先读取副本获取数据，以分担 leader 节点的负载。
     *
     * @return 一个映射，键是目标节点 {@link Node}，值是对应的 {@link FetchSessionHandler.FetchRequestData}，包含了要获取数据的分区信息。
     */
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        // 如果分区分配发生变化，则更新度量指标
        metricsManager.maybeUpdateAssignment(subscriptions);

        // 创建一个映射，用于存储可发送Fetch请求的节点及其对应的会话处理器构建器
        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();
        // 获取当前时间（毫秒）
        long currentTimeMs = time.milliseconds();
        // 获取主题名称到主题ID的映射
        Map<String, Uuid> topicIds = metadata.topicIds();

        // 遍历所有可获取数据的分区
        for (TopicPartition partition : fetchablePartitions()) {
            // 获取该分区的当前消费位置信息
            SubscriptionState.FetchPosition position = subscriptions.position(partition);

            // 如果找不到该分区的消费位置信息，则抛出非法状态异常
            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + partition);

            // 获取当前分区的leader节点信息
            Optional<Node> leaderOpt = position.currentLeader.leader;

            // 如果leader节点信息为空
            if (leaderOpt.isEmpty()) {
                // 记录调试日志，请求元数据更新，因为该分区的位置信息缺少当前leader节点
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                // 请求更新元数据（非阻塞）
                metadata.requestUpdate(false);
                // 继续处理下一个分区
                continue;
            }

            // 如果设置了首选读取副本，则使用首选读取副本，否则使用分区的leader节点
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);

            // 如果选择的节点不可用
            if (isUnavailable(node)) {
                // 检查是否由于认证失败导致节点不可用，如果是则抛出异常
                maybeThrowAuthFailure(node);

                // 如果我们尝试在重新连接退避窗口期间发送，那么请求无论如何都会在发送前失败，
                // 所以现在跳过发送请求
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            // 如果该节点已经有正在处理的Fetch请求
            } else if (nodesWithPendingFetchRequests.contains(node.id())) {
                // 记录跟踪日志，跳过该分区的Fetch请求，因为之前发送到该节点的请求尚未处理完毕
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
            } else {
                // 如果存在leader节点并且没有正在处理的请求，则发出新的Fetch请求
                // 获取或创建该节点的Fetch会话处理器的构建器
                FetchSessionHandler.Builder builder = fetchable.computeIfAbsent(node, k -> {
                    // 如果sessionHandlers中不存在该节点的会话处理器，则创建一个新的
                    FetchSessionHandler fetchSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new FetchSessionHandler(logContext, n));
                    // 返回该会话处理器的构建器
                    return fetchSessionHandler.newBuilder();
                });
                // 获取分区对应的主题ID，如果不存在则使用ZERO_UUID
                Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
                // 创建Fetch请求的分区数据
                FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(topicId,
                        position.offset, // 当前消费偏移量
                        FetchRequest.INVALID_LOG_START_OFFSET, // 无效的日志起始偏移量（通常不在此处设置）
                        fetchConfig.fetchSize, // 本次Fetch请求的最大字节数
                        position.currentLeader.epoch, // 当前leader的epoch
                        Optional.empty()); // 可选的 last fetched epoch
                // 将分区数据添加到构建器中
                builder.add(partition, partitionData);

                // 记录调试日志，说明已为指定分区、位置和节点添加了Fetch请求，并指明了隔离级别
                log.debug("Added {} fetch request for partition {} at position {} to node {}", fetchConfig.isolationLevel,
                        partition, position, node);
            }
        }

        // 将fetchable映射转换为最终的请求数据映射并返回
        // 使用流式操作将Map<Node, FetchSessionHandler.Builder> 转换为 Map<Node, FetchSessionHandler.FetchRequestData>
        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * 获取指定节点的 Fetch 会话处理器。
     * <p>
     * 应用场景：主要用于测试，允许外部代码访问和验证特定节点的会话处理器状态。
     * 实现细节：直接从 {@code sessionHandlers} 映射中根据节点ID获取对应的 {@link FetchSessionHandler}。
     * 设计考虑：此方法主要为了可测试性而暴露，常规业务逻辑不应直接依赖此方法。
     *
     * @param node 节点ID
     * @return 与指定节点关联的 {@link FetchSessionHandler}，如果不存在则返回 null。
     */
    // 仅用于测试
    protected FetchSessionHandler sessionHandler(int node) {
        // 从sessionHandlers映射中获取指定nodeId的会话处理器
        return sessionHandlers.get(node);
    }

    /**
     * 内部关闭逻辑，由 {@link #close(Timer)} 方法调用，并由 {@link IdempotentCloser} 保护，
     * 以确保在任何 {@link #close()} 方法第一次被调用时仅执行一次。
     * 子类可以覆盖此方法，而无需在实例级别进行额外的同步。
     * <p>
     * 应用场景：当 {@link AbstractFetch} 实例需要被关闭和清理资源时，此方法负责执行核心的清理操作。
     * 实现细节：
     * 1. 安静地关闭 {@code fetchBuffer}，释放其占用的资源。命名为 "fetchBuffer" 以便日志记录。
     * 2. 安静地关闭 {@code decompressionBufferSupplier}，释放其占用的资源。命名为 "decompressionBufferSupplier" 以便日志记录。
     * 设计考虑：
     * - 使用 {@link Utils#closeQuietly(Closeable, String)} 来关闭资源，这样即使关闭过程中发生异常，也不会影响后续资源的关闭，并且会记录错误。
     * - 由于此方法由幂等关闭器保护，因此不需要在此方法内部进行额外的同步控制来防止重复关闭。
     * - 注释中提到“我们不需要重新启用唤醒，因为我们已经在关闭了”，这暗示了在正常操作中可能存在某种唤醒机制，但在关闭过程中不再需要。
     *
     * @param timer 用于强制执行时间限制的计时器 (在此实现中未使用，但子类可能会使用)
     */
    // 仅用于测试
    protected void closeInternal(Timer timer) {
        // 我们不需要重新启用唤醒，因为我们已经在关闭了
        // 安静地关闭 fetchBuffer，"fetchBuffer" 是关闭操作的名称，用于日志记录
        Utils.closeQuietly(fetchBuffer, "fetchBuffer");
        // 安静地关闭 decompressionBufferSupplier，"decompressionBufferSupplier" 是关闭操作的名称，用于日志记录
        Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
    }

    /**
     * 关闭此 {@link AbstractFetch} 实例并释放其资源。
     * 此方法使用 {@link IdempotentCloser} 来确保内部关闭逻辑 {@link #closeInternal(Timer)} 只执行一次。
     * <p>
     * 应用场景：当消费者客户端关闭或不再需要此 Fetch 实例时调用，以清理所有相关资源，如缓冲区和会话处理器。
     * 实现细节：调用 {@link IdempotentCloser#close(Runnable)} 方法，传入一个执行 {@link #closeInternal(Timer)} 的 lambda 表达式。
     * 设计考虑：通过幂等关闭器确保资源只被清理一次，避免了重复关闭可能导致的问题。
     *
     * @param timer 用于强制执行关闭操作时间限制的计时器。
     */
    public void close(final Timer timer) {
        // 使用幂等关闭器执行 closeInternal 方法，确保只执行一次
        idempotentCloser.close(() -> closeInternal(timer));
    }

    /**
     * 关闭此 {@link AbstractFetch} 实例并释放其资源。
     * 这是 {@link Closeable#close()} 接口的实现。
     * 它使用一个零持续时间的计时器来调用重载的 {@link #close(Timer)} 方法。
     * <p>
     * 应用场景：作为标准的 {@link Closeable} 实现，允许在 try-with-resources 语句中使用此对象，或在需要标准关闭语义的任何地方调用。
     * 实现细节：创建一个零持续时间的 {@link Timer} 实例，并调用 {@link #close(Timer)}。
     * 设计考虑：提供一个无参数的 close 方法，符合 {@link Closeable} 接口规范，并委托给带计时器的版本进行实际的关闭操作。
     */
    @Override
    public void close() {
        // 调用带有计时器的 close 方法，计时器设置为零持续时间
        close(time.timer(Duration.ZERO));
    }

    /**
     * 定义了处理来自 broker 的 Fetch 响应的契约。
     * 这是一个函数式接口，用于封装处理 Fetch 响应的逻辑。
     * <p>
     * 应用场景：当从 broker 收到 Fetch 响应（成功或失败）时，会使用此接口的实现来处理该响应。
     * 例如，可以将成功获取的数据放入缓冲区，或处理错误情况（如请求元数据更新、记录错误等）。
     * 设计考虑：
     * - 使用泛型 {@code <T>} 使得处理器可以灵活处理不同类型的响应对象，通常是 {@link ClientResponse} (代表成功的网络响应) 或 {@link Throwable} (代表发生的异常)。
     * - 作为函数式接口，可以方便地使用 lambda 表达式或方法引用来创建实例，简化代码。
     *
     * @param <T> 响应的类型，通常是 {@link ClientResponse} 或 {@link Throwable}
     */
    @FunctionalInterface
    protected interface ResponseHandler<T> {

        /**
         * 处理来自给定目标节点 {@link Node} 的响应。
         *
         * @param target 响应来源的目标节点。
         * @param data 与此响应关联的原始 {@link FetchSessionHandler.FetchRequestData} Fetch 请求数据。
         * @param response 从 broker 收到的响应对象，类型为 {@code T}。
         */
        void handle(Node target, FetchSessionHandler.FetchRequestData data, T response);
    }
}