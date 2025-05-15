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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.maybeWrapAsKafkaException;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.regroupFetchPositionsByLeader;

/**
 * 负责构建以下请求以检索分区偏移量，并处理其响应的管理器。
 * <ul>
 *      <li>ListOffset 请求</li>
 *      <li>OffsetForLeaderEpoch 请求</li>
 * </ul>
 * 请求保存在内存中，准备在下一次调用 {@link #poll(long)} 时发送。
 * <br>
 * 构建 ListOffset 请求所需的分区领导者信息是从 {@link ConsumerMetadata} 中检索的，
 * 因此该类实现了 {@link ClusterResourceListener} 以便在集群元数据更新时得到通知。
 * 应用场景: Kafka 消费者需要获取分区的偏移量信息，例如在启动时确定从哪里开始消费，或者在查找特定时间戳对应的偏移量时。
 * 设计考虑: 将偏移量相关的请求逻辑封装在此管理器中，便于统一处理和维护。通过实现 ClusterResourceListener，可以响应元数据变化，确保请求发送到正确的分区领导者。
 */
public final class OffsetsRequestManager implements RequestManager, ClusterResourceListener {

    // 消费者元数据，用于获取集群和分区领导者信息
    private final ConsumerMetadata metadata;
    // 隔离级别，用于读取已提交的或未提交的消息
    private final IsolationLevel isolationLevel;
    // 日志记录器
    private final Logger log;
    // OffsetFetcher 工具类，提供获取偏移量的辅助方法
    private final OffsetFetcherUtils offsetFetcherUtils;
    // 订阅状态，维护消费者订阅的主题和分区信息
    private final SubscriptionState subscriptionState;

    // 需要重试的 ListOffsets 请求状态集合
    private final Set<ListOffsetsRequestState> requestsToRetry;
    // 准备发送的请求列表
    private final List<NetworkClientDelegate.UnsentRequest> requestsToSend;
    // 请求超时时间（毫秒）
    private final int requestTimeoutMs;
    // 时间工具类，用于获取当前时间等
    private final Time time;
    // API 版本信息，用于确定与 broker 通信时使用的 API 版本
    private final ApiVersions apiVersions;
    // 网络客户端代理，用于发送请求和接收响应
    private final NetworkClientDelegate networkClientDelegate;
    // 提交请求管理器，用于处理偏移量提交相关的逻辑
    private final CommitRequestManager commitRequestManager;
    // 默认 API 超时时间（毫秒）
    private final long defaultApiTimeoutMs;

    /**
     * 在触发事件已过期后更新位置时发生的异常。
     * 它将在下一次调用更新获取位置时传播并清除。
     * 应用场景: 当尝试更新消费位置时，如果相关的异步操作（如获取已提交偏移量）因超时或其他原因失败，并且此时更新位置的原始触发条件（如元数据更新）已经不再有效，则会缓存此异常。
     * 设计考虑: 使用 AtomicReference 确保线程安全地缓存异常，并在后续的更新位置操作中重新抛出，以便调用者能够意识到之前的错误。
     */
    private final AtomicReference<Throwable> cachedUpdatePositionsException = new AtomicReference<>();

    /**
     * 此字段保存了最后一次触发的 OffsetFetch 请求，该请求用于检索已提交的偏移量以更新获取位置，但尚未完成。
     * 当收到响应时，它将用于更新获取位置，并且 pendingOffsetFetchEvent 将被清除。
     * 如果更新获取位置的尝试在 OffsetFetch 获得响应之前超时，则该请求将被保留，以便在下一次尝试更新获取位置时使用（如果分区保持不变）。
     * 应用场景: 在需要根据已提交偏移量更新消费位置时，如果获取已提交偏移量的请求是异步的，此字段用于跟踪这个挂起的请求。
     * 设计考虑: 避免在短时间内重复发送相同的 OffsetFetch 请求。如果一个请求正在进行中，后续的更新操作可以等待或复用这个请求的结果。
     */
    private PendingFetchCommittedRequest pendingOffsetFetchEvent;

    /**
     * OffsetsRequestManager 的构造函数。
     *
     * @param subscriptionState 消费者的订阅状态，维护了消费者订阅的主题、分区以及它们的消费位置和元数据。
     * @param metadata 消费者的元数据，包含了集群的拓扑信息，例如 broker 列表、主题分区信息以及领导者信息。
     * @param isolationLevel 事务隔离级别，定义了消费者可以读取的消息范围（例如，只能读取已提交的事务消息）。
     * @param time 时间工具，用于获取当前时间戳，常用于超时控制和时间相关的计算。
     * @param retryBackoffMs 请求失败后的重试退避时间（毫秒），指定了在重试前等待的时间间隔。
     * @param requestTimeoutMs 单个请求的超时时间（毫秒），如果请求在此时间内未收到响应，则认为超时。
     * @param defaultApiTimeoutMs 默认的 API 调用超时时间（毫秒），用于某些没有显式指定超时时间的 API 调用。
     * @param apiVersions Kafka 客户端和服务端之间的 API 版本信息，用于协商兼容的通信协议。
     * @param networkClientDelegate 网络客户端代理，封装了与 Kafka broker 进行网络通信的底层细节。
     * @param commitRequestManager 偏移量提交请求管理器，负责处理消费者提交偏移量的逻辑。
     * @param logContext 日志上下文，用于创建和管理日志记录器。
     * 应用场景: 在创建 KafkaConsumer 实例时，会间接创建 OffsetsRequestManager 实例，用于管理与获取分区偏移量相关的请求。
     * 设计考虑: 通过构造函数注入所有必要的依赖项，使得 OffsetsRequestManager 能够独立完成其职责。参数的非空检查确保了核心组件的可用性。
     */
    public OffsetsRequestManager(final SubscriptionState subscriptionState,
                                 final ConsumerMetadata metadata,
                                 final IsolationLevel isolationLevel,
                                 final Time time,
                                 final long retryBackoffMs,
                                 final int requestTimeoutMs,
                                 final long defaultApiTimeoutMs,
                                 final ApiVersions apiVersions,
                                 final NetworkClientDelegate networkClientDelegate,
                                 final CommitRequestManager commitRequestManager,
                                 final LogContext logContext) {
        // 检查 subscriptionState 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(subscriptionState);
        // 检查 metadata 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(metadata);
        // 检查 isolationLevel 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(isolationLevel);
        // 检查 time 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(time);
        // 检查 apiVersions 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(apiVersions);
        // 检查 networkClientDelegate 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(networkClientDelegate);
        // 检查 logContext 是否为 null，如果是，则抛出 NullPointerException
        requireNonNull(logContext);

        // 初始化消费者元数据
        this.metadata = metadata;
        // 初始化隔离级别
        this.isolationLevel = isolationLevel;
        // 使用 logContext 创建当前类的日志记录器
        this.log = logContext.logger(getClass());
        // 初始化需要重试的请求集合为一个新的 HashSet
        this.requestsToRetry = new HashSet<>();
        // 初始化准备发送的请求列表为一个新的 ArrayList
        this.requestsToSend = new ArrayList<>();
        // 初始化订阅状态
        this.subscriptionState = subscriptionState;
        // 初始化时间工具类
        this.time = time;
        // 初始化请求超时时间
        this.requestTimeoutMs = requestTimeoutMs;
        // 初始化默认 API 超时时间
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        // 初始化 API 版本信息
        this.apiVersions = apiVersions;
        // 初始化网络客户端代理
        this.networkClientDelegate = networkClientDelegate;
        // 初始化 OffsetFetcherUtils，传入必要的依赖项
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptionState,
                time, retryBackoffMs, apiVersions);
        // 注册集群元数据更新回调。注意，这仅依赖于上面初始化的 requestsToRetry，
        // 并且在所有管理器都初始化并且网络线程启动之前不会被调用。
        // 实现细节: 当集群元数据发生变化时（例如，分区领导者切换），会调用此管理器的 onUpdate 方法。
        this.metadata.addClusterUpdateListener(this);
        // 初始化提交请求管理器
        this.commitRequestManager = commitRequestManager;
    }

    /**
     * 表示一个待处理的获取已提交偏移量的请求。
     * 应用场景: 当需要异步获取已提交的偏移量以更新消费者的拉取位置时，会创建此类实例来跟踪请求状态。
     * 设计考虑: 将请求的分区集合和用于接收结果的 CompletableFuture 封装在一起，便于管理和传递。
     */
    private static class PendingFetchCommittedRequest {
        // 请求的分区集合
        final Set<TopicPartition> requestedPartitions;
        // 用于接收偏移量和元数据映射结果的 CompletableFuture
        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result;

        /**
         * PendingFetchCommittedRequest 的构造函数。
         *
         * @param requestedPartitions 请求的分区集合，不能为空。
         * @param result 用于接收偏移量和元数据映射结果的 CompletableFuture，不能为空。
         * 应用场景: 在发起获取已提交偏移量的请求之前，创建此对象来保存请求信息和结果回调。
         * 设计考虑: 通过构造函数注入必要的参数，并进行非空检查，确保对象的有效性。
         */
        private PendingFetchCommittedRequest(final Set<TopicPartition> requestedPartitions,
                                             final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
            // 校验 requestedPartitions 不为 null，并赋值给成员变量
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            // 校验 result 不为 null，并赋值给成员变量
            this.result = Objects.requireNonNull(result);
        }
    }

    /**
     * 确定是否有待发送的获取偏移量请求，并构建一个包含这些请求的 {@link NetworkClientDelegate.PollResult}。
     * 应用场景: 在消费者主循环的轮询阶段，调用此方法收集所有准备好发送到 broker 的偏移量相关请求。
     * 设计考虑: 将待发送请求从内部列表复制出来并清空原列表，确保请求只被发送一次，并且为下一次轮询准备好空列表。
     * @param currentTimeMs 当前时间（毫秒），可用于请求超时等判断，但在此方法中未使用。
     * @return 包含待发送请求的 {@link NetworkClientDelegate.PollResult}。
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // 复制待发送请求列表，创建一个新的 ArrayList 实例，内容与 requestsToSend 相同
        List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>(requestsToSend);
        // 清空原始的待发送请求列表，为下一次收集请求做准备
        requestsToSend.clear();
        // 使用收集到的待发送请求创建一个新的 PollResult 并返回
        return new NetworkClientDelegate.PollResult(unsentRequests);
    }

    /**
     * 为给定的分区和时间戳检索偏移量。对于每个分区，这将检索其时间戳大于或等于目标时间戳的第一条消息的偏移量。
     *
     * @param timestampsToSearch 要获取偏移量的分区和目标时间戳的映射。
     * @param requireTimestamps  如果代理不支持获取精确的时间戳偏移量，则此项为 true 时应引发 UnsupportedVersionException。
     * @return 包含找到的 {@link TopicPartition} 和 {@link OffsetAndTimestampInternal} 映射的 Future。
     *         当接收并处理请求响应后（在调用 {@link #poll(long)} 之后），该 Future 将完成。
     * 应用场景: 用户需要根据时间戳查找特定分区的消息偏移量，例如从某个历史时间点开始消费。
     * 设计考虑: 这是一个异步操作，返回 CompletableFuture 允许调用者非阻塞地等待结果。
     *           方法内部会处理元数据更新（添加临时主题）、创建请求状态对象、准备请求，并在完成后清理临时主题。
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsets(
            Map<TopicPartition, Long> timestampsToSearch,
            boolean requireTimestamps) {
        // 检查 timestampsToSearch 是否为空
        if (timestampsToSearch.isEmpty()) {
            // 如果为空，则直接返回一个已完成的、包含空映射的 CompletableFuture
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        // 将 timestampsToSearch 中的主题添加到元数据的临时主题列表中，确保元数据包含这些主题的信息
        metadata.addTransientTopics(OffsetFetcherUtils.topicsForPartitions(timestampsToSearch.keySet()));
        // 创建 ListOffsetsRequestState 对象，用于管理此次 ListOffsets 请求的状态
        ListOffsetsRequestState listOffsetsRequestState = new ListOffsetsRequestState(
                timestampsToSearch, // 需要查询的分区和时间戳
                requireTimestamps,  // 是否要求精确时间戳
                offsetFetcherUtils, // OffsetFetcher 工具类
                isolationLevel);    // 隔离级别
        // 为 listOffsetsRequestState 的全局结果 CompletableFuture 注册一个完成时的回调
        listOffsetsRequestState.globalResult.whenComplete((result, error) -> {
            // 请求完成后，从元数据中清除临时主题
            metadata.clearTransientTopics();
            // 检查是否有错误发生
            if (error != null) {
                // 如果有错误，记录调试日志，包含错误的分区和时间戳信息
                log.debug("Fetch offsets completed with error for partitions and timestamps {}.",
                        timestampsToSearch, error);
            } else {
                // 如果没有错误，记录调试日志，包含成功的分区、时间戳和结果信息
                log.debug("Fetch offsets completed successfully for partitions and timestamps {}." +
                        " Result {}", timestampsToSearch, result);
            }
        });

        // 准备获取偏移量的请求，这可能会将请求添加到 requestsToSend 列表中
        prepareFetchOffsetsRequests(timestampsToSearch, requireTimestamps, listOffsetsRequestState);
        // 返回 listOffsetsRequestState 的全局结果 CompletableFuture，并在其完成后应用转换逻辑
        return listOffsetsRequestState.globalResult.thenApply(
                // 使用 OffsetFetcherUtils 将获取到的偏移量结果转换为内部表示 OffsetAndTimestampInternal
                result -> OffsetFetcherUtils.buildOffsetsForTimeInternalResult(
                        timestampsToSearch, // 原始的查询分区和时间戳
                        result.fetchedOffsets)); // 获取到的偏移量结果
    }

    /**
     * 为没有位置的已分配分区更新获取位置。这将：
     * <ul>
     *     <li>检查是否所有已分配的分区都已有获取位置，如果是，则立即返回</li>
     *     <li>触发一个异步请求来验证位置（检测日志截断）</li>
     *     <li>如果启用了获取已提交偏移量，则获取已提交的偏移量，并使用响应更新位置</li>
     *     <li>为可能仍需要位置的分区获取分区偏移量，并使用响应更新位置</li>
     * </ul>
     *
     * @param deadlineMs 触发应用程序事件过期的截止时间（毫秒）。在此之后收到的任何错误都将被保存，
     *                   并在下次调用此函数时用于异常完成结果。
     * @return 一个 Future，它将以一个布尔值完成，该布尔值指示是否所有已分配的分区都有位置（基于
     * {@link SubscriptionState#hasAllFetchPositions()}）。如果所有位置都已可用，它将立即以 true 完成。
     * 如果某些位置缺失，则在检索到偏移量并更新位置后，该 Future 将完成。
     * 应用场景: 消费者启动或重新平衡后，需要确定每个已分配分区的起始消费位置。
     * 设计考虑: 这是一个关键的初始化步骤，确保消费者从正确的位置开始拉取消息。
     *           方法会首先检查是否已经有缓存的异常，然后验证现有位置的有效性。
     *           如果所有位置都已就绪，则快速返回。否则，会触发一系列异步操作来获取和更新缺失的位置。
     */
    public CompletableFuture<Boolean> updateFetchPositions(long deadlineMs) {
        // 创建一个新的 CompletableFuture 用于返回操作结果
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        try {
            // 检查是否有先前缓存的更新位置异常，如果有，则用该异常完成当前的 CompletableFuture 并返回
            if (maybeCompleteWithPreviousException(result)) {
                // 如果已用先前异常完成，则直接返回结果
                return result;
            }

            // 如果需要，验证当前已分配分区的位置信息（例如，检测日志截断）
            validatePositionsIfNeeded();

            // 检查订阅状态，判断是否所有已分配的分区都已经有了有效的拉取位置
            if (subscriptionState.hasAllFetchPositions()) {
                // 如果所有位置都已可用，则直接以 true 完成 CompletableFuture
                result.complete(true);
                // 返回已完成的 CompletableFuture
                return result;
            }

            // 如果某些位置缺失，则触发异步操作来获取偏移量并更新这些位置
            // updatePositionsWithOffsets 方法会返回一个 CompletableFuture，表示更新操作的完成状态
            updatePositionsWithOffsets(deadlineMs).whenComplete((__, error) -> {
                // 当 updatePositionsWithOffsets 操作完成时，此回调被执行
                if (error != null) {
                    // 如果更新操作中发生错误，则用该错误异常完成外部的 CompletableFuture
                    result.completeExceptionally(error);
                } else {
                    // 如果更新操作成功，则根据当前订阅状态（是否所有分区都有拉取位置）完成外部的 CompletableFuture
                    result.complete(subscriptionState.hasAllFetchPositions());
                }
            });

        } catch (Exception e) {
            // 如果在上述同步操作中捕获到任何异常
            // 将异常包装（如果需要）为 KafkaException，并用此异常完成 CompletableFuture
            result.completeExceptionally(maybeWrapAsKafkaException(e));
        }
        // 返回 CompletableFuture，调用者可以异步等待其完成
        return result;
    }

    /**
     * 尝试使用先前缓存的异常来完成给定的 CompletableFuture。
     * 应用场景：如果在之前的异步操作中发生了异常，并且该异常被缓存了，那么后续依赖该操作结果的 Future 应该以这个缓存的异常结束。
     * 设计考虑：通过这种方式，可以将异步操作链中的异常传播下去，而不是静默地忽略它们。
     * @param result 需要被尝试完成的 CompletableFuture
     * @return 如果使用了缓存的异常来完成 Future，则返回 true；否则返回 false
     */
    private boolean maybeCompleteWithPreviousException(CompletableFuture<Boolean> result) {
        // 获取并清除缓存的更新位置时发生的异常
        Throwable cachedException = cachedUpdatePositionsException.getAndSet(null);
        // 如果存在缓存的异常
        if (cachedException != null) {
            // 使用缓存的异常来异常完成 CompletableFuture
            result.completeExceptionally(cachedException);
            // 返回 true 表示 Future 已被异常完成
            return true;
        }
        // 如果没有缓存的异常，返回 false
        return false;
    }

    /**
     * 生成获取偏移量的请求，并在收到响应后更新位置。此方法将首先尝试使用可用的已提交偏移量。
     * 如果没有可用的已提交偏移量，它将使用从领导者检索到的分区偏移量。
     * 应用场景：当消费者需要初始化或重置其消费分区的起始位置时调用。例如，在消费者启动、分区分配发生变化，或者需要根据特定策略（如最早、最新）重置偏移量时。
     * 设计考虑：该方法区分了有提交管理器（通常用于消费者组）和没有提交管理器（通常用于独立消费者或手动管理偏移量）的情况。
     * 它首先尝试使用已提交的偏移量，如果失败或不可用，则回退到使用分区自身的偏移量（例如，最早或最新的偏移量）。
     * 整个过程是异步的，通过 CompletableFuture 进行协调。
     * @param deadlineMs 操作的截止时间（毫秒），用于超时控制
     * @return 一个 CompletableFuture<Void>，当所有位置更新操作完成时，该 Future 完成；如果发生错误，则异常完成
     */
    private CompletableFuture<Void> updatePositionsWithOffsets(long deadlineMs) {
        // 创建一个 CompletableFuture 用于表示整个更新位置操作的结果
        CompletableFuture<Void> result = new CompletableFuture<>();

        // 如果更新位置的操作在事件过期后发生错误，则缓存该异常
        cacheExceptionIfEventExpired(result, deadlineMs);

        // 用于表示更新位置操作的 CompletableFuture
        CompletableFuture<Void> updatePositions;
        // 获取当前需要初始化的分区集合
        final Set<TopicPartition> initializingPartitions = subscriptionState.initializingPartitions();
        // 检查 commitRequestManager 是否存在（即是否启用了偏移量提交机制）
        if (commitRequestManager != null) {
            // 如果存在 commitRequestManager，首先尝试使用已提交的偏移量初始化分区
            CompletableFuture<Void> refreshWithCommittedOffsets = initWithCommittedOffsetsIfNeeded(initializingPartitions, deadlineMs);

            // 在使用已提交偏移量刷新完成后，接着对那些可能仍需要重置（或等待重置）的分区，使用分区偏移量进行初始化
            // thenCompose 用于将前一个 Future 的结果传递给下一个异步操作
            updatePositions = refreshWithCommittedOffsets.thenCompose(__ -> initWithPartitionOffsetsIfNeeded(initializingPartitions));

        } else {
            // 如果不存在 commitRequestManager，直接使用分区偏移量初始化分区
            updatePositions = initWithPartitionOffsetsIfNeeded(initializingPartitions);
        }

        // 当 updatePositions 操作完成时（无论成功还是失败）
        updatePositions.whenComplete((__, resetError) -> {
            // 如果 resetError 为 null，表示操作成功
            if (resetError == null) {
                // 正常完成 result Future
                result.complete(null);
            } else {
                // 如果 resetError 不为 null，表示操作失败，使用该错误异常完成 result Future
                result.completeExceptionally(resetError);
            }
        });

        // 返回表示整个更新位置操作的 CompletableFuture
        return result;
    }

    /**
     * 缓存更新拉取位置时可能发生的异常。请注意，由于更新拉取位置是异步触发的，
     * 当触发的 UpdateFetchPositionsEvent 已过期时，可能会发现错误。
     * 在这种情况下，异常会保存在内存中，以便在处理后续的 UpdateFetchPositionsEvent 时抛出。
     * 应用场景：当一个异步更新位置的操作失败时，如果触发该操作的原始事件（如元数据更新、轮询超时）已经过期，
     * 那么这个异常不能直接影响当前的轮询结果（因为它可能已经处理完毕或超时）。
     * 此时，将异常缓存起来，以便在下一次相关的更新位置操作时能够感知到这个历史错误。
     * 设计考虑：这是一种延迟错误处理机制，确保异步操作的错误不会丢失，即使它们在原始上下文之外发生。
     *
     * @param result     更新拉取位置的 Future，用于获取异常（如果存在）
     * @param deadlineMs 触发应用程序事件的截止时间，用于识别当 result Future 中发生错误时，事件是否已过期。
     */
    private void cacheExceptionIfEventExpired(CompletableFuture<Void> result, long deadlineMs) {
        // 当 result 这个 CompletableFuture 完成时（无论正常完成还是异常完成）执行回调
        result.whenComplete((__, error) -> {
            // 检查当前时间是否已经超过了指定的截止时间 deadlineMs
            boolean updatePositionsExpired = time.milliseconds() >= deadlineMs;
            // 如果存在错误 (error != null) 并且事件已过期 (updatePositionsExpired 为 true)
            if (error != null && updatePositionsExpired) {
                // 将错误缓存到 cachedUpdatePositionsException 中，以便后续处理
                cachedUpdatePositionsException.set(error);
            }
        });
    }

    /**
     * 如果仍有分区需要位置并且定义了重置策略，则使用默认策略请求重置。
     * 应用场景：当某些分区没有有效的已提交偏移量，或者消费者配置了特定的偏移量重置策略（如 earliest, latest）时，
     * 需要根据这些策略来确定这些分区的起始消费位置。
     * 设计考虑：此方法首先标记需要重置的分区，如果在此过程中（例如，没有配置重置策略）发生异常，则立即异常完成 Future。
     * 否则，它会触发一个异步操作（resetPositionsIfNeeded）来实际获取偏移量并更新位置。
     *
     * @param initializingPartitions 应初始化的分区集合。这不会为可能已添加到订阅状态但未包含在此集合中的分区重置位置。
     * @return 当重置操作完成检索偏移量并使用它们在订阅状态中设置位置时，该 Future 将完成。
     * @throws NoOffsetForPartitionException 如果没有配置重置策略。
     */
    private CompletableFuture<Void> initWithPartitionOffsetsIfNeeded(Set<TopicPartition> initializingPartitions) {
        // 创建一个 CompletableFuture 用于表示此操作的结果
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            // 标记需要重置的分区，使用配置的重置策略。如果没有定义策略，
            // 这将引发 NoOffsetForPartitionException 异常。
            // initializingPartitions::contains 是一个 Predicate，用于过滤出在 initializingPartitions 集合中的分区进行重置
            subscriptionState.resetInitializingPositions(initializingPartitions::contains);
        } catch (Exception e) {
            // 如果在标记重置位置时发生异常（例如 NoOffsetForPartitionException）
            // 则使用该异常来异常完成 CompletableFuture
            result.completeExceptionally(e);
            // 并立即返回这个已异常完成的 Future
            return result;
        }

        // 对于等待重置的分区，生成一个 ListOffset 请求以根据策略（例如，最早、最新）检索分区偏移量，并更新位置。
        // resetPositionsIfNeeded() 会返回一个 CompletableFuture，表示异步重置位置的操作
        return resetPositionsIfNeeded();
    }

    /**
     * 为需要初始化的分区获取已提交的偏移量。这将触发一个 OffsetFetch 请求，并在收到响应后更新订阅状态中的位置。
     * 应用场景：当消费者加入一个消费者组，或者分区重新分配后，需要从上次提交的偏移量开始消费。
     * 设计考虑：此方法处理了 OffsetFetch 请求的发送和响应。为了优化性能和避免重复请求，
     * 它会检查是否可以重用一个挂起的 OffsetFetch 事件。如果 poll() 方法提供的超时时间较短，
     * 获取偏移量的操作可能会超时。为了处理这种情况，首次尝试获取已提交偏移量时，会创建一个
     * FetchCommittedOffsetsEvent（可能具有更长的超时时间）并存储起来。该事件用于首次尝试，
     * 但如果超时，后续尝试也将使用该事件以等待结果。
     *
     * @param initializingPartitions 需要更新位置的分区集合。在整个过程中（获取已提交偏移量时，以及为可能没有已提交偏移量的分区重置位置时）都将使用此集合。
     * @param deadlineMs             触发此操作的应用程序事件的截止时间。用于确定允许重用的偏移量获取操作完成的时间。
     * @return 一个 CompletableFuture<Void>，当使用获取到的已提交偏移量更新位置完成后，该 Future 完成；如果发生错误或超时，则异常完成。
     * @throws TimeoutException 如果在超时时间内无法检索到偏移量
     */
    private CompletableFuture<Void> initWithCommittedOffsetsIfNeeded(Set<TopicPartition> initializingPartitions,
                                                                     long deadlineMs) {
        // 如果需要初始化的分区集合为空，则直接返回一个已完成的 CompletableFuture
        if (initializingPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // 记录调试日志，说明正在为哪些分区刷新已提交的偏移量
        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        // 创建一个 CompletableFuture 用于表示此操作的结果
        CompletableFuture<Void> result = new CompletableFuture<>();

        // poll() 提供的超时时间越短，偏移量获取超时的可能性就越大。为了处理这种情况，
        // 在第一次尝试获取已提交的偏移量时，会创建一个 FetchCommittedOffsetsEvent（可能具有更长的超时时间）并存储起来。
        // 该事件用于第一次尝试，但如果超时，后续的尝试也将使用该事件以等待结果。
        // 检查是否可以重用一个挂起的 OffsetFetch 事件，条件是挂起事件的分区与当前需要初始化的分区相同
        if (!canReusePendingOffsetFetchEvent(initializingPartitions)) {
            // 如果不能重用，则生成一个新的 OffsetFetch 请求并在收到响应时更新位置
            // 计算获取已提交偏移量的截止时间，取 deadlineMs 和 (当前时间 + 默认API超时时间) 中的较大值
            final long fetchCommittedDeadlineMs = Math.max(deadlineMs, time.milliseconds() + defaultApiTimeoutMs);
            // 调用 commitRequestManager 的 fetchOffsets 方法异步获取已提交的偏移量
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsets =
                    commitRequestManager.fetchOffsets(initializingPartitions, fetchCommittedDeadlineMs);
            // 当 fetchOffsets 完成时（无论成功或失败），执行回调
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsetsAndRefresh =
                    fetchOffsets.whenComplete((offsets, error) -> {
                        // 清除挂起的 OffsetFetch 事件，因为请求已完成
                        pendingOffsetFetchEvent = null;
                        // 使用检索到的偏移量更新位置，并将结果（成功或错误）传递给 result 这个 CompletableFuture
                        refreshOffsets(offsets, error, result);
                    });
            // 创建一个新的 PendingFetchCommittedRequest 对象，并将其存储在 pendingOffsetFetchEvent 中，以便后续可能的重用
            pendingOffsetFetchEvent = new PendingFetchCommittedRequest(initializingPartitions, fetchOffsetsAndRefresh);
        } else {
            // 如果可以重用挂起的 OffsetFetch 请求
            // 当挂起的请求 (pendingOffsetFetchEvent.result) 完成时，执行回调
            pendingOffsetFetchEvent.result.whenComplete((__, error) -> {
                // 如果没有错误 (error == null)
                if (error == null) {
                    // 正常完成 result CompletableFuture
                    result.complete(null);
                } else {
                    // 如果有错误，则使用该错误异常完成 result CompletableFuture
                    result.completeExceptionally(error);
                }
            });
        }

        // 返回表示整个操作的 CompletableFuture
        return result;
    }

    /**
     * 使用给定的已提交偏移量来更新仍然需要它的分区的位移。
     * 应用场景: 当消费者获取到已提交的偏移量后，调用此方法来更新内部维护的消费位移。
     *          这确保了消费者从正确的位移开始拉取消息，特别是在分区分配发生变化或消费者重启后。
     * 实现细节: 如果没有错误，它会筛选出那些仍在初始化（即尚未确定起始位移）的分区的偏移量，
     *          然后调用 refreshCommittedOffsets 来实际更新这些分区的位移。
     * 设计考虑: 通过 CompletableFuture 支持异步操作，避免阻塞调用线程。
     *          错误处理确保了在获取偏移量失败时，能够将异常传递给调用者。
     *
     * @param offsets 用于更新正在初始化分区的位移的已提交偏移量。
     * @param error   在 OffsetFetch 请求响应中收到的错误。如果请求成功，则为 null。
     * @param result  一旦所有位移都已使用给定的已提交偏移量更新，则完成此 Future。
     */
    private void refreshOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets, // 参数：已提交的偏移量映射，键为 TopicPartition，值为 OffsetAndMetadata
                                final Throwable error, // 参数：OffsetFetch 请求的错误，如果成功则为 null
                                final CompletableFuture<Void> result) { // 参数：用于通知操作完成的 CompletableFuture
        // 检查 OffsetFetch 请求是否成功（即 error 是否为 null）
        if (error == null) {
            // 如果请求成功

            // 确保我们只为那些仍然需要位移的分区设置位移（例如，某些分区可能已经手动分配了位移）
            // 调用 offsetsForInitializingPartitions 方法，筛选出那些当前正在初始化（即需要设置起始位移）的分区的偏移量
            Map<TopicPartition, OffsetAndMetadata> offsetsToApply = offsetsForInitializingPartitions(offsets);

            // 调用 ConsumerUtils.refreshCommittedOffsets 方法，使用筛选后的偏移量更新订阅状态中的分区位移
            refreshCommittedOffsets(offsetsToApply, metadata, subscriptionState);

            // 标记 CompletableFuture 为成功完成
            result.complete(null);

        } else {
            // 如果 OffsetFetch 请求失败
            // 记录错误日志，说明获取已提交偏移量以更新位移时发生错误
            log.error("Error fetching committed offsets to update positions", error);
            // 将 CompletableFuture 标记为异常完成，并将错误传递给调用者
            result.completeExceptionally(error);
        }
    }

    /**
     * 从给定的集合中获取属于仍然需要位移的分区（即正在初始化的分区）的偏移量。
     * 应用场景: 在 refreshOffsets 方法中，用于确保只更新那些尚未确定起始消费位移的分区的位移。
     *          避免覆盖已经手动设置或通过其他方式确定的位移。
     * 实现细节: 遍历传入的偏移量映射，检查每个分区是否存在于当前正在初始化的分区集合中。
     * 设计考虑: 这是一个辅助方法，逻辑清晰，专注于筛选特定状态的分区。
     *
     * @param offsets 每个分区的偏移量
     * @return 与仍在初始化中的分区关联的偏移量子集
     */
    private Map<TopicPartition, OffsetAndMetadata> offsetsForInitializingPartitions(Map<TopicPartition, OffsetAndMetadata> offsets) { // 参数：包含所有已获取偏移量的映射
        // 从订阅状态中获取当前所有正在初始化的分区集合
        Set<TopicPartition> currentlyInitializingPartitions = subscriptionState.initializingPartitions();
        // 创建一个新的 HashMap 用于存储筛选结果
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        // 遍历传入的 offsets 映射
        offsets.forEach((key, value) -> { // key 是 TopicPartition，value 是 OffsetAndMetadata
            // 检查当前分区 (key) 是否存在于正在初始化的分区集合中
            if (currentlyInitializingPartitions.contains(key)) {
                // 如果是，则将该分区的偏移量信息添加到结果映射中
                result.put(key, value);
            }
        });
        // 返回筛选后的偏移量映射
        return result;
    }

    /**
     * 判断 {@link #pendingOffsetFetchEvent 挂起的偏移量获取事件} 是否可以重用。
     * 应用场景: 在尝试更新消费位移时，如果之前有一个获取已提交偏移量的请求正在进行中，此方法用于判断是否可以直接复用该请求的结果，
     *          而不是重新发送一个新的请求。这可以减少不必要的网络开销。
     * 实现细节: 检查是否存在挂起的事件，并且该事件请求的分区集合与当前需要的分区集合是否完全相同。
     * 设计考虑: 优化性能，避免重复请求。
     * <ul>
     *     <li>一个挂起的偏移量获取事件存在</li>
     *     <li>挂起的偏移量获取事件的分区集与给定的分区相同</li>
     * </ul>
     *
     * @param partitions 当前请求的分区集合
     * @return 如果可以重用挂起的偏移量获取事件，则返回 true；否则返回 false
     */
    private boolean canReusePendingOffsetFetchEvent(Set<TopicPartition> partitions) { // 参数：当前请求的分区集合
        // 检查是否存在挂起的偏移量获取事件 (pendingOffsetFetchEvent)
        if (pendingOffsetFetchEvent == null) {
            // 如果不存在挂起的事件，则不能重用，返回 false
            return false;
        }

        // 比较挂起事件中请求的分区集合 (pendingOffsetFetchEvent.requestedPartitions)
        // 与当前方法传入的分区集合 (partitions) 是否完全相同
        return pendingOffsetFetchEvent.requestedPartitions.equals(partitions);
    }

    /**
     * 为所有需要它的已分配分区重置位移。位移将根据为每个分区定义的重置策略的时间戳进行重置。
     * 这将为分区和时间戳生成 ListOffsets 请求，并将它们排队以便在下一次调用 {@link #poll(long)} 时发送。
     * <p/>
     * 应用场景: 当消费者启动时，如果某些分区没有有效的已提交偏移量，或者根据配置的重置策略（如 earliest, latest, none 或特定时间戳）需要重置时调用。
     *          例如，`auto.offset.reset` 配置为 `earliest` 时，新加入的消费者组会从分区的最早可用偏移量开始消费。
     * 实现细节:
     * 1. 调用 `offsetFetcherUtils.getOffsetResetStrategyForPartitions()` 获取需要重置位移的分区及其对应的 `AutoOffsetResetStrategy`。
     * 2. 如果没有分区需要重置，则返回一个已完成的 `CompletableFuture`。
     * 3. 否则，调用 `sendListOffsetsRequestsAndResetPositions` 方法异步发送 `ListOffsets` 请求并根据响应重置位移。
     * 设计考虑: 使用 `CompletableFuture` 支持异步操作，避免阻塞主消费线程。
     *          错误处理（如 `TopicAuthorizationException`）会被捕获并包装在返回的 `CompletableFuture` 中，由调用者处理。
     *          将实际的请求发送和响应处理逻辑委托给 `sendListOffsetsRequestsAndResetPositions` 方法。
     * 当收到响应时，位移会在内存中的订阅状态上更新。如果在响应中收到错误，
     * 它将被保存以便在此函数的下一次调用时抛出（例如 {@link org.apache.kafka.common.errors.TopicAuthorizationException}）。
     * @return 一个 CompletableFuture，在所有需要重置的分区位移都已成功重置或发生不可恢复错误时完成。
     */
    CompletableFuture<Void> resetPositionsIfNeeded() {
        // 定义一个映射，用于存储需要重置位移的分区及其对应的自动位移重置策略
        Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap;

        try {
            // 调用 offsetFetcherUtils 的 getOffsetResetStrategyForPartitions 方法
            // 获取那些需要根据其自动位移重置策略来重置位移的分区
            partitionAutoOffsetResetStrategyMap = offsetFetcherUtils.getOffsetResetStrategyForPartitions();
        } catch (Exception e) {
            // 如果在获取重置策略时发生异常（例如 NoOffsetForPartitionException）
            // 创建一个新的 CompletableFuture
            CompletableFuture<Void> result = new CompletableFuture<>();
            // 将异常传递给 CompletableFuture，使其异常完成
            result.completeExceptionally(e);
            // 返回这个异常完成的 CompletableFuture
            return result;
        }

        // 检查需要重置位移的分区映射是否为空
        if (partitionAutoOffsetResetStrategyMap.isEmpty())
            // 如果没有分区需要重置位移，则返回一个已经成功完成的 CompletableFuture
            return CompletableFuture.completedFuture(null);

        // 如果有分区需要重置位移，则调用 sendListOffsetsRequestsAndResetPositions 方法
        // 异步发送 ListOffsets 请求并根据响应重置这些分区的位移
        // 返回该方法创建的 CompletableFuture，用于跟踪异步操作的状态
        return sendListOffsetsRequestsAndResetPositions(partitionAutoOffsetResetStrategyMap);
    }

    /**
     * 为所有检测到领导者更改的已分配分区验证位移。
     * 这将为分区生成 OffsetsForLeaderEpoch 请求，其中包含已知的位移纪元和当前领导者纪元。
     * 它会将生成的请求排队，以便在下一次调用 {@link #poll(long)} 时发送。
     * <p/>
     * 应用场景: 当消费者检测到其订阅的某个分区的 Leader 发生变更时（例如，由于 Broker 故障切换），
     *          需要验证当前的消费位移是否仍然有效，以防止数据丢失（如果旧 Leader 的数据未完全同步到新 Leader）
     *          或数据重复（如果新 Leader 的日志被截断到比当前消费位移更早的位置）。
     * 实现细节:
     * 1. 调用 `offsetFetcherUtils.getPartitionsToValidate()` 获取需要验证位移的分区及其当前的拉取位移信息。
     * 2. 如果没有分区需要验证，则直接返回。
     * 3. 否则，调用 `sendOffsetsForLeaderEpochRequestsAndValidatePositions` 方法异步发送 `OffsetsForLeaderEpoch` 请求并根据响应验证位移。
     * 设计考虑: 这是一个重要的机制，用于保证在 Leader 切换时的消费一致性。
     *          如果检测到日志截断，会抛出 `LogTruncationException`，消费者通常会根据此异常重置其位移。
     *          将实际的请求发送和响应处理逻辑委托给 `sendOffsetsForLeaderEpochRequestsAndValidatePositions` 方法。
     *
     * <p/>
     *
     * 当收到响应时，将验证位移，如果检测到日志截断，
     * 则会将 {@link LogTruncationException} 保存在 cachedUpdatePositionsException 的内存中，以便在此函数的下一次调用时抛出。
     */
    void validatePositionsIfNeeded() {
        // 调用 offsetFetcherUtils 的 getPartitionsToValidate 方法
        // 获取那些因为 Leader 变更或其他原因需要验证其当前消费位移的分区及其拉取位移信息
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate = offsetFetcherUtils.getPartitionsToValidate();
        // 检查需要验证位移的分区映射是否为空
        if (partitionsToValidate.isEmpty()) {
            // 如果没有分区需要验证位移，则直接返回，不执行任何操作
            return;
        }

        // 如果有分区需要验证位移，则调用 sendOffsetsForLeaderEpochRequestsAndValidatePositions 方法
        // 异步发送 OffsetsForLeaderEpoch 请求，并根据 Broker 的响应来验证这些分区的位移
        sendOffsetsForLeaderEpochRequestsAndValidatePositions(partitionsToValidate);
    }

    /**
     * 为具有已知领导者的分区生成请求。通过将具有未知领导者的分区添加到 listOffsetsRequestState.remainingToSearch 来更新 listOffsetsRequestState。
     * 应用场景: 当需要根据时间戳获取多个分区的偏移量时，此方法负责准备 ListOffsets 请求。
     * 实现细节: 它会调用 buildListOffsetsRequests 来实际构建请求，并将构建好的请求添加到待发送队列中。如果元数据过时，则将请求状态添加到重试队列。
     * 设计考虑: 将请求的准备逻辑封装在此方法中，简化了外部调用。通过捕获 StaleMetadataException 来处理元数据过时的情况，提高了系统的健壮性。
     * @param timestampsToSearch 要搜索的分区到时间戳的映射。
     * @param requireTimestamps 如果代理不支持获取精确的时间戳偏移量，则此项为 true 时应引发 UnsupportedVersionException。
     * @param listOffsetsRequestState ListOffsets 请求的状态对象，用于跟踪请求的进度和结果。
     */
    private void prepareFetchOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                             final boolean requireTimestamps,
                                             final ListOffsetsRequestState listOffsetsRequestState) {
        // 实现细节: 尝试构建 ListOffsets 请求。
        try {
            // 实现细节: 调用 buildListOffsetsRequests 方法构建 ListOffsets 请求列表。
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = buildListOffsetsRequests(
                    timestampsToSearch, requireTimestamps, listOffsetsRequestState);
            // 实现细节: 将构建好的未发送请求添加到 requestsToSend 列表中，等待后续轮询发送。
            requestsToSend.addAll(unsentRequests);
        // 实现细节: 捕获 StaleMetadataException 异常，表示元数据可能已过时。
        } catch (StaleMetadataException e) {
            // 实现细节: 如果元数据过时，将当前的 listOffsetsRequestState 添加到 requestsToRetry 集合中，以便在元数据更新后重试。
            requestsToRetry.add(listOffsetsRequestState);
        }
    }

    /**
     * 当集群元数据更新时调用此方法。
     * 应用场景: Kafka 客户端在运行时会监控集群元数据的变化（例如 Topic leader 切换）。当元数据更新时，此回调方法被触发。
     * 实现细节: 此方法会处理之前因元数据问题而需要重试的 ListOffsets 请求。它会遍历 requestsToRetry 列表中的每个请求状态，
     *           并为其中尚未处理的分区重新准备 ListOffsets 请求。
     * 设计考虑: 通过实现 ClusterResourceListener 接口并在元数据更新时执行重试逻辑，确保了即使在集群拓扑发生变化时，
     *           获取偏移量的请求也能够最终成功。复制 requestsToRetry 列表进行处理是为了避免在迭代过程中修改列表导致的并发修改异常。
     * @param clusterResource 集群资源信息，包含了更新后的集群元数据。
     */
    @Override
    public void onUpdate(ClusterResource clusterResource) {
        // 实现细节: 重试那些等待元数据更新的请求。处理列表的副本以避免错误，
        // 因为如果在重试任何请求失败时从 fetchOffsetsByTimes 调用中修改了 requestsToRetry 列表，则可能会发生错误。
        // 实现细节: 创建一个 requestsToRetry 列表的副本，用于处理，以避免在迭代时修改原始列表。
        List<ListOffsetsRequestState> requestsToProcess = new ArrayList<>(requestsToRetry);
        // 实现细节: 清空原始的 requestsToRetry 列表，因为这些请求即将被处理。
        requestsToRetry.clear();
        // 实现细节: 遍历待处理的请求状态列表。
        requestsToProcess.forEach(requestState -> {
            // 实现细节: 从当前请求状态中获取剩余需要搜索的分区和时间戳映射。
            Map<TopicPartition, Long> timestampsToSearch =
                    new HashMap<>(requestState.remainingToSearch);
            // 实现细节: 清空当前请求状态中的 remainingToSearch，因为这些分区即将被重新请求。
            requestState.remainingToSearch.clear();
            // 实现细节: 为这些分区重新准备 ListOffsets 请求。
            prepareFetchOffsetsRequests(timestampsToSearch, requestState.requireTimestamps, requestState);
        });
    }

    /**
     * 构建 ListOffsets 请求，用于为指定分区按目标时间获取偏移量。
     * 应用场景: 当消费者需要根据特定的时间戳查找消息的起始偏移量时，会调用此方法来构建相应的 ListOffsets 请求。
     *           例如，用户希望从昨天某个特定时间点开始消费数据。
     * 实现细节: 此方法首先根据分区和目标时间戳对请求进行分组（按目标节点 Node）。
     *           如果找不到任何分区的领导者（元数据过时），则抛出 StaleMetadataException。
     *           然后，为每个目标节点创建一个或多个 ListOffsets 请求，并使用 MultiNodeRequest 来管理这些并行请求的完成状态。
     *           当所有节点的响应都收到后，会更新 ListOffsetsRequestState 的状态，包括已获取的偏移量和需要重试的分区。
     *           如果所有分区的偏移量都已成功获取，则完成全局的 Future；否则，将请求状态添加到重试队列并请求元数据更新。
     * 设计考虑: 将构建 ListOffsets 请求的复杂逻辑封装在此方法中，包括按节点分组、处理多节点请求的完成回调等。
     *           使用 MultiNodeRequest 有助于管理对多个 Broker 的并行请求，并在所有请求完成后统一处理结果。
     *           通过 Optional.of(listOffsetsRequestState) 将请求状态传递给 groupListOffsetRequests，使得在分组时可以更新请求状态中需要重试的分区。
     * @param timestampsToSearch 分区和目标时间的映射。
     * @param requireTimestamps  如果代理不支持为偏移量获取精确时间戳，则为 true 时应使用 UnsupportedVersionException 使其失败。
     * @param listOffsetsRequestState ListOffsets 请求的状态对象，用于跟踪请求的进度和结果。
     * @return 一个 {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest} 列表，
     *         可以轮询这些请求以获取相应的时间戳和偏移量。
     */
    private List<NetworkClientDelegate.UnsentRequest> buildListOffsetsRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final boolean requireTimestamps,
            final ListOffsetsRequestState listOffsetsRequestState) {
        // 实现细节: 记录调试日志，说明正在为哪些分区构建 ListOffsets 请求。
        log.debug("Building ListOffsets request for partitions {}", timestampsToSearch);
        // 实现细节: 调用 groupListOffsetRequests 方法，根据目标节点对 ListOffsets 请求进行分组。
        // 将 listOffsetsRequestState 包装在 Optional 中传递，允许 groupListOffsetRequests 在找不到领导者时更新其 remainingToSearch。
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.of(listOffsetsRequestState));
        // 实现细节: 检查按节点分组后的请求是否为空。如果为空，说明没有找到任何分区的领导者，可能是元数据过时。
        if (timestampsToSearchByNode.isEmpty()) {
            // 实现细节: 抛出 StaleMetadataException，指示元数据需要更新。
            throw new StaleMetadataException();
        }

        // 实现细节: 初始化一个空的 ArrayList，用于存储将要发送的未发送请求。
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        // 实现细节: 创建一个 MultiNodeRequest 对象，用于管理发送到多个节点的请求。
        // 参数是目标节点的数量，即 timestampsToSearchByNode 的大小。
        MultiNodeRequest multiNodeRequest = new MultiNodeRequest(timestampsToSearchByNode.size());
        // 实现细节: 为 multiNodeRequest 设置完成时的回调逻辑。
        multiNodeRequest.onComplete((multiNodeResult, error) -> {
            // 实现细节: 已完成向一组已知领导者发送请求。
            // 实现细节: 检查是否有错误发生。
            if (error == null) {
                // 实现细节: 如果没有错误，将从多节点结果中获取到的偏移量添加到 listOffsetsRequestState 的 fetchedOffsets 中。
                listOffsetsRequestState.fetchedOffsets.putAll(multiNodeResult.fetchedOffsets);
                // 实现细节: 将多节点结果中需要重试的分区添加到 listOffsetsRequestState 的待重试分区列表中。
                listOffsetsRequestState.addPartitionsToRetry(multiNodeResult.partitionsToRetry);
                // 实现细节: 使用获取到的偏移量和隔离级别更新订阅状态。
                offsetFetcherUtils.updateSubscriptionState(multiNodeResult.fetchedOffsets,
                        isolationLevel);

                // 实现细节: 检查 listOffsetsRequestState 中是否还有剩余需要搜索的分区。
                if (listOffsetsRequestState.remainingToSearch.isEmpty()) {
                    // 实现细节: 如果没有剩余需要搜索的分区，说明所有请求都已成功或已标记为重试。
                    // 创建一个 ListOffsetResult 对象，包含已获取的偏移量和所有原始请求的分区。
                    ListOffsetResult listOffsetResult =
                            new ListOffsetResult(listOffsetsRequestState.fetchedOffsets,
                                    listOffsetsRequestState.remainingToSearch.keySet());
                    // 实现细节: 完成全局结果的 CompletableFuture，值为 listOffsetResult。
                    listOffsetsRequestState.globalResult.complete(listOffsetResult);
                } else {
                    // 实现细节: 如果仍有剩余需要搜索的分区（通常是那些找不到领导者的分区）。
                    // 将当前的 listOffsetsRequestState 添加到 requestsToRetry 集合中，以便在元数据更新后重试。
                    requestsToRetry.add(listOffsetsRequestState);
                    // 实现细节: 请求元数据更新，因为可能存在未知领导者的分区。
                    metadata.requestUpdate(false);
                }
            } else {
                // 实现细节: 如果在发送多节点请求过程中发生错误。
                // 记录调试日志，说明 ListOffsets 请求失败及错误信息。
                log.debug("ListOffsets request failed with error", error);
                // 实现细节: 以异常方式完成全局结果的 CompletableFuture。
                listOffsetsRequestState.globalResult.completeExceptionally(error);
            }
        });

        // 实现细节: 遍历按节点分组的 ListOffsets 请求。
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            // 实现细节: 获取当前条目的目标节点。
            Node node = entry.getKey();
            // 实现细节: 为当前节点构建 ListOffset 请求，并获取一个表示部分结果的 CompletableFuture。
            CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                    node, // 目标节点
                    entry.getValue(), // 发往该节点的分区和请求数据
                    requireTimestamps, // 是否要求精确时间戳
                    unsentRequests); // 用于收集构建的未发送请求的列表

            // 实现细节: 为部分结果的 CompletableFuture 注册一个完成时的回调。
            partialResult.whenComplete((result, error) -> {
                // 实现细节: 检查部分结果是否包含错误。
                if (error != null) {
                    // 实现细节: 如果有错误，则以该错误异常完成整个 multiNodeRequest 的结果 Future。
                    multiNodeRequest.resultFuture.completeExceptionally(error);
                } else {
                    // 实现细节: 如果没有错误，将部分结果添加到 multiNodeRequest 中。
                    multiNodeRequest.addPartialResult(result);
                }
            });
        }
        // 实现细节: 返回收集到的所有未发送的 ListOffsets 请求。
        return unsentRequests;
    }

    /**
     * 为指定的分区和目标时间戳构建发送到特定 broker 的 ListOffsets 请求。
     * 此方法还会将请求添加到 unsentRequests 列表中。
     * 应用场景: 当需要根据时间戳（例如最早、最晚或特定时间点）获取分区的偏移量时，会调用此方法为目标 broker 构建请求。
     * 实现细节: 使用 ListOffsetsRequest.Builder 构建请求，设置消费者标识、是否需要时间戳、隔离级别、目标时间戳和超时时间。
     *           然后将构建好的请求包装成 UnsentRequest，并添加到待发送列表。同时，创建一个 CompletableFuture 用于异步处理响应。
     * 设计考虑: 将请求构建和响应处理分离。通过 CompletableFuture 实现异步化，避免阻塞调用线程。
     *
     * @param node 目标 broker 节点
     * @param targetTimes 目标时间戳映射，键为 TopicPartition，值为 ListOffsetsPartition（包含目标时间戳等信息）
     * @param requireTimestamps 是否需要在响应中包含时间戳
     * @param unsentRequests 未发送请求的列表，新构建的请求将添加到此列表
     * @return 一个 CompletableFuture，当 ListOffsets 请求完成时，它将携带 ListOffsetResult
     */
    private CompletableFuture<ListOffsetResult> buildListOffsetRequestToNode(
            Node node, // 目标 broker 节点
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> targetTimes, // 目标时间戳映射
            boolean requireTimestamps, // 是否需要在响应中包含时间戳
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) { // 未发送请求的列表
        // 使用 ListOffsetsRequest.Builder 构建 ListOffsets 请求
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamps, isolationLevel) // 设置为消费者请求，并指定是否需要时间戳和隔离级别
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(targetTimes)) // 设置目标分区和时间戳
                .setTimeoutMs(requestTimeoutMs); // 设置请求超时时间

        // 记录调试日志，说明正在为哪个 broker 创建 ListOffset 请求以重置位置
        log.debug("Creating ListOffset request {} for broker {} to reset positions", builder,
                node);

        // 创建一个 NetworkClientDelegate.UnsentRequest 对象，包装 ListOffsetsRequest.Builder 和目标节点
        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder, // ListOffsets 请求构建器
                Optional.ofNullable(node)); // 目标节点，使用 Optional 包装以处理 null 情况
        // 将新创建的未发送请求添加到列表中
        unsentRequests.add(unsentRequest);
        // 创建一个 CompletableFuture 用于异步接收 ListOffset 请求的结果
        CompletableFuture<ListOffsetResult> result = new CompletableFuture<>();
        // 为未发送的请求设置完成时的回调逻辑
        unsentRequest.whenComplete((response, error) -> {
            // 检查请求是否出错
            if (error != null) {
                // 如果发生错误，记录调试日志
                log.debug("Sending ListOffset request {} to broker {} failed",
                        builder, // 原始请求构建器
                        node,    // 目标节点
                        error);  // 发生的错误
                // 以异常方式完成 CompletableFuture
                result.completeExceptionally(error);
            } else {
                // 如果请求成功，将响应体转换为 ListOffsetsResponse
                ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                // 记录追踪日志，说明从 broker 收到了 ListOffsetResponse
                log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                try {
                    // 调用 offsetFetcherUtils 处理 ListOffsetsResponse
                    ListOffsetResult listOffsetResult = offsetFetcherUtils.handleListOffsetResponse(lor);
                    // 以正常方式完成 CompletableFuture，并设置结果
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    // 如果在处理响应时发生运行时异常，则以异常方式完成 CompletableFuture
                    result.completeExceptionally(e);
                }
            }
        });
        // 返回 CompletableFuture，调用者可以通过它异步获取结果
        return result;
    }

    /**
     * 异步发送 ListOffsets 请求，为指定分区按目标时间获取偏移量。
     * 使用检索到的偏移量重置订阅状态中的位置。
     * 此方法还会将请求添加到 unsentRequests 列表中。
     * 应用场景: 当消费者启动或发生再均衡后，需要根据配置的自动偏移量重置策略（如 earliest, latest）来初始化分区的消费位置时调用。
     * 实现细节:
     * 1. 从 partitionAutoOffsetResetStrategyMap 中提取每个分区需要查询的时间戳。
     * 2. 将这些按时间戳查询的请求按目标 broker 节点进行分组。
     * 3. 为每个节点的请求构建 ListOffsets 请求（调用 buildListOffsetRequestToNode）。
     * 4. 异步处理每个请求的响应：成功则更新订阅状态中的位置；失败则记录错误并处理。
     * 5. 使用一个全局的 CompletableFuture (globalResult) 来跟踪所有请求的完成状态。
     * 设计考虑: 批量处理和异步化是核心。通过将请求按 broker 分组，可以减少网络交互次数。
     *           使用 CompletableFuture 和 AtomicInteger 来管理多个异步操作的完成状态，确保所有操作完成后再通知调用者。
     *
     * @param partitionAutoOffsetResetStrategyMap 分区与自动偏移量重置策略的映射
     * @return 一个 {@link CompletableFuture}，当所有请求都完成时，它将完成。
     */
    private CompletableFuture<Void> sendListOffsetsRequestsAndResetPositions(
            final Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap) { // 分区到自动偏移重置策略的映射
        // 从 partitionAutoOffsetResetStrategyMap 中提取每个分区需要搜索的时间戳
        Map<TopicPartition, Long> timestampsToSearch = partitionAutoOffsetResetStrategyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().timestamp().get()));
        // 将按时间戳搜索的请求按目标 broker 节点进行分组
        // Optional.empty() 表示不需要特定的 leader epoch
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.empty());

        // 原子整数，用于跟踪预期收到的响应数量
        final AtomicInteger expectedResponses = new AtomicInteger(0);
        // 全局的 CompletableFuture，用于表示所有 ListOffsets 请求的完成状态
        final CompletableFuture<Void> globalResult = new CompletableFuture<>();
        // 存储待发送的 ListOffsets 请求列表
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

        // 遍历按节点分组的时间戳搜索请求
        timestampsToSearchByNode.forEach((node, resetTimestamps) -> {
            // 为这些分区设置下一次允许重试的时间，防止短时间内频繁重试
            subscriptionState.setNextAllowedRetry(resetTimestamps.keySet(),
                    time.milliseconds() + requestTimeoutMs);

            // 为当前节点的这些分区构建 ListOffsets 请求
            // 第三个参数 false 表示在重置位置时，通常不需要在响应中包含时间戳
            CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                    node, // 目标 broker 节点
                    resetTimestamps, // 当前节点需要重置时间戳的分区和对应的时间戳信息
                    false, // 不需要时间戳
                    unsentRequests); // 将构建的请求添加到 unsentRequests 列表

            // 为部分结果（单个节点的 ListOffsets 请求）设置完成时的回调
            partialResult.whenComplete((result, error) -> {
                // 检查请求是否成功
                if (error == null) {
                    // 如果成功，调用 offsetFetcherUtils 处理成功的响应以重置位置
                    offsetFetcherUtils.onSuccessfulResponseForResettingPositions(result,
                            partitionAutoOffsetResetStrategyMap);
                } else {
                    // 如果失败，构造一个 RuntimeException
                    RuntimeException e;
                    if (error instanceof RuntimeException) {
                        e = (RuntimeException) error;
                    } else {
                        e = new RuntimeException("Unexpected failure in ListOffsets request for " +
                                "resetting positions", error);
                    }
                    // 调用 offsetFetcherUtils 处理失败的响应以重置位置
                    offsetFetcherUtils.onFailedResponseForResettingPositions(resetTimestamps, e);
                }
                // 预期响应数量减一，如果减到 0，表示所有请求都已处理完毕
                if (expectedResponses.decrementAndGet() == 0) {
                    // 完成全局的 CompletableFuture
                    globalResult.complete(null);
                }
            });
        });

        // 检查是否有未发送的请求
        if (unsentRequests.isEmpty()) {
            // 如果没有未发送的请求（例如，没有分区需要重置位置），则直接完成全局 CompletableFuture
            globalResult.complete(null);
        } else {
            // 如果有未发送的请求，设置预期响应数量为未发送请求的数量
            expectedResponses.set(unsentRequests.size());
            // 将所有未发送的请求添加到 requestsToSend 队列中，等待网络客户端发送
            requestsToSend.addAll(unsentRequests);
        }

        // 返回全局的 CompletableFuture，调用者可以据此判断所有重置位置的操作是否完成
        return globalResult;
    }

    /**
     * 为每个需要验证的分区，异步发送请求以获取该分区的末端偏移量，
     * 该末端偏移量对应的 epoch 小于或等于该分区最后一次看到的 epoch。
     * <p/>
     * 为了提高效率，请求按节点分组。
     * 此方法还会将请求添加到 unsentRequests 列表中。
     *
     * @param partitionsToValidate 一个 TopicPartition 到 SubscriptionState.FetchPosition 的映射，表示需要验证的分区及其位置信息。
     *                             应用场景：当消费者感知到分区 leader 发生变化或需要确认当前消费位置的有效性时，会调用此方法。
     *                             设计考虑：此方法通过异步方式发送请求，避免阻塞主线程。请求按节点分组可以减少网络连接数，提高效率。
     *                                      使用 CompletableFuture 处理异步结果，便于后续的逻辑处理。
     */
    private void sendOffsetsForLeaderEpochRequestsAndValidatePositions(
            Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate) {

        // 将需要验证的拉取位置按其领导者节点重新分组
        final Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
                regroupFetchPositionsByLeader(partitionsToValidate);

        // 计算下一次允许重试的时间，即当前时间加上请求超时时间
        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        // 创建一个列表用于存储未发送的请求
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        // 遍历按节点分组后的拉取位置信息
        regrouped.forEach((node, fetchPositions) -> {

            // 如果节点信息为空（例如，找不到领导者），则请求元数据更新并返回
            if (node.isEmpty()) {
                metadata.requestUpdate(true); // 请求元数据更新
                return; // 结束当前节点的处理
            }

            // 获取节点的 API 版本信息
            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            // 如果获取不到节点的 API 版本信息，则尝试连接该节点并返回
            if (nodeApiVersions == null) {
                networkClientDelegate.tryConnect(node); // 尝试连接节点
                return; // 结束当前节点的处理
            }

            // 检查节点是否支持可用的 OffsetsForLeaderEpoch API 版本
            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                // 如果 Broker 不支持所需的协议版本（Kafka 2.3 引入），则记录调试信息并跳过验证
                log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
                                "support the required protocol version (introduced in Kafka 2.3)",
                        fetchPositions.keySet()); // 记录调试日志，说明跳过验证的原因
                // 对于这些分区，直接标记为验证完成
                for (TopicPartition partition : fetchPositions.keySet()) {
                    subscriptionState.completeValidation(partition); // 标记分区验证完成
                }
                return; // 结束当前节点的处理
            }

            // 为这些分区设置下一次允许重试的时间
            subscriptionState.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            // 为当前节点构建 OffsetsForLeaderEpoch 请求，并获取一个 CompletableFuture 用于处理结果
            CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> partialResult =
                    buildOffsetsForLeaderEpochRequestToNode(node, fetchPositions, unsentRequests);

            // 当请求完成时（无论成功或失败），执行回调逻辑
            partialResult.whenComplete((offsetsResult, error) -> {
                // 如果没有错误，表示请求成功
                if (error == null) {
                    // 调用 offsetFetcherUtils 处理成功响应，验证位置
                    offsetFetcherUtils.onSuccessfulResponseForValidatingPositions(fetchPositions,
                            offsetsResult);
                } else {
                    // 如果发生错误
                    RuntimeException e;
                    // 判断错误类型，如果是 RuntimeException，则直接使用
                    if (error instanceof RuntimeException) {
                        e = (RuntimeException) error;
                    } else {
                        // 否则，包装成新的 RuntimeException
                        e = new RuntimeException("Unexpected failure in OffsetsForLeaderEpoch " +
                                "request for validating positions", error);
                    }
                    // 调用 offsetFetcherUtils 处理失败响应
                    offsetFetcherUtils.onFailedResponseForValidatingPositions(fetchPositions, e);
                }
            });
        });

        // 将所有构建的未发送请求添加到待发送队列中
        requestsToSend.addAll(unsentRequests);
    }

    /**
     * 构建要发送到特定 Broker 的 OffsetsForLeaderEpoch 请求，用于获取指定分区和位置的偏移量。
     * 此方法还会将构建的请求添加到 unsentRequests 列表中。
     *
     * @param node 目标 Broker 节点。
     * @param fetchPositions 一个 TopicPartition 到 SubscriptionState.FetchPosition 的映射，表示要获取偏移量的分区及其位置信息。
     * @param unsentRequests 一个列表，用于收集构建好的但尚未发送的请求。
     * @return 一个 CompletableFuture，表示异步获取 OffsetsForLeaderEpochUtils.OffsetForEpochResult 的结果。
     *         应用场景：当需要向特定 Broker 查询一组分区的 Leader Epoch 对应的偏移量时调用。
     *         设计考虑：此方法封装了 OffsetsForLeaderEpoch 请求的构建和发送逻辑，返回 CompletableFuture 以支持异步处理。
     *                   通过将请求添加到外部传入的 unsentRequests 列表，实现了请求的批量管理和发送。
     */
    private CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> buildOffsetsForLeaderEpochRequestToNode(
            final Node node, // 目标 Broker 节点
            final Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions, // 需要获取偏移量的分区及其位置信息
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) { // 用于收集未发送请求的列表
        // 使用 OffsetFetcherUtils 准备 OffsetsForLeaderEpoch 请求的构建器
        AbstractRequest.Builder<OffsetsForLeaderEpochRequest> builder =
                OffsetsForLeaderEpochUtils.prepareRequest(fetchPositions);

        // 记录调试日志，说明正在创建 OffsetsForLeaderEpoch 请求
        log.debug("Creating OffsetsForLeaderEpoch request request {} to broker {}", builder, node);

        // 创建一个 NetworkClientDelegate.UnsentRequest 对象，封装请求构建器和目标节点
        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder, // 请求构建器
                Optional.ofNullable(node)); // 目标节点，使用 Optional 包装以处理 node 可能为 null 的情况
        // 将创建的未发送请求添加到列表中
        unsentRequests.add(unsentRequest);
        // 创建一个 CompletableFuture 用于异步返回结果
        CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> result = new CompletableFuture<>();
        // 当请求完成时（无论成功或失败），执行回调逻辑
        unsentRequest.whenComplete((response, error) -> {
            // 如果发生错误
            if (error != null) {
                // 记录调试日志，说明发送 OffsetsForLeaderEpoch 请求失败
                log.debug("Sending OffsetsForLeaderEpoch request {} to broker {} failed",
                        builder, // 请求构建器
                        node,    // 目标节点
                        error);  // 发生的错误
                // 以异常方式完成 CompletableFuture
                result.completeExceptionally(error);
            } else {
                // 如果请求成功，获取响应体
                OffsetsForLeaderEpochResponse offsetsForLeaderEpochResponse = (OffsetsForLeaderEpochResponse) response.responseBody();
                // 记录追踪日志，说明收到了 OffsetsForLeaderEpoch 响应
                log.trace("Received OffsetsForLeaderEpoch response {} from broker {}", offsetsForLeaderEpochResponse, node);
                try {
                    // 使用 OffsetFetcherUtils 处理响应，将响应转换为 OffsetForEpochResult
                    OffsetsForLeaderEpochUtils.OffsetForEpochResult listOffsetResult =
                            OffsetsForLeaderEpochUtils.handleResponse(fetchPositions, offsetsForLeaderEpochResponse);
                    // 以正常方式完成 CompletableFuture，并设置结果
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    // 如果在处理响应过程中发生运行时异常，则以异常方式完成 CompletableFuture
                    result.completeExceptionally(e);
                }
            }
        });
        // 返回 CompletableFuture 对象
        return result;
    }

    /**
     * 表示 ListOffsets 请求的状态。此类封装了与单个 ListOffsets 请求相关的所有信息，
     * 包括要搜索的时间戳、已获取的偏移量、剩余需要搜索的分区以及一个表示全局结果的 CompletableFuture。
     * 应用场景: 当消费者需要根据时间戳查找偏移量时（例如，调用 {@link OffsetsRequestManager#fetchOffsets(Map, boolean)}），
     *           会创建此类的实例来跟踪请求的生命周期和结果。
     * 设计考虑: 将与 ListOffsets 请求相关的状态聚合到一个类中，有助于管理复杂性，
     *           特别是当请求可能需要分批发送或处理重试时。使用 CompletableFuture 简化了异步结果的处理。
     */
    private static class ListOffsetsRequestState {

        // 要为其查找偏移量的分区到时间戳的映射。
        // 应用场景: 存储了最初请求查找偏移量的所有分区及其对应的时间戳。
        // 设计考虑: 使用 final 确保此映射在对象创建后不可变，代表了请求的原始输入。
        private final Map<TopicPartition, Long> timestampsToSearch;
        // 已成功获取到的分区到偏移量数据的映射。
        // 应用场景: 存储了已经成功从 Broker 获取到的偏移量信息。
        // 设计考虑: 这是一个累积结果的容器，随着请求的进行（可能分批）而填充。
        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        // 仍然需要为其查找偏移量的分区到时间戳的映射（例如，由于元数据不可用或需要重试）。
        // 应用场景: 跟踪那些因为各种原因（如 Leader 不可用）而未能立即获取偏移量的分区。
        // 设计考虑: 用于管理需要后续处理或重试的分区，确保所有请求最终都被处理。
        private final Map<TopicPartition, Long> remainingToSearch;
        // 代表整个 ListOffsets 请求最终结果的 CompletableFuture。
        // 应用场景: 调用者可以通过此 Future 异步获取所有请求分区的最终偏移量查找结果。
        // 设计考虑: 提供了一个统一的异步完成点，即使内部请求可能被拆分或重试。
        private final CompletableFuture<ListOffsetResult> globalResult;
        // 指示是否需要精确的时间戳。如果为 true，并且 Broker 不支持，则可能会抛出异常。
        // 应用场景: 控制 ListOffsets 请求的行为，特别是在与旧版本 Broker 交互时。
        // 设计考虑: 允许调用者指定对时间戳精确度的要求。
        final boolean requireTimestamps;
        // OffsetFetcher 工具类实例，提供偏移量获取相关的辅助方法。
        // 应用场景: 用于执行实际的偏移量获取逻辑或处理响应。
        // 设计考虑: 依赖注入，将具体的获取逻辑委托给 OffsetFetcherUtils。
        final OffsetFetcherUtils offsetFetcherUtils;
        // 事务隔离级别，用于 ListOffsets 请求。
        // 应用场景: 决定消费者可以读取哪些偏移量（例如，只读已提交的）。
        // 设计考虑: 确保偏移量查找符合用户配置的隔离级别。
        final IsolationLevel isolationLevel;


        /**
         * ListOffsetsRequestState 的构造函数。
         * 应用场景: 在发起新的 ListOffsets 请求时创建此对象。
         * 实现细节: 初始化所有 final 字段，并将 remainingToSearch 和 fetchedOffsets 初始化为空的 HashMap，
         *           同时创建一个新的 CompletableFuture 作为 globalResult。
         * 设计考虑: 构造函数接收所有必要的依赖和配置，确保对象在创建时处于一致状态。
         *
         * @param timestampsToSearch 要为其查找偏移量的分区到时间戳的映射。
         * @param requireTimestamps 是否需要精确的时间戳。
         * @param offsetFetcherUtils OffsetFetcher 工具类实例。
         * @param isolationLevel 事务隔离级别。
         */
        private ListOffsetsRequestState(Map<TopicPartition, Long> timestampsToSearch,
                                        boolean requireTimestamps,
                                        OffsetFetcherUtils offsetFetcherUtils,
                                        IsolationLevel isolationLevel) {
            // 实现细节: 初始化 remainingToSearch 为一个新的 HashMap，用于存储待处理的分区。
            remainingToSearch = new HashMap<>();
            // 实现细节: 初始化 fetchedOffsets 为一个新的 HashMap，用于存储已获取的偏移量。
            fetchedOffsets = new HashMap<>();
            // 实现细节: 初始化 globalResult 为一个新的 CompletableFuture，用于异步返回最终结果。
            globalResult = new CompletableFuture<>();

            // 实现细节: 设置要搜索的时间戳映射。
            this.timestampsToSearch = timestampsToSearch;
            // 实现细节: 设置是否需要精确时间戳的标志。
            this.requireTimestamps = requireTimestamps;
            // 实现细节: 设置 OffsetFetcherUtils 实例。
            this.offsetFetcherUtils = offsetFetcherUtils;
            // 实现细节: 设置隔离级别。
            this.isolationLevel = isolationLevel;
        }

        private void addPartitionsToRetry(Set<TopicPartition> partitionsToRetry) {
            // 实现细节: 将需要重试的分区及其对应的时间戳（从原始 timestampsToSearch 获取）添加到 remainingToSearch 映射中。
            // 使用 Stream API 将 Set<TopicPartition> 转换为 Map<TopicPartition, Long>。
            remainingToSearch.putAll(partitionsToRetry.stream()
                    .collect(Collectors.toMap(tp -> tp, timestampsToSearch::get)));
        }
    }

    /**
     * 表示一个发送到多个节点的请求的聚合状态。
     * 当一个 ListOffsets 请求需要发送到多个 Broker 时（因为分区分布在不同节点上），
     * 此类用于跟踪来自所有这些 Broker 的响应，并在所有响应都收到后合并结果。
     * 应用场景: 在 {@link OffsetsRequestManager#prepareFetchOffsetsRequests(Map, boolean, ListOffsetsRequestState)} 中，
     *           当 ListOffsets 请求按 Leader 节点分组后，如果涉及多个节点，则会为整个操作创建一个 MultiNodeRequest 实例，
     *           并为发送到每个节点的子请求创建单独的 {@link NetworkClientDelegate.UnsentRequest}。
     * 设计考虑: 此类通过 AtomicInteger 跟踪预期响应的数量，并使用 CompletableFuture 来异步通知整体操作的完成。
     *           它简化了对分布式请求结果的聚合逻辑。
     */
    private static class MultiNodeRequest {
        // 从所有节点成功获取到的分区到偏移量数据的映射。
        // 应用场景: 存储从各个 Broker 成功返回的偏移量信息。
        // 设计考虑: 这是一个累积结果的容器，随着各个子请求的完成而填充。
        final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets;
        // 从所有节点收集到的需要重试的分区集合。
        // 应用场景: 跟踪那些在任何一个 Broker 上未能成功获取偏移量且需要重试的分区。
        // 设计考虑: 聚合所有子请求中需要重试的分区。
        final Set<TopicPartition> partitionsToRetry;
        // 期望收到的响应数量的原子计数器。
        // 应用场景: 用于跟踪还有多少个发往不同 Broker 的子请求尚未收到响应。
        // 设计考虑: 使用 AtomicInteger 保证线程安全地递减计数器。
        final AtomicInteger expectedResponses;
        // 代表此多节点请求最终结果的 CompletableFuture。
        // 应用场景: 调用者可以通过此 Future 异步获取所有涉及节点请求的聚合结果。
        // 设计考虑: 提供一个统一的异步完成点，当所有子请求都处理完毕后完成。
        final CompletableFuture<ListOffsetResult> resultFuture;

        /**
         * MultiNodeRequest 的构造函数。
         * 应用场景: 当一个操作需要向多个节点发送请求时，创建此对象来跟踪整体进度和结果。
         * 实现细节: 初始化 fetchedTimestampOffsets 和 partitionsToRetry 为空集合，
         *           将 expectedResponses 初始化为指定的节点数量 (nodeCount)，
         *           并创建一个新的 CompletableFuture 作为 resultFuture。
         * 设计考虑: 构造函数接收预期的节点数量，这是确定请求何时完成的关键。
         *
         * @param nodeCount 预期将向其发送请求的节点数量。
         */
        private MultiNodeRequest(int nodeCount) {
            // 实现细节: 初始化 fetchedTimestampOffsets 为一个新的 HashMap，用于存储从各节点获取的偏移量。
            fetchedTimestampOffsets = new HashMap<>();
            // 实现细节: 初始化 partitionsToRetry 为一个新的 HashSet，用于存储需要重试的分区。
            partitionsToRetry = new HashSet<>();
            // 实现细节: 初始化 expectedResponses 为一个原子整数，其初始值为 nodeCount。
            expectedResponses = new AtomicInteger(nodeCount);
            // 实现细节: 初始化 resultFuture 为一个新的 CompletableFuture，用于异步返回聚合结果。
            resultFuture = new CompletableFuture<>();
        }
        

        /**
         * 为此多节点请求的完成注册一个回调操作。
         * 应用场景: 调用者可以使用此方法来指定当所有节点的响应都已处理完毕（无论成功或失败）时要执行的逻辑。
         * 实现细节: 将提供的 BiConsumer action 注册到 resultFuture 的 whenComplete 方法上。
         * 设计考虑: 利用 CompletableFuture 的回调机制，使调用者能够以非阻塞方式处理最终结果或异常。
         *
         * @param action 当请求完成或出现异常时要执行的操作。
         */
        private void onComplete(BiConsumer<? super ListOffsetResult, ? super Throwable> action) {
            // 实现细节: 在 resultFuture 完成时（成功或异常），执行提供的 action。
            resultFuture.whenComplete(action);
        }

        /**
         * 添加来自单个节点的局部结果。
         * 应用场景: 当从一个 Broker 收到 ListOffsets 响应并处理后，调用此方法将该 Broker 的结果合并到整体 MultiNodeRequest 中。
         * 实现细节: 将局部结果中的 fetchedOffsets 添加到 fetchedTimestampOffsets，
         *           将局部结果中的 partitionsToRetry 添加到 partitionsToRetry。
         *           然后递减 expectedResponses 计数器。如果计数器达到零，表示所有节点的响应都已收到，
         *           此时会创建一个包含所有累积结果的 ListOffsetResult，并用它来完成 resultFuture。
         *           如果在处理过程中发生运行时异常，则用该异常来完成 resultFuture。
         * 设计考虑: 线程安全地聚合来自多个异步操作的结果。当所有部分都完成后，触发整体完成。
         *
         * @param partialResult 来自单个节点的 ListOffsets 请求的部分结果。
         */
        private void addPartialResult(ListOffsetResult partialResult) {
            try {
                // 实现细节: 将部分结果中获取到的偏移量合并到全局的 fetchedTimestampOffsets 中。
                fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                // 实现细节: 将部分结果中需要重试的分区合并到全局的 partitionsToRetry 中。
                partitionsToRetry.addAll(partialResult.partitionsToRetry);

                // 实现细节: 递减预期响应计数器，如果减至0，则表示所有节点的响应都已收到。
                if (expectedResponses.decrementAndGet() == 0) {
                    // 实现细节: 创建最终的 ListOffsetResult，包含所有已获取的偏移量和所有需要重试的分区。
                    ListOffsetResult result =
                            new ListOffsetResult(fetchedTimestampOffsets,
                                    partitionsToRetry);
                    // 实现细节: 使用最终结果完成 resultFuture。
                    resultFuture.complete(result);
                }
            } catch (RuntimeException e) {
                // 实现细节: 如果在合并结果过程中发生运行时异常，则用该异常使 resultFuture 异常完成。
                resultFuture.completeExceptionally(e);
            }
        }
    }

    /**
     * 按 Leader 对分区进行分组。对于 `timestampsToSearch` 中 Leader 未知的主题分区，
     * 它们将被保留在 `listOffsetsRequestState` 的 `remainingToSearch` 中。
     * 应用场景: 在准备发送 ListOffsets 请求之前，需要确定每个分区应该向哪个 Leader Broker 发送请求。
     *           此方法负责查询元数据以获取 Leader 信息，并相应地组织请求数据。
     * 实现细节: 遍历 `timestampsToSearch` 中的每个分区：
     *           1. 尝试从 `metadata` 获取当前 Leader 和 Epoch。
     *           2. 如果 Leader 未知，记录调试信息，请求元数据更新，并将该分区及其时间戳添加到 `listOffsetsRequestState` (如果存在) 的 `remainingToSearch` 映射中。
     *           3. 如果 Leader 已知，创建一个 `ListOffsetsRequestData.ListOffsetsPartition` 对象，包含分区索引、目标时间戳和当前 Leader Epoch，并将其添加到 `partitionDataMap` 中。
     *           4. 最后，使用 `offsetFetcherUtils.regroupPartitionMapByNode` 将 `partitionDataMap` 按 Leader 节点重新分组。
     * 设计考虑: 此方法处理了 Leader 未知的情况，确保不会向错误的节点发送请求，并通过请求元数据更新来尝试解决此问题。
     *           将请求按节点分组是为了优化网络通信，可以将发往同一节点的所有分区请求合并为一个批次。
     *
     * @param timestampsToSearch      分区到目标时间戳的映射。
     * @param listOffsetsRequestState 可选的请求状态对象，如果提供，其 `remainingToSearch` 映射将被扩展，
     *                                以包含所有由于 Leader 未知（需要元数据更新）而无法执行请求的分区。
     * @return 按 Leader 节点分组的请求数据映射，键是 Leader 节点，值是该节点负责的分区及其对应的请求数据。
     */
    private Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> groupListOffsetRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final Optional<ListOffsetsRequestState> listOffsetsRequestState) {
        // 实现细节: 创建一个 HashMap 用于存储 TopicPartition 到 ListOffsetsPartition 请求数据的映射。
        final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> partitionDataMap = new HashMap<>();
        // 实现细节: 遍历传入的 timestampsToSearch 映射中的每一个条目。
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // 实现细节: 获取当前条目的 TopicPartition。
            TopicPartition tp = entry.getKey();
            // 实现细节: 获取当前条目的目标时间戳。
            Long offset = entry.getValue();
            // 实现细节: 从元数据中获取当前 TopicPartition 的 Leader 和 Epoch 信息。
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            // 实现细节: 检查 Leader 是否未知 (leader.isEmpty() 表示 Leader 未找到)。
            if (leaderAndEpoch.leader.isEmpty()) {
                // 实现细节: 如果 Leader 未知，记录调试日志。
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                // 实现细节: 请求元数据更新，因为 Leader 信息缺失。
                metadata.requestUpdate(true);
                // 实现细节: 如果提供了 listOffsetsRequestState，则将此分区和时间戳添加到其 remainingToSearch 映射中。
                listOffsetsRequestState.ifPresent(offsetsRequestState -> offsetsRequestState.remainingToSearch.put(tp, offset));
            } else {
                // 实现细节: 如果 Leader 已知，获取当前的 Leader Epoch。如果 Epoch 不存在，则使用 UNKNOWN_EPOCH。
                int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                // 实现细节: 创建一个新的 ListOffsetsPartition 请求数据对象，并设置分区索引、时间戳和当前 Leader Epoch。
                partitionDataMap.put(tp, new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(tp.partition())
                        .setTimestamp(offset)
                        .setCurrentLeaderEpoch(currentLeaderEpoch));
            }
        }
        // 实现细节: 使用 OffsetFetcherUtils 工具类将 partitionDataMap 按 Leader 节点重新分组，并返回结果。
        return offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap);
    }

    // 仅用于测试
    // 应用场景: 在单元测试中，用于验证 OffsetsRequestManager 内部待重试请求的数量是否符合预期。
    // 实现细节: 返回内部 requestsToRetry 集合的大小。
    // 设计考虑: 提供一个访问内部状态的方法，以便进行白盒测试，但将其可见性限制在包内或通过注释标记为测试专用。
    int requestsToRetry() {
        // 实现细节: 返回 requestsToRetry 集合中元素的数量。
        return requestsToRetry.size();
    }

    // 仅用于测试
    // 应用场景: 在单元测试中，用于验证 OffsetsRequestManager 内部准备发送的请求数量是否符合预期。
    // 实现细节: 返回内部 requestsToSend 列表的大小。
    // 设计考虑: 类似于 requestsToRetry()，提供测试所需的内部状态访问。
    int requestsToSend() {
        // 实现细节: 返回 requestsToSend 列表中元素的数量。
        return requestsToSend.size();
    }
}
