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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.metrics.OffsetCommitMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;
import static org.apache.kafka.common.protocol.Errors.COORDINATOR_LOAD_IN_PROGRESS;

/**
 * CommitRequestManager 类负责管理消费者的位移提交请求。
 * 它实现了 RequestManager 接口，用于处理网络请求的生命周期，
 * 同时实现了 MemberStateListener 接口，用于响应消费者组成员状态的变化。
 * 这个类是消费者客户端正确提交和获取位移的关键组件，确保了消费进度的持久化和一致性。
 * 设计考虑：将位移提交逻辑封装在此类中，可以更好地组织代码，分离关注点，并方便进行测试和维护。
 * 应用场景：在Kafka消费者客户端中，每当需要提交消费位移（手动或自动）或获取已提交位移时，都会涉及到此类。
 */
public class CommitRequestManager implements RequestManager, MemberStateListener {
    // Kafka 客户端的时间工具，用于获取当前时间戳，对于超时控制、重试间隔等非常重要。
    private final Time time;
    // 消费者的订阅状态，维护了消费者订阅的主题、分区以及这些分区的消费位移等信息。
    private final SubscriptionState subscriptions;
    // 消费者元数据，包含了集群的元数据信息，例如 broker 列表、主题分区信息等。
    private final ConsumerMetadata metadata;
    // 日志上下文，用于创建和管理日志记录器。
    private final LogContext logContext;
    // SLF4J 日志记录器实例，用于记录此类操作的日志。
    private final Logger log;
    // 自动提交状态，如果启用了自动提交，则此 Optional 包含 AutoCommitState 实例，否则为空。
    private final Optional<AutoCommitState> autoCommitState;
    // 协调器请求管理器，用于查找和管理与消费者协调器的连接。
    private final CoordinatorRequestManager coordinatorRequestManager;
    // 位移提交回调调用器，用于在位移提交完成后异步调用用户提供的回调函数。
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    // 位移提交相关的指标管理器，用于收集和报告位移提交的性能指标。
    private final OffsetCommitMetricsManager metricsManager;
    // 重试提交请求时的初始退避时间（毫秒）。
    private final long retryBackoffMs;
    // 消费者组的ID。
    private final String groupId;
    // 消费者实例的ID，用于静态成员资格。
    private final Optional<String> groupInstanceId;
    // 重试提交请求时的最大退避时间（毫秒）。
    private final long retryBackoffMaxMs;
    // 仅用于测试：用于在重试退避中添加随机抖动，以避免惊群效应。
    private final OptionalDouble jitter;
    // 配置项，指示当 broker 不支持获取稳定位移时是否抛出异常。
    private final boolean throwOnFetchStableOffsetUnsupported;
    // 存储待处理的位移提交和获取请求的集合。
    final PendingRequests pendingRequests;
    // 标志位，指示 CommitRequestManager 是否正在关闭过程中。
    private boolean closing = false;

    /**
     * 上次提交请求中发送的成员 epoch。如果上次请求中未包含 epoch，则为空。
     * 用于日志记录。
     */
    private Optional<Integer> lastEpochSentOnCommit;

    /**
     *  通过 {@link MemberStateListener#onMemberEpochUpdated(Optional, String)} 接收到的成员ID和最新的成员 epoch，
     *  将包含在 OffsetFetch 和 OffsetCommit 请求中。这将拥有从 broker 接收到的最新 memberEpoch。
     */
    private final MemberInfo memberInfo;

    /**
     * CommitRequestManager 的公共构造函数。
     * 这是主要的构造函数，用于在生产环境中创建 CommitRequestManager 实例。
     * 它会从 ConsumerConfig 中读取必要的配置，并初始化所有依赖项。
     * 设计考虑：提供一个简洁的公共构造函数，隐藏内部实现的复杂性。
     *
     * @param time 时间工具，用于获取当前时间等。
     * @param logContext 日志上下文。
     * @param subscriptions 消费者的订阅状态。
     * @param config 消费者配置。
     * @param coordinatorRequestManager 协调器请求管理器。
     * @param offsetCommitCallbackInvoker 位移提交回调调用器。
     * @param groupId 消费者组ID。
     * @param groupInstanceId 消费者实例ID (可选)。
     * @param metrics Kafka 指标收集器。
     * @param metadata 消费者元数据。
     */
    public CommitRequestManager(
        final Time time, // 传入时间工具实例
        final LogContext logContext, // 传入日志上下文
        final SubscriptionState subscriptions, // 传入订阅状态
        final ConsumerConfig config, // 传入消费者配置
        final CoordinatorRequestManager coordinatorRequestManager, // 传入协调器请求管理器
        final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker, // 传入位移提交回调调用器
        final String groupId, // 传入消费者组ID
        final Optional<String> groupInstanceId, // 传入消费者实例ID
        final Metrics metrics, // 传入指标收集器
        final ConsumerMetadata metadata // 传入消费者元数据
    ) {
        // 调用另一个构造函数，传入从配置中获取的重试退避参数和空的 jitter
        this(time,
            logContext,
            subscriptions,
            config,
            coordinatorRequestManager,
            offsetCommitCallbackInvoker,
            groupId,
            groupInstanceId,
            config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG), // 从配置中获取重试退避时间
            config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG), // 从配置中获取最大重试退避时间
            OptionalDouble.empty(), // 传入空的 jitter
            metrics,
            metadata);
    }

    // 仅用于测试的构造函数，允许更灵活地控制内部状态和依赖项。
    // 设计考虑：提供一个测试专用的构造函数，可以方便地注入 mock 对象或自定义参数，从而简化单元测试的编写。
    CommitRequestManager(
        final Time time, // 传入时间工具实例
        final LogContext logContext, // 传入日志上下文
        final SubscriptionState subscriptions, // 传入订阅状态
        final ConsumerConfig config, // 传入消费者配置
        final CoordinatorRequestManager coordinatorRequestManager, // 传入协调器请求管理器
        final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker, // 传入位移提交回调调用器
        final String groupId, // 传入消费者组ID
        final Optional<String> groupInstanceId, // 传入消费者实例ID
        final long retryBackoffMs, // 传入重试退避时间
        final long retryBackoffMaxMs, // 传入最大重试退避时间
        final OptionalDouble jitter, // 传入 jitter 值
        final Metrics metrics, // 传入指标收集器
        final ConsumerMetadata metadata // 传入消费者元数据
    ) {
        // 检查 coordinatorRequestManager 是否为 null，因为提交位移时需要协调器
        Objects.requireNonNull(coordinatorRequestManager, "Coordinator is needed upon committing offsets");
        // 初始化时间工具
        this.time = time;
        // 初始化日志上下文
        this.logContext = logContext;
        // 初始化日志记录器，使用当前类的名称
        this.log = logContext.logger(getClass());
        // 初始化待处理请求集合
        this.pendingRequests = new PendingRequests();
        // 检查是否启用了自动提交位移
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            // 如果启用了自动提交，获取自动提交间隔
            final long autoCommitInterval =
                Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            // 创建并初始化 AutoCommitState 实例
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval, logContext));
        } else {
            // 如果未启用自动提交，将 autoCommitState 设置为空
            this.autoCommitState = Optional.empty();
        }
        // 初始化协调器请求管理器
        this.coordinatorRequestManager = coordinatorRequestManager;
        // 初始化消费者组ID
        this.groupId = groupId;
        // 初始化消费者实例ID
        this.groupInstanceId = groupInstanceId;
        // 初始化订阅状态
        this.subscriptions = subscriptions;
        // 初始化消费者元数据
        this.metadata = metadata;
        // 初始化重试退避时间
        this.retryBackoffMs = retryBackoffMs;
        // 初始化最大重试退避时间
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        // 初始化 jitter
        this.jitter = jitter;
        // 初始化当 broker 不支持获取稳定位移时是否抛出异常的标志
        this.throwOnFetchStableOffsetUnsupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        // 初始化成员信息对象
        this.memberInfo = new MemberInfo();
        // 初始化位移提交指标管理器
        this.metricsManager = new OffsetCommitMetricsManager(metrics);
        // 初始化位移提交回调调用器
        this.offsetCommitCallbackInvoker = offsetCommitCallbackInvoker;
        // 初始化上次提交请求中发送的成员 epoch 为空
        this.lastEpochSentOnCommit = Optional.empty();
    }

    /**
     * 如果存在任何 {@link OffsetFetchRequest} 和 {@link OffsetCommitRequest} 请求，则轮询这些请求。
     * 如果启用了自动提交功能，此函数也会尝试自动提交位移。
     * 应用场景: 该方法是 CommitRequestManager 的核心轮询逻辑，在消费者的主循环中被调用，用于发送待处理的位移提交和获取请求。
     * 实现细节: 
     * 1. 检查协调器是否已知且无致命错误，否则无法进行轮询。
     * 2. 如果正在关闭，则处理并清空待处理的位移提交请求。
     * 3. 尝试执行异步自动提交（如果启用且满足条件）。
     * 4. 如果没有待发送的请求，则直接返回空结果。
     * 5. 从待处理请求队列中取出当前可发送的请求。
     * 6. 计算下一次轮询前需要等待的最短时间，基于仍在退避中的提交和获取请求的剩余退避时间。
     * 设计考虑: 
     * - 优先处理协调器未知或关闭状态，避免不必要的请求发送。
     * - 将自动提交逻辑放在实际发送请求之前，确保在发送其他请求时，自动提交的位移是最新的。
     * - 计算 `timeUntilNextPoll` 是为了优化轮询效率，避免在没有可发送请求或请求仍在退避时频繁轮询。
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // 仅当协调器节点已知且不存在致命错误时才进行轮询
        // 设计考虑: 协调器是处理位移提交和获取请求的必要组件，如果协调器未知或存在问题，则无法继续。
        if (coordinatorRequestManager.coordinator().isEmpty()) {
            // 如果协调器未知，则检查待处理请求中是否有因协调器致命错误而需要失败的请求。
            pendingRequests.maybeFailOnCoordinatorFatalError();
            // 返回空结果，表示本次轮询没有可发送的请求。
            return EMPTY;
        }

        // 如果 CommitRequestManager 正在关闭过程中
        // 设计考虑: 关闭过程中，应优先处理完所有待处理的提交请求，确保位移被持久化。
        if (closing) {
            // 清空并处理所有待处理的位移提交请求。
            return drainPendingOffsetCommitRequests();
        }

        // 尝试执行异步自动提交（如果启用了自动提交功能并且满足提交条件）
        // 实现细节: 此方法会检查是否到达自动提交时间间隔，并生成一个位移提交请求（如果需要）。
        maybeAutoCommitAsync();
        // 如果当前没有待发送的请求（包括用户手动提交的请求和上面可能生成的自动提交请求）
        // 设计考虑: 避免在没有请求时进行不必要的网络操作。
        if (!pendingRequests.hasUnsentRequests())
            // 返回空结果。
            return EMPTY;

        // 从待处理请求队列中取出当前时间可以发送的请求列表。
        // 实现细节: `drain` 方法会根据请求的退避时间和当前时间来决定哪些请求可以发送。
        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drain(currentTimeMs);
        // 计算下一次轮询前需要等待的最短时间。
        // 这个时间是所有仍在退避中的未发送位移提交请求和未发送位移获取请求的剩余退避时间的最小值。
        // 设计考虑: 这个返回值用于通知调用者（通常是 NetworkClientDelegate）何时可以再次调用 poll 方法，以优化CPU使用。
        final long timeUntilNextPoll = Math.min(
            // 计算未发送的位移提交请求中最小的剩余退避时间。
            findMinTime(unsentOffsetCommitRequests(), currentTimeMs),
            // 计算未发送的位移获取请求中最小的剩余退避时间。
            findMinTime(unsentOffsetFetchRequests(), currentTimeMs));
        // 返回轮询结果，包含下次轮询前的等待时间和本次轮询要发送的请求列表。
        return new NetworkClientDelegate.PollResult(timeUntilNextPoll, requests);
    }

    /**
     * 标记 CommitRequestManager 开始关闭流程。
     * 应用场景: 当消费者准备关闭时，会调用此方法来通知 CommitRequestManager。
     * 实现细节: 将 `closing` 标志位设置为 true。
     * 设计考虑: 通过一个标志位来控制关闭状态，使得 poll 方法在关闭期间可以执行特殊的逻辑（如清空待处理请求）。
     */
    @Override
    public void signalClose() {
        // 将 closing 标志位设置为 true，表示 CommitRequestManager 正在关闭。
        closing = true;
    }

    /**
     * 返回应用程序线程在响应请求管理器结果之前可以安全等待的延迟时间。
     * 例如，当发送心跳时，订阅状态可能会改变，因此阻塞时间超过心跳间隔可能意味着应用程序线程对更改不敏感。
     * 应用场景: 此方法用于确定消费者主线程在调用 poll 等待网络IO时，最长可以阻塞多久而不会错过重要的事件（如自动提交）。
     * 实现细节: 如果启用了自动提交，则返回距离下一次自动提交的剩余时间；否则返回 Long.MAX_VALUE。
     * 设计考虑: 确保消费者能够及时响应自动提交事件，避免因长时间阻塞而错过提交时机。
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        // 如果 autoCommitState 存在（即启用了自动提交）
        // 则调用其 remainingMs 方法获取距离下一次自动提交的剩余毫秒数。
        // 否则（未启用自动提交），返回 Long.MAX_VALUE，表示可以无限期等待（理论上）。
        return autoCommitState.map(ac -> ac.remainingMs(currentTimeMs)).orElse(Long.MAX_VALUE);
    }

    /**
     * 在给定的请求集合中查找最小的剩余退避时间。
     * 应用场景: 用于计算 poll 方法返回的 `timeUntilNextPoll`，确定下一次可以尝试发送请求的时间点。
     * 实现细节: 遍历所有请求，获取每个请求的剩余退避时间，然后返回其中的最小值。如果请求集合为空，则返回 Long.MAX_VALUE。
     * 设计考虑: 这是一个辅助方法，用于简化 `poll` 方法中计算最小等待时间的逻辑。
     *
     * @param requests 请求状态对象的集合，这些对象知道自己的退避状态。
     * @param currentTimeMs 当前时间戳，用于计算剩余退避时间。
     * @return 集合中所有请求的最小剩余退避时间；如果集合为空，则返回 {@link Long#MAX_VALUE}。
     */
    private static long findMinTime(final Collection<? extends RequestState> requests, final long currentTimeMs) {
        // 使用 Java Stream API 处理请求集合。
        return requests.stream()
            // 将每个 RequestState 对象映射为其剩余的退避毫秒数。
            // request.remainingBackoffMs(currentTimeMs) 会计算该请求距离下次可尝试发送还需等待多久。
            .mapToLong(request -> request.remainingBackoffMs(currentTimeMs))
            // 找到所有剩余退避时间中的最小值。
            .min()
            // 如果流为空（即请求集合为空），则返回 Long.MAX_VALUE，表示没有请求在退避，或者说可以立即轮询。
            .orElse(Long.MAX_VALUE);
    }

    /**
     * 尝试将给定的 Throwable 包装成 TimeoutException。
     * 如果原始异常已经是 TimeoutException，则直接返回；否则，创建一个新的 TimeoutException 并包装原始异常。
     * 应用场景: 在处理异步操作的结果时，如果发生超时，可能需要将通用的 Throwable 转换为更具体的 TimeoutException。
     * 实现细节: 简单的类型检查和包装。
     * 设计考虑: 提供一个统一的方式来处理超时相关的异常。
     *
     * @param t 可能需要包装的异常。
     * @return 如果 t 是 TimeoutException，则返回 t；否则返回一个新的 TimeoutException，其中 t 作为原因。
     */
    private KafkaException maybeWrapAsTimeoutException(Throwable t) {
        // 检查传入的异常 t 是否已经是 TimeoutException 的实例。
        if (t instanceof TimeoutException)
            // 如果是，则直接强制类型转换并返回。
            return (TimeoutException) t;
        else
            // 如果不是，则创建一个新的 TimeoutException，并将原始异常 t 作为其原因（cause）。
            return new TimeoutException(t);
    }

    /**
     * 生成一个提交已消费位移的请求。将该请求添加到待处理请求队列中，以便在下一次调用 {@link #poll(long)} 时发送出去。
     * 如果要提交的位移为空，则不会生成请求，并返回一个已完成的 future。
     * 应用场景: 主要用于自动提交位移的场景 ({@link #maybeAutoCommitAsync()})。
     * 实现细节:
     * 1. 检查要提交的位移是否为空，如果为空则直接返回一个包含空映射的已完成的 Future。
     * 2. 如果位移不为空，则设置自动提交状态为“正在进行中”。
     * 3. 将位移提交请求添加到 `pendingRequests` 队列。
     * 4. 获取与该请求关联的 Future，并为其注册一个完成时的回调（`autoCommitCallback`）。
     * 设计考虑:
     * - 通过返回 Future，调用者可以异步地获取提交结果或处理提交失败的情况。
     * - `setInflightCommitStatus(true)` 用于防止在当前自动提交请求未完成时，又发起新的自动提交请求。
     *
     * @param requestState 包含要提交的位移信息的提交请求状态对象。
     * @return 一个 Future，其中包含已提交的位移；如果请求失败，则包含错误信息。
     */
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> requestAutoCommit(final OffsetCommitRequestState requestState) {
        // 获取自动提交状态对象。这里假设 autoCommitState.isPresent() 为 true，因为此方法主要由自动提交逻辑调用。
        AutoCommitState autocommit = autoCommitState.get();
        // 用于存储操作结果的 CompletableFuture。
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result;
        // 检查请求状态中要提交的位移是否为空。
        if (requestState.offsets.isEmpty()) {
            // 如果没有位移需要提交，则直接创建一个已完成的 CompletableFuture，其结果为空的 Map。
            // 设计考虑: 避免发送不必要的空提交请求到 broker。
            result = CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            // 如果有位移需要提交，则将自动提交状态标记为“正在进行中”（in-flight）。
            // 这可以防止在当前自动提交完成之前，又触发新的自动提交。
            autocommit.setInflightCommitStatus(true);
            // 将位移提交请求状态对象添加到待处理请求队列中，并获取返回的实际请求对象（可能包含更多内部状态）。
            OffsetCommitRequestState request = pendingRequests.addOffsetCommitRequest(requestState);
            // 获取与此提交请求关联的 CompletableFuture。
            result = request.future;
            // 当此 Future 完成时（无论是成功还是失败），调用 autoCommitCallback。
            // autoCommitCallback 会处理提交结果，例如更新自动提交计时器或记录错误。
            result.whenComplete(autoCommitCallback(request.offsets));
        }
        // 返回 CompletableFuture，调用者可以通过它来跟踪提交操作的状态。
        return result;
    }

    /**
     * 如果启用了自动提交，并且自动提交间隔已到期，则此方法将生成并排队一个请求以提交所有已消费的位移，
     * 并将自动提交计时器重置为间隔时间。该请求将在下一次调用 {@link #poll(long)} 时发送。
     * <p/>
     * 如果请求以可重试错误完成，则此方法将使用指数退避重置自动提交计时器。
     * 如果请求以不可重试错误失败，则不执行任何操作，因此下一次提交将在间隔到期时生成。
     * <p/>
     * 如果前一个自动提交请求尚未收到响应，则此方法不会生成新的提交请求。
     * 在这种情况下，下一个自动提交请求将在收到正在进行的请求的响应后，在下一次调用 poll 时发送。
     * 应用场景: 在每次 `poll` 方法调用开始时被调用，用于检查是否需要执行自动位移提交。
     * 实现细节:
     * 1. 检查是否启用了自动提交 (`autoCommitEnabled()`) 以及是否到达了提交时间点 (`autoCommitState.get().shouldAutoCommit()`)。
     * 2. 如果满足条件，则创建一个包含所有已消费位移的 `OffsetCommitRequestState`。
     * 3. 调用 `requestAutoCommit` 方法来实际处理这个提交请求，并获取一个 Future。
     * 4. 无论是否生成了请求（例如，如果没有已消费的位移），都重置自动提交计时器到标准间隔。
     * 5. 如果提交请求的 Future 结果是可重试的错误，则根据退避策略调整计时器。
     * 设计考虑:
     * - `shouldAutoCommit` 封装了判断是否应该提交的逻辑，包括时间间隔和是否有正在进行的提交。
     * - 分离了创建请求、发送请求（由 `requestAutoCommit` 处理）和管理计时器的逻辑。
     * - 通过 `maybeResetTimerWithBackoff` 处理提交失败时的退避，避免过于频繁地重试失败的提交。
     */
    public void maybeAutoCommitAsync() {
        // 检查是否启用了自动提交，并且根据自动提交状态判断是否应该进行自动提交。
        // autoCommitState.get().shouldAutoCommit() 会检查是否到达提交时间间隔，并且当前没有正在进行的自动提交。
        if (autoCommitEnabled() && autoCommitState.get().shouldAutoCommit()) {
            // 创建一个位移提交请求状态对象，包含所有已消费分区的位移信息。
            // subscriptions.allConsumed() 获取所有已订阅且已消费的 TopicPartition 及其 OffsetAndMetadata。
            // Long.MAX_VALUE 表示提交请求的默认超时时间（通常由请求的默认超时配置决定，这里可能是一个占位符或特殊值）。
            OffsetCommitRequestState requestState = createOffsetCommitRequest(
                subscriptions.allConsumed(),
                Long.MAX_VALUE);
            // 调用 requestAutoCommit 方法来处理这个异步提交请求，并获取一个 CompletableFuture 以跟踪其结果。
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = requestAutoCommit(requestState);
            // 重置自动提交计时器到配置的间隔（即使由于 offsets 为空而没有实际生成请求）。
            // 这是为了确保下一次检查仍然按照正常的间隔进行。
            resetAutoCommitTimer();
            // 如果提交请求的 Future 完成时带有可重试的错误，则根据退避策略重置计时器。
            // 这样可以避免在出现临时网络问题等情况时过于频繁地尝试提交。
            maybeResetTimerWithBackoff(result);
        }
    }

    /**
     * 如果 future 因 RetriableCommitFailedException 而失败，则重置自动提交计时器以进行退避重试。
     * 应用场景: 当自动提交因可重试错误失败时，需要延迟一段时间后再次尝试，避免立即重试导致过多请求。
     * 实现细节: 使用 CompletableFuture 的 whenComplete 方法，在异步操作完成后检查结果。
     *           如果发生错误且是 RetriableCommitFailedException，则调用 resetAutoCommitTimer 设置新的提交间隔。
     * 设计考虑: 采用退避策略可以减轻 broker 的压力，并提高提交成功的概率。
     * @param result 一个 CompletableFuture 对象，代表异步提交操作的结果，包含提交的位移信息或发生的错误。
     */
    private void maybeResetTimerWithBackoff(final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
        // 当 CompletableFuture 完成时（无论成功还是失败），执行提供的回调函数
        result.whenComplete((offsets, error) -> {
            // 检查是否发生了错误
            if (error != null) {
                // 检查错误是否是 RetriableCommitFailedException 的实例
                if (error instanceof RetriableCommitFailedException) {
                    // 记录调试信息，表明自动提交因可重试错误失败
                    log.debug("异步自动提交位移 {} 因可重试错误失败。", offsets, error);
                    // 使用配置的重试退避时间重置自动提交计时器
                    resetAutoCommitTimer(retryBackoffMs);
                } else {
                    // 记录调试信息，表明自动提交因其他错误失败
                    log.debug("异步自动提交位移 {} 失败: {}", offsets, error.getMessage());
                }
            } else {
                // 记录调试信息，表明异步自动提交成功完成
                log.debug("已完成异步自动提交位移 {}", offsets);
            }
        });
    }

    /**
     * 如果启用了自动提交，则提交已消费的位移，无论自动提交间隔如何。
     * 此方法用于在撤销分区之前提交位移。它将重试提交最新的位移，直到请求成功、因致命错误失败或超时。
     * 注意：
     * <ul>
     *     <li>将 {@link Errors#STALE_MEMBER_EPOCH STALE_MEMBER_EPOCH} 视为可重试错误，并将重试，包括从 broker 收到的成员 ID 和最新的成员 epoch。</li>
     *     <li>将 {@link Errors#UNKNOWN_TOPIC_OR_PARTITION UNKNOWN_TOPIC_OR_PARTITION} 视为致命错误，并且即使该错误扩展了 RetriableException，也不会重试。
     *         原因是如果主题或分区被删除，由于自动提交会持续重试，撤销将无法及时完成。</li>
     * </ul>
     *
     * 另请注意，即使存在另一个由基于间隔逻辑的自动提交生成的正在进行的提交请求，此方法也会生成一个提交请求，
     * 以确保在撤销分区之前提交最新的位移。
     * 应用场景: 在消费者分区被撤销（例如，由于 rebalance）之前，需要确保已消费的位移被可靠地提交，以避免消息丢失或重复消费。
     * 实现细节: 首先检查是否启用了自动提交。如果启用，则创建一个新的 OffsetCommitRequestState，并调用 autoCommitSyncBeforeRevocationWithRetries 方法进行带重试的提交。
     * 设计考虑: 即使存在正在进行的自动提交请求，也要强制进行一次同步提交，以保证分区撤销前的位移提交的及时性和确定性。
     *           对于 STALE_MEMBER_EPOCH 错误进行重试，因为这通常是由于协调器切换或成员 epoch 更新导致的临时性问题。
     *           对于 UNKNOWN_TOPIC_OR_PARTITION 错误不进行重试，因为这表示分区确实不存在了，继续重试没有意义，反而会阻塞分区撤销流程。
     *
     * @param deadlineMs 提交操作的截止时间（毫秒）。
     * @return 一个 CompletableFuture 对象，当位移成功提交时完成。如果提交因不可重试错误失败或重试超时，则会异常完成。
     */
    public CompletableFuture<Void> maybeAutoCommitSyncBeforeRevocation(final long deadlineMs) {
        // 检查是否启用了自动提交功能
        if (!autoCommitEnabled()) {
            // 如果未启用自动提交，则直接返回一个已完成的 CompletableFuture (值为 null)
            return CompletableFuture.completedFuture(null);
        }

        // 创建一个新的 CompletableFuture 用于表示本次提交操作的结果
        CompletableFuture<Void> result = new CompletableFuture<>();
        // 创建一个位移提交请求状态对象，包含所有已消费的位移和截止时间
        OffsetCommitRequestState requestState =
            createOffsetCommitRequest(subscriptions.allConsumed(), deadlineMs);
        // 调用私有方法，以带重试的方式执行同步自动提交
        autoCommitSyncBeforeRevocationWithRetries(requestState, result);
        // 返回代表提交操作结果的 CompletableFuture
        return result;
    }

    /**
     * 使用重试机制执行分区撤销前的同步自动提交。
     * 应用场景: 这是 {@link #maybeAutoCommitSyncBeforeRevocation(long)} 的核心实现，负责实际的提交尝试和重试逻辑。
     * 实现细节: 发起一次自动提交请求。如果成功，则完成外部的 CompletableFuture。如果失败，则根据错误类型进行处理：
     *           - 如果是可重试错误 (RetriableException) 或过期的 epoch 错误 (且有可用的新 epoch)，则检查是否超时。
     *             - 如果超时，则以超时异常完成外部 Future。
     *             - 如果是 UnknownTopicOrPartitionException，则以该异常完成外部 Future (不重试)。
     *             - 否则，更新请求中的位移为最新的已消费位移，重置请求的 Future，然后递归调用自身进行重试。
     *           - 如果是不可重试错误，则以该错误异常完成外部 Future。
     * 设计考虑: 递归调用实现重试逻辑，确保在遇到可重试错误时能够持续尝试，直到成功、超时或遇到致命错误。
     *           在重试前更新位移，确保提交的是最新的消费进度。
     * @param requestAttempt 当前的位移提交请求尝试对象。
     * @param result 用于通知外部调用者提交结果的 CompletableFuture。
     */
    private void autoCommitSyncBeforeRevocationWithRetries(OffsetCommitRequestState requestAttempt,
                                                           CompletableFuture<Void> result) {
        // 发起一次自动提交请求，返回一个代表该次提交尝试结果的 CompletableFuture
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitAttempt = requestAutoCommit(requestAttempt);
        // 当该次提交尝试完成时，执行回调
        commitAttempt.whenComplete((committedOffsets, error) -> {
            // 如果没有错误，表示提交成功
            if (error == null) {
                // 完成外部的 CompletableFuture，表示整个提交操作成功
                result.complete(null);
            } else {
                // 如果发生错误，判断错误类型
                // 如果是可重试异常，或者是过期的 epoch 错误且有可用的有效 epoch
                if (error instanceof RetriableException || isStaleEpochErrorAndValidEpochAvailable(error)) {
                    // 检查当前提交尝试是否已超时
                    if (requestAttempt.isExpired()) {
                        // 如果已超时，记录调试信息，并以超时异常完成外部 CompletableFuture
                        log.debug("分区撤销前的同步自动提交已超时，不再重试");
                        result.completeExceptionally(maybeWrapAsTimeoutException(error));
                    // 如果错误是 UnknownTopicOrPartitionException (主题或分区被删除)
                    } else if (error instanceof UnknownTopicOrPartitionException) {
                        // 记录调试信息，并以该异常完成外部 CompletableFuture (不重试)
                        log.debug("分区撤销前的同步自动提交失败，因为主题或分区已被删除");
                        result.completeExceptionally(error);
                    } else {
                        // 对于其他可重试错误，准备进行重试
                        // 记录调试信息，表明将重试自动提交
                        log.debug("成员 {} 在收到可重试错误 {} 后将重试自动提交最新位移",
                            memberInfo.memberId,
                            error.getMessage());
                        // 确保重试时提交的是最新的已消费位移
                        requestAttempt.offsets = subscriptions.allConsumed();
                        // 重置请求的内部 Future，以便下次重试
                        requestAttempt.resetFuture();
                        // 递归调用自身，进行下一次提交尝试
                        autoCommitSyncBeforeRevocationWithRetries(requestAttempt, result);
                    }
                } else {
                    // 如果是不可重试的错误
                    // 记录调试信息，并以该错误异常完成外部 CompletableFuture
                    log.debug("分区撤销前的同步自动提交因不可重试错误失败", error);
                    result.completeExceptionally(error);
                }
            }
        });
    }

    /**
     * 清除正在进行的自动提交标志并记录自动提交完成状态。
     * 应用场景: 作为自动提交请求完成后的回调函数，用于更新内部状态和记录日志。
     * 实现细节: 返回一个 BiConsumer，该 BiConsumer 会在自动提交完成后被调用。
     *           它首先清除 AutoCommitState 中的 in-flight 标志，然后根据提交结果（成功或失败）记录相应的日志。
     *           如果成功，还会调用 offsetCommitCallbackInvoker 来执行用户定义的提交拦截器。
     * 设计考虑: 将回调逻辑封装在一个单独的方法中，使得代码更清晰，并且方便在发起自动提交请求时传递。
     * @param allConsumedOffsets 本次尝试提交的所有已消费位移。用于日志记录和传递给拦截器。
     * @return 一个 BiConsumer，用于处理自动提交完成后的逻辑。
     */
    private BiConsumer<? super Map<TopicPartition, OffsetAndMetadata>, ? super Throwable> autoCommitCallback(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        // 返回一个 BiConsumer 实例，它接受响应和可能的异常作为参数
        return (response, throwable) -> {
            // 如果 autoCommitState 存在（即启用了自动提交）
            autoCommitState.ifPresent(autoCommitState -> autoCommitState.setInflightCommitStatus(false)); // 将正在进行的提交状态设置为 false
            // 检查提交过程中是否发生异常
            if (throwable == null) {
                // 如果没有异常，表示提交成功
                // 将所有已消费的位移加入到拦截器调用队列中，以便后续执行用户定义的拦截器逻辑
                offsetCommitCallbackInvoker.enqueueInterceptorInvocation(allConsumedOffsets);
                // 记录调试日志，表明自动提交成功完成
                log.debug("已完成自动提交位移 {}", allConsumedOffsets);
            // 如果异常是 RetriableCommitFailedException 的实例
            } else if (throwable instanceof RetriableCommitFailedException) {
                // 记录调试日志，表明自动提交因可重试错误失败
                log.debug("自动提交位移 {} 因可重试错误失败: {}",
                        allConsumedOffsets, throwable.getMessage());
            } else {
                // 对于其他类型的异常，记录警告日志
                log.warn("自动提交位移 {} 失败", allConsumedOffsets, throwable);
            }
        };
    }

    /**
     * 生成一个提交位移的请求，即使因可重试错误失败也不会重试。
     * 生成的请求将被添加到队列中，在下一次调用 {@link #poll(long)} 时发送。
     * 应用场景: 用于实现消费者的异步提交 (commitAsync) API。用户调用此 API 提交位移，不期望方法阻塞等待提交完成，也不期望内部自动重试。
     * 实现细节: 如果传入的 offsets 为空，则获取当前所有已消费的位移。如果位移为空，则直接返回一个已完成的 Future。
     *           否则，创建一个 OffsetCommitRequestState，将其添加到待处理请求队列中。
     *           然后创建一个新的 CompletableFuture (asyncCommitResult) 返回给调用者，并将其与请求内部的 Future 关联起来，
     *           当请求完成时，根据结果完成或异常完成 asyncCommitResult。
     * 设计考虑: 提供一个不带内部重试的异步提交机制。如果发生可重试错误，会将错误包装成 RetriableCommitFailedException 再抛出，
     *           由调用者决定如何处理。
     *
     * @param offsets 每个分区的待提交位移。如果为 {@link Optional#empty()}，则提交所有已消费的位移。
     * @return 一个 CompletableFuture 对象，当收到响应时完成，成功或异常取决于响应内容。
     *         如果请求因可重试错误失败，future 将以 {@link RetriableCommitFailedException} 异常完成。
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitAsync(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        // 获取要提交的位移：如果传入的 offsets 存在，则使用它；否则，获取当前所有已消费的位移
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = offsets.orElseGet(subscriptions::allConsumed);
        // 如果要提交的位移集合为空
        if (commitOffsets.isEmpty()) {
            // 记录调试信息，跳过空位移的提交
            log.debug("跳过提交空位移");
            // 返回一个已成功完成的 CompletableFuture，其结果为空 Map
            return CompletableFuture.completedFuture(Map.of());
        }
        // 如果提交的位移中包含新的 epoch 信息，则可能更新 lastSeenEpoch
        maybeUpdateLastSeenEpochIfNewer(commitOffsets);
        // 创建一个位移提交请求状态对象，使用 Long.MAX_VALUE 表示没有截止时间（因为是异步提交，不在此处处理超时）
        OffsetCommitRequestState commitRequest = createOffsetCommitRequest(commitOffsets, Long.MAX_VALUE);
        // 将创建的提交请求添加到待处理请求队列中
        pendingRequests.addOffsetCommitRequest(commitRequest);

        // 创建一个新的 CompletableFuture，用于返回给异步提交的调用者
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> asyncCommitResult = new CompletableFuture<>();
        // 当内部的提交请求完成时（无论成功还是失败），执行回调
        commitRequest.future.whenComplete((committedOffsets, error) -> {
            // 如果发生了错误
            if (error != null) {
                // 使用转换后的异常（例如，将 RetriableException 包装为 RetriableCommitFailedException）使外部 Future 异常完成
                asyncCommitResult.completeExceptionally(commitAsyncExceptionForError(error));
            } else {
                // 如果没有错误，表示提交成功，使用提交的位移使外部 Future 正常完成
                asyncCommitResult.complete(commitOffsets);
            }
        });
        // 返回代表异步提交操作结果的 CompletableFuture
        return asyncCommitResult;
    }

    /**
     * 同步提交位移，如果遇到预期的可重试错误并且重试超时时间尚未过期，则会进行重试。
     * 应用场景：当消费者需要确保位移已成功提交到 Kafka 时使用此方法。例如，在处理完一批消息后，
     * 调用此方法可以保证这些消息的位移被持久化，即使发生临时网络故障或协调器问题，也会尝试重试。
     * 实现细节：
     * 1. 获取要提交的位移，如果未提供，则获取所有已消费的位移。
     * 2. 如果没有位移需要提交，则直接返回一个已完成的 Future。
     * 3. 尝试更新 lastSeenEpoch（如果新的 epoch 更大）。
     * 4. 创建一个 OffsetCommitRequestState 对象来封装提交请求的详细信息。
     * 5. 调用 commitSyncWithRetries 方法来执行提交并处理重试逻辑。
     * 设计考虑：
     * - 使用 CompletableFuture 异步返回结果，但方法本身是同步阻塞的，直到提交成功或失败。
     * - 提供了 deadlineMs 参数来控制重试的总时长，避免无限重试。
     * - 依赖 subscriptions::allConsumed 来获取默认的提交位移，简化了调用。
     *
     * @param offsets    要提交的位移 (类型为 Optional<Map<TopicPartition, OffsetAndMetadata>>，表示可能没有指定位移，此时会提交所有已消费的位移)
     * @param deadlineMs 请求失败时将重试直至此时间点 (如果失败是预期的可重试错误)
     * @return 一个 Future，当成功响应时完成 (类型为 CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>，包含成功提交的位移信息)
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitSync(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets,
                                                                                final long deadlineMs) {
        // 获取要提交的位移。如果 offsets 参数存在，则使用它；否则，获取所有已消费的位移。
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = offsets.orElseGet(subscriptions::allConsumed);
        // 如果要提交的位移集合为空，则直接返回一个已完成的 Future，其中包含一个空 Map。
        if (commitOffsets.isEmpty()) {
            // 无位移提交，直接返回成功
            return CompletableFuture.completedFuture(Map.of());
        }
        // 尝试使用提供的位移信息更新内部的 lastSeenEpoch，如果新 epoch 更大。
        // 这有助于处理消费者组成员关系变化（如 rebalance）导致 epoch 更新的情况。
        maybeUpdateLastSeenEpochIfNewer(commitOffsets);
        // 创建一个 CompletableFuture 用于异步返回提交结果。
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = new CompletableFuture<>();
        // 创建一个 OffsetCommitRequestState 对象，封装了提交请求的详细信息，如位移、组ID、实例ID、截止时间等。
        OffsetCommitRequestState requestState = createOffsetCommitRequest(commitOffsets, deadlineMs);
        // 调用 commitSyncWithRetries 方法，使用创建的请求状态和结果 Future 来执行同步提交，并处理重试逻辑。
        commitSyncWithRetries(requestState, result);
        // 返回 CompletableFuture，调用者可以等待其完成以获取提交结果。
        return result;
    }

    /**
     * 创建一个 OffsetCommitRequestState 对象，用于封装位移提交请求的详细信息。
     * 应用场景：在准备发送位移提交请求之前，需要将所有相关参数（如位移、组ID、实例ID、重试参数等）收集到一个对象中。
     * 实现细节：
     * - 检查是否存在 jitter (随机抖动) 配置。
     * - 如果存在 jitter，则创建一个包含 jitter 值的 OffsetCommitRequestState 实例。
     * - 如果不存在 jitter，则创建一个不包含 jitter 值的 OffsetCommitRequestState 实例。
     * 设计考虑：
     * - jitter 用于在重试时引入随机性，避免多个消费者同时重试导致“惊群效应”。
     * - 将创建请求状态的逻辑封装在一个私有方法中，使 commitSync 方法更简洁。
     *
     * @param offsets    要提交的位移 (类型为 Map<TopicPartition, OffsetAndMetadata>)
     * @param deadlineMs 请求的截止时间 (类型为 long)
     * @return OffsetCommitRequestState 对象 (封装了位移提交请求的状态和参数)
     */
    private OffsetCommitRequestState createOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                               final long deadlineMs) {
        // 检查 jitter 是否存在 (jitter 用于在重试退避中添加随机性)
        return jitter.isPresent() ?
            // 如果 jitter 存在，则创建一个包含 jitter 值的 OffsetCommitRequestState 实例
            new OffsetCommitRequestState(
                offsets, // 要提交的位移
                groupId, // 消费者组 ID
                groupInstanceId, // 消费者实例 ID (可选)
                deadlineMs, // 请求截止时间
                retryBackoffMs, // 重试退避时间 (毫秒)
                retryBackoffMaxMs, // 最大重试退避时间 (毫秒)
                jitter.getAsDouble(), // jitter 值
                memberInfo) : // 成员信息 (包含 memberId 和 groupEpoch)
            // 如果 jitter 不存在，则创建一个不包含 jitter 值的 OffsetCommitRequestState 实例
            new OffsetCommitRequestState(
                offsets, // 要提交的位移
                groupId, // 消费者组 ID
                groupInstanceId, // 消费者实例 ID (可选)
                deadlineMs, // 请求截止时间
                retryBackoffMs, // 重试退避时间 (毫秒)
                retryBackoffMaxMs, // 最大重试退避时间 (毫秒)
                memberInfo); // 成员信息 (包含 memberId 和 groupEpoch)
    }

    /**
     * 使用重试逻辑执行同步位移提交。
     * 应用场景：这是实际执行位移提交并处理可重试错误的核心逻辑。
     * 实现细节：
     * 1. 将位移提交请求添加到待处理请求队列中。
     * 2. 为请求的 Future 设置一个回调函数 (whenComplete)。
     *    - 如果请求成功 (error 为 null)，则完成外部传入的 result Future，并使用提交的位移作为结果。
     *    - 如果请求失败 (error 不为 null)：
     *      - 如果错误是可重试的 (RetriableException)：
     *        - 如果请求已过期 (isExpired)，则记录日志并通过 maybeWrapAsTimeoutException 包装错误后完成 result Future。
     *        - 如果请求未过期，则重置请求的 Future (resetFuture) 并递归调用 commitSyncWithRetries 以进行下一次尝试。
     *      - 如果错误不是可重试的，则通过 commitSyncExceptionForError 包装错误后完成 result Future。
     * 设计考虑：
     * - 递归调用实现重试逻辑，直到成功、超时或遇到不可重试错误。
     * - 使用 CompletableFuture 的 whenComplete 回调来处理异步请求的结果。
     * - pendingRequests 用于管理待发送的请求。
     *
     * @param requestAttempt 当前的位移提交请求尝试 (类型为 OffsetCommitRequestState)
     * @param result         用于返回最终提交结果的 CompletableFuture (类型为 CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>)
     */
    private void commitSyncWithRetries(OffsetCommitRequestState requestAttempt,
                                       CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
        // 将当前的位移提交请求尝试添加到待处理请求队列中，等待发送。
        pendingRequests.addOffsetCommitRequest(requestAttempt);

        // 当请求的 Future 完成时（无论成功或失败），执行以下回调逻辑。
        // 这个回调处理提交请求的结果，并根据情况决定是否重试。
        requestAttempt.future.whenComplete((res, error) -> {
            // 检查请求是否成功完成 (error 为 null 表示成功)
            if (error == null) {
                // 如果请求成功，则使用请求中的位移信息完成外部传入的 result Future。
                result.complete(requestAttempt.offsets);
            } else {
                // 如果请求失败 (error 不为 null)
                // 检查错误是否是可重试的异常 (RetriableException)
                if (error instanceof RetriableException) {
                    // 如果是可重试的异常
                    // 检查当前请求尝试是否已过期 (根据 deadlineMs 判断)
                    if (requestAttempt.isExpired()) {
                        // 如果请求已过期，记录日志，表示不再重试。
                        log.info("OffsetCommit timeout expired so it won't be retried anymore");
                        // 使用 maybeWrapAsTimeoutException 包装原始错误（可能将其转换为 TimeoutException），
                        // 然后用此异常完成 result Future。
                        result.completeExceptionally(maybeWrapAsTimeoutException(error));
                    } else {
                        // 如果请求未过期，准备进行下一次重试。
                        // 重置请求的内部 Future，以便可以再次发送。
                        requestAttempt.resetFuture();
                        // 递归调用 commitSyncWithRetries，使用相同的 requestAttempt 和 result Future 进行重试。
                        commitSyncWithRetries(requestAttempt, result);
                    }
                } else {
                    // 如果错误不是可重试的异常
                    // 使用 commitSyncExceptionForError 方法处理/包装错误，
                    // 然后用此异常完成 result Future。
                    result.completeExceptionally(commitSyncExceptionForError(error));
                }
            }
        });
    }

    /**
     * 为同步提交操作处理错误，可能会包装特定的异常。
     * 应用场景：在同步提交位移失败后，对捕获到的异常进行特定处理，例如将 StaleMemberEpochException 转换为更具体的 CommitFailedException。
     * 实现细节：
     * - 检查错误是否为 StaleMemberEpochException。
     * - 如果是，则返回一个新的 CommitFailedException，并附带更详细的错误信息。
     * - 否则，返回原始错误。
     * 设计考虑：
     * - 封装了特定异常的转换逻辑，使得上层调用者可以捕获更通用的 CommitFailedException。
     *
     * @param error 捕获到的原始异常 (类型为 Throwable)
     * @return 处理或包装后的异常 (类型为 Throwable)
     */
    private Throwable commitSyncExceptionForError(Throwable error) {
        // 检查错误是否是 StaleMemberEpochException 的实例
        // StaleMemberEpochException 表示消费者的 epoch 过期，通常发生在 rebalance 之后。
        if (error instanceof StaleMemberEpochException) {
            // 如果是 StaleMemberEpochException，则将其包装成一个 CommitFailedException。
            // 这样做是为了向上层调用者提供一个更明确的提交失败原因。
            return new CommitFailedException("OffsetCommit failed with stale member epoch. " // 提交失败，成员 epoch 过期
                + Errors.STALE_MEMBER_EPOCH.message()); // 附加 Kafka 错误码的描述信息
        }
        // 如果错误不是 StaleMemberEpochException，则返回原始错误，不做任何修改。
        return error;
    }

    /**
     * 为异步提交操作处理错误，可能会包装特定的异常。
     * 应用场景：在异步提交位移失败后，对捕获到的异常进行特定处理，例如将 RetriableException 转换为更具体的 RetriableCommitFailedException。
     * 实现细节：
     * - 检查错误是否为 RetriableException。
     * - 如果是，则返回一个新的 RetriableCommitFailedException，并包装原始错误。
     * - 否则，返回原始错误。
     * 设计考虑：
     * - 封装了特定异常的转换逻辑，使得上层调用者可以捕获更通用的 RetriableCommitFailedException，并据此决定是否重试。
     *
     * @param error 捕获到的原始异常 (类型为 Throwable)
     * @return 处理或包装后的异常 (类型为 Throwable)
     */
    private Throwable commitAsyncExceptionForError(Throwable error) {
        // 检查错误是否是 RetriableException 的实例
        // RetriableException 表示一个可重试的错误。
        if (error instanceof RetriableException) {
            // 如果是 RetriableException，则将其包装成一个 RetriableCommitFailedException。
            // 这样做是为了向上层调用者提供一个明确的可重试提交失败异常。
            return new RetriableCommitFailedException(error); // 包装原始的可重试错误
        }
        // 如果错误不是 RetriableException，则返回原始错误，不做任何修改。
        return error;
    }

    /**
     * 将一个获取已提交位移的请求加入队列，该请求将在下一次调用 {@link #poll(long)} 时发送。
     * 应用场景：当消费者需要查询特定分区的已提交位移时使用此方法。例如，在消费者启动或手动管理位移时。
     * 实现细节：
     * 1. 如果要获取位移的分区集合为空，则直接返回一个包含空 Map 的已完成 Future。
     * 2. 创建一个 CompletableFuture 用于异步返回获取结果。
     * 3. 创建一个 OffsetFetchRequestState 对象来封装获取请求的详细信息。
     * 4. 调用 fetchOffsetsWithRetries 方法来执行获取并处理重试逻辑。
     * 设计考虑：
     * - 使用 CompletableFuture 异步返回结果。
     * - 提供了 deadlineMs 参数来控制重试的总时长。
     * - 将实际的获取和重试逻辑委托给 fetchOffsetsWithRetries 方法。
     *
     * @param partitions       要获取位移的分区集合 (类型为 Set<TopicPartition>)
     * @param deadlineMs       请求失败时将重试直至此时间点 (如果失败是预期的可重试错误)
     * @return 一个 Future，当收到成功响应或请求失败且无法重试时完成。
     *         注意，只要请求因可重试的预期错误而失败且重试时间尚未过期，就会重试该请求。
     *         (类型为 CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>，包含获取到的位移信息)
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsets(
        final Set<TopicPartition> partitions, // 要为其获取位移的分区集合
        final long deadlineMs) { // 请求的截止时间
        // 检查要获取位移的分区集合是否为空
        if (partitions.isEmpty()) {
            // 如果为空，则直接返回一个已完成的 CompletableFuture，其中包含一个空的、不可变的 Map。
            // 表示没有分区需要获取位移，因此结果为空。
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        // 创建一个 CompletableFuture 用于异步返回获取到的位移结果。
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = new CompletableFuture<>();
        // 创建一个 OffsetFetchRequestState 对象，封装了获取请求的详细信息，如分区、截止时间等。
        OffsetFetchRequestState request = createOffsetFetchRequest(partitions, deadlineMs);
        // 调用 fetchOffsetsWithRetries 方法，使用创建的请求状态和结果 Future 来执行获取操作，并处理重试逻辑。
        fetchOffsetsWithRetries(request, result);
        // 返回 CompletableFuture，调用者可以等待其完成以获取位移信息。
        return result;
    }

    /**
     * 创建一个 OffsetFetchRequestState 对象，用于封装获取已提交位移请求的详细信息。
     * 此方法主要用于测试，因此可见性为包级私有 (default)。
     * 应用场景：在准备发送获取已提交位移的请求之前，需要将所有相关参数收集到一个对象中。
     * 实现细节：
     * - 检查是否存在 jitter (随机抖动) 配置。
     * - 如果存在 jitter，则创建一个包含 jitter 值的 OffsetFetchRequestState 实例。
     * - 如果不存在 jitter，则创建一个不包含 jitter 值的 OffsetFetchRequestState 实例。
     * 设计考虑：
     * - jitter 用于在重试时引入随机性。
     * - 将创建请求状态的逻辑封装起来，便于管理和测试。
     *
     * @param partitions 要获取位移的分区集合 (类型为 Set<TopicPartition>)
     * @param deadlineMs 请求的截止时间 (类型为 long)
     * @return OffsetFetchRequestState 对象 (封装了获取已提交位移请求的状态和参数)
     */
    // Visible for testing (包级私有，主要用于测试)
    OffsetFetchRequestState createOffsetFetchRequest(final Set<TopicPartition> partitions,
                                                             final long deadlineMs) {
        // 检查 jitter 是否存在 (jitter 用于在重试退避中添加随机性)
        return jitter.isPresent() ?
            // 如果 jitter 存在，则创建一个包含 jitter 值的 OffsetFetchRequestState 实例
            new OffsetFetchRequestState(
                partitions, // 要获取位移的分区
                retryBackoffMs, // 重试退避时间 (毫秒)
                retryBackoffMaxMs, // 最大重试退避时间 (毫秒)
                deadlineMs, // 请求截止时间
                jitter.getAsDouble(), // jitter 值
                memberInfo) : // 成员信息 (包含 memberId 和 groupEpoch)
            // 如果 jitter 不存在，则创建一个不包含 jitter 值的 OffsetFetchRequestState 实例
            new OffsetFetchRequestState(
                partitions, // 要获取位移的分区
                retryBackoffMs, // 重试退避时间 (毫秒)
                retryBackoffMaxMs, // 最大重试退避时间 (毫秒)
                deadlineMs, // 请求截止时间
                memberInfo); // 成员信息 (包含 memberId 和 groupEpoch)
    }

    /**
     * 带重试机制地获取位移。
     * 应用场景：当需要从 broker 获取分区的已提交位移时调用此方法。如果获取失败且错误是可重试的，则会进行重试。
     * 实现细节：
     * 1. 将位移获取请求添加到待处理请求队列中。
     * 2. 当请求完成时，检查是否有错误。
     * 3. 如果没有错误，则更新 lastSeenEpoch 并完成结果 Future。
     * 4. 如果发生可重试错误（RetriableException 或 StaleMemberEpochException 且有有效的 epoch），并且请求未过期，则重置请求的 Future 并递归调用自身进行重试。
     * 5. 如果请求已过期或发生不可重试错误，则以异常完成结果 Future。
     * 设计考虑：
     * - 使用 CompletableFuture 进行异步处理，避免阻塞调用线程。
     * - 通过递归调用实现重试逻辑，简化了重试状态的管理。
     * - 区分可重试和不可重试错误，以及请求是否过期，来决定是否继续重试。
     *
     * @param fetchRequest 要发送的位移获取请求状态对象
     * @param result 用于接收获取结果的 CompletableFuture 对象，结果是一个包含 TopicPartition 到 OffsetAndMetadata 映射的 Map
     */
    private void fetchOffsetsWithRetries(final OffsetFetchRequestState fetchRequest,
                                         final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
        // 将位移获取请求添加到待处理请求中，并返回一个表示当前请求结果的 CompletableFuture
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> currentResult = pendingRequests.addOffsetFetchRequest(fetchRequest);

        // 当请求失败且为可重试异常，并且重试超时时间未到期时，重试相同的获取请求。
        currentResult.whenComplete((res, error) -> {
            // 从正在进行的位移获取请求集合中移除当前请求
            boolean inflightRemoved = pendingRequests.inflightOffsetFetches.remove(fetchRequest);
            // 如果未能成功移除，记录警告日志，说明可能存在重复的正在进行的请求
            if (!inflightRemoved) {
                log.warn("发现一个重复的、正在进行的请求，但在出站缓冲区中找不到它: " + fetchRequest);
            }
            // 如果没有错误
            if (error == null) {
                // 如果需要，更新 lastSeenEpoch
                maybeUpdateLastSeenEpochIfNewer(res);
                // 成功完成结果 Future
                result.complete(res);
            } else {
                // 如果错误是可重试的，或者是 StaleMemberEpochException 并且有可用的有效 epoch
                if (error instanceof RetriableException || isStaleEpochErrorAndValidEpochAvailable(error)) {
                    // 如果请求已过期
                    if (fetchRequest.isExpired()) {
                        // 记录调试日志，表明位移获取请求已超时，不再重试
                        log.debug("针对 {} 的 OffsetFetch 请求已超时，将不再重试", fetchRequest.requestedPartitions);
                        // 以超时异常完成结果 Future
                        result.completeExceptionally(maybeWrapAsTimeoutException(error));
                    } else {
                        // 重置请求的 Future，以便进行下一次重试
                        fetchRequest.resetFuture();
                        // 递归调用自身，进行重试
                        fetchOffsetsWithRetries(fetchRequest, result);
                    }
                } else
                    // 如果是不可重试错误，则以该错误完成结果 Future
                    result.completeExceptionally(error);
            }
        });
    }

    /**
     * 检查错误是否为 StaleMemberEpochException 并且当前成员信息中存在有效的 epoch。
     * 应用场景：在处理位移获取或提交请求的错误时，判断是否因为 epoch 过期导致，并且本地有更新的 epoch 可以用于重试。
     * 实现细节：检查 throwable 是否是 StaleMemberEpochException 的实例，并且 memberInfo.memberEpoch 是否有值。
     * 设计考虑：此方法用于确定在遇到 StaleMemberEpochException 时是否可以安全地重试请求（因为我们有更新的 epoch）。
     *
     * @param error 发生的异常
     * @return 如果错误是 StaleMemberEpochException 并且存在有效的成员 epoch，则返回 true，否则返回 false
     */
    private boolean isStaleEpochErrorAndValidEpochAvailable(Throwable error) {
        // 检查错误是否是 StaleMemberEpochException 的实例，并且 memberInfo 中存在有效的成员 epoch
        return error instanceof StaleMemberEpochException && memberInfo.memberEpoch.isPresent();
    }

    /**
     * 更新自动提交计时器。
     * 应用场景：在消费者轮询循环中定期调用，以更新自动提交任务的下一次执行时间。
     * 实现细节：如果 autoCommitState 存在（即启用了自动提交），则调用其 updateTimer 方法。
     * 设计考虑：将自动提交的计时器管理委托给 AutoCommitState 类，保持 CommitRequestManager 的职责集中。
     *
     * @param currentTimeMs 当前时间（毫秒）
     */
    public void updateAutoCommitTimer(final long currentTimeMs) {
        // 如果 autoCommitState 存在（即启用了自动提交），则调用其 updateTimer 方法更新计时器
        this.autoCommitState.ifPresent(t -> t.updateTimer(currentTimeMs));
    }

    // 仅供测试使用
    /**
     * 获取未发送的位移提交请求队列。
     * 应用场景：主要用于单元测试，验证待发送的位移提交请求是否正确管理。
     * 实现细节：返回 pendingRequests 内部的 unsentOffsetCommits 队列。
     * 设计考虑：提供此方法是为了方便测试，实际生产代码不应直接访问此内部状态。
     *
     * @return 未发送的位移提交请求队列
     */
    Queue<OffsetCommitRequestState> unsentOffsetCommitRequests() {
        // 返回待处理请求中未发送的位移提交请求队列
        return pendingRequests.unsentOffsetCommits;
    }

    /**
     * 获取未发送的位移获取请求列表。
     * 应用场景：主要用于内部逻辑，例如在 poll 方法中收集所有待发送的请求。
     * 实现细节：返回 pendingRequests 内部的 unsentOffsetFetches 列表。
     * 设计考虑：封装对未发送请求列表的访问。
     *
     * @return 未发送的位移获取请求列表
     */
    private List<OffsetFetchRequestState> unsentOffsetFetchRequests() {
        // 返回待处理请求中未发送的位移获取请求列表
        return pendingRequests.unsentOffsetFetches;
    }

    /**
     * 更新成员使用的最新成员 epoch。
     * 当消费者协调器通知成员 epoch 更新时调用此方法（例如，在成功加入组或重平衡后）。
     * 应用场景：作为 MemberStateListener 接口的实现，在消费者组成员状态（特别是 epoch）发生变化时被调用。
     * 实现细节：
     * 1. 如果新的 memberEpoch 为空而旧的 memberEpoch 存在，则记录一条信息，表明成员已离开组，后续请求将不包含 epoch。
     * 2. 更新内部存储的 memberId 和 memberEpoch。
     * 设计考虑：确保 CommitRequestManager 持有的成员信息与消费者组的实际状态同步，以便在发送请求时使用正确的 epoch 和成员 ID。
     *
     * @param memberEpoch 收到的新成员 epoch。将包含在新请求中。
     * @param memberId 当前成员 ID。将包含在新请求中。
     */
    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
        // 如果新的成员 epoch 为空，而旧的成员 epoch 存在（表示成员刚刚离开组）
        if (memberEpoch.isEmpty() && memberInfo.memberEpoch.isPresent()) {
            // 记录日志，说明成员已离开组，后续的位移提交/获取请求将不再包含 epoch
            log.info("成员 {} 在后续的位移提交/获取请求中将不包含 epoch，因为它已离开该组。", memberInfo.memberId);
        }
        // 更新成员ID
        memberInfo.memberId = memberId;
        // 更新成员 epoch
        memberInfo.memberEpoch = memberEpoch;
    }

    /**
     * 检查是否启用了自动提交。
     * 应用场景：在需要根据是否启用自动提交来执行不同逻辑的地方使用，例如决定是否在关闭时进行最后一次提交。
     * 实现细节：检查 autoCommitState optional 是否有值。
     * 设计考虑：提供一个清晰的方法来查询自动提交的启用状态。
     *
     * @return 如果在配置 {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG} 中定义启用了自动提交，则返回 true
     */
    public boolean autoCommitEnabled() {
        // 检查 autoCommitState 是否存在，如果存在则表示启用了自动提交
        return autoCommitState.isPresent();
    }

    /**
     * 将自动提交计时器重置为自动提交间隔，以便下一次自动提交从现在开始按间隔发送。
     * 如果未启用自动提交，则此操作不执行任何操作。
     * 应用场景：例如，在手动提交成功后，可以重置自动提交计时器，以避免紧接着又进行一次自动提交。
     * 实现细节：如果 autoCommitState 存在，则调用其 resetTimer 方法。
     * 设计考虑：允许外部代码在特定条件下影响自动提交的调度。
     */
    public void resetAutoCommitTimer() {
        // 如果 autoCommitState 存在（即启用了自动提交），则调用其 resetTimer 方法重置计时器
        autoCommitState.ifPresent(AutoCommitState::resetTimer);
    }

    /**
     * 将自动提交计时器重置为提供的时间（退避时间），以便下一次自动提交在该时间点发送。
     * 如果未启用自动提交，则此操作无效。
     * 应用场景: 当需要延迟下一次自动提交时调用，例如在发生可重试错误后，希望等待一段时间再尝试自动提交。
     * 实现细节: 如果 autoCommitState 存在（即启用了自动提交），则调用其 resetTimer 方法。
     * 设计考虑: 使用 Optional<AutoCommitState> 来优雅地处理自动提交未启用的情况，避免空指针异常。
     */
    public void resetAutoCommitTimer(long retryBackoffMs) {
        // 如果 autoCommitState 存在（即启用了自动提交）
        autoCommitState.ifPresent(s -> 
            // 调用 AutoCommitState 的 resetTimer 方法，传入指定的退避时间
            s.resetTimer(retryBackoffMs)
        );
    }

    /**
     * 在关闭期间清空正在处理的位移提交请求，因为我们希望在关闭之前确保所有待处理的提交都已发送。
     * 应用场景: 在消费者关闭流程中，确保所有异步提交的位移请求都被处理，防止数据丢失。
     * 实现细节: 检查是否有未发送的位移提交请求，如果有，则将它们排出并封装成 PollResult 返回。
     * 设计考虑: 返回 PollResult 结构，其中包含请求列表和最大超时时间，以便网络客户端统一处理。
     */
    public NetworkClientDelegate.PollResult drainPendingOffsetCommitRequests() {
        // 检查待处理请求中是否有未发送的位移提交
        if (pendingRequests.unsentOffsetCommits.isEmpty())
            // 如果没有未发送的提交，则返回一个空的 PollResult
            return EMPTY;
        // 从待处理请求中排出所有待处理的提交请求
        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drainPendingCommits();
        // 创建一个新的 PollResult，其中包含排出的请求和 Long.MAX_VALUE 作为超时时间（表示立即发送）
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, requests);
    }

    /**
     * 如果传入的位移信息中包含新的（更大的）leader epoch，则可能更新元数据中对应主题分区的最后可见 epoch。
     * 应用场景: 在处理位移提交或获取的响应时，利用响应中的 epoch 信息来更新本地的元数据视图，有助于检测分区 leader 的变化。
     * 实现细节: 遍历传入的 offsets 映射，对每个 TopicPartition 和 OffsetAndMetadata，如果 OffsetAndMetadata 不为 null 并且包含 leaderEpoch，
     * 则调用 metadata.updateLastSeenEpochIfNewer 来尝试更新。
     * 设计考虑: 这是一个内部辅助方法，用于保持元数据中 leader epoch 的最新状态。
     * @param offsets 包含主题分区及其对应位移和元数据信息的映射
     */
    private void maybeUpdateLastSeenEpochIfNewer(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 遍历 offsets 映射中的每个条目（TopicPartition -> OffsetAndMetadata）
        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            // 检查 offsetAndMetadata 是否为 null
            if (offsetAndMetadata != null)
                // 如果 offsetAndMetadata 不为 null，则尝试获取其 leaderEpoch
                // 如果 leaderEpoch 存在，则调用 metadata 的 updateLastSeenEpochIfNewer 方法，
                // 用当前分区的 epoch 更新元数据中记录的该分区的最后可见 epoch（如果新 epoch 更大）
                offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
        });
    }

    /**
     * 表示一个可重试的位移提交请求的状态。此类封装了提交特定位移所需的所有信息，
     * 并处理请求的构建、发送以及响应的处理，包括错误处理和重试逻辑。
     * 应用场景: 每当消费者需要提交位移时，都会创建一个此类的实例来管理该提交请求的生命周期。
     * 设计考虑: 继承自 RetriableRequestState，复用了通用的重试逻辑和状态管理。
     */
    class OffsetCommitRequestState extends RetriableRequestState {
        // 要提交的位移信息，键是主题分区，值是位移和元数据
        private Map<TopicPartition, OffsetAndMetadata> offsets;
        // 消费者组ID
        private final String groupId;
        // 消费者实例ID (可选), 用于静态成员资格
        private final Optional<String> groupInstanceId;

        /**
         * 包含已提交位移的 Future。当收到提交请求的响应时，此 Future 完成。
         * 应用场景: 调用者可以通过此 Future 异步地获取提交操作的结果或异常。
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        /**
         * OffsetCommitRequestState 的构造函数。
         * @param offsets 要提交的位移信息
         * @param groupId 消费者组ID
         * @param groupInstanceId 消费者实例ID (可选)
         * @param deadlineMs 请求的截止时间（毫秒）
         * @param retryBackoffMs 重试退避时间（毫秒）
         * @param retryBackoffMaxMs 最大重试退避时间（毫秒）
         * @param memberInfo 成员信息，包含成员ID和 epoch
         */
        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final long deadlineMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final MemberInfo memberInfo) {
            // 调用父类 RetriableRequestState 的构造函数，初始化重试相关的参数
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, memberInfo, deadlineTimer(time, deadlineMs));
            // 初始化要提交的位移
            this.offsets = offsets;
            // 初始化消费者组ID
            this.groupId = groupId;
            // 初始化消费者实例ID
            this.groupInstanceId = groupInstanceId;
            // 初始化用于异步结果的 CompletableFuture
            this.future = new CompletableFuture<>();
        }

        // 专用于测试的构造函数，允许设置 jitter
        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final long deadlineMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final double jitter, // 重试退避的抖动因子
                                 final MemberInfo memberInfo) {
            // 调用父类 RetriableRequestState 的构造函数，初始化重试相关的参数，包括 jitter
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2, // maxRetries 参数，这里固定为2，可能需要根据实际情况调整或从配置读取
                retryBackoffMaxMs, jitter, memberInfo, deadlineTimer(time, deadlineMs));
            // 初始化要提交的位移
            this.offsets = offsets;
            // 初始化消费者组ID
            this.groupId = groupId;
            // 初始化消费者实例ID
            this.groupInstanceId = groupInstanceId;
            // 初始化用于异步结果的 CompletableFuture
            this.future = new CompletableFuture<>();
        }

        /**
         * 将此状态对象转换为一个未发送的网络请求。
         * 应用场景: 在准备发送位移提交请求到 Kafka broker 之前调用此方法。
         * 实现细节: 构建 OffsetCommitRequestData 对象，填充 groupId、groupInstanceId、memberId、epoch 以及要提交的位移信息，
         * 然后使用 OffsetCommitRequest.Builder 创建请求。
         * @return 一个 NetworkClientDelegate.UnsentRequest 对象，封装了待发送的 OffsetCommitRequest
         */
        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            // 创建一个映射，用于按主题组织提交的位移数据
            Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
            // 遍历要提交的位移信息
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                // 获取当前处理的主题分区
                TopicPartition topicPartition = entry.getKey();
                // 获取当前处理的位移和元数据
                OffsetAndMetadata offsetAndMetadata = entry.getValue();

                // 获取或创建一个新的 OffsetCommitRequestTopic 对象，用于存储该主题下的分区位移信息
                OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(), // 尝试获取已有的主题对象
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                            .setName(topicPartition.topic()) // 如果不存在，则创建一个新的并设置主题名称
                    );

                // 向主题对象中添加分区的位移提交信息
                topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition()) // 设置分区索引
                    .setCommittedOffset(offsetAndMetadata.offset()) // 设置提交的位移
                    // 设置提交的 leader epoch，如果不存在则使用 RecordBatch.NO_PARTITION_LEADER_EPOCH
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata()) // 设置提交的元数据
                );
                // 将更新后的主题对象放回映射中
                requestTopicDataMap.put(topicPartition.topic(), topic);
            }

            // 创建 OffsetCommitRequestData 对象，这是实际发送给 broker 的数据结构
            OffsetCommitRequestData data = new OffsetCommitRequestData()
                    .setGroupId(this.groupId) // 设置消费者组ID
                    .setGroupInstanceId(groupInstanceId.orElse(null)) // 设置消费者实例ID，如果不存在则为 null
                    .setTopics(new ArrayList<>(requestTopicDataMap.values())); // 设置要提交位移的主题列表
            // 设置成员ID
            data = data.setMemberId(memberInfo.memberId);
            // 检查成员 epoch 是否存在
            if (memberInfo.memberEpoch.isPresent()) {
                // 如果存在，则设置 generationId 或 memberEpoch
                data = data.setGenerationIdOrMemberEpoch(memberInfo.memberEpoch.get());
                // 记录最后一次在提交请求中发送的 epoch
                lastEpochSentOnCommit = memberInfo.memberEpoch;
            } else {
                // 如果不存在，则清空记录
                lastEpochSentOnCommit = Optional.empty();
            }

            // 使用 OffsetCommitRequestData 创建 OffsetCommitRequest.Builder
            OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(data);

            // 构建请求并关联响应处理逻辑
            return buildRequestWithResponseHandling(builder);
        }

        /**
         * 处理 OffsetCommitResponse。如果响应中没有错误，此方法将成功完成请求的 future。
         * 如果响应包含错误，此方法将：
         *   - 处理预期的错误，并根据错误类型使用特定的异常使 future 失败
         *   - 对于所有意外错误（即使是可重试的），使用不可恢复的 KafkaException 使 future 失败
         * 应用场景: 当从 broker 收到位移提交响应后，此方法被调用以处理响应内容。
         * 实现细节: 遍历响应中的每个主题和分区，检查错误码。根据不同的错误码执行不同的处理逻辑，
         * 例如标记协调器未知、完成 future (成功或异常)等。
         */
        @Override
        public void onResponse(final ClientResponse response) {
            // 记录请求延迟指标
            metricsManager.recordRequestLatency(response.requestLatencyMs());
            // 获取响应接收时间
            long currentTimeMs = response.receivedTimeMs();
            // 将响应体转换为 OffsetCommitResponse 类型
            OffsetCommitResponse commitResponse = (OffsetCommitResponse) response.responseBody();
            // 用于存储未授权的主题
            Set<String> unauthorizedTopics = new HashSet<>();
            // 标记是否已注册失败尝试
            boolean failedRequestRegistered = false;
            // 遍历响应中每个主题的数据
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                // 遍历主题中每个分区的数据
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    // 创建 TopicPartition 对象
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());

                    // 获取分区的错误码
                    Errors error = Errors.forCode(partition.errorCode());
                    // 如果没有错误
                    if (error == Errors.NONE) {
                        // 从原始提交请求中获取该分区的位移和元数据
                        OffsetAndMetadata offsetAndMetadata = offsets.get(tp);
                        // 获取提交的位移值
                        long offset = offsetAndMetadata.offset();
                        // 记录调试日志，表示位移提交成功
                        log.debug("OffsetCommit completed successfully for offset {} partition {}", offset, tp);
                        // 继续处理下一个分区
                        continue;
                    }

                    // 如果这是此响应中遇到的第一个错误
                    if (!failedRequestRegistered) {
                        // 调用 onFailedAttempt，通知父类此次尝试失败，可能触发重试逻辑
                        onFailedAttempt(currentTimeMs);
                        // 标记已注册失败尝试
                        failedRequestRegistered = true;
                    }

                    // 根据具体的错误类型进行处理
                    if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                        // 组授权失败，使用 GroupAuthorizationException 完成 future
                        future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
                        return; // 终止处理
                    } else if (error == Errors.COORDINATOR_NOT_AVAILABLE ||
                        error == Errors.NOT_COORDINATOR ||
                        error == Errors.REQUEST_TIMED_OUT) {
                        // 协调器不可用、不是协调器或请求超时
                        // 标记协调器未知，以便后续重新发现
                        coordinatorRequestManager.markCoordinatorUnknown(error.message(), currentTimeMs);
                        // 使用原始异常完成 future
                        future.completeExceptionally(error.exception());
                        return; // 终止处理
                    } else if (error == Errors.OFFSET_METADATA_TOO_LARGE ||
                        error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                        // 位移元数据过大或无效的提交位移大小
                        // 使用原始异常完成 future
                        future.completeExceptionally(error.exception());
                        return; // 终止处理
                    } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS ||
                        error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        // 协调器正在加载或未知的主题/分区，这些是可重试的错误
                        // 注释: just retry (表明这是一个可重试的错误)
                        // 使用原始异常完成 future，父类 RetriableRequestState 会处理重试
                        future.completeExceptionally(error.exception());
                        return; // 终止处理
                    } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                        // 未知的成员ID
                        log.error("OffsetCommit failed with {}", error);
                        // 使用 CommitFailedException (不可重试) 完成 future
                        future.completeExceptionally(new CommitFailedException("OffsetCommit " +
                            "failed with unknown member ID. " + error.message()));
                        return; // 终止处理
                    } else if (error == Errors.STALE_MEMBER_EPOCH) {
                        // 过期的成员 epoch
                        log.error("OffsetCommit failed for member {} with stale member epoch error. Last epoch sent: {}",
                            memberInfo.memberId, // 记录当前成员ID
                            // 记录上次发送的 epoch，如果存在的话
                            lastEpochSentOnCommit.isPresent() ? lastEpochSentOnCommit.get() : "undefined");
                        // 使用原始异常 (StaleMemberEpochException，通常是可重试的) 完成 future
                        future.completeExceptionally(error.exception());
                        return; // 终止处理
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        // 主题授权失败
                        // 注释: Collect all unauthorized topics before failing (在失败前收集所有未授权的主题)
                        // 将未授权的主题添加到集合中，稍后统一处理
                        unauthorizedTopics.add(tp.topic());
                    } else {
                        // 其他所有未明确处理的错误
                        // 注释: Fail with a non-retriable KafkaException for all unexpected errors (even if they are retriable)
                        // (对于所有意外错误（即使它们是可重试的），都使用不可重试的 KafkaException 使其失败)
                        // 使用 KafkaException (不可重试) 完成 future
                        future.completeExceptionally(new KafkaException("Unexpected error in commit: " + error.message()));
                        return; // 终止处理
                    }
                }
            }

            // 在处理完所有分区后，检查是否有未授权的主题
            if (!unauthorizedTopics.isEmpty()) {
                // 如果有未授权的主题
                log.error("OffsetCommit failed due to not authorized to commit to topics {}", unauthorizedTopics);
                // 使用 TopicAuthorizationException 完成 future
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                // 如果没有错误，或者所有错误都已通过 return 处理（例如可重试错误导致 future.completeExceptionally 后返回）
                // 并且没有未授权的主题，则表示提交成功（或者对于某些错误，future 已经被异常完成了）
                // 如果 failedRequestRegistered 为 false，意味着所有分区都没有错误，此时可以安全地完成 future
                // 如果 failedRequestRegistered 为 true，但所有错误都已通过 return 处理，这里也需要完成 future
                // 注意：如果一个可重试错误发生，future 会被异常完成，然后 RetriableRequestState 会处理重试。
                // 只有当所有分区都成功，或者发生了不可重试的错误（如 GROUP_AUTHORIZATION_FAILED），或者所有可重试错误都已处理完毕且没有其他问题时，
                // 才会走到这里。如果 future 尚未被异常完成，则表示所有分区都成功了。
                if (!future.isDone()) { // 确保 future 尚未被其他逻辑完成
                    future.complete(null); // 成功完成 future，传入 null 表示没有特定结果值
                }
            }
        }

        /**
         * 返回此请求的描述字符串，用于日志记录。
         * @return 请求的描述字符串
         */
        @Override
        String requestDescription() {
            // 返回一个描述性的字符串，包含正在提交的位移信息
            return "OffsetCommit request for offsets " + offsets;
        }

        /**
         * 返回与此请求关联的 CompletableFuture。
         * @return CompletableFuture 对象
         */
        @Override
        CompletableFuture<?> future() {
            // 返回用于异步获取结果的 future 对象
            return future;
        }

        /**
         * 重置此请求的 CompletableFuture。当请求需要重试时，会创建一个新的 Future。
         * 应用场景: 在父类 RetriableRequestState 决定重试此请求时调用。
         */
        void resetFuture() {
            // 创建一个新的 CompletableFuture 实例，替换旧的
            future = new CompletableFuture<>();
        }

        /**
         * 从待发送请求缓冲区中移除此请求。
         * 应用场景: 当请求成功完成、失败且不可重试，或达到最大重试次数后，需要从待处理队列中移除。
         */
        @Override
        void removeRequest() {
            // 尝试从未发送的位移提交请求集合中移除当前请求状态对象 (this)
            if (!unsentOffsetCommitRequests().remove(this)) {
                // 如果移除失败（即请求不在集合中），记录警告日志
                log.warn("OffsetCommit request to remove not found in the outbound buffer: {}", this);
            }
        }
    }

    // 仅用于测试
    // 获取上次提交请求中发送的成员 epoch。如果上次请求中未包含 epoch，则为空。
    Optional<Integer> lastEpochSentOnCommit() {
        // 返回 lastEpochSentOnCommit 字段的值
        return lastEpochSentOnCommit;
    }

    /**
     * 表示一个可以根据成员ID和epoch信息进行重试或中止的请求。
     * 应用场景：用于封装需要重试逻辑的请求，例如OffsetCommitRequest和OffsetFetchRequest。
     * 设计考虑：将可重试请求的通用逻辑（如成员信息、超时处理、请求构建）抽象到此类中，以减少代码重复。
     */
    abstract class RetriableRequestState extends TimedRequestState {
        // 成员信息（ID和epoch），如果存在，则包含在请求中。

        /**
         * 成员信息（ID和epoch），如果存在，则包含在请求中。
         * 实现细节：此字段为 final，确保一旦设置就不会改变。
         */
        final MemberInfo memberInfo;

        /**
         * RetriableRequestState 的构造函数。
         * @param logContext 日志上下文。
         * @param owner 请求的所有者，通常是类的简单名称。
         * @param retryBackoffMs 重试退避时间（毫秒）。
         * @param retryBackoffMaxMs 最大重试退避时间（毫秒）。
         * @param memberInfo 成员信息。
         * @param timer 用于计算超时的计时器。
         */

        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs,
                              long retryBackoffMaxMs, MemberInfo memberInfo, Timer timer) {
            // 调用父类 TimedRequestState 的构造函数
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs, timer);
            // 初始化成员信息
            this.memberInfo = memberInfo;
        }

        // 仅用于测试的构造函数，允许指定重试退避指数基数和抖动。
        // 设计考虑：提供此构造函数是为了在测试中更精确地控制重试行为。

        // 仅用于测试
        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs, int retryBackoffExpBase,
                              long retryBackoffMaxMs, double jitter, MemberInfo memberInfo, Timer timer) {
            // 调用父类 TimedRequestState 的构造函数，并传入额外的重试参数
            super(logContext, owner, retryBackoffMs, retryBackoffExpBase, retryBackoffMaxMs, jitter, timer);
            // 初始化成员信息
            this.memberInfo = memberInfo;
        }

        /**
         * @return 包含请求名称和参数的字符串，用于日志记录。
         * 实现细节：子类必须实现此方法以提供具体的请求描述。
         */

        abstract String requestDescription();

        /**
         * @return 将使用请求响应或失败来完成的 Future。
         * 实现细节：子类必须实现此方法以提供具体的 Future 实例。
         */

        abstract CompletableFuture<?> future();

        /**
         * 如果请求已至少发送一次并且已达到超时，则使用 TimeoutException 完成请求 future。
         * 应用场景：在轮询待处理请求时调用，以检查是否有请求超时。
         */

        void maybeExpire() {
            // 检查请求是否已尝试发送 (numAttempts > 0) 并且是否已超时 (isExpired())
            if (numAttempts > 0 && isExpired()) {
                // 从待处理请求中移除此请求
                removeRequest();
                // 使用 TimeoutException 异常地完成 future
                future().completeExceptionally(new TimeoutException(requestDescription() +
                    " 在超时之前无法完成。"));
            }
        }

        /**
         * 使用给定的构建器构建请求，包括响应处理逻辑。
         * 设计考虑：将请求构建和响应处理逻辑封装在一起，便于管理。
         * @param builder 请求构建器。
         * @return 未发送的请求对象。
         */

        NetworkClientDelegate.UnsentRequest buildRequestWithResponseHandling(final AbstractRequest.Builder<?> builder) {
            // 创建一个 NetworkClientDelegate.UnsentRequest 实例
            NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
                builder, // 请求构建器
                coordinatorRequestManager.coordinator() // 目标协调器节点
            );
            // 设置请求完成时的回调函数
            request.whenComplete(
                (response, throwable) -> {
                    // 获取请求处理程序的完成时间
                    long completionTimeMs = request.handler().completionTimeMs();
                    // 处理客户端响应
                    handleClientResponse(response, throwable, completionTimeMs);
                });
            // 返回构建的未发送请求
            return request;
        }

        /**
         * 处理客户端响应，包括成功和失败的情况。
         * @param response 客户端响应，如果发生错误则为 null。
         * @param error 发生的异常，如果成功则为 null。
         * @param requestCompletionTimeMs 请求完成的时间戳。
         */

        private void handleClientResponse(final ClientResponse response,
                                          final Throwable error,
                                          final long requestCompletionTimeMs) {
            try {
                // 检查是否有错误发生
                if (error == null) {
                    // 如果没有错误，调用 onResponse 处理成功响应
                    onResponse(response);
                } else {
                    // 如果有错误，记录调试日志
                    log.debug("{} 请求因错误完成", requestDescription(), error);
                    // 调用 onFailedAttempt 处理失败尝试
                    onFailedAttempt(requestCompletionTimeMs);
                    // 让协调器管理器处理可能的协调器断开连接
                    coordinatorRequestManager.handleCoordinatorDisconnect(error, requestCompletionTimeMs);
                    // 使用错误异常地完成 future
                    future().completeExceptionally(error);
                }
            } catch (Throwable t) {
                // 捕获处理响应过程中的任何意外异常
                log.error("处理 {} 的响应时发生意外错误", requestDescription(), t);
                // 使用捕获到的异常异常地完成 future
                future().completeExceptionally(t);
            }
        }

        /**
         * 返回此请求状态的基本字符串表示形式，包括成员信息。
         * @return 包含请求状态基本信息和成员信息的字符串。
         */

        @Override
        public String toStringBase() {
            // 调用父类的 toStringBase() 方法，并附加成员信息
            return super.toStringBase() + ", " + memberInfo;
        }

        /**
         * 处理成功的响应。
         * @param response 客户端响应。
         * 实现细节：子类必须实现此方法以处理特定类型的成功响应。
         */

        abstract void onResponse(final ClientResponse response);

        /**
         * 从待处理请求中移除此请求。
         * 实现细节：子类必须实现此方法以定义如何移除请求。
         */
        abstract void removeRequest();
    }

    /**
     * 表示一个 OffsetFetch 请求的状态。
     * 应用场景：用于跟踪和管理获取已提交位移的请求。
     * 设计考虑：封装了 OffsetFetch 请求的特定逻辑，如请求的分区、Future结果等。
     */

    class OffsetFetchRequestState extends RetriableRequestState {

        /**
         * 需要获取已提交位移的分区集合。
         * 实现细节：此字段为 public final，允许外部读取但不能修改。
         */
        public final Set<TopicPartition> requestedPartitions;

        /**
         * 带有请求结果的 Future。可以使用 {@link #resetFuture()} 重置以在重试请求时获取新结果。
         * 实现细节：这是一个 CompletableFuture，允许异步获取结果。
         */

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        /**
         * OffsetFetchRequestState 的构造函数。
         * @param partitions 需要获取位移的分区。
         * @param retryBackoffMs 重试退避时间（毫秒）。
         * @param retryBackoffMaxMs 最大重试退避时间（毫秒）。
         * @param deadlineMs 请求的截止时间戳。
         * @param memberInfo 成员信息。
         */

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final long deadlineMs,
                                       final MemberInfo memberInfo) {
            // 调用父类 RetriableRequestState 的构造函数
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, memberInfo, deadlineTimer(time, deadlineMs));
            // 初始化请求的分区
            this.requestedPartitions = partitions;
            // 初始化 CompletableFuture
            this.future = new CompletableFuture<>();
        }

        /**
         * OffsetFetchRequestState 的构造函数（用于测试）。
         * @param partitions 需要获取位移的分区。
         * @param retryBackoffMs 重试退避时间（毫秒）。
         * @param retryBackoffMaxMs 最大重试退避时间（毫秒）。
         * @param deadlineMs 请求的截止时间戳。
         * @param jitter 重试抖动因子。
         * @param memberInfo 成员信息。
         */

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final long deadlineMs,
                                       final double jitter,
                                       final MemberInfo memberInfo) {
            // 调用父类 RetriableRequestState 的构造函数，并指定重试退避指数基数为2
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2,
                retryBackoffMaxMs, jitter, memberInfo, deadlineTimer(time, deadlineMs));
            // 初始化请求的分区
            this.requestedPartitions = partitions;
            // 初始化 CompletableFuture
            this.future = new CompletableFuture<>();
        }

        /**
         * 检查此请求是否与另一个 OffsetFetchRequestState 请求相同（基于请求的分区）。
         * @param request 要比较的另一个 OffsetFetchRequestState 请求。
         * @return 如果请求的分区相同，则返回 true；否则返回 false。
         */

        public boolean sameRequest(final OffsetFetchRequestState request) {
            // 比较两个请求的 requestedPartitions 集合是否相等
            return requestedPartitions.equals(request.requestedPartitions);
        }

        /**
         * 将此状态转换为一个未发送的 OffsetFetch 请求。
         * 实现细节：根据 memberInfo 中是否存在 memberEpoch，构建不同版本的 OffsetFetchRequest.Builder。
         * @return 未发送的 OffsetFetch 请求。
         */

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            // 根据 memberInfo.memberEpoch 是否存在来创建 OffsetFetchRequest.Builder
            OffsetFetchRequest.Builder builder = memberInfo.memberEpoch.
                // 如果 memberEpoch 存在，则使用包含 memberId 和 epoch 的构造函数
                map(epoch -> new OffsetFetchRequest.Builder(
                    groupId, // 消费者组ID
                    memberInfo.memberId, // 成员ID
                    epoch, // 成员epoch
                    true, // requireStable，是否需要稳定的位移
                    new ArrayList<>(this.requestedPartitions), // 请求的分区列表
                    throwOnFetchStableOffsetUnsupported // 如果不支持获取稳定位移是否抛出异常
                ))
                // 如果 memberEpoch 不存在，则使用不包含 memberId 和 epoch 的构造函数
                // 构建请求时不传递成员ID/epoch，将逻辑留给请求构建器在不存在时选择默认值。
                .orElseGet(() -> new OffsetFetchRequest.Builder(
                    groupId, // 消费者组ID
                    true, // requireStable
                    new ArrayList<>(this.requestedPartitions), // 请求的分区列表
                    throwOnFetchStableOffsetUnsupported // 如果不支持获取稳定位移是否抛出异常
                ));
            // 使用父类的 buildRequestWithResponseHandling 方法构建并返回未发送的请求
            return buildRequestWithResponseHandling(builder);
        }

        /**
         * 处理 OffsetFetch 响应，包括成功和失败的情况。
         * @param response 客户端响应。
         */

        @Override
        void onResponse(final ClientResponse response) {
            // 获取响应接收时间
            long currentTimeMs = response.receivedTimeMs();
            // 将响应体转换为 OffsetFetchResponse 类型
            OffsetFetchResponse fetchResponse = (OffsetFetchResponse) response.responseBody();
            // 获取组级别的错误
            Errors responseError = fetchResponse.groupLevelError(groupId);
            // 检查是否存在组级别的错误
            if (responseError != Errors.NONE) {
                // 如果存在错误，调用 onFailure 处理失败
                onFailure(currentTimeMs, responseError);
                // 直接返回，不再继续处理
                return;
            }
            // 如果没有组级别错误，调用 onSuccess 处理成功响应
            onSuccess(currentTimeMs, fetchResponse);
        }

        /**
         * 处理失败的响应。如果错误是可重试的，则会重试；否则，如果错误是不可恢复的或意外的，则会以异常方式完成结果 future。
         * 应用场景: 当OffsetFetch请求失败时，此方法被调用以决定下一步操作，是重试还是彻底失败。
         * 实现细节: 根据不同的错误类型执行不同的逻辑，例如标记协调器未知、记录错误日志、或直接完成future并抛出异常。
         * 设计考虑: 集中处理所有失败情况，提供统一的错误处理机制，区分可重试和不可重试错误，以提高系统的健壮性和用户体验。
         * @param currentTimeMs 当前时间戳（毫秒），用于记录失败尝试的时间。
         * @param responseError 响应中的错误类型。
         */
        private void onFailure(final long currentTimeMs,
                               final Errors responseError) {
            // 记录调试日志，表明OffsetFetch失败及原因
            log.debug("Offset fetch failed: {}", responseError.message());
            // 调用onFailedAttempt记录失败尝试，这可能会影响重试策略
            onFailedAttempt(currentTimeMs);
            // 获取错误对应的ApiException对象
            ApiException exception = responseError.exception();
            // 如果错误是COORDINATOR_LOAD_IN_PROGRESS（协调器正在加载）
            if (responseError == COORDINATOR_LOAD_IN_PROGRESS) {
                // 以异常方式完成future，传递原始异常
                future.completeExceptionally(exception);
            // 如果错误是UNKNOWN_MEMBER_ID（未知的成员ID）
            } else if (responseError == Errors.UNKNOWN_MEMBER_ID) {
                // 记录错误日志，表明成员已不在组内
                log.error("OffsetFetch failed with {} because the member is not part of the group" +
                    " anymore.", responseError);
                // 以异常方式完成future，传递原始异常
                future.completeExceptionally(exception);
            // 如果错误是STALE_MEMBER_EPOCH（过期的成员epoch）
            } else if (responseError == Errors.STALE_MEMBER_EPOCH) {
                // 记录错误日志，表明消费者已不在组内（可能已离开、被隔离或失败），请求无法重试并将失败
                log.error("OffsetFetch failed with {} and the consumer is not part " +
                    "of the group anymore (it probably left the group, got fenced" +
                    " or failed). The request cannot be retried and will fail.", responseError);
                // 以异常方式完成future，传递原始异常
                future.completeExceptionally(exception);
            // 如果错误是NOT_COORDINATOR（不是协调器）或COORDINATOR_NOT_AVAILABLE（协调器不可用）
            } else if (responseError == Errors.NOT_COORDINATOR || responseError == Errors.COORDINATOR_NOT_AVAILABLE) {
                // 重新发现协调器并重试
                // 标记协调器为未知状态，以便后续重新发现
                coordinatorRequestManager.markCoordinatorUnknown("error response " + responseError.name(), currentTimeMs);
                // 以异常方式完成future，传递原始异常，上层逻辑可能会根据此异常类型进行重试
                future.completeExceptionally(exception);
            // 如果异常是可重试异常的实例
            } else if (exception instanceof RetriableException) {
                // 以异常方式完成future，传递原始可重试异常，上层逻辑可能会进行重试
                future.completeExceptionally(exception);
            // 如果错误是GROUP_AUTHORIZATION_FAILED（组授权失败）
            } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                // 以组授权失败异常完成future
                future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
            } else {
                // 对于所有其他意外错误，以不可重试的KafkaException失败
                future.completeExceptionally(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
            }
        }

        /**
         * 获取请求的描述字符串。
         * 应用场景: 用于日志记录和调试，提供请求的简明摘要。
         * 实现细节: 返回一个包含请求分区信息的字符串。
         * @return 请求的描述字符串。
         */
        @Override
        String requestDescription() {
            // 返回描述字符串，指明是针对哪些分区的OffsetFetch请求
            return "OffsetFetch request for partitions " + requestedPartitions;
        }

        /**
         * 获取与此请求关联的CompletableFuture。
         * 应用场景: 外部代码可以通过此future获取请求的结果或处理发生的异常。
         * @return 与此请求关联的CompletableFuture。
         */
        @Override
        CompletableFuture<?> future() {
            // 返回当前请求状态的future对象
            return future;
        }

        /**
         * 重置此请求的CompletableFuture。
         * 应用场景: 在重试请求之前，可能需要重置future以便重新发送请求并等待新的结果。
         * 实现细节: 创建一个新的CompletableFuture实例并替换旧的。
         */
        void resetFuture() {
            // 创建一个新的CompletableFuture实例，用于下一次请求尝试
            future = new CompletableFuture<>();
        }

        /**
         * 从待发送的OffsetFetch请求列表中移除此请求。
         * 应用场景: 当请求成功发送或不再需要发送时，调用此方法进行清理。
         * 实现细节: 尝试从unsentOffsetFetchRequests集合中移除当前请求实例。
         */
        @Override
        void removeRequest() {
            // 尝试从待发送的OffsetFetch请求列表中移除当前请求
            if (!unsentOffsetFetchRequests().remove(this)) {
                // 如果在出站缓冲区中未找到要移除的OffsetFetch请求，则记录警告日志
                log.warn("OffsetFetch request to remove not found in the outbound buffer: {}", this);
            }
        }

        /**
         * 处理没有组级别错误的OffsetFetch响应。此方法将查找分区级别的错误并相应地使future失败，同时记录失败的请求尝试。
         * 如果未找到分区级别的错误，则此方法将使用响应中包含的位移完成future，并记录成功的请求尝试。
         * 应用场景: 当OffsetFetch请求成功返回，但可能包含分区级别错误时调用。
         * 实现细节: 遍历响应中的每个分区数据，检查错误，并根据错误类型更新future状态或收集位移信息。
         * 设计考虑: 分离组级别和分区级别错误的处理，使得逻辑更清晰。对不同类型的分区错误进行特定处理。
         * @param currentTimeMs 当前时间戳（毫秒），用于记录成功或失败尝试的时间。
         * @param response OffsetFetch响应对象。
         */
        private void onSuccess(final long currentTimeMs,
                               final OffsetFetchResponse response) {
            // 用于存储未授权的主题名称集合，初始化为null
            Set<String> unauthorizedTopics = null;
            // 从响应中获取特定groupId的分区数据映射
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                    response.partitionDataMap(groupId);
            // 用于存储成功获取到的位移和元数据，初始化大小以优化性能
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            // 用于存储位移提交不稳定的主题分区集合
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            // 标志位，指示是否已记录失败请求尝试
            boolean failedRequestRegistered = false;
            // 遍历响应中的每个分区数据条目
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                // 获取当前处理的主题分区
                TopicPartition tp = entry.getKey();
                // 获取当前分区的响应数据
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                // 如果分区数据包含错误
                if (partitionData.hasError()) {
                    // 获取错误类型
                    Errors error = partitionData.error;
                    // 记录调试日志，表明获取分区位移失败及其原因
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    // 如果尚未记录失败请求尝试
                    if (!failedRequestRegistered) {
                        // 调用onFailedAttempt记录失败尝试
                        onFailedAttempt(currentTimeMs);
                        // 设置标志位为true，表示已记录
                        failedRequestRegistered = true;
                    }

                    // 如果错误是UNKNOWN_TOPIC_OR_PARTITION（未知主题或分区）
                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        // 以KafkaException异常完成future，指示主题或分区不存在
                        future.completeExceptionally(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        // 直接返回，不再处理其他分区
                        return;
                    // 如果错误是TOPIC_AUTHORIZATION_FAILED（主题授权失败）
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        // 如果未授权主题集合为null，则初始化
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        // 将当前主题添加到未授权主题集合中
                        unauthorizedTopics.add(tp.topic());
                    // 如果错误是UNSTABLE_OFFSET_COMMIT（不稳定的位移提交）
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        // 将当前主题分区添加到不稳定位移提交的主题分区集合中
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        // 对于所有其他意外的分区错误（即使它们是可重试的），都以不可重试的KafkaException失败
                        future.completeExceptionally(new KafkaException("Unexpected error in fetch offset " +
                                "response for partition " + tp + ": " + error.message()));
                        // 直接返回，不再处理其他分区
                        return;
                    }
                // 如果分区数据没有错误且位移大于等于0 (表示有效位移)
                } else if (partitionData.offset >= 0) {
                    // 记录位置和位移（-1表示没有已提交的位移可获取）；
                    // 如果没有已提交的位移，则记录为null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    // 如果位移小于0 (例如-1)，表示没有找到已提交的位移
                    log.info("Found no committed offset for partition {}", tp);
                    // 将该分区的位移记录为null
                    offsets.put(tp, null);
                }
            }

            // 如果存在未授权的主题
            if (unauthorizedTopics != null) {
                // 以TopicAuthorizationException异常完成future
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            // 如果存在不稳定位移提交的主题分区
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // TODO: 优化问题：当单个分区出错时，是否需要重试所有分区？
                // 记录信息日志，说明哪些分区的位移在broker端仍不稳定
                log.info("The following partitions still have unstable offsets " +
                        "which are not cleared on the broker side: {}" +
                        ", this could be either " +
                        "transactional offsets waiting for completion, or " +
                        "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                // 以UnstableOffsetCommitException异常完成future
                future.completeExceptionally(new UnstableOffsetCommitException("There are " +
                    "unstable offsets for the requested topic partitions"));
            } else {
                // 如果所有分区都成功获取位移且没有错误
                // 调用onSuccessfulAttempt记录成功尝试
                onSuccessfulAttempt(currentTimeMs);
                // 以获取到的位移集合成功完成future
                future.complete(offsets);
            }
        }

        /**
         * 将当前请求的future链接到另一个future。当当前future完成时，另一个future也会以相同的结果或异常完成。
         * 应用场景: 用于将一个异步操作的结果传递给另一个异步操作，例如在请求合并或重试时。
         * 实现细节: 使用CompletableFuture的whenComplete方法注册一个回调，在当前future完成时触发。
         * @param otherFuture 要链接到的目标CompletableFuture。
         */
        private void chainFuture(
            final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> otherFuture) {
            // 当此future完成时（无论是正常完成还是异常完成）
            this.future.whenComplete((r, t) -> {
                // 如果存在异常 t
                if (t != null) {
                    // 以相同的异常 t 完成 otherFuture
                    otherFuture.completeExceptionally(t);
                } else {
                    // 否则，以正常结果 r 完成 otherFuture
                    otherFuture.complete(r);
                }
            });
        }

        /**
         * 获取此对象的基本字符串表示形式，通常用于日志记录和调试。
         * 此方法覆盖了父类 {@link TimedRequestState#toStringBase()} 的行为，添加了特定于 OffsetFetchRequestState 的信息。
         * @return 此对象的基本字符串表示形式，包含请求的分区信息。
         */
        @Override
        public String toStringBase() {
            // 调用父类的toStringBase()方法获取通用部分，并附加请求的分区信息
            return super.toStringBase() +
                    ", requestedPartitions=" + requestedPartitions;
        }
    }

    /**
     * <p>此类用于暂存未发送的 {@link OffsetCommitRequestState} 和 {@link OffsetFetchRequestState}。
     * <li>unsentOffsetCommits 保存尚未发送的位移提交请求</li>
     * <li>unsentOffsetFetches 保存尚未发送的位移获取请求</li>
     * <li>inflightOffsetFetches 保存已发送但尚未完成的位移获取请求</li>
     * <p>
     * {@code addOffsetFetchRequest} 方法会对请求进行去重，以避免发送相同的请求。
     * 应用场景: 作为CommitRequestManager内部管理待处理请求的核心数据结构。
     * 设计考虑: 使用不同的集合分别管理不同状态的请求，便于逻辑处理和状态跟踪。使用队列来保证位移提交的顺序性。
     */
    class PendingRequests {
        // 使用队列来确保提交的顺序性
        // 存储待发送的位移提交请求队列
        Queue<OffsetCommitRequestState> unsentOffsetCommits = new LinkedList<>();
        // 存储待发送的位移获取请求列表
        List<OffsetFetchRequestState> unsentOffsetFetches = new ArrayList<>();
        // 存储已发送但未收到响应的位移获取请求列表
        List<OffsetFetchRequestState> inflightOffsetFetches = new ArrayList<>();

        /**
         * 检查是否存在未发送的请求（包括位移提交和位移获取请求）。
         * 应用场景: 在poll循环中判断是否需要发送网络请求。
         * @return 如果存在未发送的请求，则返回true；否则返回false。
         */
        // 仅用于测试
        boolean hasUnsentRequests() {
            // 如果未发送的提交请求队列不为空，或者未发送的获取请求列表不为空，则返回true
            return !unsentOffsetCommits.isEmpty() || !unsentOffsetFetches.isEmpty();
        }

        /**
         * 将一个提交请求添加到队列中，以便在下一次调用 {@link #poll(long)} 时发送出去。
         * 此方法用于所有类型的提交（同步、异步、自动提交）。
         * 应用场景: 当应用程序调用commitSync, commitAsync或自动提交触发时，会调用此方法将请求加入待发送队列。
         * @param request 要添加的位移提交请求状态对象。
         * @return 添加到队列的位移提交请求状态对象。
         */
        OffsetCommitRequestState addOffsetCommitRequest(OffsetCommitRequestState request) {
            // 记录调试日志，表明正在将OffsetCommit请求加入队列及其包含的位移信息
            log.debug("Enqueuing OffsetCommit request for offsets: {}", request.offsets);
            // 将请求添加到未发送提交请求的队列中
            unsentOffsetCommits.add(request);
            // 返回添加的请求对象
            return request;
        }

        /**
         * <p>将一个位移获取请求添加到待发送缓冲区。如果已存在相同的请求，我们会将新的 future 链接到现有的 future 上。
         * 应用场景: 当消费者需要获取特定分区的已提交位移时，会调用此方法。例如，在消费者启动或重新平衡后，需要知道从哪里开始消费。
         * 实现细节: 通过检查 `unsentOffsetFetches` (未发送) 和 `inflightOffsetFetches` (已发送但未收到响应) 列表来判断是否存在重复请求。
         * 设计考虑: 避免重复发送相同的位移获取请求，以减少网络开销和服务器负载。通过链接 future，可以确保所有等待相同位移获取结果的调用者都能得到通知。
         *
         * <p>如果请求是新的，它会注册一个回调，在请求完成后将自身从 {@code inflightOffsetFetches} 中移除。
         * @param request 要添加的位移获取请求状态对象
         * @return 表示位移获取操作结果的 CompletableFuture，其中包含主题分区到其位移和元数据的映射
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final OffsetFetchRequestState request) {
            // 检查是否存在重复的未发送的位移获取请求
            Optional<OffsetFetchRequestState> dupe =
                    unsentOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();
            // 检查是否存在重复的已发送但未收到响应的位移获取请求
            Optional<OffsetFetchRequestState> inflight =
                    inflightOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();

            // 如果存在重复请求 (无论是在未发送列表还是已发送列表)
            if (dupe.isPresent() || inflight.isPresent()) {
                // 记录调试信息，表明找到了重复的未发送位移获取请求
                log.debug("发现针对分区 {} 的重复未发送位移获取请求", request.requestedPartitions);
                // 将新请求的 future 链接到现有重复请求的 future 上
                // 如果 dupe 存在，则使用 dupe；否则使用 inflight (通过 orElseGet 获取)
                dupe.orElseGet(inflight::get).chainFuture(request.future);
            } else {
                // 如果没有重复请求，这是一个新的位移获取请求
                // 记录调试信息，表明正在将新的位移获取请求加入队列
                log.debug("将针对分区 {} 的位移获取请求加入队列", request.requestedPartitions);
                // 将新请求添加到未发送的位移获取请求列表中
                this.unsentOffsetFetches.add(request);
            }
            // 返回与此请求关联的 future，调用者可以通过此 future 获取请求结果
            return request.future;
        }


        /**
         * 清空 {@code unsentOffsetCommits}（未发送的位移提交请求），并将 {@code unsentOffsetFetches}（未发送的位移获取请求）中所有可发送的请求
         * 移动到 {@code inflightOffsetFetches}（已发送但未收到响应的位移获取请求）中，以记录所有进行中的请求。
         * 注意：可发送的请求由其计时器确定，因为我们期望在失败尝试后进行退避。请参阅 {@link RequestState}。
         * 应用场景: 此方法在消费者准备向网络客户端发送请求之前被调用，用于收集所有准备就绪的位移提交和获取请求。
         * 实现细节: 该方法会首先处理过期的提交请求，然后筛选出可发送的提交请求和获取请求，并将它们转换为 `NetworkClientDelegate.UnsentRequest` 对象。
         *           不可发送的请求会保留在相应的未发送列表中，等待下次轮询。
         * 设计考虑: 通过集中处理待发送请求，可以有效地管理请求的发送时机，并结合退避策略处理失败的请求，提高系统的健壮性。
         * @param currentTimeMs 当前时间戳（毫秒），用于判断请求是否可以发送（例如，是否已超过退避时间）
         * @return 一个不可修改的 {@link NetworkClientDelegate.UnsentRequest} 列表，包含所有准备发送的位移提交和获取请求
         */
        List<NetworkClientDelegate.UnsentRequest> drain(final long currentTimeMs) {
            // 筛选出当前还不能发送的位移提交请求 (例如，处于退避状态)
            List<OffsetCommitRequestState> unreadyCommitRequests = unsentOffsetCommits.stream()
                .filter(request -> !request.canSendRequest(currentTimeMs)) // 根据 canSendRequest 方法判断
                .collect(Collectors.toList()); // 收集到列表中

            // 处理并移除已过期的未发送位移提交请求
            failAndRemoveExpiredCommitRequests();

            // 收集所有可发送的未发送位移提交请求
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = unsentOffsetCommits.stream()
                .filter(request -> request.canSendRequest(currentTimeMs)) // 筛选出可发送的请求
                .peek(request -> request.onSendAttempt(currentTimeMs)) // 在发送尝试前调用 onSendAttempt，更新请求状态 (如发送时间、尝试次数)
                .map(OffsetCommitRequestState::toUnsentRequest) // 将 OffsetCommitRequestState 转换为 UnsentRequest
                .collect(Collectors.toCollection(ArrayList::new)); // 收集到 ArrayList 中

            // 将未发送的位移获取请求按是否可发送进行分区
            Map<Boolean, List<OffsetFetchRequestState>> partitionedBySendability =
                    unsentOffsetFetches.stream()
                            .collect(Collectors.partitioningBy(request -> request.canSendRequest(currentTimeMs))); // true 分区为可发送，false 分区为不可发送

            // 处理所有可发送的位移获取请求
            for (OffsetFetchRequestState request : partitionedBySendability.get(true)) { // 遍历可发送的请求列表
                request.onSendAttempt(currentTimeMs); // 在发送尝试前调用 onSendAttempt，更新请求状态
                unsentRequests.add(request.toUnsentRequest()); // 将 OffsetFetchRequestState 转换为 UnsentRequest 并添加到待发送列表
                inflightOffsetFetches.add(request); // 将请求添加到已发送但未收到响应的位移获取请求列表中进行跟踪
            }

            // 清空所有未发送的位移提交和获取请求列表 (因为它们要么被处理，要么被移到 unready 列表)
            clearAll();
            // 将之前筛选出的不可发送的位移获取请求重新加回 unsentOffsetFetches 列表
            unsentOffsetFetches.addAll(partitionedBySendability.get(false));
            // 将之前筛选出的不可发送的位移提交请求重新加回 unsentOffsetCommits 列表
            unsentOffsetCommits.addAll(unreadyCommitRequests);

            // 返回收集到的所有可发送请求的不可修改列表
            return Collections.unmodifiableList(unsentRequests);
        }


        /**
         * 查找已过期的未发送提交请求，将其移除，并使用 TimeoutException 完成其 future。
         * 应用场景: 在准备发送请求之前，需要清理掉那些因为超时而不再有效的提交请求。
         * 实现细节: 遍历所有未发送的提交请求，检查它们是否已过期。如果请求过期，其关联的 future 会被 TimeoutException 完成，并且请求会从待处理队列中移除。
         * 设计考虑: 及时处理过期请求可以防止系统资源被无效请求占用，并确保调用者能够及时收到超时通知。
         */
        private void failAndRemoveExpiredCommitRequests() {
            // 创建一个包含所有未发送位移提交请求的队列副本，用于遍历和可能的移除操作，避免在遍历原始集合时进行修改
            Queue<OffsetCommitRequestState> requestsToPurge = new LinkedList<>(unsentOffsetCommits);
            // 遍历队列中的每个请求，并调用其 maybeExpire 方法
            // maybeExpire 方法会检查请求是否已超时，如果超时，则会完成其 future 并将其从 unsentOffsetCommits 中移除
            requestsToPurge.forEach(RetriableRequestState::maybeExpire);
        }


        /**
         * 清空所有未发送的位移提交请求和未发送的位移获取请求列表。
         * 应用场景: 在 `drain` 方法中，当可发送的请求被收集后，或者在协调器发生严重错误需要清空所有待处理请求时调用。
         * 实现细节: 直接调用相应列表的 `clear()` 方法。
         * 设计考虑: 提供一个统一的方法来清空这些列表，简化代码并确保一致性。
         */
        private void clearAll() {
            // 清空未发送的位移提交请求列表
            unsentOffsetCommits.clear();
            // 清空未发送的位移获取请求列表
            unsentOffsetFetches.clear();
        }


        /**
         * 将所有待处理的（未发送的）位移提交请求转换为 {@link NetworkClientDelegate.UnsentRequest} 列表，并清空所有未发送请求列表。
         * 应用场景: 当消费者准备关闭或遇到协调器错误，需要立即处理所有挂起的提交请求时（例如，尝试最后一次提交）。
         *           与 `drain` 方法不同，此方法不检查请求是否可发送 (例如，不考虑退避时间)。
         * 实现细节: 将 `unsentOffsetCommits` 中的所有请求转换为 `UnsentRequest`，然后调用 `clearAll()` 清空未发送列表。
         * 设计考虑: 提供一种快速通道来处理所有待处理的提交请求，忽略正常的发送条件，用于特殊情况下的清理或最后尝试。
         * @return 包含所有待处理位移提交请求的 {@link NetworkClientDelegate.UnsentRequest} 列表
         */
        private List<NetworkClientDelegate.UnsentRequest> drainPendingCommits() {
            // 将所有未发送的位移提交请求转换为 UnsentRequest 对象
            List<NetworkClientDelegate.UnsentRequest> res = unsentOffsetCommits.stream()
                .map(OffsetCommitRequestState::toUnsentRequest) // 对每个请求调用 toUnsentRequest 方法
                .collect(Collectors.toCollection(ArrayList::new)); // 收集到 ArrayList 中
            // 清空所有未发送的位移提交和获取请求列表
            clearAll();
            // 返回转换后的请求列表
            return res;
        }


        /**
         * 检查协调器请求管理器是否报告了严重错误。如果是，则使所有未发送的提交请求和位移获取请求失败，并清空它们。
         * 应用场景: 当与消费者协调器的通信发生不可恢复的错误时（例如，协调器节点丢失或授权失败），需要快速失败所有相关的待处理请求。
         * 实现细节: 获取协调器管理器的 `fatalError()`。如果存在错误，则遍历所有未发送的提交和获取请求，
         *           使用该错误异常地完成它们的 future，然后调用 `clearAll()` 清空列表。
         * 设计考虑: 确保在协调器出现严重问题时，应用程序能够及时知道相关的位移操作已失败，而不是无限期等待。
         */
        private void maybeFailOnCoordinatorFatalError() {
            // 检查协调器请求管理器是否报告了严重错误
            coordinatorRequestManager.fatalError().ifPresent(error -> { // 如果存在严重错误 (error 不为 null)
                    // 记录警告日志，说明由于协调器严重错误，将使所有未发送的提交请求和位移获取请求失败
                    log.warn("由于协调器发生严重错误，所有未发送的提交请求和位移获取请求都将失败。", error);
                    // 遍历所有未发送的位移提交请求，并使用错误信息异常地完成它们的 future
                    unsentOffsetCommits.forEach(request -> request.future.completeExceptionally(error));
                    // 遍历所有未发送的位移获取请求，并使用错误信息异常地完成它们的 future
                    unsentOffsetFetches.forEach(request -> request.future.completeExceptionally(error));
                    // 清空所有未发送的位移提交和获取请求列表
                    clearAll();
                }
            );
        }
    }


    /**
     * 封装自动提交的状态并管理自动提交计时器。
     * 应用场景：当启用消费者自动提交位移时，此类用于跟踪提交间隔和状态。
     * 实现细节：内部使用一个计时器来确定何时应该触发自动提交，并记录是否有正在进行的提交操作。
     * 设计考虑：将自动提交相关的逻辑封装在一个独立的类中，可以使 CommitRequestManager 的主类更简洁，职责更清晰。
     */
    private static class AutoCommitState {
        // 计时器，用于确定下一次自动提交的时间。
        private final Timer timer;
        // 自动提交的时间间隔，单位为毫秒。
        private final long autoCommitInterval;
        // 标志位，指示当前是否有正在进行的提交操作。如果为 true，则表示有提交正在进行，应避免重复提交。
        private boolean hasInflightCommit;

        // 日志记录器，用于记录与自动提交相关的日志信息。
        private final Logger log;

        /**
         * AutoCommitState 的构造函数。
         * @param time 时间工具，用于创建计时器。
         * @param autoCommitInterval 自动提交的时间间隔（毫秒）。
         * @param logContext 日志上下文，用于创建日志记录器。
         */
        public AutoCommitState(
                final Time time, // 时间工具实例
                final long autoCommitInterval, // 自动提交间隔
                final LogContext logContext) { // 日志上下文
            // 初始化自动提交间隔
            this.autoCommitInterval = autoCommitInterval;
            // 使用传入的时间工具和自动提交间隔创建一个新的计时器
            this.timer = time.timer(autoCommitInterval);
            // 初始化时，没有正在进行的提交操作
            this.hasInflightCommit = false;
            // 使用传入的日志上下文创建当前类的日志记录器
            this.log = logContext.logger(getClass());
        }

        /**
         * 判断是否应该执行自动提交。
         * 应用场景：在消费者的轮询逻辑中，会调用此方法来检查是否到了自动提交位移的时间点。
         * 实现细节：首先检查计时器是否已到期，然后检查是否有正在进行的提交。只有当计时器到期且没有正在进行的提交时，才应该执行自动提交。
         * @return 如果应该执行自动提交，则返回 true；否则返回 false。
         */
        public boolean shouldAutoCommit() {
            // 如果计时器尚未到期
            if (!this.timer.isExpired()) {
                // 则不应执行自动提交
                return false;
            }
            // 如果当前有正在进行的提交操作
            if (this.hasInflightCommit) {
                // 记录一条跟踪日志，说明由于上一个提交仍在进行中而跳过本次自动提交
                log.trace("Skipping auto-commit on the interval because a previous one is still in-flight.");
                // 则不应执行自动提交
                return false;
            }
            // 如果计时器已到期且没有正在进行的提交，则应该执行自动提交
            return true;
        }

        /**
         * 重置计时器，使用配置的自动提交间隔。
         * 应用场景：在成功完成一次自动提交后，或者在某些需要重新开始计时的情况下调用。
         * 实现细节：将计时器重置为初始配置的 `autoCommitInterval`。
         */
        public void resetTimer() {
            // 使用配置的自动提交间隔重置计时器
            this.timer.reset(autoCommitInterval);
        }

        /**
         * 重置计时器，使用指定的重试退避时间。
         * 应用场景：当自动提交失败并需要延迟一段时间后重试时调用。
         * 实现细节：将计时器重置为传入的 `retryBackoffMs`。
         * @param retryBackoffMs 重试退避时间（毫秒）。
         */
        public void resetTimer(long retryBackoffMs) {
            // 使用指定的重试退避时间重置计时器
            this.timer.reset(retryBackoffMs);
        }

        /**
         * 获取距离下次计时器到期的剩余时间（毫秒）。
         * 应用场景：可以用于了解距离下一次自动提交还有多长时间。
         * 实现细节：首先更新计时器的当前时间，然后返回剩余时间。
         * @param currentTimeMs 当前时间戳（毫秒）。
         * @return 剩余时间（毫秒）。
         */
        public long remainingMs(final long currentTimeMs) {
            // 使用当前时间更新计时器的状态
            this.timer.update(currentTimeMs);
            // 返回计时器剩余的毫秒数
            return this.timer.remainingMs();
        }

        /**
         * 更新计时器的当前时间。
         * 应用场景：在检查计时器状态（如是否到期、剩余时间）之前，需要调用此方法来同步计时器的内部时间。
         * @param currentTimeMs 当前时间戳（毫秒）。
         */
        public void updateTimer(final long currentTimeMs) {
            // 使用当前时间更新计时器的状态
            this.timer.update(currentTimeMs);
        }

        /**
         * 设置是否有正在进行的提交操作的状态。
         * 应用场景：在开始一次位移提交前，应将此状态设置为 true；在提交完成（成功或失败）后，应设置为 false。
         * @param inflightCommitStatus true 表示有正在进行的提交，false 表示没有。
         */
        public void setInflightCommitStatus(final boolean inflightCommitStatus) {
            // 更新是否有正在进行的提交操作的状态
            this.hasInflightCommit = inflightCommitStatus;
        }
    }

    /**
     * 存储成员ID和成员epoch信息。
     * 应用场景：在消费者加入组后，协调器会分配成员ID和epoch。这些信息在后续的位移提交和获取请求中需要使用。
     * 实现细节：包含一个字符串类型的成员ID和一个可选的整数类型的成员epoch。
     * 设计考虑：将成员相关信息封装起来，便于在请求中传递和管理。
     */
    static class MemberInfo {
        // 消费者的成员ID，由协调器分配。
        String memberId = "";
        // 消费者的成员epoch，表示成员在组中的代次。如果消费者尚未加入组或epoch未知，则为空。
        Optional<Integer> memberEpoch = Optional.empty();

        @Override
        public String toString() {
            return "memberId=" + memberId +
                    ", memberEpoch=" + (memberEpoch.isPresent() ? memberEpoch.get() : "undefined");
        }
    }
}
