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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * {@code RequestManagers} 提供了一种在系统中传递一组 {@link RequestManager} 实例的方法。
 * 这允许调用者既可以使用特定的 {@link RequestManager} 实例，也可以通过 {@link #entries()} 方法遍历列表。
 * 
 * 应用场景:
 *  - 在消费者客户端中，不同的功能（如元数据获取、位移提交、心跳、数据拉取等）由不同的 RequestManager 负责。
 *  - RequestManagers 类将这些 RequestManager 聚合起来，方便统一管理和访问。
 *  - 例如，在关闭消费者时，可以方便地遍历所有 RequestManager 并关闭它们。
 *
 * 设计考虑:
 *  - 使用 Optional 包装可能不存在的 RequestManager，使得代码更健壮，避免空指针异常。
 *  - 提供两个构造函数，分别用于常规消费者和共享消费（KIP-881）场景，体现了设计的灵活性和可扩展性。
 *  - 实现了 Closeable 接口，确保所有管理的 RequestManager 资源能够被正确释放。
 */
public class RequestManagers implements Closeable {

    // 日志记录器，用于记录 RequestManagers 相关的日志信息
    private final Logger log;
    // 可选的 CoordinatorRequestManager，用于管理与协调器的通信，例如查找协调器、加入组等
    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    // 可选的 CommitRequestManager，用于管理位移提交请求
    public final Optional<CommitRequestManager> commitRequestManager;
    // 可选的 ConsumerHeartbeatRequestManager，用于管理消费者心跳请求（KIP-848 之前的旧版心跳）
    public final Optional<ConsumerHeartbeatRequestManager> consumerHeartbeatRequestManager;
    // 可选的 ShareHeartbeatRequestManager，用于管理共享消费者的心跳请求 (KIP-881)
    public final Optional<ShareHeartbeatRequestManager> shareHeartbeatRequestManager;
    // 可选的 ConsumerMembershipManager，用于管理消费者组成员关系 (KIP-848)
    public final Optional<ConsumerMembershipManager> consumerMembershipManager;
    // 可选的 ShareMembershipManager，用于管理共享消费者组成员关系 (KIP-881)
    public final Optional<ShareMembershipManager> shareMembershipManager;
    // OffsetsRequestManager，用于管理获取位移的请求 (例如 list offsets)
    public final OffsetsRequestManager offsetsRequestManager;
    // TopicMetadataRequestManager，用于管理主题元数据请求
    public final TopicMetadataRequestManager topicMetadataRequestManager;
    // FetchRequestManager，用于管理数据拉取请求
    public final FetchRequestManager fetchRequestManager;
    // 可选的 ShareConsumeRequestManager，用于管理共享消费的数据拉取请求 (KIP-881)
    public final Optional<ShareConsumeRequestManager> shareConsumeRequestManager;
    // 存储所有 RequestManager (以 Optional 包装) 的列表，方便统一操作，例如关闭
    private final List<Optional<? extends RequestManager>> entries;
    // 幂等关闭器，确保 close 方法只执行一次，防止资源重复释放
    private final IdempotentCloser closer = new IdempotentCloser();

    /**
     * 构造函数，用于创建常规消费者的 RequestManagers 实例。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param offsetsRequestManager OffsetsRequestManager 实例，不能为空。
     * @param topicMetadataRequestManager TopicMetadataRequestManager 实例。
     * @param fetchRequestManager FetchRequestManager 实例。
     * @param coordinatorRequestManager 可选的 CoordinatorRequestManager 实例。
     * @param commitRequestManager 可选的 CommitRequestManager 实例。
     * @param heartbeatRequestManager 可选的 ConsumerHeartbeatRequestManager 实例。
     * @param membershipManager 可选的 ConsumerMembershipManager 实例。
     */
    public RequestManagers(LogContext logContext,
                           OffsetsRequestManager offsetsRequestManager,
                           TopicMetadataRequestManager topicMetadataRequestManager,
                           FetchRequestManager fetchRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager,
                           Optional<ConsumerHeartbeatRequestManager> heartbeatRequestManager,
                           Optional<ConsumerMembershipManager> membershipManager) {
        // 初始化日志记录器
        this.log = logContext.logger(RequestManagers.class);
        // 初始化 OffsetsRequestManager，并确保其不为 null
        this.offsetsRequestManager = requireNonNull(offsetsRequestManager, "OffsetsRequestManager cannot be null");
        // 初始化 CoordinatorRequestManager
        this.coordinatorRequestManager = coordinatorRequestManager;
        // 初始化 CommitRequestManager
        this.commitRequestManager = commitRequestManager;
        // 初始化 TopicMetadataRequestManager
        this.topicMetadataRequestManager = topicMetadataRequestManager;
        // 初始化 FetchRequestManager
        this.fetchRequestManager = fetchRequestManager;
        // 对于常规消费者，ShareConsumeRequestManager 为空
        this.shareConsumeRequestManager = Optional.empty();
        // 初始化 ConsumerHeartbeatRequestManager
        this.consumerHeartbeatRequestManager = heartbeatRequestManager;
        // 对于常规消费者，ShareHeartbeatRequestManager 为空
        this.shareHeartbeatRequestManager = Optional.empty();
        // 初始化 ConsumerMembershipManager
        this.consumerMembershipManager = membershipManager;
        // 对于常规消费者，ShareMembershipManager 为空
        this.shareMembershipManager = Optional.empty();

        // 创建一个列表用于存储所有的 RequestManager
        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        // 添加 CoordinatorRequestManager (如果存在)
        list.add(coordinatorRequestManager);
        // 添加 CommitRequestManager (如果存在)
        list.add(commitRequestManager);
        // 添加 ConsumerHeartbeatRequestManager (如果存在)
        list.add(heartbeatRequestManager);
        // 添加 ConsumerMembershipManager (如果存在)
        list.add(membershipManager);
        // 添加 OffsetsRequestManager (必须存在，所以用 Optional.of 包装)
        list.add(Optional.of(offsetsRequestManager));
        // 添加 TopicMetadataRequestManager (必须存在，所以用 Optional.of 包装)
        list.add(Optional.of(topicMetadataRequestManager));
        // 添加 FetchRequestManager (必须存在，所以用 Optional.of 包装)
        list.add(Optional.of(fetchRequestManager));
        // 将列表设置为不可修改，确保 entries 集合的不可变性，增加线程安全性
        entries = Collections.unmodifiableList(list);
    }

    /**
     * 构造函数，用于创建共享消费者 (KIP-881) 的 RequestManagers 实例。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param shareConsumeRequestManager ShareConsumeRequestManager 实例。
     * @param coordinatorRequestManager 可选的 CoordinatorRequestManager 实例。
     * @param shareHeartbeatRequestManager 可选的 ShareHeartbeatRequestManager 实例。
     * @param shareMembershipManager 可选的 ShareMembershipManager 实例。
     */
    public RequestManagers(LogContext logContext,
                           ShareConsumeRequestManager shareConsumeRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<ShareHeartbeatRequestManager> shareHeartbeatRequestManager,
                           Optional<ShareMembershipManager> shareMembershipManager) {
        // 初始化日志记录器
        this.log = logContext.logger(RequestManagers.class);
        // 初始化 ShareConsumeRequestManager (必须存在，所以用 Optional.of 包装)
        this.shareConsumeRequestManager = Optional.of(shareConsumeRequestManager);
        // 初始化 CoordinatorRequestManager
        this.coordinatorRequestManager = coordinatorRequestManager;
        // 对于共享消费者，CommitRequestManager 为空 (共享消费不直接管理位移提交)
        this.commitRequestManager = Optional.empty();
        // 对于共享消费者，ConsumerHeartbeatRequestManager 为空
        this.consumerHeartbeatRequestManager = Optional.empty();
        // 初始化 ShareHeartbeatRequestManager
        this.shareHeartbeatRequestManager = shareHeartbeatRequestManager;
        // 对于共享消费者，ConsumerMembershipManager 为空
        this.consumerMembershipManager = Optional.empty();
        // 初始化 ShareMembershipManager
        this.shareMembershipManager = shareMembershipManager;
        // 对于共享消费者，OffsetsRequestManager 为 null (不直接使用)
        this.offsetsRequestManager = null;
        // 对于共享消费者，TopicMetadataRequestManager 为 null (不直接使用)
        this.topicMetadataRequestManager = null;
        // 对于共享消费者，FetchRequestManager 为 null (不直接使用，由 ShareConsumeRequestManager 替代)
        this.fetchRequestManager = null;

        // 创建一个列表用于存储所有的 RequestManager
        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        // 添加 CoordinatorRequestManager (如果存在)
        list.add(coordinatorRequestManager);
        // 添加 ShareHeartbeatRequestManager (如果存在)
        list.add(shareHeartbeatRequestManager);
        // 添加 ShareMembershipManager (如果存在)
        list.add(shareMembershipManager);
        // 添加 ShareConsumeRequestManager (必须存在，所以用 Optional.of 包装)
        list.add(Optional.of(shareConsumeRequestManager));
        // 将列表设置为不可修改，确保 entries 集合的不可变性
        entries = Collections.unmodifiableList(list);
    }

    /**
     * 返回所有 RequestManager (以 Optional 包装) 的不可修改列表。
     * 允许外部代码遍历所有 RequestManager，例如在关闭时统一处理。
     *
     * @return 包含所有 RequestManager 的不可修改列表。
     */
    public List<Optional<? extends RequestManager>> entries() {
        // 返回存储所有 RequestManager 的不可修改列表
        return entries;
    }

    /**
     * @method close
     * @brief 关闭 RequestManagers 实例，并释放其管理的所有 RequestManager 资源。
     *
     * 应用场景:
     *  - 当 KafkaConsumer 关闭时，会调用此方法来确保所有相关的请求管理器都被正确关闭，释放网络连接、线程等资源。
     *
     * 实现细节:
     *  - 使用 {@link IdempotentCloser} 来确保关闭操作只执行一次，防止重复关闭导致的潜在问题。
     *  - 遍历 {@link #entries} 列表中的所有 RequestManager。
     *  - 过滤出实际存在（Optional 不为空）且实现了 {@link Closeable} 接口的 RequestManager。
     *  - 对每个符合条件的 RequestManager 调用 {@link org.apache.kafka.common.utils.Utils#closeQuietly(Closeable, String)} 方法进行安静关闭，忽略关闭过程中可能抛出的异常，并记录日志。
     *
     * 设计考虑:
     *  - 幂等关闭：避免多次关闭操作可能引发的副作用。
     *  - 统一管理：通过遍历 entries 列表，可以方便地关闭所有注册的 RequestManager，简化了资源管理逻辑。
     *  - 静默关闭：在关闭单个 RequestManager 时忽略异常，确保即使某个管理器关闭失败，其他管理器也能继续关闭，增强了系统的健壮性。
     */
    @Override
    public void close() {
        // 使用 IdempotentCloser 确保 close 方法只被执行一次
        closer.close(
                () -> { // 定义关闭时执行的逻辑
                    // 记录调试日志，表示开始关闭 RequestManagers
                    log.debug("Closing RequestManagers");

                    // 遍历所有注册的 RequestManager
                    entries.stream()
                            // 过滤掉空的 Optional 对象，确保 RequestManager 实例存在
                            .filter(Optional::isPresent)
                            // 获取 Optional 中的 RequestManager 实例
                            .map(Optional::get)
                            // 过滤出实现了 Closeable 接口的 RequestManager
                            .filter(rm -> rm instanceof Closeable)
                            // 将 RequestManager 实例转换为 Closeable 类型
                            .map(rm -> (Closeable) rm)
                            // 对每个 Closeable 实例调用 closeQuietly 方法进行关闭，并传入类名用于日志记录
                            .forEach(c -> closeQuietly(c, c.getClass().getSimpleName()));
                    // 记录调试日志，表示 RequestManagers 已成功关闭
                    log.debug("RequestManagers has been closed");
                },
                () -> 
                    // 如果已经关闭过，则记录调试日志，表示 RequestManagers 已经被关闭
                    log.debug("RequestManagers was already closed")
        );
    }

    /**
     * @method supplier
     * @brief 创建一个 {@link Supplier}，用于在 {@link AsyncKafkaConsumer} 调用时延迟创建 {@code RequestManagers} 实例。
     * 这个方法主要用于常规消费者（非共享消费者）的场景。
     *
     * 应用场景:
     *  - 在 {@link AsyncKafkaConsumer} 初始化过程中，需要创建一组 {@link RequestManager} 来处理各种后台任务，如心跳、位移提交、数据拉取等。
     *  - 使用 {@link Supplier} 可以实现延迟初始化，即只有在实际需要时才创建这些管理器，可能有助于优化启动性能或资源使用。
     *
     * 实现细节:
     *  - 返回一个 {@link CachedSupplier} 实例，它会缓存第一次创建的 {@code RequestManagers} 对象，后续调用直接返回缓存对象。
     *  - 在 {@code create()} 方法内部，首先获取 {@link NetworkClientDelegate} 和 {@link FetchConfig}。
     *  - 初始化核心的 {@link FetchRequestManager} 和 {@link TopicMetadataRequestManager}。
     *  - 如果配置了消费者组ID ({@code groupRebalanceConfig.groupId != null})，则会进一步初始化与组管理相关的管理器：
     *    - {@link CoordinatorRequestManager}: 用于与协调器通信。
     *    - {@link CommitRequestManager}: 用于处理位移提交。
     *    - {@link ConsumerMembershipManager}: 用于管理消费者组成员关系 (KIP-848)。
     *    - {@link ConsumerHeartbeatRequestManager}: 用于发送心跳 (旧版心跳机制)。
     *  - 如果启用了客户端遥测 ({@code clientTelemetryReporter.isPresent()}), 则更新遥测报告器中的组员ID标签。
     *  - 将 {@link CommitRequestManager} 和应用线程的成员状态监听器注册到 {@link ConsumerMembershipManager}。
     *  - 初始化 {@link OffsetsRequestManager}，用于处理获取位移的请求。
     *  - 最后，使用所有初始化好的（或可能为null的）管理器实例来创建一个新的 {@code RequestManagers} 对象。
     *
     * 设计考虑:
     *  - 延迟加载与缓存: {@link CachedSupplier} 确保 {@code RequestManagers} 及其包含的各种管理器只被创建一次，提高了效率。
     *  - 模块化: 将不同功能的请求管理逻辑封装在各自的 {@code RequestManager} 实现中，使得 {@code RequestManagers} 类本身更像一个聚合器和协调者。
     *  - 条件初始化: 某些管理器（如与组管理相关的）仅在特定条件下（如配置了组ID）才会被创建，避免了不必要的资源消耗。
     *  - 依赖注入: 许多必要的组件（如 {@code Time}, {@code LogContext}, {@code ConsumerConfig} 等）作为参数传入，方便测试和配置。
     *  - 参数众多: {@code @SuppressWarnings({"checkstyle:ParameterNumber"})} 注解表明该方法参数较多，这是由于构建完整的 {@code RequestManagers} 需要多种配置和上下文信息。
     *
     * @param time 时间工具，用于获取当前时间戳和处理超时。
     * @param logContext 日志上下文，用于创建和管理日志记录器。
     * @param backgroundEventHandler 后台事件处理器，用于在后台线程和应用线程之间传递事件。
     * @param metadata 消费者元数据，包含集群、主题、分区等信息。
     * @param subscriptions 订阅状态，跟踪消费者当前订阅的主题和分区。
     * @param fetchBuffer 拉取缓冲区，用于存储从broker拉取到的数据。
     * @param config 消费者配置信息。
     * @param groupRebalanceConfig 组再均衡相关的配置，如组ID、实例ID等。
     * @param apiVersions API版本管理器，用于跟踪broker支持的API版本。
     * @param fetchMetricsManager 拉取指标管理器，用于收集和报告与数据拉取相关的指标。
     * @param networkClientDelegateSupplier {@link NetworkClientDelegate} 的供应器，提供网络通信能力。
     * @param clientTelemetryReporter 可选的客户端遥测报告器，用于发送客户端指标数据。
     * @param metrics Kafka指标注册表。
     * @param offsetCommitCallbackInvoker 位移提交回调调用器，用于在应用线程中执行位移提交回调。
     * @param applicationThreadMemberStateListener 应用线程的成员状态监听器，用于响应成员状态变化。
     * @return 一个 {@link Supplier<RequestManagers>} 实例，用于延迟创建 {@code RequestManagers}。
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public static Supplier<RequestManagers> supplier(final Time time,
                                                     final LogContext logContext,
                                                     final BackgroundEventHandler backgroundEventHandler,
                                                     final ConsumerMetadata metadata,
                                                     final SubscriptionState subscriptions,
                                                     final FetchBuffer fetchBuffer,
                                                     final ConsumerConfig config,
                                                     final GroupRebalanceConfig groupRebalanceConfig,
                                                     final ApiVersions apiVersions,
                                                     final FetchMetricsManager fetchMetricsManager,
                                                     final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                                     final Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                                     final Metrics metrics,
                                                     final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
                                                     final MemberStateListener applicationThreadMemberStateListener
                                                     ) {
        // 返回一个 CachedSupplier 实例，用于延迟创建和缓存 RequestManagers 对象
        return new CachedSupplier<>() {
            @Override
            protected RequestManagers create() {
                // 从供应器获取 NetworkClientDelegate 实例，用于网络通信
                final NetworkClientDelegate networkClientDelegate = networkClientDelegateSupplier.get();
                // 根据消费者配置创建 FetchConfig 对象，包含拉取相关的配置
                final FetchConfig fetchConfig = new FetchConfig(config);
                // 从配置中获取重试退避时间（毫秒）
                long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                // 从配置中获取最大重试退避时间（毫秒）
                long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
                // 从配置中获取请求超时时间（毫秒）
                final int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                // 从配置中获取默认API超时时间（毫秒）
                final int defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

                // 创建 FetchRequestManager 实例，用于管理数据拉取请求
                final FetchRequestManager fetch = new FetchRequestManager(logContext,
                        time,
                        metadata,
                        subscriptions,
                        fetchConfig,
                        fetchBuffer,
                        fetchMetricsManager,
                        networkClientDelegate,
                        apiVersions);
                // 创建 TopicMetadataRequestManager 实例，用于管理主题元数据请求
                final TopicMetadataRequestManager topic = new TopicMetadataRequestManager(
                        logContext,
                        time,
                        config);
                // 初始化 ConsumerHeartbeatRequestManager 为 null，后续可能根据条件创建
                ConsumerHeartbeatRequestManager heartbeatRequestManager = null;
                // 初始化 ConsumerMembershipManager 为 null，后续可能根据条件创建
                ConsumerMembershipManager membershipManager = null;
                // 初始化 CoordinatorRequestManager 为 null，后续可能根据条件创建
                CoordinatorRequestManager coordinator = null;
                // 初始化 CommitRequestManager 为 null，后续可能根据条件创建
                CommitRequestManager commitRequestManager = null;

                // 检查是否配置了组ID，如果配置了，则初始化与组管理相关的管理器
                if (groupRebalanceConfig != null && groupRebalanceConfig.groupId != null) {
                    // 获取服务器端分配器配置，可能为空
                    Optional<String> serverAssignor = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
                    // 创建 CoordinatorRequestManager 实例，用于管理与协调器的通信
                    coordinator = new CoordinatorRequestManager(
                            logContext,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            groupRebalanceConfig.groupId);
                    // 创建 CommitRequestManager 实例，用于管理位移提交请求
                    commitRequestManager = new CommitRequestManager(
                            time,
                            logContext,
                            subscriptions,
                            config,
                            coordinator, // 依赖 CoordinatorRequestManager
                            offsetCommitCallbackInvoker,
                            groupRebalanceConfig.groupId,
                            groupRebalanceConfig.groupInstanceId,
                            metrics,
                            metadata);
                    // 创建 ConsumerMembershipManager 实例，用于管理消费者组成员身份 (KIP-848)
                    membershipManager = new ConsumerMembershipManager(
                            groupRebalanceConfig.groupId,
                            groupRebalanceConfig.groupInstanceId,
                            groupRebalanceConfig.rebalanceTimeoutMs,
                            serverAssignor,
                            subscriptions,
                            commitRequestManager, // 依赖 CommitRequestManager
                            metadata,
                            logContext,
                            backgroundEventHandler,
                            time,
                            metrics);

                    // 更新客户端遥测报告器中的组成员ID标签。
                    // 根据 KIP-1082，消费者将生成成员ID作为进程的化身ID。
                    // 因此，我们可以在初始化期间更新组成员ID。
                    if (clientTelemetryReporter.isPresent()) {
                        // 如果客户端遥测报告器存在
                        clientTelemetryReporter.get()
                            // 更新指标标签，添加 GROUP_MEMBER_ID 和其对应的值
                            .updateMetricsLabels(Map.of(ClientTelemetryProvider.GROUP_MEMBER_ID, membershipManager.memberId()));
                    }

                    // 将 CommitRequestManager 注册为成员状态监听器
                    membershipManager.registerStateListener(commitRequestManager);
                    // 将应用线程的成员状态监听器注册到成员管理器
                    membershipManager.registerStateListener(applicationThreadMemberStateListener);
                    // 创建 ConsumerHeartbeatRequestManager 实例，用于管理消费者心跳 (旧版心跳机制)
                    heartbeatRequestManager = new ConsumerHeartbeatRequestManager(
                            logContext,
                            time,
                            config,
                            coordinator, // 依赖 CoordinatorRequestManager
                            subscriptions,
                            membershipManager, // 依赖 ConsumerMembershipManager
                            backgroundEventHandler,
                            metrics);
                }

                // 创建 OffsetsRequestManager 实例，用于管理获取位移的请求 (例如 list offsets)
                final OffsetsRequestManager listOffsets = new OffsetsRequestManager(subscriptions,
                    metadata,
                    fetchConfig.isolationLevel,
                    time,
                    retryBackoffMs,
                    requestTimeoutMs,
                    defaultApiTimeoutMs,
                    apiVersions,
                    networkClientDelegate,
                    commitRequestManager, // 可能为 null，OffsetsRequestManager 会处理这种情况
                    logContext);

                // 创建并返回 RequestManagers 实例，聚合所有创建的管理器
                return new RequestManagers(
                        logContext,
                        listOffsets,
                        topic,
                        fetch,
                        Optional.ofNullable(coordinator), // CoordinatorRequestManager 可能为 null
                        Optional.ofNullable(commitRequestManager), // CommitRequestManager 可能为 null
                        Optional.ofNullable(heartbeatRequestManager), // ConsumerHeartbeatRequestManager 可能为 null
                        Optional.ofNullable(membershipManager) // ConsumerMembershipManager 可能为 null
                );
            }
        };
    }

    /**
     * @method supplier
     * @brief 创建一个 {@link Supplier}，用于在 {@link ShareConsumerImpl} 调用时延迟创建 {@code RequestManagers} 实例。
     * 这个方法专门为共享消费者（KIP-881）场景设计，用于提供一套适用于共享消费模式的请求管理器。
     *
     * 应用场景:
     *  - 在 {@link ShareConsumerImpl} (共享消费者实现) 初始化过程中，需要创建一组特定的 {@link RequestManager} 来处理共享消费相关的后台任务，
     *    例如与协调器的通信、组成员管理、心跳发送以及共享数据拉取等。
     *  - 使用 {@link Supplier} (特别是 {@link CachedSupplier}) 可以实现延迟初始化和缓存，即只有在实际需要时才创建这些管理器，
     *    并确保它们只被创建一次，有助于优化启动性能和资源使用。
     *
     * 实现细节:
     *  - 返回一个 {@link CachedSupplier} 实例，它会缓存第一次通过其 {@code create()} 方法创建的 {@code RequestManagers} 对象，
     *    后续调用 {@code get()} 方法将直接返回缓存的对象。
     *  - 在 {@code create()} 方法内部，会根据传入的配置和上下文信息，逐步初始化共享消费所需的各个管理器：
     *    - {@link CoordinatorRequestManager}: 用于管理与消费者组协调器的通信。
     *    - {@link ShareMembershipManager}: 用于管理共享消费者的组成员关系 (KIP-881 特有)。
     *    - 如果配置了客户端遥测 ({@code clientTelemetryReporter.isPresent()}), 则会更新遥测报告器中的组员ID标签。
     *      根据 KIP-1082，消费者将生成成员ID作为进程的化身ID，因此可以在初始化期间更新组成员ID。
     *    - {@link ShareHeartbeatRequestManager}: 用于管理共享消费者的心跳发送 (KIP-881 特有)。
     *    - {@link ShareConsumeRequestManager}: 用于管理共享消费者的数据拉取逻辑 (KIP-881 特有)。
     *  - {@link ShareConsumeRequestManager} 会被注册为 {@link ShareMembershipManager} 的状态监听器，以便在成员状态变化时得到通知并采取相应行动。
     *  - 最后，使用所有初始化好的管理器实例来创建一个新的 {@code RequestManagers} 对象，这个对象专门配置用于共享消费场景。
     *
     * 设计考虑:
     *  - 延迟加载与缓存: {@link CachedSupplier} 确保 {@code RequestManagers} 及其包含的各种管理器只被创建一次，提高了效率并避免了不必要的重复初始化。
     *  - 模块化: 将不同功能的请求管理逻辑封装在各自的 {@code RequestManager} 实现中，使得 {@code RequestManagers} 类本身更像一个聚合器和协调者，
     *    这个 {@code supplier} 方法则负责根据共享消费的需求来组装这些模块。
     *  - 依赖注入: 众多必要的组件（如 {@code Time}, {@code LogContext}, {@code ConsumerConfig} 等）作为参数传入，方便测试、配置和管理这些依赖。
     *  - 共享消费特化: 此版本的 {@code supplier} 方法及其内部逻辑明确针对 KIP-881 引入的共享消费模型，使用了如 {@code ShareFetchBuffer}、
     *    {@code ShareMembershipManager}、{@code ShareHeartbeatRequestManager} 和 {@code ShareConsumeRequestManager} 等共享消费特有的组件。
     *  - 参数数量: {@code @SuppressWarnings({"checkstyle:ParameterNumber"})} 注解表明该方法参数较多，这是因为构建一个完整的、适用于特定场景的
     *    {@code RequestManagers} 实例需要多种配置和上下文信息。
     *
     * @param time 时间工具，用于获取当前时间戳和处理超时。
     * @param logContext 日志上下文，用于创建和管理日志记录器。
     * @param backgroundEventHandler 后台事件处理器，用于在后台线程和应用线程之间传递事件。
     * @param metadata 消费者元数据，包含集群、主题、分区等信息。
     * @param subscriptions 订阅状态，跟踪消费者当前订阅的主题和分区。
     * @param fetchBuffer 共享拉取缓冲区 ({@link ShareFetchBuffer})，用于存储从broker拉取到的共享数据。
     * @param config 消费者配置信息 ({@link ConsumerConfig})。
     * @param groupRebalanceConfig 组再均衡相关的配置，如组ID、实例ID等。
     * @param shareFetchMetricsManager 共享拉取指标管理器 ({@link ShareFetchMetricsManager})，用于收集和报告与共享数据拉取相关的指标。
     * @param clientTelemetryReporter 可选的客户端遥测报告器 ({@link ClientTelemetryReporter})，用于发送客户端指标数据。
     * @param metrics Kafka指标注册表 ({@link Metrics})。
     * @return 一个 {@link Supplier<RequestManagers>} 实例，用于延迟创建适用于共享消费的 {@code RequestManagers}。
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public static Supplier<RequestManagers> supplier(final Time time, // 时间工具实例
                                                     final LogContext logContext, // 日志上下文实例
                                                     final BackgroundEventHandler backgroundEventHandler, // 后台事件处理器实例
                                                     final ConsumerMetadata metadata, // 消费者元数据实例
                                                     final SubscriptionState subscriptions, // 订阅状态实例
                                                     final ShareFetchBuffer fetchBuffer, // 共享拉取缓冲区实例
                                                     final ConsumerConfig config, // 消费者配置实例
                                                     final GroupRebalanceConfig groupRebalanceConfig, // 组再均衡配置实例
                                                     final ShareFetchMetricsManager shareFetchMetricsManager, // 共享拉取指标管理器实例
                                                     final Optional<ClientTelemetryReporter> clientTelemetryReporter, // 可选的客户端遥测报告器
                                                     final Metrics metrics // Kafka 指标注册表实例
    ) {
        // 返回一个新的 CachedSupplier 实例，用于延迟创建和缓存 RequestManagers
        return new CachedSupplier<>() {
            @Override
            protected RequestManagers create() { // 定义创建 RequestManagers 实例的逻辑
                // 从配置中获取重试退避时间（毫秒）
                long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                // 从配置中获取最大重试退避时间（毫秒）
                long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
                // 根据消费者配置创建 FetchConfig 实例，用于数据拉取相关的配置
                FetchConfig fetchConfig = new FetchConfig(config);

                // 创建 CoordinatorRequestManager 实例，用于管理与协调器的通信
                CoordinatorRequestManager coordinator = new CoordinatorRequestManager(
                        logContext, // 日志上下文
                        retryBackoffMs, // 重试退避时间
                        retryBackoffMaxMs, // 最大重试退避时间
                        groupRebalanceConfig.groupId); // 消费者组ID
                // 创建 ShareMembershipManager 实例，用于管理共享消费者的组成员关系
                ShareMembershipManager shareMembershipManager = new ShareMembershipManager(
                        logContext, // 日志上下文
                        groupRebalanceConfig.groupId, // 消费者组ID
                        null, // 静态成员ID，此处为 null，表示动态成员或由内部生成
                        subscriptions, // 订阅状态
                        metadata, // 消费者元数据
                        time, // 时间工具
                        metrics); // 指标注册表

                // 更新客户端遥测报告器中的组员ID标签。
                // 根据 KIP-1082，消费者将生成成员ID作为进程的化身ID。
                // 因此，我们可以在初始化期间更新组成员ID。
                clientTelemetryReporter.ifPresent(telemetryReporter -> telemetryReporter // 如果客户端遥测报告器存在
                    .updateMetricsLabels(Map.of(ClientTelemetryProvider.GROUP_MEMBER_ID, shareMembershipManager.memberId()))); // 则更新指标标签，将组员ID设置为 ShareMembershipManager 生成的成员ID

                // 创建 ShareHeartbeatRequestManager 实例，用于管理共享消费者的心跳
                ShareHeartbeatRequestManager shareHeartbeatRequestManager = new ShareHeartbeatRequestManager(
                        logContext, // 日志上下文
                        time, // 时间工具
                        config, // 消费者配置
                        coordinator, // 协调器请求管理器
                        subscriptions, // 订阅状态
                        shareMembershipManager, // 共享成员资格管理器
                        backgroundEventHandler, // 后台事件处理器
                        metrics); // 指标注册表
                // 创建 ShareConsumeRequestManager 实例，用于管理共享消费的数据拉取
                ShareConsumeRequestManager shareConsumeRequestManager = new ShareConsumeRequestManager(
                        time, // 时间工具
                        logContext, // 日志上下文
                        groupRebalanceConfig.groupId, // 消费者组ID
                        metadata, // 消费者元数据
                        subscriptions, // 订阅状态
                        fetchConfig, // 拉取配置
                        fetchBuffer, // 共享拉取缓冲区
                        backgroundEventHandler, // 后台事件处理器
                        shareFetchMetricsManager, // 共享拉取指标管理器
                        retryBackoffMs, // 重试退避时间
                        retryBackoffMaxMs); // 最大重试退避时间
                // 将 ShareConsumeRequestManager 注册为 ShareMembershipManager 的状态监听器
                // 这样当成员状态发生变化时，ShareConsumeRequestManager 可以得到通知并做出相应处理
                shareMembershipManager.registerStateListener(shareConsumeRequestManager);

                // 创建并返回一个新的 RequestManagers 实例，专门用于共享消费者
                // 这个构造函数接收共享消费特定的管理器
                return new RequestManagers(
                        logContext, // 日志上下文
                        shareConsumeRequestManager, // 共享消费请求管理器
                        Optional.of(coordinator), // 协调器请求管理器 (包装在 Optional 中)
                        Optional.of(shareHeartbeatRequestManager), // 共享心跳请求管理器 (包装在 Optional 中)
                        Optional.of(shareMembershipManager) // 共享成员资格管理器 (包装在 Optional 中)
                );
            }
        };
    }
}
