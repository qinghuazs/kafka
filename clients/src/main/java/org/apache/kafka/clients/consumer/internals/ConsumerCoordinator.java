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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS;
import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;

/**
 *此类管理与消费者协调器的协调过程。
 * 应用场景：作为消费者客户端的核心组件，负责与服务端的协调器（通常是GroupCoordinator）通信，
 * 管理消费者组的成员状态、分区分配、位移提交等。
 * 实现细节：继承自 AbstractCoordinator，封装了与协调器交互的通用逻辑，并针对消费者的特性进行了扩展。
 * 设计考虑：将协调逻辑集中处理，简化了消费者其他部分的实现，提高了代码的模块化和可维护性。
 */
public final class ConsumerCoordinator extends AbstractCoordinator { // ConsumerCoordinator 类的定义，继承自 AbstractCoordinator，负责消费者与协调器之间的协调工作。
    /**
     * TopicPartition 比较器，用于对 TopicPartition 对象进行排序。
     * 应用场景：在需要对 TopicPartition 集合进行排序或比较时使用，例如在处理分区分配结果时。
     * 实现细节：这是一个静态常量，确保全局只有一个比较器实例。
     */
    private static final TopicPartitionComparator COMPARATOR = new TopicPartitionComparator();

    /**
     * 消费者组重平衡配置。
     * 应用场景：存储与消费者组重平衡相关的配置信息，如会话超时时间、心跳间隔等。
     * 实现细节：通过构造函数注入，在协调器初始化时确定。
     */
    private final GroupRebalanceConfig rebalanceConfig;
    /**
     * 日志记录器。
     * 应用场景：用于记录协调器运行过程中的日志信息，方便调试和问题排查。
     * 实现细节：通过 LogContext 获取特定于 ConsumerCoordinator 类的日志实例。
     */
    private final Logger log;
    /**
     * 分区分配器列表。
     * 应用场景：存储消费者配置的分区分配策略实现类。在重平衡时，leader 会使用这些分配器来决定如何将分区分配给组内成员。
     * 实现细节：在协调器初始化时通过构造函数传入。
     */
    private final List<ConsumerPartitionAssignor> assignors;
    /**
     * 消费者元数据。
     * 应用场景：提供对集群元数据（如主题、分区、broker信息）的访问。
     * 实现细节：协调器依赖此对象获取集群的最新状态。
     */
    private final ConsumerMetadata metadata;
    /**
     * 消费者协调器度量指标。
     * 应用场景：收集和报告与协调器操作相关的度量数据，如提交延迟、重平衡时间等。
     * 实现细节：用于监控协调器的性能和健康状况。
     */
    private final ConsumerCoordinatorMetrics coordinatorMetrics;
    /**
     * 订阅状态。
     * 应用场景：维护消费者当前的订阅信息（订阅的主题、正则表达式、分配到的分区等）以及这些分区的消费状态（如位移、暂停状态）。
     * 实现细节：协调器通过此对象了解消费者的订阅情况和消费进度。
     */
    private final SubscriptionState subscriptions;
    /**
     * 默认的位移提交回调。
     * 应用场景：当异步提交位移且用户未提供自定义回调时，使用此默认回调处理提交结果。
     * 实现细节：通常用于记录提交成功或失败的日志。
     */
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    /**
     * 是否启用自动提交位移。
     * 应用场景：标记消费者是否配置为自动定期提交消费位移。
     * 实现细节：布尔型标志，影响协调器是否启动自动提交任务。
     */
    private final boolean autoCommitEnabled;
    /**
     * 自动提交位移的时间间隔（毫秒）。
     * 应用场景：当启用自动提交时，此值定义了提交操作的频率。
     * 实现细节：整型值，用于设置自动提交定时器。
     */
    private final int autoCommitIntervalMs;
    /**
     * 消费者拦截器。
     * 应用场景：允许用户在消费者接收消息或提交位移等关键点执行自定义逻辑。
     * 实现细节：在协调器处理相关操作时，会调用这些拦截器的方法。
     */
    private final ConsumerInterceptors<?, ?> interceptors;
    // 跟踪需要调用回调的异步提交数量
    // 包私有，用于测试
    /**
     * 在途异步提交计数器。
     * 应用场景：跟踪当前有多少个异步位移提交请求正在处理中，并且其回调尚未被调用。
     * 实现细节：使用 AtomicInteger 保证线程安全更新。
     * 设计考虑：用于确保所有异步提交的回调都被正确执行，尤其是在关闭消费者或发生重平衡时。
     */
    final AtomicInteger inFlightAsyncCommits;
    // 跟踪等待协调器查找完成的挂起异步提交数量
    /**
     * 等待协调器查找的挂起异步提交计数器。
     * 应用场景：在协调器节点未知或正在重新发现时，异步提交请求可能会被挂起，此计数器跟踪这些挂起的请求数量。
     * 实现细节：使用 AtomicInteger 保证线程安全更新。
     * 设计考虑：用于管理在协调器不稳定期间的异步提交行为。
     */
    private final AtomicInteger pendingAsyncCommits;

    // 此集合必须是线程安全的，因为它会从位移提交请求的响应处理程序中修改，
    // 而响应处理程序可能从心跳线程中调用
    /**
     * 已完成的位移提交队列。
     * 应用场景：存储已完成的异步位移提交操作及其结果，等待后续处理（如调用用户回调）。
     * 实现细节：使用 ConcurrentLinkedQueue 保证多线程环境下的安全访问，因为提交响应可能由心跳线程处理。
     * 设计考虑：解耦提交响应处理和用户回调执行，允许它们在不同线程中进行。
     */
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;
    /**
     * 异步提交是否被隔离的标志。
     * 应用场景：当消费者实例因为成员 ID 冲突等原因被协调器隔离（fenced）时，此标志设为 true，后续的异步提交将失败。
     * 实现细节：使用 AtomicBoolean 保证线程安全更新。
     * 设计考虑：防止被隔离的消费者实例继续提交位移，确保消费者组的正确性。
     */
    private final AtomicBoolean asyncCommitFenced;
    /**
     * 当获取稳定位移（fetch stable offsets）不受支持时是否抛出异常的标志。
     * 应用场景：控制在事务性消费场景下，如果 broker 不支持获取稳定LSO（Last Stable Offset），消费者的行为。
     * 实现细节：布尔型配置，影响 `offsetsForTimes` 等操作的行为。
     */
    private final boolean throwOnFetchStableOffsetsUnsupported;
    /**
     * 消费者的机架 ID (rack ID)。
     * 应用场景：用于 Kafka 的机架感知功能，帮助优化副本放置和消费者获取数据的路径，以减少跨机架流量。
     * 实现细节：Optional 类型，如果未配置则为空。
     */
    private final Optional<String> rackId;
    /**
     * 当前消费者是否为消费者组的 leader。
     * 应用场景：标记此消费者实例是否在当前一代（generation）中被选举为 leader。
     * Leader 负责执行分区分配逻辑。
     * 实现细节：布尔型标志，在成功加入组并成为 leader 后设置为 true。
     */
    private boolean isLeader = false;
    /**
     * 已加入的订阅主题集合。
     * 应用场景：记录消费者成功加入组时所基于的订阅信息（通常是所有订阅的主题名称）。
     * 实现细节：在 `onJoinComplete` 时更新，用于后续的协议处理和状态检查。
     */
    private Set<String> joinedSubscription;
    /**
     * 元数据快照。
     * 应用场景：在重平衡开始时，捕获当前集群元数据的一个快照，供分区分配器使用。
     * 实现细节：包含了机架信息、订阅信息、集群元数据和元数据版本。
     * 设计考虑：确保在分区分配过程中使用的元数据是一致的，即使集群状态在分配期间发生变化。
     */
    private MetadataSnapshot metadataSnapshot;
    /**
     * 分配结果快照。
     * 应用场景：存储当前消费者被分配到的分区信息，作为元数据快照的一部分。
     * 实现细节：在分区分配完成后更新。
     */
    private MetadataSnapshot assignmentSnapshot;
    /**
     * 下一次自动提交位移的定时器。
     * 应用场景：如果启用了自动提交，此定时器用于触发定期的位移提交操作。
     * 实现细节：使用 `Time.timer()` 创建，并在每次提交后重置。
     */
    private Timer nextAutoCommitTimer;
    /**
     * 消费者组元数据。
     * 应用场景：封装了消费者组的相关信息，如组ID、成员ID、年代号（generation ID）、实例ID（group instance ID）。
     * 实现细节：在协调器初始化和重平衡过程中更新。
     */
    private ConsumerGroupMetadata groupMetadata;
    // 保存已提交位移请求的请求和future，以启用异步调用。
    /**
     * 挂起的已提交位移请求。
     * 应用场景：用于异步获取已提交位移（`committed()` 方法）。当一个异步获取请求发出后，此对象会保存请求的上下文和用于接收结果的 Future。
     * 实现细节：如果连续发起相同的请求（相同的分区和年代），可以复用之前的请求。
     * 设计考虑：支持 `committed()` 方法的异步非阻塞调用。
     */
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    /**
     * 内部静态类，用于表示一个挂起的（等待响应的）获取已提交位移的请求。
     * 应用场景：当调用 `ConsumerCoordinator.committed(Set<TopicPartition>, Duration)` 方法异步获取位移时，
     * 会创建一个此类的实例来跟踪请求的分区、请求时的年代以及用于接收结果的 `RequestFuture`。
     * 实现细节：包含了请求的分区集合、请求时的年代信息以及一个 `RequestFuture` 用于异步获取结果。
     * 设计考虑：封装了异步获取已提交位移请求的状态，便于管理和复用。
     */
    private static class PendingCommittedOffsetRequest { // PendingCommittedOffsetRequest 类的定义，用于封装一个挂起的已提交位移请求的信息。
        /**
         * 请求获取已提交位移的主题分区集合。
         * 应用场景：指定了需要查询哪些分区的已提交位移。
         */
        private final Set<TopicPartition> requestedPartitions;
        /**
         * 发起请求时的消费者年代（Generation）。
         * 应用场景：用于确保请求的上下文与当前消费者的状态一致，防止处理过期的请求。
         */
        private final Generation requestedGeneration;
        /**
         * 用于接收已提交位移结果的 `RequestFuture`。
         * 应用场景：调用者可以通过此 `RequestFuture` 异步地获取查询结果。
         */
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        /**
         * PendingCommittedOffsetRequest 的构造函数。
         * @param requestedPartitions 请求获取已提交位移的主题分区集合，不能为空。
         * @param generationAtRequestTime 发起请求时的消费者年代信息。
         * @param response 用于接收结果的 `RequestFuture`，不能为空。
         */
        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions, // 构造函数的参数 requestedPartitions，表示请求的分区集合。
                                              final Generation generationAtRequestTime, // 构造函数的参数 generationAtRequestTime，表示请求时的年代信息。
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) { // 构造函数的参数 response，表示用于接收结果的 Future。
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions); // 初始化 requestedPartitions 字段，并确保其不为 null。
            this.response = Objects.requireNonNull(response); // 初始化 response 字段，并确保其不为 null。
            this.requestedGeneration = generationAtRequestTime; // 初始化 requestedGeneration 字段。
        }

        /**
         * 判断当前请求是否与给定的分区集合和年代信息表示同一个请求。
         * 应用场景：用于判断是否可以复用一个已存在的 `PendingCommittedOffsetRequest`，避免重复发送相同的请求。
         * @param currentRequest 当前要比较的分区集合。
         * @param currentGeneration 当前要比较的年代信息。
         * @return 如果年代相同且请求的分区集合也相同，则返回 true；否则返回 false。
         */
        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) { // sameRequest 方法定义，用于比较两个请求是否相同。
            // 比较请求时的年代是否与当前年代相同，并且请求的分区集合是否与当前请求的分区集合相同。
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    /**
     * 当前消费者组选择的重平衡协议。
     * 应用场景：在消费者加入组时，会与协调器协商使用何种重平衡协议（如 EAGER, COOPERATIVE）。
     * 此字段存储了最终选定的协议。
     * 实现细节：根据所有分配器共同支持的协议，并选择 ID 最高的那个（通常代表更高级的协议）。
     */
    private final RebalanceProtocol protocol;
    // 封装了调用 ConsumerRebalanceListener 方法的逻辑
    /**
     * ConsumerRebalanceListener 调用器。
     * 应用场景：负责在重平衡的不同阶段（如分区被撤销、分区被分配）安全地调用用户提供的 `ConsumerRebalanceListener` 中的回调方法。
     * 实现细节：封装了回调执行的逻辑，包括错误处理和度量收集。
     * 设计考虑：将监听器回调的复杂逻辑（如顺序、错误处理、超时）集中管理。
     */
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    // onJoinPrepare 中的挂起提交位移请求
    /**
     * 在 `onJoinPrepare` 阶段自动提交位移的 `RequestFuture`。
     * 应用场景：当消费者准备加入组（`onJoinPrepare`）时，如果启用了自动提交，会尝试提交一次当前位移。
     * 此 `RequestFuture` 用于跟踪这个提交操作的结果。
     * 实现细节：如果提交正在进行，此字段不为 null。
     * 设计考虑：确保在重新加入组之前，尽可能提交掉已处理消息的位移。
     */
    private RequestFuture<Void> autoCommitOffsetRequestFuture = null;
    // 一个用于 join prepare 的计时器，用于知道何时停止。
    // 它将被设置为重平衡超时时间，以便即使位移提交失败，成员也能成功加入组。
    /**
     * `onJoinPrepare` 阶段的定时器。
     * 应用场景：在 `onJoinPrepare` 阶段，特别是等待自动提交位移完成时，此定时器用于控制等待的超时时间。
     * 超时时间通常设置为重平衡超时（rebalance timeout）。
     * 实现细节：确保即使位移提交失败或超时，消费者也能在合理的时间内继续加入组的过程。
     * 设计考虑：防止 `onJoinPrepare` 阶段因位移提交问题而无限期阻塞，影响重平衡的进行。
     */
    private Timer joinPrepareTimer = null;

    /**
     * 初始化协调管理器。
     * 应用场景：创建 ConsumerCoordinator 实例时调用，进行所有必要的初始化设置。
     * 实现细节：包括设置配置、日志、元数据、分配器、度量、回调、定时器等。
     * 设计考虑：集中了所有依赖注入和初始状态设置，确保协调器在创建后处于可用状态。
     * @param rebalanceConfig 消费者组重平衡配置。
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param client 消费者网络客户端，用于与 Kafka broker 通信。
     * @param assignors 分区分配器列表。
     * @param metadata 消费者元数据，提供集群信息。
     * @param subscriptions 订阅状态，维护消费者的订阅和分区分配信息。
     * @param metrics 度量收集器。
     * @param metricGrpPrefix 度量组前缀。
     * @param time 时间工具，用于获取当前时间和创建定时器。
     * @param autoCommitEnabled 是否启用自动提交位移。
     * @param autoCommitIntervalMs 自动提交位移的时间间隔（毫秒）。
     * @param interceptors 消费者拦截器。
     * @param throwOnFetchStableOffsetsUnsupported 当获取稳定位移不受支持时是否抛出异常。
     * @param rackId 消费者的机架 ID。
     * @param clientTelemetryReporter 客户端遥测报告器。
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig, // 构造函数参数：重平衡配置
                               LogContext logContext, // 构造函数参数：日志上下文
                               ConsumerNetworkClient client, // 构造函数参数：消费者网络客户端
                               List<ConsumerPartitionAssignor> assignors, // 构造函数参数：分区分配器列表
                               ConsumerMetadata metadata, // 构造函数参数：消费者元数据
                               SubscriptionState subscriptions, // 构造函数参数：订阅状态
                               Metrics metrics, // 构造函数参数：度量收集器
                               String metricGrpPrefix, // 构造函数参数：度量组前缀
                               Time time, // 构造函数参数：时间工具
                               boolean autoCommitEnabled, // 构造函数参数：是否启用自动提交
                               int autoCommitIntervalMs, // 构造函数参数：自动提交间隔
                               ConsumerInterceptors<?, ?> interceptors, // 构造函数参数：消费者拦截器
                               boolean throwOnFetchStableOffsetsUnsupported, // 构造函数参数：获取稳定位移不支持时是否抛异常
                               String rackId, // 构造函数参数：机架ID
                               Optional<ClientTelemetryReporter> clientTelemetryReporter) { // 构造函数参数：客户端遥测报告器
        super(rebalanceConfig, // 调用父类 AbstractCoordinator 的构造函数，传入重平衡配置
              logContext, // 调用父类构造函数，传入日志上下文
              client, // 调用父类构造函数，传入网络客户端
              metrics, // 调用父类构造函数，传入度量收集器
              metricGrpPrefix, // 调用父类构造函数，传入度量组前缀
              time, // 调用父类构造函数，传入时间工具
              clientTelemetryReporter); // 调用父类构造函数，传入客户端遥测报告器
        this.rebalanceConfig = rebalanceConfig; // 初始化重平衡配置字段
        this.log = logContext.logger(ConsumerCoordinator.class); // 初始化日志记录器，使用 ConsumerCoordinator 类名
        this.metadata = metadata; // 初始化消费者元数据字段
        this.rackId = rackId == null || rackId.isEmpty() ? Optional.empty() : Optional.of(rackId); // 初始化机架ID，如果为null或空则设为Optional.empty()
        this.metadataSnapshot = new MetadataSnapshot(this.rackId, subscriptions, metadata.fetch(), metadata.updateVersion()); // 初始化元数据快照，包含机架ID、订阅、当前元数据和版本
        this.subscriptions = subscriptions; // 初始化订阅状态字段
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback(); // 初始化默认的位移提交回调处理器
        this.autoCommitEnabled = autoCommitEnabled; // 初始化是否启用自动提交的标志
        this.autoCommitIntervalMs = autoCommitIntervalMs; // 初始化自动提交间隔时间
        this.assignors = assignors; // 初始化分区分配器列表
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>(); // 初始化已完成位移提交的队列，使用线程安全的 ConcurrentLinkedQueue
        this.coordinatorMetrics = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix); // 初始化消费者协调器度量指标
        this.interceptors = interceptors; // 初始化消费者拦截器
        this.inFlightAsyncCommits = new AtomicInteger(); // 初始化在途异步提交计数器
        this.pendingAsyncCommits = new AtomicInteger(); // 初始化等待协调器查找的挂起异步提交计数器
        this.asyncCommitFenced = new AtomicBoolean(false); // 初始化异步提交是否被隔离的标志，默认为 false
        this.groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, // 初始化消费者组元数据
            JoinGroupRequest.UNKNOWN_GENERATION_ID, JoinGroupRequest.UNKNOWN_MEMBER_ID, rebalanceConfig.groupInstanceId); // 使用组ID、未知的年代ID、未知的成员ID和组实例ID
        this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported; // 初始化获取稳定位移不支持时是否抛异常的标志

        if (autoCommitEnabled) // 检查是否启用了自动提交
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs); // 如果启用了自动提交，则创建并初始化下一次自动提交的定时器

        // 选择重平衡协议，规则如下：
        //   1. 只考虑所有分配器都支持的协议。如果所有分配器没有共同支持的协议，则抛出异常。
        //   2. 如果有多个共同支持的协议，则选择ID最高的那个（即ID号表示协议的先进程度）。
        // 我们知道列表中至少有一个分配器，无需再次检查NPE
        if (!assignors.isEmpty()) { // 检查分区分配器列表是否为空
            // 从第一个分配器获取其支持的协议列表作为基础
            List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());

            // 遍历其余的分配器
            for (ConsumerPartitionAssignor assignor : assignors) {
                // 保留当前 supportedProtocols 列表与当前分配器支持协议列表的交集
                // 这样，supportedProtocols 中最终只剩下所有分配器都支持的协议
                supportedProtocols.retainAll(assignor.supportedProtocols());
            }

            // 如果没有共同支持的协议
            if (supportedProtocols.isEmpty()) {
                // 抛出 IllegalArgumentException 异常，说明指定的分配器没有共同支持的重平衡协议
                throw new IllegalArgumentException("Specified assignors " + // 异常消息：指定的分配器
                    assignors.stream().map(ConsumerPartitionAssignor::name).collect(Collectors.toSet()) + // 获取所有分配器的名称集合
                    " do not have commonly supported rebalance protocol"); // 异常消息：没有共同支持的重平衡协议
            }

            // 对共同支持的协议列表进行排序（RebalanceProtocol实现了Comparable接口，通常按ID升序）
            Collections.sort(supportedProtocols);

            // 选择排序后列表中的最后一个协议，即ID最高的协议
            protocol = supportedProtocols.get(supportedProtocols.size() - 1);
        // 如果消费者使用的是手动分区分配策略
        } else { // 如果分配器列表为空
            // 将协议设置为 null
            protocol = null;
        }

        // 初始化 ConsumerRebalanceListener 调用器
        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
            logContext, // 传入日志上下文
            subscriptions, // 传入订阅状态
            time, // 传入时间工具
            new RebalanceCallbackMetricsManager(metrics, metricGrpPrefix) // 创建并传入重平衡回调度量管理器
        );
        this.metadata.requestUpdate(true); // 请求立即更新一次元数据，参数 true 表示需要阻塞等待更新完成
    }

    // 包私有，用于测试
    /**
     * @return 如果当前消费者是领导者，则返回 true，否则返回 false。
     * 应用场景：在消费者组中，领导者负责执行分区分配和监控元数据变化。
     * 实现细节：直接返回 isLeader 字段的值。
     * 设计考虑：提供一个简单的方法来检查当前消费者的领导者状态。
     */
    boolean isLeader() {
        // 返回当前实例是否是领导者的状态
        return this.isLeader;
    }

    // 包私有，用于测试
    /**
     * @return 当前的订阅状态。
     * 应用场景：用于获取消费者当前的订阅信息，包括订阅的主题、分配的分区等。
     * 实现细节：直接返回 subscriptions 字段的值。
     * 设计考虑：提供一个访问内部订阅状态的方法，主要用于测试。
     */
    SubscriptionState subscriptionState() {
        // 返回当前的订阅状态对象
        return this.subscriptions;
    }

    /**
     * 获取消费者协议类型。
     * @return 消费者协议类型字符串。
     * 应用场景：在加入消费者组时，需要指定协议类型，以便协调器知道如何处理该消费者的请求。
     * 实现细节：返回 ConsumerProtocol 中定义的协议类型常量。
     * 设计考虑：遵循 Kafka 消费者协议规范。
     */
    @Override
    public String protocolType() {
        // 返回消费者协议的类型字符串
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    /**
     * 生成加入消费者组请求所需的元数据。
     * @return 包含各个分区分配器协议元数据的集合。
     * 应用场景：当消费者尝试加入一个消费者组时，会调用此方法生成协议元数据，发送给协调器。
     * 实现细节：
     * 1. 记录当前订阅信息用于调试。
     * 2. 将当前订阅的主题集合赋值给 joinedSubscription 字段。
     * 3. 创建一个 JoinGroupRequestProtocolCollection 用于存储各个分配器的协议信息。
     * 4. 遍历配置的每个 ConsumerPartitionAssignor：
     *    a. 基于当前已加入的订阅 (joinedSubscription)、分配器特定的用户数据、已分配的分区列表、当前年代的 generationId 和机架 ID (rackId) 创建一个 Subscription 对象。
     *    b. 使用 ConsumerProtocol 将 Subscription 对象序列化为 ByteBuffer。
     *    c. 创建一个 JoinGroupRequestProtocol 对象，设置分配器的名称和序列化后的元数据，并将其添加到 protocolSet 中。
     * 5. 返回包含所有分配器协议元数据的 protocolSet。
     * 设计考虑：此方法确保了消费者在加入组时，能够向协调器提供所有必要的信息，以便协调器和领导者能够正确地执行分区分配。
     * 每个分配器都可以定义自己的元数据格式，通过这种方式实现了灵活性。
     */
    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        // 记录调试信息：当前订阅的加入组情况
        log.debug("Joining group with current subscription: {}", subscriptions.subscription());
        // 更新 joinedSubscription 为当前的订阅集合
        this.joinedSubscription = subscriptions.subscription();
        // 创建一个协议集合，用于存放不同分配策略的元数据
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();

        // 将 joinedSubscription（当前消费者实际加入组时使用的订阅主题列表）转换为列表
        List<String> topics = new ArrayList<>(joinedSubscription);
        // 遍历所有配置的分区分配器
        for (ConsumerPartitionAssignor assignor : assignors) {
            // 为每个分配器创建一个 Subscription 对象，包含主题、用户数据、已分配分区、年代信息和机架ID
            Subscription subscription = new Subscription(topics, // 订阅的主题列表
                                                         assignor.subscriptionUserData(joinedSubscription), // 分配器特定的用户数据
                                                         subscriptions.assignedPartitionsList(), // 当前已分配的分区列表
                                                         generation().generationId, // 当前的年代ID
                                                         rackId); // 机架ID
            // 将 Subscription 对象序列化为字节缓冲区
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

            // 将分配器的名称和序列化后的元数据添加到协议集合中
            protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(assignor.name()) // 设置分配器的名称
                    .setMetadata(Utils.toArray(metadata))); // 设置序列化后的元数据 (转换为字节数组)
        }
        // 返回构建好的协议集合
        return protocolSet;
    }

    /**
     * 根据集群元数据更新基于模式的订阅。
     * @param cluster 当前的集群元数据。
     * 应用场景：当消费者使用正则表达式订阅主题时，集群中的主题可能会发生变化（新增或删除）。此方法用于根据最新的集群主题列表，更新消费者的实际订阅主题。
     * 实现细节：
     * 1. 从集群元数据中获取所有主题。
     * 2. 过滤出与消费者订阅模式匹配的主题。
     * 3. 如果通过模式匹配到的主题列表与当前订阅不同（即发生了变化），则更新订阅状态，并请求元数据更新以反映这些新主题。
     * 设计考虑：确保基于模式的订阅能够动态适应集群主题的变化，及时消费新增的匹配主题。
     */
    public void updatePatternSubscription(Cluster cluster) {
        // 从集群元数据中获取所有主题，然后过滤出与当前订阅模式匹配的主题
        final Set<String> topicsToSubscribe = cluster.topics().stream() // 获取集群中的所有主题名称流
                .filter(subscriptions::matchesSubscribedPattern) // 使用 SubscriptionState 中的方法检查主题是否匹配订阅的正则表达式模式
                .collect(Collectors.toSet()); // 将匹配的主题收集到一个 Set 中
        // 如果从模式订阅成功（意味着订阅的主题列表发生了变化）
        if (subscriptions.subscribeFromPattern(topicsToSubscribe)) {
            // 请求元数据更新，因为可能有新的主题加入订阅
            // 请求为新的主题更新元数据
            metadata.requestUpdateForNewTopics();

        }
    }

    /**
     * 根据名称查找分区分配器。
     * @param name 分配器的名称。
     * @return 如果找到则返回对应的 ConsumerPartitionAssignor 实例，否则返回 null。
     * 应用场景：在处理来自协调器的分配结果时，需要根据协调器选择的分配策略名称找到对应的分配器实例。
     * 实现细节：遍历已配置的分配器列表，比较名称是否匹配。
     * 设计考虑：提供一个简单的查找机制，用于在运行时确定使用哪个分配器。
     */
    private ConsumerPartitionAssignor lookupAssignor(String name) {
        // 遍历当前协调器配置的所有分区分配器
        for (ConsumerPartitionAssignor assignor : this.assignors) {
            // 如果分配器的名称与传入的名称匹配
            if (assignor.name().equals(name)) {
                // 返回该分配器实例
                return assignor;
            }
        }
        // 如果没有找到匹配的分配器，则返回 null
        return null;
    }

    /**
     * 可能会更新已加入的订阅信息，特别是在使用模式订阅时。
     * @param assignedPartitions 已分配给当前消费者的分区集合。
     * 应用场景：当消费者使用模式订阅时，领导者分配的分区可能包含最初未显式请求但符合模式的主题。
     * 此方法用于检查这种情况，并相应地更新消费者的内部订阅状态 (joinedSubscription 和 subscriptions)。
     * 实现细节：
     * 1. 检查当前是否为模式订阅。
     * 2. 如果是模式订阅，遍历分配到的分区，找出那些主题不在 joinedSubscription（加入组时使用的订阅）中的新主题。
     * 3. 如果发现了这样的新主题：
     *    a. 创建当前订阅 (subscriptions.subscription()) 和 joinedSubscription 的副本。
     *    b. 将新发现的主题添加到这两个副本中。
     *    c. 使用更新后的主题列表尝试从模式更新订阅 (subscriptions.subscribeFromPattern)。如果订阅发生变化，则请求元数据更新。
     *    d. 更新 joinedSubscription 为包含新主题的集合。
     * 设计考虑：确保消费者的订阅状态与领导者的分配决策保持一致，即使这些决策引入了新的、符合模式的主题。
     * 这样做可以避免消费者忽略由领导者分配的、但最初未在 joinedSubscription 中的分区。
     */
    private void maybeUpdateJoinedSubscription(Set<TopicPartition> assignedPartitions) {
        // 检查当前订阅是否是基于模式的订阅（例如，使用正则表达式）
        // 如果订阅是基于模式的（例如，使用正则表达式订阅主题）
                if (subscriptions.hasPatternSubscription()) {
            // 检查分配结果是否包含原始订阅中没有的主题。
            // 如果是，我们将遵循领导者的决定，并将这些主题添加到订阅中，只要它们仍然匹配订阅的模式。

            // 用于存储新添加的主题名称
            Set<String> addedTopics = new HashSet<>();
            // 这是一个副本，因为它稍后会传递给监听器
            // 遍历所有分配给当前消费者的分区
            for (TopicPartition tp : assignedPartitions) {
                // 如果当前分区的主题不在 joinedSubscription（即消费者加入组时声明要订阅的主题）中
                if (!joinedSubscription.contains(tp.topic())) {
                    // 将该主题添加到 addedTopics 集合中
                    addedTopics.add(tp.topic());
                }
            }

            // 如果有新添加的主题
            if (!addedTopics.isEmpty()) {
                // 创建一个新的订阅集合，基于当前的实际订阅 (subscriptions.subscription())
                Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
                // 创建一个新的 joinedSubscription 集合，基于当前的 joinedSubscription
                Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
                // 将新发现的主题添加到这两个集合中
                newSubscription.addAll(addedTopics);
                newJoinedSubscription.addAll(addedTopics);

                // 尝试从新的主题集合更新模式订阅
                // 如果订阅状态因此发生变化（例如，实际订阅的主题列表改变了）
                if (this.subscriptions.subscribeFromPattern(newSubscription)) {
                    // 请求元数据更新，因为订阅的主题列表可能已更改
                    // 请求为新的主题更新元数据
            metadata.requestUpdateForNewTopics();

                }
                // 更新 joinedSubscription 为包含新主题的集合
                this.joinedSubscription = newJoinedSubscription;
            }
        }
    }

    /**
     * 调用分配器的 onAssignment 方法，通知其新的分区分配结果。
     * @param assignor 要通知的分区分配器。
     * @param assignment 新的分区分配结果。
     * @return 如果调用过程中发生异常，则返回该异常；否则返回 null。
     * 应用场景：在消费者完成加入组并收到分区分配后，需要通知相应的分配器，以便分配器可以更新其内部状态或执行其他与分配相关的逻辑。
     * 实现细节：
     * 1. 记录通知分配器的日志。
     * 2. 调用分配器的 onAssignment 方法，传入分配结果和当前的消费者组元数据。
     * 3. 捕获并返回在调用过程中可能发生的任何异常。
     * 设计考虑：提供一个标准的机制来让分配器感知到新的分区分配，允许分配器根据分配结果执行自定义操作。
     */
    private Exception invokeOnAssignment(final ConsumerPartitionAssignor assignor, final Assignment assignment) {
        // 记录日志，通知分配器有关新的分配信息
        log.info("Notifying assignor about the new {}", assignment);

        try {
            // 调用分配器的 onAssignment 方法，传入分配结果和消费者组元数据
            assignor.onAssignment(assignment, groupMetadata);
        } catch (Exception e) {
            // 如果在调用 onAssignment 期间发生异常，则捕获并返回该异常
            return e;
        }

        // 如果没有异常发生，则返回 null
        return null;
    }

    /**
     * 当消费者成功加入组并收到分区分配后调用的回调方法。
     * @param generation 新的年代ID。
     * @param memberId 消费者在组内的成员ID。
     * @param assignmentStrategy 协调器选择的分区分配策略名称。
     * @param assignmentBuffer 包含序列化后的分区分配数据的字节缓冲区。
     * @throws IllegalStateException 如果分配协议无效，或者分配数据不足。
     * @throws KafkaException 如果用户提供的重平衡回调抛出异常。
     * 应用场景：这是消费者完成一次成功的重平衡（JoinGroup 和 SyncGroup）后的核心处理逻辑。
     * 它负责解析分配结果，更新内部状态，并调用用户提供的监听器。
     * 实现细节：
     * 1. 记录调试日志。
     * 2. 如果当前消费者不是领导者，则清除 assignmentSnapshot（领导者才负责监控元数据变化）。
     * 3. 根据 assignmentStrategy 查找对应的 ConsumerPartitionAssignor 实例。
     * 4. 更新 groupMetadata，包含新的年代、成员ID等信息。
     * 5. 获取当前消费者拥有的分区 (ownedPartitions)。
     * 6. 校验 assignmentBuffer 是否包含足够的数据来反序列化分配结果。
     * 7. 从 assignmentBuffer 反序列化得到 Assignment 对象，并获取新分配的分区 (assignedPartitions)。
     * 8. 检查新分配的分区是否与当前订阅匹配 (subscriptions.checkAssignmentMatchedSubscription)。如果不匹配，则请求重新加入组，并返回。
     * 9. 计算新增的分区 (addedPartitions = assignedPartitions - ownedPartitions)。
     * 10. 如果使用的是 COOPERATIVE 重平衡协议：
     *     a. 计算被撤销的分区 (revokedPartitions = ownedPartitions - assignedPartitions)。
     *     b. 记录详细的分配更新日志。
     *     c. 如果有被撤销的分区，调用 rebalanceListenerInvoker.invokePartitionsRevoked() 通知用户监听器，并记录可能发生的第一个异常。
     *     d. 如果撤销了分区，则请求重新加入组（这是协作重平衡协议的要求，确保状态同步）。
     * 11. 调用 maybeUpdateJoinedSubscription() 更新基于模式的订阅（如果适用）。
     * 12. 调用 invokeOnAssignment() 通知分配器新的分配结果，并记录可能发生的第一个异常。
     * 13. 如果启用了自动提交，则重置自动提交计时器。
     * 14. 调用 subscriptions.assignFromSubscribed() 更新内部的订阅状态以反映新的分配。
     * 15. 调用 rebalanceListenerInvoker.invokePartitionsAssigned() 通知用户监听器新增的分区，并记录可能发生的第一个异常。
     * 16. 如果在上述过程中捕获到任何异常，则抛出该异常（如果是 KafkaException 则直接抛出，否则包装成 KafkaException 抛出）。
     * 设计考虑：此方法是重平衡流程的关键环节，需要正确处理各种情况，包括协议错误、数据校验、用户回调执行以及不同重平衡协议（如 EAGER 和 COOPERATIVE）的特定逻辑。
     * 使用 AtomicReference<Exception> 来捕获和处理在多个步骤中可能发生的第一个异常。
     */
    @Override
    protected void onJoinComplete(int generation, // 新的年代ID
                                  String memberId, // 消费者的成员ID
                                  String assignmentStrategy, // 使用的分区分配策略名称
                                  ByteBuffer assignmentBuffer) { // 包含分区分配数据的缓冲区
        // 记录调试日志，表明正在执行 onJoinComplete
        log.debug("Executing onJoinComplete with generation {} and memberId {}", generation, memberId);

        // 只有领导者负责监控元数据变化（例如分区变化）
        // 如果当前实例不是领导者，则将 assignmentSnapshot 置为 null
        if (!isLeader)
            assignmentSnapshot = null;

        // 根据分配策略名称查找对应的分配器实例
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        // 如果找不到分配器，说明协调器选择了无效的分配协议，抛出异常
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // 让分配器有机会根据接收到的分配更新内部状态
        // 更新消费者组元数据，包含组ID、年代、成员ID和实例ID
        groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);

        // 获取当前消费者已经拥有的分区，并使用比较器排序
        SortedSet<TopicPartition> ownedPartitions = new TreeSet<>(COMPARATOR);
        ownedPartitions.addAll(subscriptions.assignedPartitions());

        // 分配缓冲区至少应该能编码短版本（通常是版本号）
        // 如果剩余字节数小于2，说明数据不足，抛出异常
        if (assignmentBuffer.remaining() < 2)
            throw new IllegalStateException("There are insufficient bytes available to read assignment from the sync-group response (" +
                "actual byte size " + assignmentBuffer.remaining() + ") , this is not expected; " +
                "it is possible that the leader's assign function is buggy and did not return any assignment for this member, " +
                "or because static member is configured and the protocol is buggy hence did not get the assignment for this member");

        // 从字节缓冲区反序列化分区分配信息
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // 获取新分配的分区，并使用比较器排序
        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(COMPARATOR);
        assignedPartitions.addAll(assignment.partitions());

        // 检查新分配的分区是否与当前消费者的订阅匹配
        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            // 如果不匹配，构造详细原因并发起重新加入组的请求
            final String fullReason = String.format("received assignment %s does not match the current subscription %s; " +
                    "it is likely that the subscription has changed since we joined the group, will re-join with current subscription",
                    assignment.partitions(), subscriptions.prettyString());
            requestRejoin("received assignment does not match the current subscription", fullReason);
            // 直接返回，不再继续处理此次分配
            return;
        }

        // 用于存储在回调或分配器通知过程中发生的第一个异常
        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        // 计算新增的分区：新分配的分区 减去 原本拥有的分区
        SortedSet<TopicPartition> addedPartitions = new TreeSet<>(COMPARATOR);
        addedPartitions.addAll(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        // 如果使用的是协作式重平衡协议 (COOPERATIVE)
        if (protocol == RebalanceProtocol.COOPERATIVE) {
            // 计算被撤销的分区：原本拥有的分区 减去 新分配的分区
            SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(COMPARATOR);
            revokedPartitions.addAll(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);

            // 记录日志，详细说明分配的更新情况
            log.info("Updating assignment with\n" +
                    "\tAssigned partitions:                       {}\n" +
                    "\tCurrent owned partitions:                  {}\n" +
                    "\tAdded partitions (assigned - owned):       {}\n" +
                    "\tRevoked partitions (owned - assigned):     {}\n",
                assignedPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions
            );

            // 如果有被撤销的分区
            if (!revokedPartitions.isEmpty()) {
                // 撤销先前拥有但不再分配的分区；
                // 注意：我们应该在触发撤销回调之后才更改分配（或更新分配器的状态）
                // 调用用户提供的 ConsumerRebalanceListener 的 onPartitionsRevoked 方法
                firstException.compareAndSet(null, rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions));

                // 如果撤销了任何分区，之后需要重新加入组（这是协作式协议的要求）
                final String fullReason = String.format("need to revoke partitions %s as indicated " +
                        "by the current assignment and re-join", revokedPartitions);
                requestRejoin("need to revoke partitions and re-join", fullReason);
                // 注意：在协作模式下，如果发生分区撤销，通常会在这里发起 rejoin 并可能提前返回，
                // 但当前代码逻辑是继续执行，这依赖于 requestRejoin 的具体实现是否会立即中断当前流程。
                // 实际的协作式重平衡通常会在 revoke 后等待下一次 poll 或 rejoin 来完成后续的 assign。
            }
        }

        // 领导者可能分配了一些匹配我们订阅模式但未明确请求的分区，
        // 所以我们在这里更新已加入的订阅。
        maybeUpdateJoinedSubscription(assignedPartitions);

        // 在这里捕获任何异常，以确保我们可以完成用户回调。
        // 通知分配器新的分配结果
        firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment));

        // 从现在开始重新调度自动提交
        if (autoCommitEnabled) {
            // 更新并重置下一次自动提交的计时器
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);
        }

        // 根据订阅的主题更新实际分配的分区
        subscriptions.assignFromSubscribed(assignedPartitions);

        // 添加先前未拥有但现在已分配的分区，并调用用户提供的 ConsumerRebalanceListener 的 onPartitionsAssigned 方法
        firstException.compareAndSet(null, rebalanceListenerInvoker.invokePartitionsAssigned(addedPartitions));

        // 如果在上述过程中捕获到任何异常
        if (firstException.get() != null) {
            // 如果异常是 KafkaException 的实例，则直接抛出
            if (firstException.get() instanceof KafkaException) {
                throw (KafkaException) firstException.get();
            // 如果消费者使用的是手动分区分配策略
        } else {
                // 否则，将其包装为 KafkaException 抛出，并附带原始异常信息
                throw new KafkaException("User rebalance callback throws an error", firstException.get());
            }
        }
    }

    /**
     * 尝试更新订阅元数据。
     * 应用场景：在消费者轮询或元数据发生变化时调用，确保消费者持有最新的订阅信息和集群元数据快照。
     * 实现细节：检查元数据版本是否有更新，如果有，则获取最新的集群信息，并根据需要更新模式订阅和元数据快照。
     * 设计考虑：通过版本号比较来避免不必要的元数据更新，提高效率。
     */
    void maybeUpdateSubscriptionMetadata() {
        // 获取元数据的当前更新版本号
        int version = metadata.updateVersion();
        // 如果当前元数据版本大于快照中的版本，说明元数据已更新
        if (version > metadataSnapshot.version) {
            // 获取最新的集群元数据
            Cluster cluster = metadata.fetch();


            // 如果当前订阅是基于模式的订阅（例如，订阅所有以 "test-" 开头的主题）
            if (subscriptions.hasPatternSubscription())
                // 更新模式订阅，根据新的集群元数据匹配符合模式的主题
                updatePatternSubscription(cluster);


            // 更新当前的元数据快照，该快照将用于检查可能需要重新平衡的订阅更改（例如，新的分区）。
            // 应用场景：在元数据更新后，保存一份当前状态的快照，用于后续比较和决策，例如判断是否因为订阅变化（如新增分区）而需要触发再均衡。
            // 实现细节：创建一个新的 MetadataSnapshot 对象，包含当前的机架 ID、订阅信息、集群元数据和版本号。
            // 设计考虑：通过快照机制，可以清晰地追踪元数据的变化，并据此执行相应的逻辑。
            // 创建新的元数据快照，记录当前的机架ID、订阅信息、集群对象和元数据版本
            metadataSnapshot = new MetadataSnapshot(rackId, subscriptions, cluster, version);
        }
    }

    /**
     * 检查协调器是否未知并且同步地确保协调器准备就绪失败。
     * 应用场景：在需要同步等待协调器准备就绪的场景下使用，例如在 poll 方法中首次发现协调器未知时。
     * 实现细节：首先调用 coordinatorUnknown() 判断协调器是否未知，如果未知，则调用 ensureCoordinatorReady(timer) 尝试同步使其准备就绪，并取反判断是否失败。
     * 设计考虑：封装了协调器未知且准备失败的判断逻辑，使调用方代码更简洁。
     * @param timer 用于限制确保协调器准备就绪操作的计时器
     * @return 如果协调器未知且未能成功准备就绪，则返回 true；否则返回 false
     */
    private boolean coordinatorUnknownAndUnreadySync(Timer timer) {
        // 如果协调器未知 并且 (取反)确保协调器准备就绪(同步方式)的结果为 false (即准备失败)
        return coordinatorUnknown() && !ensureCoordinatorReady(timer);
    }

    /**
     * 检查协调器是否未知并且异步地确保协调器准备就绪失败。
     * 应用场景：在可以异步检查协调器状态的场景下使用。
     * 实现细节：首先调用 coordinatorUnknown() 判断协调器是否未知，如果未知，则调用 ensureCoordinatorReadyAsync() 尝试异步使其准备就绪，并取反判断是否失败。
     * 设计考虑：与 coordinatorUnknownAndUnreadySync 类似，但用于异步场景。
     * @return 如果协调器未知且未能成功准备就绪（异步检查），则返回 true；否则返回 false
     */
    private boolean coordinatorUnknownAndUnreadyAsync() {
        // 如果协调器未知 并且 (取反)确保协调器准备就绪(异步方式)的结果为 false (即准备失败)
        return coordinatorUnknown() && !ensureCoordinatorReadyAsync();
    }

    /**
     * 轮询协调器事件。此方法确保协调器已知，并且如果消费者使用组管理，则确保其已加入组。如果启用了定期偏移量提交，此方法也会处理。
     * <p>
     * 如果超时到期或者不需要等待重新加入组，则会提前返回。
     * 应用场景：这是消费者主循环中与协调器交互的核心方法，负责处理组成员关系、元数据更新、心跳、位移提交等关键协调任务。
     * 实现细节：
     * 1. 更新订阅元数据。
     * 2. 调用已完成的位移提交回调。
     * 3. 如果是自动分配分区：
     *    a. 检查协议是否配置。
     *    b. 发送心跳。
     *    c. 如果协调器未知且未准备好，则返回 false。
     *    d. 如果需要重新加入组或正在重新加入组：
     *       i. 如果是模式订阅，则请求更新元数据并确保元数据新鲜。
     *       ii. 确保消费者活跃在组中，根据 waitForJoinGroup 决定是否等待。
     * 4. 如果是手动分配分区：
     *    a. 如果元数据更新被请求且没有可用的 ready 节点，则等待元数据更新。
     *    b. 轮询网络客户端以发送挂起的协调器请求。
     * 5. 尝试异步自动提交位移。
     * 设计考虑：此方法逻辑复杂，因为它整合了消费者协调的多个方面。通过清晰的步骤划分和条件判断来管理不同的协调状态和场景。
     * @param timer 用于限制此方法阻塞时间的计时器
     * @param waitForJoinGroup 布尔标志，指示我们是否应该等待直到重新加入组完成
     * @throws KafkaException 如果再平衡回调抛出异常
     * @return 当且仅当操作成功时返回 true
     */
    public boolean poll(Timer timer, boolean waitForJoinGroup) { // poll 方法，用于轮询协调器事件
        // 尝试更新订阅相关的元数据信息
        // 再次尝试更新订阅相关的元数据信息（因为集群元数据可能已更新）
                    // 再次尝试更新订阅相关的元数据信息（因为集群元数据可能已更新）
        maybeUpdateSubscriptionMetadata();


        // 调用已完成的偏移量提交回调
        invokeCompletedOffsetCommitCallbacks();


        // 检查当前消费者是否使用了自动分区分配策略
        if (subscriptions.hasAutoAssignedPartitions()) {
            // 如果是自动分配分区，但协议 (protocol) 未设置（通常在消费者组初始化时设置）
            if (protocol == null) {
                // 抛出非法状态异常，因为用户配置了自动分区分配，但没有提供有效的分配策略（协议）
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions"); // 提示用户分区分配策略配置为空
            }
            // 总是更新心跳的最后轮询时间，这样即使（比如说）找不到协调器，
            // 心跳线程也不会因为应用程序不活动而主动离开组。
            // 应用场景：确保即使在协调器暂时不可用或应用处理缓慢的情况下，消费者也不会因为心跳超时而被踢出组。
            // 实现细节：调用 pollHeartbeat 更新上次心跳时间。
            // 设计考虑：这是维持组成员资格的关键机制，防止因短暂的网络问题或应用繁忙导致不必要的重平衡。
            // 更新心跳的最后轮询时间，参数为当前时间戳
            pollHeartbeat(timer.currentTimeMs());
            // 如果协调器未知并且同步尝试使其准备就绪失败
            if (coordinatorUnknownAndUnreadySync(timer)) {
                // 返回 false，表示本次 poll 操作未成功（协调器问题）
                // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
            }

            // 如果需要重新加入组或者正在等待重新加入组
            if (rejoinNeededOrPending()) {
                // 由于初始元数据获取和初始再平衡之间可能存在竞争条件，
                // 我们需要确保在初次加入之前元数据是最新的。这确保了
                // 我们在加入之前至少已经将模式与集群的主题匹配过一次。
                // 应用场景：特别是在使用模式订阅（如正则表达式匹配主题名）时，确保在加入组并参与分区分配前，消费者已经基于最新的集群信息识别了所有应订阅的主题。
                // 实现细节：如果订阅了模式，会检查是否允许更新元数据，如果允许则请求更新，并确保元数据是新鲜的。
                // 设计考虑：避免因陈旧元数据导致的不完整订阅或错误的初始分区分配。
                // 如果当前订阅是基于模式的订阅（例如，订阅所有以 "test-" 开头的主题）
            // 如果订阅是基于模式的（例如，使用正则表达式订阅主题）
                if (subscriptions.hasPatternSubscription()) {
                    // 对于使用基于模式订阅的消费者组，在创建主题后，
                    // 任何在元数据刷新后发现该主题的消费者都可能触发整个消费者组的再平衡。
                    // 如果消费者在截然不同的时间刷新元数据，则一次主题创建可能会触发多次再平衡。我们可以显著
                    // 减少由单个主题创建引起的再平衡次数，方法是要求消费者在重新加入组之前刷新元数据，
                    // 只要刷新退避时间已过。
                    // 应用场景：优化模式订阅下新主题创建导致的频繁重平衡问题。
                    // 实现细节：检查元数据更新的冷却时间是否已过，如果已过，则请求立即更新元数据。
                    // 设计考虑：通过在加入组前主动刷新元数据，尽量让所有成员在相近的时间点感知到新主题，从而减少重平衡次数。
                    // 检查当前时间是否允许元数据更新（即是否过了退避期）
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
                        // 请求立即更新元数据
                        this.metadata.requestUpdate(true);
                    }

                    // 确保客户端元数据是最新的，如果获取失败
                    if (!client.ensureFreshMetadata(timer)) {
                        // 返回 false，表示本次 poll 操作未成功（协调器问题）
                // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
                    }

                    // 尝试更新订阅相关的元数据信息
        // 再次尝试更新订阅相关的元数据信息（因为集群元数据可能已更新）
                    // 再次尝试更新订阅相关的元数据信息（因为集群元数据可能已更新）
        maybeUpdateSubscriptionMetadata();

                }

                // 如果不需要等待加入组完成，我们将使用一个0毫秒的计时器
                // 调用 ensureActiveGroup 确保消费者在组中是活跃的。如果 waitForJoinGroup 为 true，则使用传入的 timer；否则，使用一个0ms的timer（表示不等待）。
                // 如果 ensureActiveGroup 返回 false (表示未能成功确保活跃状态)
                if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                    // 由于在被调用者中我们可能使用了不同的计时器，我们仍然需要
                    // 在调用后更新原始计时器的当前时间。
                    // 应用场景：确保外部调用者使用的计时器状态得到正确更新，即使内部逻辑使用了临时或不同的计时器。
                    // 实现细节：调用 timer.update() 更新计时器。
                    // 设计考虑：保持计时器状态的一致性。
                    // 更新传入的 timer 的当前时间
                    timer.update(time.milliseconds());


                    // 返回 false，表示本次 poll 操作未成功（协调器问题）
                // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
                }
            }
        // 如果消费者使用的是手动分区分配策略
        } else {
            // 对于手动分配的分区，我们不会尝试主动查找协调器；
            // 相反，我们只在必要时尝试刷新元数据。
            // 如果到所有节点的连接都失败，尝试发送获取请求时触发的唤醒
            // 会导致轮询立即返回，从而导致轮询的紧密循环。没有
            // 唤醒，没有通道的 poll() 将阻塞超时，从而延迟重新连接。
            // ensureCoordinatorReady 中的 awaitMetadataUpdate() 会使用配置的退避时间发起新连接，并避免繁忙循环。
            // 应用场景：处理手动分区分配模式下的元数据更新和网络连接问题。
            // 实现细节：如果元数据需要更新且当前没有可用的 Kafka 节点，则调用 client.awaitMetadataUpdate(timer) 等待元数据更新，这会尝试建立新连接。
            // 设计考虑：在手动分配模式下，协调器的角色减弱，但仍需处理元数据和连接问题，此逻辑旨在避免因连接问题导致的CPU空转。
            // 如果元数据更新被请求 并且 客户端当前没有可用的（ready）节点
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                // 等待元数据更新，这通常涉及到尝试重新连接到 Kafka 集群
                client.awaitMetadataUpdate(timer);
            }

            // 如果有挂起的协调器请求，确保它们有机会被发送出去。
            // 应用场景：即使在手动分区模式下，也可能有一些与协调器相关的请求（例如，如果之前尝试过加入组但失败了），这里确保这些请求能被处理。
            // 实现细节：调用 client.pollNoWakeup() 来处理网络I/O，发送挂起的请求。
            // 设计考虑：确保所有待处理的网络请求都能得到处理。
            // 轮询网络客户端以发送任何挂起的请求，但不唤醒选择器（如果它正在阻塞）
            client.pollNoWakeup();
        }

        // 尝试异步自动提交偏移量，参数为当前时间戳
        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        // poll 操作成功完成
        return true;
    }

    /**
     * 返回下一次需要调用 {@link ConsumerNetworkClient#poll(Timer)} 的时间。
     * 应用场景：供消费者主循环判断下一次 poll 操作可以等待的最长时间，以优化CPU使用和及时响应事件。
     * 实现细节：如果启用了自动提交，则返回下一次自动提交时间和下一次心跳时间中的较小值；否则，只返回下一次心跳时间。
     * 设计考虑：平衡了及时提交位移、发送心跳和避免不必要轮询的需求。
     * @param now 当前时间（毫秒）
     * @return 调用者在下一次调用 poll() 之前应该等待的最大时间（毫秒）
     */
    public long timeToNextPoll(long now) { // timeToNextPoll 方法，计算距离下一次 poll 的时间
        // 如果未启用自动提交偏移量
        if (!autoCommitEnabled)
            // 返回距离下一次心跳的时间
            return timeToNextHeartbeat(now);


        // 返回下一次自动提交剩余时间 和 距离下一次心跳时间 中的最小值
        return Math.min(nextAutoCommitTimer.remainingMs(), timeToNextHeartbeat(now));
    }

    /**
     * 更新消费者组的订阅信息。
     * 应用场景：当消费者组的领导者（leader）确定了组内所有成员共同订阅的主题集合后调用此方法。
     * 实现细节：
     * 1. 将合并后的主题集合更新到本地的订阅状态中，如果订阅发生变化，则请求更新元数据。
     * 2. 确保客户端持有最新的元数据。
     * 3. 尝试更新订阅元数据快照。
     * 设计考虑：此方法由组领导者调用，用于同步整个组对主题的关注点，并确保后续的分区分配基于最新的信息。
     * @param topics 消费者组共同订阅的主题集合
     * @throws TimeoutException 如果在确保元数据新鲜时超时
     */
    private void updateGroupSubscription(Set<String> topics) {
        // leader 将开始监视组感兴趣的任何主题的更改，
        // 这确保最终将看到所有元数据更改
        // 应用场景：当作为消费者组的 leader 时，需要关注所有成员订阅的主题集合的变化。
        // 实现细节：调用 subscriptions.groupSubscribe(topics) 更新组订阅，如果订阅的主题列表发生变化，则请求元数据更新。
        // 设计考虑：确保 leader 能够及时感知到与组订阅相关的主题元数据变化。
        // 更新组订阅的主题列表，如果订阅的主题集合发生变化，则返回 true
        if (this.subscriptions.groupSubscribe(topics))
            // 请求为新的主题更新元数据
            metadata.requestUpdateForNewTopics();


        // 更新元数据（如果需要）并跟踪用于分配的元数据，以便
        // 我们可以在再平衡完成后检查是否有任何更改
        // 应用场景：在更新组订阅后，需要确保本地元数据是最新的，以便后续的分区分配基于准确的信息。
        // 实现细节：调用 client.ensureFreshMetadata() 强制刷新元数据。
        // 设计考虑：关键操作前确保数据一致性。
        // 确保客户端元数据是最新的，使用一个最大超时时间的计时器；如果更新失败
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)))
            // 抛出超时异常
            throw new TimeoutException();


        // 尝试更新订阅相关的元数据信息
        // 再次尝试更新订阅相关的元数据信息（因为集群元数据可能已更新）
                    // 再次尝试更新订阅相关的元数据信息（因为集群元数据可能已更新）
        maybeUpdateSubscriptionMetadata();

    }

    /**
     * 检查给定的分配器名称是否属于“从已订阅主题分配”类型的分配器。
     * 应用场景：在某些特定的分配逻辑中，可能需要区分不同类型的分配器。
     * 实现细节：检查分配器名称是否存在于预定义的 ASSIGN_FROM_SUBSCRIBED_ASSIGNORS 集合中。
     * 设计考虑：提供一种简单的方式来识别特定类型的分配策略。
     * @param name 分区分配器的名称
     * @return 如果该分配器是从已订阅主题分配的类型，则返回 true；否则返回 false
     */
    private boolean isAssignFromSubscribedTopicsAssignor(String name) {
        // 检查预定义的 ASSIGN_FROM_SUBSCRIBED_ASSIGNORS 集合是否包含给定的分配器名称
        return ASSIGN_FROM_SUBSCRIBED_ASSIGNORS.contains(name);
    }

    /**
     * user-customized assignor may have created some topics that are not in the subscription list
     * and assign their partitions to the members; in this case we would like to update the leader's
     * own metadata with the newly added topics so that it will not trigger a subsequent rebalance
     * when these topics gets updated from metadata refresh.
     *
     * We skip the check for in-product assignors since this will not happen in in-product assignors.
     *
     * TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
     *       we may need to modify the ConsumerPartitionAssignor API to better support this case.
     *
     * @param assignorName          the selected assignor name
     * @param assignments           the assignments after assignor assigned
     * @param allSubscribedTopics   all consumers' subscribed topics
     */
    private void maybeUpdateGroupSubscription(String assignorName,
                                              Map<String, Assignment> assignments,
                                              Set<String> allSubscribedTopics) {
        if (!isAssignFromSubscribedTopicsAssignor(assignorName)) {
            Set<String> assignedTopics = new HashSet<>();
            for (Assignment assigned : assignments.values()) {
                for (TopicPartition tp : assigned.partitions())
                    assignedTopics.add(tp.topic());
            }

            if (!assignedTopics.containsAll(allSubscribedTopics)) {
                SortedSet<String> notAssignedTopics = new TreeSet<>(allSubscribedTopics);
                notAssignedTopics.removeAll(assignedTopics);
                log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
            }

            if (!allSubscribedTopics.containsAll(assignedTopics)) {
                SortedSet<String> newlyAddedTopics = new TreeSet<>(assignedTopics);
                newlyAddedTopics.removeAll(allSubscribedTopics);
                log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

                allSubscribedTopics.addAll(newlyAddedTopics);
                updateGroupSubscription(allSubscribedTopics);
            }
        }
    }

    @Override
    protected Map<String, ByteBuffer> onLeaderElected(String leaderId,
                                                      String assignmentStrategy,
                                                      List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions,
                                                      boolean skipAssignment) {
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        String assignorName = assignor.name();

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();

        // collect all the owned partitions
        Map<String, List<TopicPartition>> ownedPartitions = new HashMap<>();

        for (JoinGroupResponseData.JoinGroupResponseMember memberSubscription : allSubscriptions) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberSubscription.metadata()));
            subscription.setGroupInstanceId(Optional.ofNullable(memberSubscription.groupInstanceId()));
            subscriptions.put(memberSubscription.memberId(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
            ownedPartitions.put(memberSubscription.memberId(), subscription.ownedPartitions());
        }

        // leader 将开始监视组感兴趣的任何主题的更改，
        // 这确保最终将看到所有元数据更改
        // 应用场景：当作为消费者组的 leader 时，需要关注所有成员订阅的主题集合的变化。
        // 实现细节：调用 subscriptions.groupSubscribe(topics) 更新组订阅，如果订阅的主题列表发生变化，则请求元数据更新。
        // 设计考虑：确保 leader 能够及时感知到与组订阅相关的主题元数据变化。
        updateGroupSubscription(allSubscribedTopics);

        isLeader = true;

        if (skipAssignment) {
            log.info("Skipped assignment for returning static leader at generation {}. The static leader " +
                "will continue with its existing assignment.", generation().generationId);
            assignmentSnapshot = metadataSnapshot;
            return Collections.emptyMap();
        }

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignorName, subscriptions);

        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        // skip the validation for built-in cooperative sticky assignor since we've considered
        // the "generation" of ownedPartition inside the assignor
        if (protocol == RebalanceProtocol.COOPERATIVE && !assignorName.equals(COOPERATIVE_STICKY_ASSIGNOR_NAME)) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        maybeUpdateGroupSubscription(assignorName, assignments, allSubscribedTopics);

        // metadataSnapshot could be updated when the subscription is updated therefore
        // we must take the assignment snapshot after.
        assignmentSnapshot = metadataSnapshot;

        log.info("Finished assignment for group at generation {}: {}", generation().generationId, assignments);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    /**
     * Used by COOPERATIVE rebalance protocol only.
     *
     * Validate the assignments returned by the assignor such that no owned partitions are going to
     * be reassigned to a different consumer directly: if the assignor wants to reassign an owned partition,
     * it must first remove it from the new assignment of the current owner so that it is not assigned to any
     * member, and then in the next rebalance it can finally reassign those partitions not owned by anyone to consumers.
     */
    private void validateCooperativeAssignment(final Map<String, List<TopicPartition>> ownedPartitions,
                                               final Map<String, Assignment> assignments) {
        Set<TopicPartition> totalRevokedPartitions = new HashSet<>();
        SortedSet<TopicPartition> totalAddedPartitions = new TreeSet<>(COMPARATOR);
        for (final Map.Entry<String, Assignment> entry : assignments.entrySet()) {
            final Assignment assignment = entry.getValue();
            final Set<TopicPartition> addedPartitions = new HashSet<>(assignment.partitions());
            addedPartitions.removeAll(ownedPartitions.get(entry.getKey()));
            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions.get(entry.getKey()));
            revokedPartitions.removeAll(assignment.partitions());

            totalAddedPartitions.addAll(addedPartitions);
            totalRevokedPartitions.addAll(revokedPartitions);
        }

        // if there are overlap between revoked partitions and added partitions, it means some partitions
        // immediately gets re-assigned to another member while it is still claimed by some member
        totalAddedPartitions.retainAll(totalRevokedPartitions);
        if (!totalAddedPartitions.isEmpty()) {
            log.error("With the COOPERATIVE protocol, owned partitions cannot be " +
                "reassigned to other members; however the assignor has reassigned partitions {} which are still owned " +
                "by some members", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
        }
    }

    @Override
    protected boolean onJoinPrepare(Timer timer, int generation, String memberId) {
        log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
        if (joinPrepareTimer == null) {
            // We should complete onJoinPrepare before rebalanceTimeoutMs,
            // and continue to join group to avoid member got kicked out from group
            joinPrepareTimer = time.timer(rebalanceConfig.rebalanceTimeoutMs);
        // 如果消费者使用的是手动分区分配策略
        } else {
            joinPrepareTimer.update();
        }

        // async commit offsets prior to rebalance if auto-commit enabled
        // and there is no in-flight offset commit request
        if (autoCommitEnabled && autoCommitOffsetRequestFuture == null) {
            maybeMarkPartitionsPendingRevocation();
            autoCommitOffsetRequestFuture = maybeAutoCommitOffsetsAsync();
        }

        // wait for commit offset response before timer expired
        if (autoCommitOffsetRequestFuture != null) {
            Timer pollTimer = timer.remainingMs() < joinPrepareTimer.remainingMs() ?
                    timer : joinPrepareTimer;
            client.poll(autoCommitOffsetRequestFuture, pollTimer);
            joinPrepareTimer.update();

            // Keep retrying/waiting the offset commit when:
            // 1. offset commit haven't done (and joinPrepareTimer not expired)
            // 2. failed with retriable exception (and joinPrepareTimer not expired)
            // Otherwise, continue to revoke partitions, ex:
            // 1. if joinPrepareTimer has expired
            // 2. if offset commit failed with non-retriable exception
            // 3. if offset commit success
            boolean onJoinPrepareAsyncCommitCompleted = true;
            if (joinPrepareTimer.isExpired()) {
                log.error("Asynchronous auto-commit of offsets failed: joinPrepare timeout. Will continue to join group");
            } else if (!autoCommitOffsetRequestFuture.isDone()) {
                onJoinPrepareAsyncCommitCompleted = false;
            } else if (autoCommitOffsetRequestFuture.failed() && autoCommitOffsetRequestFuture.isRetriable()) {
                log.debug("Asynchronous auto-commit of offsets failed with retryable error: {}. Will retry it.",
                        autoCommitOffsetRequestFuture.exception().getMessage());
                onJoinPrepareAsyncCommitCompleted = false;
            } else if (autoCommitOffsetRequestFuture.failed() && !autoCommitOffsetRequestFuture.isRetriable()) {
                log.error("Asynchronous auto-commit of offsets failed: {}. Will continue to join group.",
                        autoCommitOffsetRequestFuture.exception().getMessage());
            }
            if (autoCommitOffsetRequestFuture.isDone()) {
                autoCommitOffsetRequestFuture = null;
            }
            if (!onJoinPrepareAsyncCommitCompleted) {
                pollTimer.sleep(Math.min(pollTimer.remainingMs(), rebalanceConfig.retryBackoffMs));
                timer.update();
                // 返回 false，表示本次 poll 操作未成功（协调器问题）
                // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
            }
        }

        // the generation / member-id can possibly be reset by the heartbeat thread
        // upon getting errors or heartbeat timeouts; in this case whatever is previously
        // owned partitions would be lost, we should trigger the callback and cleanup the assignment;
        // otherwise we can proceed normally and revoke the partitions depending on the protocol,
        // and in that case we should only change the assignment AFTER the revoke callback is triggered
        // so that users can still access the previously owned partitions to commit offsets etc.
        Exception exception = null;
        final SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(COMPARATOR);
        if (generation == Generation.NO_GENERATION.generationId ||
            memberId.equals(Generation.NO_GENERATION.memberId)) {
            revokedPartitions.addAll(subscriptions.assignedPartitions());

            if (!revokedPartitions.isEmpty()) {
                log.info("Giving away all assigned partitions as lost since generation/memberID has been reset," +
                    "indicating that consumer is in old state or no longer part of the group");
                exception = rebalanceListenerInvoker.invokePartitionsLost(revokedPartitions);

                subscriptions.assignFromSubscribed(Collections.emptySet());
            }
        // 如果消费者使用的是手动分区分配策略
        } else {
            switch (protocol) {
                case EAGER:
                    // revoke all partitions
                    revokedPartitions.addAll(subscriptions.assignedPartitions());
                    exception = rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions);

                    subscriptions.assignFromSubscribed(Collections.emptySet());

                    break;

                case COOPERATIVE:
                    // only revoke those partitions that are not in the subscription anymore.
                    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    revokedPartitions.addAll(ownedPartitions.stream()
                        .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
                        .collect(Collectors.toSet()));

                    if (!revokedPartitions.isEmpty()) {
                        exception = rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions);

                        ownedPartitions.removeAll(revokedPartitions);
                        subscriptions.assignFromSubscribed(ownedPartitions);
                    }

                    break;
            }
        }

        isLeader = false;
        subscriptions.resetGroupSubscription();
        joinPrepareTimer = null;
        autoCommitOffsetRequestFuture = null;
        timer.update();

        if (exception != null) {
            throw new KafkaException("User rebalance callback throws an error", exception);
        }
        // poll 操作成功完成
        return true;
    }

    private void maybeMarkPartitionsPendingRevocation() {
        if (protocol != RebalanceProtocol.EAGER) {
            return;
        }

        // When asynchronously committing offsets prior to the revocation of a set of partitions, there will be a
        // window of time between when the offset commit is sent and when it returns and revocation completes. It is
        // possible for pending fetches for these partitions to return during this time, which means the application's
        // position may get ahead of the committed position prior to revocation. This can cause duplicate consumption.
        // To prevent this, we mark the partitions as "pending revocation," which stops the Fetcher from sending new
        // fetches or returning data from previous fetches to the user.
        Set<TopicPartition> partitions = subscriptions.assignedPartitions();
        log.debug("Marking assigned partitions pending for revocation: {}", partitions);
        subscriptions.markPendingRevocation(partitions);
    }

    @Override
    public void onLeavePrepare() {
        // Save the current Generation, as the hb thread can change it at any time
        final Generation currentGeneration = generation();

        log.debug("Executing onLeavePrepare with generation {}", currentGeneration);

        // we should reset assignment and trigger the callback before leaving group
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(COMPARATOR);
        droppedPartitions.addAll(subscriptions.assignedPartitions());

        if (subscriptions.hasAutoAssignedPartitions() && !droppedPartitions.isEmpty()) {
            final Exception e;
            if ((currentGeneration.generationId == Generation.NO_GENERATION.generationId ||
                currentGeneration.memberId.equals(Generation.NO_GENERATION.memberId)) ||
                rebalanceInProgress()) {
                e = rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);
            // 如果消费者使用的是手动分区分配策略
        } else {
                e = rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions);
            }

            subscriptions.assignFromSubscribed(Collections.emptySet());

            if (e != null) {
                throw new KafkaException("User rebalance callback throws an error", e);
            }
        }
    }

    /**
     * @throws KafkaException if the callback throws exception
     */
    @Override
    public boolean rejoinNeededOrPending() {
        if (!subscriptions.hasAutoAssignedPartitions())
            // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        if (assignmentSnapshot != null && !assignmentSnapshot.matches(metadataSnapshot)) {
            final String fullReason = String.format("cached metadata has changed from %s at the beginning of the rebalance to %s",
                assignmentSnapshot, metadataSnapshot);
            requestRejoinIfNecessary("cached metadata has changed", fullReason);
            // poll 操作成功完成
        return true;
        }

        // we need to join if our subscription has changed since the last join
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            final String fullReason = String.format("subscription has changed from %s at the beginning of the rebalance to %s",
                joinedSubscription, subscriptions.subscription());
            requestRejoinIfNecessary("subscription has changed", fullReason);
            // poll 操作成功完成
        return true;
        }

        return super.rejoinNeededOrPending();
    }

    /**
     * Refresh the committed offsets for partitions that require initialization.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean initWithCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();
        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);

        // "offsets" will be null if the offset fetch requests did not receive responses within the given timeout
        if (offsets == null)
            // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;

        refreshCommittedOffsets(offsets, this.metadata, this.subscriptions);
        // poll 操作成功完成
        return true;
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final Timer timer) {
        if (partitions.isEmpty()) return Collections.emptyMap();

        final Generation generationForOffsetRequest = generationIfStable();
        if (pendingCommittedOffsetRequest != null &&
            !pendingCommittedOffsetRequest.sameRequest(partitions, generationForOffsetRequest)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        long attempts = 0L;
        do {
            if (!ensureCoordinatorReady(timer)) return null;

            // contact coordinator to fetch committed offsets
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            // 如果消费者使用的是手动分区分配策略
        } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generationForOffsetRequest, future);
            }
            client.poll(future, timer);

            if (future.isDone()) {
                pendingCommittedOffsetRequest = null;

                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
                    throw future.exception();
                // 如果消费者使用的是手动分区分配策略
        } else {
                    timer.sleep(retryBackoff.backoff(attempts++));
                }
            // 如果消费者使用的是手动分区分配策略
        } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * Return the consumer group metadata.
     *
     * @return the current consumer group metadata
     */
    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync(timer);
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                ensureCoordinatorReady(timer);
                client.poll(timer);
                // 调用已完成的偏移量提交回调
        invokeCompletedOffsetCommitCallbacks();

            }
        } finally {
            super.close(timer);
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        if (asyncCommitFenced.get()) {
            throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
                + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                + ", current member.id is " + memberId());
        }
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    public RequestFuture<Void> commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 调用已完成的偏移量提交回调
        invokeCompletedOffsetCommitCallbacks();


        RequestFuture<Void> future = null;
        if (offsets.isEmpty()) {
            // No need to check coordinator if offsets is empty since commit of empty offsets is completed locally.
            future = doCommitOffsetsAsync(offsets, callback);
        } else if (!coordinatorUnknownAndUnreadyAsync()) {
            // we need to make sure coordinator is ready before committing, since
            // this is for async committing we do not try to block, but just try once to
            // clear the previous discover-coordinator future, resend, or get responses;
            // if the coordinator is not ready yet then we would just proceed and put that into the
            // pending requests, and future poll calls would still try to complete them.
            //
            // the key here though is that we have to try sending the discover-coordinator if
            // it's not known or ready, since this is the only place we can send such request
            // under manual assignment (there we would not have heartbeat thread trying to auto-rediscover
            // the coordinator).
            future = doCommitOffsetsAsync(offsets, callback);
        // 如果消费者使用的是手动分区分配策略
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    // 轮询网络客户端以发送任何挂起的请求，但不唤醒选择器（如果它正在阻塞）
            client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
        return future;
    }

    private RequestFuture<Void> doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        inFlightAsyncCommits.incrementAndGet();
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<>() {
            @Override
            public void onSuccess(Void value) {
                inFlightAsyncCommits.decrementAndGet();

                if (interceptors != null)
                    interceptors.onCommit(offsets);
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                inFlightAsyncCommits.decrementAndGet();

                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                }
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
                if (commitException instanceof FencedInstanceIdException) {
                    asyncCommitFenced.set(true);
                }
            }
        });
        return future;
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @throws FencedInstanceIdException if a static member gets fenced
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        // 调用已完成的偏移量提交回调
        invokeCompletedOffsetCommitCallbacks();


        if (offsets.isEmpty()) {
            // We guarantee that the callbacks for all commitAsync() will be invoked when
            // commitSync() completes, even if the user tries to commit empty offsets.
            return invokePendingAsyncCommits(timer);
        }

        long attempts = 0L;
        do {
            // 如果协调器未知并且同步尝试使其准备就绪失败
            if (coordinatorUnknownAndUnreadySync(timer)) {
                // 返回 false，表示本次 poll 操作未成功（协调器问题）
                // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
            }

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, timer);

            // We may have had in-flight offset commits when the synchronous commit began. If so, ensure that
            // the corresponding callbacks are invoked prior to returning in order to preserve the order that
            // the offset commits were applied.
            // 调用已完成的偏移量提交回调
        invokeCompletedOffsetCommitCallbacks();


            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                // poll 操作成功完成
        return true;
            }

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            timer.sleep(retryBackoff.backoff(attempts++));
        } while (timer.notExpired());

        // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
    }

    private void maybeAutoCommitOffsetsSync(Timer timer) {
        if (autoCommitEnabled) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                if (!commitOffsetsSync(allConsumedOffsets, timer))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    public void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            nextAutoCommitTimer.update(now);
            if (nextAutoCommitTimer.isExpired()) {
                nextAutoCommitTimer.reset(autoCommitIntervalMs);
                autoCommitOffsetsAsync();
            }
        }
    }

    private boolean invokePendingAsyncCommits(Timer timer) {
        if (inFlightAsyncCommits.get() == 0) {
            // poll 操作成功完成
        return true;
        }

        long attempts = 0L;
        do {
            ensureCoordinatorReady(timer);
            client.poll(timer);
            // 调用已完成的偏移量提交回调
        invokeCompletedOffsetCommitCallbacks();


            if (inFlightAsyncCommits.get() == 0) {
                // poll 操作成功完成
        return true;
            }

            timer.sleep(retryBackoff.backoff(attempts++));
        } while (timer.notExpired());

        // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
    }

    private RequestFuture<Void> autoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        return commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
            if (exception != null) {
                if (exception instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error.", offsets,
                        exception);
                    nextAutoCommitTimer.updateAndReset(rebalanceConfig.retryBackoffMs);
                // 如果消费者使用的是手动分区分配策略
        } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                }
            // 如果消费者使用的是手动分区分配策略
        } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });
    }

    private RequestFuture<Void> maybeAutoCommitOffsetsAsync() {
        if (autoCommitEnabled)
            return autoCommitOffsetsAsync();
        return null;
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * NOTE: This is visible only for testing
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }

            OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                            new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                    .setName(topicPartition.topic())
                    );

            topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
            );
            requestTopicDataMap.put(topicPartition.topic(), topic);
        }

        final Generation generation;
        final String groupInstanceId;
        // 检查当前消费者是否使用了自动分区分配策略
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = generationIfStable();
            groupInstanceId = rebalanceConfig.groupInstanceId.orElse(null);
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in poll().
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");

                if (rebalanceInProgress()) {
                    // if the client knows it is already rebalancing, we can use RebalanceInProgressException instead of
                    // CommitFailedException to indicate this is not a fatal error
                    return RequestFuture.failure(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                        "consumer is undergoing a rebalance for auto partition assignment. You can try completing the rebalance " +
                        "by calling poll() and then retry the operation."));
                // 如果消费者使用的是手动分区分配策略
        } else {
                    return RequestFuture.failure(new CommitFailedException("Offset commit cannot be completed since the " +
                        "consumer is not part of an active group for auto partition assignment; it is likely that the consumer " +
                        "was kicked out of the group."));
                }
            }
        // 如果消费者使用的是手动分区分配策略
        } else {
            generation = Generation.NO_GENERATION;
            groupInstanceId = null;
        }

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationIdOrMemberEpoch(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(groupInstanceId)
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets, generation));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, Generation generation) {
            super(generation);
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            coordinatorMetrics.commitSensor.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);

                    long offset = offsetAndMetadata.offset();

                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                    // 如果消费者使用的是手动分区分配策略
        } else {
                        if (error.exception() instanceof RetriableException) {
                            log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        // 如果消费者使用的是手动分区分配策略
        } else {
                            log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        }

                        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                            return;
                        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                            unauthorizedTopics.add(tp.topic());
                        } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                                || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                            // raise the error to the user
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                                || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                            // just retry
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                                || error == Errors.NOT_COORDINATOR
                                || error == Errors.REQUEST_TIMED_OUT) {
                            markCoordinatorUnknown(error);
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.info("OffsetCommit failed with {} due to group instance id {} fenced", sentGeneration, rebalanceConfig.groupInstanceId);

                            // if the generation has changed or we are not in rebalancing, do not raise the fatal error but rebalance-in-progress
                            if (generationUnchanged()) {
                                future.raise(error);
                            // 如果消费者使用的是手动分区分配策略
        } else {
                                KafkaException exception;
                                synchronized (ConsumerCoordinator.this) {
                                    if (ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                        exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                            "consumer member's old generation is fenced by its group instance id, it is possible that " +
                                            "this consumer has already participated another rebalance and got a new generation");
                                    // 如果消费者使用的是手动分区分配策略
        } else {
                                        exception = new CommitFailedException();
                                    }
                                }
                                future.raise(exception);
                            }
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer should not try to commit offset in between join-group and sync-group,
                             * and hence on broker-side it is not expected to see a commit offset request
                             * during CompletingRebalance phase; if it ever happens then broker would return
                             * this error to indicate that we are still in the middle of a rebalance.
                             * In this case we would throw a RebalanceInProgressException,
                             * request re-join but do not reset generations. If the callers decide to retry they
                             * can go ahead and call poll to finish up the rebalance first, and then try commit again.
                             */
                            requestRejoin("offset commit failed since group is already rebalancing");
                            future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                "consumer group is executing a rebalance at the moment. You can try completing the rebalance " +
                                "by calling poll() and then retry commit again"));
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            log.info("OffsetCommit failed with {}: {}", sentGeneration, error.message());

                            // only need to reset generation and re-join group if generation has not changed or we are not in rebalancing;
                            // otherwise only raise rebalance-in-progress error
                            KafkaException exception;
                            synchronized (ConsumerCoordinator.this) {
                                if (!generationUnchanged() && ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                    exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                        "consumer member's generation is already stale, meaning it has already participated another rebalance and " +
                                        "got a new generation. You can try completing the rebalance by calling poll() and then retry commit again");
                                // 如果消费者使用的是手动分区分配策略
        } else {
                                    // don't reset generation member ID when ILLEGAL_GENERATION, since the member might be still valid
                                    resetStateOnResponseError(ApiKeys.OFFSET_COMMIT, error, error != Errors.ILLEGAL_GENERATION);
                                    exception = new CommitFailedException();
                                }
                            }
                            future.raise(exception);
                            return;
                        // 如果消费者使用的是手动分区分配策略
        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            // 如果消费者使用的是手动分区分配策略
        } else {
                future.complete(null);
            }
        }
    }

    /**
     * 为一组分区获取已提交的位移。这是一个非阻塞调用。
     * 返回的 future 可以被轮询以获取从 broker 返回的实际位移。
     * 
     * 应用场景：当消费者需要知道某些分区的已提交位移时使用，例如在重平衡后或手动查询位移时。
     * 实现细节：
     * 1. 首先检查并获取协调器节点
     * 2. 构造位移获取请求
     * 3. 异步发送请求并注册响应处理器
     * 设计考虑：使用异步方式避免阻塞，提高系统响应性
     *
     * @param partitions 需要获取位移的分区集合
     * @return 包含已提交位移的请求 future
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        // 检查并获取协调器节点
        Node coordinator = checkAndGetCoordinator();
        // 如果协调器不可用，返回错误 future
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // 记录调试日志
        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // 构造请求对象
        OffsetFetchRequest.Builder requestBuilder =
            new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

        // 发送请求并注册回调处理器
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    /**
     * 位移获取响应处理器，负责处理从协调器返回的位移获取响应。
     * 
     * 应用场景：处理异步位移获取请求的响应，解析响应数据并更新本地状态。
     * 实现细节：继承自 CoordinatorResponseHandler，专门处理 OffsetFetchResponse 类型的响应。
     * 设计考虑：
     * 1. 使用内部类方式实现，可以访问外部类的状态
     * 2. 通过继承 CoordinatorResponseHandler 复用通用的响应处理逻辑
     * 3. 采用异步回调方式处理响应，避免阻塞
     */
    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        /**
         * 构造函数
         * 实现细节：调用父类构造函数，传入 NO_GENERATION 表示此请求不依赖于特定的消费者组代数
         */
        private OffsetFetchResponseHandler() {
            super(Generation.NO_GENERATION);
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            // 检查响应中是否存在组级别的错误
            Errors responseError = response.groupLevelError(rebalanceConfig.groupId);
            if (responseError != Errors.NONE) {
                // 记录调试日志
                log.debug("Offset fetch failed: {}", responseError.message());

                // 处理协调器相关错误
                if (responseError == Errors.COORDINATOR_NOT_AVAILABLE ||
                    responseError == Errors.NOT_COORDINATOR) {
                    // 标记协调器为未知并重试
                    markCoordinatorUnknown(responseError);
                    future.raise(responseError);
                } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                    // 处理组授权失败错误
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else if (responseError.exception() instanceof RetriableException) {
                    // 处理可重试错误
                    future.raise(responseError);
                } else {
                    // 处理其他未预期的错误
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
                }
                return;
            }

            // 用于存储未授权的主题
            Set<String> unauthorizedTopics = null;
            // 获取响应中的分区数据
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                response.partitionDataMap(rebalanceConfig.groupId);
            // 创建结果 Map，用于存储解析后的位移元数据
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            // 存储具有不稳定位移的主题分区
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();

            // 遍历处理每个分区的响应数据
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                
                // 处理分区级别的错误
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        // 主题或分区不存在
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        // 主题授权失败
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        // 位移提交不稳定
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        // 处理其他未预期的错误
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                            tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // 处理有效的位移数据
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    // 处理没有提交位移的情况
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            // 处理最终的结果
            if (unauthorizedTopics != null) {
                // 如果存在未授权的主题，抛出异常
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // 如果存在不稳定的位移，记录日志并重试
                log.info("The following partitions still have unstable offsets " +
                             "which are not cleared on the broker side: {}" +
                             ", this could be either " +
                             "transactional offsets waiting for completion, or " +
                             "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                future.raise(new UnstableOffsetCommitException("There are unstable offsets for the requested topic partitions"));
            } else {
                // 成功获取所有位移，完成 future
                future.complete(offsets);
            }
        }
    }

    /**
     * 元数据快照类，用于捕获特定时刻的集群元数据状态。
     * 
     * 应用场景：
     * 1. 在重平衡过程中保存集群的元数据状态
     * 2. 用于比较两个时间点的元数据是否发生变化
     * 3. 支持机架感知的分区分配
     * 
     * 实现细节：
     * 1. 包含版本号和每个主题的分区信息
     * 2. 分区信息包含了机架位置信息
     * 3. 使用不可变对象模式设计
     * 
     * 设计考虑：
     * 1. 将元数据状态封装为快照，便于后续比较和使用
     * 2. 通过版本号快速判断元数据是否变化
     * 3. 支持机架感知的分区分配策略
     */
    private static class MetadataSnapshot {
        /**
         * 元数据版本号
         * 应用场景：用于快速判断两个快照是否表示相同的元数据状态
         */
        private final int version;
        
        /**
         * 每个主题的分区信息映射
         * 应用场景：存储主题的分区列表及其机架位置信息
         * Key: 主题名称
         * Value: 该主题的分区列表，包含机架信息
         */
        private final Map<String, List<PartitionRackInfo>> partitionsPerTopic;

        /**
         * 构造函数，创建一个新的元数据快照
         * 
         * @param clientRack 客户端所在的机架ID
         * @param subscription 订阅状态
         * @param cluster 集群信息
         * @param version 元数据版本号
         */
        private MetadataSnapshot(Optional<String> clientRack, SubscriptionState subscription, Cluster cluster, int version) {
            // 创建主题到分区信息的映射
            Map<String, List<PartitionRackInfo>> partitionsPerTopic = new HashMap<>();
            // 遍历所有订阅的主题
            for (String topic : subscription.metadataTopics()) {
                // 获取主题的分区信息
                List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
                if (partitions != null) {
                    // 将分区信息转换为带有机架信息的格式
                    List<PartitionRackInfo> partitionRacks = partitions.stream()
                            .map(p -> new PartitionRackInfo(clientRack, p))
                            .collect(Collectors.toList());
                    // 保存主题的分区信息
                    partitionsPerTopic.put(topic, partitionRacks);
                }
            }
            // 初始化字段
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        /**
         * 比较两个元数据快照是否匹配
         * 
         * 应用场景：判断元数据是否发生变化，用于决定是否需要触发重平衡
         * 实现细节：
         * 1. 首先比较版本号，如果相同则认为匹配
         * 2. 如果版本号不同，则比较实际的分区数据
         * 
         * @param other 要比较的另一个元数据快照
         * @return 如果两个快照匹配返回true，否则返回false
         */
        boolean matches(MetadataSnapshot other) {
            return version == other.version || partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        @Override
        public String toString() {
            return "(version" + version + ": " + partitionsPerTopic + ")";
        }
    }

    /**
     * 消费者协调器的度量指标管理类。
     * 
     * 应用场景：
     * 1. 收集和监控位移提交的性能指标
     * 2. 跟踪消费者分配的分区数量
     * 3. 为运维和监控提供必要的度量数据
     * 
     * 实现细节：
     * 1. 使用 Sensor 收集位移提交的延迟指标
     * 2. 维护分区分配状态的度量指标
     * 3. 支持平均值、最大值等统计指标
     * 
     * 设计考虑：
     * 1. 将度量指标的管理集中在一个类中，便于维护
     * 2. 使用 Kafka 的度量框架，保持一致性
     * 3. 选择关键指标进行监控，避免过多的性能开销
     */
    private class ConsumerCoordinatorMetrics {
        /**
         * 位移提交的度量传感器
         * 应用场景：用于收集位移提交操作的性能指标
         */
        private final Sensor commitSensor;

        /**
         * 构造函数，初始化所有度量指标
         * 
         * @param metrics Kafka 度量系统实例
         * @param metricGrpPrefix 度量指标组的前缀
         */
        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            // 构造度量指标组名称
            String metricGrpName = metricGrpPrefix + COORDINATOR_METRICS_SUFFIX;

            // 创建位移提交延迟的传感器
            this.commitSensor = metrics.sensor("commit-latency");
            // 添加平均提交延迟指标
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                metricGrpName,
                "The average time taken for a commit request"), new Avg());
            // 添加最大提交延迟指标
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                metricGrpName,
                "The max time taken for a commit request"), new Max());
            // 添加提交操作计数指标
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            // 创建已分配分区数量的度量指标
            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class PartitionRackInfo {
        private final Set<String> racks;

        PartitionRackInfo(Optional<String> clientRack, PartitionInfo partition) {
            if (clientRack.isPresent() && partition.replicas() != null) {
                racks = Arrays.stream(partition.replicas()).map(Node::rack).collect(Collectors.toSet());
            // 如果消费者使用的是手动分区分配策略
        } else {
                racks = Collections.emptySet();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                // poll 操作成功完成
        return true;
            }
            if (!(o instanceof PartitionRackInfo)) {
                // 返回 false，表示本次 poll 操作未成功（协调器问题）
                // 返回 false，表示本次 poll 操作未成功（元数据问题）
                        // 返回 false，表示本次 poll 操作未成功（未能加入或保持活跃组）
                    return false;
            }
            PartitionRackInfo rackInfo = (PartitionRackInfo) o;
            return Objects.equals(racks, rackInfo.racks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(racks);
        }

        @Override
        public String toString() {
            return racks.isEmpty() ? "NO_RACKS" : "racks=" + racks;
        }
    }

    /**
     * OffsetCommitCompletion 是一个内部静态类，用于封装偏移量提交操作完成后的回调逻辑。
     * 应用场景：当异步提交偏移量操作完成后，此类用于执行用户提供的回调函数，并传递提交结果（成功或失败）。
     * 实现细节：它存储了回调接口、提交的偏移量信息以及可能发生的异常。
     * 设计考虑：将回调逻辑封装在一个独立的类中，使得偏移量提交的处理更加模块化和清晰。
     */
    private static class OffsetCommitCompletion {
        // OffsetCommitCallback 类型的回调接口，用于在偏移量提交完成后调用
        private final OffsetCommitCallback callback;
        // 存储已提交的 TopicPartition 及其对应的 OffsetAndMetadata 的映射
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        // 存储偏移量提交过程中可能发生的异常
        private final Exception exception;

        /**
         * OffsetCommitCompletion 的构造函数。
         * @param callback 偏移量提交完成后的回调接口。
         * @param offsets 已提交的 TopicPartition 及其对应的 OffsetAndMetadata 的映射。
         * @param exception 偏移量提交过程中发生的异常，如果没有异常则为 null。
         */
        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            // 初始化回调接口
            this.callback = callback;
            // 初始化已提交的偏移量信息
            this.offsets = offsets;
            // 初始化异常信息
            this.exception = exception;
        }

        /**
         * 执行回调方法。
         * 应用场景：在偏移量提交操作（无论是成功还是失败）完成后，调用此方法来通知用户。
         * 实现细节：检查回调接口是否为 null，如果不为 null，则调用其 onComplete 方法，并传入偏移量和异常信息。
         * 设计考虑：提供一个统一的调用点来执行回调，简化了外部代码的逻辑。
         */
        public void invoke() {
            // 检查回调接口是否已设置
            if (callback != null)
                // 如果回调接口不为 null，则调用其 onComplete 方法，传入偏移量和异常信息
                callback.onComplete(offsets, exception);
        }
    }

    /* 下面是仅供测试使用的类 */
    /**
     * 获取当前的再均衡协议。
     * 应用场景：主要用于测试，以验证消费者协调器内部使用的再均衡协议是否正确。
     * 实现细节：直接返回内部存储的 protocol 字段。
     * 设计考虑：提供一个访问内部状态的方法，方便进行单元测试和集成测试。
     * @return RebalanceProtocol 当前使用的再均衡协议。
     */
    RebalanceProtocol getProtocol() {
        // 返回内部存储的再均衡协议实例
        return protocol;
    }

    /**
     * 轮询消费者协调器的状态，允许指定是否应该阻塞等待协调器准备就绪。
     * 应用场景：用于测试场景，模拟消费者的轮询操作，并检查协调器的状态变化。
     * 实现细节：调用另一个重载的 poll 方法，并传递一个指示是否阻塞的布尔值。
     * 设计考虑：提供一个简化的 poll 方法接口，默认情况下允许阻塞等待。
     * @param timer 用于控制轮询超时的计时器。
     * @return boolean 如果轮询成功并且协调器状态发生变化或有事件处理，则返回 true；否则返回 false。
     */
    boolean poll(Timer timer) {
        // 调用另一个 poll 方法，并设置 ensureCoordinatorReady 为 true，表示需要确保协调器准备就绪
        return poll(timer, true);
    }
}

