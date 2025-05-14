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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.events.AllTopicsMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.clients.consumer.internals.events.CurrentLagEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.PausePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetOffsetEvent;
import org.apache.kafka.clients.consumer.internals.events.ResumePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.SeekUnvalidatedEvent;
import org.apache.kafka.clients.consumer.internals.events.StopFindCoordinatorOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicRe2JPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.events.UpdatePatternSubscriptionEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * 此 {@link Consumer} 实现使用 {@link ApplicationEventHandler 事件处理器} 来处理
 * {@link ApplicationEvent 应用程序事件}，以便网络 I/O 可以在专用的
 * {@link ConsumerNetworkThread 网络线程} 中处理。有关实现细节，请访问
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Consumer+threading+refactor+design">此文档</a>。
 *
 * <p/>
 *
 * <em>注意：</em>此 {@link Consumer} 实现是 KIP-848 修订的消费者组协议的一部分。
 * 不应直接调用此类；用户应像以前一样创建 {@link KafkaConsumer}。
 * 此消费者实现了新的消费者组协议，并旨在成为未来版本中的默认实现。
 */
public class AsyncKafkaConsumer<K, V> implements ConsumerDelegate<K, V> { // AsyncKafkaConsumer 类定义，实现了 ConsumerDelegate 接口，用于异步处理 Kafka 消费逻辑。泛型 K, V 分别代表消息的键和值类型。

    // 表示没有当前线程的常量值，用于并发控制，确保消费者操作的线程安全。
    private static final long NO_CURRENT_THREAD = -1L;

    /**
     * 一个在应用程序线程中创建并执行的 {@link org.apache.kafka.clients.consumer.internals.events.EventProcessor 事件处理器}，
     * 用于处理由 {@link ConsumerNetworkThread 网络线程} 生成的 {@link BackgroundEvent 后台事件}。
     * 这些事件通常分为两类：
     *
     * <ul>
     *     <li>网络线程中发生的、需要传播到应用程序线程的错误</li>
     *     <li>需要在应用程序线程上执行的 {@link ConsumerRebalanceListener} 回调</li>
     * </ul>
     * 此内部类负责在主应用线程中处理来自网络线程的事件，例如错误传递和再平衡回调的执行。
     * 应用场景：当网络线程遇到需要应用层面处理的事件时，会通过此处理器通知应用线程。
     * 设计考虑：将网络I/O与应用逻辑分离，避免阻塞网络线程，同时确保回调在正确的线程上下文中执行。
     */
    private class BackgroundEventProcessor implements EventProcessor<BackgroundEvent> { // BackgroundEventProcessor 内部类定义，实现了 EventProcessor 接口，专门处理 BackgroundEvent。

        /**
         * 处理传入的 {@link BackgroundEvent 后台事件}。
         * 此方法是事件处理的核心，根据事件类型分发到具体的处理逻辑。
         * 应用场景：作为后台事件队列的消费者，处理各种类型的后台通知。
         * 设计考虑：使用 switch 语句清晰地根据事件类型进行分发，易于扩展新的事件类型。
         * @param event 要处理的 {@link BackgroundEvent 后台事件}，不能为 null。
         */
        @Override
        public void process(final BackgroundEvent event) {
            // 根据事件的类型进行分支处理
            switch (event.type()) {
                // 如果事件类型是错误事件
                case ERROR:
                    // 调用 process 方法处理错误事件
                    process((ErrorEvent) event);
                    // 中断 switch 语句
                    break;

                // 如果事件类型是需要消费者再平衡监听器回调的事件
                case CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED:
                    // 调用 process 方法处理消费者再平衡监听器回调事件
                    process((ConsumerRebalanceListenerCallbackNeededEvent) event);
                    // 中断 switch 语句
                    break;

                // 如果事件类型不是预期的类型
                default:
                    // 抛出 IllegalArgumentException 异常，说明遇到了未预期的后台事件类型
                    throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");

            }
        }

        /**
         * 处理 {@link ErrorEvent 错误事件}。
         * 此方法简单地重新抛出事件中包含的错误，将其传播到应用程序线程。
         * 应用场景：当网络线程发生异常时，通过此方法将异常通知给应用线程。
         * 设计考虑：直接抛出异常，使得应用线程可以捕获并处理来自网络线程的错误。
         * @param event 包含错误的 {@link ErrorEvent 错误事件}。
         */
        private void process(final ErrorEvent event) {
            // 抛出事件中携带的错误对象
            throw event.error();
        }

        /**
         * 处理 {@link ConsumerRebalanceListenerCallbackNeededEvent 需要消费者再平衡监听器回调的事件}。
         * 此方法调用 {@code invokeRebalanceCallbacks} 来执行适当的 {@link ConsumerRebalanceListener} 回调，
         * 然后将 {@link ConsumerRebalanceListenerCallbackCompletedEvent 回调完成事件} 添加到应用程序事件处理器。
         * 如果回调执行过程中发生错误，则会抛出该错误。
         * 应用场景：在消费者分区分配发生变化时，执行用户定义的再平衡监听器逻辑。
         * 设计考虑：确保再平衡回调在应用线程中执行，并将回调结果（成功或失败）通知回网络线程或相关组件。
         * @param event {@link ConsumerRebalanceListenerCallbackNeededEvent 需要消费者再平衡监听器回调的事件}，其中包含回调方法名、分区信息和用于通知完成的 future。
         */
        private void process(final ConsumerRebalanceListenerCallbackNeededEvent event) {
            // 调用 invokeRebalanceCallbacks 方法执行再平衡监听器的回调
            ConsumerRebalanceListenerCallbackCompletedEvent invokedEvent = invokeRebalanceCallbacks(
                rebalanceListenerInvoker, // 再平衡监听器调用器
                event.methodName(),       // 需要调用的回调方法名 (例如 "onPartitionsAssigned", "onPartitionsRevoked")
                event.partitions(),       // 相关的分区集合
                event.future()            // 用于通知回调完成的 CompletableFuture
            );
            // 将回调完成事件添加到应用程序事件处理器队列中，以便网络线程可以感知回调已执行完毕
            applicationEventHandler.add(invokedEvent);
            // 检查回调执行过程中是否发生错误
            if (invokedEvent.error().isPresent()) {
                // 如果存在错误，则获取并抛出该错误
                throw invokedEvent.error().get();
            }
        }
    }

    // 应用程序事件处理器，用于将事件从消费者API调用传递到网络线程。
    private final ApplicationEventHandler applicationEventHandler;
    // Time 接口的实例，用于获取当前时间和进行时间相关的操作，例如超时计算。
    private final Time time;
    // 原子引用的 Optional<ConsumerGroupMetadata>，存储消费者组元数据。使用 AtomicReference 保证线程安全更新。
    private final AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata = new AtomicReference<>(Optional.empty());
    // 异步消费者度量指标收集器，用于收集和报告与异步消费者相关的性能指标。
    private final AsyncConsumerMetrics kafkaConsumerMetrics;
    // 日志记录器，用于记录消费者的日志信息。
    private Logger log;
    // 客户端ID，用于在 Kafka 集群中唯一标识此消费者实例。
    private final String clientId;
    // 阻塞队列，用于存储从网络线程传递到应用程序线程的后台事件。
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    // 后台事件处理器，负责轮询 backgroundEventQueue 并将事件分派给 backgroundEventProcessor。
    private final BackgroundEventHandler backgroundEventHandler;
    // 后台事件处理器实例，在应用程序线程中处理后台事件。
    private final BackgroundEventProcessor backgroundEventProcessor;
    // 可完成事件清理器，用于清理已完成的 CompletableEvent，防止内存泄漏。
    private final CompletableEventReaper backgroundEventReaper;
    // 反序列化器集合，包含键和值的反序列化器，用于将从 Kafka 获取的字节数据转换为对象。
    private final Deserializers<K, V> deserializers;

    /**
     * 一个线程安全的 {@link FetchBuffer fetch buffer}，用于存储在 {@link ConsumerNetworkThread 网络线程} 中结果可用时填充的结果。
     * 由于 fetch buffer 在应用程序线程和网络 I/O 线程之间的交互，它在两个线程之间共享，因此被设计为线程安全的。
     * 应用场景：在异步消费模式下，网络线程获取数据后放入此缓冲区，应用线程从中读取数据进行处理。
     * 设计考虑：线程安全是首要考虑，确保多线程访问时数据的一致性和完整性。
     */
    private final FetchBuffer fetchBuffer; // 拉取缓冲区，线程安全，用于在网络线程和应用线程之间传递拉取到的数据
    private final FetchCollector<K, V> fetchCollector; // 拉取收集器，用于从 FetchBuffer 中收集和处理拉取到的记录
    private final ConsumerInterceptors<K, V> interceptors; // 消费者拦截器，用于在消费消息的各个阶段执行自定义逻辑
    private final IsolationLevel isolationLevel; // 隔离级别，定义消费者可以读取的消息类型（例如，只读已提交的事务消息）

    private final SubscriptionState subscriptions; // 订阅状态，维护消费者当前的订阅信息（主题、分区）和消费位置

    /**
     * 这是分配给此消费者的分区快照。但是，这仅在消费者属于消费者组的情况下填充和使用。用户自分配的分区不会出现在这里。
     * 应用场景：在消费者组模式下，用于跟踪当前分配给该消费者的分区集合。
     * 设计考虑：使用 AtomicReference 保证快照的原子性更新和读取。
     */
    private final AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot = new AtomicReference<>(Collections.emptySet()); // 消费者组分配的分区快照，原子引用保证线程安全
    private final ConsumerMetadata metadata; // 消费者元数据，维护集群的元数据信息，如 broker、主题、分区等
    private final Metrics metrics; // 度量指标收集器，用于收集和报告消费者的各种性能指标
    private final long retryBackoffMs; // 重试退避时间（毫秒），在操作失败后等待多长时间再重试
    private final int requestTimeoutMs; // 请求超时时间（毫秒），网络请求的最长等待时间
    private final Duration defaultApiTimeoutMs; // 默认 API 超时时间，用于各种需要超时的 API 调用
    private final boolean autoCommitEnabled; // 是否启用自动提交偏移量
    private volatile boolean closed = false; // 消费者是否已关闭的标志，volatile 保证多线程间的可见性
    // 初始值是必需的，以避免在构造函数中引发异常时出现 NPE
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty(); // 客户端遥测报告器，用于收集和发送客户端遥测数据

    // 为了避免在 poll() 中重复扫描订阅，在元数据更新期间缓存结果
    private boolean cachedSubscriptionHasAllFetchPositions; // 缓存订阅是否具有所有拉取位置的标志，用于优化 poll() 操作
    private final WakeupTrigger wakeupTrigger = new WakeupTrigger(); // 唤醒触发器，用于从阻塞的 poll() 操作中唤醒消费者线程
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker; // 偏移量提交回调调用器，用于异步执行偏移量提交后的回调逻辑
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker; // 消费者再平衡监听器调用器，用于执行再平衡相关的回调逻辑
    // 上一个触发的异步提交 future。用于等待所有先前的异步提交完成。
    // 我们只需要跟踪最后一个，因为它们保证按顺序完成。
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> lastPendingAsyncCommit = null; // 最后一个待处理的异步提交的 Future，用于确保异步提交按序完成

    // currentThread 持有当前访问 AsyncKafkaConsumer 的线程的 threadId
    // 并用于防止多线程访问
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD); // 当前访问消费者的线程 ID，用于确保单线程访问关键操作
    private final AtomicInteger refCount = new AtomicInteger(0); // 引用计数器，允许多次进入同一线程的消费者方法

    /**
     * 成员状态监听器，用于响应消费者组成员身份和分区分配的变化。
     * 应用场景：当消费者在组内的 epoch 更新或分区分配发生变化时，此监听器会被调用。
     * 设计考虑：通过回调机制，将底层的成员状态变化通知给上层逻辑进行处理。
     */
    private final MemberStateListener memberStateListener = new MemberStateListener() { // 成员状态监听器，用于处理组成员 epoch 更新和组分配更新事件
        /**
         * 当成员的 epoch 更新时调用。
         * @param memberEpoch 可选的成员 epoch，如果不存在则为空
         * @param memberId 成员 ID
         */
        @Override
        public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
            // 调用 updateGroupMetadata 方法更新组元数据
            updateGroupMetadata(memberEpoch, memberId);
        }

        /**
         * 当组分配的分区更新时调用。
         * @param partitions 新分配的分区集合
         */
        @Override
        public void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {
            // 调用 setGroupAssignmentSnapshot 方法设置组分配快照
            setGroupAssignmentSnapshot(partitions);
        }
    };

    /**
     * {@code AsyncKafkaConsumer} 的构造函数。
     * 这是供用户使用的主要构造函数，它使用默认的工厂方法创建内部组件。
     * 应用场景：当用户创建一个新的 {@code AsyncKafkaConsumer} 实例时调用。
     * 设计考虑：提供一个简化的构造函数，隐藏内部组件的创建细节，方便用户使用。
     * @param config 消费者配置
     * @param keyDeserializer 键的反序列化器
     * @param valueDeserializer 值的反序列化器
     */
    AsyncKafkaConsumer(final ConsumerConfig config, // 消费者配置对象
                       final Deserializer<K> keyDeserializer, // 键的反序列化器
                       final Deserializer<V> valueDeserializer) { // 值的反序列化器
        // 调用另一个构造函数，传入默认的 Time、ApplicationEventHandlerFactory、CompletableEventReaperFactory、
        // FetchCollectorFactory、ConsumerMetadataFactory 和一个新创建的 LinkedBlockingQueue 作为 backgroundEventQueue
        this(
            config, // 传递消费者配置
            keyDeserializer, // 传递键的反序列化器
            valueDeserializer, // 传递值的反序列化器
            Time.SYSTEM, // 使用系统时间作为时间源
            ApplicationEventHandler::new, // 使用 ApplicationEventHandler 的构造函数引用作为工厂
            CompletableEventReaper::new, // 使用 CompletableEventReaper 的构造函数引用作为工厂
            FetchCollector::new, // 使用 FetchCollector 的构造函数引用作为工厂
            ConsumerMetadata::new, // 使用 ConsumerMetadata 的构造函数引用作为工厂
            new LinkedBlockingQueue<>() // 创建一个新的 LinkedBlockingQueue 作为后台事件队列
        );
    }

    // 仅用于测试
    /**
     * {@code AsyncKafkaConsumer} 的构造函数，允许注入自定义的工厂和组件，主要用于测试。
     * 应用场景：在单元测试或集成测试中，替换某些组件为 mock 对象，以便更好地控制测试环境和验证逻辑。
     * 设计考虑：提供最大的灵活性，允许测试代码完全控制消费者的内部依赖。
     * @param config 消费者配置
     * @param keyDeserializer 键的反序列化器
     * @param valueDeserializer 值的反序列化器
     * @param time 时间工具
     * @param applicationEventHandlerFactory 应用程序事件处理器工厂
     * @param backgroundEventReaperFactory 后台事件收集器工厂
     * @param fetchCollectorFactory 拉取收集器工厂
     * @param metadataFactory 消费者元数据工厂
     * @param backgroundEventQueue 后台事件队列
     */
    AsyncKafkaConsumer(final ConsumerConfig config, // 消费者配置对象
                       final Deserializer<K> keyDeserializer, // 键的反序列化器
                       final Deserializer<V> valueDeserializer, // 值的反序列化器
                       final Time time, // 时间工具，用于获取当前时间等
                       final ApplicationEventHandlerFactory applicationEventHandlerFactory, // 应用程序事件处理器工厂
                       final CompletableEventReaperFactory backgroundEventReaperFactory, // 后台可完成事件收集器工厂
                       final FetchCollectorFactory<K, V> fetchCollectorFactory, // 拉取收集器工厂
                       final ConsumerMetadataFactory metadataFactory, // 消费者元数据工厂
                       final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue) { // 后台事件队列，用于在不同线程间传递事件
        try {
            // 创建组重平衡配置，指定协议类型为 CONSUMER
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                config, // 传入消费者配置
                GroupRebalanceConfig.ProtocolType.CONSUMER // 指定协议类型为消费者
            );
            // 从配置中获取客户端 ID
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            // 从配置中获取是否启用自动提交
            this.autoCommitEnabled = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            // 创建日志上下文
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            // 设置后台事件队列
            this.backgroundEventQueue = backgroundEventQueue;
            // 获取当前类的日志记录器
            this.log = logContext.logger(getClass());

            // 记录调试信息：正在初始化 Kafka 消费者
            log.debug("Initializing the Kafka consumer");
            // 从配置中获取默认 API 超时时间，并转换为 Duration 对象
            this.defaultApiTimeoutMs = Duration.ofMillis(config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
            // 设置时间工具
            this.time = time;
            // 获取度量报告器列表
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            // 获取客户端遥测报告器
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            // 如果客户端遥测报告器存在，则将其添加到报告器列表中
            this.clientTelemetryReporter.ifPresent(reporters::add);
            // 创建度量指标收集器
            this.metrics = createMetrics(config, time, reporters);
            // 创建异步消费者度量指标对象
            this.kafkaConsumerMetrics = new AsyncConsumerMetrics(metrics); // 创建并设置异步消费者度量指标对象，关联全局度量收集器。
            // 从配置中获取重试退避时间
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            // 从配置中获取请求超时时间
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);

            // 获取配置的消费者拦截器列表
            List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
            // 创建消费者拦截器组合对象
            this.interceptors = new ConsumerInterceptors<>(interceptorList, metrics);
            // 创建反序列化器组合对象
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer, metrics);
            // 创建订阅状态对象
            this.subscriptions = createSubscriptionState(config, logContext);
            // 配置集群资源监听器
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                    interceptorList, // 传入拦截器列表
                    Arrays.asList(deserializers.keyDeserializer(), deserializers.valueDeserializer())); // 传入键和值的反序列化器
            // 构建消费者元数据对象
            this.metadata = metadataFactory.build(config, subscriptions, logContext, clusterResourceListeners);
            // 解析并验证引导服务器地址列表
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            // 使用引导服务器地址初始化元数据
            metadata.bootstrap(addresses);

            // 创建拉取度量指标管理器
            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            // 创建拉取配置对象
            FetchConfig fetchConfig = new FetchConfig(config);
            // 设置隔离级别
            this.isolationLevel = fetchConfig.isolationLevel;

            // 创建 API 版本对象
            ApiVersions apiVersions = new ApiVersions();
            // 创建应用程序事件队列
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            // 创建后台事件处理器
            this.backgroundEventHandler = new BackgroundEventHandler(
                backgroundEventQueue, // 传入后台事件队列
                time, // 传入时间工具
                kafkaConsumerMetrics // 传入异步消费者度量指标
            );

            // 此 FetchBuffer 在应用程序线程和网络线程之间共享。
            this.fetchBuffer = new FetchBuffer(logContext); // 创建拉取缓冲区
            // 创建 NetworkClientDelegate 的供应器
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time, // 时间工具
                    logContext, // 日志上下文
                    metadata, // 消费者元数据
                    config, // 消费者配置
                    apiVersions, // API 版本
                    metrics, // 度量指标收集器
                    fetchMetricsManager.throttleTimeSensor(), // 拉取度量管理器的节流时间传感器
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null), // 客户端遥测发送器
                    backgroundEventHandler, // 后台事件处理器
                    false, // 是否是共享消费者
                    kafkaConsumerMetrics // 异步消费者度量指标
            );
            // 创建偏移量提交回调调用器
            this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors); // 创建并设置偏移量提交回调调用器，使用配置的拦截器。
            // 初始化并设置组元数据
            this.groupMetadata.set(initializeGroupMetadata(config, groupRebalanceConfig)); // 初始化并设置消费者组元数据，使用消费者配置和再平衡配置。
            // 创建 RequestManagers 的供应器
            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(time, // 时间工具
                    logContext, // 日志上下文
                    backgroundEventHandler, // 后台事件处理器
                    metadata, // 消费者元数据
                    subscriptions, // 订阅状态
                    fetchBuffer, // 拉取缓冲区
                    config, // 消费者配置
                    groupRebalanceConfig, // 组重平衡配置
                    apiVersions, // API 版本
                    fetchMetricsManager, // 拉取度量管理器
                    networkClientDelegateSupplier, // NetworkClientDelegate 供应器
                    clientTelemetryReporter, // 客户端遥测报告器
                    metrics, // 度量指标收集器
                    offsetCommitCallbackInvoker, // 偏移量提交回调调用器
                    memberStateListener // 成员状态监听器
            );
            // 创建 ApplicationEventProcessor 的供应器
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext, // 日志上下文
                    metadata, // 消费者元数据
                    subscriptions, // 订阅状态
                    requestManagersSupplier // RequestManagers 供应器
            );
            // 构建应用程序事件处理器
            this.applicationEventHandler = applicationEventHandlerFactory.build(
                    logContext, // 日志上下文
                    time, // 时间工具
                    applicationEventQueue, // 应用程序事件队列
                    new CompletableEventReaper(logContext), // 可完成事件收集器
                    applicationEventProcessorSupplier, // ApplicationEventProcessor 供应器
                    networkClientDelegateSupplier, // NetworkClientDelegate 供应器
                    requestManagersSupplier, // RequestManagers 供应器
                    kafkaConsumerMetrics // 异步消费者度量指标
            );

            // 创建消费者再平衡监听器调用器
            this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
                    logContext, // 日志上下文
                    subscriptions, // 订阅状态
                    time, // 时间工具
                    new RebalanceCallbackMetricsManager(metrics) // 再平衡回调度量管理器
            );
            // 创建后台事件处理器实例
            this.backgroundEventProcessor = new BackgroundEventProcessor();
            // 构建后台事件收集器
            this.backgroundEventReaper = backgroundEventReaperFactory.build(logContext);

            // FetchCollector 仅在应用程序线程中使用。
            this.fetchCollector = fetchCollectorFactory.build(logContext, // 日志上下文
                    metadata, // 消费者元数据
                    subscriptions, // 订阅状态
                    fetchConfig, // 拉取配置
                    deserializers, // 反序列化器
                    fetchMetricsManager, // 拉取度量管理器
                    time); // 时间工具

            // 如果存在组元数据并且组协议是 CONSUMER
            if (groupMetadata.get().isPresent() &&
                GroupProtocol.of(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG)) == GroupProtocol.CONSUMER) {
                // 忽略 GROUP_REMOTE_ASSIGNOR_CONFIG 配置项，因为它由后台线程使用
                config.ignore(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG); // 由后台线程使用
            }
            // 记录未使用的配置项
            config.logUnused();
            // 注册应用程序信息以供 JMX 使用
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            // 记录调试信息：Kafka 消费者已初始化
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) { // 捕获任何在构造过程中发生的异常
            // 如果内部对象已构造，则调用 close 方法；这是为了防止资源泄漏。参见 KAFKA-2121
            // 如果 `log` 为 null，则表示没有初始化任何内部对象，因此根本不需要调用 `close`。
            if (this.log != null) { // 检查日志记录器是否已初始化
                // 调用 close 方法关闭消费者，超时时间为零，强制关闭
                close(Duration.ZERO, true);
            }
            // 现在传播异常
            throw new KafkaException("Failed to construct kafka consumer", t); // 抛出 KafkaException，包装原始异常
        }
    }

    // 仅用于测试
    /**
     * 构造一个新的 AsyncKafkaConsumer 实例，主要用于测试场景。
     * 此构造函数允许直接注入各种依赖项，方便进行单元测试和集成测试。
     *
     * @param logContext 日志上下文，用于记录消费者内部操作。
     * @param clientId 客户端ID，用于标识此消费者实例。
     * @param deserializers 键和值的反序列化器，用于将从 Kafka 获取的字节数据转换为 Java 对象。
     * @param fetchBuffer 拉取缓冲区，用于存储从服务器拉取的消息批次。
     * @param fetchCollector 拉取收集器，负责将拉取到的数据转换为 {@link ConsumerRecords}。
     * @param interceptors 消费者拦截器列表，允许在消费消息的各个阶段插入自定义逻辑。
     * @param time 时间工具，用于获取当前时间戳和处理超时等逻辑。
     * @param applicationEventHandler 应用程序事件处理器，处理来自应用程序的事件，如提交偏移量、订阅主题等。
     * @param backgroundEventQueue 后台事件队列，用于在应用线程和网络线程之间传递事件。
     * @param backgroundEventReaper 后台事件收割者，负责清理已完成的后台事件。
     * @param rebalanceListenerInvoker 消费者再平衡监听器调用器，负责在分区分配发生变化时调用用户提供的监听器。
     * @param metrics 度量收集器，用于收集和报告消费者的性能指标。
     * @param subscriptions 订阅状态管理器，维护消费者当前订阅的主题和分区信息。
     * @param metadata 消费者元数据管理器，维护集群和主题的元数据信息。
     * @param retryBackoffMs 重试退避时间（毫秒），在操作失败后等待多长时间再重试。
     * @param requestTimeoutMs 请求超时时间（毫秒），网络请求的最长等待时间。
     * @param defaultApiTimeoutMs 默认 API 超时时间（毫秒），各种消费者 API 调用的默认超时设置。
     * @param groupId 消费者组 ID，标识此消费者所属的组。
     * @param autoCommitEnabled 是否启用自动提交偏移量。
     * @implNote 此构造函数的设计目标是提供最大的灵活性，以便在测试环境中模拟各种场景和依赖关系。
     *           它暴露了许多内部组件，这在生产代码中通常是不推荐的，但在测试中非常有用。
     */
    AsyncKafkaConsumer(LogContext logContext,
                       String clientId,
                       Deserializers<K, V> deserializers,
                       FetchBuffer fetchBuffer,
                       FetchCollector<K, V> fetchCollector,
                       ConsumerInterceptors<K, V> interceptors,
                       Time time,
                       ApplicationEventHandler applicationEventHandler,
                       BlockingQueue<BackgroundEvent> backgroundEventQueue,
                       CompletableEventReaper backgroundEventReaper,
                       ConsumerRebalanceListenerInvoker rebalanceListenerInvoker,
                       Metrics metrics,
                       SubscriptionState subscriptions,
                       ConsumerMetadata metadata,
                       long retryBackoffMs,
                       int requestTimeoutMs,
                       int defaultApiTimeoutMs,
                       String groupId,
                       boolean autoCommitEnabled) {
        this.log = logContext.logger(getClass()); // 初始化日志记录器，使用提供的 logContext 获取当前类的 logger 实例。
        this.subscriptions = subscriptions; // 设置订阅状态对象。
        this.clientId = clientId; // 设置客户端 ID。
        this.fetchBuffer = fetchBuffer; // 设置拉取缓冲区。
        this.fetchCollector = fetchCollector; // 设置拉取收集器。
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED; // 设置隔离级别为 READ_UNCOMMITTED，表示可以读取未提交的事务消息。
        this.interceptors = Objects.requireNonNull(interceptors); // 设置消费者拦截器，并确保其不为 null。
        this.time = time; // 设置时间工具。
        this.backgroundEventQueue = backgroundEventQueue; // 设置后台事件队列。
        this.rebalanceListenerInvoker = rebalanceListenerInvoker; // 设置再平衡监听器调用器。
        this.backgroundEventProcessor = new BackgroundEventProcessor(); // 创建并设置后台事件处理器。
        this.backgroundEventReaper = backgroundEventReaper; // 设置后台事件收割者。
        this.metrics = metrics; // 设置度量收集器。
        this.groupMetadata.set(initializeGroupMetadata(groupId, Optional.empty())); // 初始化并设置消费者组元数据。groupId 用于标识消费者组，Optional.empty() 表示没有指定 groupInstanceId。
        this.metadata = metadata; // 设置消费者元数据。
        this.retryBackoffMs = retryBackoffMs; // 设置重试退避时间。
        this.requestTimeoutMs = requestTimeoutMs; // 设置请求超时时间。
        this.defaultApiTimeoutMs = Duration.ofMillis(defaultApiTimeoutMs); // 设置默认 API 超时时间，将其从毫秒转换为 Duration 对象。
        this.deserializers = deserializers; // 设置反序列化器。
        this.applicationEventHandler = applicationEventHandler; // 设置应用程序事件处理器。
        this.kafkaConsumerMetrics = new AsyncConsumerMetrics(metrics); // 创建并设置异步消费者度量指标对象，关联全局度量收集器。 // 创建并设置异步消费者度量指标对象。
        this.clientTelemetryReporter = Optional.empty(); // 设置客户端遥测报告器为空，表示当前不启用遥测。
        this.autoCommitEnabled = autoCommitEnabled; // 设置是否启用自动提交。
        this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors); // 创建并设置偏移量提交回调调用器，使用配置的拦截器。 // 创建并设置偏移量提交回调调用器，使用提供的拦截器。
        this.backgroundEventHandler = new BackgroundEventHandler( // 创建并设置后台事件处理器，用于处理后台队列中的事件。
            backgroundEventQueue, // 后台事件队列，用于接收来自网络线程的事件。
            time, // 时间工具，用于事件处理中的时间相关操作。
            kafkaConsumerMetrics // Kafka 消费者度量指标，用于记录相关指标。
        );
    }

    /**
     * 构造一个新的 AsyncKafkaConsumer 实例，这是主要的生产环境构造函数。
     * 它根据提供的 {@link ConsumerConfig} 初始化消费者，并设置所有必要的内部组件。
     *
     * @param logContext 日志上下文，用于记录消费者内部操作。
     * @param time 时间工具，用于获取当前时间戳和处理超时等逻辑。
     * @param config 消费者配置对象，包含了所有与消费者行为相关的配置项。
     * @param keyDeserializer 消息键的反序列化器。
     * @param valueDeserializer 消息值的反序列化器。
     * @param client Kafka 客户端，用于与 Kafka 集群进行网络通信。
     * @param subscriptions 订阅状态管理器，维护消费者当前订阅的主题和分区信息。
     * @param metadata 消费者元数据管理器，维护集群和主题的元数据信息。
     * @implNote 此构造函数负责组装和配置 AsyncKafkaConsumer 的所有核心组件。
     *           它从 {@link ConsumerConfig} 中读取配置，并据此创建和初始化如 FetchCollector、
     *           ApplicationEventHandler、BackgroundEventHandler 等关键对象。
     *           设计上，它封装了大部分复杂性，为用户提供了一个相对简单的创建消费者的入口。
     */
    AsyncKafkaConsumer(LogContext logContext,
                       Time time,
                       ConsumerConfig config,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer,
                       KafkaClient client,
                       SubscriptionState subscriptions,
                       ConsumerMetadata metadata) {
        this.log = logContext.logger(getClass()); // 初始化日志记录器，使用提供的 logContext 获取当前类的 logger 实例。
        this.subscriptions = subscriptions; // 设置订阅状态对象。
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG); // 从配置中获取并设置客户端 ID。
        this.autoCommitEnabled = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG); // 从配置中获取并设置是否启用自动提交。
        this.fetchBuffer = new FetchBuffer(logContext); // 创建并设置拉取缓冲区，使用提供的 logContext。
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED; // 设置隔离级别为 READ_UNCOMMITTED。
        this.time = time; // 设置时间工具。
        this.metrics = new Metrics(time); // 创建并设置度量收集器，使用提供的时间工具。
        this.interceptors = new ConsumerInterceptors<>(Collections.emptyList(), metrics); // 创建并设置消费者拦截器，初始为空列表，并关联度量收集器。
        this.metadata = metadata; // 设置消费者元数据。
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG); // 从配置中获取并设置重试退避时间。
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG); // 从配置中获取并设置请求超时时间。
        this.defaultApiTimeoutMs = Duration.ofMillis(config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)); // 从配置中获取默认 API 超时时间，并转换为 Duration 对象。
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer, metrics); // 创建并设置反序列化器，使用提供的键/值反序列化器和度量收集器。
        this.clientTelemetryReporter = Optional.empty(); // 设置客户端遥测报告器为空。

        ConsumerMetrics metricsRegistry = new ConsumerMetrics(CONSUMER_METRIC_GROUP_PREFIX); // 创建消费者度量指标注册表，使用预定义的度量组前缀。
        FetchMetricsManager fetchMetricsManager = new FetchMetricsManager(metrics, metricsRegistry.fetcherMetrics); // 创建拉取度量管理器，关联全局度量收集器和拉取相关的度量指标。
        this.fetchCollector = new FetchCollector<>(logContext, // 创建并设置拉取收集器。
                metadata, // 消费者元数据。
                subscriptions, // 订阅状态。
                new FetchConfig(config), // 拉取配置，从消费者配置中创建。
                deserializers, // 反序列化器。
                fetchMetricsManager, // 拉取度量管理器。
                time); // 时间工具。
        this.kafkaConsumerMetrics = new AsyncConsumerMetrics(metrics); // 创建并设置异步消费者度量指标对象，关联全局度量收集器。

        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig( // 创建消费者组再平衡配置。
            config, // 消费者配置。
            GroupRebalanceConfig.ProtocolType.CONSUMER // 指定协议类型为 CONSUMER。
        );

        this.groupMetadata.set(initializeGroupMetadata(config, groupRebalanceConfig)); // 初始化并设置消费者组元数据，使用消费者配置和再平衡配置。

        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>(); // 创建应用程序事件队列，用于应用线程和网络线程之间的通信。
        this.backgroundEventQueue = new LinkedBlockingQueue<>(); // 创建后台事件队列，用于网络线程向应用线程传递事件。
        this.backgroundEventHandler = new BackgroundEventHandler( // 创建并设置后台事件处理器。
            backgroundEventQueue, // 后台事件队列。
            time, // 时间工具。
            kafkaConsumerMetrics // Kafka 消费者度量指标。
        );
        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker( // 创建并设置再平衡监听器调用器。
            logContext, // 日志上下文。
            subscriptions, // 订阅状态。
            time, // 时间工具。
            new RebalanceCallbackMetricsManager(metrics) // 再平衡回调度量管理器，关联全局度量收集器。
        );
        ApiVersions apiVersions = new ApiVersions(); // 创建 API 版本对象，用于跟踪 Kafka broker 支持的 API 版本。
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> new NetworkClientDelegate( // 创建 NetworkClientDelegate 的 공급자(Supplier)。
            time, // 时间工具。
            config, // 消费者配置。
            logContext, // 日志上下文。
            client, // Kafka 客户端。
            metadata, // 消费者元数据。
            backgroundEventHandler, // 后台事件处理器。
            false, // 指示是否为 fetcher 线程创建，这里为 false。
            kafkaConsumerMetrics // Kafka 消费者度量指标。
        );
        this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors); // 创建并设置偏移量提交回调调用器，使用配置的拦截器。
        Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier( // 创建 RequestManagers 的 공급자(Supplier)。
            time, // 时间工具。
            logContext, // 日志上下文。
            backgroundEventHandler, // 后台事件处理器。
            metadata, // 消费者元数据。
            subscriptions, // 订阅状态。
            fetchBuffer, // 拉取缓冲区。
            config, // 消费者配置。
            groupRebalanceConfig, // 组再平衡配置。
            apiVersions, // API 版本。
            fetchMetricsManager, // 拉取度量管理器。
            networkClientDelegateSupplier, // NetworkClientDelegate 공급자。
            clientTelemetryReporter, // 客户端遥测报告器。
            metrics, // 度量收集器。
            offsetCommitCallbackInvoker, // 偏移量提交回调调用器。
            memberStateListener // 成员状态监听器 (注意: memberStateListener 在此构造函数中未显式初始化，可能依赖于默认值或后续设置)。
        );
        Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier( // 创建 ApplicationEventProcessor 的 공급자(Supplier)。
                logContext, // 日志上下文。
                metadata, // 消费者元数据。
                subscriptions, // 订阅状态。
                requestManagersSupplier // RequestManagers 공급자。
        );
        this.applicationEventHandler = new ApplicationEventHandler(logContext, // 创建并设置应用程序事件处理器。
                time, // 时间工具。
                applicationEventQueue, // 应用程序事件队列。
                new CompletableEventReaper(logContext), // 可完成事件收割者，用于清理已完成的事件。
                applicationEventProcessorSupplier, // ApplicationEventProcessor 공급자。
                networkClientDelegateSupplier, // NetworkClientDelegate 공급자。
                requestManagersSupplier, // RequestManagers 공급자。
                kafkaConsumerMetrics); // Kafka 消费者度量指标。
        this.backgroundEventProcessor = new BackgroundEventProcessor(); // 创建并设置后台事件处理器实例。
        this.backgroundEventReaper = new CompletableEventReaper(logContext); // 创建并设置后台事件收割者实例，用于清理后台事件队列中已完成的事件。
    }

    // 用于测试的辅助接口
    /**
     * {@link ApplicationEventHandler} 的工厂接口，主要用于测试目的。
     * 通过此工厂，可以在测试中注入自定义的或模拟的 {@link ApplicationEventHandler} 实现，
     * 以便更好地控制和验证消费者的行为，特别是与应用程序事件处理相关的逻辑。
     *
     * @implNote 此接口的设计遵循了工厂模式，旨在解耦 {@link AsyncKafkaConsumer} 与其依赖的
     *           {@link ApplicationEventHandler} 的具体实现。在生产代码中，通常会使用默认的
     *           实现，但在测试环境中，能够替换此实现对于隔离测试单元至关重要。
     */
    interface ApplicationEventHandlerFactory {

        /**
         * 构建并返回一个 {@link ApplicationEventHandler} 实例。
         *
         * @param logContext 日志上下文，用于记录事件处理器内部的操作。
         * @param time 时间工具，用于事件处理中的时间相关操作。
         * @param applicationEventQueue 应用程序事件队列，事件处理器将从此队列中拉取事件进行处理。
         * @param applicationEventReaper 应用程序事件收割者，用于清理已完成的应用程序事件。
         * @param applicationEventProcessorSupplier {@link ApplicationEventProcessor} 的 공급자(Supplier)，提供事件处理的核心逻辑单元。
         * @param networkClientDelegateSupplier {@link NetworkClientDelegate} 的 공급자(Supplier)，提供网络通信的代理。
         * @param requestManagersSupplier {@link RequestManagers} 的 공급자(Supplier)，提供请求管理器的集合。
         * @param asyncConsumerMetrics 异步消费者度量指标，用于记录与事件处理相关的性能指标。
         * @return 构建的 {@link ApplicationEventHandler} 实例。
         * @implNote 此构建方法接收创建 {@link ApplicationEventHandler} 所需的所有依赖项。
         *           在测试中，可以提供这些参数的模拟实现，以精确控制测试环境。
         */
        ApplicationEventHandler build(
            final LogContext logContext,
            final Time time,
            final BlockingQueue<ApplicationEvent> applicationEventQueue,
            final CompletableEventReaper applicationEventReaper,
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
            final Supplier<RequestManagers> requestManagersSupplier,
            final AsyncConsumerMetrics asyncConsumerMetrics
        );

    }

    // 用于测试的辅助接口
    /**
     * {@link CompletableEventReaper} 的工厂接口，主要用于测试目的。
     * 此工厂允许在测试中创建和注入自定义的或模拟的 {@link CompletableEventReaper} 实例，
     * 这对于测试那些依赖于事件完成和清理机制的组件非常有用。
     *
     * @implNote 与 {@link ApplicationEventHandlerFactory} 类似，此接口也遵循工厂模式，
     *           旨在提高代码的可测试性。通过替换 {@link CompletableEventReaper} 的实现，
     *           测试可以更精确地控制事件的生命周期和清理过程。
     */
    interface CompletableEventReaperFactory {

        /**
         * 构建并返回一个 {@link CompletableEventReaper} 实例。
         *
         * @param logContext 日志上下文，用于记录事件收割者内部的操作。
         * @return 构建的 {@link CompletableEventReaper} 实例。
         * @implNote 此方法接收创建 {@link CompletableEventReaper} 所需的日志上下文。
         *           在测试中，可以提供一个配置好的日志上下文，或者一个模拟的上下文，
         *           以便观察或控制日志输出。
         */
        CompletableEventReaper build(final LogContext logContext);

    }

    // 用于测试的辅助接口
    /**
     * {@link FetchCollector} 的工厂接口，主要用于测试目的。
     * 此工厂使得在测试中可以注入自定义的或模拟的 {@link FetchCollector} 实现，
     * 这对于测试依赖于消息拉取和处理逻辑的组件非常关键。
     *
     * @param <K> 消息键的类型
     * @param <V> 消息值的类型
     * @implNote 此接口同样遵循工厂模式，增强了 {@link AsyncKafkaConsumer} 的可测试性。
     *           通过提供不同的 {@link FetchCollector} 实现，测试可以模拟各种网络条件、
     *           消息格式或拉取行为，从而更全面地验证消费者的健壮性。
     */
    interface FetchCollectorFactory<K, V> {

        /**
         * 构建并返回一个 {@link FetchCollector} 实例。
         *
         * @param logContext 日志上下文，用于记录拉取收集器内部的操作。
         * @param metadata 消费者元数据，提供关于集群和主题的信息，辅助拉取决策。
         * @param subscriptions 订阅状态，指明消费者当前感兴趣的主题和分区。
         * @param fetchConfig 拉取配置，包含与消息拉取相关的参数，如最大字节数、最大等待时间等。
         * @param deserializers 键和值的反序列化器，用于将拉取到的原始字节数据转换为消息对象。
         * @param metricsManager 拉取度量管理器，用于收集和报告与消息拉取相关的性能指标。
         * @param time 时间工具，用于处理拉取过程中的超时和时间戳等。
         * @return 构建的 {@link FetchCollector} 实例。
         * @implNote 此构建方法汇集了创建 {@link FetchCollector} 所需的全部依赖。
         *           在测试场景下，可以灵活地提供这些参数的模拟对象，以模拟特定的拉取场景，
         *           例如，模拟网络延迟、特定错误响应或不同格式的消息数据。
         */
        FetchCollector<K, V> build(
            final LogContext logContext,
            final ConsumerMetadata metadata,
            final SubscriptionState subscriptions,
            final FetchConfig fetchConfig,
            final Deserializers<K, V> deserializers,
            final FetchMetricsManager metricsManager,
            final Time time
        );

    }

    // auxiliary interface for testing
    // 用于测试的辅助接口
    interface ConsumerMetadataFactory { // ConsumerMetadataFactory 接口定义，用于在测试场景下构建 ConsumerMetadata 对象。

        /**
         * 构建 ConsumerMetadata 对象。
         * 应用场景：在测试中，需要一个可控的方式来创建 ConsumerMetadata 实例，以便模拟不同的集群状态或配置。
         * 设计考虑：通过接口将 ConsumerMetadata 的创建逻辑解耦，方便测试时替换为 mock 实现。
         * @param config 消费者配置对象，包含消费者的各项配置信息。
         * @param subscriptions 订阅状态对象，维护消费者当前的订阅信息。
         * @param logContext 日志上下文，用于记录日志。
         * @param clusterResourceListeners 集群资源监听器列表，用于监听集群元数据的变化。
         * @return 构建的 ConsumerMetadata 对象。
         */
        ConsumerMetadata build(
            final ConsumerConfig config, // 消费者配置
            final SubscriptionState subscriptions, // 订阅状态
            final LogContext logContext, // 日志上下文
            final ClusterResourceListeners clusterResourceListeners // 集群资源监听器
        );

    }

    private Optional<ConsumerGroupMetadata> initializeGroupMetadata(final ConsumerConfig config,
                                                                    final GroupRebalanceConfig groupRebalanceConfig) {
        final Optional<ConsumerGroupMetadata> groupMetadata = initializeGroupMetadata(
            groupRebalanceConfig.groupId,
            groupRebalanceConfig.groupInstanceId
        );
        if (groupMetadata.isEmpty()) {
            config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
            config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        }
        return groupMetadata;
    }

    /**
     * 根据 groupId 和 groupInstanceId 初始化消费者组元数据。
     * 应用场景：当消费者加入一个消费组时，需要此元数据来标识其在组内的身份。
     * 设计考虑：校验 groupId 的有效性，如果无效则不创建元数据。
     * @param groupId 消费者组ID。
     * @param groupInstanceId 消费者实例ID（可选），用于静态成员资格。
     * @return 如果 groupId 有效，则返回包含 ConsumerGroupMetadata 的 Optional；否则返回空的 Optional。
     */
    private Optional<ConsumerGroupMetadata> initializeGroupMetadata(final String groupId, // 消费者组ID
                                                                    final Optional<String> groupInstanceId) { // 消费者实例ID（可选）
        // 检查 groupId 是否为 null
        if (groupId != null) {
            // 检查 groupId 是否为空字符串
            if (groupId.isEmpty()) {
                // 如果 groupId 为空，则抛出 InvalidGroupIdException 异常
                throw new InvalidGroupIdException("The configured " + ConsumerConfig.GROUP_ID_CONFIG
                    + " should not be an empty string or whitespace.");
            } else {
                // 如果 groupId 有效，则调用 initializeConsumerGroupMetadata 创建 ConsumerGroupMetadata 对象，并用 Optional 包装返回
                return Optional.of(initializeConsumerGroupMetadata(groupId, groupInstanceId));
            }
        }
        // 如果 groupId 为 null，则返回空的 Optional
        return Optional.empty();
    }
       

    /**
     * 创建 ConsumerGroupMetadata 实例。
     * 应用场景：在确定 groupId 有效后，实际创建 ConsumerGroupMetadata 对象。
     * 设计考虑：使用 Kafka 协议中定义的未知年代和成员ID作为初始值。
     * @param groupId 消费者组ID。
     * @param groupInstanceId 消费者实例ID（可选）。
     * @return 新创建的 ConsumerGroupMetadata 对象。
     */
    private ConsumerGroupMetadata initializeConsumerGroupMetadata(final String groupId, // 消费者组ID
                                                                  final Optional<String> groupInstanceId) { // 消费者实例ID（可选）
        // 创建并返回一个新的 ConsumerGroupMetadata 对象
        return new ConsumerGroupMetadata(
            groupId, // 组ID
            JoinGroupRequest.UNKNOWN_GENERATION_ID, // 初始年代ID设为未知
            JoinGroupRequest.UNKNOWN_MEMBER_ID, // 初始成员ID设为未知
            groupInstanceId // 组实例ID
        );
    }
    

    private void updateGroupMetadata(final Optional<Integer> memberEpoch, final String memberId) {
        memberEpoch.ifPresent(epoch -> groupMetadata.updateAndGet(
                oldGroupMetadataOptional -> oldGroupMetadataOptional.map(
                    oldGroupMetadata -> new ConsumerGroupMetadata(
                        oldGroupMetadata.groupId(),
                        memberEpoch.orElse(oldGroupMetadata.generationId()),
                        memberId,
                        oldGroupMetadata.groupInstanceId()
                    )
                )
            )
        );
    }

    /**
     * 设置消费者组分配的分区快照。
     * 应用场景：在消费者组完成再平衡后，协调者会将分配给该消费者的分区通知给它，此时需要更新这个快照。
     * 设计考虑：使用 AtomicReference 保证线程安全地更新快照，并使用 Collections.unmodifiableSet 使快照不可修改，防止外部意外修改。
     * @param partitions 分配给该消费者的主题分区集合。
     */
    void setGroupAssignmentSnapshot(final Set<TopicPartition> partitions) { // 分配的分区集合
        // 将传入的分区集合设置为不可修改的集合，并更新 groupAssignmentSnapshot
        groupAssignmentSnapshot.set(Collections.unmodifiableSet(partitions));
    }

    /**
     * 为订阅注册度量指标。
     * 应用场景：当订阅发生变化，可能需要注册新的度量指标来监控与新订阅相关的性能。
     * 设计考虑：只有当度量指标尚未存在时才进行注册，并通知客户端遥测报告器。
     * @param metric 要注册的 Kafka 度量指标。
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) { // 要注册的 Kafka 度量指标
        // 检查 metrics 集合中是否已包含该度量指标的名称
        if (!metrics().containsKey(metric.metricName())) {
            // 如果不包含，则通知 clientTelemetryReporter (如果存在) 度量指标发生变化
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        } else {
            // 如果已包含，则记录调试日志，说明跳过注册，因为现有消费者度量指标不能被覆盖
            log.debug("Skipping registration for metric {}. Existing consumer metrics cannot be overwritten.", metric.metricName());
        }
    }


    /**
     * 从订阅中取消注册度量指标。
     * 应用场景：当订阅发生变化，某些之前注册的度量指标可能不再需要，此时应取消注册。
     * 设计考虑：只有当度量指标尚未存在时才进行取消注册（逻辑上似乎应该是存在时才取消，这里可能是笔误或者特定场景下的逻辑），并通知客户端遥测报告器。
     * @param metric 要取消注册的 Kafka 度量指标。
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) { // 要取消注册的 Kafka 度量指标
        // 检查 metrics 集合中是否已包含该度量指标的名称
        if (!metrics().containsKey(metric.metricName())) {
            // 如果不包含（这里逻辑可能与预期相反，通常是存在才移除），则通知 clientTelemetryReporter (如果存在) 度量指标被移除
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        } else {
            // 如果已包含，则记录调试日志，说明跳过取消注册，因为现有消费者度量指标不能被移除
            log.debug("Skipping unregistration for metric {}. Existing consumer metrics cannot be removed.", metric.metricName());
        }
    }
    

    /**
     * 使用 {@link ApplicationEventHandler} 的 poll 实现。
     *  1. 轮询后台事件。如果存在拉取响应事件，则处理记录并返回。如果是其他类型的事件，则处理它。
     *  2. 如果需要，发送拉取请求。
     *  如果超时到期，则返回空的 ConsumerRecord。
     *
     * @param timeout poll 循环的超时时间
     * @return ConsumerRecord。如果超时到期，则可能为空。
     *
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果在调用此函数之前或期间调用线程被中断
     * @throws org.apache.kafka.common.errors.RecordTooLargeException 如果拉取的记录大于允许的最大大小
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误
     * @throws java.lang.IllegalStateException 如果消费者未订阅任何主题或手动分配任何分区以供消费，或发生意外错误
     * @throws org.apache.kafka.clients.consumer.OffsetOutOfRangeException 如果消费者的拉取位置超出范围并且未配置偏移量重置策略。
     * @throws org.apache.kafka.common.errors.TopicAuthorizationException 如果消费者无权从分区读取
     * @throws org.apache.kafka.common.errors.SerializationException 如果拉取的记录无法反序列化
     * @throws org.apache.kafka.common.errors.UnsupportedAssignorException 如果 `group.remote.assignor` 配置设置为代理上不可用的分配器。
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) { // poll 循环的超时时间
        // 根据传入的超时时间创建一个计时器
        Timer timer = time.timer(timeout);

        // 获取锁并确保消费者处于打开状态，这是线程安全和状态检查的关键步骤
        acquireAndEnsureOpen();
        try {
            // 记录 poll 操作开始的时间，用于度量指标
            kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            // 检查消费者是否有有效的订阅或用户分配的分区
            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                // 如果没有订阅或分配，则抛出 IllegalStateException，因为 poll 操作无法进行
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            // 进入主循环，直到计时器超时
            do {

                // Make sure to let the background thread know that we are still polling.
                // 确保通知后台线程我们仍在轮询。通过向 applicationEventHandler 添加 PollEvent 来实现。
                applicationEventHandler.add(new PollEvent(timer.currentTimeMs()));

                // We must not allow wake-ups between polling for fetches and returning the records.
                // If the polled fetches are not empty the consumed position has already been updated in the polling
                // of the fetches. A wakeup between returned fetches and returning records would lead to never
                // returning the records in the fetches. Thus, we trigger a possible wake-up before we poll fetches.
                // 在轮询拉取数据和返回记录之间，我们绝不能允许唤醒操作。
                // 如果轮询到的拉取数据不为空，那么在轮询拉取数据时，消费位置就已经更新了。
                // 在返回拉取数据和实际返回记录之间发生唤醒，将导致永远无法返回拉取数据中的记录。
                // 因此，在轮询拉取数据之前，我们会触发一个可能的唤醒。
                wakeupTrigger.maybeTriggerWakeup();

                // 如果需要，更新分配元数据。这可能涉及到与协调者的通信，以获取最新的分区分配信息。
                updateAssignmentMetadataIfNeeded(timer);
                // 轮询拉取数据。这是一个关键步骤，实际从 Kafka 获取消息。
                final Fetch<K, V> fetch = pollForFetches(timer);
                // 检查拉取到的数据是否不为空
                if (!fetch.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    // 在返回拉取到的记录之前，我们可以发送下一轮的拉取请求，
                    // 并避免阻塞等待它们的响应，以便在用户处理拉取到的记录时启用流水线操作。
                    //
                    // 注意：由于消费位置已经更新，我们绝不能允许在返回拉取到的记录之前触发唤醒或任何其他错误。
                    sendPrefetches(timer);

                    // 如果拉取到的记录集合为空（但 fetch 本身不为空，可能意味着元数据更新等情况）
                    if (fetch.records().isEmpty()) {
                        // 记录追踪日志，说明从 poll() 返回空记录，因为消费者的位置至少在一个主题分区上前进过
                        log.trace("Returning empty records from `poll()` "
                            + "since the consumer's position has advanced for at least one topic partition");
                    }

                    // 通过拦截器处理消费到的记录，并返回 ConsumerRecords 对象
                    return interceptors.onConsume(new ConsumerRecords<>(fetch.records(), fetch.nextOffsets()));
                }
                // We will wait for retryBackoffMs
                // 如果本次轮询没有获取到数据，并且计时器未超时，循环将继续。这里可能隐含了重试退避的逻辑（虽然代码中未直接显示）。
            } while (timer.notExpired()); // 当计时器未超时时，继续循环

            // 如果计时器超时仍未获取到数据，则返回空的 ConsumerRecords
            return ConsumerRecords.empty();
        } finally {
            // 记录 poll 操作结束的时间，用于度量指标
            kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            // 释放锁，确保在操作完成后释放资源
            release();
        }
    }

    /**
     * 为所有已订阅的主题和分区，提交上次 {@link #poll(Duration) poll()} 调用返回的偏移量。
     * <p>
     * 此方法会同步提交偏移量，意味着它会阻塞直到提交成功或发生超时。
     * 应用场景：当需要确保偏移量已成功提交，然后再继续处理时使用，例如在关闭消费者之前。
     * 设计考虑：同步提交提供了更强的持久性保证，但可能会影响应用程序的吞吐量，因为它会阻塞调用线程。
     */
    @Override
    public void commitSync() {
        // 调用带有超时参数的 commitSync 方法，使用默认的 API 超时时间
        commitSync(defaultApiTimeoutMs);
    }

    /**
     * 此方法向 EventHandler 发送一个提交事件并返回。
     * <p>
     * 这是一个异步提交偏移量的方法，它不会阻塞调用线程。提交结果（成功或失败）将通过回调函数（如果提供）或日志来通知。
     * 应用场景：当应用程序不需要立即知道提交结果，并且希望避免阻塞主处理流程时使用。
     * 设计考虑：异步提交可以提高应用程序的吞吐量，但需要通过回调或日志来处理提交失败的情况。
     */
    @Override
    public void commitAsync() {
        // 调用带有 null 回调的 commitAsync 方法，表示不关心提交结果的即时通知
        commitAsync(null);
    }

    /**
     * 异步提交消费者获取的最新偏移量，并允许注册一个回调函数来处理提交结果。
     * <p>
     * 应用场景：当需要异步提交偏移量，并在提交完成后执行特定逻辑（例如记录日志、更新状态）时使用。
     * 设计考虑：通过回调机制，应用程序可以灵活地处理提交成功或失败的情况，而不会阻塞主线程。
     * @param callback 用户提供的回调函数，在提交完成时调用。如果为 null，则提交结果将通过日志记录。
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        // 调用私有的 commitAsync 方法，传入空的偏移量 Optional 和用户提供的回调
        // 这表示提交当前消费者已消费的偏移量
        commitAsync(Optional.empty(), callback);
    }

    /**
     * 异步提交指定分区的特定偏移量，并允许注册一个回调函数来处理提交结果。
     * <p>
     * 应用场景：当需要精确控制提交哪些分区的哪些偏移量时使用，例如在进行手动分区管理或实现复杂的提交策略时。
     * 设计考虑：允许应用程序提交自定义的偏移量集合，提供了更大的灵活性。
     * @param offsets 一个映射，包含要提交的 TopicPartition 及其对应的 OffsetAndMetadata。OffsetAndMetadata 包含偏移量和可选的元数据。
     * @param callback 用户提供的回调函数，在提交完成时调用。如果为 null，则提交结果将通过日志记录。
     */
    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // 调用私有的 commitAsync 方法，传入包含指定偏移量的 Optional 和用户提供的回调
        commitAsync(Optional.of(offsets), callback);
    }

    /**
     * 异步提交偏移量的私有辅助方法。
     * <p>
     * 此方法是所有公共 `commitAsync` 方法的底层实现。它获取消费者锁，确保消费者处于打开状态，
     * 然后创建一个 {@link AsyncCommitEvent} 并将其传递给 {@link #commit(CommitEvent)} 方法进行处理。
     * 提交完成后，它会处理回调逻辑，包括调用拦截器和用户提供的回调。
     * 应用场景：内部用于处理所有异步提交请求。
     * 设计考虑：通过集中的私有方法处理异步提交逻辑，确保一致性和可维护性。使用 `CompletableFuture` 来处理异步结果和回调。
     * @param offsets 一个 Optional，可能包含要提交的特定偏移量映射。如果为空，则提交消费者当前位置的偏移量。
     * @param callback 用户提供的回调函数，在提交完成时调用。可以为 null。
     */
    private void commitAsync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, OffsetCommitCallback callback) {
        // 获取锁并确保消费者处于打开状态，这是执行任何操作的前提
        acquireAndEnsureOpen();
        try {
            // 创建一个异步提交事件，封装了要提交的偏移量信息
            AsyncCommitEvent asyncCommitEvent = new AsyncCommitEvent(offsets);
            // 调用 commit 方法处理提交事件，并获取一个 CompletableFuture 来跟踪提交结果
            // lastPendingAsyncCommit 用于跟踪最近一次异步提交操作，以便在关闭时可以等待其完成
            lastPendingAsyncCommit = commit(asyncCommitEvent).whenComplete((committedOffsets, throwable) -> {
                // 当提交操作完成时（无论成功还是失败），此回调被触发

                // 如果没有异常发生（即提交成功）
                if (throwable == null) {
                    // 将已提交的偏移量加入到拦截器调用队列中，以便后续执行消费者拦截器的 onCommit 方法
                    offsetCommitCallbackInvoker.enqueueInterceptorInvocation(committedOffsets);
                }

                // 如果用户没有提供回调函数
                if (callback == null) {
                    // 如果提交过程中发生了异常
                    if (throwable != null) {
                        // 记录错误日志，说明偏移量提交失败
                        log.error("Offset commit with offsets {} failed", committedOffsets, throwable);
                    }
                    // 没有回调，直接返回
                    return;
                }

                // 如果用户提供了回调函数，则将其和提交结果（或异常）加入到用户回调调用队列中
                offsetCommitCallbackInvoker.enqueueUserCallbackInvocation(callback, committedOffsets, (Exception) throwable);
            });
        } finally {
            // 释放锁，确保在操作完成后（即使发生异常）锁也能被释放
            release();
        }
    }

    /**
     * 处理提交事件的私有辅助方法，可以是同步或异步提交。
     * <p>
     * 此方法首先检查消费者组ID是否有效，然后执行任何挂起的回调。
     * 如果提交事件中指定的偏移量映射为空，则直接返回一个已完成的 Future。
     * 否则，将提交事件添加到应用程序事件处理器队列中，由后台线程处理实际的提交操作，并返回与该事件关联的 Future。
     * 应用场景：作为 `commitSync` 和 `commitAsync` 的核心逻辑，统一处理提交请求的入队。
     * 设计考虑：将实际的提交操作委托给事件处理器，实现了提交逻辑的解耦和异步化。
     * @param commitEvent 要处理的提交事件，可以是 {@link SyncCommitEvent} 或 {@link AsyncCommitEvent}。
     * @return 一个 {@link CompletableFuture}，表示提交操作的结果。对于异步提交，调用者可以注册回调；对于同步提交，调用者可以阻塞等待结果。
     */
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit(final CommitEvent commitEvent) {
        // 检查 group.id 是否有效，如果无效（例如，没有配置 group.id 但尝试提交偏移量），则可能抛出 InvalidGroupIdException
        maybeThrowInvalidGroupIdException();
        // 执行任何先前排队的偏移量提交回调（包括拦截器和用户回调）
        // 这确保了回调按照它们被排队的顺序执行
        offsetCommitCallbackInvoker.executeCallbacks();

        // 检查提交事件中是否包含偏移量信息，并且该偏移量映射是否为空
        if (commitEvent.offsets().isPresent() && commitEvent.offsets().get().isEmpty()) {
            // 如果要提交的偏移量映射为空，则认为没有实际的提交操作需要执行，直接返回一个已完成的 Future，值为 null
            return CompletableFuture.completedFuture(null);
        }

        // 将提交事件添加到应用程序事件处理器的队列中，由后台的 ConsumerNetworkThread 负责处理
        applicationEventHandler.add(commitEvent);
        // 返回与此提交事件关联的 CompletableFuture，调用者可以用它来跟踪提交的完成状态和结果
        return commitEvent.future();
    }

    /**
     * 为指定的分区设置下一个要消费的记录的偏移量。
     * <p>
     * 此方法会覆盖消费者当前的消费位置。如果消费者之前没有为该分区分配位置，则此方法会设置初始位置。
     * 如果为尚未分配给此消费者的分区调用此方法，则该分区的查找位置将在未来的分配中生效。
     * 应用场景：当需要从特定位置开始消费消息时，例如在处理失败后重试、跳过某些消息或从历史数据开始处理。
     * 设计考虑：`seek` 操作本身是异步的，它向事件处理器发送一个 {@link SeekUnvalidatedEvent} 事件。
     * 实际的查找操作将在后台线程中进行验证和应用。
     * @param partition 要查找的分区，不能为 null。
     * @param offset 要查找的偏移量，必须为非负数。
     * @throws IllegalArgumentException 如果偏移量为负数。
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        // 检查偏移量是否为负数，如果是，则抛出 IllegalArgumentException
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 记录日志，说明正在为指定分区查找指定的偏移量
            log.info("Seeking to offset {} for partition {}", offset, partition);
            // 创建一个 SeekUnvalidatedEvent 事件，表示一个未经校验的查找请求
            // 该事件包含了超时时间、目标分区、目标偏移量以及空的 leader epoch (因为此 seek 方法不提供 epoch)
            SeekUnvalidatedEvent seekUnvalidatedEventEvent = new SeekUnvalidatedEvent(
                defaultApiTimeoutDeadlineMs(), // 查找操作的截止时间
                partition,                     // 目标分区
                offset,                        // 目标偏移量
                Optional.empty()               // leader epoch，此处为空
            );
            // 将查找事件添加到应用程序事件处理器，并等待其处理完成（addAndGet 会阻塞直到事件被处理）
            // 注意：这里的 addAndGet 实际上是将事件加入队列，并返回事件的 Future，然后调用 Future.get() 等待完成。
            // 这意味着 seek 操作在返回前，查找请求已经被后台线程接受并开始处理（或已处理完毕）。
            applicationEventHandler.addAndGet(seekUnvalidatedEventEvent);
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 为指定的分区设置下一个要消费的记录的偏移量，使用 {@link OffsetAndMetadata} 对象。
     * <p>
     * {@link OffsetAndMetadata} 允许除了偏移量之外，还指定 leader epoch 和自定义元数据。
     * leader epoch 用于防止由于 leader 变更导致的日志截断而读取到陈旧数据（“幽灵读取”）。
     * 应用场景：与 {@link #seek(TopicPartition, long)} 类似，但提供了更精细的控制，特别是当需要利用 leader epoch 来确保数据一致性时。
     * 设计考虑：同样是异步操作，通过发送 {@link SeekUnvalidatedEvent} 实现。
     * @param partition 要查找的分区，不能为 null。
     * @param offsetAndMetadata 包含偏移量、可选的 leader epoch 和元数据的对象，不能为 null。
     * @throws IllegalArgumentException 如果 {@link OffsetAndMetadata#offset()} 返回负数。
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        // 从 OffsetAndMetadata 对象中获取偏移量
        long offset = offsetAndMetadata.offset();
        // 检查偏移量是否为负数
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查 OffsetAndMetadata 是否包含 leader epoch 信息
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                // 如果包含 leader epoch，则记录包含 epoch 的查找信息
                log.info("Seeking to offset {} for partition {} with epoch {}",
                    offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                // 如果不包含 leader epoch，则记录不包含 epoch 的查找信息
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }

            // 创建并发送 SeekUnvalidatedEvent 事件
            // 此事件包含了超时时间、目标分区、目标偏移量以及从 OffsetAndMetadata 中获取的 leader epoch
            applicationEventHandler.addAndGet(new SeekUnvalidatedEvent(
                defaultApiTimeoutDeadlineMs(),      // 查找操作的截止时间
                partition,                          // 目标分区
                offsetAndMetadata.offset(),         // 目标偏移量
                offsetAndMetadata.leaderEpoch()     // leader epoch
            ));
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 将给定分区的偏移量重置到最早的可用偏移量。
     * 此方法会异步发送重置偏移量的请求。
     * 应用场景：当需要从最早的消息开始重新处理某个或某些分区的数据时使用。
     * 设计考虑：通过委托给私有的 seek 方法，并指定 EARLIEST策略，简化了API。
     * @param partitions 需要重置偏移量的分区集合，不能为空。
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        // 调用私有的 seek 方法，使用 EARLIEST 策略将偏移量重置到最早
        seek(partitions, AutoOffsetResetStrategy.EARLIEST);
    }

    /**
     * 将给定分区的偏移量重置到最新的可用偏移量。
     * 此方法会异步发送重置偏移量的请求。
     * 应用场景：当需要从最新的消息开始处理某个或某些分区的数据，跳过历史消息时使用。
     * 设计考虑：通过委托给私有的 seek 方法，并指定 LATEST 策略，简化了API。
     * @param partitions 需要重置偏移量的分区集合，不能为空。
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        // 调用私有的 seek 方法，使用 LATEST 策略将偏移量重置到最新
        seek(partitions, AutoOffsetResetStrategy.LATEST);
    }

    /**
     * 私有辅助方法，用于将指定分区的偏移量重置到由 {@code offsetResetStrategy} 定义的位置。
     * 这是 {@code seekToBeginning} 和 {@code seekToEnd} 的核心实现。
     * 实现细节：
     * 1. 校验分区集合是否为 null。
     * 2. 获取锁并确保消费者处于打开状态。
     * 3. 创建一个 {@link ResetOffsetEvent} 并将其添加到应用事件处理器队列中，由后台线程异步处理。
     * 4. 释放锁。
     * @param partitions 需要重置偏移量的分区集合。
     * @param offsetResetStrategy 偏移量重置策略 (EARLIEST 或 LATEST)。
     */
    private void seek(Collection<TopicPartition> partitions, AutoOffsetResetStrategy offsetResetStrategy) {
        // 检查分区集合是否为 null，如果是则抛出 IllegalArgumentException 异常
        if (partitions == null)
            throw new IllegalArgumentException("分区集合不能为空");

        // 获取锁并确保消费者处于打开状态，这是所有公共API的标准操作，保证线程安全和消费者状态的有效性
        acquireAndEnsureOpen();
        try {
            // 创建一个 ResetOffsetEvent 事件，包含要重置的分区、重置策略和默认的API超时截止时间
            // 将此事件添加到 applicationEventHandler 队列中，由后台线程异步处理偏移量重置请求
            applicationEventHandler.addAndGet(new ResetOffsetEvent(
                partitions, // 需要重置偏移量的分区
                offsetResetStrategy, // 偏移量重置策略 (最早或最新)
                defaultApiTimeoutDeadlineMs()) // 默认的API超时截止时间
            );
        } finally {
            // 释放锁，确保在操作完成后或发生异常时都能释放锁
            release();
        }
    }

    /**
     * 获取指定分区消费者将要读取的下一条记录的偏移量。
     * 此方法使用默认的API超时时间。
     * 应用场景：查询特定分区的当前消费位置。
     * 设计考虑：这是一个阻塞操作，直到获取到位置或超时。它委托给带有超时参数的重载方法。
     * @param partition 需要查询位置的分区，不能为空。
     * @return 该分区的下一个偏移量。
     * @throws IllegalStateException 如果该分区未分配给此消费者。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能确定位置。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public long position(TopicPartition partition) {
        // 调用带有超时参数的 position 方法，使用默认的 API 超时时间
        return position(partition, defaultApiTimeoutMs);
    }

    /**
     * 获取指定分区消费者将要读取的下一条记录的偏移量，并带有指定的超时时间。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 检查指定分区是否已分配给此消费者，如果未分配则抛出 {@link IllegalStateException}。
     * 3. 创建一个计时器，并在循环中尝试获取有效位置：
     *    a. 从 {@code subscriptions} 获取分区的有效位置。
     *    b. 如果找到有效位置，则返回其偏移量。
     *    c. 如果未找到，则调用 {@code updateFetchPositions} 尝试更新获取位置（这可能涉及网络请求）。
     *    d. 更新计时器并检查是否需要触发唤醒。
     * 4. 如果循环结束（超时），则抛出 {@link TimeoutException}。
     * 5. 释放锁。
     * 应用场景：在允许一定等待时间的情况下，查询特定分区的当前消费位置。
     * 设计考虑：这是一个阻塞操作。循环和 {@code updateFetchPositions} 的调用是为了处理位置信息可能需要从服务器获取或等待后台操作完成的情况。
     * @param partition 需要查询位置的分区，不能为空。
     * @param timeout 等待位置可用的最长时间。
     * @return 该分区的下一个偏移量。
     * @throws IllegalStateException 如果该分区未分配给此消费者。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能确定位置。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public long position(TopicPartition partition, Duration timeout) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查该分区是否已分配给此消费者
            if (!subscriptions.isAssigned(partition))
                // 如果未分配，则抛出 IllegalStateException，因为只能检查已分配分区的位置
                throw new IllegalStateException("只能检查分配给此消费者的分区的位置。");

            // 创建一个计时器，用于控制操作的超时
            Timer timer = time.timer(timeout);
            // 循环直到计时器过期
            do {
                // 从订阅状态中获取分区的有效位置信息
                SubscriptionState.FetchPosition position = subscriptions.validPosition(partition);
                // 如果获取到有效的位置信息
                if (position != null)
                    // 返回该位置的偏移量
                    return position.offset;

                // 如果没有获取到有效位置，则尝试更新获取位置（这可能涉及网络请求或等待后台操作）
                updateFetchPositions(timer);
                // 更新计时器状态
                timer.update();
                // 检查是否需要触发唤醒操作（例如，如果另一个线程调用了 wakeup()）
                wakeupTrigger.maybeTriggerWakeup();
            } while (timer.notExpired()); // 当计时器未过期时继续循环

            // 如果循环结束仍未获取到位置，则表示超时
            throw new TimeoutException("在 " + timeout.toMillis() + " 毫秒的超时时间到期之前，未能确定分区 " + partition + " 的位置");
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 获取给定分区集合的最后提交偏移量。
     * 此方法使用默认的API超时时间。
     * 应用场景：查询消费者组为特定分区提交的最新偏移量，常用于监控或恢复场景。
     * 设计考虑：这是一个阻塞操作，直到获取到提交的偏移量或超时。它委托给带有超时参数的重载方法。
     * @param partitions 需要查询已提交偏移量的分区集合，不能为空。
     * @return 分区到其已提交偏移量和元数据的映射。如果某个分区没有已提交的偏移量，则返回的映射中将不包含该分区。
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException 如果消费者配置中没有提供有效的 {@code group.id}。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能确定提交的偏移量。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        // 调用带有超时参数的 committed 方法，使用默认的 API 超时时间
        return committed(partitions, defaultApiTimeoutMs);
    }

    /**
     * 获取给定分区集合的最后提交偏移量，并带有指定的超时时间。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 记录操作开始时间，用于度量。
     * 3. 检查是否配置了有效的 {@code group.id}，如果没有则抛出 {@link InvalidGroupIdException}。
     * 4. 如果分区集合为空，则直接返回空映射。
     * 5. 创建一个 {@link FetchCommittedOffsetsEvent} 并将其添加到应用事件处理器队列中。
     * 6. 将事件的 Future 设置为唤醒触发器的活动任务，以便在等待期间可以被唤醒。
     * 7. 阻塞等待事件处理完成并获取结果。
     * 8. 如果发生超时，则抛出自定义的 {@link TimeoutException}，并提示用户调整配置。
     * 9. 清除唤醒触发器的活动任务。
     * 10. 记录提交操作的耗时。
     * 11. 释放锁。
     * 应用场景：在允许一定等待时间的情况下，查询消费者组为特定分区提交的最新偏移量。
     * 设计考虑：这是一个阻塞操作。通过应用事件处理器将实际的偏移量获取操作异步化到后台线程，同时主线程等待结果。
     * @param partitions 需要查询已提交偏移量的分区集合，不能为空。
     * @param timeout 等待已提交偏移量可用的最长时间。
     * @return 分区到其已提交偏移量和元数据的映射。如果某个分区没有已提交的偏移量，则返回的映射中将不包含该分区。
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException 如果消费者配置中没有提供有效的 {@code group.id}。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能确定提交的偏移量。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        // 记录操作开始的纳秒时间，用于后续度量操作耗时
        long start = time.nanoseconds();
        try {
            // 检查 group.id 是否有效，如果无效则抛出异常
            maybeThrowInvalidGroupIdException();
            // 如果请求的分区集合为空
            if (partitions.isEmpty()) {
                // 直接返回一个空的映射
                return Collections.emptyMap();
            }

            // 创建一个 FetchCommittedOffsetsEvent 事件，用于获取已提交的偏移量
            // 参数包括：要查询的分区集合，以及根据超时时间计算出的截止时间点
            final FetchCommittedOffsetsEvent event = new FetchCommittedOffsetsEvent(
                partitions, // 需要查询的分区
                calculateDeadlineMs(time, timeout)); // 计算出的截止时间
            // 将此事件的 future 设置为唤醒触发器的当前活动任务，以便在等待时可以被唤醒
            wakeupTrigger.setActiveTask(event.future());
            try {
                // 将事件添加到应用事件处理器并等待其完成，返回获取到的已提交偏移量映射
                return applicationEventHandler.addAndGet(event);
            } catch (TimeoutException e) {
                // 如果捕获到超时异常，则抛出更具体的超时异常信息
                throw new TimeoutException("在 " + timeout.toMillis() + " 毫秒的超时时间到期之前，未能确定分区 " + partitions + " 的最后提交偏移量。请尝试将 " +
                    ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " 调大以放宽阈值。");
            } finally {
                // 清除唤醒触发器中的活动任务
                wakeupTrigger.clearTask();
            }
        } finally {
            // 记录 committed 操作的耗时
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            // 释放锁
            release();
        }
    }

    /**
     * 私有辅助方法，检查消费者是否配置了有效的 {@code group.id}。
     * 如果 {@code group.id} 未配置或无效（例如，对于简单消费者），则抛出 {@link InvalidGroupIdException}。
     * 实现细节：检查 {@code groupMetadata} 是否为空。{@code groupMetadata} 在消费者初始化时根据 {@code group.id} 配置进行设置。
     * 应用场景：在执行需要消费者组功能（如偏移量提交、组成员管理）的操作之前调用此方法进行校验。
     * 设计考虑：将此校验逻辑封装成一个独立方法，便于在多个地方复用。
     */
    private void maybeThrowInvalidGroupIdException() {
        // 获取当前的消费者组元数据
        // 如果 groupMetadata 为空 (Optional.empty())，表示没有配置有效的 group.id
        if (groupMetadata.get().isEmpty()) {
            // 抛出 InvalidGroupIdException 异常，提示用户需要配置有效的 group.id
            throw new InvalidGroupIdException("要使用组管理或偏移量提交 API，您必须在消费者配置中提供一个有效的 " + ConsumerConfig.GROUP_ID_CONFIG + "。");
        }
    }

    /**
     * 获取此消费者的所有度量指标。
     * 应用场景：监控消费者的性能和状态。
     * 设计考虑：返回的是一个不可修改的映射，以防止外部修改内部度量状态。
     * @return 度量指标名称到度量指标对象的映射。
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // 返回一个不可修改的度量指标映射视图
        return Collections.unmodifiableMap(metrics.metrics());
    }

    /**
     * 获取指定主题的分区元数据。
     * 此方法使用默认的API超时时间。
     * 应用场景：查询主题的分区信息，例如分区数量、leader副本等。
     * 设计考虑：这是一个阻塞操作，直到获取到分区信息或超时。它委托给带有超时参数的重载方法。
     * @param topic 需要查询分区信息的主题，不能为空。
     * @return 该主题的分区信息列表。如果主题不存在，则返回空列表。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能获取元数据。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        // 调用带有超时参数的 partitionsFor 方法，使用默认的 API 超时时间
        return partitionsFor(topic, defaultApiTimeoutMs);
    }

    /**
     * 获取指定主题的分区元数据，并带有指定的超时时间。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 尝试从本地元数据缓存中获取主题的分区信息。
     * 3. 如果本地缓存中存在且不为空，则直接返回。
     * 4. 如果超时时间为0且本地缓存未命中，则立即抛出 {@link TimeoutException}。
     * 5. 创建一个 {@link TopicMetadataEvent} 并将其添加到应用事件处理器队列中，以异步获取主题元数据。
     * 6. 将事件的 Future 设置为唤醒触发器的活动任务。
     * 7. 阻塞等待事件处理完成并获取结果。
     * 8. 从结果映射中获取指定主题的分区信息，如果不存在则返回空列表。
     * 9. 清除唤醒触发器的活动任务。
     * 10. 释放锁。
     * 应用场景：在允许一定等待时间的情况下，查询主题的分区信息。
     * 设计考虑：这是一个阻塞操作。首先尝试从本地缓存获取，失败则通过应用事件处理器异步从服务器获取。
     * @param topic 需要查询分区信息的主题，不能为空。
     * @param timeout 等待元数据可用的最长时间。
     * @return 该主题的分区信息列表。如果主题不存在，则返回空列表。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能获取元数据。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 从元数据对象中获取当前的集群元数据快照
            Cluster cluster = this.metadata.fetch();
            // 尝试从集群元数据中获取指定主题的分区信息
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            // 如果获取到的分区信息不为空 (即本地缓存命中)
            if (!parts.isEmpty())
                // 直接返回这些分区信息
                return parts;

            // 如果超时时间为0 (即不允许等待)
            if (timeout.toMillis() == 0L) {
                // 并且本地缓存未命中，则直接抛出超时异常
                throw new TimeoutException();
            }

            // 创建一个 TopicMetadataEvent 事件，用于请求特定主题的元数据
            // 参数包括：主题名称，以及根据超时时间计算出的截止时间点
            final TopicMetadataEvent topicMetadataEvent = new TopicMetadataEvent(topic, calculateDeadlineMs(time, timeout));
            // 将此事件的 future 设置为唤醒触发器的当前活动任务
            wakeupTrigger.setActiveTask(topicMetadataEvent.future());
            try {
                // 将事件添加到应用事件处理器并等待其完成，返回包含主题元数据的映射
                Map<String, List<PartitionInfo>> topicMetadata =
                        applicationEventHandler.addAndGet(topicMetadataEvent);

                // 从返回的映射中获取指定主题的分区信息，如果主题不存在，则返回一个空列表
                return topicMetadata.getOrDefault(topic, Collections.emptyList());
            } finally {
                // 清除唤醒触发器中的活动任务
                wakeupTrigger.clearTask();
            }
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 获取集群中所有主题的元数据。
     * 此方法使用默认的API超时时间。
     * 应用场景：查询集群中存在的所有主题及其分区信息。
     * 设计考虑：这是一个阻塞操作，直到获取到主题列表或超时。它委托给带有超时参数的重载方法。
     * @return 主题名称到该主题分区信息列表的映射。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能获取元数据。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        // 调用带有超时参数的 listTopics 方法，使用默认的 API 超时时间
        return listTopics(defaultApiTimeoutMs);
    }

    /**
     * 获取集群中所有主题的元数据，并带有指定的超时时间。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 如果超时时间为0，则立即抛出 {@link TimeoutException} (因为获取所有主题元数据通常需要网络请求)。
     * 3. 创建一个 {@link AllTopicsMetadataEvent} 并将其添加到应用事件处理器队列中，以异步获取所有主题的元数据。
     * 4. 将事件的 Future 设置为唤醒触发器的活动任务。
     * 5. 阻塞等待事件处理完成并获取结果。
     * 6. 清除唤醒触发器的活动任务。
     * 7. 释放锁。
     * 应用场景：在允许一定等待时间的情况下，查询集群中所有主题的元数据。
     * 设计考虑：这是一个阻塞操作。通过应用事件处理器将实际的元数据获取操作异步化到后台线程。
     * @param timeout 等待元数据可用的最长时间。
     * @return 主题名称到该主题分区信息列表的映射。
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在超时时间内未能获取元数据。
     * @throws org.apache.kafka.common.errors.InterruptException 如果线程在操作期间被中断。
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 如果超时时间为0 (即不允许等待)
            if (timeout.toMillis() == 0L) {
                // 直接抛出超时异常，因为获取所有主题元数据通常需要网络请求
                throw new TimeoutException();
            }

            // 创建一个 AllTopicsMetadataEvent 事件，用于请求所有主题的元数据
            // 参数是根据超时时间计算出的截止时间点
            final AllTopicsMetadataEvent topicMetadataEvent = new AllTopicsMetadataEvent(calculateDeadlineMs(time, timeout));
            // 将此事件的 future 设置为唤醒触发器的当前活动任务
            wakeupTrigger.setActiveTask(topicMetadataEvent.future());
            try {
                // 将事件添加到应用事件处理器并等待其完成，返回包含所有主题元数据的映射
                return applicationEventHandler.addAndGet(topicMetadataEvent);
            } finally {
                // 清除唤醒触发器中的活动任务
                wakeupTrigger.clearTask();
            }
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 获取当前被暂停消费的分区集合。
     * 应用场景：查询哪些分区当前处于暂停状态，不进行消息拉取。
     * 设计考虑：直接从 {@code subscriptions} 状态中获取，并返回一个不可修改的集合以保证线程安全和状态的只读性。
     * @return 当前暂停的分区集合。如果没有任何分区被暂停，则返回空集合。
     */
    @Override
    public Set<TopicPartition> paused() {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 从订阅状态对象中获取当前所有被暂停的分区集合
            // 并返回该集合的一个不可修改的视图，以防止外部直接修改内部状态
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 暂停消费指定分区集合中的消息。
     * <p>
     * 此方法会向应用事件处理器提交一个 {@link PausePartitionsEvent}，
     * 该事件将在后台处理，以暂停对指定分区的消息拉取。
     * <p>
     * 应用场景：
     * 当消费者暂时不需要处理某些分区的消息时，例如，下游系统出现故障或需要进行维护，
     * 可以调用此方法暂停这些分区的消费，以避免消息积压或处理错误。
     * <p>
     * 实现细节：
     * 1. 获取锁并确保消费者未关闭。
     * 2. 检查输入的分区集合是否为 null，如果是则抛出 {@link NullPointerException}。
     * 3. 如果分区集合不为空，则创建一个 {@link PausePartitionsEvent} 并通过 {@code applicationEventHandler.addAndGet}
     *    异步提交该事件。该事件包含了要暂停的分区和默认的 API 超时时间。
     * 4. 无论成功与否，最终都会释放锁。
     * <p>
     * 设计考虑：
     * - 异步处理：暂停操作通过事件提交给后台线程处理，避免阻塞调用线程。
     * - 参数校验：对输入参数进行非空校验，确保操作的有效性。
     * - 资源管理：使用 try-finally 块确保锁的正确释放。
     *
     * @param partitions 需要暂停消费的分区集合，不能为 null。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        // 获取锁并确保消费者实例处于打开状态，如果已关闭则抛出异常
        acquireAndEnsureOpen();
        try {
            // 检查传入的分区集合是否为 null，如果是，则抛出 NullPointerException，并附带错误消息
            Objects.requireNonNull(partitions, "The partitions to pause must be nonnull");

            // 如果分区集合不为空
            if (!partitions.isEmpty())
                // 向应用事件处理器添加并执行一个 PausePartitionsEvent 事件
                // 这个事件会指示后台线程暂停对指定分区的消息获取
                // defaultApiTimeoutDeadlineMs() 用于设置此操作的超时截止时间
                applicationEventHandler.addAndGet(new PausePartitionsEvent(partitions, defaultApiTimeoutDeadlineMs()));
        } finally {
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * 恢复消费指定分区集合中的消息。
     * <p>
     * 此方法会向应用事件处理器提交一个 {@link ResumePartitionsEvent}，
     * 该事件将在后台处理，以恢复对指定分区的消息拉取。
     * <p>
     * 应用场景：
     * 当之前被暂停的分区可以重新开始处理消息时，例如，下游系统恢复正常或维护完成，
     * 可以调用此方法恢复这些分区的消费。
     * <p>
     * 实现细节：
     * 1. 获取锁并确保消费者未关闭。
     * 2. 检查输入的分区集合是否为 null，如果是则抛出 {@link NullPointerException}。
     * 3. 如果分区集合不为空，则创建一个 {@link ResumePartitionsEvent} 并通过 {@code applicationEventHandler.addAndGet}
     *    异步提交该事件。该事件包含了要恢复的分区和默认的 API 超时时间。
     * 4. 无论成功与否，最终都会释放锁。
     * <p>
     * 设计考虑：
     * - 异步处理：恢复操作通过事件提交给后台线程处理，避免阻塞调用线程。
     * - 参数校验：对输入参数进行非空校验，确保操作的有效性。
     * - 资源管理：使用 try-finally 块确保锁的正确释放。
     *
     * @param partitions 需要恢复消费的分区集合，不能为 null。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        // 获取锁并确保消费者实例处于打开状态，如果已关闭则抛出异常
        acquireAndEnsureOpen();
        try {
            // 检查传入的分区集合是否为 null，如果是，则抛出 NullPointerException，并附带错误消息
            Objects.requireNonNull(partitions, "The partitions to resume must be nonnull");

            // 如果分区集合不为空
            if (!partitions.isEmpty())
                // 向应用事件处理器添加并执行一个 ResumePartitionsEvent 事件
                // 这个事件会指示后台线程恢复对指定分区的消息获取
                // defaultApiTimeoutDeadlineMs() 用于设置此操作的超时截止时间
                applicationEventHandler.addAndGet(new ResumePartitionsEvent(partitions, defaultApiTimeoutDeadlineMs()));
        } finally {
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * 根据给定的时间戳查找每个分区的偏移量和时间戳。
     * <p>
     * 此方法是 {@link #offsetsForTimes(Map, Duration)} 的重载版本，使用默认的 API 超时时间。
     * <p>
     * 应用场景：
     * - 当需要从某个特定时间点开始消费消息时，可以使用此方法找到对应时间的偏移量。
     * - 用于数据回溯或定位特定时间范围内的消息。
     * <p>
     * 实现细节：
     * 调用 {@link #offsetsForTimes(Map, Duration)} 方法，并传入默认的 API 超时时间 {@code defaultApiTimeoutMs}。
     * <p>
     * 设计考虑：
     * - 便捷性：提供一个使用默认超时的版本，简化常见场景下的调用。
     *
     * @param timestampsToSearch 一个映射，键是 {@link TopicPartition}，值是对应分区要查找的时间戳 (毫秒)。
     *                           时间戳应该是自 epoch 以来的毫秒数。不允许负数时间戳。
     * @return 一个映射，键是 {@link TopicPartition}，值是 {@link OffsetAndTimestamp} 对象，
     *         包含了对应分区在给定时间戳之后的第一条消息的偏移量和时间戳。
     *         如果某个分区在给定时间戳之后没有消息，或者时间戳超出了分区的日志范围，则对应的值可能为 null。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code timestampsToSearch} 为 null。
     * @throws IllegalArgumentException 如果任何一个分区的时间戳为负数。
     * @throws TimeoutException 如果在配置的超时时间内未能获取到偏移量信息。
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        // 调用另一个 offsetsForTimes 重载方法，使用默认的 API 超时时间
        return offsetsForTimes(timestampsToSearch, defaultApiTimeoutMs);
    }

    /**
     * 根据给定的时间戳查找每个分区的偏移量和时间戳，并指定超时时间。
     * <p>
     * 此方法会向应用事件处理器提交一个 {@link ListOffsetsEvent}，
     * 该事件将在后台处理，以从 Kafka 服务器获取指定时间戳对应的偏移量信息。
     * <p>
     * 应用场景：
     * - 当需要从某个特定时间点开始消费消息时，可以使用此方法找到对应时间的偏移量。
     * - 用于数据回溯或定位特定时间范围内的消息，并允许控制操作的超时。
     * <p>
     * 实现细节：
     * 1. 获取锁并确保消费者未关闭。
     * 2. 校验 {@code timestampsToSearch} 参数：
     *    - 集合本身不能为 null。
     *    - 集合中的每个时间戳都不能为负数，否则抛出 {@link IllegalArgumentException}。
     * 3. 如果 {@code timestampsToSearch} 为空，则直接返回一个空映射。
     * 4. 创建一个 {@link ListOffsetsEvent}，包含要查询的时间戳、计算得到的截止时间以及一个布尔标志 (true 表示需要时间戳)。
     * 5. 如果超时时间为 0：
     *    - 将事件添加到应用事件处理器 (非阻塞)。
     *    - 立即返回事件的空结果集 (通常是一个空映射或包含 null 值的映射)。
     * 6. 如果超时时间大于 0：
     *    - 通过 {@code applicationEventHandler.addAndGet} 异步提交事件并等待结果。
     *    - 将返回的内部 {@link OffsetAndTimestampInternal} 映射转换为外部的 {@link OffsetAndTimestamp} 映射。
     *    - 如果在超时时间内未能获取结果，则捕获 {@link TimeoutException} 并重新抛出，附带更详细的错误信息。
     * 7. 无论成功与否，最终都会释放锁。
     * <p>
     * 设计考虑：
     * - 异步处理：通过事件提交给后台线程处理，避免阻塞调用线程，除非明确等待结果。
     * - 参数校验：对输入参数进行严格校验，确保操作的有效性和一致性。
     * - 超时控制：允许用户指定操作的超时时间，避免无限期等待。
     * - 兼容性：保留了与当前消费者实现相同的参数校验错误，以避免 API 级别的更改。
     * - 结果转换：将内部数据结构转换为 API 暴露的数据结构。
     * - 资源管理：使用 try-finally 块确保锁的正确释放。
     *
     * @param timestampsToSearch 一个映射，键是 {@link TopicPartition}，值是对应分区要查找的时间戳 (毫秒)。
     *                           时间戳应该是自 epoch 以来的毫秒数。不允许负数时间戳。
     * @param timeout 操作的超时时间。
     * @return 一个映射，键是 {@link TopicPartition}，值是 {@link OffsetAndTimestamp} 对象，
     *         包含了对应分区在给定时间戳之后的第一条消息的偏移量和时间戳。
     *         如果某个分区在给定时间戳之后没有消息，或者时间戳超出了分区的日志范围，则对应的值可能为 null。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code timestampsToSearch} 为 null。
     * @throws IllegalArgumentException 如果任何一个分区的时间戳为负数。
     * @throws TimeoutException 如果在指定的超时时间内未能获取到偏移量信息。
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        // 获取锁并确保消费者实例处于打开状态
        acquireAndEnsureOpen();
        try {
            // 保持与当前消费者实现相同的参数验证错误，以避免API级别的更改。
            // 检查 timestampsToSearch 是否为 null，如果是，则抛出 NullPointerException
            requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");
            // 遍历 timestampsToSearch 映射中的每个条目
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // 此处排除最早和最新的偏移量，以确保返回的 OffsetAndTimestamp 中的时间戳始终为正。
                // 如果条目的时间戳值小于0
                if (entry.getValue() < 0)
                    // 抛出 IllegalArgumentException，指示目标时间不能为负
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
            }

            // 如果 timestampsToSearch 为空
            if (timestampsToSearch.isEmpty()) {
                // 返回一个空的、不可修改的映射
                return Collections.emptyMap();
            }
            // 创建一个 ListOffsetsEvent 对象，用于请求偏移量
            ListOffsetsEvent listOffsetsEvent = new ListOffsetsEvent(
                    timestampsToSearch, // 要搜索的时间戳映射
                    calculateDeadlineMs(time, timeout), // 计算操作的截止时间
                    true); // true 表示需要返回时间戳信息

            // 如果超时时间为零，则立即返回空结果；否则尝试获取结果，如果无法及时完成则抛出超时异常。
            // 如果超时时间转换为毫秒后等于0
            if (timeout.toMillis() == 0L) {
                // 将 listOffsetsEvent 添加到应用事件处理器（非阻塞）
                applicationEventHandler.add(listOffsetsEvent);
                // 返回 listOffsetsEvent 的空结果集
                return listOffsetsEvent.emptyResults();
            }

            try {
                // 向应用事件处理器添加 listOffsetsEvent 并等待其完成，获取结果
                Map<TopicPartition, OffsetAndTimestampInternal> offsets = applicationEventHandler.addAndGet(listOffsetsEvent);
                // 创建一个新的 HashMap 用于存储最终结果，大小与获取到的偏移量映射大小相同
                Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>(offsets.size());
                // 遍历获取到的偏移量映射
                offsets.forEach((k, v) -> results.put(k, v != null ? v.buildOffsetAndTimestamp() : null));
                // 返回结果映射
                return results;
            } catch (TimeoutException e) {
                // 如果捕获到 TimeoutException
                // 抛出新的 TimeoutException，并附带更详细的错误消息
                throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
            }
        } finally {
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * 获取指定分区集合的最早可用偏移量。
     * <p>
     * 此方法是 {@link #beginningOffsets(Collection, Duration)} 的重载版本，使用默认的 API 超时时间。
     * <p>
     * 应用场景：
     * - 当需要从分区的起始位置开始消费消息时，可以使用此方法获取起始偏移量。
     * - 用于了解分区的当前数据范围的起点。
     * <p>
     * 实现细节：
     * 调用 {@link #beginningOffsets(Collection, Duration)} 方法，并传入默认的 API 超时时间 {@code defaultApiTimeoutMs}。
     * <p>
     * 设计考虑：
     * - 便捷性：提供一个使用默认超时的版本，简化常见场景下的调用。
     *
     * @param partitions 需要获取起始偏移量的分区集合。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应分区的最早可用偏移量。
     *         如果某个分区不存在或无法获取偏移量，则对应的值可能为 null 或抛出异常 (取决于具体实现和错误情况)。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     * @throws TimeoutException 如果在配置的超时时间内未能获取到偏移量信息。
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        // 调用另一个 beginningOffsets 重载方法，使用默认的 API 超时时间
        return beginningOffsets(partitions, defaultApiTimeoutMs);
    }

    /**
     * 获取指定分区集合的最早可用偏移量，并指定超时时间。
     * <p>
     * 此方法内部调用 {@link #beginningOrEndOffset(Collection, long, Duration)}，
     * 并传入 {@link ListOffsetsRequest#EARLIEST_TIMESTAMP} 作为时间戳参数，以获取起始偏移量。
     * <p>
     * 应用场景：
     * - 当需要从分区的起始位置开始消费消息时，可以使用此方法获取起始偏移量，并允许控制操作的超时。
     * - 用于了解分区的当前数据范围的起点。
     * <p>
     * 实现细节：
     * 调用私有辅助方法 {@code beginningOrEndOffset}，将时间戳参数设置为 {@code ListOffsetsRequest.EARLIEST_TIMESTAMP}，
     * 表示请求最早的偏移量。
     * <p>
     * 设计考虑：
     * - 代码复用：通过一个通用的私有方法处理获取起始和末尾偏移量的逻辑。
     * - 超时控制：允许用户指定操作的超时时间。
     *
     * @param partitions 需要获取起始偏移量的分区集合。
     * @param timeout 操作的超时时间。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应分区的最早可用偏移量。
     *         如果某个分区不存在或无法获取偏移量，则对应的值可能为 null 或抛出异常 (取决于具体实现和错误情况)。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     * @throws TimeoutException 如果在指定的超时时间内未能获取到偏移量信息。
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        // 调用 beginningOrEndOffset 方法获取分区的起始偏移量
        // ListOffsetsRequest.EARLIEST_TIMESTAMP 表示请求最早的可用偏移量
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    /**
     * 获取指定分区集合的最新可用偏移量 (即下一个将要写入消息的偏移量)。
     * <p>
     * 此方法是 {@link #endOffsets(Collection, Duration)} 的重载版本，使用默认的 API 超时时间。
     * <p>
     * 应用场景：
     * - 当需要了解分区的当前末尾位置 (LSO, Log End Offset) 时，可以使用此方法。
     * - 用于监控分区的写入进度或计算消费滞后 (lag)。
     * <p>
     * 实现细节：
     * 调用 {@link #endOffsets(Collection, Duration)} 方法，并传入默认的 API 超时时间 {@code defaultApiTimeoutMs}。
     * <p>
     * 设计考虑：
     * - 便捷性：提供一个使用默认超时的版本，简化常见场景下的调用。
     *
     * @param partitions 需要获取末尾偏移量的分区集合。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应分区的最新可用偏移量。
     *         如果某个分区不存在或无法获取偏移量，则对应的值可能为 null 或抛出异常 (取决于具体实现和错误情况)。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     * @throws TimeoutException 如果在配置的超时时间内未能获取到偏移量信息。
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        // 调用另一个 endOffsets 重载方法，使用默认的 API 超时时间
        return endOffsets(partitions, defaultApiTimeoutMs);
    }

    /**
     * 获取指定分区集合的最新可用偏移量 (即下一个将要写入消息的偏移量)，并指定超时时间。
     * <p>
     * 此方法内部调用 {@link #beginningOrEndOffset(Collection, long, Duration)}，
     * 并传入 {@link ListOffsetsRequest#LATEST_TIMESTAMP} 作为时间戳参数，以获取末尾偏移量。
     * <p>
     * 应用场景：
     * - 当需要了解分区的当前末尾位置 (LSO, Log End Offset) 时，可以使用此方法，并允许控制操作的超时。
     * - 用于监控分区的写入进度或计算消费滞后 (lag)。
     * <p>
     * 实现细节：
     * 调用私有辅助方法 {@code beginningOrEndOffset}，将时间戳参数设置为 {@code ListOffsetsRequest.LATEST_TIMESTAMP}，
     * 表示请求最新的偏移量。
     * <p>
     * 设计考虑：
     * - 代码复用：通过一个通用的私有方法处理获取起始和末尾偏移量的逻辑。
     * - 超时控制：允许用户指定操作的超时时间。
     *
     * @param partitions 需要获取末尾偏移量的分区集合。
     * @param timeout 操作的超时时间。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应分区的最新可用偏移量。
     *         如果某个分区不存在或无法获取偏移量，则对应的值可能为 null 或抛出异常 (取决于具体实现和错误情况)。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     * @throws TimeoutException 如果在指定的超时时间内未能获取到偏移量信息。
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        // 调用 beginningOrEndOffset 方法获取分区的末尾偏移量
        // ListOffsetsRequest.LATEST_TIMESTAMP 表示请求最新的可用偏移量
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    /**
     * 获取指定分区集合的起始或末尾偏移量。
     * <p>
     * 这是一个私有辅助方法，被 {@link #beginningOffsets(Collection, Duration)} 和 {@link #endOffsets(Collection, Duration)} 调用。
     * 它通过向应用事件处理器提交一个 {@link ListOffsetsEvent} 来实现。
     * <p>
     * 实现细节：
     * 1. 获取锁并确保消费者未关闭。
     * 2. 校验 {@code partitions} 参数：
     *    - 集合本身不能为 null。
     * 3. 如果 {@code partitions} 为空，则直接返回一个空映射。
     * 4. 将输入的分区集合转换为一个 {@code Map<TopicPartition, Long>}，其中每个分区对应的值是传入的 {@code timestamp}
     *    (例如 {@link ListOffsetsRequest#EARLIEST_TIMESTAMP} 或 {@link ListOffsetsRequest#LATEST_TIMESTAMP})。
     * 5. 创建一个 {@link ListOffsetsEvent}，包含转换后的时间戳映射、计算得到的截止时间以及一个布尔标志 (false 表示不需要时间戳，只需要偏移量)。
     * 6. 如果超时时间为 0 ({@code timeout.isZero()} 为 true)：
     *    - 将事件添加到应用事件处理器 (非阻塞)。
     *    - 立即返回事件的空结果集 (通常是一个空映射或包含 null 值的映射)。
     * 7. 如果超时时间大于 0：
     *    - 通过 {@code applicationEventHandler.addAndGet} 异步提交事件并等待结果。
     *    - 将返回的 {@code Map<TopicPartition, OffsetAndTimestampInternal>} 转换为 {@code Map<TopicPartition, Long>}，
     *      只提取偏移量信息。
     *    - 如果在超时时间内未能获取结果，则捕获 {@link TimeoutException} 并重新抛出，附带更详细的错误信息。
     * 8. 无论成功与否，最终都会释放锁。
     * <p>
     * 设计考虑：
     * - 代码复用：将获取起始和末尾偏移量的通用逻辑提取到此方法中。
     * - 异步处理：通过事件提交给后台线程处理，避免阻塞调用线程，除非明确等待结果。
     * - 参数校验：对输入参数进行校验。
     * - 超时控制：允许用户指定操作的超时时间。
     * - 兼容性：保留了与当前消费者实现相同的参数校验错误。
     * - 结果转换：将内部数据结构转换为 API 暴露的数据结构。
     * - 资源管理：使用 try-finally 块确保锁的正确释放。
     *
     * @param partitions 需要获取偏移量的分区集合。
     * @param timestamp 特殊的时间戳值，用于指示请求最早 ({@link ListOffsetsRequest#EARLIEST_TIMESTAMP}) 或
     *                  最新 ({@link ListOffsetsRequest#LATEST_TIMESTAMP}) 的偏移量。
     * @param timeout 操作的超时时间。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应分区的偏移量。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code partitions} 为 null。
     * @throws TimeoutException 如果在指定的超时时间内未能获取到偏移量信息。
     */
    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Duration timeout) {
        // 获取锁并确保消费者实例处于打开状态
        acquireAndEnsureOpen();
        try {
            // 保持与当前消费者实现相同的参数验证错误，以避免API级别的更改。
            // 检查 partitions 是否为 null，如果是，则抛出 NullPointerException
            requireNonNull(partitions, "Partitions cannot be null");

            // 如果 partitions 为空
            if (partitions.isEmpty()) {
                // 返回一个空的、不可修改的映射
                return Collections.emptyMap();
            }

            // 将分区集合转换为一个映射，其中键是 TopicPartition，值是传入的 timestamp
            Map<TopicPartition, Long> timestampToSearch = partitions
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> timestamp));
            // 创建一个 ListOffsetsEvent 对象，用于请求偏移量
            ListOffsetsEvent listOffsetsEvent = new ListOffsetsEvent(
                    timestampToSearch, // 要搜索的时间戳映射（实际上是 EARLIEST_TIMESTAMP 或 LATEST_TIMESTAMP）
                    calculateDeadlineMs(time, timeout), // 计算操作的截止时间
                    false); // false 表示不需要返回时间戳信息，只需要偏移量

            // 如果超时时间为零，则立即返回空结果；否则尝试获取结果，如果无法及时完成则抛出超时异常。
            // 如果超时时间为零
            if (timeout.isZero()) {
                // 将 listOffsetsEvent 添加到应用事件处理器（非阻塞）
                applicationEventHandler.add(listOffsetsEvent);
                // 返回 listOffsetsEvent 的空结果集
                return listOffsetsEvent.emptyResults();
            }

            // 声明一个用于存储从事件处理器获取的偏移量和时间戳映射的变量
            Map<TopicPartition, OffsetAndTimestampInternal> offsetAndTimestampMap;
            try {
                // 向应用事件处理器添加 listOffsetsEvent 并等待其完成，获取结果
                offsetAndTimestampMap = applicationEventHandler.addAndGet(listOffsetsEvent);
                // 将获取到的 offsetAndTimestampMap 转换为只包含偏移量的映射
                return offsetAndTimestampMap.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey, // 键保持不变 (TopicPartition)
                        entry -> entry.getValue().offset())); // 值取 OffsetAndTimestampInternal 中的 offset
            } catch (TimeoutException e) {
                // 如果捕获到 TimeoutException
                // 抛出新的 TimeoutException，并附带更详细的错误消息
                // 注意：这里的错误消息 "Failed to get offsets by times" 可能不太准确，因为此方法也用于获取起始/末尾偏移量，而不仅仅是按时间戳获取
                throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
            }
        } finally {
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * 获取指定主题分区的当前消费滞后 (lag)。
     * <p>
     * 消费滞后是指分区的末尾偏移量 (Log End Offset, LSO) 与消费者当前已提交偏移量之间的差值。
     * 如果没有已提交的偏移量，或者无法获取 LSO，则返回 {@link OptionalLong#empty()}。
     * <p>
     * 此方法会向应用事件处理器提交一个 {@link CurrentLagEvent}，
     * 该事件将在后台处理，以计算并返回指定分区的消费滞后。
     * <p>
     * 应用场景：
     * - 监控消费者的消费进度，了解其与分区最新数据的差距。
     * - 用于告警或自动伸缩等场景。
     * <p>
     * 实现细节：
     * 1. 获取锁并确保消费者未关闭。
     * 2. 创建一个 {@link CurrentLagEvent}，包含要查询的主题分区、当前的隔离级别以及默认的 API 超时截止时间。
     * 3. 通过 {@code applicationEventHandler.addAndGet} 异步提交事件并等待结果 (一个 {@link OptionalLong} 表示的滞后值)。
     * 4. 无论成功与否，最终都会释放锁。
     * <p>
     * 设计考虑：
     * - 异步处理：通过事件提交给后台线程处理，避免阻塞调用线程。
     * - 结果类型：使用 {@link OptionalLong} 表示结果，清晰地处理可能不存在滞后值的情况。
     * - 资源管理：使用 try-finally 块确保锁的正确释放。
     *
     * @param topicPartition 需要计算滞后的主题分区，不能为 null。
     * @return 一个 {@link OptionalLong}，如果成功计算出滞后，则包含滞后值；否则为空。
     *         滞后值是非负的。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws NullPointerException 如果 {@code topicPartition} 为 null。
     * @throws TimeoutException 如果在配置的超时时间内未能计算出滞后。
     */
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        // 获取锁并确保消费者实例处于打开状态
        acquireAndEnsureOpen();
        try {
            // 向应用事件处理器添加并执行一个 CurrentLagEvent 事件
            // 这个事件会计算并返回指定主题分区的当前消费滞后（lag）
            // 参数包括：
            //   topicPartition: 要查询滞后的主题分区
            //   isolationLevel: 当前的隔离级别
            //   defaultApiTimeoutDeadlineMs(): 操作的超时截止时间
            return applicationEventHandler.addAndGet(new CurrentLagEvent(
                topicPartition,
                isolationLevel,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * 获取此消费者的消费者组元数据。
     * <p>
     * 这通常用于事务性生产者，以确保事务的原子性 (exactly-once semantics)。
     * 如果消费者没有配置 {@code group.id}，此方法会抛出 {@link InvalidGroupIdException}。
     * <p>
     * 应用场景：
     * - 在实现 Kafka 事务时，生产者需要知道消费者的组元数据，以便将偏移量提交和消息生产包含在同一个事务中。
     * <p>
     * 实现细节：
     * 1. 获取锁并确保消费者未关闭。
     * 2. 调用 {@code maybeThrowInvalidGroupIdException()} 检查是否配置了有效的 {@code group.id}，如果没有则抛出异常。
     * 3. 从 {@code groupMetadata} (一个 {@code CompletableFuture<ConsumerGroupMetadata>}) 中获取结果。
     *    这里使用了两次 {@code .get()}，第一次是从 {@code CompletableFuture} 获取，第二次可能是因为内部实现返回了嵌套的 Future 或者是一个 Supplier。
     *    (需要查看 {@code groupMetadata} 的具体类型和初始化方式来确定确切原因，但通常表示等待异步操作完成并获取其结果)。
     * 4. 无论成功与否，最终都会释放锁。
     * <p>
     * 设计考虑：
     * - 线程安全：通过锁保护对共享状态的访问。
     * - 组ID校验：确保只有配置了组ID的消费者才能获取组元数据。
     * - 异步获取：{@code groupMetadata} 可能是一个异步计算的值，通过 {@code .get()} 等待其完成。
     * - 资源管理：使用 try-finally 块确保锁的正确释放。
     *
     * @return 当前消费者的 {@link ConsumerGroupMetadata}。
     * @throws IllegalStateException 如果消费者已关闭。
     * @throws InvalidGroupIdException 如果消费者没有配置有效的 {@code group.id}。
     * @throws KafkaException 如果在获取组元数据时发生其他 Kafka 相关的错误。
     */
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        // 获取锁并确保消费者实例处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查 group.id 是否有效，如果无效（例如未设置），则可能抛出 InvalidGroupIdException
            maybeThrowInvalidGroupIdException();
            // 从 groupMetadata (可能是一个 CompletableFuture 或类似的异步结果持有者) 中获取消费者组元数据
            // 两次 .get() 调用暗示了 groupMetadata 可能是一个持有 Future 的 Future，或者是一个返回 Future 的 Supplier
            return groupMetadata.get().get();
        } finally {
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * 强制消费者组进行再平衡。
     * <p>
     * <strong>注意：</strong>此操作在新版消费者组协议 (KIP-848 及更高版本) 中不受支持。
     * 调用此方法将会记录一条警告日志，但不会执行任何实际的再平衡操作。
     * <p>
     * 应用场景 (旧版协议)：
     * - 在某些特殊情况下，管理员或应用程序可能需要手动触发一次消费者组的再平衡，
     *   例如，当检测到组成员状态异常或需要重新分配分区时。
     * <p>
     * 实现细节 (新版协议)：
     * - 记录一条警告日志，提示此操作在新协议中不受支持。
     * <p>
     * 设计考虑 (新版协议)：
     * - 兼容性：保留此方法以避免破坏现有 API，但明确指出其在新协议中的行为。
     * - 日志提示：通过日志告知用户此操作的当前状态。
     */
    @Override
    public void enforceRebalance() {
        // 记录一条警告日志，说明此操作在新的消费者组协议中不受支持
        // 记录一条警告日志，说明此操作在新的消费者组协议中不受支持
        log.warn("Operation not supported in new consumer group protocol");
    }

    /**
     * 强制消费者组进行再平衡，并提供一个原因字符串。
     * <p>
     * <strong>注意：</strong>此操作在新版消费者组协议 (KIP-848 及更高版本) 中不受支持。
     * 调用此方法将会记录一条警告日志，但不会执行任何实际的再平衡操作。原因字符串也会被忽略。
     * <p>
     * 应用场景 (旧版协议)：
     * - 与 {@link #enforceRebalance()} 类似，但允许提供一个描述性的原因，可能用于日志记录或审计。
     * <p>
     * 实现细节 (新版协议)：
     * - 记录一条警告日志，提示此操作在新协议中不受支持。
     * <p>
     * 设计考虑 (新版协议)：
     * - 兼容性：保留此方法以避免破坏现有 API。
     * - 日志提示：通过日志告知用户此操作的当前状态。
     *
     * @param reason 强制再平衡的原因。在新协议中此参数被忽略。
     */
    @Override
    public void enforceRebalance(String reason) {
        // 记录一条警告日志，说明此操作在新的消费者组协议中不受支持
        // 记录一条警告日志，说明此操作在新的消费者组协议中不受支持
        log.warn("Operation not supported in new consumer group protocol");
    }

    /**
     * 关闭消费者，使用默认的超时时间 ({@link ConsumerUtils#DEFAULT_CLOSE_TIMEOUT_MS})。
     * <p>
     * 此方法是 {@link #close(Duration)} 的便捷版本。
     * 它会尝试优雅地关闭消费者，包括提交任何待处理的偏移量 (如果启用了自动提交或在关闭时提交)、
     * 离开消费者组、关闭网络连接等。
     * <p>
     * 应用场景：
     * - 当应用程序结束或不再需要消费消息时，应调用此方法来释放资源并确保消费者状态的正确清理。
     * <p>
     * 实现细节：
     * - 调用 {@link #close(Duration)} 方法，并传入一个由 {@code DEFAULT_CLOSE_TIMEOUT_MS} 转换而来的 {@link Duration} 对象。
     * <p>
     * 设计考虑：
     * - 便捷性：提供一个无需指定超时时间的关闭方法，使用合理的默认值。
     * - 资源释放：确保所有与消费者相关的资源得到妥善处理。
     *
     * @see #close(Duration)
     */
    @Override
    public void close() {
        // 调用另一个 close 重载方法，使用默认的关闭超时时间
        // DEFAULT_CLOSE_TIMEOUT_MS 是一个以毫秒为单位的常量
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    /**
     * 关闭此消费者，并等待长达指定超时时间以完成待处理操作。
     * 应用场景：当不再需要消费者实例时，调用此方法以释放资源并确保优雅关闭。
     * 设计考虑：
     * - 超时机制：允许应用程序指定等待关闭操作完成的最长时间，防止无限期阻塞。
     * - 线程安全：通过 acquire/release 机制确保关闭操作的原子性和线程安全。
     * - 幂等性：如果消费者已经关闭，则此方法不执行任何操作。
     * @param timeout 等待关闭完成的最长时间
     * @throws IllegalArgumentException 如果超时时间为负
     */
    @Override
    public void close(Duration timeout) { // 关闭消费者，带有超时参数
        // 检查超时参数是否有效
        if (timeout.toMillis() < 0)
            // 如果超时时间为负，则抛出 IllegalArgumentException 异常
            throw new IllegalArgumentException("The timeout cannot be negative.");
        // 获取锁，确保在关闭过程中其他线程不能修改消费者状态
        acquire();
        try {
            // 检查消费者是否尚未关闭
            if (!closed) {
                // 需要在设置标志之前关闭，因为关闭函数本身可能会触发需要消费者仍然打开的再平衡回调
                // 调用私有的 close 方法执行实际的关闭逻辑，不吞咽异常
                close(timeout, false);
            }
        } finally {
            // 在 finally 块中确保设置 closed 标志并释放锁
            // 将 closed 标志设置为 true，表示消费者已关闭
            closed = true;
            // 释放锁
            release();
        }
    }

    /**
     * 在实现 {@link AsyncKafkaConsumer} 的 {@link #close(Duration)} 方法时，请牢记以下原则。
     * 将来，这些原则可能会正式成为顶级 {@link KafkaConsumer#close(Duration)} API 的一部分，但目前它们仍保留在此处。
     *
     * <ol>
     *     <li>
     *         {@link ConsumerRebalanceListener} 回调（如果适用）的执行必须在应用程序线程上执行，以确保它不会干扰后台线程上的网络 I/O。
     *     </li>
     *     <li>
     *         在尝试离开消费者组之前，{@link ConsumerRebalanceListener} 回调执行必须完成。在此上下文中，“完成”并不一定意味着
     *         <em>成功</em>；即使执行因错误而<em>失败</em>，执行也是“完成”的。
     *     </li>
     *     <li>
     *         在 {@link ConsumerRebalanceListener} 回调执行期间抛出的任何错误都将被捕获，以确保它不会阻止其余 {@link #close()} 逻辑的执行。
     *     </li>
     *     <li>
     *         在 {@link ConsumerRebalanceListener} 的整个执行期间，应用程序线程将被阻塞。消费者不采用短路回调执行的机制，因此执行不受 {@link #close(Duration)} 中超时的限制。
     *     </li>
     *     <li>
     *         给定的 {@link ConsumerRebalanceListener} 实现可能会受到应用程序线程中断状态的影响。如果回调实现执行任何阻塞操作，则可能会导致错误。实现可以选择通过 {@link Thread#isInterrupted()} 或 {@link Thread#isInterrupted()} 抢先检查线程的中断标志并更改其行为。
     *     </li>
     *     <li>
     *         如果在执行 {@link ConsumerRebalanceListener} 回调<em>之前</em>应用程序线程被中断，则线程的中断状态将为 {@link ConsumerRebalanceListener} 执行保留。
     *     </li>
     *     <li>
     *         如果在执行 {@link ConsumerRebalanceListener} 回调<em>之前</em>应用程序线程被中断，<em>但是</em>回调清除了中断状态，则 {@link #close()} 方法将不会在 {@link #close()} 的其余执行期间努力恢复应用程序线程的中断状态。
     *     </li>
     *     <li>
     *         离开消费者组是通过发出“离开组”网络请求来实现的。消费者将尝试在“尽力而为”的基础上离开该组。不保证消费者在 {@link #close()} 方法完成处理之前已成功离开该组。
     *     </li>
     *     <li>
     *         无论超时是否过去或应用程序线程是否收到 {@link InterruptException} 或 {@link InterruptedException}，消费者都将尝试离开该组。
     *     </li>
     *     <li>
     *         应用程序线程将等待确认消费者已离开该组，直到发生以下情况之一：
     *
     *         <ol>
     *             <li>从组协调器收到“离开组”响应的确认</li>
     *             <li>用户提供的超时已过</li>
     *             <li>抛出 {@link InterruptException} 或 {@link InterruptedException}</li>
     *         </ol>
     *     </li>
     * </ol>
     */
    /**
     * 执行实际的关闭逻辑。
     * 应用场景：由公共的 close 方法调用，负责协调关闭过程中的各个步骤，如自动提交、离开消费者组、关闭网络线程等。
     * 设计考虑：
     * - 顺序性：确保关闭操作按正确的顺序执行，例如，在关闭网络线程之前提交偏移量和离开组。
     * - 错误处理：通过 AtomicReference 捕获第一个发生的异常，并在最后根据 swallowException 参数决定是否抛出。
     * - 资源清理：确保所有相关资源（拦截器、度量、反序列化器等）都被正确关闭。
     * @param timeout 关闭操作的超时时间
     * @param swallowException 是否吞咽在关闭过程中发生的异常
     */
    private void close(Duration timeout, boolean swallowException) { // 私有 close 方法，执行实际的关闭操作
        // 记录跟踪级别的日志，表示开始关闭 Kafka 消费者
        log.trace("Closing the Kafka consumer");
        // 创建一个 AtomicReference 用于存储关闭过程中发生的第一个异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // 我们已经通过超时关闭，从现在开始不允许唤醒。
        // 禁用唤醒触发器，防止在关闭过程中被意外唤醒
        wakeupTrigger.disableWakeups(); // 禁用唤醒

        // 为关闭请求创建一个计时器，确保关闭操作在指定的超时时间内完成
        final Timer closeTimer = createTimerForCloseRequests(timeout);
        // 如果客户端遥测报告器存在，则调用其 initiateClose 方法开始关闭遥测报告
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        // 更新关闭计时器，记录当前时间点
        // 再次更新关闭计时器，记录当前时间点
        closeTimer.update();
        // 准备关闭网络线程
        // 在关闭网络线程之前，我们需要确保以下操作按正确的顺序发生...
        // 尝试在关闭时自动提交偏移量，如果失败则记录错误并存储异常
        swallow(log, Level.ERROR, "Failed to auto-commit offsets",
            () -> autoCommitOnClose(closeTimer), firstException); // 调用 autoCommitOnClose 方法
        // 尝试停止查找协调器，如果失败则记录错误并存储异常
        swallow(log, Level.ERROR, "Failed to stop finding coordinator",
            this::stopFindCoordinatorOnClose, firstException); // 调用 stopFindCoordinatorOnClose 方法
        // 尝试运行再平衡回调以释放组分配，如果失败则记录错误并存储异常
        swallow(log, Level.ERROR, "Failed to release group assignment",
            this::runRebalanceCallbacksOnClose, firstException); // 调用 runRebalanceCallbacksOnClose 方法
        // 尝试在关闭消费者时离开消费者组，如果失败则记录错误并存储异常
        swallow(log, Level.ERROR, "Failed to leave group while closing consumer",
            () -> leaveGroupOnClose(closeTimer), firstException); // 调用 leaveGroupOnClose 方法
        // 尝试等待待处理的异步提交并执行提交回调，如果失败则记录错误并存储异常
        swallow(log, Level.ERROR, "Failed invoking asynchronous commit callbacks while closing consumer",
            () -> awaitPendingAsyncCommitsAndExecuteCommitCallbacks(closeTimer, false), firstException); // 调用 awaitPendingAsyncCommitsAndExecuteCommitCallbacks 方法
        // 如果应用程序事件处理器不为 null，则安静地关闭它
        if (applicationEventHandler != null)
            // 使用剩余的关闭时间关闭应用程序事件处理器（网络线程）
            closeQuietly(() -> applicationEventHandler.close(Duration.ofMillis(closeTimer.remainingMs())), "Failed shutting down network thread", firstException);
        // 更新关闭计时器，记录当前时间点
        // 再次更新关闭计时器，记录当前时间点
        closeTimer.update();

        // close() 方法可能在构造函数内部被调用。在这种情况下，可能尚未构造清理器或后台事件队列，
        // 因此首先检查它们以避免 NullPointerException。
        // 如果后台事件清理器和后台事件队列都不为 null
        if (backgroundEventReaper != null && backgroundEventQueue != null)
            // 清理后台事件队列中剩余的事件
            backgroundEventReaper.reap(backgroundEventQueue);

        // 安静地关闭消费者拦截器，如果失败则记录错误并存储异常
        closeQuietly(interceptors, "consumer interceptors", firstException);
        // 安静地关闭 Kafka 消费者度量指标，如果失败则记录错误并存储异常
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        // 安静地关闭消费者度量指标，如果失败则记录错误并存储异常
        closeQuietly(metrics, "consumer metrics", firstException);
        // 安静地关闭反序列化器，如果失败则记录错误并存储异常
        closeQuietly(deserializers, "consumer deserializers", firstException);
        // 如果客户端遥测报告器存在，则安静地关闭它，如果失败则记录错误并存储异常
        clientTelemetryReporter.ifPresent(reporter -> closeQuietly(reporter, "async consumer telemetry reporter", firstException));

        // 注销应用程序信息，用于 JMX 监控
        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        // 记录调试级别的日志，表示 Kafka 消费者已关闭
        log.debug("Kafka consumer has been closed");
        // 获取在关闭过程中可能发生的第一个异常
        Throwable exception = firstException.get();
        // 如果存在异常并且不应吞咽该异常
        if (exception != null && !swallowException) {
            // 如果异常是 InterruptException 的实例
            if (exception instanceof InterruptException) {
                // 重新抛出 InterruptException
                throw (InterruptException) exception;
            }
            // 将异常包装为 KafkaException 并抛出
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    /**
     * 为关闭请求创建一个计时器。
     * 应用场景：在执行关闭操作时，需要一个计时器来跟踪剩余时间，确保操作在指定的超时时间内完成。
     * 设计考虑：
     * - 健壮性：处理 this.time 可能为 null 的情况（例如在构造函数中发生异常）。
     * - 超时合并：取用户提供的超时时间和内部请求超时时间中的较小值，作为实际的超时时间。
     * @param timeout 用户指定的关闭超时时间
     * @return 一个配置好的计时器实例
     */
    private Timer createTimerForCloseRequests(Duration timeout) { // 为关闭请求创建计时器
        // 如果在构造函数中设置 this.time 字段之前发生异常，this.time 可能为 null
        // 如果 this.time 为 null，则使用系统时间；否则使用 this.time
        final Time time = (this.time == null) ? Time.SYSTEM : this.time;
        // 返回一个计时器，其超时时间为用户提供的超时时间和请求超时时间中的较小值
        return time.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    /**
     * 在关闭消费者时执行自动提交（如果已启用）。
     * 应用场景：确保在消费者关闭前，将已消费但尚未提交的偏移量进行同步提交。
     * 设计考虑：
     * - 条件执行：仅当消费者属于某个组（groupMetadata 不为空）且启用了自动提交时才执行。
     * - 同步提交：使用 commitSyncAllConsumed 方法进行同步提交，确保提交操作完成。
     * - 事件通知：向应用程序事件处理器添加 CommitOnCloseEvent 事件，通知网络线程进行相应的处理。
     * @param timer 用于控制提交操作的计时器
     */
    private void autoCommitOnClose(final Timer timer) { // 在关闭时自动提交偏移量
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        if (groupMetadata.get().isEmpty())
            return;

        // 如果启用了自动提交
        if (autoCommitEnabled)
            // 同步提交所有已消费的偏移量
            commitSyncAllConsumed(timer);

        // 向应用程序事件处理器添加一个 CommitOnCloseEvent 事件，通知网络线程进行关闭提交通知
        applicationEventHandler.add(new CommitOnCloseEvent());
    }

    /**
     * 在关闭消费者时运行再平衡监听器回调。
     * 应用场景：当消费者关闭时，如果它之前被分配了一些分区，需要调用 onPartitionsRevoked 或 onPartitionsLost 回调通知监听器。
     * 设计考虑：
     * - 条件执行：仅当消费者属于某个组且当前分配了分区时才执行。
     * - 回调选择：根据成员时期（memberEpoch）决定调用 onPartitionsRevoked（正常撤销）还是 onPartitionsLost（分区丢失，通常发生在协调器故障或会话超时）。
     * - 错误处理：如果回调执行过程中发生异常，则将其包装为 KafkaException 并抛出。
     */
    private void runRebalanceCallbacksOnClose() { // 在关闭时运行再平衡回调
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        if (groupMetadata.get().isEmpty())
            return;

        // 获取当前消费者在组内的成员时期 (generation ID)
        int memberEpoch = groupMetadata.get().get().generationId();

        // 获取当前分配给该消费者的分区快照
        Set<TopicPartition> assignedPartitions = groupAssignmentSnapshot.get();

        // 如果没有分配的分区
        if (assignedPartitions.isEmpty())
            // 无需撤销任何分区，直接返回
            return;

        // 创建一个有序集合用于存放被丢弃（撤销或丢失）的分区，使用 TOPIC_PARTITION_COMPARATOR 进行排序
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 将所有已分配的分区添加到 droppedPartitions 集合中
        droppedPartitions.addAll(assignedPartitions);

        // 用于存储再平衡监听器回调可能抛出的异常
        final Exception error;

        // 如果成员时期大于 0，表示是正常的成员关系，调用 onPartitionsRevoked 回调
        if (memberEpoch > 0)
            error = rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions);
        // 否则，表示可能是由于会话超时等原因导致分区丢失，调用 onPartitionsLost 回调
        else
            error = rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);

        // 如果回调执行过程中发生错误
        if (error != null)
            // 将错误包装为 KafkaException（如果需要）并抛出
            throw ConsumerUtils.maybeWrapAsKafkaException(error);
    }

    private void leaveGroupOnClose(final Timer timer) {
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        if (groupMetadata.get().isEmpty())
            return;

        log.debug("Leaving the consumer group during consumer close");
        try {
            applicationEventHandler.addAndGet(new LeaveGroupOnCloseEvent(calculateDeadlineMs(timer)));
            log.info("Completed leaving the group");
        } catch (TimeoutException e) {
            log.warn("Consumer attempted to leave the group but couldn't " +
                "complete it within {} ms. It will proceed to close.", timer.timeoutMs());
        } finally {
            timer.update();
        }
    }

    private void stopFindCoordinatorOnClose() {
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        // 如果消费者不属于任何组（即 groupMetadata 为空），则直接返回
        if (groupMetadata.get().isEmpty())
            return;
        log.debug("Stop finding coordinator during consumer close");
        applicationEventHandler.add(new StopFindCoordinatorOnCloseEvent());
    }

    // 仅用于测试
    /**
     * @author Trae
     * @date 2024/07/26
     * @description 同步提交所有已消费的偏移量。此方法主要用于关闭消费者前的最后一次提交，确保所有已处理的消息偏移量都被持久化。
     * 应用场景：在消费者关闭流程中，为了保证数据不丢失，需要调用此方法将所有已消费但尚未提交的偏移量进行同步提交。
     * 设计考虑：此方法设计为包可见性，主要供内部测试使用。它封装了同步提交的逻辑，并处理了可能发生的异常，确保关闭流程的健壮性。
     * @param timer 定时器，用于控制提交操作的超时时间。
     */
    void commitSyncAllConsumed(final Timer timer) {
        // 记录调试日志，表明正在发送关闭时的同步自动提交请求
        log.debug("Sending synchronous auto-commit on closing");
        try {
            // 调用 commitSync 方法，使用定时器剩余的时间作为超时时间
            commitSync(Duration.ofMillis(timer.remainingMs()));
        } catch (Exception e) {
            // 与异步自动提交失败的处理方式一致，我们不向上抛出异常
            // 记录警告日志，表明同步自动提交失败，并附带异常信息
            log.warn("Synchronous auto-commit failed", e);
        }
        // 更新定时器状态，这通常意味着重置或记录经过的时间
        timer.update();
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 唤醒消费者。如果消费者当前在 {@link #poll(Duration)} 方法中阻塞，则此方法会使其立即返回一个空的 {@link ConsumerRecords}。
     * 如果没有活动的轮询，则下一次调用 {@link #poll(Duration)} 或任何其他阻塞消费者的方法时，会立即抛出 {@link org.apache.kafka.common.errors.WakeupException}。
     * 应用场景：当需要从另一个线程中断消费者的长时间轮询操作时，可以调用此方法。例如，在应用程序关闭时，需要优雅地停止消费者线程。
     * 设计考虑：此方法通过内部的 `wakeupTrigger` 机制实现。它是一个线程安全的操作，可以从任何线程调用。
     */
    @Override
    public void wakeup() {
        // 调用 wakeupTrigger 的 wakeup 方法来触发唤醒逻辑
        wakeupTrigger.wakeup();
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 同步提交消费者获取的最新偏移量。此方法将向事件处理器发送一个提交事件，并等待该事件完成。
     * 它会提交由上一次 {@link #poll(Duration)} 返回的所有消息的偏移量。
     * 这是一个阻塞操作，直到提交成功或发生超时。
     * 应用场景：当需要确保偏移量已经被成功提交到 Kafka 时使用，例如在处理完一批重要的消息后。
     * 设计考虑：此方法封装了同步提交的复杂性，包括向事件队列添加提交事件、等待事件完成以及处理超时。
     * 它依赖于内部的事件处理机制来与网络线程交互。
     * @param timeout 阻塞操作的最大等待时间。
     */
    @Override
    public void commitSync(final Duration timeout) {
        // 调用私有的 commitSync 方法，传入一个空的 Optional 表示提交当前消费的偏移量，以及指定的超时时间
        commitSync(Optional.empty(), timeout);
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 同步提交为指定主题分区提供的偏移量映射。
     * 此方法将向事件处理器发送一个提交事件，并等待该事件完成。
     * 这是一个阻塞操作，直到提交成功或发生超时（使用默认API超时时间）。
     * 应用场景：当需要精确控制提交哪些分区的哪些偏移量时使用。例如，在实现自定义的偏移量管理策略时。
     * 设计考虑：此方法允许用户提交特定的偏移量，而不是依赖于消费者自动跟踪的偏移量。它同样依赖事件处理机制。
     * @param offsets 一个映射，包含每个主题分区的偏移量和元数据，这些将被提交。
     */
    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 调用私有的 commitSync 方法，传入包含指定偏移量的 Optional，以及默认的 API 超时时间
        commitSync(Optional.of(offsets), defaultApiTimeoutMs);
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 同步提交为指定主题分区提供的偏移量映射，并指定超时时间。
     * 此方法将向事件处理器发送一个提交事件，并等待该事件完成。
     * 这是一个阻塞操作，直到提交成功或发生指定的超时。
     * 应用场景：与上一个 `commitSync` 方法类似，但允许用户指定自定义的超时时间，以适应不同的网络条件或处理要求。
     * 设计考虑：提供了更灵活的超时控制，允许调用者根据具体情况调整等待时间。
     * @param offsets 一个映射，包含每个主题分区的偏移量和元数据，这些将被提交。
     * @param timeout 阻塞操作的最大等待时间。
     */
    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        // 调用私有的 commitSync 方法，传入包含指定偏移量的 Optional，以及指定的超时时间
        commitSync(Optional.of(offsets), timeout);
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 私有的同步提交方法，是所有公共 `commitSync` 方法的底层实现。
     * 它负责获取锁，确保消费者处于打开状态，创建同步提交事件，将其发送到事件处理器，
     * 等待异步提交完成，执行回调，并处理唤醒逻辑和度量记录。
     * 应用场景：作为内部核心提交逻辑，被其他 `commitSync` 重载方法调用。
     * 设计考虑：此方法集中了同步提交的核心逻辑，包括线程同步、事件创建、结果等待、拦截器调用和资源释放。
     * 使用 `CompletableFuture` 来处理异步操作的结果。
     * @param offsets 一个可选的偏移量映射。如果为空，则提交当前消费的偏移量；否则，提交指定的偏移量。
     * @param timeout 提交操作的超时时间。
     */
    private void commitSync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, Duration timeout) {
        // 获取锁并确保消费者处于打开状态，如果已关闭则抛出异常
        acquireAndEnsureOpen();
        // 记录提交操作开始的时间（纳秒）
        long commitStart = time.nanoseconds();
        try {
            // 创建一个同步提交事件，包含偏移量信息和计算出的截止时间
            SyncCommitEvent syncCommitEvent = new SyncCommitEvent(offsets, calculateDeadlineMs(time, timeout));
            // 调用内部的 commit 方法（可能是异步的），返回一个 CompletableFuture，表示提交操作的结果
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitFuture = commit(syncCommitEvent);

            // 创建一个定时器，用于等待挂起的异步提交和执行回调
            Timer requestTimer = time.timer(timeout.toMillis());
            // 等待任何挂起的异步提交完成，并执行相关的提交回调
            awaitPendingAsyncCommitsAndExecuteCommitCallbacks(requestTimer, true);

            // 将当前的提交操作 (commitFuture) 设置为唤醒触发器的活动任务
            // 这样如果 wakeup() 被调用，这个 future 可能会被中断
            wakeupTrigger.setActiveTask(commitFuture);
            // 等待同步提交操作完成，并获取提交的偏移量结果
            // ConsumerUtils.getResult 会处理超时和中断
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = ConsumerUtils.getResult(commitFuture, requestTimer);
            // 调用拦截器的 onCommit 方法，通知它们提交已完成
            interceptors.onCommit(committedOffsets);
        } finally {
            // 清除唤醒触发器中的活动任务
            wakeupTrigger.clearTask();
            // 记录同步提交操作的耗时到度量系统中
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            // 释放之前获取的锁
            release();
        }
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 等待上一个挂起的异步提交完成，并执行所有排队的偏移量提交回调。
     * 应用场景：在执行同步提交之前，需要确保之前的异步提交已经完成，以避免潜在的竞态条件或顺序问题。
     * 同时，这也确保了异步提交的回调能够及时执行。
     * 设计考虑：此方法通过创建一个新的 `CompletableFuture` 来等待 `lastPendingAsyncCommit` 完成，
     * 而不是直接等待 `lastPendingAsyncCommit`，是为了避免 `wakeupTrigger` 意外地完成 `lastPendingAsyncCommit`。
     * 错误处理由原始的异步提交 future 及其回调负责，这里只关心等待其完成。
     * @param timer 用于控制等待操作的超时时间。
     * @param enableWakeup 是否允许此等待操作被 `wakeup()` 中断。
     */
    private void awaitPendingAsyncCommitsAndExecuteCommitCallbacks(Timer timer, boolean enableWakeup) {
        // 如果没有挂起的异步提交，则直接返回
        if (lastPendingAsyncCommit == null) {
            return;
        }

        try {
            // 创建一个新的 CompletableFuture 用于等待
            final CompletableFuture<Void> futureToAwait = new CompletableFuture<>();
            // 我们不希望唤醒触发器完成我们挂起的异步提交 future，
            // 所以在这里创建一个新的 future。挂起异步提交中的任何错误都将由
            // 异步提交 future / 提交回调处理 - 在这里，我们只想等待它完成。
            // 当 lastPendingAsyncCommit 完成时（无论成功还是失败），都会完成 futureToAwait
            lastPendingAsyncCommit.whenComplete((v, t) -> futureToAwait.complete(null));
            // 如果允许唤醒
            if (enableWakeup) {
                // 将 futureToAwait 设置为唤醒触发器的活动任务
                wakeupTrigger.setActiveTask(futureToAwait);
            }
            // 等待 futureToAwait 完成，会受到定时器的超时限制
            ConsumerUtils.getResult(futureToAwait, timer);
            // 异步提交已处理完毕，将其置为 null
            lastPendingAsyncCommit = null;
        } finally {
            // 如果允许唤醒
            if (enableWakeup) {
                // 清除唤醒触发器中的活动任务
                wakeupTrigger.clearTask();
            }
            // 更新定时器状态
            timer.update();
        }
        // 执行所有排队的偏移量提交回调
        offsetCommitCallbackInvoker.executeCallbacks();
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 获取客户端实例ID。此ID用于唯一标识此消费者实例，通常与遥测功能相关。
     * 应用场景：在启用遥测功能（通过 `enable.metrics.push` 配置）后，可以使用此方法获取客户端的唯一标识符，
     * 该标识符可用于在监控系统中跟踪特定的消费者实例。
     * 设计考虑：此方法依赖于 `clientTelemetryReporter`。如果遥测未启用，则会抛出 `IllegalStateException`。
     * @param timeout 获取客户端实例ID的超时时间。
     * @return 客户端实例的 {@link Uuid}。
     * @throws IllegalStateException 如果遥测未启用。
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        // 检查客户端遥测报告器是否为空（即遥测是否未启用）
        if (clientTelemetryReporter.isEmpty()) {
            // 如果遥测未启用，则抛出 IllegalStateException 异常
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        // 调用 ClientTelemetryUtils 的 fetchClientInstanceId 方法获取客户端实例 ID
        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 获取当前分配给此消费者的主题分区集合。
     * 如果是手动分配（使用 {@link #assign(Collection)}），则返回手动分配的分区。
     * 如果是自动分配（使用 {@link #subscribe} 系列方法），则返回由消费者组协议动态分配的分区。
     * 应用场景：当需要了解消费者当前正在消费哪些分区时，可以调用此方法。
     * 设计考虑：此方法是线程安全的，通过 `acquireAndEnsureOpen` 和 `release` 保证了对内部状态的同步访问。
     * 返回的是一个不可修改的集合，以防止外部意外修改内部状态。
     * @return 当前分配给此消费者的主题分区集合，如果未进行分配，则为空集。
     */
    @Override
    public Set<TopicPartition> assignment() {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 从 subscriptions 对象获取已分配的分区，并返回一个不可修改的副本
            return Collections.unmodifiableSet(subscriptions.assignedPartitions());
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 获取当前订阅的主题集合。如果之前没有调用过订阅方法，则返回空集。
     * 注意：此方法返回的是通过 {@link #subscribe(Collection)} 或 {@link #subscribe(Pattern)} 等方法明确订阅的主题或模式，
     * 而不是实际分配到的分区所属的主题。
     * 应用场景：当需要了解消费者配置的订阅目标（具体主题或主题模式）时使用。
     * 设计考虑：与 `assignment()` 类似，此方法也是线程安全的，并返回不可修改的集合。
     * @return 当前订阅的主题集合。
     */
    @Override
    public Set<String> subscription() {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 从 subscriptions 对象获取订阅的主题集合，并返回一个不可修改的副本
            return Collections.unmodifiableSet(subscriptions.subscription());
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 手动将此消费者分配给指定的主题分区集合。
     * 调用此方法会取代之前通过 {@link #subscribe} 系列方法进行的任何自动订阅。
     * 手动分配不会使用消费者组管理功能（如偏移量提交到 Kafka、组成员协调等）。
     * 应用场景：当需要完全控制消费者消费哪些分区时，例如在某些特定的流处理场景或测试环境中。
     * 设计考虑：此方法会清除之前的订阅状态，并直接设置新的分区分配。
     * 它会触发一个 `AssignmentChangeEvent`，该事件可能会触发自动提交（如果已配置且指定了组ID），
     * 以确保从取消订阅的分区提交偏移量。
     * @param partitions 要分配给此消费者的主题分区集合，不能为 null。
     *                   如果为空集合，则等同于调用 {@link #unsubscribe()}。
     * @throws IllegalArgumentException 如果 `partitions` 为 null，或者其中任何分区的topic为 null 或空字符串。
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查传入的分区集合是否为 null
            if (partitions == null) {
                // 如果为 null，则抛出 IllegalArgumentException
                throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
            }

            // 如果分区集合为空
            if (partitions.isEmpty()) {
                // 调用 unsubscribe 方法取消所有订阅和分配
                unsubscribe();
                // 直接返回
                return;
            }

            // 遍历分区集合，检查每个分区的topic是否有效
            for (TopicPartition tp : partitions) {
                // 获取分区的主题名称，如果分区对象为 null，则主题为 null
                String topic = (tp != null) ? tp.topic() : null;
                // 如果主题名称为空白（null 或空字符串）
                if (isBlank(topic))
                    // 抛出 IllegalArgumentException，指示分区的主题不能为空
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }

            // 清理那些不属于新分配主题的分区的缓冲数据
            // 创建一个 HashSet 用于存储当前分配中也存在于新分配中的分区
            final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

            // 遍历当前已分配的分区
            for (TopicPartition tp : subscriptions.assignedPartitions()) {
                // 如果当前已分配的分区也存在于新的分配集合中
                if (partitions.contains(tp))
                    // 将其添加到 currentTopicPartitions 集合中
                    currentTopicPartitions.add(tp);
            }

            // 让 fetchBuffer 只保留 currentTopicPartitions 中的数据，丢弃其他分区的数据
            fetchBuffer.retainAll(currentTopicPartitions);

            // 分配更改事件将触发自动提交（如果已配置并且指定了组ID）。这是
            // 为了确保消费者正在取消订阅的主题分区的偏移量得到提交，因为之后将不会有重平衡。
            //
            // 有关处理此事件的更多详细信息，请参阅 ApplicationEventProcessor.process() 方法。
            // 向应用程序事件处理器添加并等待一个 AssignmentChangeEvent
            applicationEventHandler.addAndGet(new AssignmentChangeEvent(
                time.milliseconds(), // 事件发生的时间戳
                defaultApiTimeoutDeadlineMs(), // 默认API超时截止时间
                partitions // 新的分配分区集合
            ));
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * @author Trae
     * @date 2024/07/26
     * @description 取消订阅所有主题或模式，并取消分配所有分区。
     * 这会清除消费者之前通过 {@link #subscribe} 或 {@link #assign} 设置的所有状态。
     * 应用场景：当消费者不再需要消费任何消息，或者需要切换到新的订阅配置之前，可以调用此方法。
     * 设计考虑：此方法会清空 `fetchBuffer`，发送一个 `UnsubscribeEvent` 到事件处理器，
     * 并尝试处理后台事件队列中的事件，特别是忽略 `GroupAuthorizationException` 以确保取消订阅的成功。
     * 最后会重置组元数据。
     * @throws KafkaException 如果取消订阅过程中发生不可恢复的错误。
     */
    @Override
    public void unsubscribe() {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 清空 fetchBuffer，不保留任何分区的数据
            fetchBuffer.retainAll(Collections.emptySet());
            // 创建一个定时器，使用默认的 API 超时时间
            Timer timer = time.timer(defaultApiTimeoutMs);
            // 创建一个 UnsubscribeEvent，并计算其截止时间
            UnsubscribeEvent unsubscribeEvent = new UnsubscribeEvent(calculateDeadlineMs(timer));
            // 将 UnsubscribeEvent 添加到应用程序事件处理器
            applicationEventHandler.add(unsubscribeEvent);
            // 记录日志，表明正在取消订阅所有主题/模式以及已分配的分区
            log.info("Unsubscribing all topics or patterns and assigned partitions {}",
                    subscriptions.assignedPartitions());

            try {
                // 如果用户遇到致命错误，他们会在后台队列中收到一些异常。
                // 运行取消订阅时，应忽略这些异常，否则用户无法成功取消订阅。
                // 处理后台事件，等待取消订阅事件的 future 完成，忽略 GroupAuthorizationException
                processBackgroundEvents(unsubscribeEvent.future(), timer, e -> e instanceof GroupAuthorizationException);
                // 记录日志，表明已成功取消订阅所有主题/模式和已分配的分区
                log.info("Unsubscribed all topics or patterns and assigned partitions");
            } catch (TimeoutException e) {
                // 如果等待取消订阅事件完成时发生超时，记录错误日志
                log.error("Failed while waiting for the unsubscribe event to complete");
            }
            // 重置消费者组的元数据信息
            resetGroupMetadata();
        } catch (Exception e) {
            // 如果取消订阅过程中发生其他异常，记录错误日志并向上抛出
            log.error("Unsubscribe failed", e);
            throw e;
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 重置消费者组元数据。
     * <p>
     * 此方法用于在需要时（例如，消费者组ID或实例ID发生变化时）重新初始化消费者组的元数据。
     * 它会原子地更新 {@code groupMetadata} 字段，如果旧的元数据存在，则使用旧的组ID和组实例ID来初始化新的元数据。
     * <p>
     * 应用场景：
     * <ul>
     *     <li>当消费者重新加入一个组，或者其配置（如 group.id 或 group.instance.id）发生变更时，可能需要重置组元数据。</li>
     * </ul>
     * 设计考虑：
     * <ul>
     *     <li>使用 {@code AtomicReference#updateAndGet} 确保对 {@code groupMetadata} 的更新是原子性的，避免并发问题。</li>
     *     <li>通过映射旧的元数据来保留 groupId 和 groupInstanceId（如果存在），确保了元数据重置的一致性。</li>
     * </ul>
     */
    private void resetGroupMetadata() {
        // 原子地更新 groupMetadata
        groupMetadata.updateAndGet(
            // 接收一个旧的 Optional<ConsumerGroupMetadata> 作为输入
            oldGroupMetadataOptional -> oldGroupMetadataOptional
                // 如果旧的元数据存在，则对其进行映射
                .map(oldGroupMetadata -> initializeConsumerGroupMetadata(
                    // 使用旧元数据的 groupId 初始化新的消费者组元数据
                    oldGroupMetadata.groupId(),
                    // 使用旧元数据的 groupInstanceId 初始化新的消费者组元数据
                    oldGroupMetadata.groupInstanceId()
                ))
        );
    }

    // 仅用于测试，包可见性
    /**
     * 获取唤醒触发器。
     * <p>
     * 此方法主要用于测试目的，允许测试代码访问内部的 {@link WakeupTrigger} 实例，
     * 以便模拟唤醒事件或验证其状态。
     * <p>
     * 应用场景：
     * <ul>
     *     <li>单元测试中，需要检查 {@code WakeupTrigger} 是否被正确设置或清除。</li>
     *     <li>集成测试中，可能需要手动触发唤醒逻辑。</li>
     * </ul>
     * 设计考虑：
     * <ul>
     *     <li>将其可见性设置为包可见（默认），而不是 public，以限制其在生产代码中的直接访问，
     *         强调其主要用途是测试。</li>
     * </ul>
     * @return {@link WakeupTrigger} 实例
     */
    WakeupTrigger wakeupTrigger() {
        // 返回 wakeupTrigger 实例
        return wakeupTrigger;
    }

    /**
     * 轮询等待新的数据拉取。
     * <p>
     * 此方法是消费者获取数据的主要逻辑入口之一。它首先检查是否已有可用数据，如果有则立即返回。
     * 否则，它会发送新的拉取请求（如果需要），然后等待一段时间直到数据到达或超时。
     * <p>
     * 应用场景：
     * <ul>
     *     <li>在 {@link #poll(Duration)} 方法中被调用，用于实际执行数据拉取操作。</li>
     * </ul>
     * 设计考虑：
     * <ul>
     *     <li>超时管理：根据是否启用了提交偏移量管理来动态调整轮询超时时间。</li>
     *     <li>立即返回：如果本地缓冲区已有数据，则避免不必要的等待。</li>
     *     <li>发送拉取请求：确保在等待数据前，必要的拉取请求已被发送。</li>
     *     <li>避免卡死：如果某些分区的拉取位置未知（可能由于查找偏移量失败并处于退避状态），则会缩短轮询超时，避免长时间阻塞。</li>
     *     <li>唤醒机制：使用 {@link WakeupTrigger} 允许其他线程中断等待。</li>
     *     <li>定时器管理：使用内部的 {@code pollTimer} 来控制本次等待的超时，并更新外部传入的 {@code timer} 以反映消耗的时间。</li>
     * </ul>
     *
     * @param timer 整体轮询操作的计时器，用于控制总的超时时间。
     * @return 拉取到的数据 {@link Fetch}，可能为空。
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
        // 计算轮询超时时间
        long pollTimeout = isCommittedOffsetsManagementEnabled() // 检查是否启用了已提交偏移量的管理
                // 如果启用了，则取应用事件处理器允许的最大等待时间和计时器剩余时间的较小值
                ? Math.min(applicationEventHandler.maximumTimeToWait(), timer.remainingMs())
                // 如果未启用，则直接使用计时器的剩余时间
                : timer.remainingMs();

        // 如果数据已经可用，则立即返回
        // 尝试从缓冲区收集已拉取的数据
        final Fetch<K, V> fetch = collectFetch();
        // 如果收集到的数据不为空
        if (!fetch.isEmpty()) {
            // 直接返回已拉取的数据
            return fetch;
        }

        // 发送任何新的拉取请求（不会重新发送正在处理的拉取请求）
        sendFetches(timer);

        // 如果我们缺少某些位置信息（因为偏移量查找可能在失败后进行退避），
        // 我们不希望在轮询中卡住

        // 注意：使用 cachedSubscriptionHasAllFetchPositions 意味着我们必须在此方法之前调用 updateAssignmentMetadataIfNeeded。
        // 如果缓存的订阅状态表明并非所有分区都有拉取位置，并且计算出的轮询超时大于重试退避时间
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            // 将轮询超时设置为重试退避时间，以避免长时间等待那些可能无法立即获取数据的分区
            pollTimeout = retryBackoffMs;
        }

        // 记录追踪日志，说明正在以指定的超时时间轮询拉取数据
        log.trace("Polling for fetches with timeout {}", pollTimeout);

        // 创建用于本次轮询操作的内部计时器
        Timer pollTimer = time.timer(pollTimeout);
        // 设置唤醒触发器的任务为 fetchBuffer，当 fetchBuffer 被唤醒时，表示可能有新数据到达
        wakeupTrigger.setFetchAction(fetchBuffer);

        // 等待一段时间，以便一些拉取的数据到达，因为可能没有立即可用的数据。注意这里
        // 使用了一个更短的、专用的 "pollTimer"，它会更新 "timer"，以便调用方法 (poll)
        // 能够正确处理整体超时。
        try {
            // 等待 fetchBuffer 变为非空，或者 pollTimer 超时
            fetchBuffer.awaitNotEmpty(pollTimer);
        // 捕获中断异常
        } catch (InterruptException e) {
            // 记录追踪日志，说明在拉取过程中发生中断
            log.trace("Interrupt during fetch", e);
            // 重新抛出中断异常
            throw e;
        } finally {
            // 更新外部传入的计时器，减去本次轮询消耗的时间
            timer.update(pollTimer.currentTimeMs());
            // 清除唤醒触发器上设置的任务
            wakeupTrigger.clearTask();
        }

        // 再次尝试从缓冲区收集已拉取的数据并返回
        return collectFetch();
    }

    /**
     * 执行“{@link FetchCollector#collectFetch(FetchBuffer) 拉取收集}”步骤，通过从 {@link #fetchBuffer} 中读取原始数据，
     * 将其转换为格式正确的 {@link CompletedFetch}，验证它和内部 {@link SubscriptionState 状态} 是否正确，
     * 然后将其全部转换为 {@link Fetch} 以供返回。
     *
     * <p/>
     *
     * 此方法将在返回前 {@link ConsumerNetworkThread#wakeup() 唤醒网络线程}。这样做是为了优化，
     * 以便可以<em>预取下一轮数据</em>。
     * <p>
     * 应用场景：
     * <ul>
     *     <li>在 {@link #pollForFetches(Timer)} 方法中，当数据可能已到达或等待超时后调用，用于整理和返回拉取到的数据。</li>
     *     <li>在 {@link #pollForFetches(Timer)} 方法开始时也会调用，以检查是否已有立即可用的数据。</li>
     * </ul>
     * 设计考虑：
     * <ul>
     *     <li>数据转换与校验：封装了从原始缓冲区数据到结构化 {@link Fetch} 对象的转换逻辑，包括校验。</li>
     *     <li>预取优化：在返回当前批次数据之前，主动唤醒网络线程，使其可以开始拉取下一批数据，从而提高吞吐量和减少延迟。</li>
     * </ul>
     *
     * @return 从缓冲区收集并处理后的 {@link Fetch} 对象，其中包含拉取到的消费者记录。
     */
    private Fetch<K, V> collectFetch() {
        // 调用 fetchCollector 从 fetchBuffer 中收集拉取的数据
        final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);

        // 通知网络线程唤醒并开始下一轮的拉取。
        // 这是一个优化措施，目的是让网络线程可以提前开始准备下一批数据，
        // 从而在应用线程处理完当前数据后，能够更快地获取到新数据。
        applicationEventHandler.wakeupNetworkThread();

        // 返回收集到的拉取数据
        return fetch;
    }

    /**
     * 将拉取位置设置为已提交的位置（如果存在），或者使用用户配置的偏移量重置策略进行重置。
     * <p>
     * 此方法负责确保消费者从正确的位置开始拉取数据。它会尝试获取已提交的偏移量，
     * 如果找不到，则会根据 {@code auto.offset.reset} 配置（如 earliest 或 latest）来确定起始偏移量。
     * <p>
     * 应用场景：
     * <ul>
     *     <li>消费者启动或分区重新分配后，需要确定从哪里开始读取数据。</li>
     *     <li>当调用 {@code seekToBeginning} 或 {@code seekToEnd} 后，虽然偏移量已设置，但此方法可能仍被间接调用以验证或更新元数据。</li>
     * </ul>
     * 设计考虑：
     * <ul>
     *     <li>异步执行：通过向 {@link ApplicationEventHandler}提交一个 {@link CheckAndUpdatePositionsEvent} 事件来异步执行此操作，
     *         避免阻塞应用主线程。</li>
     *     <li>超时控制：操作受传入的 {@code timer} 控制，如果超时则认为操作失败。</li>
     *     <li>状态更新：操作成功后会更新 {@code cachedSubscriptionHasAllFetchPositions} 标志。</li>
     *     <li>异常处理：方法声明可能抛出 {@link AuthenticationException} 和 {@link NoOffsetForPartitionException}，
     *         尽管在当前异步实现中，这些异常更可能通过事件的 Future 返回。</li>
     * </ul>
     *
     * @param timer 用于控制操作超时的计时器。
     * @return 如果操作在没有超时的情况下完成，则返回 {@code true}；否则返回 {@code false}。
     * @throws AuthenticationException 如果身份验证失败。有关更多详细信息，请参阅异常说明。
     * @throws NoOffsetForPartitionException 如果对于给定的分区没有存储偏移量，并且没有定义偏移量重置策略。
     */
    private boolean updateFetchPositions(final Timer timer) {
        // 首先将缓存的标志位设置为 false，表示当前不确定所有订阅分区是否都有有效的拉取位置
        cachedSubscriptionHasAllFetchPositions = false;
        try {
            // 创建一个检查并更新位置的事件，设置其截止时间
            CheckAndUpdatePositionsEvent checkAndUpdatePositionsEvent = new CheckAndUpdatePositionsEvent(calculateDeadlineMs(timer));
            // 将此事件的 Future 设置为唤醒触发器的活动任务，以便在事件完成或被唤醒时得到通知
            wakeupTrigger.setActiveTask(checkAndUpdatePositionsEvent.future());
            // 将事件添加到应用事件处理器并等待其完成，返回值将更新 cachedSubscriptionHasAllFetchPositions
            // addAndGet 会阻塞直到事件处理完成或超时
            cachedSubscriptionHasAllFetchPositions = applicationEventHandler.addAndGet(checkAndUpdatePositionsEvent);
        // 如果在等待事件完成时发生超时
        } catch (TimeoutException e) {
            // 操作超时，返回 false
            return false;
        } finally {
            // 无论成功还是失败，最终都要清除唤醒触发器上的任务
            wakeupTrigger.clearTask();
        }
        // 如果事件成功完成（没有抛出 TimeoutException），则返回 true
        return true;
    }

    /**
     * 指示消费者是否正在使用基于 Kafka 的偏移量管理策略，
     * 根据配置 {@link CommonClientConfigs#GROUP_ID_CONFIG}
     * 
     * @return 如果启用了提交偏移量管理，则返回 true；否则返回 false。
     *         此方法检查 groupMetadata 是否存在，如果存在，则表示消费者配置了 group.id，
     *         因此会使用 Kafka 的偏移量管理机制。
     * 应用场景：在进行偏移量提交或获取已提交偏移量等操作前，判断是否应使用 Kafka 的偏移量管理。
     * 设计考虑：通过检查 groupMetadata 的存在性来确定是否启用偏移量管理，这是一种简单且直接的方式。
     */
    private boolean isCommittedOffsetsManagementEnabled() {
        // 检查 groupMetadata 是否存在（即 group.id 是否已配置）
        return groupMetadata.get().isPresent();
    }

    /**
     * 此方法通知后台线程{@link CreateFetchRequestsEvent 创建拉取请求}。
     *
     * <p/>
     *
     * 此方法采取以下步骤以保持与{@link ClassicKafkaConsumer}中同名方法的兼容性：
     *
     * <ul>
     *     <li>
     *         该方法将等待请求创建的确认，然后继续。
     *     </li>
     *     <li>
     *         该方法会将请求创建过程中遇到的异常<b>立即</b>抛给用户。
     *     </li>
     *     <li>
     *         该方法将抑制等待确认时发生的{@link TimeoutException 超时异常}。
     *         请求创建过程中的超时是此消费者线程通信机制的副产品。
     *         在{@link ClassicKafkaConsumer}的请求创建步骤中不会抛出该异常类型。
     *         此外，超时不会影响{@link #pollForFetches(Timer) 阻塞请求}的逻辑，
     *         因为它可以处理超时后创建的请求。
     *     </li>
     * </ul>
     *
     * @param timer 定时器，用于限制消费者等待请求创建的时间，
     *              在实践中用于避免使用{@link Long#MAX_VALUE}进行“永久”等待。
     * 应用场景：在需要主动触发数据拉取时调用，例如在 poll 方法中发现没有足够数据时。
     * 设计考虑：通过 applicationEventHandler 异步发送创建拉取请求的事件给后台线程处理，
     *         同时通过 addAndGet 方法等待后台线程处理完成的确认，以确保请求被及时处理。
     *         对 TimeoutException 的特殊处理是为了兼容 ClassicKafkaConsumer 的行为，并避免不必要的异常中断。
     */
    private void sendFetches(Timer timer) {
        try {
            // 向应用事件处理器添加一个创建拉取请求的事件，并等待其完成
            // calculateDeadlineMs(timer) 计算事件处理的截止时间
            applicationEventHandler.addAndGet(new CreateFetchRequestsEvent(calculateDeadlineMs(timer)));
        } catch (TimeoutException e) {
            // 根据上述注释，可以忽略超时异常
            // 这是因为超时是线程通信的副产品，并且不会影响后续的拉取逻辑
        }
    }

    /**
     * 此方法通知后台线程为预取情况{@link CreateFetchRequestsEvent 创建拉取请求}，
     * 即在{@link #poll(Duration)}退出之前。在预取情况下，应用程序线程
     * 在继续之前不会等待请求创建的确认。
     *
     * <p/>
     *
     * 调用此方法时，{@link KafkaConsumer#poll(Duration)}已有数据准备好返回给用户，
     * 这意味着消费位置已经更新。为了防止记录中出现潜在的间隙，
     * 此方法旨在抑制所有异常。
     *
     * @param timer 为事件及其{@link CompletableFuture future}提供上限。
     * 应用场景：在 poll 方法即将返回数据给用户之前，预先触发下一次数据拉取，以提高吞吐量和减少延迟。
     * 设计考虑：与 sendFetches 不同，sendPrefetches 使用 add 方法异步发送事件，不等待后台线程的确认。
     *         这是因为预取操作的优先级较低，不应阻塞当前 poll 操作的完成。
     *         同时，为了防止预取过程中的异常影响主流程，所有异常都会被捕获并记录，但不会向上抛出。
     */
    private void sendPrefetches(Timer timer) {
        try {
            // 向应用事件处理器添加一个创建拉取请求的事件，不等待其完成
            // calculateDeadlineMs(timer) 计算事件处理的截止时间
            applicationEventHandler.add(new CreateFetchRequestsEvent(calculateDeadlineMs(timer)));
        } catch (Throwable t) {
            // 任何意外错误都将被记录以进行故障排除，但不会抛出。
            // 这是为了确保预取过程中的问题不会中断正常的消费流程。
            log.warn("在 Consumer.poll() 中预取数据时发生意外错误，但已被抑制", t);
        }
    }

    /**
     * 如果需要，更新分配元数据。
     * 此方法执行以下操作：
     * 1. 执行待处理的偏移量提交回调。
     * 2. 如果消费者订阅了主题模式，则向后台线程发送一个事件以更新模式订阅，并等待其完成。
     * 3. 处理后台事件队列中的事件。
     * 4. 更新拉取位置。
     *
     * @param timer 定时器，用于限制操作的持续时间。
     * @return 如果成功更新了拉取位置，则返回 true；否则返回 false。
     * 应用场景：在每次 poll 操作之前调用，以确保消费者的元数据（如分区分配、订阅模式的解析结果）是最新的，
     *         并且所有待处理的回调都已执行。
     * 设计考虑：这是一个关键的协调方法，确保消费者状态与集群状态同步。
     *         对于模式订阅，需要与后台线程同步更新，以确保正确解析和分配分区。
     *         处理后台事件是为了及时响应网络线程中发生的错误或需要应用线程处理的回调。
     */
    @Override
    public boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        // 执行所有待处理的偏移量提交回调
        offsetCommitCallbackInvoker.executeCallbacks();
        // 检查当前订阅是否为模式订阅（例如，使用正则表达式）
        if (subscriptions.hasPatternSubscription()) {
            try {
                // 如果是模式订阅，向应用事件处理器发送一个更新模式订阅的事件，并等待其完成
                // 这会触发后台线程重新评估匹配模式的主题，并可能更新分区分配
                applicationEventHandler.addAndGet(new UpdatePatternSubscriptionEvent(calculateDeadlineMs(timer)));
            } catch (TimeoutException e) {
                // 如果更新模式订阅超时，则返回 false，表示元数据更新未成功
                return false;
            } finally {
                // 无论成功还是失败，都更新计时器
                timer.update();
            }
        }
        // 处理后台线程产生的事件，例如错误或需要主线程执行的回调
        processBackgroundEvents();

        // 更新拉取位置，这可能涉及到重置偏移量或处理日志截断
        return updateFetchPositions(timer);
    }

    /**
     * 订阅给定的主题集合以进行消费。
     * 如果先前已订阅了其他主题或模式，此调用将替换先前的订阅。
     *
     * @param topics 要订阅的主题名称集合，不能为空或包含 null 或空字符串。
     * @throws IllegalArgumentException 如果 topics 为 null 或为空，或者包含 null 或空字符串。
     * 应用场景：当消费者需要开始消费一组明确指定的主题时使用。
     * 设计考虑：这是一个便捷方法，内部调用 subscribeInternal 并传入一个空的 ConsumerRebalanceListener。
     */
    @Override
    public void subscribe(Collection<String> topics) {
        // 内部调用，不带 ConsumerRebalanceListener
        subscribeInternal(topics, Optional.empty());
    }

    /**
     * 订阅给定的主题集合以进行消费，并使用自定义的 {@link ConsumerRebalanceListener 再平衡监听器}。
     * 如果先前已订阅了其他主题或模式，此调用将替换先前的订阅。
     *
     * @param topics 要订阅的主题名称集合，不能为空或包含 null 或空字符串。
     * @param listener 用户提供的 {@link ConsumerRebalanceListener 再平衡监听器}，用于在分区分配发生变化时执行自定义逻辑，不能为空。
     * @throws IllegalArgumentException 如果 topics 为 null 或为空，或者包含 null 或空字符串，或者 listener 为 null。
     * 应用场景：当消费者需要订阅一组明确指定的主题，并且需要在分区分配或撤销时执行特定操作（如状态清理、偏移量管理）时使用。
     * 设计考虑：允许用户传入自定义的再平衡监听器，提供了在分区生命周期关键点进行干预的能力。
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        // 检查监听器是否为 null
        if (listener == null)
            // 如果监听器为 null，则抛出 IllegalArgumentException
            throw new IllegalArgumentException("RebalanceListener 不能为空");

        // 内部调用，并传入提供的 ConsumerRebalanceListener
        subscribeInternal(topics, Optional.of(listener));
    }

    /**
     * 订阅与给定正则表达式模式匹配的所有主题。
     * 如果先前已订阅了其他主题或模式，此调用将替换先前的订阅。
     *
     * @param pattern 用于匹配要订阅的主题的正则表达式模式，不能为空。
     * @throws IllegalArgumentException 如果 pattern 为 null。
     * 应用场景：当消费者需要动态地消费符合特定命名规则的主题时使用，例如，所有以 "log-" 开头的主题。
     * 设计考虑：这是一个便捷方法，内部调用 subscribeInternal 并传入一个空的 ConsumerRebalanceListener。
     *         模式匹配的实际主题列表会在后台定期刷新。
     */
    @Override
    public void subscribe(Pattern pattern) {
        // 内部调用，不带 ConsumerRebalanceListener
        subscribeInternal(pattern, Optional.empty());
    }

    /**
     * 使用 {@link SubscriptionPattern} 订阅与给定正则表达式模式匹配的所有主题，并使用自定义的 {@link ConsumerRebalanceListener 再平衡监听器}。
     * 如果先前已订阅了其他主题或模式，此调用将替换先前的订阅。
     * {@link SubscriptionPattern} 是一个包装了 {@link Pattern} 的类，用于支持未来的扩展，例如 re2j 模式。
     *
     * @param pattern {@link SubscriptionPattern} 实例，包含用于匹配要订阅的主题的正则表达式模式，不能为空。
     * @param listener 用户提供的 {@link ConsumerRebalanceListener 再平衡监听器}，用于在分区分配发生变化时执行自定义逻辑，不能为空。
     * @throws IllegalArgumentException 如果 pattern 或 listener 为 null。
     * 应用场景：与 `subscribe(Pattern, ConsumerRebalanceListener)` 类似，但使用 `SubscriptionPattern` 作为参数，
     *         这通常是为了内部使用或支持更高级的模式匹配选项。
     * 设计考虑：此方法委托给 `subscribeToRegex`，表明其主要处理基于正则表达式的模式订阅。
     */
    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener) {
        // 检查监听器是否为 null
        if (listener == null)
            // 如果监听器为 null，则抛出 IllegalArgumentException
            throw new IllegalArgumentException("RebalanceListener 不能为空");
        // 调用 subscribeToRegex 方法处理基于正则表达式的订阅，并传入提供的监听器
        subscribeToRegex(pattern, Optional.of(listener));
    }

    /**
     * 使用 {@link SubscriptionPattern} 订阅与给定正则表达式模式匹配的所有主题。
     * 如果先前已订阅了其他主题或模式，此调用将替换先前的订阅。
     * {@link SubscriptionPattern} 是一个包装了 {@link Pattern} 的类，用于支持未来的扩展，例如 re2j 模式。
     *
     * @param pattern {@link SubscriptionPattern} 实例，包含用于匹配要订阅的主题的正则表达式模式，不能为空。
     * @throws IllegalArgumentException 如果 pattern 为 null。
     * 应用场景：与 `subscribe(Pattern)` 类似，但使用 `SubscriptionPattern` 作为参数。
     * 设计考虑：此方法委托给 `subscribeToRegex`，不带 ConsumerRebalanceListener。
     */
    @Override
    public void subscribe(SubscriptionPattern pattern) {
        // 调用 subscribeToRegex 方法处理基于正则表达式的订阅，不带 ConsumerRebalanceListener
        subscribeToRegex(pattern, Optional.empty());
    }

    /**
     * 订阅与给定正则表达式模式匹配的所有主题，并使用自定义的 {@link ConsumerRebalanceListener 再平衡监听器}。
     * 如果先前已订阅了其他主题或模式，此调用将替换先前的订阅。
     *
     * @param pattern 用于匹配要订阅的主题的正则表达式模式，不能为空。
     * @param listener 用户提供的 {@link ConsumerRebalanceListener 再平衡监听器}，用于在分区分配发生变化时执行自定义逻辑，不能为空。
     * @throws IllegalArgumentException 如果 pattern 或 listener 为 null。
     * 应用场景：当消费者需要动态地消费符合特定命名规则的主题，并且需要在分区分配或撤销时执行特定操作时使用。
     * 设计考虑：允许用户传入自定义的再平衡监听器，提供了在分区生命周期关键点进行干预的能力。
     *         模式匹配的实际主题列表会在后台定期刷新。
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        // 检查监听器是否为 null
        if (listener == null)
            // 如果监听器为 null，则抛出 IllegalArgumentException
            throw new IllegalArgumentException("RebalanceListener 不能为空");

        // 内部调用，并传入提供的 ConsumerRebalanceListener
        subscribeInternal(pattern, Optional.of(listener));
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        // 获取锁，确保在关闭过程中其他线程不能修改消费者状态
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * 获取保护此消费者免受多线程访问的轻量级锁。然而，当锁不可用时，我们不会阻塞，
     * 而是直接抛出异常（因为不支持多线程使用）。
     * 
     * 应用场景：在每个公共API方法开始时调用，以确保线程安全。
     * 实现细节：使用AtomicLong currentThread来跟踪当前持有锁的线程ID，使用AtomicInteger refCount来支持可重入。
     * 设计考虑：选择抛出异常而不是阻塞是为了明确指出KafkaConsumer不是线程安全的，并防止潜在的死锁或不一致状态。
     *
     * @throws ConcurrentModificationException 如果另一个线程已经持有锁
     */
    private void acquire() {
        // 获取当前线程对象
        final Thread thread = Thread.currentThread();
        // 获取当前线程的ID
        final long threadId = thread.getId();
        // 检查当前线程是否是已经持有锁的线程，如果不是，并且无法通过CAS操作将currentThread设置为当前线程ID（表示锁已被其他线程持有）
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            // 抛出并发修改异常，指示KafkaConsumer不适用于多线程访问
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                " otherThread(id: " + currentThread.get() + ")"
            );
        // 增加引用计数，支持锁的可重入
        refCount.incrementAndGet();
    }

    /**
     * 释放保护消费者免受多线程访问的轻量级锁。
     * 
     * 应用场景：在每个公共API方法结束时（通常在finally块中）调用，以释放锁。
     * 实现细节：减少引用计数，如果计数器归零，则将currentThread重置为NO_CURRENT_THREAD，表示锁已完全释放。
     * 设计考虑：确保锁在操作完成后总是被释放，即使发生异常。
     */
    private void release() {
        // 减少引用计数，如果引用计数变为0
        if (refCount.decrementAndGet() == 0)
            // 将当前持有锁的线程ID设置为NO_CURRENT_THREAD，表示锁已释放
            currentThread.set(NO_CURRENT_THREAD);
    }

    /**
     * 内部订阅方法，用于通过正则表达式模式订阅主题。
     * 
     * 应用场景：当用户调用 `subscribe(Pattern, ConsumerRebalanceListener)` 时，此方法被间接调用。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 检查 group.id 是否有效。
     * 3. 验证传入的 Pattern 对象不为 null 且其字符串表示不为空。
     * 4. 记录订阅日志。
     * 5. 创建一个 {@link TopicPatternSubscriptionChangeEvent} 并将其添加到应用事件处理器队列中，
     *    该事件将在后台网络线程中处理，以更新消费者的订阅状态并通知协调器。
     * 设计考虑：
     * - 将实际的订阅逻辑（与协调器交互）异步化到网络线程，避免阻塞应用线程。
     * - 使用 Optional<ConsumerRebalanceListener> 来处理可选的重平衡监听器。
     * - 在 finally 块中释放锁，确保锁总是被释放。
     *
     * @param pattern 用于匹配主题名称的正则表达式模式，不能为空。
     * @param listener 可选的消费者重平衡监听器，用于在分区分配发生变化时接收通知。
     * @throws IllegalArgumentException 如果主题模式为 null 或为空。
     * @throws InvalidGroupIdException 如果配置了无效的 group.id。
     */
    private void subscribeInternal(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查并可能抛出无效的 group.id 异常
            maybeThrowInvalidGroupIdException();
            // 如果模式为null或模式的字符串表示为空
            if (pattern == null || pattern.toString().isEmpty())
                // 抛出非法参数异常
                throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));
            // 记录日志，表明已订阅到指定的模式
            log.info("Subscribed to pattern: '{}'", pattern);
            // 向应用事件处理器添加一个主题模式订阅变更事件
            applicationEventHandler.addAndGet(new TopicPatternSubscriptionChangeEvent(
                pattern, // 订阅的模式
                listener, // 重平衡监听器
                defaultApiTimeoutDeadlineMs() // 默认API超时截止时间
            ));
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 订阅 RE2/J 模式。这将生成一个事件来更新订阅状态中的模式，
     * 因此它将包含在发送给代理的下一个心跳请求中。
     * 客户端不执行模式验证（除了 null/空检查）。
     * 
     * 应用场景：当用户使用支持 RE2/J 语法的正则表达式进行订阅时调用。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 检查 group.id 是否有效。
     * 3. 验证传入的 SubscriptionPattern 对象及其内部模式不为 null 或空。
     * 4. 记录订阅日志。
     * 5. 创建一个 {@link TopicRe2JPatternSubscriptionChangeEvent} 并将其添加到应用事件处理器队列中，
     *    该事件将在后台网络线程中处理。
     * 设计考虑：与 `subscribeInternal(Pattern, ...)` 类似，将网络操作异步化，并确保资源正确释放。
     *           此方法专门处理 `SubscriptionPattern` 类型的参数，可能用于更高级或特定类型的模式匹配。
     *
     * @param pattern 要订阅的 {@link SubscriptionPattern RE2/J 模式对象}，不能为空，其内部模式也不能为空。
     * @param listener 可选的消费者重平衡监听器。
     * @throws IllegalArgumentException 如果订阅模式或其内部模式为 null 或为空。
     * @throws InvalidGroupIdException 如果配置了无效的 group.id。
     */
    private void subscribeToRegex(SubscriptionPattern pattern,
                                  Optional<ConsumerRebalanceListener> listener) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查并可能抛出无效的 group.id 异常
            maybeThrowInvalidGroupIdException();
            // 如果订阅模式无效，则抛出异常
            throwIfSubscriptionPatternIsInvalid(pattern);
            // 记录日志，表明正在订阅到指定的正则表达式
            log.info("Subscribing to regular expression {}", pattern);
            // 向应用事件处理器添加一个 RE2/J 主题模式订阅变更事件
            applicationEventHandler.addAndGet(new TopicRe2JPatternSubscriptionChangeEvent(
                pattern, // 订阅的 RE2/J 模式
                listener, // 重平衡监听器
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)) // 计算的API超时截止时间
            ));
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 检查提供的 {@link SubscriptionPattern} 是否有效，如果无效则抛出 {@link IllegalArgumentException}。
     * 
     * 应用场景：在尝试使用 {@link SubscriptionPattern} 进行订阅之前，用于验证输入。
     * 实现细节：检查 {@link SubscriptionPattern} 对象本身是否为 null，以及其内部的模式字符串是否为空。
     * 设计考虑：这是一个辅助方法，用于封装通用的验证逻辑，使订阅方法的代码更简洁。
     *
     * @param subscriptionPattern 要验证的订阅模式。
     * @throws IllegalArgumentException 如果订阅模式为 null 或其内部模式为空。
     */
    private void throwIfSubscriptionPatternIsInvalid(SubscriptionPattern subscriptionPattern) {
        // 如果订阅模式为null
        if (subscriptionPattern == null) {
            // 抛出非法参数异常，指示主题订阅模式不能为空
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be null");
        }
        // 如果订阅模式的内部模式字符串为空
        if (subscriptionPattern.pattern().isEmpty()) {
            // 抛出非法参数异常，指示主题订阅模式不能为空字符串
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be empty");
        }
    }

    /**
     * 内部订阅方法，用于通过主题名称集合订阅主题。
     * 
     * 应用场景：当用户调用 `subscribe(Collection<String>, ConsumerRebalanceListener)` 时，此方法被间接调用。
     * 实现细节：
     * 1. 获取锁并确保消费者处于打开状态。
     * 2. 检查 group.id 是否有效。
     * 3. 验证主题集合不为 null。如果集合为空，则视为取消订阅操作。
     * 4. 遍历主题集合，确保每个主题名称都不是 null 或空字符串。
     * 5. 清理 fetchBuffer 中不属于新订阅主题的数据：
     *    a. 创建一个 currentTopicPartitions 集合，用于存放当前已分配分区中属于新订阅主题的分区。
     *    b. 遍历当前已分配的分区 (subscriptions.assignedPartitions())，如果分区的主题在新订阅的主题列表中，则将其添加到 currentTopicPartitions。
     *    c. 调用 fetchBuffer.retainAll(currentTopicPartitions) 来保留这些分区的数据，并丢弃其他分区的数据。
     * 6. 记录订阅日志。
     * 7. 创建一个 {@link TopicSubscriptionChangeEvent} 并将其添加到应用事件处理器队列中，
     *    该事件将在后台网络线程中处理，以更新消费者的订阅状态并通知协调器。
     * 设计考虑：
     * - 将实际的订阅逻辑异步化到网络线程。
     * - 处理空主题列表作为取消订阅的快捷方式。
     * - 在订阅新主题时，主动清理不再相关的已缓冲数据，以节省内存并避免处理过时数据。
     * - 在 finally 块中释放锁。
     *
     * @param topics 要订阅的主题名称集合，不能为空。如果为空集合，则行为等同于取消订阅。
     * @param listener 可选的消费者重平衡监听器。
     * @throws IllegalArgumentException 如果主题集合为 null，或包含 null 或空的主题名称。
     * @throws InvalidGroupIdException 如果配置了无效的 group.id。
     */
    private void subscribeInternal(Collection<String> topics, Optional<ConsumerRebalanceListener> listener) {
        // 获取锁并确保消费者处于打开状态
        acquireAndEnsureOpen();
        try {
            // 检查并可能抛出无效的 group.id 异常
            maybeThrowInvalidGroupIdException();
            // 如果主题集合为null
            if (topics == null)
                // 抛出非法参数异常
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            // 如果主题集合为空
            if (topics.isEmpty()) {
                // 将订阅空主题列表视为取消订阅
                unsubscribe();
            } else {
                // 遍历主题集合中的每个主题
                for (String topic : topics) {
                    // 如果主题字符串为空白（null或仅包含空格）
                    if (isBlank(topic))
                        // 抛出非法参数异常，指示主题集合不能包含null或空主题
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }

                // 清理不属于新分配主题的缓冲数据
                // 创建一个用于存储当前有效主题分区的集合
                final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

                // 遍历当前订阅状态中已分配的所有分区
                for (TopicPartition tp : subscriptions.assignedPartitions()) {
                    // 如果当前分区的主题包含在要订阅的主题列表中
                    if (topics.contains(tp.topic()))
                        // 将该分区添加到当前有效主题分区集合中
                        currentTopicPartitions.add(tp);
                }

                // 保留 fetchBuffer 中属于 currentTopicPartitions 的数据，移除其他的
                fetchBuffer.retainAll(currentTopicPartitions);
                // 记录日志，表明已订阅到指定的主题列表
                log.info("Subscribed to topic(s): {}", String.join(", ", topics));
                // 向应用事件处理器添加一个主题订阅变更事件
                applicationEventHandler.addAndGet(new TopicSubscriptionChangeEvent(
                    new HashSet<>(topics), // 要订阅的主题集合的副本
                    listener, // 重平衡监听器
                    defaultApiTimeoutDeadlineMs() // 默认API超时截止时间
                ));
            }
        } finally {
            // 释放锁
            release();
        }
    }

    /**
     * 处理由 {@link ConsumerNetworkThread 网络线程} 生成的事件（如果有）。
     * 在处理事件时可能会发生 {@link ErrorEvent 错误}。
     * 在这种情况下，处理器将获取对第一个错误的引用，继续处理剩余事件，然后抛出发生的第一个错误。
     *
     * 应用场景：在每次 poll 操作或其他可能与网络线程交互的操作（如 commitSync）之前或之后调用，
     * 以确保应用线程能够及时响应网络线程产生的事件，例如错误或需要执行的回调。
     * 实现细节：
     * 1. 从 backgroundEventHandler 中取出所有待处理的后台事件。
     * 2. 如果事件列表不为空：
     *    a. 记录事件在队列中的等待时间。
     *    b. 遍历每个事件：
     *       i. 如果事件是 CompletableEvent 的实例，则将其添加到 backgroundEventReaper 中，以便后续清理。
     *       ii. 调用 backgroundEventProcessor.process(event) 来实际处理事件。
     *       iii. 如果处理过程中发生异常，将其包装为 KafkaException，并尝试设置到 firstError。
     *            如果 firstError 已被设置，则记录警告日志。
     *    c. 记录整个事件队列的处理时间。
     * 3. 调用 backgroundEventReaper.reap() 来清理已完成的 CompletableEvent。
     * 4. 如果 firstError 中捕获到了异常，则抛出该异常。
     * 设计考虑：
     * - 错误处理机制：即使在处理某个事件时出错，也会尝试处理完所有事件，然后才抛出第一个遇到的错误，
     *   这样可以确保尽可能多的事件得到处理，并且应用线程能够感知到最重要的错误。
     * - 度量收集：记录事件在队列中的等待时间和处理时间，有助于监控和诊断性能问题。
     * - CompletableEvent 清理：使用 EventReaper 模式来管理和清理那些代表异步操作完成的事件。
     * - 线程模型：此方法在应用线程中执行，处理来自网络线程的事件，是两个线程间通信的关键部分。
     *
     * @return 如果处理了任何事件，则返回 {@code true}；否则返回 {@code false}。
     * @throws KafkaException 如果在处理任何后台事件时发生错误。
     * Visible for testing. // 注释：此方法对测试可见
     */
    boolean processBackgroundEvents() {
        // 用于存储处理事件过程中发生的第一个Kafka异常
        AtomicReference<KafkaException> firstError = new AtomicReference<>();

        // 从后台事件处理器中取出所有待处理的事件
        List<BackgroundEvent> events = backgroundEventHandler.drainEvents();
        // 如果事件列表不为空
        if (!events.isEmpty()) {
            // 记录开始处理事件的时间戳
            long startMs = time.milliseconds();
            // 遍历所有取出的后台事件
            for (BackgroundEvent event : events) {
                // 记录后台事件在队列中的等待时间
                kafkaConsumerMetrics.recordBackgroundEventQueueTime(time.milliseconds() - event.enqueuedMs());
                try {
                    // 如果事件是 CompletableEvent 的实例（表示一个可完成的事件，通常关联一个Future）
                    if (event instanceof CompletableEvent)
                        // 将其添加到后台事件收割者中，用于后续检查完成状态和清理
                        backgroundEventReaper.add((CompletableEvent<?>) event);

                    // 使用后台事件处理器处理当前事件
                    backgroundEventProcessor.process(event);
                } catch (Throwable t) {
                    // 如果处理事件时发生任何类型的异常
                    // 尝试将异常包装成 KafkaException
                    KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);

                    // 尝试原子地设置 firstError 为当前异常 e，如果 firstError 当前为 null
                    // 如果设置失败（表示 firstError 已被其他异常设置），则记录警告日志
                    if (!firstError.compareAndSet(null, e))
                        log.warn("An error occurred when processing the background event: {}", e.getMessage(), e);
                }
            }
            // 记录后台事件队列的总处理时间
            kafkaConsumerMetrics.recordBackgroundEventQueueProcessingTime(time.milliseconds() - startMs);
        }

        // 调用后台事件收割者的 reap 方法，检查并处理已完成的 CompletableEvent
        backgroundEventReaper.reap(time.milliseconds());

        // 如果在处理事件过程中捕获到了错误
        if (firstError.get() != null)
            // 抛出捕获到的第一个 KafkaException
            throw firstError.get();

        // 返回事件列表是否为空的逆（即，如果处理了事件则返回true）
        return !events.isEmpty();
    }

    /**
     * 此方法用于处理调用者既需要阻塞等待事件完成，又需要处理后台事件的场景。
     * 对于某些事件，为了完全处理相关逻辑，{@link ConsumerNetworkThread 后台线程} 需要应用程序线程的协助才能完成。
     * 如果应用程序线程在提交事件后简单地阻塞等待，处理过程将会死锁。
     * 此处的逻辑基本上是一个循环，在每次迭代中执行两个任务：
     *
     * <ol>
     *     <li>处理后台事件（如果存在）</li>
     *     <li><em>短暂</em>等待 {@link CompletableApplicationEvent 事件} 完成</li>
     * </ol>
     *
     * <p/>
     *
     * 每次迭代都为应用程序线程提供了处理后台事件的机会，这对于完成整个处理过程可能是必需的。
     *
     * <p/>
     *
     * 以 {@link #unsubscribe()} 为例。要开始取消订阅，应用程序线程会将一个 {@link UnsubscribeEvent} 入队到应用程序事件队列中。
     * 该事件最终会触发后台线程中的重新平衡逻辑。关键在于，作为此重新平衡工作的一部分，
     * 对于消费者拥有的任何分区，都需要调用 {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} 回调。
     * 但是，此回调必须在应用程序线程上执行。为实现这一点，后台线程会将一个
     * {@link ConsumerRebalanceListenerCallbackNeededEvent} 入队到其后台事件队列中。应用程序线程会定期查询该事件队列，
     * 以查看是否有工作要做。当应用程序线程看到 {@link ConsumerRebalanceListenerCallbackNeededEvent} 时，会对其进行处理，
     * 然后应用程序线程会将一个 {@link ConsumerRebalanceListenerCallbackCompletedEvent} 入队到应用程序事件队列中。
     * 稍后，后台线程将看到该事件，对其进行处理，并继续执行重新平衡逻辑。在执行
     * {@link ConsumerRebalanceListener} 回调之前，重新平衡逻辑无法完成。
     *
     * @param future                    包含 {@link CompletableFuture} 的事件；应用程序线程将在此 future 上等待完成
     * @param timer                     限制等待事件完成时间的总体计时器
     * @param ignoreErrorEventException 用于忽略后台错误的断言。
     *                                  在处理后台事件时发现的任何与断言匹配的异常都不会被传播。
     * @return 如果事件在超时时间内完成，则返回 {@code true}，否则返回 {@code false}
     * @param <T> future 的结果类型
     */
    // 仅用于测试
    <T> T processBackgroundEvents(Future<T> future, Timer timer, Predicate<Exception> ignoreErrorEventException) { // 处理后台事件，同时等待指定的 Future 完成
        do { // 循环直到计时器过期
            boolean hadEvents = false; // 标记本次迭代是否处理了后台事件
            try {
                hadEvents = processBackgroundEvents(); // 调用另一个 processBackgroundEvents 方法处理实际的后台事件
            } catch (Exception e) { // 捕获处理后台事件时可能发生的异常
                if (!ignoreErrorEventException.test(e)) // 如果异常不应被忽略
                    throw e; // 重新抛出异常
            }

            try {
                if (future.isDone()) { // 检查目标 Future 是否已经完成
                    // 如果事件已完成（无论是成功还是其他方式），则尝试返回
                    // 而无需等待。我们在这里使用 ConsumerUtils.getResult() 方法来处理
                    // 异常类型的转换。
                    return ConsumerUtils.getResult(future); // 获取 Future 的结果并返回
                } else if (!hadEvents) { // 如果 Future 未完成且本次迭代没有处理后台事件
                    // 如果上述处理没有产生任何事件，那么让我们稍等片刻，以便
                    // 后台线程完成任务，或者用我们下一个循环中要处理的事情填充后台事件队列。
                    Timer pollInterval = time.timer(100L); // 创建一个 100 毫秒的轮询间隔计时器
                    return ConsumerUtils.getResult(future, pollInterval); // 在轮询间隔内等待 Future 完成并返回结果
                }
            } catch (TimeoutException e) { // 捕获等待 Future 结果时的超时异常
                // 忽略此异常，因为我们将重试该事件直到超时到期。
            } finally {
                timer.update(); // 更新总体计时器
            }
        } while (timer.notExpired()); // 只要总体计时器未过期，就继续循环

        // 如果循环结束（计时器过期）但 Future 仍未完成，则抛出超时异常
        throw new TimeoutException("Operation timed out before completion");
    }

    /**
     * 调用消费者再平衡监听器的回调方法。
     * 此方法根据提供的 methodName（例如 ON_PARTITIONS_REVOKED, ON_PARTITIONS_ASSIGNED, ON_PARTITIONS_LOST）
     * 来调用 {@link ConsumerRebalanceListener} 中相应的方法。
     * 它会捕获回调期间发生的任何异常，并将其包装在 {@link ConsumerRebalanceListenerCallbackCompletedEvent} 中返回。
     * 应用场景：在消费者组发生再平衡时，由后台线程触发，用于在应用线程中执行用户定义的再平衡回调逻辑。
     * 设计考虑：将回调的执行与异常处理封装在一起，简化了调用方的逻辑，并确保回调的完成状态能够被正确传递。
     *
     * @param rebalanceListenerInvoker 用于调用具体回调方法的调用器
     * @param methodName               要调用的回调方法的名称枚举
     * @param partitions               与回调相关的分区集合
     * @param future                   一个 CompletableFuture，用于在回调完成后通知调用者
     * @return 一个 {@link ConsumerRebalanceListenerCallbackCompletedEvent} 对象，包含回调的执行结果（成功或错误）
     */
    static ConsumerRebalanceListenerCallbackCompletedEvent invokeRebalanceCallbacks(ConsumerRebalanceListenerInvoker rebalanceListenerInvoker,
                                                                                    ConsumerRebalanceListenerMethodName methodName,
                                                                                    SortedSet<TopicPartition> partitions,
                                                                                    CompletableFuture<Void> future) { // 调用再平衡回调方法
        Exception e = null; // 初始化异常变量

        try {
            // 根据方法名调用相应的回调
            switch (methodName) {
                case ON_PARTITIONS_REVOKED: // 如果是分区被撤销的回调
                    e = rebalanceListenerInvoker.invokePartitionsRevoked(partitions); // 调用撤销分区的回调
                    break;

                case ON_PARTITIONS_ASSIGNED: // 如果是分区被分配的回调
                    e = rebalanceListenerInvoker.invokePartitionsAssigned(partitions); // 调用分配分区的回调
                    break;

                case ON_PARTITIONS_LOST: // 如果是分区丢失的回调
                    e = rebalanceListenerInvoker.invokePartitionsLost(partitions); // 调用丢失分区的回调
                    break;

                default: // 如果方法名无效
                    // 抛出非法参数异常，指示未预期的调用方法
                    throw new IllegalArgumentException("The method " + methodName.fullyQualifiedMethodName() + " to invoke was not expected");
            }
        } catch (WakeupException | InterruptException ex) { // 捕获 WakeupException 或 InterruptException
            e = ex; // 将捕获到的异常赋值给 e
        }

        final Optional<KafkaException> error; // 定义一个 Optional 类型的 KafkaException 错误变量

        if (e != null) // 如果在回调执行过程中捕获到异常
            // 将异常包装成 KafkaException（如果需要），并创建一个包含该异常的 Optional 对象
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(e, "User rebalance callback throws an error"));
        else
            error = Optional.empty(); // 如果没有异常，则创建一个空的 Optional 对象

        // 创建并返回一个 ConsumerRebalanceListenerCallbackCompletedEvent 对象，
        // 其中包含方法名、future 和错误信息（如果存在）
        return new ConsumerRebalanceListenerCallbackCompletedEvent(methodName, future, error);
    }

    /**
     * 获取消费者的客户端 ID。
     * 客户端 ID 是在消费者配置中指定的，用于在 Kafka 集群中唯一标识此消费者实例。
     * 应用场景：用于日志记录、监控以及问题排查，帮助识别特定的消费者实例。
     * 设计考虑：直接返回成员变量，简单高效。
     * @return 客户端 ID 字符串
     */
    @Override
    public String clientId() { // 获取客户端ID
        return clientId; // 返回存储的客户端ID
    }

    /**
     * 获取消费者的度量注册表。
     * 度量注册表包含了此消费者收集的各种性能指标。
     * 应用场景：用于监控消费者的性能，例如拉取速率、提交延迟等。
     * 设计考虑：直接返回成员变量，允许外部访问度量数据。
     * @return {@link Metrics} 度量注册表实例
     */
    @Override
    public Metrics metricsRegistry() { // 获取度量注册表
        return metrics; // 返回存储的度量对象
    }

    /**
     * 获取异步消费者的特定度量指标。
     * 这提供了对 {@link AsyncConsumerMetrics} 接口的访问，该接口定义了异步消费者特有的度量指标。
     * 应用场景：用于更细致地监控异步消费者的行为和性能。
     * 设计考虑：提供一个专门的接口来访问异步消费者相关的度量，与通用的 {@link Metrics} 区分开。
     * @return {@link AsyncConsumerMetrics} 异步消费者度量指标实例
     */
    @Override
    public AsyncConsumerMetrics kafkaConsumerMetrics() { // 获取 Kafka 消费者度量
        return kafkaConsumerMetrics; // 返回存储的 Kafka 消费者度量对象
    }

    // 仅用于测试
    /**
     * 获取消费者的订阅状态。
     * 订阅状态维护了消费者当前订阅的主题、分配的分区以及这些分区的消费位置等信息。
     * 应用场景：主要用于内部逻辑和测试，以检查和验证消费者的订阅和分配状态。
     * 设计考虑：提供对内部状态的访问，但标记为仅用于测试，表明不应在生产代码中直接依赖此方法。
     * @return {@link SubscriptionState} 消费者的订阅状态实例
     */
    SubscriptionState subscriptions() { // 获取订阅状态
        return subscriptions; // 返回存储的订阅状态对象
    }

    /**
     * 计算默认 API 超时的截止时间（毫秒）。
     * 此方法使用当前时间和配置的默认 API 超时时间来计算一个未来的时间点，表示操作必须在此时间点之前完成。
     * 应用场景：用于设置需要与 Kafka broker 交互的各种操作的超时时间，例如提交偏移量、获取元数据等。
     * 设计考虑：将超时计算逻辑封装在一个私有方法中，便于在多处复用。
     * @return 默认 API 超时的截止时间（毫秒）
     */
    private long defaultApiTimeoutDeadlineMs() { // 计算默认API超时截止时间（毫秒）
        // 使用当前时间和默认API超时毫秒数计算截止时间
        return calculateDeadlineMs(time, defaultApiTimeoutMs);
    }
}
