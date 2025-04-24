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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createConsumerNetworkClient;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * 一个使用{@link GroupProtocol#CLASSIC 经典消费者组协议}从Kafka集群消费记录的客户端。
 * 在此实现中，所有网络I/O都发生在发起调用的应用程序线程中。
 *
 * <p/>
 *
 * 此{@link ConsumerDelegate}实现是为了向后兼容性而存在，允许用户继续使用
 * 经典消费者组协议(pre-KIP 848)。
 *
 * <p/>
 *
 * <em>注意：</em>不应直接调用此类；用户应该像以前一样创建和使用{@link KafkaConsumer} API。
 */
public class ClassicKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    // 表示没有当前线程访问消费者的标记值
    private static final long NO_CURRENT_THREAD = -1L;
    // 默认的重平衡原因
    public static final String DEFAULT_REASON = "rebalance enforced by user";

    // 度量指标管理器，用于收集和管理消费者的各种性能指标
    private final Metrics metrics;
    // Kafka消费者特定的度量指标管理器
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    // 日志记录器
    private Logger log;
    // 客户端ID，用于在集群中唯一标识此消费者
    private final String clientId;
    // 消费者组ID，可选值，用于标识消费者所属的消费者组
    private final Optional<String> groupId;
    // 消费者协调器，负责管理消费者组成员身份和分区分配
    private final ConsumerCoordinator coordinator;
    // 反序列化器，用于将消息的key和value从字节数组转换为Java对象
    private final Deserializers<K, V> deserializers;
    // 消息获取器，负责从Kafka服务器获取消息
    private final Fetcher<K, V> fetcher;
    // 偏移量获取器，用于获取分区的偏移量信息
    private final OffsetFetcher offsetFetcher;
    // 主题元数据获取器，用于获取主题的元数据信息
    private final TopicMetadataFetcher topicMetadataFetcher;
    // 消费者拦截器，用于在消费消息前后进行拦截处理
    private final ConsumerInterceptors<K, V> interceptors;
    // 隔离级别，定义了消费者读取消息的隔离级别
    private final IsolationLevel isolationLevel;

    // 时间工具类，用于处理时间相关的操作
    private final Time time;
    // 消费者网络客户端，处理与Kafka服务器的网络通信
    private final ConsumerNetworkClient client;
    // 订阅状态管理器，维护主题订阅和分区分配的状态
    private final SubscriptionState subscriptions;
    // 集群元数据管理器，维护Kafka集群的元数据信息
    private final ConsumerMetadata metadata;
    // 重试等待时间(毫秒)
    private final long retryBackoffMs;
    // 最大重试等待时间(毫秒)
    private final long retryBackoffMaxMs;
    // 请求超时时间(毫秒)
    private final int requestTimeoutMs;
    // 默认API超时时间(毫秒)
    private final int defaultApiTimeoutMs;
    // 消费者是否已关闭的标志
    private volatile boolean closed = false;
    // 分区分配器列表，用于在消费者组中分配分区
    private final List<ConsumerPartitionAssignor> assignors;
    // 客户端遥测报告器，用于收集和报告客户端遥测数据
    // 初始值设为空是为了避免构造函数抛出异常时的空指针异常
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    // 当前访问此消费者的线程ID，用于防止多线程访问
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // 引用计数器，允许获取了currentThread的线程进行重入访问
    private final AtomicInteger refcount = new AtomicInteger(0);

    // 缓存订阅是否具有所有获取位置的标志，避免在poll()时重复扫描订阅
    private boolean cachedSubscriptionHasAllFetchPositions;

    /**
     * 创建一个新的ClassicKafkaConsumer实例
     * 
     * @param config 消费者配置，包含所有必要的配置参数
     * @param keyDeserializer 消息key的反序列化器
     * @param valueDeserializer 消息value的反序列化器
     * @throws InvalidGroupIdException 当配置的消费者组ID为空字符串时抛出
     * @throws KafkaException 当消费者初始化失败时抛出
     */
    ClassicKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            // 创建消费者组重平衡配置，设置协议类型为CONSUMER
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);
            // 验证消费者组ID不能为空字符串
            if (groupRebalanceConfig.groupId != null && groupRebalanceConfig.groupId.isEmpty()) {
                throw new InvalidGroupIdException("The configured " + ConsumerConfig.GROUP_ID_CONFIG
                        + " should not be an empty string or whitespace.");
            }

            // 初始化基本配置
            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId); // 设置消费者组ID，允许为null
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG); // 设置客户端ID
            LogContext logContext = createLogContext(config, groupRebalanceConfig); // 创建日志上下文
            this.log = logContext.logger(getClass()); // 初始化日志记录器
            boolean enableAutoCommit = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG); // 是否启用自动提交偏移量

            log.debug("Initializing the Kafka consumer");
            // 初始化超时和时间相关配置
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG); // 请求超时时间
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG); // 默认API超时时间
            this.time = Time.SYSTEM; // 使用系统时间
            
            // 初始化度量指标和遥测报告相关组件
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config); // 创建度量指标报告器列表
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config); // 创建遥测报告器
            this.clientTelemetryReporter.ifPresent(reporters::add); // 如果存在遥测报告器，添加到报告器列表
            this.metrics = createMetrics(config, time, reporters); // 创建度量指标管理器
            
            // 设置重试相关配置
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG); // 重试等待时间
            this.retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG); // 最大重试等待时间

            // 初始化拦截器和反序列化器
            List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config); // 配置消费者拦截器列表
            this.interceptors = new ConsumerInterceptors<>(interceptorList, metrics); // 创建消费者拦截器
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer, metrics); // 创建反序列化器
            
            // 初始化订阅状态和元数据管理
            this.subscriptions = createSubscriptionState(config, logContext); // 创建订阅状态管理器
            // 配置集群资源监听器，用于监控度量指标、拦截器和反序列化器的变化
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    metrics.reporters(),
                    interceptorList,
                    Arrays.asList(this.deserializers.keyDeserializer(), this.deserializers.valueDeserializer()));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners); // 创建元数据管理器
            
            // 解析并验证broker地址列表，然后进行引导程序初始化
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            this.metadata.bootstrap(addresses);

            // 初始化消息获取相关组件
            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics); // 创建获取度量指标管理器
            FetchConfig fetchConfig = new FetchConfig(config); // 创建获取配置
            this.isolationLevel = fetchConfig.isolationLevel; // 设置隔离级别

            // 初始化网络客户端
            ApiVersions apiVersions = new ApiVersions(); // 创建API版本管理器
            // 创建消费者网络客户端，用于处理与Kafka服务器的网络通信
            this.client = createConsumerNetworkClient(config,
                    metrics,
                    logContext,
                    apiVersions,
                    time,
                    metadata,
                    fetchMetricsManager.throttleTimeSensor(), // 用于限流的传感器
                    retryBackoffMs,
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

            // 初始化分区分配器
            // 根据配置的分区分配策略创建分配器实例列表
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // 初始化消费者协调器
            // 如果没有设置消费者组ID，则不需要创建协调器
            if (groupId.isEmpty()) {
                // 忽略自动提交相关的配置
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
                config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
                this.coordinator = null;
            } else {
                // 创建消费者协调器，负责组成员管理和分区分配
                this.coordinator = new ConsumerCoordinator(groupRebalanceConfig,
                        logContext,
                        this.client,
                        assignors, // 分区分配器列表
                        this.metadata,
                        this.subscriptions,
                        metrics,
                        CONSUMER_METRIC_GROUP_PREFIX,
                        this.time,
                        enableAutoCommit, // 是否启用自动提交
                        config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG), // 自动提交间隔
                        this.interceptors,
                        config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED),
                        config.getString(ConsumerConfig.CLIENT_RACK_CONFIG), // 机架ID
                        clientTelemetryReporter);
            }
            // 初始化消息获取器
            // 负责从Kafka服务器获取消息数据
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    this.metadata,
                    this.subscriptions,
                    fetchConfig, // 获取配置
                    this.deserializers, // 反序列化器
                    fetchMetricsManager, // 度量指标管理器
                    this.time,
                    apiVersions);
            
            // 初始化偏移量获取器
            // 负责获取分区的偏移量信息
            this.offsetFetcher = new OffsetFetcher(logContext,
                    client,
                    metadata,
                    subscriptions,
                    time,
                    retryBackoffMs, // 重试等待时间
                    requestTimeoutMs, // 请求超时时间
                    isolationLevel, // 隔离级别
                    apiVersions);
            
            // 初始化主题元数据获取器
            // 负责获取主题的元数据信息
            this.topicMetadataFetcher = new TopicMetadataFetcher(logContext,
                    client,
                    retryBackoffMs,
                    retryBackoffMaxMs);

            // 初始化消费者度量指标
            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);

            // 完成初始化
            config.logUnused(); // 记录未使用的配置项
            // 注册JMX信息
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // 异常处理：如果在构造过程中发生异常
            // 如果已经创建了内部对象（log不为null），则需要关闭这些对象以防止资源泄漏
            // 参考KAFKA-2121 issue
            if (this.log != null) {
                close(Duration.ZERO, true); // 立即关闭消费者
            }
            // 抛出包装后的异常
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    /**
     * 用于测试的构造函数，允许注入模拟的组件进行单元测试
     * 
     * @param logContext 日志上下文，用于创建日志记录器
     * @param time 时间工具类，用于处理时间相关的操作
     * @param config 消费者配置，包含所有必要的配置参数
     * @param keyDeserializer 消息key的反序列化器
     * @param valueDeserializer 消息value的反序列化器
     * @param client Kafka客户端，用于与Kafka服务器通信
     * @param subscriptions 订阅状态管理器，维护主题订阅和分区分配的状态
     * @param metadata 集群元数据管理器，维护Kafka集群的元数据信息
     * @param assignors 分区分配器列表，用于在消费者组中分配分区
     */
    ClassicKafkaConsumer(LogContext logContext,
                         Time time,
                         ConsumerConfig config,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer,
                         KafkaClient client,
                         SubscriptionState subscriptions,
                         ConsumerMetadata metadata,
                         List<ConsumerPartitionAssignor> assignors) {
        // 初始化基础组件
        this.log = logContext.logger(getClass()); // 创建日志记录器
        this.time = time; // 设置时间工具类
        this.subscriptions = subscriptions; // 设置订阅状态管理器
        this.metadata = metadata; // 设置元数据管理器
        this.metrics = new Metrics(time); // 创建度量指标管理器
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG); // 设置客户端ID
        this.groupId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_ID_CONFIG)); // 设置消费者组ID
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer, metrics); // 创建反序列化器
        this.isolationLevel = ConsumerUtils.configuredIsolationLevel(config); // 设置隔离级别
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG); // 设置默认API超时时间
        this.assignors = assignors; // 设置分区分配器列表
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX); // 创建Kafka消费者度量指标管理器
        this.interceptors = new ConsumerInterceptors<>(Collections.emptyList(), metrics); // 创建空的消费者拦截器列表
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG); // 设置重试等待时间
        this.retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG); // 设置最大重试等待时间
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG); // 设置请求超时时间
        this.clientTelemetryReporter = Optional.empty(); // 初始化空的遥测报告器

        // 获取消费者组相关的配置参数
        int sessionTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG); // 会话超时时间
        int rebalanceTimeoutMs = config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG); // 重平衡超时时间
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG); // 心跳间隔时间
        boolean enableAutoCommit = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG); // 是否启用自动提交
        boolean throwOnStableOffsetNotSupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED); // 是否在不支持获取稳定偏移量时抛出异常
        int autoCommitIntervalMs = config.getInt(AUTO_COMMIT_INTERVAL_MS_CONFIG); // 自动提交间隔
        String rackId = config.getString(CLIENT_RACK_CONFIG); // 机架ID
        Optional<String> groupInstanceId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)); // 消费者组实例ID

        // 创建消费者网络客户端
        this.client = new ConsumerNetworkClient(
            logContext,
            client,
            metadata,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            heartbeatIntervalMs
        );

        // 如果设置了消费者组ID，则创建消费者协调器
        if (groupId.isPresent()) {
            // 创建重平衡配置
            GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(
                sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId.get(),
                groupInstanceId,
                retryBackoffMs,
                retryBackoffMaxMs,
                true
            );
            // 创建消费者协调器
            this.coordinator = new ConsumerCoordinator(
                rebalanceConfig,
                logContext,
                this.client,
                assignors,
                metadata,
                subscriptions,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                time,
                enableAutoCommit,
                autoCommitIntervalMs,
                interceptors,
                throwOnStableOffsetNotSupported,
                rackId,
                clientTelemetryReporter
            );
        } else {
            this.coordinator = null; // 如果没有消费者组ID，则不需要协调器
        }

        // 获取消息获取相关的配置参数
        int maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG); // 最大获取字节数
        int maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG); // 最大等待时间
        int minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG); // 最小获取字节数
        int fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG); // 每个分区的最大获取字节数
        int maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG); // 每次轮询的最大记录数
        boolean checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG); // 是否检查CRC校验和

        // 创建消息获取相关的组件
        ConsumerMetrics metricsRegistry = new ConsumerMetrics(CONSUMER_METRIC_GROUP_PREFIX); // 创建消费者度量指标注册表
        FetchMetricsManager metricsManager = new FetchMetricsManager(metrics, metricsRegistry.fetcherMetrics); // 创建获取度量指标管理器
        ApiVersions apiVersions = new ApiVersions(); // 创建API版本管理器
        // 创建获取配置
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                checkCrcs,
                rackId,
                isolationLevel
        );
        // 创建消息获取器
        this.fetcher = new Fetcher<>(
            logContext,
            this.client,
            metadata,
            subscriptions,
            fetchConfig,
            deserializers,
            metricsManager,
            time,
            apiVersions
        );
        // 创建偏移量获取器
        this.offsetFetcher = new OffsetFetcher(
            logContext,
            this.client,
            metadata,
            subscriptions,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            isolationLevel,
            apiVersions
        );
        // 创建主题元数据获取器
        this.topicMetadataFetcher = new TopicMetadataFetcher(
            logContext,
            this.client,
            retryBackoffMs,
            retryBackoffMaxMs
        );
    }

    /**
     * 获取当前消费者被分配的所有主题分区
     * 
     * @return 一个不可修改的TopicPartition集合，包含所有分配给该消费者的分区
     */
    public Set<TopicPartition> assignment() {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 返回一个不可修改的已分配分区集合的副本
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 获取当前消费者订阅的所有主题
     * 
     * @return 一个包含所有已订阅主题名称的不可修改集合
     */
    public Set<String> subscription() {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 返回一个已订阅主题集合的副本
            return Set.copyOf(this.subscriptions.subscription());
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 订阅指定的主题集合，并提供一个重平衡监听器
     * 
     * @param topics 要订阅的主题集合
     * @param listener 在分区分配发生变化时接收通知的监听器
     * @throws IllegalArgumentException 如果监听器为null
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        // 验证重平衡监听器不能为null
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        // 调用内部订阅方法，将监听器包装为Optional
        subscribeInternal(topics, Optional.of(listener));
    }

    /**
     * 为订阅注册一个新的度量指标
     * 
     * @param metric 要注册的度量指标
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        // 检查度量指标是否已存在
        if (!metrics().containsKey(metric.metricName())) {
            // 如果不存在，通知遥测报告器添加新的度量指标
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        } else {
            // 如果已存在，记录跳过注册的日志
            log.debug("Skipping registration for metric {}. Existing consumer metrics cannot be overwritten.", metric.metricName());
        }
    }

    /**
     * 从订阅中注销一个度量指标
     * 
     * @param metric 要注销的度量指标
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        // 检查度量指标是否已存在
        if (!metrics().containsKey(metric.metricName())) {
            // 如果不存在，通知遥测报告器移除度量指标
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        }  else {
            // 如果已存在，记录跳过注销的日志
            log.debug("Skipping unregistration for metric {}. Existing consumer metrics cannot be removed.", metric.metricName());
        }
    }

    /**
     * 订阅指定的主题集合，不提供重平衡监听器
     * 
     * @param topics 要订阅的主题集合
     */
    @Override
    public void subscribe(Collection<String> topics) {
        // 调用内部订阅方法，不提供监听器
        subscribeInternal(topics, Optional.empty());
    }

    /**
     * subscribe方法的内部实现，用于处理主题订阅逻辑
     * 
     * @param topics 要订阅的主题集合
     * @param listener 可选的重平衡监听器，用于接收分区分配变化的通知
     * @throws IllegalArgumentException 如果topics为null或包含null/空字符串元素
     * @throws IllegalStateException 如果之前已使用模式订阅，或已手动分配分区，或未配置分区分配策略
     */
    private void subscribeInternal(Collection<String> topics, Optional<ConsumerRebalanceListener> listener) {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 检查消费者组ID是否有效
            maybeThrowInvalidGroupIdException();
            // 验证主题集合不能为null
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            
            if (topics.isEmpty()) {
                // 如果主题集合为空，等同于取消订阅
                this.unsubscribe();
            } else {
                // 验证每个主题名称不能为空或空字符串
                for (String topic : topics) {
                    if (isBlank(topic))
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }

                // 检查是否配置了分区分配器
                throwIfNoAssignorsConfigured();

                // 清理不再属于新订阅主题的分区数据
                final Set<TopicPartition> currentTopicPartitions = new HashSet<>();
                // 收集仍然属于新订阅主题的分区
                for (TopicPartition tp : subscriptions.assignedPartitions()) {
                    if (topics.contains(tp.topic()))
                        currentTopicPartitions.add(tp);
                }
                // 清理不再需要的分区数据
                fetcher.clearBufferedDataForUnassignedPartitions(currentTopicPartitions);

                // 记录订阅信息
                log.info("Subscribed to topic(s): {}", String.join(", ", topics));
                // 更新订阅状态，如果发生变化则请求更新主题元数据
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 使用指定的主题模式和重平衡监听器订阅主题
     * 
     * @param pattern 用于匹配主题的正则表达式模式
     * @param listener 重平衡监听器，用于处理分区分配和撤销事件
     * @throws IllegalArgumentException 如果监听器为null
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        // 验证重平衡监听器不能为null
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        // 调用内部订阅方法，将监听器包装为Optional
        subscribeInternal(pattern, Optional.of(listener));
    }

    /**
     * 使用指定的主题模式订阅主题，不提供重平衡监听器
     * 
     * @param pattern 用于匹配主题的正则表达式模式
     */
    @Override
    public void subscribe(Pattern pattern) {
        // 调用内部订阅方法，传入空的监听器
        subscribeInternal(pattern, Optional.empty());
    }

    /**
     * 使用RE2/J模式和重平衡监听器订阅主题（不支持）
     * 
     * @param pattern RE2/J订阅模式
     * @param callback 重平衡监听器
     * @throws UnsupportedOperationException 当使用经典协议时，不支持RE2/J模式订阅
     */
    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException(String.format("Subscribe to RE2/J pattern is not supported when using" +
            "the %s protocol defined in config %s", GroupProtocol.CLASSIC, ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    /**
     * 使用RE2/J模式订阅主题（不支持）
     * 
     * @param pattern RE2/J订阅模式
     * @throws UnsupportedOperationException 当使用经典协议时，不支持RE2/J模式订阅
     */
    @Override
    public void subscribe(SubscriptionPattern pattern) {
        throw new UnsupportedOperationException(String.format("Subscribe to RE2/J pattern is not supported when using" +
            "the %s protocol defined in config %s", GroupProtocol.CLASSIC, ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    /**
     * 内部订阅方法，用于实现基于模式的主题订阅功能
     * 
     * 该方法支持动态分区分配，会定期根据模式匹配检查当前存在的主题。
     * 通过配置metadata.max.age.ms可以控制元数据刷新频率，从而影响主题匹配的检查频率。
     * 
     * 重平衡在以下情况下触发：
     * 1. 匹配模式的主题发生变化
     * 2. 消费者组成员发生变化
     * 注意：重平衡只在调用poll(Duration)方法时执行
     *
     * @param pattern 要订阅的主题模式
     * @param listener 可选的重平衡监听器，用于接收分区分配和撤销的通知
     * @throws IllegalArgumentException 如果pattern为null或为空
     * @throws IllegalStateException 如果之前已经订阅了主题，或者已经手动分配了分区，或者没有配置分区分配策略
     */
    private void subscribeInternal(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        // 检查消费者组ID是否有效
        maybeThrowInvalidGroupIdException();
        // 验证主题模式不能为null或空
        if (pattern == null || pattern.toString().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        // 获取消费者的独占访问权
        acquireAndEnsureOpen();
        try {
            // 检查是否配置了分区分配器
            throwIfNoAssignorsConfigured();
            // 记录订阅信息
            log.info("Subscribed to pattern: '{}'", pattern);
            // 更新订阅状态
            this.subscriptions.subscribe(pattern, listener);
            // 更新协调器中的模式订阅信息
            this.coordinator.updatePatternSubscription(metadata.fetch());
            // 请求更新元数据以发现新主题
            this.metadata.requestUpdateForNewTopics();
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 取消所有主题的订阅
     * 
     * 该方法会执行以下操作：
     * 1. 清理未分配分区的缓存数据
     * 2. 如果是消费者组成员，准备离开组并可能执行离组操作
     * 3. 清除所有订阅信息
     */
    public void unsubscribe() {
        // 获取消费者的独占访问权
        acquireAndEnsureOpen();
        try {
            // 清理未分配分区的缓存数据
            fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
            // 如果存在协调器（即属于消费者组），执行离组操作
            if (this.coordinator != null) {
                // 准备离开消费者组
                this.coordinator.onLeavePrepare();
                // 尝试离开消费者组
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
            }
            // 清除所有订阅信息
            this.subscriptions.unsubscribe();
            // 记录取消订阅的操作
            log.info("Unsubscribed all topics or patterns and assigned partitions");
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    @Override
    /**
     * 手动为消费者分配分区的方法
     * 
     * 应用场景：
     * 1. 用户需要完全控制分区分配，不依赖消费者组的自动分配机制
     * 2. 实现自定义的分区分配策略
     * 3. 在特定场景下固定消费者与分区的对应关系
     * 
     * 实现细节：
     * 1. 参数验证和状态检查
     * 2. 清理旧分配的资源
     * 3. 更新分区分配状态
     * 4. 触发必要的元数据更新
     * 
     * @param partitions 要分配给该消费者的主题分区集合
     * @throws IllegalArgumentException 如果分区集合为null或包含无效的主题分区
     */
    public void assign(Collection<TopicPartition> partitions) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 验证分区集合参数
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                // 如果分区集合为空，则取消所有订阅
                this.unsubscribe();
            } else {
                // 验证每个主题分区的有效性
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (isBlank(topic))
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                }
                // 清理未分配分区的缓存数据，避免内存泄漏
                fetcher.clearBufferedDataForUnassignedPartitions(partitions);

                // 如果存在消费者协调器，尝试自动提交偏移量
                // 因为手动分配不会触发重平衡，所以需要主动提交
                if (coordinator != null)
                    this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

                // 记录分配的分区信息
                log.info("Assigned to partition(s): {}", partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
                
                // 更新订阅状态，如果有新的主题，请求更新元数据
                if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 从Kafka服务器拉取消息的公共接口方法
     * 
     * 应用场景：
     * 1. 消费者定期调用此方法获取订阅主题的消息
     * 2. 支持长轮询，在超时时间内等待新消息到达
     * 
     * @param timeout 拉取消息的最大等待时间
     * @return 拉取到的消息记录集合
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout));
    }

    /**
     * 实际执行消息拉取的内部方法
     * 
     * 实现细节：
     * 1. 线程安全和状态检查
     * 2. 元数据和分配更新
     * 3. 消息拉取和处理
     * 4. 性能优化：支持流水线操作
     * 
     * @param timer 用于控制拉取超时的计时器
     * @return 拉取到的消息记录集合
     * @throws KafkaException 如果重平衡回调抛出异常
     */
    private ConsumerRecords<K, V> poll(final Timer timer) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 记录poll操作开始的时间点
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            // 检查消费者是否已订阅主题或手动分配了分区
            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                // 检查是否需要唤醒阻塞的操作
                client.maybeTriggerWakeup();

                // 更新分配的元数据，但不阻塞等待加入消费者组
                // 这样可以在后台异步处理组成员关系
                updateAssignmentMetadataIfNeeded(timer, false);

                // 尝试拉取消息
                final Fetch<K, V> fetch = pollForFetches(timer);
                if (!fetch.isEmpty()) {
                    // 在返回获取的记录之前，发送下一轮的获取请求
                    // 这样可以在用户处理当前批次的消息时，并行地准备下一批次
                    // 实现了消息获取的流水线操作，提高吞吐量
                    if (sendFetches() > 0 || client.hasPendingRequests()) {
                        client.transmitSends();
                    }

                    // 如果返回的记录集为空，记录日志（这种情况通常发生在消费位置已经更新）
                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }

                    // 通过拦截器处理消息并返回
                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records(), fetch.nextOffsets()));
                }
            } while (timer.notExpired()); // 在超时前持续尝试

            // 如果超时仍未获取到消息，返回空记录集
            return ConsumerRecords.empty();
        } finally {
            // 释放线程锁
            release();
            // 记录poll操作结束的时间点
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    /**
     * 发送获取请求到Kafka服务器
     * 
     * @return 发送的获取请求数量
     */
    private int sendFetches() {
        // 在元数据变更时验证获取位置的有效性
        offsetFetcher.validatePositionsOnMetadataChange();
        // 发送获取请求并返回请求数量
        return fetcher.sendFetches();
    }

    /**
     * 根据需要更新分配的元数据信息
     * 
     * @param timer 操作超时计时器
     * @param waitForJoinGroup 是否等待加入消费者组
     * @return 如果更新成功返回true，否则返回false
     */
    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        // 如果存在协调器且协调器轮询失败，则返回false
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }
        // 更新获取位置并返回结果
        return updateFetchPositions(timer);
    }

    /**
     * 轮询并获取消息
     * 
     * @param timer 操作超时计时器
     * @return 获取到的消息批次
     * @throws KafkaException 如果重平衡回调抛出异常
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
        // 计算轮询超时时间：如果没有协调器，使用剩余时间；否则取协调器下次轮询时间和剩余时间的较小值
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        // 如果已经有可用的数据，立即返回
        final Fetch<K, V> fetch = fetcher.collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // 发送新的获取请求（不会重发待处理的请求）
        sendFetches();

        // 如果缺少某些分区的位置信息，我们不希望在轮询时被阻塞
        // 因为偏移量查找可能在失败后正在进行退避

        // 注意：使用cachedSubscriptionHasAllFetchPositions意味着我们必须在调用此方法之前
        // 调用updateAssignmentMetadataIfNeeded
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        // 创建轮询计时器并执行网络轮询
        Timer pollTimer = time.timer(pollTimeout);
        client.poll(pollTimer, () -> {
            // 由于获取可能由后台线程完成，我们需要这个轮询条件
            // 以确保不会在poll()中不必要地阻塞
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());

        // 返回获取到的消息
        return fetcher.collectFetch();
    }

    /**
     * 同步提交当前消费的所有分区的偏移量，使用默认的API超时时间
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 同步提交当前消费的所有分区的偏移量
     * 
     * @param timeout 提交操作的超时时间
     */
    @Override
    public void commitSync(Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    /**
     * 同步提交指定分区的偏移量，使用默认的API超时时间
     * 
     * @param offsets 要提交的分区偏移量映射
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 同步提交指定分区的偏移量
     * 
     * @param offsets 要提交的分区偏移量映射
     * @param timeout 提交操作的超时时间
     * @throws TimeoutException 如果在超时时间内未能成功提交偏移量
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            // 检查消费者组ID是否有效
            maybeThrowInvalidGroupIdException();
            // 更新每个分区的最新epoch
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            // 同步提交偏移量，如果提交失败则抛出超时异常
            if (!coordinator.commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            // 记录同步提交的度量指标
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 异步提交当前消费的所有分区的偏移量，不使用回调
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    /**
     * 异步提交当前消费的所有分区的偏移量
     * 
     * @param callback 提交完成时的回调函数
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    /**
     * 异步提交指定分区的偏移量
     * 
     * @param offsets 要提交的分区偏移量映射
     * @param callback 提交完成时的回调函数
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 检查消费者组ID是否有效
            maybeThrowInvalidGroupIdException();
            log.debug("Committing offsets: {}", offsets);
            // 更新每个分区的最新epoch
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            // 异步提交偏移量
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 将指定分区的消费位置设置到指定的偏移量
     * 
     * @param partition 要设置位置的分区
     * @param offset 目标偏移量
     * @throws IllegalArgumentException 如果偏移量为负数
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        // 验证偏移量不能为负数
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            // 创建新的获取位置，跳过验证
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offset,
                    Optional.empty(), // 这将确保我们跳过验证
                    this.metadata.currentLeader(partition));
            // 更新分区的消费位置
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 将指定分区的消费位置设置到指定的偏移量和元数据
     * 
     * @param partition 要设置位置的分区
     * @param offsetAndMetadata 包含偏移量和元数据的对象
     * @throws IllegalArgumentException 如果偏移量为负数
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        // 获取偏移量并验证不能为负数
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 根据是否存在leader epoch选择不同的日志格式
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                        offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }
            // 获取当前的leader和epoch信息
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            // 创建新的获取位置
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(),
                    currentLeaderAndEpoch);
            // 更新最新看到的epoch
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            // 更新分区的消费位置
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 将指定分区的消费位置重置到最早的可用偏移量
     * 
     * 应用场景：
     * 1. 当需要从头开始重新消费某些分区的数据时
     * 2. 当发生数据丢失或需要重新处理历史数据时
     * 
     * @param partitions 需要重置位置的分区集合。如果为空集合，则重置所有已分配的分区
     * @throws IllegalArgumentException 如果partitions参数为null
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        // 参数校验：确保分区集合不为null
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 如果传入的分区集合为空，则使用所有已分配的分区；否则使用指定的分区
            Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
            // 请求将指定分区的偏移量重置到最早位置
            subscriptions.requestOffsetReset(parts, AutoOffsetResetStrategy.EARLIEST);
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 将指定分区的消费位置重置到最新的可用偏移量
     * 
     * 应用场景：
     * 1. 当需要跳过历史数据，只消费最新数据时
     * 2. 当处理实时数据流，不关心历史数据时
     * 
     * @param partitions 需要重置位置的分区集合。如果为空集合，则重置所有已分配的分区
     * @throws IllegalArgumentException 如果partitions参数为null
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        // 参数校验：确保分区集合不为null
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 如果传入的分区集合为空，则使用所有已分配的分区；否则使用指定的分区
            Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
            // 请求将指定分区的偏移量重置到最新位置
            subscriptions.requestOffsetReset(parts, AutoOffsetResetStrategy.LATEST);
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 获取指定分区的当前消费位置（偏移量）
     * 使用默认的API超时时间
     * 
     * 应用场景：
     * 1. 监控消费进度
     * 2. 在处理消息前检查当前位置
     * 
     * @param partition 要查询位置的分区
     * @return 当前消费位置（偏移量）
     */
    @Override
    public long position(TopicPartition partition) {
        // 使用默认API超时时间调用带超时参数的position方法
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 获取指定分区的当前消费位置（偏移量），带有超时控制
     * 
     * 应用场景：
     * 1. 需要精确控制查询超时时间的场景
     * 2. 在严格的时间限制下监控消费进度
     * 
     * @param partition 要查询位置的分区
     * @param timeout 操作超时时间
     * @return 当前消费位置（偏移量）
     * @throws IllegalStateException 如果指定的分区未被分配给该消费者
     * @throws TimeoutException 如果在指定时间内无法确定位置
     */
    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 检查分区是否已分配给该消费者
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            // 创建一个计时器用于超时控制
            Timer timer = time.timer(timeout);
            do {
                // 尝试获取分区的有效位置
                SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
                if (position != null)
                    return position.offset;

                // 如果位置无效，更新获取位置并进行一次网络轮询
                updateFetchPositions(timer);
                client.poll(timer);
            } while (timer.notExpired()); // 在超时前持续尝试

            // 如果超时仍未获取到位置，抛出超时异常
            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                    "for partition " + partition + " could be determined");
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 获取指定分区集合的已提交偏移量
     * 使用默认的API超时时间
     * 
     * 应用场景：
     * 1. 检查消费进度
     * 2. 故障恢复时确定从何处继续消费
     * 
     * @param partitions 要查询已提交偏移量的分区集合
     * @return 分区到已提交偏移量的映射
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        // 使用默认API超时时间调用带超时参数的committed方法
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 获取指定分区集合的已提交偏移量，带有超时控制
     * 
     * 应用场景：
     * 1. 需要精确控制查询超时时间的场景
     * 2. 在严格的时间限制下检查消费进度
     * 
     * @param partitions 要查询已提交偏移量的分区集合
     * @param timeout 操作超时时间
     * @return 分区到已提交偏移量的映射
     * @throws TimeoutException 如果在指定时间内无法获取已提交的偏移量
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        // 记录开始时间，用于度量指标统计
        long start = time.nanoseconds();
        try {
            // 检查消费者组ID是否有效
            maybeThrowInvalidGroupIdException();
            // 通过协调器获取已提交的偏移量
            final Map<TopicPartition, OffsetAndMetadata> offsets;
            offsets = coordinator.fetchCommittedOffsets(partitions, time.timer(timeout));
            
            // 如果在超时时间内未能获取偏移量，抛出超时异常
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                        "committed offset for partitions " + partitions + " could be determined. Try tuning " +
                        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " larger to relax the threshold.");
            } else {
                // 更新每个分区的最新epoch信息
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            // 记录获取已提交偏移量的耗时
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            // 释放消费者的独占访问权
            release();
        }
    }

    /**
     * 获取客户端实例ID
     * 
     * 应用场景：
     * 1. 监控和跟踪特定消费者实例
     * 2. 调试和故障排除时识别具体的消费者实例
     * 
     * @param timeout 获取实例ID的超时时间
     * @return 客户端实例的唯一标识符
     * @throws IllegalStateException 如果遥测功能未启用
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        // 检查遥测报告器是否可用
        if (clientTelemetryReporter.isEmpty()) {
            // 如果遥测功能未启用，抛出异常并提示用户启用该功能
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        // 通过遥测工具获取客户端实例ID
        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * 获取消费者的所有度量指标
     * 
     * 应用场景：
     * 1. 监控消费者性能
     * 2. 收集消费者运行时统计信息
     * 3. 进行性能调优和问题诊断
     * 
     * @return 不可修改的度量指标映射，键为度量指标名称，值为度量指标对象
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // 返回一个不可修改的度量指标映射，确保外部代码不能修改内部度量指标集合
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * 获取指定主题的分区信息
     * 使用默认的API超时时间
     * 
     * 应用场景：
     * 1. 在订阅主题前了解其分区布局
     * 2. 手动分配分区时获取分区信息
     * 
     * @param topic 要查询的主题名称
     * @return 主题的分区信息列表
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        // 使用默认API超时时间调用带超时参数的partitionsFor方法
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 获取指定主题的分区信息，带有超时控制
     * 
     * 应用场景：
     * 1. 需要精确控制查询超时时间的场景
     * 2. 在严格的时间限制下获取主题元数据
     * 
     * @param topic 要查询的主题名称
     * @param timeout 操作超时时间
     * @return 主题的分区信息列表，如果主题不存在则返回空列表
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        // 获取消费者的独占访问权并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 首先尝试从本地缓存的集群元数据中获取分区信息
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            // 如果本地缓存中有分区信息，直接返回
            if (!parts.isEmpty())
                return parts;

            // 如果本地缓存中没有分区信息，创建定时器并从服务器获取主题元数据
            Timer timer = time.timer(timeout);
            List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topic, metadata.allowAutoTopicCreation(), timer);
            // 返回获取到的元数据，如果获取失败则返回空列表
            return topicMetadata != null ? topicMetadata : Collections.emptyList();
        } finally {
            // 释放消费者的独占访问权
            release();
        }
    }

    @Override
    /**
     * 获取所有主题的元数据信息，使用默认的API超时时间
     * 
     * @return 主题名称到分区信息列表的映射
     */
    public Map<String, List<PartitionInfo>> listTopics() {
        // 使用默认的API超时时间调用带超时参数的方法
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 获取所有主题的元数据信息，可指定超时时间
     * 
     * @param timeout 获取元数据的超时时间
     * @return 主题名称到分区信息列表的映射
     * @throws TimeoutException 如果在指定的超时时间内未能完成操作
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 通过主题元数据获取器获取所有主题的元数据信息
            return topicMetadataFetcher.getAllTopicMetadata(time.timer(timeout));
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 暂停指定分区的消息消费
     * 暂停后，poll()方法将不会返回这些分区的消息，直到这些分区被恢复
     * 
     * @param partitions 要暂停的主题分区集合
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 记录暂停分区的调试日志
            log.debug("Pausing partitions {}", partitions);
            // 遍历分区集合，将每个分区标记为暂停状态
            for (TopicPartition partition: partitions) {
                subscriptions.pause(partition);
            }
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 恢复指定分区的消息消费
     * 恢复后，这些分区的消息将可以通过poll()方法获取
     * 
     * @param partitions 要恢复的主题分区集合
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 记录恢复分区的调试日志
            log.debug("Resuming partitions {}", partitions);
            // 遍历分区集合，将每个分区标记为恢复状态
            for (TopicPartition partition: partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 获取当前已暂停的所有分区
     * 
     * @return 已暂停的主题分区集合（不可修改）
     */
    @Override
    public Set<TopicPartition> paused() {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 返回不可修改的已暂停分区集合
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 根据时间戳查找对应的偏移量，使用默认的API超时时间
     * 
     * @param timestampsToSearch 主题分区到时间戳的映射
     * @return 主题分区到偏移量和时间戳的映射
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        // 使用默认的API超时时间调用带超时参数的方法
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 根据时间戳查找对应的偏移量，可指定超时时间
     * 
     * @param timestampsToSearch 主题分区到时间戳的映射
     * @param timeout 查找操作的超时时间
     * @return 主题分区到偏移量和时间戳的映射
     * @throws IllegalArgumentException 如果任何时间戳值为负数
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 验证所有时间戳值是否合法（不能为负数）
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // 显式排除最早和最新偏移量，确保返回的OffsetAndTimestamp中的时间戳始终为正数
                if (entry.getValue() < 0)
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                            entry.getValue() + ". The target time cannot be negative.");
            }
            // 通过偏移量获取器查找指定时间戳对应的偏移量
            return offsetFetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 获取指定分区的起始偏移量，使用默认的API超时时间
     * 
     * @param partitions 要查询的主题分区集合
     * @return 主题分区到其起始偏移量的映射
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        // 使用默认的API超时时间调用带超时参数的方法
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 获取指定分区的起始偏移量，可指定超时时间
     * 
     * @param partitions 要查询的主题分区集合
     * @param timeout 查询操作的超时时间
     * @return 主题分区到其起始偏移量的映射
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 通过偏移量获取器查询分区的起始偏移量
            return offsetFetcher.beginningOffsets(partitions, time.timer(timeout));
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 获取指定分区的结束偏移量，使用默认的API超时时间
     * 
     * @param partitions 要查询的主题分区集合
     * @return 主题分区到其结束偏移量的映射
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        // 使用默认的API超时时间调用带超时参数的方法
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * 获取指定分区的结束偏移量，可指定超时时间
     * 
     * @param partitions 要查询的主题分区集合
     * @param timeout 查询操作的超时时间
     * @return 主题分区到其结束偏移量的映射
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 通过偏移量获取器查询分区的结束偏移量
            return offsetFetcher.endOffsets(partitions, time.timer(timeout));
        } finally {
            // 释放线程锁
            release();
        }
    }

    @Override
    /**
     * 获取指定主题分区的消费延迟（消费者落后于生产者的消息数）
     * 
     * @param topicPartition 要查询的主题分区
     * @return 返回消费延迟值的Optional包装，如果无法计算延迟则返回空
     */
    public OptionalLong currentLag(TopicPartition topicPartition) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 尝试获取分区的消费延迟值
            final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

            // 如果无法获取延迟值（通常是因为不知道日志末端偏移量），并且还没有发送获取偏移量的请求
            // 则发送一个异步请求来获取该分区的末端偏移量
            // 我们不需要等待响应，因为这里不会同步轮询网络客户端
            if (lag == null) {
                if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                        !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                    // 记录日志，表明我们正在请求日志末端偏移量以计算延迟
                    log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                    // 标记已请求该分区的末端偏移量
                    subscriptions.requestPartitionEndOffset(topicPartition);
                    // 发送获取末端偏移量的请求，使用0毫秒超时表示不等待响应
                    offsetFetcher.endOffsets(Collections.singleton(topicPartition), time.timer(0L));
                }

                // 由于当前无法计算延迟，返回空的Optional
                return OptionalLong.empty();
            }

            // 返回包含延迟值的Optional
            return OptionalLong.of(lag);
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 获取当前消费者的消费者组元数据信息
     * 
     * @return 返回包含消费者组信息的ConsumerGroupMetadata对象
     * @throws InvalidGroupIdException 如果消费者没有配置有效的消费者组ID
     * @throws IllegalStateException 如果消费者已经关闭
     */
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 检查是否配置了有效的消费者组ID
            maybeThrowInvalidGroupIdException();
            // 从协调器获取消费者组元数据
            return coordinator.groupMetadata();
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 强制触发一次消费者组的重平衡操作，并指定重平衡的原因
     * 
     * @param reason 触发重平衡的原因，如果为null或空字符串则使用默认原因
     * @throws IllegalStateException 如果消费者没有加入任何消费者组或者消费者已关闭
     */
    @Override
    public void enforceRebalance(final String reason) {
        // 获取线程锁并确保消费者未关闭
        acquireAndEnsureOpen();
        try {
            // 检查消费者是否加入了消费者组
            if (coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            // 请求重新加入组，如果没有指定原因则使用默认原因
            coordinator.requestRejoin(reason == null || reason.isEmpty() ? DEFAULT_REASON : reason);
        } finally {
            // 释放线程锁
            release();
        }
    }

    /**
     * 使用默认原因强制触发一次消费者组的重平衡操作
     * 
     * @throws IllegalStateException 如果消费者没有加入任何消费者组或者消费者已关闭
     */
    @Override
    public void enforceRebalance() {
        // 调用带有原因参数的重载方法，传入null使用默认原因
        enforceRebalance(null);
    }

    /**
     * 使用默认超时时间关闭消费者
     * 默认超时时间为{@link ConsumerUtils#DEFAULT_CLOSE_TIMEOUT_MS}毫秒
     */
    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    /**
     * 关闭消费者，清理所有资源
     * 
     * @param timeout 等待资源清理完成的最大时间
     * @throws IllegalArgumentException 如果超时时间为负数
     * @throws InterruptException 如果在关闭过程中线程被中断
     * @throws KafkaException 如果在关闭过程中发生其他错误
     */
    @Override
    public void close(Duration timeout) {
        // 验证超时时间不能为负数
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        // 获取线程锁
        acquire();
        try {
            if (!closed) {
                // 在设置closed标志之前先执行关闭操作
                // 因为关闭过程中可能会触发重平衡回调，此时消费者需要保持开启状态
                close(timeout, false);
            }
        } finally {
            // 设置关闭标志并释放线程锁
            closed = true;
            release();
        }
    }

    /**
     * 唤醒可能阻塞的消费者操作
     * 这个方法是线程安全的，可以从其他线程调用
     */
    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * 创建用于请求的计时器
     * 
     * @param timeout 超时时间
     * @return 返回配置了超时时间的Timer实例
     */
    private Timer createTimerForRequest(final Duration timeout) {
        // time字段可能为null（如果构造函数中发生异常），此时使用系统时间
        final Time localTime = (time == null) ? Time.SYSTEM : time;
        // 使用请求超时时间和传入超时时间的较小值作为计时器的超时时间
        return localTime.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    /**
     * 执行实际的关闭操作，清理所有资源
     * 
     * @param timeout 等待资源清理完成的最大时间
     * @param swallowException 是否吞掉异常。如果为true，异常会被记录但不会抛出
     * @throws InterruptException 如果在关闭过程中线程被中断且swallowException为false
     * @throws KafkaException 如果在关闭过程中发生其他错误且swallowException为false
     */
    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        // 用于记录第一个发生的异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // 创建关闭操作的计时器
        final Timer closeTimer = createTimerForRequest(timeout);
        // 通知遥测报告器开始关闭
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        closeTimer.update();

        // 按顺序关闭需要超时控制的对象
        // 这些对象在关闭过程中可能会向服务器发送请求，需要单独的超时控制
        if (coordinator != null) {
            // 关闭协调器，这是一个阻塞调用，受closeTimer剩余时间限制
            swallow(log, Level.ERROR, "Failed to close coordinator with a timeout(ms)=" + closeTimer.timeoutMs(), 
                    () -> coordinator.close(closeTimer), firstException);
        }

        if (fetcher != null) {
            // 计算剩余的超时时间，确保不超过请求超时时间
            long remainingDurationInTimeout = Math.max(0, timeout.toMillis() - closeTimer.elapsedMs());
            if (remainingDurationInTimeout > 0) {
                remainingDurationInTimeout = Math.min(requestTimeoutMs, remainingDurationInTimeout);
            }

            // 重置计时器，使用剩余的超时时间
            closeTimer.reset(remainingDurationInTimeout);

            // 关闭获取器，这是一个阻塞调用，受closeTimer剩余时间限制
            swallow(log, Level.ERROR, "Failed to close fetcher with a timeout(ms)=" + closeTimer.timeoutMs(), 
                    () -> fetcher.close(closeTimer), firstException);
        }

        // 按顺序关闭其他资源，这些操作通常不需要等待服务器响应
        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(client, "consumer network client", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
        clientTelemetryReporter.ifPresent(reporter -> 
                closeQuietly(reporter, "consumer telemetry reporter", firstException));

        // 注销JMX监控
        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");

        // 处理关闭过程中可能发生的异常
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    /**
     * 将获取位置设置为已提交的位置（如果存在）
     * 或使用用户配置的偏移量重置策略进行重置。
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果认证失败。详见异常信息
     * @throws NoOffsetForPartitionException 如果给定分区没有存储偏移量且没有定义偏移量重置策略
     * @return 如果操作在超时前完成则返回true
     */
    private boolean updateFetchPositions(final Timer timer) {
        // 如果由于leader变更导致任何分区被截断，我们需要验证偏移量
        offsetFetcher.validatePositionsIfNeeded();

        // 检查并缓存所有订阅是否都有获取位置
        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        // 如果所有分区都有有效的获取位置，直接返回true
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // 如果存在没有有效位置且不在等待重置的分区，则需要获取已提交的偏移量
        // 我们只在有分区缺失位置时才进行协调器查找，因此手动分配分区的消费者
        // 可以通过始终确保分配的分区有初始位置来避免对协调器的依赖
        if (coordinator != null && !coordinator.initWithCommittedOffsetsIfNeeded(timer)) return false;

        // 如果仍有分区需要位置且定义了重置策略，则使用默认策略请求重置
        // 如果没有定义重置策略且存在缺失位置的分区，则会抛出异常
        subscriptions.resetInitializingPositions();

        // 最后发送异步请求来查找和更新任何等待重置的分区的位置
        offsetFetcher.resetPositionsIfNeeded();

        return true;
    }

    /**
     * 获取轻量级锁并确保消费者未被关闭。
     * 这是一个复合操作，首先获取锁，然后检查消费者状态。
     * 
     * @throws IllegalStateException 如果消费者已经被关闭
     */
    private void acquireAndEnsureOpen() {
        // 首先尝试获取锁
        acquire();
        // 检查消费者是否已关闭
        if (this.closed) {
            // 如果已关闭，释放锁并抛出异常
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * 获取保护消费者免受多线程访问的轻量级锁。
     * 与传统锁不同，当锁不可用时不会阻塞，而是直接抛出异常（因为不支持多线程使用）。
     * 这种设计确保了消费者的线程安全性，同时避免了死锁的可能性。
     *
     * @throws ConcurrentModificationException 如果另一个线程已经持有锁
     */
    private void acquire() {
        // 获取当前线程信息
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        
        // 检查当前线程是否可以获取锁
        // 条件1：当前线程已持有锁（threadId == currentThread.get()）
        // 条件2：没有线程持有锁，且当前线程成功获取锁（currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)）
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                    "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                    " otherThread(id: " + currentThread.get() + ")"
            );
        
        // 增加引用计数，支持同一线程的重入
        refcount.incrementAndGet();
    }

    /**
     * 释放保护消费者免受多线程访问的轻量级锁。
     * 这个方法实现了一个引用计数机制，只有当引用计数降为0时才真正释放锁。
     * 这种设计允许同一个线程多次获取锁（重入），同时确保了线程安全性。
     */
    private void release() {
        // 减少引用计数，如果降为0则释放锁
        if (refcount.decrementAndGet() == 0)
            // 将当前线程ID重置为无线程状态
            currentThread.set(NO_CURRENT_THREAD);
    }

    /**
     * 检查是否配置了分区分配器，如果没有配置则抛出异常。
     * 这是一个安全检查方法，确保消费者在进行分区分配操作前已正确配置了分配策略。
     *
     * @throws IllegalStateException 如果没有配置任何分区分配器
     */
    private void throwIfNoAssignorsConfigured() {
        // 检查分配器列表是否为空
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    /**
     * 检查是否设置了有效的消费者组ID，如果没有则抛出异常。
     * 此方法在执行需要消费者组ID的操作（如组管理或偏移量提交）之前调用。
     *
     * @throws InvalidGroupIdException 如果没有配置消费者组ID
     */
    private void maybeThrowInvalidGroupIdException() {
        // 检查groupId是否为空
        if (groupId.isEmpty())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }

    /**
     * 如果提供的epoch比当前记录的更新，则更新最后看到的epoch值。
     * 这个方法用于跟踪分区leader的变化，帮助确保消息的一致性。
     *
     * @param topicPartition 要更新的主题分区
     * @param offsetAndMetadata 包含leader epoch信息的偏移量元数据
     */
    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        // 只有在offsetAndMetadata不为null时才进行更新
        if (offsetAndMetadata != null)
            // 如果存在leader epoch，则更新最后看到的epoch
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    // Functions below are for testing only
    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public Metrics metricsRegistry() {
        return metrics;
    }

    @Override
    public KafkaConsumerMetrics kafkaConsumerMetrics() {
        return kafkaConsumerMetrics;
    }

    @Override
    public boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }
}
