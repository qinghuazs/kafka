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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @class ConsumerUtils
 * @brief 消费者相关的工具类，提供创建消费者组件、配置解析等静态方法。
 * @details 这个类是 final 的，不能被继承。它包含了消费者客户端内部使用的一些常量和辅助方法。
 * 应用场景：在 Kafka 消费者客户端的初始化和运行过程中，用于创建网络客户端、日志上下文、度量系统等核心组件，以及解析配置项。
 * 设计考虑：将这些通用的辅助功能和常量集中管理，便于维护和复用，避免在多个类中重复实现。
 */
public final class ConsumerUtils {

    /**
     * @field THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED
     * @brief 配置项名称，用于指示当获取稳定偏移量不受支持时是否抛出异常。
     * @details 此配置项在 {@link ConsumerConfig} 中只有包级可见性，因此在主要使用它的 internals 包中无法直接访问。
     *          曾尝试移动相关代码，但最终认为保持现状更好。
     * 应用场景：控制消费者在遇到不支持获取稳定偏移量的情况时的行为。
     * 设计考虑：作为一个内部配置项，用于在特定场景下调整消费者的容错行为。
     */
    static final String THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = "internal.throw.on.fetch.stable.offset.unsupported";
    /**
     * @field DEFAULT_CLOSE_TIMEOUT_MS
     * @brief 默认的关闭超时时间（毫秒）。
     * @details 消费者关闭操作的默认等待时间，值为 30 秒。
     * 应用场景：在关闭消费者时，等待相关资源释放的最长时间。
     * 设计考虑：提供一个合理的默认值，避免无限期等待导致程序卡死。
     */
    public static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;
    /**
     * @field CONSUMER_JMX_PREFIX
     * @brief 消费者 JMX 指标的前缀。
     * @details 用于 JMX 监控的消费者相关指标的命名空间前缀，值为 "kafka.consumer"。
     * 应用场景：通过 JMX 监控 Kafka 消费者的运行状态和性能指标。
     * 设计考虑：定义统一的 JMX 前缀，方便管理和识别消费者相关的指标。
     */
    public static final String CONSUMER_JMX_PREFIX = "kafka.consumer";
    /**
     * @field CONSUMER_METRIC_GROUP_PREFIX
     * @brief 消费者度量组的前缀。
     * @details 用于度量系统的消费者相关指标组的名称前缀，值为 "consumer"。
     * 应用场景：在度量系统中组织和分类消费者相关的性能指标。
     * 设计考虑：定义统一的度量组前缀，方便查找和分析消费者指标。
     */
    public static final String CONSUMER_METRIC_GROUP_PREFIX = "consumer";
    /**
     * @field CONSUMER_SHARE_METRIC_GROUP_PREFIX
     * @brief 共享消费者度量组的前缀。
     * @details 用于共享消费者（KIP-848 引入的 Share Consumer）的度量指标组的名称前缀，值为 "consumer-share"。
     * 应用场景：在度量系统中区分普通消费者和共享消费者的指标。
     * 设计考虑：为新的共享消费者特性提供独立的度量命名空间。
     */
    public static final String CONSUMER_SHARE_METRIC_GROUP_PREFIX = "consumer-share";
    /**
     * @field COORDINATOR_METRICS_SUFFIX
     * @brief 协调器度量指标的后缀。
     * @details 用于消费者协调器相关度量指标的名称后缀，值为 "-coordinator-metrics"。
     * 应用场景：标识与消费者协调器（如组管理、偏移量管理）相关的度量指标。
     * 设计考虑：通过后缀明确指标的归属，便于分析协调器性能。
     */
    public static final String COORDINATOR_METRICS_SUFFIX = "-coordinator-metrics";
    /**
     * @field CONSUMER_METRICS_SUFFIX
     * @brief 消费者度量指标的后缀。
     * @details 用于一般消费者度量指标的名称后缀，值为 "-metrics"。
     * 应用场景：标识通用的消费者度量指标。
     * 设计考虑：通过后缀明确指标的归属。
     */
    public static final String CONSUMER_METRICS_SUFFIX = "-metrics";
    /**
     * @field CONSUMER_METRIC_GROUP
     * @brief 完整的消费者度量组名称。
     * @details 由 {@link #CONSUMER_METRIC_GROUP_PREFIX} 和 {@link #CONSUMER_METRICS_SUFFIX} 组合而成，值为 "consumer-metrics"。
     * 应用场景：作为消费者核心度量指标的完整组名。
     * 设计考虑：提供一个便捷的常量来表示主要的消费者度量组。
     */
    public static final String CONSUMER_METRIC_GROUP = CONSUMER_METRIC_GROUP_PREFIX + CONSUMER_METRICS_SUFFIX;

    /**
     * @field CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION
     * @brief 每个连接允许的最大未完成请求数。
     * @details 一个固定的、足够大的值即可满足最大需求。这里设置为 100。
     * 应用场景：控制消费者与 Broker 之间单个连接上可以同时发送但未收到响应的请求数量，防止请求积压过多。
     * 设计考虑：平衡吞吐量和资源消耗。过小可能限制吞吐，过大可能消耗过多内存或导致请求超时。
     */
    public static final int CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100;

    /**
     * @field CONSUMER_CLIENT_ID_METRIC_TAG
     * @brief 消费者客户端 ID 的度量标签名称。
     * @details 用于在度量系统中标记客户端 ID 的标签键，值为 "client-id"。
     * 应用场景：在度量指标中通过客户端 ID 区分不同的消费者实例。
     * 设计考虑：提供标准的标签名，方便按客户端 ID 聚合和过滤指标。
     */
    private static final String CONSUMER_CLIENT_ID_METRIC_TAG = "client-id";
    /**
     * @field log
     * @brief 日志记录器实例。
     * @details 使用 SLF4J API 获取的 Logger 对象，用于记录 ConsumerUtils 类相关的日志信息。
     * 应用场景：记录该工具类在运行过程中的重要事件、警告或错误信息，便于调试和监控。
     * 设计考虑：使用标准的日志框架，方便集成和管理日志输出。
     */
    private static final Logger log = LoggerFactory.getLogger(ConsumerUtils.class);

    /**
     * @method createConsumerNetworkClient
     * @brief 创建并初始化一个 {@link ConsumerNetworkClient} 实例。
     * @details 此方法封装了创建 {@link ConsumerNetworkClient} 的逻辑，该客户端负责处理消费者的网络通信。
     *          它首先使用 {@link ClientUtils#createNetworkClient} 创建一个底层的 {@link NetworkClient}，
     *          然后根据消费者配置（如心跳间隔）包装成 {@link ConsumerNetworkClient}。
     * @param config 消费者配置对象，包含网络、超时等相关设置。
     * @param metrics 度量系统实例，用于收集网络客户端相关的指标。
     * @param logContext 日志上下文，用于在日志中区分不同的消费者实例。
     * @param apiVersions API 版本管理器，用于跟踪 Broker 支持的 API 版本。
     * @param time 时间工具类实例，用于获取当前时间等操作。
     * @param metadata Kafka 集群元数据管理器。
     * @param throttleTimeSensor 节流时间传感器，用于监控请求被节流的时间。
     * @param retryBackoffMs 重试退避时间（毫秒）。
     * @param clientTelemetrySender 客户端遥测数据发送器。
     * @return 初始化完成的 {@link ConsumerNetworkClient} 实例。
     * 应用场景：在消费者启动时，创建用于与 Kafka Broker 通信的网络客户端。
     * 设计考虑：将网络客户端的创建逻辑集中在此方法中，简化消费者的初始化过程，并确保所有必要的参数都被正确配置。
     */
    public static ConsumerNetworkClient createConsumerNetworkClient(ConsumerConfig config,
                                                                    Metrics metrics,
                                                                    LogContext logContext,
                                                                    ApiVersions apiVersions,
                                                                    Time time,
                                                                    Metadata metadata,
                                                                    Sensor throttleTimeSensor,
                                                                    long retryBackoffMs,
                                                                    ClientTelemetrySender clientTelemetrySender) {
        // 调用 ClientUtils 工具类创建底层的 NetworkClient 实例
        // 参数包括：消费者配置、度量系统、度量组前缀、日志上下文、API版本、时间工具、最大并发请求数、元数据、节流传感器、遥测发送器
        NetworkClient netClient = ClientUtils.createNetworkClient(config, // 传入消费者配置
                metrics, // 传入度量系统实例
                CONSUMER_METRIC_GROUP_PREFIX, // 指定消费者度量组前缀
                logContext, // 传入日志上下文
                apiVersions, // 传入API版本管理器
                time, // 传入时间工具实例
                CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION, // 设置每个连接的最大并发请求数
                metadata, // 传入集群元数据管理器
                throttleTimeSensor, // 传入节流时间传感器
                clientTelemetrySender); // 传入客户端遥测数据发送器

        // 获取心跳间隔时间配置，单位为毫秒
        // 目的是避免长时间阻塞，防止心跳线程饿死
        // Will avoid blocking an extended period of time to prevent heartbeat thread starvation (原始注释：将避免长时间阻塞以防止心跳线程饿死)
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG); // 从配置中读取心跳间隔

        // 创建并返回 ConsumerNetworkClient 实例
        // ConsumerNetworkClient 是对 NetworkClient 的封装，增加了消费者特定的逻辑
        return new ConsumerNetworkClient(
                logContext, // 传入日志上下文
                netClient, // 传入底层网络客户端
                metadata, // 传入集群元数据管理器
                time, // 传入时间工具实例
                retryBackoffMs, // 传入重试退避时间
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), // 从配置中读取请求超时时间
                heartbeatIntervalMs); // 传入心跳间隔时间
    }

    /**
     * @method createLogContext
     * @brief 创建一个 {@link LogContext} 实例，用于在日志中提供消费者相关的上下文信息。
     * @details 日志上下文通常包含客户端 ID、消费者组 ID 以及可能的实例 ID，方便在聚合日志中定位特定消费者的日志。
     * @param config 消费者配置对象，用于获取客户端 ID。
     * @param groupRebalanceConfig 消费者组重平衡配置对象，用于获取消费者组 ID 和实例 ID。
     * @return 创建好的 {@link LogContext} 实例。
     * 应用场景：在消费者初始化的早期阶段创建，并传递给后续需要记录日志的组件。
     * 设计考虑：提供一个标准化的日志前缀格式，有助于日志的规范化和可读性。根据是否有实例 ID，生成不同格式的日志前缀。
     */
    public static LogContext createLogContext(ConsumerConfig config, GroupRebalanceConfig groupRebalanceConfig) {
        // 从 groupRebalanceConfig 中获取 groupId，可能为 null，因此使用 Optional 包装
        Optional<String> groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        // 从消费者配置中获取 clientId
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        // 检查 groupRebalanceConfig 中是否设置了 groupInstanceId
        // If group.instance.id is set, we will append it to the log context. (原始注释：如果设置了 group.instance.id，我们会将其附加到日志上下文中。)
        if (groupRebalanceConfig.groupInstanceId.isPresent()) {
            // 如果存在 groupInstanceId，则日志上下文包含 instanceId, clientId, groupId
            return new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                    ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "); // groupId 为 null 时显示 "null"
        } else {
            // 如果不存在 groupInstanceId，则日志上下文包含 clientId, groupId
            return new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "); // groupId 为 null 时显示 "null"
        }
    }

    /**
     * @method configuredIsolationLevel
     * @brief 从消费者配置中获取并解析配置的事务隔离级别。
     * @details 读取 {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG} 配置项，并将其转换为 {@link IsolationLevel} 枚举值。
     * @param config 消费者配置对象。
     * @return 解析得到的 {@link IsolationLevel} 枚举值。
     * 应用场景：确定消费者在读取事务性消息时的行为，例如是读取已提交的事务消息 (READ_COMMITTED) 还是所有消息 (READ_UNCOMMITTED)。
     * 设计考虑：将配置字符串转换为强类型的枚举，提高代码的健壮性和可读性。配置值不区分大小写。
     */
    public static IsolationLevel configuredIsolationLevel(ConsumerConfig config) {
        // 从配置中获取隔离级别配置字符串，并转换为大写
        String s = config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT);
        // 将字符串转换为 IsolationLevel 枚举值
        return IsolationLevel.valueOf(s);
    }

    /**
     * @method createSubscriptionState
     * @brief 创建一个 {@link SubscriptionState} 实例，用于管理消费者的订阅状态和偏移量。
     * @details 此方法会从消费者配置中读取自动偏移量重置策略 (auto.offset.reset)，并用其初始化 {@link SubscriptionState}。
     * @param config 消费者配置对象。
     * @param logContext 日志上下文。
     * @return 初始化完成的 {@link SubscriptionState} 实例。
     * 应用场景：在消费者初始化时创建，用于跟踪消费者订阅的主题、分区以及这些分区的消费位移。
     * 设计考虑：将订阅状态的管理逻辑封装在 {@link SubscriptionState} 类中，此类负责根据配置初始化该状态对象。
     */
    public static SubscriptionState createSubscriptionState(ConsumerConfig config, LogContext logContext) {
        // 从配置中获取 "auto.offset.reset" 配置项的值
        String s = config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        // 将字符串形式的策略转换为 AutoOffsetResetStrategy 枚举值
        AutoOffsetResetStrategy strategy = AutoOffsetResetStrategy.fromString(s);
        // 使用日志上下文和解析得到的重置策略创建并返回一个新的 SubscriptionState 实例
        return new SubscriptionState(logContext, strategy);
    }

    /**
     * @method createMetrics
     * @brief 创建一个 {@link Metrics} 实例，用于收集和报告消费者的度量指标，使用默认的报告器。
     * @details 此方法是 {@link #createMetrics(ConsumerConfig, Time, List)} 的重载版本，
     *          它会使用 {@link CommonClientConfigs#metricsReporters(String, ConsumerConfig)} 获取默认配置的度量报告器列表。
     * @param config 消费者配置对象。
     * @param time 时间工具类实例。
     * @return 初始化完成的 {@link Metrics} 实例。
     * 应用场景：在消费者初始化时创建度量系统，用于监控消费者的各项性能指标。
     * 设计考虑：提供一个便捷的方法来创建带有默认报告器的度量系统。
     */
    public static Metrics createMetrics(ConsumerConfig config, Time time) {
        // 调用另一个 createMetrics 重载方法，传入通过 CommonClientConfigs 获取的默认度量报告器列表
        // CommonClientConfigs.metricsReporters 会根据配置实例化配置的 MetricsReporter
        return createMetrics(config, time, CommonClientConfigs.metricsReporters(
                config.getString(ConsumerConfig.CLIENT_ID_CONFIG), config)); // 获取客户端ID并传入配置
    }

    /**
     * @method createMetrics
     * @brief 创建并初始化一个 {@link Metrics} 实例，用于收集和报告消费者的度量指标。
     * @details 此方法根据消费者配置构建 {@link MetricConfig} 和 {@link MetricsContext}，然后使用这些配置创建 {@link Metrics} 对象。
     *          度量指标对于监控消费者性能和健康状况至关重要。
     * @param config 消费者配置对象，包含度量相关的设置，如采样数量、时间窗口、报告级别等。
     * @param time 时间工具类实例，用于度量系统内部的时间戳等操作。
     * @param reporters 度量报告器列表，用于将收集到的度量指标发送到外部系统（如 JMX、监控平台）。
     * @return 初始化完成的 {@link Metrics} 实例。
     * 应用场景：在消费者启动时，创建用于监控其运行状态的度量系统。
     * 设计考虑：将度量系统的创建逻辑封装起来，方便消费者初始化，并确保所有相关的配置都被正确应用。
     */
    public static Metrics createMetrics(ConsumerConfig config, Time time, List<MetricsReporter> reporters) {
        // 从配置中获取客户端ID
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        // 创建一个只包含客户端ID的标签映射，用于标记度量指标来源
        Map<String, String> metricsTags = Collections.singletonMap(CONSUMER_CLIENT_ID_METRIC_TAG, clientId);
        // 创建度量配置对象
        MetricConfig metricConfig = new MetricConfig()
                // 设置度量采样数量
                .samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                // 设置度量采样时间窗口和单位（毫秒）
                .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                // 设置度量记录级别 (INFO, DEBUG, TRACE)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                // 设置度量标签
                .tags(metricsTags);
        // 创建 Kafka 度量上下文，指定 JMX 前缀和从配置中提取的以 "metrics.context." 开头的原始配置项
        MetricsContext metricsContext = new KafkaMetricsContext(CONSUMER_JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        // 使用度量配置、报告器列表、时间工具和度量上下文创建并返回 Metrics 实例
        return new Metrics(metricConfig, reporters, time, metricsContext);
    }

    /**
     * @method createFetchMetricsManager
     * @brief 创建并初始化一个 {@link FetchMetricsManager} 实例，用于管理与消息拉取相关的度量指标。
     * @details 此方法首先创建一个 {@link FetchMetricsRegistry}，用于注册和组织拉取相关的度量指标，
     *          然后使用该注册表和传入的 {@link Metrics} 实例创建 {@link FetchMetricsManager}。
     * @param metrics 度量系统实例，{@link FetchMetricsManager} 将使用它来记录指标。
     * @return 初始化完成的 {@link FetchMetricsManager} 实例。
     * 应用场景：在消费者初始化时，创建用于监控消息拉取性能的管理器。
     * 设计考虑：将拉取相关的度量管理逻辑封装起来，便于集成到消费者中。
     */
    public static FetchMetricsManager createFetchMetricsManager(Metrics metrics) {
        // 创建一个只包含消费者客户端ID度量标签的集合
        Set<String> metricsTags = Collections.singleton(CONSUMER_CLIENT_ID_METRIC_TAG);
        // 创建 FetchMetricsRegistry 实例，用于注册和管理与 fetch 相关的指标
        // 参数为度量标签和消费者度量组前缀
        FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry(metricsTags, CONSUMER_METRIC_GROUP_PREFIX);
        // 使用 Metrics 实例和 FetchMetricsRegistry 实例创建并返回 FetchMetricsManager
        return new FetchMetricsManager(metrics, metricsRegistry);
    }

    /**
     * @method createShareFetchMetricsManager
     * @brief 创建并初始化一个 {@link ShareFetchMetricsManager} 实例，用于管理共享消费者（Share Consumer）的消息拉取相关的度量指标。
     * @details 此方法与 {@link #createFetchMetricsManager(Metrics)} 类似，但专门为共享消费者设计。
     *          它创建一个 {@link ShareFetchMetricsRegistry}，并使用它和传入的 {@link Metrics} 实例创建 {@link ShareFetchMetricsManager}。
     * @param metrics 度量系统实例，{@link ShareFetchMetricsManager} 将使用它来记录指标。
     * @return 初始化完成的 {@link ShareFetchMetricsManager} 实例。
     * 应用场景：在共享消费者（KIP-848）初始化时，创建用于监控其消息拉取性能的管理器。
     * 设计考虑：为共享消费者提供独立的度量管理机制，以区分于普通消费者的度量。
     */
    public static ShareFetchMetricsManager createShareFetchMetricsManager(Metrics metrics) {
        // 创建一个只包含消费者客户端ID度量标签的集合
        Set<String> metricsTags = Collections.singleton(CONSUMER_CLIENT_ID_METRIC_TAG);
        // 创建 ShareFetchMetricsRegistry 实例，用于注册和管理与共享消费者 fetch 相关的指标
        // 参数为度量标签和共享消费者度量组前缀
        ShareFetchMetricsRegistry metricsRegistry = new ShareFetchMetricsRegistry(metricsTags, CONSUMER_SHARE_METRIC_GROUP_PREFIX);
        // 使用 Metrics 实例和 ShareFetchMetricsRegistry 实例创建并返回 ShareFetchMetricsManager
        return new ShareFetchMetricsManager(metrics, metricsRegistry);
    }

    /**
     * @method configuredConsumerInterceptors
     * @brief 根据消费者配置加载并实例化配置的消费者拦截器列表。
     * @details 此方法利用 {@link ClientUtils#configuredInterceptors(ConsumerConfig, String, Class)} 通用方法，
     *          从 {@link ConsumerConfig#INTERCEPTOR_CLASSES_CONFIG} 配置项中获取拦截器类名列表，
     *          然后实例化这些拦截器并返回一个列表。拦截器允许用户在消息消费的不同阶段插入自定义逻辑。
     * @param <K> 消息键的类型。
     * @param <V> 消息值的类型。
     * @param config 消费者配置对象，其中包含 {@link ConsumerConfig#INTERCEPTOR_CLASSES_CONFIG} 配置项。
     * @return 配置的 {@link ConsumerInterceptor} 实例列表。如果未配置拦截器，则返回空列表。
     * 应用场景：在消费者初始化时，加载用户自定义的拦截器，用于消息的预处理、后处理、监控等。
     * 设计考虑：提供一种可插拔的机制，允许用户扩展消费者的行为，而无需修改核心代码。
     *           使用 {@code @SuppressWarnings("unchecked")} 是因为 {@link ClientUtils#configuredInterceptors} 返回的是原始列表，需要进行类型转换。
     */
    @SuppressWarnings("unchecked")
    public static <K, V> List<ConsumerInterceptor<K, V>> configuredConsumerInterceptors(ConsumerConfig config) {
        // 调用 ClientUtils.configuredInterceptors 方法加载和配置拦截器
        // 参数包括：消费者配置、拦截器类配置项的键名、拦截器接口的 Class 对象
        // 返回的是一个 Object 类型的列表，因此需要强制类型转换为 List<ConsumerInterceptor<K, V>>
        return (List<ConsumerInterceptor<K, V>>) ClientUtils.configuredInterceptors(config, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
    }

    /**
     * @method refreshCommittedOffsets
     * @brief 使用提供的已提交偏移量更新订阅状态和元数据：
     * <li>使用已提交的偏移量更新分区偏移量</li>
     * <li>如果已提交偏移量的元数据中发现了更新的 leader epoch，则使用它更新元数据</li>
     * </p>
     * 此方法将忽略 {@code offsetsAndMetadata} 参数中包含的任何可能不再分配给此消费者的分区。
     *
     * @param offsetsAndMetadata 用于更新订阅状态和元数据对象的已提交偏移量和元数据。
     * @param metadata           元数据对象，如果已提交偏移量的元数据中发现了新的 leader epoch，则会用其更新此对象。
     * @param subscriptions      订阅状态对象，将分区的偏移量设置为已提交的偏移量。
     * 应用场景：当消费者从 Broker 获取到已提交的偏移量信息后（例如，在消费者启动或重新平衡后），调用此方法来同步本地的订阅状态和元数据。
     *          这确保了消费者从正确的偏移量开始消费，并拥有关于分区 leader epoch 的最新信息，这对于防止数据丢失或重复非常重要。
     * 设计考虑：
     * - 原子性：虽然此方法本身不保证跨多个数据结构的原子更新，但它处理的是从 Broker 获取的快照信息，旨在将本地状态与远端状态对齐。
     * - 容错性：忽略不再分配的分区，可以处理分区分配发生变化的情况，增强了方法的鲁棒性。
     * - Leader Epoch 更新：通过更新 leader epoch，消费者可以检测到可能的数据截断，并采取相应的措施（例如，重置偏移量或抛出异常）。
     */
    public static void refreshCommittedOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata,
                                               final ConsumerMetadata metadata,
                                               final SubscriptionState subscriptions) {
        // 遍历所有提供的已提交偏移量和元数据条目
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            // 获取当前条目对应的主题分区
            final TopicPartition tp = entry.getKey();
            // 获取当前条目对应的偏移量和元数据
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            // 检查偏移量和元数据是否为 null (理论上不应该为 null，但作为防御性编程)
            if (offsetAndMetadata != null) {
                // 首先，如果需要，更新 leader epoch
                // 如果 offsetAndMetadata 中存在 leaderEpoch，则调用 metadata.updateLastSeenEpochIfNewer 更新元数据中该分区的 leader epoch
                entry.getValue().leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));

                // 当收到响应时，分区可能已经不再分配给此消费者，
                // 因此，如果是这种情况，我们需要忽略 seek 操作
                if (subscriptions.isAssigned(tp)) {
                    // 如果该分区仍然分配给当前消费者
                    // 获取该分区当前的 leader 和 epoch 信息
                    final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                    // 创建一个新的 FetchPosition 对象，包含已提交的偏移量、leader epoch (如果存在) 以及当前 leader 和 epoch
                    final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                            leaderAndEpoch);

                    // 调用 subscriptions.seekUnvalidated 方法，使用新的 position 更新该分区的消费位置
                    // "Unvalidated" 表示这个 seek 操作不会立即去验证偏移量是否有效，而是直接设置
                    subscriptions.seekUnvalidated(tp, position);

                    // 记录日志，表明已将分区的偏移量设置为已提交的偏移量
                    log.info("Setting offset for partition {} to the committed offset {}", tp, position);
                } else {
                    // 如果该分区已不再分配给当前消费者
                    // 记录日志，表明忽略返回的偏移量和元数据，因为其对应的分区已不再分配
                    log.info("Ignoring the returned {} since its partition {} is no longer assigned",
                            offsetAndMetadata, tp);
                }
            }
        }
    }

    /**
     * @method getResult
     * @brief 从 {@link Future} 对象中获取结果，带有指定的超时时间。
     * @details 此方法封装了 {@link Future#get(long, TimeUnit)} 的调用，并处理了可能发生的各种异常，
     *          将它们转换为 Kafka 特定的异常或标准的 Java 异常。
     * @param <T> Future 结果的类型。
     * @param future 要获取结果的 Future 对象。
     * @param timeoutMs 等待结果的超时时间（毫秒）。
     * @return Future 的结果。
     * @throws IllegalStateException 如果 Future 执行因 IllegalStateException 失败。
     * @throws KafkaException 如果 Future 执行因其他 Kafka 相关异常失败，或者底层异常被包装为 KafkaException。
     * @throws InterruptException 如果当前线程在等待期间被中断。
     * @throws TimeoutException 如果在指定的超时时间内未能获取到结果。
     * 应用场景：当需要从异步操作（表示为 Future）中获取结果，并且希望对等待时间进行控制，同时统一处理可能发生的异常时使用。
     * 设计考虑：
     * - 异常转换：将底层的 {@link ExecutionException}、{@link InterruptedException} 和 {@link java.util.concurrent.TimeoutException}
     *   转换为更具体的或 Kafka 相关的异常，便于上层调用者处理。
     * - {@link IllegalStateException} 的特殊处理：如果 {@link ExecutionException} 的原因是 {@link IllegalStateException}，则直接抛出该异常，
     *   这通常表示程序处于非法状态，而不是临时的执行错误。
     */
    public static <T> T getResult(Future<T> future, long timeoutMs) {
        try {
            // 尝试在指定的超时时间内获取 Future 的结果
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            // 如果在 Future 执行期间发生异常
            // 检查根本原因是否为 IllegalStateException
            if (e.getCause() instanceof IllegalStateException)
                // 如果是，则直接抛出 IllegalStateException
                throw (IllegalStateException) e.getCause();
            // 否则，将根本原因包装为 KafkaException 并抛出
            throw maybeWrapAsKafkaException(e.getCause());
        } catch (InterruptedException e) {
            // 如果当前线程在等待期间被中断
            // 抛出 Kafka 的 InterruptException
            throw new InterruptException(e);
        } catch (java.util.concurrent.TimeoutException e) {
            // 如果等待超时
            // 抛出 Kafka 的 TimeoutException
            throw new TimeoutException(e);
        }
    }

    /**
     * @method getResult
     * @brief 从 {@link Future} 对象中获取结果，使用 {@link Timer} 对象来管理超时。
     * @details 此方法是 {@link #getResult(Future, long)} 的一个重载版本，它接受一个 {@link Timer} 对象，
     *          并使用 {@link Timer#remainingMs()} 获取剩余的超时时间。
     * @param <T> Future 结果的类型。
     * @param future 要获取结果的 Future 对象。
     * @param timer 用于管理超时的 Timer 对象。
     * @return Future 的结果。
     * @throws KafkaException 如果 Future 执行失败或发生其他相关异常。
     * @throws InterruptException 如果当前线程在等待期间被中断。
     * @throws TimeoutException 如果在 Timer 剩余的时间内未能获取到结果。
     * 应用场景：当已经有一个 Timer 对象在跟踪某个操作的剩余时间时，可以使用此方法方便地从 Future 获取结果，并复用该 Timer 的超时逻辑。
     * 设计考虑：提供便利性，避免调用者手动从 Timer 获取剩余时间再调用另一个 getResult 方法。
     */
    public static <T> T getResult(Future<T> future, Timer timer) {
        // 调用另一个 getResult 方法，传入 Future 和从 Timer 获取的剩余毫秒数作为超时时间
        return getResult(future, timer.remainingMs());
    }

    /**
     * @method getResult
     * @brief 从 {@link Future} 对象中获取结果，无限期等待直到结果可用。
     * @details 此方法是 {@link #getResult(Future, long)} 的一个重载版本，它调用 {@link Future#get()}，
     *          会一直阻塞直到 Future 完成。同样，它会处理可能发生的异常并进行转换。
     * @param <T> Future 结果的类型。
     * @param future 要获取结果的 Future 对象。
     * @return Future 的结果。
     * @throws IllegalStateException 如果 Future 执行因 IllegalStateException 失败。
     * @throws KafkaException 如果 Future 执行因其他 Kafka 相关异常失败，或者底层异常被包装为 KafkaException。
     * @throws InterruptException 如果当前线程在等待期间被中断。
     * 应用场景：当需要阻塞等待异步操作完成，并且不关心超时时使用。
     * 设计考虑：提供一个简单的阻塞获取结果的方法，并统一异常处理逻辑。
     */
    public static <T> T getResult(Future<T> future) {
        try {
            // 尝试无限期等待并获取 Future 的结果
            return future.get();
        } catch (ExecutionException e) {
            // 如果在 Future 执行期间发生异常
            // 检查根本原因是否为 IllegalStateException
            if (e.getCause() instanceof IllegalStateException)
                // 如果是，则直接抛出 IllegalStateException
                throw (IllegalStateException) e.getCause();
            // 否则，将根本原因包装为 KafkaException 并抛出
            throw maybeWrapAsKafkaException(e.getCause());
        } catch (InterruptedException e) {
            // 如果当前线程在等待期间被中断
            // 抛出 Kafka 的 InterruptException
            throw new InterruptException(e);
        }
    }

    /**
     * @method maybeWrapAsKafkaException
     * @brief 如果给定的 {@link Throwable} 不是 {@link KafkaException} 的实例，则将其包装成 {@link KafkaException}。
     * @details 此辅助方法用于统一异常类型，确保上层调用者主要处理 KafkaException 及其子类。
     * @param t 要检查和可能包装的 Throwable 对象。
     * @return 如果 t 已经是 KafkaException，则直接返回 t；否则返回一个新的 KafkaException，其原因为 t。
     * 应用场景：在捕获到通用异常（如 {@link ExecutionException} 的 cause）后，将其转换为 Kafka 体系内的异常，方便统一处理。
     * 设计考虑：简化异常处理逻辑，使得调用者可以专注于处理 Kafka 相关的异常情况。
     */
    public static KafkaException maybeWrapAsKafkaException(Throwable t) {
        // 检查传入的 Throwable 是否已经是 KafkaException 的实例
        if (t instanceof KafkaException)
            // 如果是，则直接将其类型转换为 KafkaException 并返回
            return (KafkaException) t;
        else
            // 如果不是，则创建一个新的 KafkaException，并将原始的 Throwable 作为其原因 (cause)
            return new KafkaException(t);
    }

    /**
     * @method maybeWrapAsKafkaException
     * @brief 如果给定的 {@link Throwable} 不是 {@link KafkaException} 的实例，则将其包装成带有自定义消息的 {@link KafkaException}。
     * @details 此方法是 {@link #maybeWrapAsKafkaException(Throwable)} 的重载版本，允许在包装时提供一个自定义的错误消息。
     * @param t 要检查和可能包装的 Throwable 对象。
     * @param message 如果需要创建新的 KafkaException，则使用此消息。
     * @return 如果 t 已经是 KafkaException，则直接返回 t；否则返回一个新的 KafkaException，其消息为 message，原因为 t。
     * 应用场景：与上一个方法类似，但允许在包装异常时提供更具体的上下文信息。
     * 设计考虑：在转换异常的同时，能够保留或添加更丰富的错误描述。
     */
    public static KafkaException maybeWrapAsKafkaException(Throwable t, String message) {
        // 检查传入的 Throwable 是否已经是 KafkaException 的实例
        if (t instanceof KafkaException)
            // 如果是，则直接将其类型转换为 KafkaException 并返回
            return (KafkaException) t;
        else
            // 如果不是，则创建一个新的 KafkaException，使用提供的消息和原始的 Throwable 作为其原因 (cause)
            return new KafkaException(message, t);
    }
}
