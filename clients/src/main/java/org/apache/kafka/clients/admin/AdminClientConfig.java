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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * AdminClient配置类，包含了所有Kafka管理客户端的配置项常量。
 * 该类定义了AdminClient与Kafka集群交互所需的各种配置，主要包括：
 * 1. 集群连接配置：引导服务器、控制器、DNS查找等
 * 2. 网络通信配置：连接超时、请求超时、缓冲区大小等
 * 3. 重试和重连机制：退避策略、最大重试次数等
 * 4. 度量指标配置：采样窗口、记录级别、推送开关等
 * 5. 安全配置：安全协议、SSL/SASL支持等
 * 6. 元数据管理：最大有效期、恢复策略等
 * 
 * 应用场景：
 * - 用于创建、配置和管理Kafka主题
 * - 管理ACL权限和配额
 * - 管理消费者组
 * - 执行分区重分配
 * - 获取集群状态信息
 */
public class AdminClientConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /**
     * <code>bootstrap.servers</code>
     * 引导服务器配置，用于建立与Kafka集群的初始连接。
     * 格式为host1:port1,host2:port2,...
     * 生产环境建议配置多个服务器地址，以提高可用性。
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    private static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

    /**
     * <code>bootstrap.controllers</code>
     * KRaft控制器引导配置，用于在KRaft模式下建立与控制器的初始连接。
     * 格式为host1:port1,host2:port2,...
     * 仅在使用KRaft（Kafka Raft元数据）模式时需要配置。
     */
    public static final String BOOTSTRAP_CONTROLLERS_CONFIG = "bootstrap.controllers";
    public static final String BOOTSTRAP_CONTROLLERS_DOC = "A list of host/port pairs to use for establishing the initial " +
            "connection to the KRaft controller quorum. This list should be in the form <code>host1:port1,host2:port2,...</code>.";

    /**
     * <code>client.dns.lookup</code>
     * 客户端DNS查找配置，控制如何解析主机名。
     * - use_all_dns_ips：轮询使用DNS返回的所有IP地址
     * - resolve_canonical_bootstrap_servers_only：仅解析引导服务器的规范名称
     */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;
    private static final String CLIENT_DNS_LOOKUP_DOC = CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC;

    /**
     * <code>reconnect.backoff.ms</code>
     * 重连退避时间配置，指定在重试连接到特定主机之前等待的基础时间（毫秒）。
     * 默认值为50ms，避免在连接失败时立即重试，减轻服务器压力。
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;
    private static final String RECONNECT_BACKOFF_MS_DOC = CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC;

    /**
     * <code>reconnect.backoff.max.ms</code>
     * 最大重连退避时间配置，指定重连退避时间的上限（毫秒）。
     * 默认值为1000ms，防止重试间隔无限增长。
     * 实际重试间隔会指数增长，但不超过此值，并添加20%的随机抖动。
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;
    private static final String RECONNECT_BACKOFF_MAX_MS_DOC = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC;

    /**
     * <code>retry.backoff.ms</code>
     * 重试退避时间配置，指定在重试失败的请求之前等待的基础时间（毫秒）。
     * 默认值为100ms，用于控制重试请求的频率。
     * 主要用于处理可重试的临时错误，如Leader选举期间的请求。
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    private static final String RETRY_BACKOFF_MS_DOC = CommonClientConfigs.RETRY_BACKOFF_MS_DOC;

    /**
     * <code>retry.backoff.max.ms</code>
     * 最大重试退避时间配置，指定重试退避时间的上限（毫秒）。
     * 默认值为1000ms，防止重试间隔过长影响操作响应时间。
     * 如果retry.backoff.ms大于此值，则直接使用此值作为固定重试间隔。
     */
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG;
    private static final String RETRY_BACKOFF_MAX_MS_DOC = CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC;

    /**
     * <code>enable.metrics.push</code>
     * 指标推送开关配置，控制是否启用将客户端指标推送到Kafka集群。
     * 默认为false，设置为true时会将指标数据推送给订阅了相应指标的集群。
     * 用于集中化的指标收集和监控。
     */
    public static final String ENABLE_METRICS_PUSH_CONFIG = CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG;
    public static final String ENABLE_METRICS_PUSH_DOC = CommonClientConfigs.ENABLE_METRICS_PUSH_DOC;

    /** 
     * <code>socket.connection.setup.timeout.ms</code>
     * 套接字连接建立超时配置，指定等待TCP连接建立的时间（毫秒）。
     * 如果在超时前未能建立连接，客户端会关闭连接并进行重试。
     * 默认值为10000ms（10秒）。
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /** 
     * <code>socket.connection.setup.timeout.max.ms</code>
     * 最大套接字连接建立超时配置，指定连接超时时间的上限（毫秒）。
     * 实际超时时间会在失败后指数增长，但不超过此值。
     * 默认值为30000ms（30秒）。
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /** 
     * <code>connections.max.idle.ms</code>
     * 连接最大空闲时间配置，指定空闲连接被关闭前的最长时间（毫秒）。
     * 默认值为300000ms（5分钟），用于释放长时间未使用的连接资源。
     * 设置为-1表示禁用空闲连接关闭。
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
    private static final String CONNECTIONS_MAX_IDLE_MS_DOC = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC;

    /** 
     * <code>request.timeout.ms</code>
     * 请求超时配置，指定等待请求响应的最长时间（毫秒）。
     * 默认值为30000ms（30秒）。
     * 如果在超时前未收到响应，客户端会重试请求或报告失败。
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /** 
     * <code>client.id</code>
     * 客户端标识配置，用于标识AdminClient实例。
     * 此ID会在请求日志中显示，便于追踪请求来源。
     * 默认为空字符串，建议设置为有意义的应用程序名称。
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
    private static final String CLIENT_ID_DOC = CommonClientConfigs.CLIENT_ID_DOC;

    /** 
     * <code>metadata.max.age.ms</code>
     * 元数据最大有效期配置，指定强制刷新元数据的时间间隔（毫秒）。
     * 默认值为300000ms（5分钟）。
     * 即使没有发现分区Leader变更，也会在此时间后刷新元数据，以发现新增的broker或分区。
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /** 
     * <code>send.buffer.bytes</code>
     * TCP发送缓冲区大小配置（字节）。
     * 默认值为128KB，影响网络I/O的吞吐量。
     * 设置为-1时使用操作系统默认值。
     * 较大的缓冲区有助于提高网络吞吐量，但会增加内存使用。
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;
    private static final String SEND_BUFFER_DOC = CommonClientConfigs.SEND_BUFFER_DOC;

    /** 
     * <code>receive.buffer.bytes</code>
     * TCP接收缓冲区大小配置（字节）。
     * 默认值为64KB，影响网络I/O的吞吐量。
     * 设置为-1时使用操作系统默认值。
     * 较大的缓冲区有助于提高网络吞吐量，但会增加内存使用。
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;
    private static final String RECEIVE_BUFFER_DOC = CommonClientConfigs.RECEIVE_BUFFER_DOC;

    /** 
     * <code>metric.reporters</code>
     * 指标报告器类配置，指定用于收集和报告指标的类列表。
     * 默认使用JMX报告器（org.apache.kafka.common.metrics.JmxReporter）。
     * 可以实现MetricsReporter接口来自定义指标收集和报告方式。
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    private static final String METRIC_REPORTER_CLASSES_DOC = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;

    /** 
     * <code>metrics.num.samples</code>
     * 指标样本数量配置，指定用于计算指标的样本数。
     * 默认值为2，样本数越多，指标计算越平滑，但内存占用也越大。
     * 影响指标的统计精度和内存使用。
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    private static final String METRICS_NUM_SAMPLES_DOC = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC;

    /** 
     * <code>metrics.sample.window.ms</code>
     * 指标采样窗口配置，指定计算指标样本的时间窗口（毫秒）。
     * 默认值为30000ms（30秒）。
     * 较大的窗口可以得到更平滑的指标，但会降低对突发事件的敏感度。
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    private static final String METRICS_SAMPLE_WINDOW_MS_DOC = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC;

    /** 
     * <code>metrics.recording.level</code>
     * 指标记录级别配置，控制收集指标的详细程度。
     * 支持三个级别：
     * - INFO：只记录基本的性能监控指标
     * - DEBUG：记录更详细的指标，用于问题诊断
     * - TRACE：记录所有可能的指标，适用于深入分析
     * 默认为INFO级别，较高级别会增加系统开销。
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /** 
     * <code>security.protocol</code>
     * 安全协议配置，指定与Kafka集群通信使用的协议。
     * 支持以下协议：
     * - PLAINTEXT：无安全认证（默认）
     * - SSL：使用SSL/TLS加密
     * - SASL_PLAINTEXT：使用SASL认证
     * - SASL_SSL：同时使用SASL认证和SSL加密
     */
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    public static final String DEFAULT_SECURITY_PROTOCOL = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
    private static final String SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;
    private static final String METRICS_RECORDING_LEVEL_DOC = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC;

    /** 
     * <code>retries</code>
     * 重试次数配置，指定请求失败时的最大重试次数。
     * 默认值为Integer.MAX_VALUE，建议设置为0或最大值。
     * 配合request.timeout.ms使用来控制重试行为。
     */
    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;

    /** 
     * <code>default.api.timeout.ms</code>
     * 默认API超时配置，指定AdminClient操作的默认超时时间（毫秒）。
     * 默认值为60000ms（60秒）。
     * 如果单个操作未指定超时时间，将使用此值。
     */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;

    /** 
     * <code>metadata.recovery.strategy</code>
     * 元数据恢复策略配置，指定在元数据获取失败时的恢复策略。
     * 支持以下策略：
     * - REBOOTSTRAP：重新连接引导服务器获取元数据
     * - RETRY：继续使用当前连接重试获取元数据
     * 默认使用RETRY策略，在网络不稳定时更保守。
     */
    public static final String METADATA_RECOVERY_STRATEGY_CONFIG = CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG;
    public static final String METADATA_RECOVERY_STRATEGY_DOC = CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC;
    public static final String DEFAULT_METADATA_RECOVERY_STRATEGY = CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY;

    /** 
     * <code>metadata.recovery.rebootstrap.trigger.ms</code>
     * 元数据恢复重引导触发时间配置（毫秒）。
     * 当使用REBOOTSTRAP策略时，在此时间后触发重新连接引导服务器。
     * 默认值为60000ms（60秒）。
     * 设置为0表示立即触发重引导。
     */
    public static final String METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG = CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG;
    public static final String METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC = CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC;
    public static final long DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS = CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS;

    /**
     * <code>security.providers</code>
     * 安全提供者配置，指定用于SSL/SASL的安全提供者类。
     * 可以配置自定义的安全提供者来扩展认证和加密功能。
     * 默认为null，使用JVM默认的安全提供者。
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /**
     * 静态初始化块，用于定义AdminClient的所有配置项。
     * 每个配置项都通过ConfigDef.define()方法定义，包含以下属性：
     * - 配置名称：用于在属性文件中指定该配置
     * - 数据类型：如STRING、INT、LONG、LIST等
     * - 默认值：当用户未指定时使用的值
     * - 重要性：HIGH、MEDIUM、LOW，用于文档生成和配置验证
     * - 文档说明：配置项的详细描述
     * - 取值范围：可选，用于验证配置值的合法性
     */
    static {
        // 初始化配置定义对象
        CONFIG = new ConfigDef()
                // 引导服务器配置，类型为服务器列表，重要性高
                .define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.LIST,
                                        "",  // 默认值为空，必须由用户指定
                                        Importance.HIGH,  // 重要性：高
                                        BOOTSTRAP_SERVERS_DOC).
                                 // KRaft控制器配置，类型为控制器列表，重要性高
                                 define(BOOTSTRAP_CONTROLLERS_CONFIG,
                                         Type.LIST,
                                         "",  // 默认值为空
                                         Importance.HIGH,  // 重要性：高
                                         BOOTSTRAP_CONTROLLERS_DOC)
                                                                // 客户端ID配置，用于标识请求来源
                                .define(CLIENT_ID_CONFIG, 
                                        Type.STRING,  // 类型：字符串
                                        "",  // 默认值为空字符串
                                        Importance.MEDIUM,  // 重要性：中
                                        CLIENT_ID_DOC)
                                                                // 元数据最大有效期配置
                                .define(METADATA_MAX_AGE_CONFIG, 
                                        Type.LONG,  // 类型：长整型
                                        5 * 60 * 1000,  // 默认值：5分钟
                                        atLeast(0),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        METADATA_MAX_AGE_DOC)
                                                                // TCP发送缓冲区大小配置
                                .define(SEND_BUFFER_CONFIG, 
                                        Type.INT,  // 类型：整型
                                        128 * 1024,  // 默认值：128KB
                                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),  // 取值范围：大于等于最小值
                                        Importance.MEDIUM,  // 重要性：中
                                        SEND_BUFFER_DOC)
                                                                // TCP接收缓冲区大小配置
                                .define(RECEIVE_BUFFER_CONFIG, 
                                        Type.INT,  // 类型：整型
                                        64 * 1024,  // 默认值：64KB
                                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),  // 取值范围：大于等于最小值
                                        Importance.MEDIUM,  // 重要性：中
                                        RECEIVE_BUFFER_DOC)
                                                                // 重连退避时间配置
                                .define(RECONNECT_BACKOFF_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        50L,  // 默认值：50毫秒
                                        atLeast(0L),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        RECONNECT_BACKOFF_MS_DOC)
                                                                // 最大重连退避时间配置
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        1000L,  // 默认值：1秒
                                        atLeast(0L),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        RECONNECT_BACKOFF_MAX_MS_DOC)
                                                                // 重试退避时间配置
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,  // 默认值：100毫秒
                                        atLeast(0L),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        RETRY_BACKOFF_MS_DOC)
                                                                // 最大重试退避时间配置
                                .define(RETRY_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,  // 默认值：1秒
                                        atLeast(0L),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        RETRY_BACKOFF_MAX_MS_DOC)
                                                                // 指标推送开关配置
                                .define(ENABLE_METRICS_PUSH_CONFIG,
                                        Type.BOOLEAN,  // 类型：布尔值
                                        false,  // 默认值：关闭
                                        Importance.LOW,  // 重要性：低
                                        ENABLE_METRICS_PUSH_DOC)
                                                                // 请求超时配置
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,  // 类型：整型
                                        30000,  // 默认值：30秒
                                        atLeast(0),  // 取值范围：大于等于0
                                        Importance.MEDIUM,  // 重要性：中
                                        REQUEST_TIMEOUT_MS_DOC)
                                                                // 套接字连接建立超时配置
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,  // 默认值：10秒
                                        Importance.MEDIUM,  // 重要性：中
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                                                                // 最大套接字连接建立超时配置
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,  // 默认值：30秒
                                        Importance.MEDIUM,  // 重要性：中
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                                                                // 连接最大空闲时间配置
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        5 * 60 * 1000,  // 默认值：5分钟
                                        Importance.MEDIUM,  // 重要性：中
                                        CONNECTIONS_MAX_IDLE_MS_DOC)
                                                                // 重试次数配置
                                .define(RETRIES_CONFIG,
                                        Type.INT,  // 类型：整型
                                        Integer.MAX_VALUE,  // 默认值：最大整数值
                                        between(0, Integer.MAX_VALUE),  // 取值范围：0到最大整数
                                        Importance.LOW,  // 重要性：低
                                        CommonClientConfigs.RETRIES_DOC)
                                                                // 默认API超时配置
                                .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                                        Type.INT,  // 类型：整型
                                        60000,  // 默认值：60秒
                                        atLeast(0),  // 取值范围：大于等于0
                                        Importance.MEDIUM,  // 重要性：中
                                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC)
                                                                // 指标采样窗口配置
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        30000,  // 默认值：30秒
                                        atLeast(0),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        METRICS_SAMPLE_WINDOW_MS_DOC)
                                                                // 指标样本数量配置
                                .define(METRICS_NUM_SAMPLES_CONFIG, 
                                        Type.INT,  // 类型：整型
                                        2,  // 默认值：2个样本
                                        atLeast(1),  // 取值范围：大于等于1
                                        Importance.LOW,  // 重要性：低
                                        METRICS_NUM_SAMPLES_DOC)
                                                                // 指标报告器类配置
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,  // 类型：列表
                                        JmxReporter.class.getName(),  // 默认值：JMX报告器
                                        Importance.LOW,  // 重要性：低
                                        METRIC_REPORTER_CLASSES_DOC)
                                                                // 指标记录级别配置
                                .define(METRICS_RECORDING_LEVEL_CONFIG,
                                        Type.STRING,  // 类型：字符串
                                        Sensor.RecordingLevel.INFO.toString(),  // 默认值：INFO级别
                                        in(Sensor.RecordingLevel.INFO.toString(),  // 取值范围：INFO、DEBUG、TRACE
                                           Sensor.RecordingLevel.DEBUG.toString(), 
                                           Sensor.RecordingLevel.TRACE.toString()),
                                        Importance.LOW,  // 重要性：低
                                        METRICS_RECORDING_LEVEL_DOC)
                                                                // DNS查找配置
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,  // 类型：字符串
                                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),  // 默认值：使用所有DNS IP
                                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),  // 取值范围：使用所有DNS IP或仅解析规范引导服务器
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,  // 重要性：中
                                        CLIENT_DNS_LOOKUP_DOC)
                                // security support
                                                                // 安全提供者配置
                                .define(SECURITY_PROVIDERS_CONFIG,
                                        Type.STRING,  // 类型：字符串
                                        null,  // 默认值：null，使用JVM默认提供者
                                        Importance.LOW,  // 重要性：低
                                        SECURITY_PROVIDERS_DOC)
                                                                // 安全协议配置
                                .define(SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,  // 类型：字符串
                                        DEFAULT_SECURITY_PROTOCOL,  // 默认值：PLAINTEXT
                                        ConfigDef.CaseInsensitiveValidString  // 取值范围：SecurityProtocol枚举值
                                                .in(Utils.enumOptions(SecurityProtocol.class)),
                                        Importance.MEDIUM,  // 重要性：中
                                        SECURITY_PROTOCOL_DOC)
                                                                // 添加SSL支持，包含所有SSL相关配置
                                .withClientSslSupport()
                                // 添加SASL支持，包含所有SASL相关配置
                                .withClientSaslSupport()
                                                                // 元数据恢复策略配置
                                .define(METADATA_RECOVERY_STRATEGY_CONFIG,
                                        Type.STRING,  // 类型：字符串
                                        DEFAULT_METADATA_RECOVERY_STRATEGY,  // 默认值：RETRY
                                        ConfigDef.CaseInsensitiveValidString  // 取值范围：MetadataRecoveryStrategy枚举值
                                                .in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                                        Importance.LOW,  // 重要性：低
                                        METADATA_RECOVERY_STRATEGY_DOC)
                                                                // 元数据恢复重引导触发时间配置
                                .define(METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                                        Type.LONG,  // 类型：长整型
                                        DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,  // 默认值：60秒
                                        atLeast(0),  // 取值范围：大于等于0
                                        Importance.LOW,  // 重要性：低
                                        METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC);
    }

    /**
     * 处理解析后的配置值，执行以下操作：
     * 1. 验证SASL机制配置的有效性
     * 2. 检查并警告是否禁用了指数退避机制
     * 3. 处理重连退避相关的配置
     *
     * @param parsedValues 已解析的配置值Map
     * @return 处理后的配置值Map
     */
    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        // 验证SASL机制配置
        CommonClientConfigs.postValidateSaslMechanismConfig(this);
        // 检查是否禁用了指数退避，如果禁用则输出警告日志
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        // 处理重连退避配置，确保配置值在合理范围内
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    /**
     * AdminClientConfig的主要构造函数
     * 用于创建新的AdminClient配置实例
     *
     * @param props 配置属性Map，包含所有AdminClient的配置项
     */
    public AdminClientConfig(Map<?, ?> props) {
        this(props, false);
    }

    /**
     * AdminClientConfig的受保护构造函数
     * 允许子类控制是否启用配置日志记录
     *
     * @param props 配置属性Map，包含所有AdminClient的配置项
     * @param doLog 是否启用配置处理的日志记录
     */
    protected AdminClientConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    /**
     * 获取所有可用的配置项名称
     * 用于配置验证和文档生成
     *
     * @return 包含所有配置项名称的Set集合
     */
    public static Set<String> configNames() {
        return CONFIG.names();
    }

    /**
     * 获取AdminClient的配置定义
     * 用于配置验证和动态配置管理
     *
     * @return AdminClient的配置定义对象
     */
    public static ConfigDef configDef() {
        return  new ConfigDef(CONFIG);
    }

    /**
     * 生成AdminClient配置的HTML文档
     * 主要用于自动生成配置文档
     *
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        // 生成HTML格式的配置文档，使用adminclientconfigs_前缀
        System.out.println(CONFIG.toHtml(4, config -> "adminclientconfigs_" + config));
    }

}
