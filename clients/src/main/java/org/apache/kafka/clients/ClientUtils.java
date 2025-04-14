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
package org.apache.kafka.clients;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

/**
 * Kafka客户端工具类，提供了一系列静态工具方法，用于处理客户端通用功能，包括：
 * 1. 解析和验证Kafka服务器地址
 * 2. 创建网络通道和客户端
 * 3. DNS解析和地址过滤
 * 4. 拦截器配置等
 */
public final class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    /**
     * 私有构造函数，防止实例化工具类
     */
    private ClientUtils() {
    }

    /**
     * 解析并验证Kafka服务器地址列表
     * 
     * @param config 客户端配置对象，包含bootstrap.servers和client.dns.lookup等配置
     * @return 解析后的服务器地址列表
     * @throws ConfigException 当地址格式无效或无法解析时抛出异常
     */
    public static List<InetSocketAddress> parseAndValidateAddresses(AbstractConfig config) {
        //获取bootstrap.servers配置的值，多个用逗号分隔，如：127.0.0.1:9092,otstrap.servers配置的值，多个用逗号分隔，如：IP_ADDRESS:9092,IP_ADDRESS:9093
        List<String> urls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        //获取 client.dns.lookup 配置的值，用于解析主机名
        String clientDnsLookupConfig = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG);
        //根据DNS查找配置解析并验证Kafka服务器地址列表
        return parseAndValidateAddresses(urls, clientDnsLookupConfig);
    }

    /**
     * 根据DNS查找配置解析并验证Kafka服务器地址列表
     * 
     * @param urls Kafka服务器URL列表
     * @param clientDnsLookupConfig DNS查找配置
     * @return 解析后的服务器地址列表
     */
    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, String clientDnsLookupConfig) {
        return parseAndValidateAddresses(urls, ClientDnsLookup.forConfig(clientDnsLookupConfig));
    }

    /**
     * 使用指定的DNS查找策略解析并验证Kafka服务器地址列表
     * 
     * @param urls Kafka服务器URL列表
     * @param clientDnsLookup DNS查找策略
     * @return 解析后的服务器地址列表
     * @throws ConfigException 当地址无效或无法解析时抛出异常
     */
    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, ClientDnsLookup clientDnsLookup) {
        // 创建一个列表用于存储解析后的服务器地址
        List<InetSocketAddress> addresses = new ArrayList<>();
        // 遍历所有提供的URL
        for (String url : urls) {
            // 检查URL是否有效（非空且非空字符串）
            if (url != null && !url.isEmpty()) {
                try {
                    // 从URL中提取主机名和端口号
                    String host = getHost(url);
                    Integer port = getPort(url);
                    // 如果主机名或端口号解析失败，抛出配置异常
                    if (host == null || port == null)
                        throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);

                    // 根据DNS查找策略进行不同的处理
                    if (clientDnsLookup == ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY) {
                        // 使用DNS解析获取所有可能的IP地址
                        InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                        // 遍历所有解析到的IP地址
                        for (InetAddress inetAddress : inetAddresses) {
                            // 获取规范主机名（完全限定域名FQDN）
                            String resolvedCanonicalName = inetAddress.getCanonicalHostName();
                            // 使用规范主机名和端口号创建套接字地址
                            InetSocketAddress address = new InetSocketAddress(resolvedCanonicalName, port);
                            // 检查地址是否可解析
                            if (address.isUnresolved()) {
                                // 如果地址无法解析，记录警告日志
                                log.warn("Couldn't resolve server {} from {} as DNS resolution of the canonical hostname {} failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolvedCanonicalName, host);
                            } else {
                                // 将可解析的地址添加到列表中
                                addresses.add(address);
                            }
                        }
                    } else {
                        // 使用原始主机名直接创建套接字地址
                        InetSocketAddress address = new InetSocketAddress(host, port);
                        // 检查地址是否可解析
                        if (address.isUnresolved()) {
                            // 如果地址无法解析，记录警告日志
                            log.warn("Couldn't resolve server {} from {} as DNS resolution failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, host);
                        } else {
                            // 将可解析的地址添加到列表中
                            addresses.add(address);
                        }
                    }

                } catch (IllegalArgumentException e) {
                    // 端口号格式无效时抛出配置异常
                    throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                } catch (UnknownHostException e) {
                    // 主机名无法解析时抛出配置异常
                    throw new ConfigException("Unknown host in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        // 如果没有任何可解析的地址，抛出配置异常
        if (addresses.isEmpty())
            throw new ConfigException("No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    /**
     * 根据提供的配置创建新的通道构建器
     * 该方法用于创建和配置Kafka客户端与服务器之间的网络通信通道
     *
     * @param config 客户端配置对象，包含安全协议、SASL机制等配置信息
     * @param time 时间实现对象，用于处理时间相关的操作
     * @param logContext 日志上下文对象，用于日志记录
     *
     * @return 根据配置信息创建的ChannelBuilder实例
     */
    public static ChannelBuilder createChannelBuilder(AbstractConfig config, Time time, LogContext logContext) {
        // 从配置中获取安全协议类型（如PLAINTEXT、SSL、SASL_PLAINTEXT、SASL_SSL等）
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        // 从配置中获取SASL认证机制（如PLAIN、GSSAPI、SCRAM等）
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);
        // 使用ChannelBuilders工厂类创建客户端通道构建器
        // 参数说明：
        // - securityProtocol: 安全协议类型
        // - JaasContext.Type.CLIENT: 指定为客户端类型的JAAS上下文
        // - config: 客户端配置
        // - null: 监听器名称（客户端端不需要）
        // - clientSaslMechanism: SASL认证机制
        // - time: 时间实现
        // - logContext: 日志上下文
        return ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT, config, null,
                clientSaslMechanism, time, logContext);
    }

    /**
     * 解析主机名获取IP地址列表
     * 
     * @param host 要解析的主机名
     * @param hostResolver 主机名解析器
     * @return 解析后的IP地址列表
     * @throws UnknownHostException 当主机名无法解析时抛出异常
     */
    static List<InetAddress> resolve(String host, HostResolver hostResolver) throws UnknownHostException {
        InetAddress[] addresses = hostResolver.resolve(host);
        List<InetAddress> result = filterPreferredAddresses(addresses);
        if (log.isDebugEnabled())
            log.debug("Resolved host {} as {}", host, result.stream().map(InetAddress::getHostAddress).collect(Collectors.joining(",")));
        return result;
    }

    /**
     * 过滤IP地址列表，确保返回的地址列表中所有地址类型一致（全部为IPv4或IPv6）
     * 
     * @param allAddresses 所有IP地址数组
     * @return 经过过滤的IP地址列表
     */
    static List<InetAddress> filterPreferredAddresses(InetAddress[] allAddresses) {
        List<InetAddress> preferredAddresses = new ArrayList<>();
        Class<? extends InetAddress> clazz = null;
        for (InetAddress address : allAddresses) {
            if (clazz == null) {
                clazz = address.getClass();
            }
            if (clazz.isInstance(address)) {
                preferredAddresses.add(address);
            }
        }
        return preferredAddresses;
    }

    /**
     * 创建NetworkClient实例，用于处理网络通信
     * 
     * @param config 客户端配置
     * @param metrics 度量指标收集器
     * @param metricsGroupPrefix 度量指标组前缀
     * @param logContext 日志上下文
     * @param apiVersions API版本信息
     * @param time 时间实现
     * @param maxInFlightRequestsPerConnection 每个连接最大的未完成请求数
     * @param metadata 元数据对象
     * @param throttleTimeSensor 限流传感器
     * @param clientTelemetrySender 客户端遥测发送器
     * @return 新创建的NetworkClient实例
     */
    public static NetworkClient createNetworkClient(AbstractConfig config,
                                                    Metrics metrics,
                                                    String metricsGroupPrefix,
                                                    LogContext logContext,
                                                    ApiVersions apiVersions,
                                                    Time time,
                                                    int maxInFlightRequestsPerConnection,
                                                    Metadata metadata,
                                                    Sensor throttleTimeSensor,
                                                    ClientTelemetrySender clientTelemetrySender) {
        return createNetworkClient(config,
                config.getString(CommonClientConfigs.CLIENT_ID_CONFIG),
                metrics,
                metricsGroupPrefix,
                logContext,
                apiVersions,
                time,
                maxInFlightRequestsPerConnection,
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                metadata,
                null,
                new DefaultHostResolver(),
                throttleTimeSensor,
                clientTelemetrySender);
    }

    public static NetworkClient createNetworkClient(AbstractConfig config,
                                                    String clientId,
                                                    Metrics metrics,
                                                    String metricsGroupPrefix,
                                                    LogContext logContext,
                                                    ApiVersions apiVersions,
                                                    Time time,
                                                    int maxInFlightRequestsPerConnection,
                                                    int requestTimeoutMs,
                                                    MetadataUpdater metadataUpdater,
                                                    HostResolver hostResolver) {
        return createNetworkClient(config,
                clientId,
                metrics,
                metricsGroupPrefix,
                logContext,
                apiVersions,
                time,
                maxInFlightRequestsPerConnection,
                requestTimeoutMs,
                null,
                metadataUpdater,
                hostResolver,
                null,
                null);
    }

    public static NetworkClient createNetworkClient(AbstractConfig config,
                                                    String clientId,
                                                    Metrics metrics,
                                                    String metricsGroupPrefix,
                                                    LogContext logContext,
                                                    ApiVersions apiVersions,
                                                    Time time,
                                                    int maxInFlightRequestsPerConnection,
                                                    int requestTimeoutMs,
                                                    Metadata metadata,
                                                    MetadataUpdater metadataUpdater,
                                                    HostResolver hostResolver,
                                                    Sensor throttleTimeSensor,
                                                    ClientTelemetrySender clientTelemetrySender) {
        ChannelBuilder channelBuilder = null;
        Selector selector = null;

        try {
            channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            selector = new Selector(config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics,
                    time,
                    metricsGroupPrefix,
                    channelBuilder,
                    logContext);
            return new NetworkClient(metadataUpdater,
                    metadata,
                    selector,
                    clientId,
                    maxInFlightRequestsPerConnection,
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                    config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                    requestTimeoutMs,
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    apiVersions,
                    throttleTimeSensor,
                    logContext,
                    hostResolver,
                    clientTelemetrySender,
                    config.getLong(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG),
                    MetadataRecoveryStrategy.forName(config.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG))
            );
        } catch (Throwable t) {
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new NetworkClient", t);
        }
    }

    /**
     * 配置并创建拦截器列表
     * 
     * @param config 客户端配置
     * @param interceptorClassesConfigName 拦截器类配置名
     * @param clazz 拦截器类型
     * @return 配置好的拦截器列表
     */
    public static <T> List configuredInterceptors(AbstractConfig config,
                                                  String interceptorClassesConfigName,
                                                  Class<T> clazz) {
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        return config.getConfiguredInstances(
                interceptorClassesConfigName,
                clazz,
                Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, clientId));
    }

    /**
     * 配置集群资源监听器
     * 
     * @param candidateLists 候选监听器列表
     * @return 配置好的集群资源监听器
     */
    public static ClusterResourceListeners configureClusterResourceListeners(List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();

        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        return clusterResourceListeners;
    }
}
