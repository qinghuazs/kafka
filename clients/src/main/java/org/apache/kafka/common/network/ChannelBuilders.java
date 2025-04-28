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
package org.apache.kafka.common.network;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Kafka网络层的通道构建器工厂类，负责创建和配置不同安全协议(SSL、SASL、PLAINTEXT)的通道构建器。
 * 该类提供了客户端和服务器端通道构建器的创建方法，支持以下安全协议：
 * 1. PLAINTEXT - 无安全认证的明文通信
 * 2. SSL - 基于SSL/TLS的安全通信
 * 3. SASL_PLAINTEXT - 基于SASL的安全认证，但通信内容为明文
 * 4. SASL_SSL - 同时使用SASL认证和SSL加密通信
 */
public class ChannelBuilders {
    // 日志记录器实例
    private static final Logger log = LoggerFactory.getLogger(ChannelBuilders.class);

    // 私有构造函数，防止实例化
    private ChannelBuilders() { }

    /**
     * 创建客户端通道构建器的工厂方法
     * 
     * @param securityProtocol 安全协议类型，如PLAINTEXT、SSL、SASL_PLAINTEXT、SASL_SSL
     * @param contextType JAAS上下文类型，当使用SASL时必须非空
     * @param config 客户端配置信息
     * @param listenerName 监听器名称，当contextType为SERVER时使用，否则为null
     * @param clientSaslMechanism SASL机制名称，如PLAIN、GSSAPI等，仅在客户端模式下使用
     * @param time 时间实例，用于处理超时等时间相关操作
     * @param logContext 日志上下文实例，用于日志记录
     * 
     * @return 配置好的通道构建器实例
     * @throws IllegalArgumentException 当安全协议相关的参数配置不正确时抛出
     */
    public static ChannelBuilder clientChannelBuilder(
            SecurityProtocol securityProtocol,
            JaasContext.Type contextType,
            AbstractConfig config,
            ListenerName listenerName,
            String clientSaslMechanism,
            Time time,
            LogContext logContext) {

        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (contextType == null)
                throw new IllegalArgumentException("`contextType` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
            if (clientSaslMechanism == null)
                throw new IllegalArgumentException("`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `" + securityProtocol + "`");
        }
        return create(securityProtocol, ConnectionMode.CLIENT, contextType, config, listenerName, false, clientSaslMechanism,
            null, null, time, logContext, null);
    }

    /**
     * 创建服务器端通道构建器的工厂方法
     * 
     * @param listenerName 监听器名称，用于标识特定的网络监听器
     * @param isInterBrokerListener 是否用于broker间通信的监听器
     * @param securityProtocol 安全协议类型
     * @param config 服务器配置信息
     * @param credentialCache SASL/SCRAM认证的凭证缓存，当启用SCRAM时使用
     * @param tokenCache 委派令牌缓存，用于存储和管理委派令牌
     * @param time 时间实例
     * @param logContext 日志上下文实例
     * @param apiVersionSupplier API版本响应的提供者，用于认证前的版本协商
     * 
     * @return 配置好的服务器端通道构建器
     */
    public static ChannelBuilder serverChannelBuilder(ListenerName listenerName,
                                                      boolean isInterBrokerListener,
                                                      SecurityProtocol securityProtocol,
                                                      AbstractConfig config,
                                                      CredentialCache credentialCache,
                                                      DelegationTokenCache tokenCache,
                                                      Time time,
                                                      LogContext logContext,
                                                      Function<Short, ApiVersionsResponse> apiVersionSupplier) {
        return create(securityProtocol, ConnectionMode.SERVER, JaasContext.Type.SERVER, config, listenerName,
                isInterBrokerListener, null, credentialCache, tokenCache, time, logContext,
                apiVersionSupplier);
    }

    /**
     * 根据安全协议类型创建对应的通道构建器
     * 该方法是实际创建通道构建器的核心实现，支持多种安全协议类型
     * 
     * @param securityProtocol 安全协议类型
     * @param connectionMode 连接模式(客户端/服务器端)
     * @param contextType JAAS上下文类型
     * @param config 配置信息
     * @param listenerName 监听器名称
     * @param isInterBrokerListener 是否为broker间通信监听器
     * @param clientSaslMechanism 客户端SASL机制
     * @param credentialCache 凭证缓存
     * @param tokenCache 令牌缓存
     * @param time 时间实例
     * @param logContext 日志上下文
     * @param apiVersionSupplier API版本响应提供者
     * @return 配置好的通道构建器实例
     */
    private static ChannelBuilder create(SecurityProtocol securityProtocol,
                                         ConnectionMode connectionMode,
                                         JaasContext.Type contextType,
                                         AbstractConfig config,
                                         ListenerName listenerName,
                                         boolean isInterBrokerListener,
                                         String clientSaslMechanism,
                                         CredentialCache credentialCache,
                                         DelegationTokenCache tokenCache,
                                         Time time,
                                         LogContext logContext,
                                         Function<Short, ApiVersionsResponse> apiVersionSupplier) {
        // 获取通道构建器的配置信息，包括监听器特定的配置
        Map<String, Object> configs = channelBuilderConfigs(config, listenerName);

        ChannelBuilder channelBuilder;
        switch (securityProtocol) {
            case SSL:
                // 检查SSL协议下连接模式是否正确设置
                requireNonNullMode(connectionMode, securityProtocol);
                // 创建SSL通道构建器，用于处理SSL/TLS加密通信
                channelBuilder = new SslChannelBuilder(connectionMode, listenerName, isInterBrokerListener, logContext);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                // 检查SASL协议下连接模式是否正确设置
                requireNonNullMode(connectionMode, securityProtocol);
                Map<String, JaasContext> jaasContexts;
                String sslClientAuthOverride = null;
                if (connectionMode == ConnectionMode.SERVER) {
                    // 服务器模式：获取所有启用的SASL机制
                    @SuppressWarnings("unchecked")
                    List<String> enabledMechanisms = (List<String>) configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
                    jaasContexts = new HashMap<>(enabledMechanisms.size());
                    // 为每个启用的SASL机制加载服务器端JAAS上下文
                    for (String mechanism : enabledMechanisms)
                        jaasContexts.put(mechanism, JaasContext.loadServerContext(listenerName, mechanism, configs));

                    // 对于SASL_SSL协议，只有在指定了监听器前缀配置时才启用SSL客户端认证
                    if (listenerName != null && securityProtocol == SecurityProtocol.SASL_SSL) {
                        // 获取全局和监听器特定的SSL客户端认证配置
                        String configuredClientAuth = (String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                        String listenerClientAuth = (String) config.originalsWithPrefix(listenerName.configPrefix(), true)
                                .get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);

                        // 如果没有设置监听器级别的配置，则使用NONE作为默认值
                        // 这确保了SASL_SSL监听器默认不要求SSL客户端认证
                        if (listenerClientAuth == null) {
                            sslClientAuthOverride = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT);
                            if (configuredClientAuth != null && !configuredClientAuth.equalsIgnoreCase(SslClientAuth.NONE.name())) {
                                // 警告用户全局SSL客户端认证配置不会应用到SASL_SSL监听器
                                log.warn("Broker configuration '{}' is applied only to SSL listeners. Listener-prefixed configuration can be used" +
                                        " to enable SSL client authentication for SASL_SSL listeners. In future releases, broker-wide option without" +
                                        " listener prefix may be applied to SASL_SSL listeners as well. All configuration options intended for specific" +
                                        " listeners should be listener-prefixed.", BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                            }
                        }
                    }
                } else {
                    // 客户端模式：根据上下文类型选择适当的JAAS上下文
                    // 对于broker间通信使用服务器上下文，对于普通客户端使用客户端上下文
                    JaasContext jaasContext = contextType == JaasContext.Type.CLIENT ? JaasContext.loadClientContext(configs) :
                            JaasContext.loadServerContext(listenerName, clientSaslMechanism, configs);
                    jaasContexts = Collections.singletonMap(clientSaslMechanism, jaasContext);
                }
                // 创建SASL通道构建器，用于处理SASL认证
                channelBuilder = new SaslChannelBuilder(connectionMode,
                        jaasContexts,
                        securityProtocol,
                        listenerName,
                        isInterBrokerListener,
                        clientSaslMechanism,
                        credentialCache,
                        tokenCache,
                        sslClientAuthOverride,
                        time,
                        logContext,
                        apiVersionSupplier);
                break;
            case PLAINTEXT:
                // 创建明文通道构建器，不提供任何安全特性
                channelBuilder = new PlaintextChannelBuilder(listenerName);
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }

    /**
     * 获取通道构建器的配置信息
     * 该方法会处理监听器特定的配置覆盖，并标记已使用的配置项
     * 
     * @param config 基础配置对象
     * @param listenerName 监听器名称，用于获取特定监听器的配置
     * @return 可变的配置Map，包含所有需要的配置项
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> channelBuilderConfigs(final AbstractConfig config, final ListenerName listenerName) {
        Map<String, Object> parsedConfigs;
        if (listenerName == null)
            parsedConfigs = (Map<String, Object>) config.values();
        else
            parsedConfigs = config.valuesWithPrefixOverride(listenerName.configPrefix());

        config.originals().entrySet().stream()
            .filter(e -> !parsedConfigs.containsKey(e.getKey())) // exclude already parsed configs
            // exclude already parsed listener prefix configs
            .filter(e -> !(listenerName != null && e.getKey().startsWith(listenerName.configPrefix()) &&
                parsedConfigs.containsKey(e.getKey().substring(listenerName.configPrefix().length()))))
            // exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and key `some.prop` exists in parsed configs.
            .filter(e -> !(listenerName != null && parsedConfigs.containsKey(e.getKey().substring(e.getKey().indexOf('.') + 1))))
            .forEach(e -> parsedConfigs.put(e.getKey(), e.getValue()));
        return parsedConfigs;
    }

    /**
     * 检查连接模式是否为空
     * 当使用SSL或SASL安全协议时，连接模式不能为空
     * 
     * @param connectionMode 连接模式
     * @param securityProtocol 安全协议类型
     * @throws IllegalArgumentException 当连接模式为空时抛出
     */
    private static void requireNonNullMode(ConnectionMode connectionMode, SecurityProtocol securityProtocol) {
        if (connectionMode == null)
            throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
    }

    /**
     * 创建Kafka主体构建器
     * 用于构建认证主体，支持自定义主体构建器的实现
     * 
     * @param configs 配置信息
     * @param kerberosShortNamer Kerberos名称简化器，用于处理Kerberos主体名称
     * @param sslPrincipalMapper SSL主体映射器，用于映射SSL证书中的主体信息
     * @return 配置好的主体构建器实例
     */
    public static KafkaPrincipalBuilder createPrincipalBuilder(Map<String, ?> configs,
                                                               KerberosShortNamer kerberosShortNamer,
                                                               SslPrincipalMapper sslPrincipalMapper) {
        Class<?> principalBuilderClass = (Class<?>) configs.get(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        final KafkaPrincipalBuilder builder;

        if (principalBuilderClass == null || principalBuilderClass == DefaultKafkaPrincipalBuilder.class) {
            builder = new DefaultKafkaPrincipalBuilder(kerberosShortNamer, sslPrincipalMapper);
        } else if (KafkaPrincipalBuilder.class.isAssignableFrom(principalBuilderClass)) {
            builder = (KafkaPrincipalBuilder) Utils.newInstance(principalBuilderClass);
        } else {
            throw new InvalidConfigurationException("Type " + principalBuilderClass.getName() + " is not " +
                    "an instance of " + KafkaPrincipalBuilder.class.getName());
        }

        if (builder instanceof Configurable)
            ((Configurable) builder).configure(configs);

        return builder;
    }

}
