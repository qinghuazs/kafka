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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.DefaultLogin;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslClientCallbackHandler;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslServerCallbackHandler;
import org.apache.kafka.common.security.kerberos.KerberosClientCallbackHandler;
import org.apache.kafka.common.security.kerberos.KerberosLogin;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramServerCallbackHandler;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

/**
 * SASL通道构建器，负责创建和管理SASL认证通道。
 * 该类支持多种SASL认证机制(如GSSAPI/Kerberos、PLAIN、SCRAM等)，并可以与SSL/TLS结合使用。
 * 实现了ChannelBuilder接口用于创建网络通道，以及ListenerReconfigurable接口支持动态配置更新。
 */
public class SaslChannelBuilder implements ChannelBuilder, ListenerReconfigurable {
    // 用于启用Java GSS委托到本地GSS库的系统属性
    static final String GSS_NATIVE_PROP = "sun.security.jgss.native";

    // 安全协议类型(SASL_PLAINTEXT或SASL_SSL)
    private final SecurityProtocol securityProtocol;
    // 监听器名称，用于服务器端标识不同的监听器配置
    private final ListenerName listenerName;
    // 是否为broker间通信的监听器
    private final boolean isInterBrokerListener;
    // 客户端使用的SASL机制名称
    private final String clientSaslMechanism;
    // 连接模式(SERVER或CLIENT)
    private final ConnectionMode connectionMode;
    // JAAS上下文映射，key为认证机制名称
    private final Map<String, JaasContext> jaasContexts;
    // 凭证缓存，用于存储SCRAM等机制的凭证
    private final CredentialCache credentialCache;
    // 委托令牌缓存
    private final DelegationTokenCache tokenCache;
    // 登录管理器映射，key为认证机制名称
    private final Map<String, LoginManager> loginManagers;
    // Subject映射，存储认证主体信息
    private final Map<String, Subject> subjects;
    // API版本响应提供者函数
    private final Function<Short, ApiVersionsResponse> apiVersionSupplier;
    // SSL客户端认证覆盖配置
    private final String sslClientAuthOverride;
    // SASL回调处理器映射
    private final Map<String, AuthenticateCallbackHandler> saslCallbackHandlers;
    // 每个认证机制的最大重认证时间间隔
    private final Map<String, Long> connectionsMaxReauthMsByMechanism;
    // 时间工具类
    private final Time time;
    // 日志上下文
    private final LogContext logContext;
    // 日志记录器
    private final Logger log;

    // SSL工厂，用于SASL_SSL协议
    private SslFactory sslFactory;
    // 配置信息
    private Map<String, ?> configs;
    // Kerberos名称转换器
    private KerberosShortNamer kerberosShortNamer;

    /**
     * 构造SASL通道构建器
     * 
     * @param connectionMode 连接模式(SERVER/CLIENT)
     * @param jaasContexts JAAS配置上下文映射，包含每个认证机制的配置
     * @param securityProtocol 安全协议类型(SASL_PLAINTEXT/SASL_SSL)
     * @param listenerName 监听器名称，用于服务器端
     * @param isInterBrokerListener 是否为broker间通信的监听器
     * @param clientSaslMechanism 客户端使用的SASL机制
     * @param credentialCache 凭证缓存，用于存储认证凭证
     * @param tokenCache 委托令牌缓存
     * @param sslClientAuthOverride SSL客户端认证覆盖配置
     * @param time 时间工具类
     * @param logContext 日志上下文
     * @param apiVersionSupplier API版本响应提供者函数
     */
    public SaslChannelBuilder(ConnectionMode connectionMode,
                              Map<String, JaasContext> jaasContexts,
                              SecurityProtocol securityProtocol,
                              ListenerName listenerName,
                              boolean isInterBrokerListener,
                              String clientSaslMechanism,
                              CredentialCache credentialCache,
                              DelegationTokenCache tokenCache,
                              String sslClientAuthOverride,
                              Time time,
                              LogContext logContext,
                              Function<Short, ApiVersionsResponse> apiVersionSupplier) {
        // 初始化连接模式
        this.connectionMode = connectionMode;
        // 设置JAAS上下文映射
        this.jaasContexts = jaasContexts;
        // 初始化登录管理器映射，大小与JAAS上下文数量相同
        this.loginManagers = new HashMap<>(jaasContexts.size());
        // 初始化Subject映射，大小与JAAS上下文数量相同
        this.subjects = new HashMap<>(jaasContexts.size());
        // 设置安全协议类型
        this.securityProtocol = securityProtocol;
        // 设置监听器名称
        this.listenerName = listenerName;
        // 设置是否为broker间通信监听器
        this.isInterBrokerListener = isInterBrokerListener;
        // 设置客户端SASL机制
        this.clientSaslMechanism = clientSaslMechanism;
        // 设置凭证缓存
        this.credentialCache = credentialCache;
        // 设置令牌缓存
        this.tokenCache = tokenCache;
        // 设置SSL客户端认证覆盖配置
        this.sslClientAuthOverride = sslClientAuthOverride;
        // 初始化SASL回调处理器映射
        this.saslCallbackHandlers = new HashMap<>();
        // 初始化重认证时间间隔映射
        this.connectionsMaxReauthMsByMechanism = new HashMap<>();
        // 设置时间工具类
        this.time = time;
        // 设置日志上下文
        this.logContext = logContext;
        // 创建日志记录器
        this.log = logContext.logger(getClass());
        // 设置API版本响应提供者
        this.apiVersionSupplier = apiVersionSupplier;

        // 服务器模式下必须提供API版本响应提供者
        if (connectionMode == ConnectionMode.SERVER && apiVersionSupplier == null) {
            throw new IllegalArgumentException("Server channel builder must provide an ApiVersionResponse supplier");
        }
    }

    /**
     * 配置SASL通道构建器
     * 该方法负责初始化和配置所有必要的组件，包括回调处理器、登录管理器、认证主体等
     *
     * @param configs 配置参数映射
     * @throws KafkaException 如果配置过程中发生错误
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            // 保存配置信息
            this.configs = configs;
            // 根据连接模式创建相应的回调处理器
            if (connectionMode == ConnectionMode.SERVER) {
                // 服务器模式：创建服务器端回调处理器和重认证时间间隔映射
                createServerCallbackHandlers(configs);
                createConnectionsMaxReauthMsMap(configs);
            } else
                // 客户端模式：创建客户端回调处理器
                createClientCallbackHandler(configs);
            // 配置所有SASL回调处理器
            for (Map.Entry<String, AuthenticateCallbackHandler> entry : saslCallbackHandlers.entrySet()) {
                String mechanism = entry.getKey();
                entry.getValue().configure(configs, mechanism, jaasContexts.get(mechanism).configurationEntries());
            }

            // 获取默认登录类
            Class<? extends Login> defaultLoginClass = defaultLoginClass();
            // 如果是服务器模式且启用了Kerberos认证，配置Kerberos名称转换器
            if (connectionMode == ConnectionMode.SERVER && jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM)) {
                String defaultRealm;
                try {
                    defaultRealm = defaultKerberosRealm();
                } catch (Exception ke) {
                    defaultRealm = "";
                }
                // 获取Kerberos主体到本地用户名的转换规则
                List<String> principalToLocalRules = (List<String>) configs.get(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG);
                if (principalToLocalRules != null)
                    kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
            }
            // 为每个SASL机制创建登录管理器和认证主体
            for (Map.Entry<String, JaasContext> entry : jaasContexts.entrySet()) {
                String mechanism = entry.getKey();
                // 对于静态JAAS配置，如果启用了Kerberos则使用KerberosLogin
                // 对于动态JAAS配置，仅对GSSAPI的LoginContext使用KerberosLogin
                LoginManager loginManager = LoginManager.acquireLoginManager(entry.getValue(), mechanism, defaultLoginClass, configs);
                loginManagers.put(mechanism, loginManager);
                Subject subject = loginManager.subject();
                subjects.put(mechanism, subject);
                // 如果是服务器模式且使用GSSAPI机制，可能需要添加本地GSSAPI凭证
                if (connectionMode == ConnectionMode.SERVER && mechanism.equals(SaslConfigs.GSSAPI_MECHANISM))
                    maybeAddNativeGssapiCredentials(subject);
            }
            // 如果使用SASL_SSL协议，配置SSL工厂
            if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
                // 禁用SSL客户端认证，因为我们使用SASL认证
                this.sslFactory = new SslFactory(connectionMode, sslClientAuthOverride, isInterBrokerListener);
                this.sslFactory.configure(configs);
            }
        } catch (Throwable e) {
            // 发生错误时关闭资源并抛出异常
            close();
            throw new KafkaException(e);
        }
    }

    /**
     * 获取可重配置的配置项集合
     * 如果使用SASL_SSL协议，返回SSL相关的可重配置配置项；否则返回空集合
     *
     * @return 可重配置的配置项集合
     */
    @Override
    public Set<String> reconfigurableConfigs() {
        return securityProtocol == SecurityProtocol.SASL_SSL ? SslConfigs.RECONFIGURABLE_CONFIGS : Collections.emptySet();
    }

    /**
     * 验证重配置参数
     * 如果使用SASL_SSL协议，验证SSL工厂的重配置参数
     *
     * @param configs 待验证的配置参数
     * @throws ConfigException 如果验证失败
     */
    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL)
            try {
                sslFactory.validateReconfiguration(configs);
            } catch (IllegalStateException e) {
                throw new ConfigException("SASL重配置失败: " + e);
            }
    }

    /**
     * 应用重配置
     * 如果使用SASL_SSL协议，重新配置SSL工厂
     *
     * @param configs 新的配置参数
     */
    @Override
    public void reconfigure(Map<String, ?> configs) {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL)
            sslFactory.reconfigure(configs);
    }

    /**
     * 获取监听器名称
     *
     * @return 当前通道构建器的监听器名称
     */
    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    /**
     * 构建Kafka通道
     * 创建一个新的KafkaChannel实例，包括传输层和认证器
     *
     * @param id 通道标识符
     * @param key 选择键
     * @param maxReceiveSize 最大接收大小
     * @param memoryPool 内存池
     * @param metadataRegistry 元数据注册表
     * @return 新创建的KafkaChannel实例
     * @throws KafkaException 如果通道创建失败
     */
    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        TransportLayer transportLayer = null;
        try {
            // 获取Socket通道和Socket
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Socket socket = socketChannel.socket();
            // 构建传输层
            transportLayer = buildTransportLayer(id, key, socketChannel, metadataRegistry);
            final TransportLayer finalTransportLayer = transportLayer;
            // 创建认证器供应商
            Supplier<Authenticator> authenticatorCreator;
            if (connectionMode == ConnectionMode.SERVER) {
                // 服务器模式：创建服务器认证器
                authenticatorCreator = () -> buildServerAuthenticator(configs,
                        Collections.unmodifiableMap(saslCallbackHandlers),
                        id,
                        finalTransportLayer,
                        Collections.unmodifiableMap(subjects),
                        Collections.unmodifiableMap(connectionsMaxReauthMsByMechanism),
                        metadataRegistry);
            } else {
                // 客户端模式：创建客户端认证器
                LoginManager loginManager = loginManagers.get(clientSaslMechanism);
                authenticatorCreator = () -> buildClientAuthenticator(configs,
                        saslCallbackHandlers.get(clientSaslMechanism),
                        id,
                        socket.getInetAddress().getHostName(),
                        loginManager.serviceName(),
                        finalTransportLayer,
                        subjects.get(clientSaslMechanism));
            }
            // 创建并返回新的KafkaChannel实例
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            // 理想情况下这些资源应该由KafkaChannel关闭
            // 但如果由于错误导致KafkaChannel未创建，构建器应该关闭这些资源
            Utils.closeQuietly(transportLayer, "transport layer for channel Id: " + id);
            throw new KafkaException(e);
        }
    }

    /**
     * 关闭SASL通道构建器并释放相关资源
     * 该方法会按顺序执行以下操作：
     * 1. 释放所有登录管理器资源
     * 2. 关闭所有认证回调处理器
     * 3. 如果使用了SSL，关闭SSL工厂
     */
    @Override
    public void close()  {
        // 释放所有登录管理器资源，这些管理器负责维护认证凭证
        for (LoginManager loginManager : loginManagers.values())
            loginManager.release();
        // 清空登录管理器映射
        loginManagers.clear();
        // 关闭所有认证回调处理器，这些处理器负责处理认证过程中的回调
        for (AuthenticateCallbackHandler handler : saslCallbackHandlers.values())
            handler.close();
        // 如果使用了SSL，关闭SSL工厂以释放相关资源
        if (sslFactory != null) sslFactory.close();
    }

    /**
     * 构建传输层实例
     * 根据安全协议类型(SASL_SSL或SASL_PLAINTEXT)创建相应的传输层
     * 可被测试覆盖以提供模拟实现
     *
     * @param id 通道标识符
     * @param key 选择键，用于NIO操作
     * @param socketChannel Socket通道
     * @param metadataRegistry 通道元数据注册表
     * @return 创建的传输层实例
     * @throws IOException 如果创建传输层时发生I/O错误
     */
    protected TransportLayer buildTransportLayer(String id, SelectionKey key, SocketChannel socketChannel,
                                                 ChannelMetadataRegistry metadataRegistry) throws IOException {
        // 如果使用SASL_SSL协议，创建SSL传输层
        if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
            // 使用SSL工厂创建SSL引擎，并构建SSL传输层
            return SslTransportLayer.create(id, key,
                sslFactory.createSslEngine(socketChannel.socket()),
                metadataRegistry);
        } else {
            // 如果使用SASL_PLAINTEXT协议，创建明文传输层
            return new PlaintextTransportLayer(key);
        }
    }

    /**
     * 构建SASL服务器认证器
     * 用于服务器端的SASL认证处理，可被测试覆盖以提供模拟实现
     *
     * @param configs 配置参数映射
     * @param callbackHandlers 认证回调处理器映射，key为认证机制名称
     * @param id 通道标识符
     * @param transportLayer 传输层实例
     * @param subjects 认证主体映射，key为认证机制名称
     * @param connectionsMaxReauthMsByMechanism 每个认证机制的最大重认证时间间隔
     * @param metadataRegistry 通道元数据注册表
     * @return 创建的SASL服务器认证器实例
     */
    protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                               Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                               String id,
                                                               TransportLayer transportLayer,
                                                               Map<String, Subject> subjects,
                                                               Map<String, Long> connectionsMaxReauthMsByMechanism,
                                                               ChannelMetadataRegistry metadataRegistry) {
        // 创建并返回新的SASL服务器认证器实例
        // 传入所有必要的参数，包括Kerberos名称转换器、监听器名称、安全协议等
        return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects,
                kerberosShortNamer, listenerName, securityProtocol, transportLayer,
                connectionsMaxReauthMsByMechanism, metadataRegistry, time, apiVersionSupplier);
    }

    /**
     * 构建SASL客户端认证器
     * 用于客户端的SASL认证处理，可被测试覆盖以提供模拟实现
     *
     * @param configs 配置参数映射
     * @param callbackHandler 认证回调处理器
     * @param id 通道标识符
     * @param serverHost 服务器主机名
     * @param servicePrincipal 服务主体名称
     * @param transportLayer 传输层实例
     * @param subject 认证主体
     * @return 创建的SASL客户端认证器实例
     */
    protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                                                               AuthenticateCallbackHandler callbackHandler,
                                                               String id,
                                                               String serverHost,
                                                               String servicePrincipal,
                                                               TransportLayer transportLayer, Subject subject) {
        // 创建并返回新的SASL客户端认证器实例
        // 传入所有必要的参数，包括认证机制、服务主体名称、服务器主机名等
        return new SaslClientAuthenticator(configs, callbackHandler, id, subject, servicePrincipal,
                serverHost, clientSaslMechanism, transportLayer, time, logContext);
    }

    /**
     * 获取登录管理器映射
     * 该方法用于测试目的，允许测试代码访问内部的登录管理器
     *
     * @return 登录管理器映射，key为认证机制名称
     */
    Map<String, LoginManager> loginManagers() {
        return loginManagers;
    }

    /**
     * 获取默认的Kerberos领域
     * 通过创建临时的Kerberos主体来获取系统默认的Kerberos领域
     * 详见：https://issues.apache.org/jira/browse/HADOOP-10848
     *
     * @return 默认的Kerberos领域名称
     */
    private static String defaultKerberosRealm() {
        // 创建临时Kerberos主体并获取其领域
        return new KerberosPrincipal("tmp", 1).getRealm();
    }

    /**
     * 创建客户端认证回调处理器
     * 根据配置创建适当的回调处理器实例，并将其与客户端SASL机制关联
     *
     * @param configs 配置参数映射
     */
    private void createClientCallbackHandler(Map<String, ?> configs) {
        // 从配置中获取回调处理器类
        @SuppressWarnings("unchecked")
        Class<? extends AuthenticateCallbackHandler> clazz = (Class<? extends AuthenticateCallbackHandler>) configs.get(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS);
        // 如果未配置，使用默认的回调处理器类
        if (clazz == null)
            clazz = clientCallbackHandlerClass();
        // 创建回调处理器实例
        AuthenticateCallbackHandler callbackHandler = Utils.newInstance(clazz);
        // 将回调处理器与客户端SASL机制关联
        saslCallbackHandlers.put(clientSaslMechanism, callbackHandler);
    }

    /**
     * 创建服务器端认证回调处理器
     * 为每个SASL机制创建相应的回调处理器实例
     * 支持多种认证机制：PLAIN、SCRAM、OAUTHBEARER等
     *
     * @param configs 配置参数映射
     */
    private void createServerCallbackHandlers(Map<String, ?> configs) {
        // 遍历所有配置的SASL机制
        for (String mechanism : jaasContexts.keySet()) {
            AuthenticateCallbackHandler callbackHandler;
            // 获取机制特定的配置前缀
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            // 尝试从配置中获取自定义回调处理器类
            @SuppressWarnings("unchecked")
            Class<? extends AuthenticateCallbackHandler> clazz =
                    (Class<? extends AuthenticateCallbackHandler>) configs.get(prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG);
            
            // 根据不同情况创建相应的回调处理器
            if (clazz != null)
                // 使用配置的自定义处理器类
                callbackHandler = Utils.newInstance(clazz);
            else if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM))
                // PLAIN机制使用明文处理器
                callbackHandler = new PlainServerCallbackHandler();
            else if (ScramMechanism.isScram(mechanism))
                // SCRAM机制使用SCRAM处理器，并配置凭证缓存
                callbackHandler = new ScramServerCallbackHandler(credentialCache.cache(mechanism, ScramCredential.class), tokenCache);
            else if (mechanism.equals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM))
                // OAuth Bearer机制使用OAuth处理器
                callbackHandler = new OAuthBearerUnsecuredValidatorCallbackHandler();
            else
                // 其他机制使用默认处理器
                callbackHandler = new SaslServerCallbackHandler();
            // 将创建的处理器与对应的SASL机制关联
            saslCallbackHandlers.put(mechanism, callbackHandler);
        }
    }

    /**
     * 创建连接最大重认证时间间隔映射
     * 为每个SASL机制配置其最大重认证时间间隔
     *
     * @param configs 配置参数映射
     */
    private void createConnectionsMaxReauthMsMap(Map<String, ?> configs) {
        // 遍历所有配置的SASL机制
        for (String mechanism : jaasContexts.keySet()) {
            // 获取机制特定的配置前缀
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            // 尝试获取机制特定的重认证时间间隔
            Long connectionsMaxReauthMs = (Long) configs.get(prefix + BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG);
            // 如果未配置机制特定的时间间隔，使用全局配置
            if (connectionsMaxReauthMs == null)
                connectionsMaxReauthMs = (Long) configs.get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG);
            // 如果配置了时间间隔，将其添加到映射中
            if (connectionsMaxReauthMs != null)
                connectionsMaxReauthMsByMechanism.put(mechanism, connectionsMaxReauthMs);
        }
    }

    /**
     * 获取默认的登录类
     * 根据配置的SASL机制返回相应的登录类实现
     * 
     * @return 默认的登录类，可能是以下之一：
     *         - KerberosLogin：当启用了GSSAPI(Kerberos)机制时
     *         - OAuthBearerRefreshingLogin：当使用OAuth Bearer认证时
     *         - DefaultLogin：其他情况的默认实现
     */
    protected Class<? extends Login> defaultLoginClass() {
        // 如果配置了GSSAPI(Kerberos)机制，使用KerberosLogin处理Kerberos认证
        if (jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM))
            return KerberosLogin.class;
        // 如果使用OAuth Bearer认证机制，使用OAuthBearerRefreshingLogin处理OAuth认证
        if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(clientSaslMechanism))
            return OAuthBearerRefreshingLogin.class;
        // 其他情况使用默认的登录实现
        return DefaultLogin.class;
    }

    /**
     * 获取客户端回调处理器类
     * 根据配置的SASL机制返回相应的回调处理器实现
     * 
     * @return 客户端回调处理器类，可能是以下之一：
     *         - KerberosClientCallbackHandler：处理Kerberos认证的回调
     *         - OAuthBearerSaslClientCallbackHandler：处理OAuth Bearer认证的回调
     *         - SaslClientCallbackHandler：处理其他SASL机制的回调
     */
    private Class<? extends AuthenticateCallbackHandler> clientCallbackHandlerClass() {
        switch (clientSaslMechanism) {
            // 对于GSSAPI(Kerberos)机制，使用专门的Kerberos回调处理器
            case SaslConfigs.GSSAPI_MECHANISM:
                return KerberosClientCallbackHandler.class;
            // 对于OAuth Bearer机制，使用专门的OAuth回调处理器
            case OAuthBearerLoginModule.OAUTHBEARER_MECHANISM:
                return OAuthBearerSaslClientCallbackHandler.class;
            // 其他SASL机制使用通用的客户端回调处理器
            default:
                return SaslClientCallbackHandler.class;
        }
    }

    /**
     * 为Subject添加本地GSSAPI凭证
     * 当使用本地GSS库时，需要将GSS凭证添加到Subject的私有凭证集中
     * 参考：http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html
     * 
     * @param subject 需要添加GSSAPI凭证的Subject对象
     */
    private void maybeAddNativeGssapiCredentials(Subject subject) {
        // 检查是否启用了本地JGSS支持
        boolean usingNativeJgss = Boolean.getBoolean(GSS_NATIVE_PROP);
        // 如果启用了本地JGSS且Subject还没有GSS凭证，则添加
        if (usingNativeJgss && subject.getPrivateCredentials(GSSCredential.class).isEmpty()) {
            // 获取服务主体名称
            final String servicePrincipal = SaslClientAuthenticator.firstPrincipal(subject);
            KerberosName kerberosName;
            try {
                // 解析Kerberos主体名称
                kerberosName = KerberosName.parse(servicePrincipal);
            } catch (IllegalArgumentException e) {
                throw new KafkaException("Principal has name with unexpected format " + servicePrincipal);
            }
            // 获取服务名称和主机名
            final String servicePrincipalName = kerberosName.serviceName();
            final String serviceHostname = kerberosName.hostName();

            try {
                // 获取GSS管理器实例
                GSSManager manager = gssManager();
                // 创建Kerberos v5 GSS-API机制的OID（在RFC 1964中定义）
                Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                // 创建基于主机的服务GSS名称
                GSSName gssName = manager.createName(servicePrincipalName + "@" + serviceHostname, GSSName.NT_HOSTBASED_SERVICE);
                // 创建GSS凭证，设置为仅接受模式
                GSSCredential cred = manager.createCredential(gssName,
                        GSSContext.INDEFINITE_LIFETIME, krb5Mechanism, GSSCredential.ACCEPT_ONLY);
                // 将凭证添加到Subject的私有凭证集中
                subject.getPrivateCredentials().add(cred);
                log.info("Configured native GSSAPI private credentials for {}@{}", serviceHostname, serviceHostname);
            } catch (GSSException ex) {
                log.warn("Cannot add private credential to subject; clients authentication may fail", ex);
            }
        }
    }

    /**
     * 获取GSS管理器实例
     * 该方法可被测试覆盖以提供模拟实现
     * 
     * @return GSS管理器实例
     */
    protected GSSManager gssManager() {
        return GSSManager.getInstance();
    }

    /**
     * 获取指定SASL机制的Subject
     * 该方法用于测试目的，允许访问内部Subject对象
     * 
     * @param saslMechanism SASL机制名称
     * @return 对应的Subject对象，如果不存在则返回null
     */
    protected Subject subject(String saslMechanism) {
        return subjects.get(saslMechanism);
    }
}
