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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * SSL通道构建器，负责创建和管理基于SSL/TLS的安全网络通道。
 * 实现了ChannelBuilder接口用于构建网络通道，以及ListenerReconfigurable接口支持动态配置更新。
 * 主要用于：
 * 1. 创建SSL加密的网络连接
 * 2. 管理SSL配置和证书
 * 3. 处理SSL认证和主体映射
 * 4. 支持动态更新SSL配置
 */
public class SslChannelBuilder implements ChannelBuilder, ListenerReconfigurable {
    // 监听器名称，用于服务器端标识不同的监听器配置，客户端模式下为null
    private final ListenerName listenerName;
    // 标识是否为broker间通信的监听器
    private final boolean isInterBrokerListener;
    // 连接模式(SERVER或CLIENT)
    private final ConnectionMode connectionMode;
    // SSL工厂，用于创建和管理SSL上下文及引擎
    private SslFactory sslFactory;
    // SSL相关配置信息
    private Map<String, ?> configs;
    // SSL主体映射器，用于转换SSL证书中的主体信息
    private SslPrincipalMapper sslPrincipalMapper;

    /**
     * Constructs an SSL channel builder. ListenerName is provided only
     * for server channel builder and will be null for client channel builder.
     */
    public SslChannelBuilder(ConnectionMode connectionMode,
                             ListenerName listenerName,
                             boolean isInterBrokerListener,
                             LogContext logContext) {
        this.connectionMode = connectionMode;
        this.listenerName = listenerName;
        this.isInterBrokerListener = isInterBrokerListener;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            String sslPrincipalMappingRules = (String) configs.get(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG);
            if (sslPrincipalMappingRules != null)
                sslPrincipalMapper = SslPrincipalMapper.fromRules(sslPrincipalMappingRules);
            this.sslFactory = new SslFactory(connectionMode, null, isInterBrokerListener);
            this.sslFactory.configure(this.configs);
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        sslFactory.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        sslFactory.reconfigure(configs);
    }

    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    /**
     * 构建Kafka通道
     * @param id 通道标识符
     * @param key 选择键
     * @param maxReceiveSize 最大接收大小
     * @param memoryPool 内存池
     * @param metadataRegistry 元数据注册表
     * @return 构建的Kafka通道
     * @throws KafkaException 通道构建过程中的异常
     */
    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        SslTransportLayer transportLayer = null;
        try {
            // 构建SSL传输层
            transportLayer = buildTransportLayer(sslFactory, id, key, metadataRegistry);
            final SslTransportLayer finalTransportLayer = transportLayer;
            // 创建认证器供应商
            Supplier<Authenticator> authenticatorCreator = () ->
                new SslAuthenticator(configs, finalTransportLayer, listenerName, sslPrincipalMapper);
            // 创建并返回Kafka通道实例
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            // 理想情况下这些资源应该由KafkaChannel关闭
            // 但如果在创建KafkaChannel过程中发生错误，构建器应该负责关闭资源
            Utils.closeQuietly(transportLayer, "transport layer for channel Id: " + id);
            throw new KafkaException(e);
        }
    }

    @Override
    public void close() {
        if (sslFactory != null) sslFactory.close();
    }

    protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, ChannelMetadataRegistry metadataRegistry) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        return SslTransportLayer.create(id, key, sslFactory.createSslEngine(socketChannel.socket()),
            metadataRegistry);
    }

    /**
     * Note that client SSL authentication is handled in {@link SslTransportLayer}. This class is only used
     * to transform the derived principal using a {@link KafkaPrincipalBuilder} configured by the user.
     */
    /**
     * SSL认证器内部类，实现Authenticator接口
     * 主要用于：
     * 1. 处理SSL认证过程
     * 2. 构建Kafka主体信息
     * 3. 支持主体序列化
     */
    private static class SslAuthenticator implements Authenticator {
        // SSL传输层，提供底层的SSL通信功能
        private final SslTransportLayer transportLayer;
        // Kafka主体构建器，用于创建认证主体
        private final KafkaPrincipalBuilder principalBuilder;
        // 监听器名称
        private final ListenerName listenerName;

        /**
         * 构造SSL认证器
         * @param configs SSL配置信息
         * @param transportLayer SSL传输层
         * @param listenerName 监听器名称
         * @param sslPrincipalMapper SSL主体映射器
         */
        private SslAuthenticator(Map<String, ?> configs, SslTransportLayer transportLayer, ListenerName listenerName, SslPrincipalMapper sslPrincipalMapper) {
            this.transportLayer = transportLayer;
            // 创建主体构建器
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, sslPrincipalMapper);
            this.listenerName = listenerName;
        }
        /**
         * 由于SSL认证在传输层已经完成，此方法为空实现
         * 在SSL认证中，认证过程是在握手阶段自动完成的，不需要额外的认证步骤
         */
        @Override
        public void authenticate() {}

        /**
         * 使用配置的principalBuilder构建Kafka主体
         * 主要步骤：
         * 1. 获取客户端地址信息
         * 2. 验证监听器名称的有效性
         * 3. 创建SSL认证上下文
         * 4. 使用主体构建器生成Kafka主体
         * 
         * @return 构建的Kafka主体，包含了客户端的身份信息
         * @throws IllegalStateException 当在客户端模式下调用此方法时抛出
         */
        @Override
        public KafkaPrincipal principal() {
            // 获取客户端的网络地址信息
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
            // 在客户端模式下listenerName为null，此时不应调用principal方法
            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            // 创建SSL认证上下文，包含SSL会话、客户端地址和监听器名称
            SslAuthenticationContext context = new SslAuthenticationContext(
                    transportLayer.sslSession(),
                    clientAddress,
                    listenerName.value());
            // 使用主体构建器根据上下文构建Kafka主体
            return principalBuilder.build(context);
        }

        /**
         * 获取主体序列化器
         * 如果主体构建器实现了KafkaPrincipalSerde接口，则返回其序列化功能
         * 主要用于在网络传输中序列化和反序列化主体信息
         * 
         * @return 主体序列化器的Optional包装，如果不支持序列化则返回空Optional
         */
        @Override
        public Optional<KafkaPrincipalSerde> principalSerde() {
            return principalBuilder instanceof KafkaPrincipalSerde ? Optional.of((KafkaPrincipalSerde) principalBuilder) : Optional.empty();
        }

        /**
         * 关闭认证器，释放相关资源
         * 如果主体构建器实现了Closeable接口，则安全地关闭它
         */
        @Override
        public void close() {
            if (principalBuilder instanceof Closeable)
                Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }

        /**
         * 检查认证是否完成
         * 由于SSL认证在传输层已经完成，此方法始终返回true
         * 这表明SSL认证器不需要额外的认证步骤
         * 
         * @return 始终返回true，表示认证已完成
         */
        @Override
        public boolean complete() {
            return true;
        }
    }
}
