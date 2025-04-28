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
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 明文通道构建器，用于创建和管理不需要加密的网络连接通道。
 * 该类实现了ChannelBuilder接口，主要用于以下场景：
 * 1. 在开发测试环境中快速搭建网络连接
 * 2. 在内部可信网络环境中建立高性能的通信通道
 * 3. 作为其他安全通道实现的基础类
 */
public class PlaintextChannelBuilder implements ChannelBuilder {
    /**
     * 监听器名称，用于标识网络监听器
     * 在broker端运行时不为null，在客户端运行时为null
     */
    private final ListenerName listenerName;

    /**
     * 配置信息映射，存储通道构建器的配置参数
     */
    private Map<String, ?> configs;

    /**
     * 构造明文通道构建器
     * 
     * @param listenerName 监听器名称，在broker端实例化时不为null，在客户端为null
     */
    public PlaintextChannelBuilder(ListenerName listenerName) {
        this.listenerName = listenerName;
    }

    /**
     * 配置通道构建器
     * 
     * @param configs 配置参数映射，包含构建通道所需的各种配置项
     * @throws KafkaException 如果配置过程中发生错误
     */
    public void configure(Map<String, ?> configs) throws KafkaException {
        // 保存配置信息，供后续创建认证器等组件使用
        this.configs = configs;
    }

    /**
     * 构建Kafka通道
     * 该方法实现了ChannelBuilder接口的核心方法，完成以下任务：
     * 1. 创建明文传输层
     * 2. 配置认证器
     * 3. 组装KafkaChannel
     * 
     * @param id 通道标识符
     * @param key 用于非阻塞IO操作的SelectionKey
     * @param maxReceiveSize 最大接收大小限制
     * @param memoryPool 内存池，用于管理网络数据的内存分配
     * @param metadataRegistry 元数据注册表
     * @return 配置完成的KafkaChannel实例
     * @throws KafkaException 如果通道创建过程中发生错误
     */
    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        PlaintextTransportLayer transportLayer = null;
        try {
            // 创建明文传输层
            transportLayer = buildTransportLayer(key);
            final PlaintextTransportLayer finalTransportLayer = transportLayer;
            // 创建认证器供应器，用于延迟创建认证器实例
            Supplier<Authenticator> authenticatorCreator = () -> new PlaintextAuthenticator(configs, finalTransportLayer, listenerName);
            // 构建并返回完整的KafkaChannel
            return buildChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            // 理想情况下这些资源应该由KafkaChannel关闭，但如果KafkaChannel创建失败，
            // 构建器需要负责关闭这些资源
            Utils.closeQuietly(transportLayer, "transport layer for channel Id: " + id);
            throw new KafkaException(e);
        }
    }

    /**
     * 构建Kafka通道的内部方法，主要用于测试
     * 该方法将所有组件组装成一个完整的KafkaChannel
     * 
     * @param id 通道标识符
     * @param transportLayer 传输层实现
     * @param authenticatorCreator 认证器创建器
     * @param maxReceiveSize 最大接收大小
     * @param memoryPool 内存池
     * @param metadataRegistry 元数据注册表
     * @return 组装完成的KafkaChannel实例
     */
    KafkaChannel buildChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorCreator,
                              int maxReceiveSize, MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) {
        // 创建并返回新的KafkaChannel实例，组装所有必要的组件
        return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize, memoryPool, metadataRegistry);
    }

    /**
     * 构建明文传输层
     * 该方法创建一个基本的、不带加密功能的传输层实现
     * 
     * @param key 用于非阻塞IO操作的SelectionKey
     * @return 新创建的明文传输层实例
     */
    protected PlaintextTransportLayer buildTransportLayer(SelectionKey key) {
        // 创建并返回新的明文传输层实例
        return new PlaintextTransportLayer(key);
    }

    /**
     * 关闭通道构建器
     * 由于明文通道构建器没有需要特别清理的资源，此方法为空实现
     */
    @Override
    public void close() {}

    /**
     * 明文认证器实现类
     * 提供基本的身份认证功能，主要用于：
     * 1. 在不需要安全认证的场景下提供默认实现
     * 2. 记录客户端连接信息
     * 3. 支持自定义主体构建逻辑
     */
    private static class PlaintextAuthenticator implements Authenticator {
        /** 明文传输层，用于获取底层Socket连接信息 */
        private final PlaintextTransportLayer transportLayer;
        /** Kafka主体构建器，用于创建代表连接身份的KafkaPrincipal */
        private final KafkaPrincipalBuilder principalBuilder;
        /** 监听器名称，用于标识网络监听器 */
        private final ListenerName listenerName;

        /**
         * 构造明文认证器
         * 
         * @param configs 配置参数
         * @param transportLayer 明文传输层
         * @param listenerName 监听器名称
         */
        private PlaintextAuthenticator(Map<String, ?> configs, PlaintextTransportLayer transportLayer, ListenerName listenerName) {
            // 保存传输层引用，用于后续获取客户端地址
            this.transportLayer = transportLayer;
            // 创建主体构建器，用于生成代表身份的KafkaPrincipal
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, null);
            // 保存监听器名称
            this.listenerName = listenerName;
        }

        /**
         * 执行认证
         * 明文认证器不需要实际的认证过程，因此为空实现
         */
        @Override
        public void authenticate() {}

        /**
         * 获取表示连接身份的KafkaPrincipal
         * 
         * @return 代表连接身份的KafkaPrincipal实例
         * @throws IllegalStateException 如果在客户端模式下调用此方法
         */
        @Override
        public KafkaPrincipal principal() {
            // 获取客户端的网络地址
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
            // 在客户端模式下listenerName为null，此时不应调用principal方法
            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            // 使用客户端地址和监听器名称创建认证上下文，并构建KafkaPrincipal
            return principalBuilder.build(new PlaintextAuthenticationContext(clientAddress, listenerName.value()));
        }

        /**
         * 获取主体序列化器
         * 如果主体构建器实现了KafkaPrincipalSerde接口，则返回其实例
         * 
         * @return 主体序列化器的Optional包装
         */
        @Override
        public Optional<KafkaPrincipalSerde> principalSerde() {
            // 如果主体构建器支持序列化，则返回其序列化器实例
            return principalBuilder instanceof KafkaPrincipalSerde ? Optional.of((KafkaPrincipalSerde) principalBuilder) : Optional.empty();
        }

        /**
         * 检查认证是否完成
         * 明文认证总是立即完成
         * 
         * @return 始终返回true，表示认证已完成
         */
        @Override
        public boolean complete() {
            return true;
        }

        /**
         * 关闭认证器，释放相关资源
         * 主要用于清理主体构建器的资源
         */
        @Override
        public void close() {
            // 如果主体构建器实现了Closeable接口，则安全地关闭它
            if (principalBuilder instanceof Closeable)
                Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }
    }

}
