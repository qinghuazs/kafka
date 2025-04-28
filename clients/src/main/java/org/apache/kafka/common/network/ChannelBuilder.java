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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.memory.MemoryPool;

import java.nio.channels.SelectionKey;


/**
 * ChannelBuilder接口用于基于配置构建网络通道
 * 
 * 该接口是Kafka网络层的核心组件之一，主要负责创建和管理网络通信通道。它支持不同的传输层实现（如PlaintextChannelBuilder和SslChannelBuilder），
 * 使Kafka能够灵活地处理不同的网络协议和安全需求。
 * 
 * 实现类需要：
 * 1. 提供传输层（TransportLayer）实现，处理底层网络I/O
 * 2. 配置认证器（Authenticator），处理安全认证
 * 3. 管理内存分配，支持高效的网络数据传输
 * 4. 维护通道元数据，用于监控和管理
 */
public interface ChannelBuilder extends AutoCloseable, Configurable {

    /**
     * 构建一个配置了传输层和认证器的KafkaChannel
     * 
     * 该方法是创建Kafka网络通道的核心方法，它将：
     * 1. 初始化传输层，建立底层网络连接
     * 2. 配置认证机制，确保通信安全
     * 3. 设置内存管理，优化数据传输性能
     * 4. 注册通道元数据，便于监控和管理
     * 
     * @param  id 通道标识符，用于唯一标识一个网络连接
     * @param  key Java NIO的SelectionKey，用于非阻塞IO操作
     * @param  maxReceiveSize 单个接收缓冲区的最大分配大小，用于控制内存使用
     * @param  memoryPool 用于分配缓冲区的内存池，如果为null则不使用内存池
     * @param  metadataRegistry 存储通道元数据的注册表，用于跟踪和管理通道状态
     * @return KafkaChannel 返回配置完成的Kafka通道实例
     * @throws KafkaException 当通道创建过程中发生错误时抛出异常
     */
    KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                              MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException;

    /**
     * 关闭ChannelBuilder，释放相关资源
     * 
     * 实现类需要确保：
     * 1. 正确关闭所有打开的网络连接
     * 2. 释放所有分配的系统资源
     * 3. 清理相关的安全凭证
     */
    @Override
    void close();

}
