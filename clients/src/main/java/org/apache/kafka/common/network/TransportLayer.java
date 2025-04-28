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

/*
 * 底层通信的传输层。
 * 这是一个对SocketChannel的基础封装，可以作为SocketChannel和其他网络通道实现的替代品。
 * 随着NetworkClient替代BlockingChannel和其他实现，我们将使用KafkaChannel作为网络I/O通道。
 * 
 * 该接口是Kafka网络通信的核心组件，主要职责：
 * 1. 提供统一的网络传输抽象，支持不同的传输协议（如PLAINTEXT、SSL）
 * 2. 管理底层Socket连接的生命周期（建立、维护、关闭）
 * 3. 处理安全认证和加密（对于SSL实现）
 * 4. 支持非阻塞I/O操作
 * 
 * 实现类需要：
 * 1. 确保线程安全，因为网络操作通常涉及多线程
 * 2. 正确处理网络异常和资源清理
 * 3. 实现高效的数据传输机制
 * 4. 提供必要的监控指标
 */

import org.apache.kafka.common.errors.AuthenticationException;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.Principal;

public interface TransportLayer extends ScatteringByteChannel, TransferableChannel {

    /**
     * 检查通道是否已完成握手和认证
     * 
     * 应用场景：
     * 1. 在进行数据传输前检查通道状态
     * 2. 确保安全通信已经建立
     * 3. 用于连接状态管理
     * 
     * @return 如果通道已完成握手和认证则返回true，否则返回false
     */
    boolean ready();

    /**
     * 完成Socket通道的连接过程
     * 
     * 实现细节：
     * 1. 用于非阻塞模式下完成连接的建立
     * 2. 检查连接是否成功建立
     * 3. 设置必要的Socket选项
     * 
     * @return 如果连接成功建立返回true，否则返回false
     * @throws IOException 当连接过程中发生I/O错误时抛出
     */
    boolean finishConnect() throws IOException;

    /**
     * 断开Socket通道的连接
     * 
     * 实现要求：
     * 1. 安全地关闭底层Socket连接
     * 2. 清理相关的资源和状态
     * 3. 处理任何未完成的操作
     */
    void disconnect();

    /**
     * 检查通道的网络Socket是否处于连接状态
     * 
     * 使用场景：
     * 1. 在进行I/O操作前检查连接状态
     * 2. 监控连接健康状况
     * 3. 连接恢复机制的判断依据
     * 
     * @return 如果Socket已连接返回true，否则返回false
     */
    boolean isConnected();

    /**
     * 获取底层的SocketChannel实例
     * 
     * 使用场景：
     * 1. 需要直接访问底层Socket功能时
     * 2. 进行Socket选项的精细配置
     * 3. 实现特定的网络操作
     * 
     * @return 返回底层的SocketChannel对象
     */
    SocketChannel socketChannel();

    /**
     * 获取底层的SelectionKey实例
     * 
     * 应用场景：
     * 1. 管理通道的I/O事件监听
     * 2. 修改通道的事件注册
     * 3. 获取通道的附加对象
     * 
     * @return 返回与该通道关联的SelectionKey对象
     */
    SelectionKey selectionKey();

    /**
     * 执行通道的握手过程
     * 
     * 不同实现的处理：
     * 1. PLAINTEXT实现：无需任何操作
     * 2. SSL实现：执行SSL握手
     *    - 可配置客户端认证
     *    - 使用{@link org.apache.kafka.common.config.internals.BrokerSecurityConfigs#SSL_CLIENT_AUTH_CONFIG}进行配置
     * 
     * 实现细节：
     * 1. 协议协商
     * 2. 证书验证
     * 3. 密钥交换
     * 4. 会话建立
     * 
     * @throws AuthenticationException 当SSL握手失败时抛出，通常是由于{@link javax.net.ssl.SSLException}
     * @throws IOException 当握手过程中发生读写错误时抛出
     */
    void handshake() throws AuthenticationException, IOException;

    /**
     * 获取对等方的身份信息
     * 
     * 返回值说明：
     * 1. SSL传输层且存在已认证的对等方：返回`SSLSession.getPeerPrincipal()`
     * 2. 其他情况：返回`KafkaPrincipal.ANONYMOUS`
     * 
     * 使用场景：
     * 1. 身份验证
     * 2. 访问控制
     * 3. 审计日志
     * 
     * @return 返回对等方的Principal对象
     * @throws IOException 当获取过程中发生I/O错误时抛出
     */
    Principal peerPrincipal() throws IOException;

    /**
     * 添加通道感兴趣的I/O事件
     * 
     * 实现说明：
     * 1. 通过位操作添加新的事件类型
     * 2. 自动更新SelectionKey的interest set
     * 3. 线程安全的操作
     * 
     * @param ops 要添加的操作类型，来自SelectionKey的常量
     */
    void addInterestOps(int ops);

    /**
     * 移除通道感兴趣的I/O事件
     * 
     * 实现说明：
     * 1. 通过位操作移除指定的事件类型
     * 2. 自动更新SelectionKey的interest set
     * 3. 线程安全的操作
     * 
     * @param ops 要移除的操作类型，来自SelectionKey的常量
     */
    void removeInterestOps(int ops);

    /**
     * 检查通道是否处于静默状态
     * 
     * 使用场景：
     * 1. 流量控制
     * 2. 背压处理
     * 3. 连接状态检查
     * 
     * @return 如果通道处于静默状态返回true，否则返回false
     */
    boolean isMute();

    /**
     * 检查通道是否有缓存的待读取数据
     * 
     * 实现细节：
     * 1. 检查中间缓冲区中是否有未处理的数据
     * 2. 不需要实际从网络读取新数据
     * 3. 用于优化读取操作
     * 
     * 应用场景：
     * 1. 提高数据处理效率
     * 2. 减少不必要的网络I/O
     * 3. 实现零拷贝操作
     * 
     * @return 如果有缓存的待处理数据返回true，否则返回false
     */
    boolean hasBytesBuffered();
}
