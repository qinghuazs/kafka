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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.internals.IntGaugeSuite;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一个用于执行非阻塞多连接网络I/O的nioSelector接口。
 * <p>
 * 该类与{@link NetworkSend}和{@link NetworkReceive}配合使用，用于传输大小固定的网络请求和响应。
 * <p>
 * 可以通过以下方式将连接添加到与整数ID关联的nioSelector：
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * connect调用不会阻塞TCP连接的创建，因此connect方法仅开始初始化连接。
 * 成功调用此方法并不意味着已建立有效连接。
 *
 * 在现有连接上发送请求、接收响应、处理连接完成和断开连接都是通过<code>poll()</code>调用完成的。
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * nioSelector维护着几个列表，这些列表在每次调用<code>poll()</code>时都会重置，
 * 可以通过各种getter方法获取。这些列表在每次调用<code>poll()</code>时都会重置。
 *
 * 此类不是线程安全的！
 */
public class Selector implements Selectable, AutoCloseable {

    /**
     * 表示禁用空闲超时的常量值
     */
    public static final long NO_IDLE_TIMEOUT_MS = -1;

    /**
     * 表示禁用身份验证失败延迟的常量值
     */
    public static final int NO_FAILED_AUTHENTICATION_DELAY = 0;

    /**
     * 定义了关闭连接的不同模式
     */
    private enum CloseMode {
        /**
         * 优雅关闭：处理未完成的缓冲接收，并通知断开连接
         */
        GRACEFUL(true),
        /**
         * 仅通知：丢弃所有未完成的接收，并通知断开连接
         */
        NOTIFY_ONLY(true),
        /**
         * 静默丢弃：丢弃所有未完成的接收，不通知断开连接
         */
        DISCARD_NO_NOTIFY(false);

        final boolean notifyDisconnect;

        CloseMode(boolean notifyDisconnect) {
            this.notifyDisconnect = notifyDisconnect;
        }
    }

    // 日志记录器
    private final Logger log;
    // Java NIO选择器，用于多路复用I/O操作
    private final java.nio.channels.Selector nioSelector;
    // 维护所有活动的Kafka通道，key为连接ID
    private final Map<String, KafkaChannel> channels;
    // 存储显式静音的通道集合
    private final Set<KafkaChannel> explicitlyMutedChannels;
    // 标记是否处于内存不足状态
    private boolean outOfMemory;
    // 存储已完成发送的网络请求列表
    private final List<NetworkSend> completedSends;
    // 存储已完成接收的网络响应，按接收顺序保存
    private final LinkedHashMap<String, NetworkReceive> completedReceives;
    // 存储立即连接成功的SelectionKey集合
    private final Set<SelectionKey> immediatelyConnectedKeys;
    // 正在关闭的通道映射，key为连接ID
    private final Map<String, KafkaChannel> closingChannels;
    // 具有缓冲读取数据的SelectionKey集合
    private Set<SelectionKey> keysWithBufferedRead;
    // 已断开连接的通道状态映射，key为连接ID
    private final Map<String, ChannelState> disconnected;
    // 已成功连接的连接ID列表
    private final List<String> connected;
    // 发送失败的连接ID列表
    private final List<String> failedSends;
    // 时间工具类实例
    private final Time time;
    // 选择器相关的度量指标
    private final SelectorMetrics sensors;
    // 用于创建新通道的构建器
    private final ChannelBuilder channelBuilder;
    // 单个网络接收的最大字节数
    private final int maxReceiveSize;
    // 是否记录每个连接的时间统计
    private final boolean recordTimePerConnection;
    // 空闲连接过期管理器
    private final IdleExpiryManager idleExpiryManager;
    // 延迟关闭的身份验证失败通道，按关闭顺序保存
    private final LinkedHashMap<String, DelayedAuthenticationFailureClose> delayedClosingChannels;
    // 内存池，用于管理网络操作的内存分配
    private final MemoryPool memoryPool;
    // 内存不足阈值
    private final long lowMemThreshold;
    // 身份验证失败后的延迟关闭时间(毫秒)
    private final int failedAuthenticationDelayMs;

    // 标记上一次poll调用是否在读取已缓冲数据时取得进展
    // 用于防止在没有可用内存读取更多数据时陷入紧密循环
    private boolean madeReadProgressLastPoll = true;

    /**
     * 创建一个新的NIO选择器
     * 
     * 这是Selector类的主要构造函数，用于初始化一个非阻塞I/O多路复用器。该选择器负责管理多个网络连接，
     * 提供高效的网络I/O操作。它支持连接生命周期管理、内存管理、度量收集等核心功能。
     *
     * @param maxReceiveSize 单个网络接收的最大字节数 (使用 {@link NetworkReceive#UNLIMITED} 表示无限制)
     * @param connectionMaxIdleMs 连接最大空闲时间 (使用 {@link #NO_IDLE_TIMEOUT_MS} 禁用空闲超时)
     * @param failedAuthenticationDelayMs 身份验证失败后延迟关闭连接的最小时间
     *                                    使用 {@link #NO_FAILED_AUTHENTICATION_DELAY} 可禁用此延迟
     * @param metrics 用于注册选择器度量指标的注册表
     * @param time 时间实现类，用于时间相关操作
     * @param metricGrpPrefix 选择器注册的度量指标组前缀
     * @param metricTags 添加到选择器注册的度量指标的额外标签
     * @param metricsPerConnection 是否启用每个连接的度量指标
     * @param recordTimePerConnection 是否记录每个连接的时间统计信息
     * @param channelBuilder 用于创建新连接的通道构建器
     * @param memoryPool 用于网络操作的内存池
     * @param logContext 带有附加信息的日志上下文
     */
    public Selector(int maxReceiveSize,
            long connectionMaxIdleMs,
            int failedAuthenticationDelayMs,
            Metrics metrics,
            Time time,
            String metricGrpPrefix,
            Map<String, String> metricTags,
            boolean metricsPerConnection,
            boolean recordTimePerConnection,
            ChannelBuilder channelBuilder,
            MemoryPool memoryPool,
            LogContext logContext) {
        try {
            // 创建Java NIO选择器实例，用于多路复用I/O操作
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        // 设置单个网络接收的最大字节数限制
        this.maxReceiveSize = maxReceiveSize;
        // 设置时间工具实例，用于时间相关操作
        this.time = time;
        // 初始化用于存储活动通道的映射，key为连接ID
        this.channels = new HashMap<>();
        // 初始化显式静音的通道集合
        this.explicitlyMutedChannels = new HashSet<>();
        // 初始化内存不足状态标记
        this.outOfMemory = false;
        // 初始化已完成发送的请求列表
        this.completedSends = new ArrayList<>();
        // 初始化已完成接收的响应映射，使用LinkedHashMap保持接收顺序
        this.completedReceives = new LinkedHashMap<>();
        // 初始化立即连接成功的SelectionKey集合
        this.immediatelyConnectedKeys = new HashSet<>();
        // 初始化正在关闭的通道映射
        this.closingChannels = new HashMap<>();
        // 初始化具有缓冲读取数据的SelectionKey集合
        this.keysWithBufferedRead = new HashSet<>();
        // 初始化已成功连接的连接ID列表
        this.connected = new ArrayList<>();
        // 初始化已断开连接的通道状态映射
        this.disconnected = new HashMap<>();
        // 初始化发送失败的连接ID列表
        this.failedSends = new ArrayList<>();
        // 设置日志记录器
        this.log = logContext.logger(Selector.class);
        // 初始化选择器相关的度量指标
        this.sensors = new SelectorMetrics(metrics, metricGrpPrefix, metricTags, metricsPerConnection);
        // 设置通道构建器，用于创建新的连接通道
        this.channelBuilder = channelBuilder;
        // 设置是否记录每个连接的时间统计
        this.recordTimePerConnection = recordTimePerConnection;
        // 如果设置了最大空闲时间，创建空闲连接过期管理器
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);
        // 设置内存池，用于管理网络操作的内存分配
        this.memoryPool = memoryPool;
        // 设置内存不足阈值为内存池大小的10%
        this.lowMemThreshold = (long) (0.1 * this.memoryPool.size());
        // 设置身份验证失败后的延迟关闭时间
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
        // 如果启用了身份验证失败延迟，初始化延迟关闭通道映射
        this.delayedClosingChannels = (failedAuthenticationDelayMs > NO_FAILED_AUTHENTICATION_DELAY) ? new LinkedHashMap<>() : null;
    }

    /**
     * 创建一个新的NIO选择器，不启用身份验证失败延迟
     * 
     * 这个构造函数是主构造函数的简化版本，它禁用了身份验证失败延迟机制。适用于不需要处理身份验证失败延迟的场景。
     * 内部调用主构造函数，将failedAuthenticationDelayMs设置为NO_FAILED_AUTHENTICATION_DELAY。
     *
     * @param maxReceiveSize 单个网络接收的最大字节数
     * @param connectionMaxIdleMs 连接最大空闲时间
     * @param metrics 度量指标注册表
     * @param time 时间实现类
     * @param metricGrpPrefix 度量指标组前缀
     * @param metricTags 度量指标额外标签
     * @param metricsPerConnection 是否启用每个连接的度量指标
     * @param recordTimePerConnection 是否记录每个连接的时间统计
     * @param channelBuilder 通道构建器
     * @param memoryPool 内存池
     * @param logContext 日志上下文
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    boolean recordTimePerConnection,
                    ChannelBuilder channelBuilder,
                    MemoryPool memoryPool,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags,
                metricsPerConnection, recordTimePerConnection, channelBuilder, memoryPool, logContext);
    }

    /**
     * 创建一个新的NIO选择器，不启用连接时间统计
     * 
     * 这个构造函数适用于需要身份验证失败延迟但不需要记录连接时间统计的场景。
     * 它将recordTimePerConnection设置为false，并使用MemoryPool.NONE作为内存池。
     *
     * @param maxReceiveSize 单个网络接收的最大字节数
     * @param connectionMaxIdleMs 连接最大空闲时间
     * @param failedAuthenticationDelayMs 身份验证失败延迟时间
     * @param metrics 度量指标注册表
     * @param time 时间实现类
     * @param metricGrpPrefix 度量指标组前缀
     * @param metricTags 度量指标额外标签
     * @param metricsPerConnection 是否启用每个连接的度量指标
     * @param channelBuilder 通道构建器
     * @param logContext 日志上下文
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    int failedAuthenticationDelayMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, false, channelBuilder, MemoryPool.NONE, logContext);
    }

    /**
     * 创建一个新的NIO选择器，不启用身份验证失败延迟和连接时间统计
     * 
     * 这个构造函数是更简化的版本，它同时禁用了身份验证失败延迟和连接时间统计功能。
     * 适用于基本的网络I/O场景，不需要这些高级特性。
     *
     * @param maxReceiveSize 单个网络接收的最大字节数
     * @param connectionMaxIdleMs 连接最大空闲时间
     * @param metrics 度量指标注册表
     * @param time 时间实现类
     * @param metricGrpPrefix 度量指标组前缀
     * @param metricTags 度量指标额外标签
     * @param metricsPerConnection 是否启用每个连接的度量指标
     * @param channelBuilder 通道构建器
     * @param logContext 日志上下文
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, channelBuilder, logContext);
    }

    /**
     * 创建一个新的NIO选择器，使用最简配置
     * 
     * 这是最简化的构造函数版本，适用于只需要基本网络I/O功能的场景。它：
     * - 不限制接收大小 (使用NetworkReceive.UNLIMITED)
     * - 启用每个连接的度量指标
     * - 使用空的度量标签集
     * - 不启用身份验证失败延迟
     *
     * @param connectionMaxIdleMS 连接最大空闲时间
     * @param metrics 度量指标注册表
     * @param time 时间实现类
     * @param metricGrpPrefix 度量指标组前缀
     * @param channelBuilder 通道构建器
     * @param logContext 日志上下文
     */
    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    /**
     * 创建一个新的NIO选择器，使用最简配置但支持身份验证失败延迟
     * 
     * 这个构造函数在最简配置的基础上添加了身份验证失败延迟支持。它：
     * - 不限制接收大小 (使用NetworkReceive.UNLIMITED)
     * - 启用每个连接的度量指标
     * - 使用空的度量标签集
     * - 支持配置身份验证失败延迟时间
     *
     * @param connectionMaxIdleMS 连接最大空闲时间
     * @param failedAuthenticationDelayMs 身份验证失败延迟时间
     * @param metrics 度量指标注册表
     * @param time 时间实现类
     * @param metricGrpPrefix 度量指标组前缀
     * @param channelBuilder 通道构建器
     * @param logContext 日志上下文
     */
    public Selector(long connectionMaxIdleMS, int failedAuthenticationDelayMs, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    /**
     * 开始连接到指定地址，并将连接与给定ID关联添加到此nioSelector。
     * <p>
     * 注意：此调用仅启动连接过程，连接的完成将在未来的{@link #poll(long)}调用中进行。
     * 可以通过{@link #connected()}检查在给定的poll调用后哪些连接（如果有）已完成。
     * <p>
     * 此方法实现了非阻塞连接建立，主要步骤包括：
     * 1. 创建并配置SocketChannel
     * 2. 尝试建立连接
     * 3. 注册通道到Selector
     * 4. 处理立即连接成功的情况
     *
     * @param id 新连接的标识符
     * @param address 要连接的目标地址
     * @param sendBufferSize 新连接的发送缓冲区大小
     * @param receiveBufferSize 新连接的接收缓冲区大小
     * @throws IllegalStateException 如果指定ID的连接已存在
     * @throws IOException 如果DNS解析失败或broker宕机
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        // 确保该ID未被注册使用
        ensureNotRegistered(id);
        // 创建新的SocketChannel
        SocketChannel socketChannel = SocketChannel.open();
        SelectionKey key = null;
        try {
            // 配置SocketChannel的属性（非阻塞模式、TCP参数等）
            configureSocketChannel(socketChannel, sendBufferSize, receiveBufferSize);
            // 尝试建立连接，对于本地连接可能立即成功
            boolean connected = doConnect(socketChannel, address);
            // 将通道注册到选择器，关注连接事件
            key = registerChannel(id, socketChannel, SelectionKey.OP_CONNECT);

            if (connected) {
                // 对于立即连接成功的通道，不需要触发OP_CONNECT事件
                log.debug("Immediately connected to node {}", id);
                immediatelyConnectedKeys.add(key);
                // 清除所有事件监听
                key.interestOps(0);
            }
        } catch (IOException | RuntimeException e) {
            // 发生异常时进行清理：移除key、关闭通道
            if (key != null)
                immediatelyConnectedKeys.remove(key);
            channels.remove(id);
            socketChannel.close();
            throw e;
        }
    }

    /**
     * 执行实际的连接操作。
     * <p>
     * 此方法可见性为protected，允许测试用例重写以实现阻塞连接，
     * 特别是用于模拟
     */
    protected boolean doConnect(SocketChannel channel, InetSocketAddress address) throws IOException {
        try {
            return channel.connect(address);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Can't resolve address: " + address, e);
        }
    }

    /**
     * 配置SocketChannel的各项参数。
     * <p>
     * 此方法设置以下TCP连接属性：
     * 1. 非阻塞模式 - 用于NIO操作
     * 2. TCP Keep-Alive - 保持连接活跃
     * 3. 发送和接收缓冲区大小 - 优化网络性能
     * 4. TCP_NODELAY - 禁用Nagle算法，减少延迟
     *
     * @param socketChannel 要配置的SocketChannel
     * @param sendBufferSize 发送缓冲区大小
     * @param receiveBufferSize 接收缓冲区大小
     * @throws IOException 如果配置过程中发生错误
     */
    private void configureSocketChannel(SocketChannel socketChannel, int sendBufferSize, int receiveBufferSize)
            throws IOException {
        // 设置为非阻塞模式，这是NIO操作的基础
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        // 启用TCP Keep-Alive，用于检测连接是否存活
        socket.setKeepAlive(true);
        // 如果指定了自定义缓冲区大小，则设置它
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);
        // 禁用Nagle算法，提高小数据包的传输效率
        socket.setTcpNoDelay(true);
    }

    /**
     * 将已存在的通道注册到nioSelector。
     * <p>
     * 此方法主要用于服务器端，当连接被其他线程接受但需要由Selector处理时使用。
     * <p>
     * 注册过程的关键点：
     * 1. 确保连接ID的唯一性，避免端口重用导致的冲突
     * 2. 记录连接创建的度量指标
     * 3. 初始化客户端元数据信息
     * <p>
     * 设计考虑：
     * - Kafka broker通过在连接ID中添加递增索引来避免在同一远程主机:端口的新旧连接交替时发生ID重用
     * - 即使在ApiVersionsRequest不是强制的情况下，也会记录连接信息
     * - 如果无法为连接创建KafkaChannel，会关闭socketChannel并取消其选择键
     *
     * @param id 连接的唯一标识符
     * @param socketChannel 要注册的SocketChannel
     * @throws IOException 如果注册过程中发生错误
     * @throws IllegalStateException 如果指定ID的连接已存在
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        // 确保连接ID未被使用
        ensureNotRegistered(id);
        // 注册通道并设置为读取操作
        registerChannel(id, socketChannel, SelectionKey.OP_READ);
        // 记录连接创建的度量指标
        this.sensors.connectionCreated.record();
        // 设置默认的空客户端信息，因为ApiVersionsRequest不是强制的
        // 但我们仍然需要记录该连接
        ChannelMetadataRegistry metadataRegistry = this.channel(id).channelMetadataRegistry();
        if (metadataRegistry.clientInformation() == null)
            metadataRegistry.registerClientInformation(ClientInformation.EMPTY);
    }

    /**
     * 确保指定的连接ID未被注册使用。
     * <p>
     * 此方法检查两个方面：
     * 1. 活动连接集合中是否存在该ID
     * 2. 正在关闭的连接集合中是否存在该ID
     * <p>
     * 这种双重检查确保了连接ID的唯一性，防止：
     * - 重复注册相同ID的连接
     * - 在旧连接完全关闭前重用ID
     *
     * @param id 要检查的连接ID
     * @throws IllegalStateException 如果ID已被使用或正在关闭中
     */
    private void ensureNotRegistered(String id) {
        // 检查活动连接集合
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);
        // 检查正在关闭的连接集合
        if (this.closingChannels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id + " that is still being closed");
    }

    /**
     * 将SocketChannel注册到选择器并创建对应的KafkaChannel。
     * <p>
     * 此方法完成以下任务：
     * 1. 向选择器注册通道并设置感兴趣的操作
     * 2. 创建并配置KafkaChannel
     * 3. 将通道添加到活动连接集合
     * 4. 更新空闲连接管理器
     * <p>
     * 设计考虑：
     * - 使用KafkaChannel封装底层SocketChannel，提供更高级的功能
     * - 支持空闲连接超时管理
     * - 维护连接的生命周期
     *
     * @param id 连接ID
     * @param socketChannel 要注册的SocketChannel
     * @param interestedOps 感兴趣的操作（如OP_READ、OP_WRITE等）
     * @return 注册后的SelectionKey
     * @throws IOException 如果注册过程中发生错误
     */
    protected SelectionKey registerChannel(String id, SocketChannel socketChannel, int interestedOps) throws IOException {
        // 将通道注册到选择器，设置感兴趣的操作
        SelectionKey key = socketChannel.register(nioSelector, interestedOps);
        // 创建并配置KafkaChannel
        KafkaChannel channel = buildAndAttachKafkaChannel(socketChannel, id, key);
        // 将通道添加到活动连接集合
        this.channels.put(id, channel);
        // 如果启用了空闲连接管理，更新最后活动时间
        if (idleExpiryManager != null)
            idleExpiryManager.update(channel.id(), time.nanoseconds());
        return key;
    }

    /**
     * 构建并附加一个新的KafkaChannel到给定的SocketChannel和SelectionKey
     * 
     * 此方法负责创建一个新的KafkaChannel实例，并将其与指定的SocketChannel和SelectionKey关联。
     * 它处理通道创建过程中的所有资源管理和错误处理。
     *
     * @param socketChannel 要附加通道的底层SocketChannel
     * @param id 新通道的唯一标识符
     * @param key 与通道关联的SelectionKey
     * @return 新创建的KafkaChannel实例
     * @throws IOException 如果通道创建失败
     */
    private KafkaChannel buildAndAttachKafkaChannel(SocketChannel socketChannel, String id, SelectionKey key) throws IOException {
        ChannelMetadataRegistry metadataRegistry = null;
        try {
            // 创建新的通道元数据注册表
            metadataRegistry = new SelectorChannelMetadataRegistry();
            // 使用通道构建器创建新的KafkaChannel实例
            KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize, memoryPool, metadataRegistry);
            // 将新创建的通道附加到SelectionKey
            key.attach(channel);
            return channel;
        } catch (Exception e) {
            try {
                // 发生异常时关闭SocketChannel
                socketChannel.close();
            } finally {
                // 取消SelectionKey的注册
                key.cancel();
            }
            // 理想情况下，这些资源应该由KafkaChannel关闭，但如果KafkaChannel创建失败，
            // 构建器应该负责关闭它已创建的资源
            Utils.closeQuietly(metadataRegistry, "metadataRegistry");
            throw new IOException("Channel could not be created for socket " + socketChannel, e);
        }
    }

    /**
     * 唤醒正在等待I/O操作的nioSelector
     * 
     * 此方法用于中断nioSelector的阻塞状态，通常在需要立即处理新的I/O事件时调用。
     * 例如，当新的连接请求或数据发送请求到达时，可以调用此方法来确保选择器及时响应。
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * 关闭此选择器和所有相关的连接
     * 
     * 此方法执行完整的清理过程，包括：
     * 1. 关闭所有活动的网络连接
     * 2. 关闭底层的NIO选择器
     * 3. 清理度量收集器
     * 4. 关闭通道构建器
     * 
     * 即使在关闭过程中发生异常，也会尽可能地继续清理其他资源。
     * 特别是确保度量传感器被清理，因为保留旧的传感器可能导致ReplicaFetcherThread启动失败。
     */
    @Override
    public void close() {
        // 获取所有活动连接的ID列表
        List<String> connections = new ArrayList<>(channels.keySet());
        // 用于记录第一个发生的异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        // 安静地关闭所有连接，即使发生异常也继续处理
        Utils.closeAllQuietly(firstException, "release connections",
                connections.stream().map(id -> (AutoCloseable) () -> close(id)).toArray(AutoCloseable[]::new));
        // 如果在close(id)中抛出异常，我们仍然应该能够关闭剩余的对象，
        // 特别是传感器，因为保留传感器可能导致ReplicaFetcherThread启动失败
        // （如果具有相同名称的旧传感器尚未清理）
        Utils.closeQuietly(nioSelector, "nioSelector", firstException);
        Utils.closeQuietly(sensors, "sensors", firstException);
        Utils.closeQuietly(channelBuilder, "channelBuilder", firstException);
        // 处理关闭过程中可能发生的异常
        Throwable exception = firstException.get();
        if (exception instanceof RuntimeException && !(exception instanceof SecurityException)) {
            throw (RuntimeException) exception;
        }
    }

    /**
     * 将给定的请求加入发送队列，等待后续的{@link #poll(long)}调用处理
     * 
     * 此方法处理网络请求的发送过程，包括：
     * 1. 验证目标连接的状态
     * 2. 处理正在关闭的连接
     * 3. 设置发送请求
     * 4. 处理发送过程中的异常
     *
     * @param send 要发送的网络请求
     */
    public void send(NetworkSend send) {
        // 获取目标连接ID
        String connectionId = send.destinationId();
        // 获取打开或正在关闭的通道，如果不存在则抛出异常
        KafkaChannel channel = openOrClosingChannelOrFail(connectionId);
        if (closingChannels.containsKey(connectionId)) {
            // 如果通道正在关闭，确保通过`disconnected`通知，保持通道处于触发关闭时的状态
            this.failedSends.add(connectionId);
        } else {
            try {
                // 设置要发送的数据
                channel.setSend(send);
            } catch (Exception e) {
                // 更新状态以保持一致性，通道将在`close`后被丢弃
                channel.state(ChannelState.FAILED_SEND);
                // 确保在下一次poll处理failedSends时通过`disconnected`通知
                this.failedSends.add(connectionId);
                close(channel, CloseMode.DISCARD_NO_NOTIFY);
                if (!(e instanceof CancelledKeyException)) {
                    log.error("Unexpected exception during send, closing connection {} and rethrowing exception.",
                            connectionId, e);
                    throw e;
                }
            }
        }
    }

    /**
     * 在每个连接上执行所有可能的非阻塞I/O操作。这包括完成连接建立、断开连接、
     * 启动新的发送操作，以及推进正在进行的发送或接收操作。
     * 
     * 当此方法调用完成后，用户可以通过以下方法检查已完成的操作：
     * {@link #completedSends()} - 已完成的发送操作
     * {@link #completedReceives()} - 已完成的接收操作
     * {@link #connected()} - 已建立的连接
     * {@link #disconnected()} - 已断开的连接
     * 这些列表会在每次poll调用开始时清空，并在有完成的I/O操作时重新填充。
     * 
     * 数据传输模式说明：
     * 1. 明文模式（Plaintext）：
     *    - 直接使用socketChannel进行网络读写
     *    - 数据无需加密解密处理
     * 
     * 2. SSL模式：
     *    - 写入数据前先加密，读取响应后需解密
     *    - 需要维护额外的缓冲区
     *    - 由于数据加密，无法精确读取Kafka协议要求的字节数
     *    - 每次读取最多可达SSLEngine的应用缓冲区大小
     *    - 可能读取超过请求大小的数据
     *    - 使用keysWithBufferedRead映射跟踪SSL缓冲区中有数据的通道
     *    - 当有缓冲数据可处理时，将timeout设为0并处理数据
     * 
     * 请求处理顺序保证：
     * - 每次poll调用中，每个通道最多只添加一个条目到completedReceives
     * - 这确保了来自同一通道的请求按发送顺序在broker上处理
     * - 因为SocketServer添加到请求队列的未完成请求可能被不同的请求处理线程处理
     * - 必须一次处理一个通道的请求以保证顺序性
     *
     * @param timeout 等待时间（毫秒），必须是非负数
     * @throws IllegalArgumentException 如果timeout为负数
     * @throws IllegalStateException 如果尝试发送数据但没有对应的连接，或者已有正在进行的发送操作
     */
    @Override
    public void poll(long timeout) throws IOException {
        // 验证超时参数的有效性
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        // 记录上次poll是否成功读取了数据
        boolean madeReadProgressLastCall = madeReadProgressLastPoll;
        // 清空所有完成的操作列表（发送、接收、连接、断开连接等）
        clear();

        // 检查是否有通道的SSL缓冲区中存在待处理的数据
        boolean dataInBuffers = !keysWithBufferedRead.isEmpty();

        // 如果有立即连接成功的通道，或者上次读取成功且还有缓冲数据，则立即处理（timeout=0）
        if (!immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
            timeout = 0;

        // 处理内存压力恢复的情况
        if (!memoryPool.isOutOfMemory() && outOfMemory) {
            // 从内存压力中恢复，取消静音所有未显式静音的通道
            log.trace("Broker no longer low on memory - unmuting incoming sockets");
            for (KafkaChannel channel : channels.values()) {
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    channel.maybeUnmute();
                }
            }
            outOfMemory = false;
        }

        // 执行选择操作，检查就绪的通道
        long startSelect = time.nanoseconds();
        int numReadyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        // 记录选择操作的耗时
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds(), false);

        // 如果有就绪的通道、立即连接成功的通道或缓冲数据，则处理它们
        if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
            // 获取所有就绪的SelectionKey
            Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

            // 处理具有缓冲数据的通道（SSL模式）
            if (dataInBuffers) {
                // 移除已在readyKeys中的通道，避免重复处理
                keysWithBufferedRead.removeAll(readyKeys);
                Set<SelectionKey> toPoll = keysWithBufferedRead;
                // 重置缓冲读取集合，poll调用会根据需要重新填充
                keysWithBufferedRead = new HashSet<>();
                pollSelectionKeys(toPoll, false, endSelect);
            }

            // 处理底层socket有更多数据的通道
            pollSelectionKeys(readyKeys, false, endSelect);
            // 清空已处理的就绪键，为下次select做准备
            readyKeys.clear();

            // 处理立即连接成功的通道
            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
            immediatelyConnectedKeys.clear();
        } else {
            // 没有工作也视为"进展"，因为这意味着所有数据都已处理完成
            madeReadProgressLastPoll = true;
        }

        // 记录I/O操作的结束时间
        long endIo = time.nanoseconds();
        // 记录本次I/O操作的总耗时（不包括选择操作的时间）
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds(), false);

        // 处理延迟关闭的通道
        // 某些通道（如认证失败的通道）会被延迟关闭，这里检查它们是否可以关闭了
        completeDelayedChannelClose(endIo);

        // 检查并可能关闭最旧的空闲连接
        // 使用select结束时的时间戳，确保不会关闭刚刚在pollSelectionKeys中处理过的连接
        maybeCloseOldestConnection(endSelect);
    }
        

    /**
     * 处理一组就绪的SelectionKey上的I/O操作
     * 
     * 该方法是Selector的核心实现，负责处理所有就绪的I/O事件。主要功能包括：
     * 1. 完成新建立的TCP连接（包括正常连接和立即连接）
     * 2. 处理通道的身份验证和重认证
     * 3. 执行数据的读写操作
     * 4. 处理连接异常和关闭
     * 5. 记录各种度量指标
     * 
     * 应用场景：
     * - 在Kafka的网络层中用于处理与其他broker和客户端之间的通信
     * - 支持大量并发连接的非阻塞I/O操作
     * - 处理连接的生命周期管理和异常情况
     * 
     * @param selectionKeys 要处理的SelectionKey集合
     * @param isImmediatelyConnected 是否处理刚刚建立连接的socket
     * @param currentTimeNanos 确定SelectionKey集合时的时间戳（纳秒）
     */
    // 包级私有，用于测试
    void pollSelectionKeys(Set<SelectionKey> selectionKeys,
                           boolean isImmediatelyConnected,
                           long currentTimeNanos) {
        // 遍历所有就绪的SelectionKey，按照确定的处理顺序进行处理
        for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
            // 获取与SelectionKey关联的KafkaChannel
            KafkaChannel channel = channel(key);
            // 如果需要记录每个连接的时间统计，获取当前时间戳
            long channelStartTimeNanos = recordTimePerConnection ? time.nanoseconds() : 0;
            // 标记是否发送失败
            boolean sendFailed = false;
            // 获取通道的唯一标识符
            String nodeId = channel.id();

            // 注册该连接的所有度量指标
            sensors.maybeRegisterConnectionMetrics(nodeId);
            // 如果启用了空闲连接管理，更新最后活动时间
            if (idleExpiryManager != null)
                idleExpiryManager.update(nodeId, currentTimeNanos);

            try {
                // 处理已完成握手的连接（包括正常完成和立即完成的情况）
                if (isImmediatelyConnected || key.isConnectable()) {
                    // 完成连接建立过程
                    if (channel.finishConnect()) {
                        // 将连接ID添加到已连接列表
                        this.connected.add(nodeId);
                        // 记录连接创建事件
                        this.sensors.connectionCreated.record();

                        // 获取底层的SocketChannel并记录其配置信息
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
                                nodeId);
                    } else {
                        // 如果连接未完成，继续处理下一个key
                        continue;
                    }
                }

                // 如果通道已连接但未就绪，完成准备工作（如SSL握手和身份验证）
                if (channel.isConnected() && !channel.ready()) {
                    channel.prepare();
                    if (channel.ready()) {
                        // 获取当前时间用于记录度量指标
                        long readyTimeMs = time.milliseconds();
                        // 判断是否是重新认证
                        boolean isReauthentication = channel.successfulAuthentications() > 1;
                        if (isReauthentication) {
                            // 记录重新认证成功事件
                            sensors.successfulReauthentication.record(1.0, readyTimeMs);
                            // 记录重新认证延迟时间
                            if (channel.reauthenticationLatencyMs() == null)
                                log.warn(
                                    "Should never happen: re-authentication latency for a re-authenticated channel was null; continuing...");
                            else
                                sensors.reauthenticationLatency
                                    .record(channel.reauthenticationLatencyMs().doubleValue(), readyTimeMs);
                        } else {
                            // 记录首次认证成功事件
                            sensors.successfulAuthentication.record(1.0, readyTimeMs);
                            // 如果客户端不支持重新认证，记录相应事件
                            if (!channel.connectedClientSupportsReauthentication())
                                sensors.successfulAuthenticationNoReauth.record(1.0, readyTimeMs);
                        }
                        log.debug("Successfully {}authenticated with {}", isReauthentication ?
                            "re-" : "", channel.socketDescription());
                    }
                }

                // 如果通道就绪且状态为未连接，将状态更新为就绪
                if (channel.ready() && channel.state() == ChannelState.NOT_CONNECTED)
                    channel.state(ChannelState.READY);

                // 处理在重新认证过程中接收到的响应
                Optional<NetworkReceive> responseReceivedDuringReauthentication = channel.pollResponseReceivedDuringReauthentication();
                responseReceivedDuringReauthentication.ifPresent(receive -> {
                    long currentTimeMs = time.milliseconds();
                    addToCompletedReceives(channel, receive, currentTimeMs);
                });

                // 如果通道就绪且有数据可读（从socket或缓冲区），且没有未处理的接收，且通道未被静音，则尝试读取数据
                if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasCompletedReceive(channel)
                        && !explicitlyMutedChannels.contains(channel)) {
                    attemptRead(channel);
                }

                // 如果通道有缓冲的数据且未被静音，将其添加到待处理读取的key集合
                if (channel.hasBytesBuffered() && !explicitlyMutedChannels.contains(channel)) {
                    // 这个通道在中间缓冲区中有未读取的数据（可能是由于内存不足）
                    // 由于底层socket可能在下次poll()时不会出现，我们需要记住这个通道
                    // 以便在下次poll调用时处理，否则数据可能永远卡在缓冲区中
                    // 如果尝试处理缓冲数据但没有进展，通道的缓冲状态会被清除以避免重复检查的开销
                    keysWithBufferedRead.add(key);
                }

                // 如果通道就绪，尝试向缓冲区有空间且有待发送数据的socket写入数据
                long nowNanos = channelStartTimeNanos != 0 ? channelStartTimeNanos : currentTimeNanos;
                try {
                    attemptWrite(key, channel, nowNanos);
                } catch (Exception e) {
                    sendFailed = true;
                    throw e;
                }

                // 关闭任何无效的socket
                if (!key.isValid())
                    close(channel, CloseMode.GRACEFUL);

            } catch (Exception e) {
                // 构造异常描述信息
                String desc = String.format("%s (channelId=%s)", channel.socketDescription(), channel.id());
                if (e instanceof IOException) {
                    // 处理IO异常，通常是连接断开
                    log.debug("Connection with {} disconnected", desc, e);
                } else if (e instanceof AuthenticationException) {
                    // 处理认证异常
                    boolean isReauthentication = channel.successfulAuthentications() > 0;
                    if (isReauthentication)
                        sensors.failedReauthentication.record();
                    else
                        sensors.failedAuthentication.record();
                    String exceptionMessage = e.getMessage();
                    if (e instanceof DelayedResponseAuthenticationException)
                        exceptionMessage = e.getCause().getMessage();
                    log.info("Failed {}authentication with {} ({})", isReauthentication ? "re-" : "",
                        desc, exceptionMessage);
                } else {
                    // 处理其他未预期的异常
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                }

                // 根据异常类型选择关闭方式
                if (e instanceof DelayedResponseAuthenticationException)
                    maybeDelayCloseOnAuthenticationFailure(channel);
                else
                    close(channel, sendFailed ? CloseMode.NOTIFY_ONLY : CloseMode.GRACEFUL);
            } finally {
                // 记录每个连接的处理时间（如果启用）
                maybeRecordTimePerConnection(channel, channelStartTimeNanos);
            }
        }
    }

    /**
     * 尝试向通道写入数据
     * 
     * 该方法在以下条件都满足时执行写入操作：
     * 1. 通道有待发送的数据(hasSend)
     * 2. 通道处于就绪状态(ready)
     * 3. SelectionKey可写(isWritable)
     * 4. 不需要开始客户端重新认证
     *
     * @param key 与通道关联的SelectionKey
     * @param channel 要写入的KafkaChannel
     * @param nowNanos 当前时间的纳秒值
     * @throws IOException 如果写入过程中发生I/O错误
     */
    private void attemptWrite(SelectionKey key, KafkaChannel channel, long nowNanos) throws IOException {
        if (channel.hasSend()
                && channel.ready()
                && key.isWritable()
                && !channel.maybeBeginClientReauthentication(() -> nowNanos)) {
            write(channel);
        }
    }

    /**
     * 执行实际的写入操作
     * 
     * 该方法负责：
     * 1. 将数据写入通道
     * 2. 检查是否完成发送
     * 3. 更新相关的度量指标
     * 
     * 注意：即使bytesSent < 1，如果TransportLayer有待处理的写入操作且已写入到socket通道缓冲区，
     * 也可能完成发送操作。
     *
     * @param channel 要写入的KafkaChannel
     * @throws IOException 如果写入过程中发生I/O错误
     */
    void write(KafkaChannel channel) throws IOException {
        String nodeId = channel.id();
        // 执行实际的写入操作，返回写入的字节数
        long bytesSent = channel.write();
        // 检查是否有完成的发送操作
        NetworkSend send = channel.maybeCompleteSend();
        
        if (bytesSent > 0 || send != null) {
            long currentTimeMs = time.milliseconds();
            // 记录已发送的字节数
            if (bytesSent > 0)
                this.sensors.recordBytesSent(nodeId, bytesSent, currentTimeMs);
            // 处理完成的发送操作
            if (send != null) {
                this.completedSends.add(send);
                this.sensors.recordCompletedSend(nodeId, send.size(), currentTimeMs);
            }
        }
    }

    /**
     * 确定SelectionKey的处理顺序
     * 
     * 为了防止在内存不足时读取操作出现饥饿现象（因为selectionKeys的迭代顺序可能每次都相同），
     * 当可用内存低于阈值时，会对keys进行随机打乱。
     *
     * @param selectionKeys 要处理的SelectionKey集合
     * @return 确定处理顺序后的SelectionKey集合
     */
    private Collection<SelectionKey> determineHandlingOrder(Set<SelectionKey> selectionKeys) {
        if (!outOfMemory && memoryPool.availableMemory() < lowMemThreshold) {
            List<SelectionKey> shuffledKeys = new ArrayList<>(selectionKeys);
            Collections.shuffle(shuffledKeys);
            return shuffledKeys;
        } else {
            return selectionKeys;
        }
    }

    /**
     * 尝试从通道读取数据
     * 
     * 该方法负责：
     * 1. 从通道读取数据
     * 2. 更新读取进度标记
     * 3. 处理完成的接收操作
     * 4. 处理内存压力导致的通道静音
     *
     * @param channel 要读取的KafkaChannel
     * @throws IOException 如果读取过程中发生I/O错误
     */
    private void attemptRead(KafkaChannel channel) throws IOException {
        String nodeId = channel.id();

        // 执行实际的读取操作，返回读取的字节数
        long bytesReceived = channel.read();
        if (bytesReceived != 0) {
            long currentTimeMs = time.milliseconds();
            // 记录接收到的字节数
            sensors.recordBytesReceived(nodeId, bytesReceived, currentTimeMs);
            madeReadProgressLastPoll = true;

            // 检查是否有完成的接收操作
            NetworkReceive receive = channel.maybeCompleteReceive();
            if (receive != null) {
                addToCompletedReceives(channel, receive, currentTimeMs);
            }
        }
        // 处理通道静音状态
        if (channel.isMuted()) {
            outOfMemory = true; // 通道因内存压力而静音
        } else {
            madeReadProgressLastPoll = true;
        }
    }

    /**
     * 尝试从正在关闭的通道读取数据
     * 
     * 该方法在通道关闭前尝试读取所有待处理的数据。它会检查：
     * 1. 通道是否处于就绪状态
     * 2. 通道是否被显式静音或已有完成的接收操作
     * 3. 是否可以继续读取数据
     *
     * @param channel 正在关闭的KafkaChannel
     * @return 如果通道有待处理的数据返回true，否则返回false
     */
    private boolean maybeReadFromClosingChannel(KafkaChannel channel) {
        boolean hasPending;
        if (channel.state().state() != ChannelState.State.READY)
            hasPending = false;
        else if (explicitlyMutedChannels.contains(channel) || hasCompletedReceive(channel))
            hasPending = true;
        else {
            try {
                attemptRead(channel);
                hasPending = hasCompletedReceive(channel);
            } catch (Exception e) {
                log.trace("Read from closing channel failed, ignoring exception", e);
                hasPending = false;
            }
        }
        return hasPending;
    }

    /**
     * 记录每个连接在pollSelectionKeys中花费的时间
     * 
     * @param channel 要记录时间的KafkaChannel
     * @param startTimeNanos 开始时间（纳秒）
     */
    private void maybeRecordTimePerConnection(KafkaChannel channel, long startTimeNanos) {
        if (recordTimePerConnection)
            channel.addNetworkThreadTimeNanos(time.nanoseconds() - startTimeNanos);
    }

    /**
     * 获取已完成的发送操作列表
     * 
     * @return 包含所有已完成NetworkSend的列表
     */
    @Override
    public List<NetworkSend> completedSends() {
        return this.completedSends;
    }

    /**
     * 获取已完成的接收操作集合
     * 
     * @return 包含所有已完成NetworkReceive的集合
     */
    @Override
    public Collection<NetworkReceive> completedReceives() {
        return this.completedReceives.values();
    }

    /**
     * 获取已断开连接的通道状态映射
     * 
     * @return 连接ID到ChannelState的映射
     */
    @Override
    public Map<String, ChannelState> disconnected() {
        return this.disconnected;
    }

    /**
     * 获取已成功连接的连接ID列表
     * 
     * @return 包含所有已连接ID的列表
     */
    @Override
    public List<String> connected() {
        return this.connected;
    }

    /**
     * 将指定ID的通道设置为静音状态
     * 
     * @param id 要静音的通道ID
     */
    @Override
    public void mute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        mute(channel);
    }

    /**
     * 将指定的Kafka通道设置为静音状态
     * 
     * 静音状态下的通道将不会接收新的数据，但已经缓冲的数据仍然可以被处理。
     * 这个方法通常用于流量控制或者在处理积压数据时暂时停止接收新数据。
     *
     * @param channel 要静音的Kafka通道
     */
    private void mute(KafkaChannel channel) {
        // 将通道设置为静音状态
        channel.mute();
        // 将通道添加到显式静音的通道集合中
        explicitlyMutedChannels.add(channel);
        // 从具有缓冲读取数据的SelectionKey集合中移除该通道的key
        keysWithBufferedRead.remove(channel.selectionKey());
    }

    /**
     * 取消指定ID的通道的静音状态
     * 
     * 此方法是{@link Selectable}接口的实现，用于恢复通道的正常数据接收。
     * 如果指定ID的通道不存在或已关闭，将抛出异常。
     *
     * @param id 要取消静音的通道ID
     */
    @Override
    public void unmute(String id) {
        // 获取开放或正在关闭的通道，如果不存在则抛出异常
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        unmute(channel);
    }

    /**
     * 尝试取消指定Kafka通道的静音状态
     * 
     * 此方法会检查通道是否可以被取消静音，如果可以，则恢复其数据接收能力。
     * 如果通道有未处理的缓冲数据，会将其标记为可读取状态。
     *
     * @param channel 要取消静音的Kafka通道
     */
    private void unmute(KafkaChannel channel) {
        // 只有当通道实际被取消静音时，才从显式静音集合中移除
        if (channel.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel);
            // 如果通道有缓冲的数据，将其key添加到待读取集合中
            if (channel.hasBytesBuffered()) {
                keysWithBufferedRead.add(channel.selectionKey());
                madeReadProgressLastPoll = true;
            }
        }
    }

    /**
     * 将所有活动的通道设置为静音状态
     * 
     * 此方法是{@link Selectable}接口的实现，用于批量静音所有通道。
     * 通常在需要暂停所有数据接收时使用，例如系统维护或资源受限时。
     */
    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    /**
     * 取消所有通道的静音状态
     * 
     * 此方法是{@link Selectable}接口的实现，用于批量恢复所有通道的数据接收。
     * 通常在系统恢复正常运行时使用。
     */
    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    /**
     * 完成延迟关闭的通道处理
     * 
     * 此方法用于处理因身份验证失败而延迟关闭的通道。它会检查每个延迟关闭的通道是否已到达关闭时间，
     * 如果是则关闭该通道。这种延迟关闭机制可以防止客户端立即重试失败的连接。
     *
     * @param currentTimeNanos 当前时间（纳秒）
     */
    // package-private for testing
    void completeDelayedChannelClose(long currentTimeNanos) {
        if (delayedClosingChannels == null)
            return;

        while (!delayedClosingChannels.isEmpty()) {
            // 获取并尝试关闭最早的延迟关闭通道
            DelayedAuthenticationFailureClose delayedClose = delayedClosingChannels.values().iterator().next();
            if (!delayedClose.tryClose(currentTimeNanos))
                break;
        }
    }

    /**
     * 检查并关闭最旧的空闲连接
     * 
     * 此方法用于管理连接的生命周期，防止空闲连接占用系统资源。
     * 它会检查是否有超过空闲时间限制的连接，如果有则优雅地关闭它们。
     *
     * @param currentTimeNanos 当前时间（纳秒）
     */
    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        // 获取最早过期的连接
        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();
            KafkaChannel channel = this.channels.get(connectionId);
            if (channel != null) {
                if (log.isTraceEnabled())
                    log.trace("About to close the idle connection from {} due to being idle for {} millis",
                            connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);
                // 将通道状态设置为过期
                channel.state(ChannelState.EXPIRED);
                // 优雅地关闭通道
                close(channel, CloseMode.GRACEFUL);
            }
        }
    }

    /**
     * 清除已完成的接收操作
     * 
     * 此方法由SocketServer使用，用于在处理完已接收的数据后立即释放接收缓冲区的引用，
     * 而不是等待下一次poll()调用。这有助于及时释放内存资源。
     */
    public void clearCompletedReceives() {
        this.completedReceives.clear();
    }

    /**
     * 清除已完成的发送操作
     * 
     * 此方法由SocketServer使用，用于在处理完已发送的数据后立即释放发送缓冲区的引用，
     * 而不是等待下一次poll()调用。这有助于及时释放内存资源。
     */
    public void clearCompletedSends() {
        this.completedSends.clear();
    }

    /**
     * 清除上一次poll操作的所有结果
     * 
     * 此方法在每次poll()调用开始时由Selector调用，用于清理上一次poll的所有结果。
     * 它会清除已完成的发送和接收操作、连接状态，并处理正在关闭的通道。
     * 
     * SocketServer通过调用{@link #clearCompletedSends()}和{@link #clearCompletedReceives()}
     * 来及时清理已处理的缓冲区，避免长时间持有多个连接的大量请求/响应缓冲区。
     * 而客户端则依赖Selector在每次poll()开始时调用{@link #clear()}，因为内存使用不那么关键，
     * 且每次poll清理一次可以在下一次poll之前以任意顺序处理这些结果。
     */
    private void clear() {
        // 清除所有完成的操作记录
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();

        // 处理正在关闭的通道：在所有缓冲的接收都被处理完或者有发送请求失败时移除通道
        for (Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator(); it.hasNext(); ) {
            KafkaChannel channel = it.next().getValue();
            // 检查是否有发送失败
            boolean sendFailed = failedSends.remove(channel.id());
            boolean hasPending = false;
            // 如果没有发送失败，尝试读取剩余的数据
            if (!sendFailed)
                hasPending = maybeReadFromClosingChannel(channel);
            // 如果没有待处理的数据，完成通道的关闭
            if (!hasPending) {
                doClose(channel, true);
                it.remove();
            }
        }

        // 将所有发送失败的通道标记为断开连接状态
        for (String channel : this.failedSends)
            this.disconnected.put(channel, ChannelState.FAILED_SEND);
        this.failedSends.clear();
        // 重置读取进度标记
        this.madeReadProgressLastPoll = false;
    }

    /**
     * 检查是否有数据可用，最多等待指定的超时时间
     * 
     * 该方法是NIO选择器的核心操作之一，用于检测通道上是否有I/O事件发生。它支持两种模式：
     * 1. 非阻塞模式 (timeoutMs = 0)：立即返回当前就绪的通道数
     * 2. 阻塞模式 (timeoutMs > 0)：等待指定时间直到有通道就绪
     *
     * @param timeoutMs 等待时间，以毫秒为单位，必须是非负数
     * @return 就绪的通道数量
     */
    private int select(long timeoutMs) throws IOException {
        // 检查超时参数的有效性
        if (timeoutMs < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        // 根据超时时间选择调用方式：
        // - selectNow()：非阻塞，立即返回
        // - select(timeout)：最多阻塞指定时间
        if (timeoutMs == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(timeoutMs);
    }

    /**
     * 关闭指定ID的连接
     * 
     * 这是一个公共方法，用于主动关闭指定的连接。它会根据连接的当前状态选择合适的关闭方式：
     * 1. 如果连接仍然活跃，将其标记为本地关闭并静默丢弃
     * 2. 如果连接已在关闭过程中，直接完成关闭操作
     * 
     * @param id 要关闭的连接ID
     */
    public void close(String id) {
        // 尝试从活跃连接映射中获取通道
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            // 对于本地主动关闭，虽然不需要断开连接通知，但仍更新通道状态以避免混淆
            channel.state(ChannelState.LOCAL_CLOSE);
            // 使用DISCARD_NO_NOTIFY模式关闭，静默丢弃所有未完成的接收
            close(channel, CloseMode.DISCARD_NO_NOTIFY);
        } else {
            // 检查是否是正在关闭过程中的通道
            KafkaChannel closingChannel = this.closingChannels.remove(id);
            // 如果找到了正在关闭的通道，直接完成关闭操作，保持触发关闭时的状态
            if (closingChannel != null)
                doClose(closingChannel, false);
        }
    }

    /**
     * 处理身份验证失败时的延迟关闭
     * 
     * 当连接的身份验证失败时，可能需要延迟关闭连接以防止立即重试导致的资源浪费。
     * 这个方法会创建一个延迟关闭处理器，根据配置决定是立即关闭还是延迟关闭。
     * 
     * @param channel 需要关闭的通道
     */
    private void maybeDelayCloseOnAuthenticationFailure(KafkaChannel channel) {
        // 创建延迟关闭处理器，包含通道和配置的延迟时间
        DelayedAuthenticationFailureClose delayedClose = new DelayedAuthenticationFailureClose(channel, failedAuthenticationDelayMs);
        if (delayedClosingChannels != null)
            // 如果启用了延迟关闭功能，将处理器添加到延迟关闭映射
            delayedClosingChannels.put(channel.id(), delayedClose);
        else
            // 如果未启用延迟关闭，立即关闭连接
            delayedClose.closeNow();
    }

    /**
     * 处理身份验证失败导致的连接关闭
     * 
     * 这个方法负责完成身份验证失败后的连接关闭流程，包括：
     * 1. 执行通道的身份验证失败关闭操作
     * 2. 记录任何发生的错误
     * 3. 使用优雅关闭模式关闭连接
     * 
     * @param channel 需要关闭的通道
     */
    private void handleCloseOnAuthenticationFailure(KafkaChannel channel) {
        try {
            // 完成通道的身份验证失败关闭操作
            channel.completeCloseOnAuthenticationFailure();
        } catch (Exception e) {
            // 记录关闭过程中的任何错误
            log.error("Exception handling close on authentication failure node {}", channel.id(), e);
        } finally {
            // 无论是否发生异常，都确保使用优雅关闭模式关闭连接
            close(channel, CloseMode.GRACEFUL);
        }
    }

    /**
     * 开始关闭连接的核心方法
     * 
     * 这个方法实现了Kafka的连接关闭策略，支持两种关闭模式：
     * 1. 优雅关闭(GRACEFUL)：先断开连接，但保留未处理的接收请求
     * 2. 立即关闭(其他模式)：立即关闭连接，丢弃所有未处理的接收请求
     * 
     * 设计考虑：
     * - 优雅关闭模式确保了消息的可靠性，适用于正常的业务关闭场景
     * - 立即关闭模式用于错误处理或紧急情况，优先考虑快速释放资源
     * 
     * @param channel 要关闭的通道
     * @param closeMode 关闭模式，决定如何处理未完成的请求和是否发送断开连接通知
     */
    private void close(KafkaChannel channel, CloseMode closeMode) {
        // 断开通道的底层连接
        channel.disconnect();

        // 确保已关闭的通道不会出现在已连接列表中
        // 这种情况可能发生在finishConnect成功后prepare抛出异常的情况
        connected.remove(channel.id());

        // 处理优雅关闭模式：如果有未处理的接收请求，保持通道在关闭列表中
        // 这确保了即使在连接关闭过程中，也能处理所有已接收的数据
        // 例如：当producer使用acks=0发送记录并关闭连接时，broker的单次poll可能同时接收到记录和关闭请求
        if (closeMode == CloseMode.GRACEFUL && maybeReadFromClosingChannel(channel)) {
            // 将通道添加到关闭列表，继续处理未完成的请求
            closingChannels.put(channel.id(), channel);
            log.debug("Tracking closing connection {} to process outstanding requests", channel.id());
        } else {
            // 对于非优雅关闭或没有未处理请求的情况，直接执行关闭操作
            doClose(channel, closeMode.notifyDisconnect);
        }
        
        // 从活跃通道映射中移除
        this.channels.remove(channel.id());

        // 清理相关资源
        if (delayedClosingChannels != null)
            delayedClosingChannels.remove(channel.id());

        if (idleExpiryManager != null)
            idleExpiryManager.remove(channel.id());
    }

    /**
     * 执行实际的连接关闭操作
     * 
     * 这个方法负责连接关闭的底层操作，包括：
     * 1. 清理选择器相关的资源
     * 2. 关闭底层通道
     * 3. 更新度量指标
     * 4. 处理断开连接通知
     * 
     * @param channel 要关闭的通道
     * @param notifyDisconnect 是否需要通知连接断开
     */
    private void doClose(KafkaChannel channel, boolean notifyDisconnect) {
        // 获取通道关联的SelectionKey
        SelectionKey key = channel.selectionKey();
        try {
            // 清理选择器相关的资源
            immediatelyConnectedKeys.remove(key);
            keysWithBufferedRead.remove(key);
            // 关闭通道
            channel.close();
        } catch (IOException e) {
            // 记录关闭过程中的任何IO异常
            log.error("Exception closing connection to node {}:", channel.id(), e);
        } finally {
            // 确保SelectionKey被取消并清理附加对象
            key.cancel();
            key.attach(null);
        }

        // 更新连接关闭的度量指标
        this.sensors.connectionClosed.record();
        // 从静音通道集合中移除
        this.explicitlyMutedChannels.remove(channel);
        // 如果需要通知断开连接，将通道状态添加到断开连接映射
        if (notifyDisconnect)
            this.disconnected.put(channel.id(), channel.state());
    }

    /**
     * 检查指定ID的通道是否就绪
     * 
     * 通道就绪意味着它已经完成了所有必要的初始化（如SSL握手）并可以进行数据传输。
     * 此方法用于在发送数据前验证通道状态。
     *
     * @param id 要检查的通道ID
     * @return 如果通道存在且就绪返回true，否则返回false
     */
    @Override
    public boolean isChannelReady(String id) {
        // 从活动通道映射中获取指定ID的通道
        KafkaChannel channel = this.channels.get(id);
        // 检查通道是否存在且处于就绪状态
        return channel != null && channel.ready();
    }

    /**
     * 获取打开或正在关闭的通道，如果不存在则抛出异常
     * 
     * 此方法首先尝试从活动通道中获取，如果不存在则从正在关闭的通道中获取。
     * 如果两者都不存在，则抛出异常。这是一个内部辅助方法，用于确保在进行通道操作时
     * 通道一定存在。
     *
     * @param id 要获取的通道ID
     * @return 找到的KafkaChannel实例
     * @throws IllegalStateException 如果通道不存在
     */
    private KafkaChannel openOrClosingChannelOrFail(String id) {
        // 首先尝试从活动通道中获取
        KafkaChannel channel = this.channels.get(id);
        // 如果不存在，则尝试从正在关闭的通道中获取
        if (channel == null)
            channel = this.closingChannels.get(id);
        // 如果仍然不存在，抛出异常
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no connection. Connection id " + id + " existing connections " + channels.keySet());
        return channel;
    }

    /**
     * 获取所有活动的选择器通道
     * 
     * 返回当前所有活动的KafkaChannel的列表副本。这个方法通常用于监控和调试目的，
     * 允许外部代码安全地遍历所有活动通道而不影响内部状态。
     *
     * @return 包含所有活动通道的列表副本
     */
    public List<KafkaChannel> channels() {
        // 创建并返回活动通道集合的副本，确保线程安全
        return new ArrayList<>(channels.values());
    }

    /**
     * 获取指定ID的通道
     * 
     * 此方法用于获取与特定连接ID关联的通道。如果通道不存在，返回null。
     * 这是一个直接的查找方法，不会抛出异常。
     *
     * @param id 要获取的通道ID
     * @return 找到的KafkaChannel实例，如果不存在则返回null
     */
    public KafkaChannel channel(String id) {
        // 直接从活动通道映射中获取指定ID的通道
        return this.channels.get(id);
    }

    /**
     * 获取指定ID的正在关闭的通道
     * 
     * 此方法用于获取已断开连接但尚未完全关闭的通道，这种情况通常发生在
     * 通道还有未处理的消息需要处理时。这允许在完全关闭前完成必要的清理工作。
     *
     * @param id 要获取的通道ID
     * @return 找到的正在关闭的KafkaChannel实例，如果不存在则返回null
     */
    public KafkaChannel closingChannel(String id) {
        // 从正在关闭的通道映射中获取指定ID的通道
        return closingChannels.get(id);
    }

    /**
     * 获取优先级最低的通道
     * 
     * 此方法用于在需要关闭某个通道以容纳新连接时，选择最适合关闭的通道。
     * 选择过程遵循以下优先级顺序：
     * 1. 优先选择已经处于关闭状态的通道
     * 2. 如果启用了空闲过期管理器，选择最近最少使用的通道
     * 3. 如果以上都不适用，选择任意一个活动通道
     *
     * 此方法主要用于在启用了broker级别的最大连接数限制时，
     * 为broker间监听器上的新通道腾出空间。
     *
     * @return 优先级最低的KafkaChannel实例，如果没有可用通道则返回null
     */
    public KafkaChannel lowestPriorityChannel() {
        KafkaChannel channel = null;
        // 首先检查是否有正在关闭的通道
        if (!closingChannels.isEmpty()) {
            channel = closingChannels.values().iterator().next();
        }
        // 其次检查空闲管理器中的最近最少使用通道
        else if (idleExpiryManager != null && !idleExpiryManager.lruConnections.isEmpty()) {
            String channelId = idleExpiryManager.lruConnections.keySet().iterator().next();
            channel = channel(channelId);
        }
        // 最后选择任意一个活动通道
        else if (!channels.isEmpty()) {
            channel = channels.values().iterator().next();
        }
        return channel;
    }

    /**
     * 获取与SelectionKey关联的通道
     * 
     * 此方法从SelectionKey的附件中获取关联的KafkaChannel实例。
     * 在NIO操作中，每个SelectionKey都可以附加一个对象，这里我们使用它来存储对应的KafkaChannel。
     *
     * @param key SelectionKey实例
     * @return 与该key关联的KafkaChannel实例
     */
    private KafkaChannel channel(SelectionKey key) {
        // 从SelectionKey的附件中获取KafkaChannel实例
        return (KafkaChannel) key.attachment();
    }

    /**
     * 检查指定通道是否有已完成的接收操作
     * 
     * 此方法用于检查给定通道是否已经有完成的接收操作在等待处理。
     * 这有助于防止重复处理或在处理完成前添加新的接收操作。
     *
     * @param channel 要检查的KafkaChannel实例
     * @return 如果通道有已完成的接收操作返回true，否则返回false
     */
    private boolean hasCompletedReceive(KafkaChannel channel) {
        // 检查完成接收映射中是否包含该通道的ID
        return completedReceives.containsKey(channel.id());
    }

    /**
     * 将接收操作添加到已完成接收列表
     * 
     * 此方法用于记录已完成的网络接收操作。它会：
     * 1. 确保同一通道不会有多个完成的接收操作
     * 2. 将接收操作添加到完成列表
     * 3. 更新相关的度量指标
     *
     * @param channel 完成接收操作的通道
     * @param networkReceive 完成的网络接收操作
     * @param currentTimeMs 当前时间戳（毫秒）
     * @throws IllegalStateException 如果通道已有完成的接收操作
     */
    private void addToCompletedReceives(KafkaChannel channel, NetworkReceive networkReceive, long currentTimeMs) {
        // 检查是否已存在完成的接收操作
        if (hasCompletedReceive(channel))
            throw new IllegalStateException("Attempting to add second completed receive to channel " + channel.id());

        // 将接收操作添加到完成映射
        this.completedReceives.put(channel.id(), networkReceive);
        // 记录完成接收的度量指标
        sensors.recordCompletedReceive(channel.id(), networkReceive.size(), currentTimeMs);
    }

    /**
     * 获取所有SelectionKey集合（仅用于测试）
     * 
     * 此方法返回当前NIO选择器中所有注册的SelectionKey的副本。
     * 这个方法主要用于测试目的，不应在生产代码中使用。
     *
     * @return 包含所有SelectionKey的Set副本
     */
    public Set<SelectionKey> keys() {
        // 创建并返回NIO选择器中所有key的副本
        return new HashSet<>(nioSelector.keys());
    }


    /**
     * 选择器通道元数据注册表
     * 
     * 此内部类实现了ChannelMetadataRegistry接口，用于管理通道的加密和客户端信息。
     * 它维护每个通道的加密算法信息和客户端信息，并负责更新相关的度量指标。
     * 这对于监控和调试SSL/TLS连接以及跟踪客户端连接特别重要。
     */
    class SelectorChannelMetadataRegistry implements ChannelMetadataRegistry {
        // 存储通道使用的加密信息
        private CipherInformation cipherInformation;
        // 存储通道关联的客户端信息
        private ClientInformation clientInformation;

        /**
         * 注册加密信息
         * 
         * 此方法用于更新通道的加密信息，同时维护加密算法使用统计。
         * 如果已存在加密信息，会先减少旧加密算法的计数，再增加新加密算法的计数。
         * 
         * @param cipherInformation 要注册的新加密信息
         */
        @Override
        public void registerCipherInformation(final CipherInformation cipherInformation) {
            // 如果已有加密信息，需要先处理旧的信息
            if (this.cipherInformation != null) {
                // 如果新旧加密信息相同，无需更新
                if (this.cipherInformation.equals(cipherInformation))
                    return;
                // 减少旧加密算法的使用计数
                sensors.connectionsByCipher.decrement(this.cipherInformation);
            }

            // 更新加密信息并增加新加密算法的使用计数
            this.cipherInformation = cipherInformation;
            sensors.connectionsByCipher.increment(cipherInformation);
        }

        /**
         * 获取当前的加密信息
         * 
         * @return 当前注册的加密信息，如果未设置则返回null
         */
        @Override
        public CipherInformation cipherInformation() {
            return cipherInformation;
        }

        /**
         * 注册客户端信息
         * 
         * 此方法用于更新通道的客户端信息，同时维护客户端连接统计。
         * 如果已存在客户端信息，会先减少旧客户端的计数，再增加新客户端的计数。
         * 
         * @param clientInformation 要注册的新客户端信息
         */
        @Override
        public void registerClientInformation(final ClientInformation clientInformation) {
            // 如果已有客户端信息，需要先处理旧的信息
            if (this.clientInformation != null) {
                // 如果新旧客户端信息相同，无需更新
                if (this.clientInformation.equals(clientInformation))
                    return;
                // 减少旧客户端的连接计数
                sensors.connectionsByClient.decrement(this.clientInformation);
            }

            // 更新客户端信息并增加新客户端的连接计数
            this.clientInformation = clientInformation;
            sensors.connectionsByClient.increment(clientInformation);
        }

        /**
         * 获取当前的客户端信息
         * 
         * @return 当前注册的客户端信息，如果未设置则返回null
         */
        @Override
        public ClientInformation clientInformation() {
            return clientInformation;
        }

        /**
         * 关闭注册表
         * 
         * 此方法在通道关闭时调用，负责清理所有注册的信息并更新相关统计数据。
         * 它会减少加密算法和客户端连接的计数，并清空所有存储的信息。
         */
        @Override
        public void close() {
            // 清理加密信息并更新统计
            if (this.cipherInformation != null) {
                sensors.connectionsByCipher.decrement(this.cipherInformation);
                this.cipherInformation = null;
            }

            // 清理客户端信息并更新统计
            if (this.clientInformation != null) {
                sensors.connectionsByClient.decrement(this.clientInformation);
                this.clientInformation = null;
            }
        }
    }

    class SelectorMetrics implements AutoCloseable {
        private final Metrics metrics;
        private final Map<String, String> metricTags;
        private final boolean metricsPerConnection;
        private final String metricGrpName;
        private final String perConnectionMetricGrpName;

        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor successfulAuthentication;
        public final Sensor successfulReauthentication;
        public final Sensor successfulAuthenticationNoReauth;
        public final Sensor reauthenticationLatency;
        public final Sensor failedAuthentication;
        public final Sensor failedReauthentication;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor requestsSent;
        public final Sensor bytesReceived;
        public final Sensor responsesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;
        public final IntGaugeSuite<CipherInformation> connectionsByCipher;
        public final IntGaugeSuite<ClientInformation> connectionsByClient;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection) {
            this.metrics = metrics;
            this.metricTags = metricTags;
            this.metricsPerConnection = metricsPerConnection;
            this.metricGrpName = metricGrpPrefix + "-metrics";
            this.perConnectionMetricGrpName = metricGrpPrefix + "-node-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix);
            this.connectionClosed.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-close", "connections closed"));

            this.connectionCreated = sensor("connections-created:" + tagsSuffix);
            this.connectionCreated.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-creation", "new connections established"));

            this.successfulAuthentication = sensor("successful-authentication:" + tagsSuffix);
            this.successfulAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-authentication", "connections with successful authentication"));

            this.successfulReauthentication = sensor("successful-reauthentication:" + tagsSuffix);
            this.successfulReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-reauthentication", "successful re-authentication of connections"));

            this.successfulAuthenticationNoReauth = sensor("successful-authentication-no-reauth:" + tagsSuffix);
            MetricName successfulAuthenticationNoReauthMetricName = metrics.metricName(
                    "successful-authentication-no-reauth-total", metricGrpName,
                    "The total number of connections with successful authentication where the client does not support re-authentication",
                    metricTags);
            this.successfulAuthenticationNoReauth.add(successfulAuthenticationNoReauthMetricName, new CumulativeSum());

            this.failedAuthentication = sensor("failed-authentication:" + tagsSuffix);
            this.failedAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-authentication", "connections with failed authentication"));

            this.failedReauthentication = sensor("failed-reauthentication:" + tagsSuffix);
            this.failedReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-reauthentication", "failed re-authentication of connections"));

            this.reauthenticationLatency = sensor("reauthentication-latency:" + tagsSuffix);
            MetricName reauthenticationLatencyMaxMetricName = metrics.metricName("reauthentication-latency-max",
                    metricGrpName, "The max latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyMaxMetricName, new Max());
            MetricName reauthenticationLatencyAvgMetricName = metrics.metricName("reauthentication-latency-avg",
                    metricGrpName, "The average latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyAvgMetricName, new Avg());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix);
            bytesTransferred.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "network-io", "network operations (reads or writes) on all connections"));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix, bytesTransferred);
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags,
                    "outgoing-byte", "outgoing bytes sent to all servers"));

            this.requestsSent = sensor("requests-sent:" + tagsSuffix);
            this.requestsSent.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "request", "requests sent"));
            MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", metricTags);
            this.requestsSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", metricTags);
            this.requestsSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix, bytesTransferred);
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    "incoming-byte", "bytes read off all sockets"));

            this.responsesReceived = sensor("responses-received:" + tagsSuffix);
            this.responsesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "response", "responses received"));

            this.selectTime = sensor("select-time:" + tagsSuffix);
            this.selectTime.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "select", "times the I/O layer checked for new I/O to perform"));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            this.selectTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io-wait", "waiting"));

            this.ioTime = sensor("io-time:" + tagsSuffix);
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            this.ioTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io", "doing I/O"));

            this.connectionsByCipher = new IntGaugeSuite<>(log, "sslCiphers", metrics,
                cipherInformation -> {
                    Map<String, String> tags = new LinkedHashMap<>();
                    tags.put("cipher", cipherInformation.cipher());
                    tags.put("protocol", cipherInformation.protocol());
                    tags.putAll(metricTags);
                    return metrics.metricName("connections", metricGrpName, "The number of connections with this SSL cipher and protocol.", tags);
                }, 100);

            this.connectionsByClient = new IntGaugeSuite<>(log, "clients", metrics,
                clientInformation -> {
                    Map<String, String> tags = new LinkedHashMap<>();
                    tags.put("clientSoftwareName", clientInformation.softwareName());
                    tags.put("clientSoftwareVersion", clientInformation.softwareVersion());
                    tags.putAll(metricTags);
                    return metrics.metricName("connections", metricGrpName, "The number of connections with this client and version.", tags);
                }, 100);

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, (config, now) -> channels.size());
        }

        private Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
                SampledStat stat, String baseName, String descriptiveName) {
            MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
                            String.format("The number of %s per second", descriptiveName), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
                            String.format("The total number of %s", descriptiveName), metricTags);
            if (stat == null)
                return new Meter(rateMetricName, totalMetricName);
            else
                return new Meter(stat, rateMetricName, totalMetricName);
        }

        private Meter createMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String descriptiveName) {
            return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName);
        }

        private Meter createIOThreadRatioMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                                               String baseName, String action) {
            MetricName rateMetricName = metrics.metricName(baseName + "-ratio", groupName,
                String.format("The fraction of time the I/O thread spent %s", action), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-time-ns-total", groupName,
                String.format("The total time the I/O thread spent %s", action), metricTags);
            return new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName);
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    nodeRequest.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "request", "requests sent"));
                    MetricName metricName = metrics.metricName("request-size-avg", perConnectionMetricGrpName, "The average size of requests sent.", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", perConnectionMetricGrpName, "The maximum size of any request sent.", tags);
                    nodeRequest.add(metricName, new Max());

                    String bytesSentName = "node-" + connectionId + ".bytes-sent";
                    Sensor bytesSent = sensor(bytesSentName);
                    bytesSent.add(createMeter(metrics, perConnectionMetricGrpName, tags, "outgoing-byte", "outgoing bytes"));

                    String nodeResponseName = "node-" + connectionId + ".responses-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    nodeResponse.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "response", "responses received"));

                    String bytesReceivedName = "node-" + connectionId + ".bytes-received";
                    Sensor bytesReceive = sensor(bytesReceivedName);
                    bytesReceive.add(createMeter(metrics, perConnectionMetricGrpName, tags, "incoming-byte", "incoming bytes"));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", perConnectionMetricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", perConnectionMetricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes, long currentTimeMs) {
            this.bytesSent.record(bytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String bytesSentName = "node-" + connectionId + ".bytes-sent";
                Sensor bytesSent = this.metrics.getSensor(bytesSentName);
                if (bytesSent != null)
                    bytesSent.record(bytes, currentTimeMs);
            }
        }

        public void recordCompletedSend(String connectionId, long totalBytes, long currentTimeMs) {
            requestsSent.record(totalBytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(totalBytes, currentTimeMs);
            }
        }

        public void recordBytesReceived(String connectionId, long bytes, long currentTimeMs) {
            this.bytesReceived.record(bytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String bytesReceivedName = "node-" + connectionId + ".bytes-received";
                Sensor bytesReceived = this.metrics.getSensor(bytesReceivedName);
                if (bytesReceived != null)
                    bytesReceived.record(bytes, currentTimeMs);
            }
        }

        public void recordCompletedReceive(String connectionId, long totalBytes, long currentTimeMs) {
            responsesReceived.record(totalBytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".responses-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(totalBytes, currentTimeMs);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
            connectionsByCipher.close();
            connectionsByClient.close();
        }
    }

    /**
     * Encapsulate a channel that must be closed after a specific delay has elapsed due to authentication failure.
     */
    private class DelayedAuthenticationFailureClose {
        private final KafkaChannel channel;
        private final long endTimeNanos;
        private boolean closed;

        /**
         * @param channel The channel whose close is being delayed
         * @param delayMs The amount of time by which the operation should be delayed
         */
        public DelayedAuthenticationFailureClose(KafkaChannel channel, int delayMs) {
            this.channel = channel;
            this.endTimeNanos = time.nanoseconds() + (delayMs * 1000L * 1000L);
            this.closed = false;
        }

        /**
         * Try to close this channel if the delay has expired.
         * @param currentTimeNanos The current time
         * @return True if the delay has expired and the channel was closed; false otherwise
         */
        public final boolean tryClose(long currentTimeNanos) {
            if (endTimeNanos <= currentTimeNanos)
                closeNow();
            return closed;
        }

        /**
         * Close the channel now, regardless of whether the delay has expired or not.
         */
        public final void closeNow() {
            if (closed)
                throw new IllegalStateException("Attempt to close a channel that has already been closed");
            handleCloseOnAuthenticationFailure(channel);
            closed = true;
        }
    }

    // helper class for tracking least recently used connections to enable idle connection closing
    private static class IdleExpiryManager {
        private final Map<String, Long> lruConnections;
        private final long connectionsMaxIdleNanos;
        private long nextIdleCloseCheckTime;

        public IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
            // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);
            this.nextIdleCloseCheckTime = time.nanoseconds() + this.connectionsMaxIdleNanos;
        }

        public void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        public Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            if (currentTimeNanos <= nextIdleCloseCheckTime)
                return null;

            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
                return null;
            }

            Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
            Long connectionLastActiveTime = oldestConnectionEntry.getValue();
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;

            if (currentTimeNanos > nextIdleCloseCheckTime)
                return oldestConnectionEntry;
            else
                return null;
        }

        public void remove(String connectionId) {
            lruConnections.remove(connectionId);
        }
    }

    //package-private for testing
    boolean isOutOfMemory() {
        return outOfMemory;
    }

    //package-private for testing
    boolean isMadeReadProgressLastPoll() {
        return madeReadProgressLastPoll;
    }

    // package-private for testing
    Map<?, ?> delayedClosingChannels() {
        return delayedClosingChannels;
    }
}
