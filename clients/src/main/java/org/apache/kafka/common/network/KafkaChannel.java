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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Kafka连接通道，可以存在于客户端（可能是代理间通信场景中的broker）并代表到远程broker的通道，
 * 或者反过来（存在于broker上并代表到远程客户端的通道，该客户端在代理间通信场景中可能是另一个broker）。
 * <p>
 * 每个实例包含以下组件：
 * <ul>
 * <li>唯一ID：在{@code KafkaClient}实例中标识此连接，该ID在客户端建立连接时或服务器端接受连接时生成</li>
 * <li>传输层引用：底层{@link TransportLayer}的引用，用于实现读写操作</li>
 * <li>认证器：{@link Authenticator}执行认证（或重新认证，如果启用该功能且适用于此连接），
 * 直接通过同一个{@link TransportLayer}进行读写操作</li>
 * <li>内存池：{@link MemoryPool}用于读取响应（客户端通常使用JVM堆，broker可以使用较小的池，
 * 也用于测试内存不足场景）</li>
 * <li>网络接收：{@link NetworkReceive}表示当前正在读取的未完成/进行中的请求（从服务器端角度）
 * 或响应（从客户端角度）；可以是尚未读取任何数据的非空值，或者是没有进行中的请求/响应时的空值</li>
 * <li>发送对象：{@link Send}表示当前等待发送或部分发送的请求（从客户端角度）
 * 或响应（从服务器端角度），如果没有则为null</li>
 * <li>通道静音状态：{@link ChannelMuteState}记录通道是否由于内存压力或其他原因被静音</li>
 * </ul>
 */
public class KafkaChannel implements AutoCloseable {
    /**
     * 最小重新认证间隔时间，设置为1秒（以纳秒为单位）
     */
    private static final long MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS = 1000 * 1000 * 1000;

    /**
     * KafkaChannel的静音状态：
     * <ul>
     *   <li> NOT_MUTED: 通道未静音。这是默认状态。 </li>
     *   <li> MUTED: 通道已静音。只有在此状态下才能取消静音。 </li>
     *   <li> MUTED_AND_RESPONSE_PENDING: (仅限SocketServer) 通道已静音，且SocketServer尚未向客户端发送响应
     *                                    (acks != 0)，或正在等待接收来自API层的响应(acks == 0)。 </li>
     *   <li> MUTED_AND_THROTTLED: (仅限SocketServer) 通道已静音，且由于配额违规正在进行限流。 </li>
     *   <li> MUTED_AND_THROTTLED_AND_RESPONSE_PENDING: (仅限SocketServer) 通道已静音，正在进行限流，
     *                                                  且有待处理的响应。 </li>
     * </ul>
     */
    public enum ChannelMuteState {
        /** 未静音状态 */
        NOT_MUTED,
        /** 已静音状态 */
        MUTED,
        /** 已静音且响应待处理状态 */
        MUTED_AND_RESPONSE_PENDING,
        /** 已静音且限流状态 */
        MUTED_AND_THROTTLED,
        /** 已静音、限流且响应待处理状态 */
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    }

    /** 
     * 会改变通道静音状态的Socket服务器事件：
     * <ul>
     *   <li> REQUEST_RECEIVED: 已从客户端接收到请求。 </li>
     *   <li> RESPONSE_SENT: 已向客户端发送响应(ack != 0)或SocketServer已收到来自API层的响应(acks = 0)。 </li>
     *   <li> THROTTLE_STARTED: 由于配额违规开始限流。 </li>
     *   <li> THROTTLE_ENDED: 限流结束。 </li>
     * </ul>
     *
     * 每个事件的有效状态转换：
     * <ul>
     *   <li> REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING </li>
     *   <li> RESPONSE_SENT: MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED </li>
     *   <li> THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING </li>
     *   <li> THROTTLE_ENDED: MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_RESPONSE_PENDING </li>
     * </ul>
     */
    public enum ChannelMuteEvent {
        /** 收到请求事件 */
        REQUEST_RECEIVED,
        /** 响应已发送事件 */
        RESPONSE_SENT,
        /** 限流开始事件 */
        THROTTLE_STARTED,
        /** 限流结束事件 */
        THROTTLE_ENDED
    }

    /** 通道唯一标识符 */
    private final String id;
    /** 底层传输层实现 */
    private final TransportLayer transportLayer;
    /** 认证器创建器 */
    private final Supplier<Authenticator> authenticatorCreator;
    /** 当前使用的认证器 */
    private Authenticator authenticator;
    /** 
     * 累计网络线程时间（纳秒）。
     * 在网络线程上更新，每次发送响应后读取并重置。
     */
    private long networkThreadTimeNanos;
    /** 最大接收大小限制 */
    private final int maxReceiveSize;
    /** 内存池，用于管理响应数据的内存分配 */
    private final MemoryPool memoryPool;
    /** 通道元数据注册表 */
    private final ChannelMetadataRegistry metadataRegistry;
    /** 当前接收的网络数据 */
    private NetworkReceive receive;
    /** 当前待发送的网络数据 */
    private NetworkSend send;
    /** 
     * 跟踪通道的连接和静音状态，以便在通道断开连接后
     * 仍能处理未完成的请求。
     */
    private boolean disconnected;
    /** 通道静音状态 */
    private ChannelMuteState muteState;
    /** 通道当前状态 */
    private ChannelState state;
    /** 远程地址 */
    private SocketAddress remoteAddress;
    /** 成功认证的次数 */
    private int successfulAuthentications;
    /** 是否正在进行写操作 */
    private boolean midWrite;
    /** 上次重新认证开始的时间（纳秒） */
    private long lastReauthenticationStartNanos;

    /**
     * 创建一个新的KafkaChannel实例
     *
     * @param id 通道的唯一标识符，用于在KafkaClient实例中标识此连接
     * @param transportLayer 底层传输层实现，负责实际的网络通信
     * @param authenticatorCreator 认证器创建器，用于创建处理认证的Authenticator实例
     * @param maxReceiveSize 最大接收大小限制，用于防止内存溢出
     * @param memoryPool 内存池，用于管理响应数据的内存分配
     * @param metadataRegistry 通道元数据注册表，用于存储通道相关的元数据信息
     */
    public KafkaChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorCreator,
                        int maxReceiveSize, MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) {
        // 设置通道唯一标识符
        this.id = id;
        // 设置底层传输层实现
        this.transportLayer = transportLayer;
        // 设置认证器创建器
        this.authenticatorCreator = authenticatorCreator;
        // 创建并初始化认证器实例
        this.authenticator = authenticatorCreator.get();
        // 初始化网络线程累计时间为0
        this.networkThreadTimeNanos = 0L;
        // 设置最大接收大小限制
        this.maxReceiveSize = maxReceiveSize;
        // 设置内存池
        this.memoryPool = memoryPool;
        // 设置元数据注册表
        this.metadataRegistry = metadataRegistry;
        // 初始化连接状态为未断开
        this.disconnected = false;
        // 初始化通道静音状态为未静音
        this.muteState = ChannelMuteState.NOT_MUTED;
        // 初始化通道状态为未连接
        this.state = ChannelState.NOT_CONNECTED;
    }

    /**
     * 关闭KafkaChannel，释放所有相关资源
     * 
     * @throws IOException 如果在关闭过程中发生I/O错误
     */
    public void close() throws IOException {
        // 标记通道为已断开连接状态
        this.disconnected = true;
        // 关闭所有相关资源：传输层、认证器、接收缓冲区和元数据注册表
        Utils.closeAll(transportLayer, authenticator, receive, metadataRegistry);
    }

    /**
     * 获取当前通道认证的主体信息
     * 
     * @return 返回认证器中的Kafka主体信息，代表已认证的客户端身份
     */
    public KafkaPrincipal principal() {
        return authenticator.principal();
    }

    /**
     * 获取Kafka主体序列化/反序列化器
     * 
     * @return 返回可选的KafkaPrincipalSerde实例，用于主体信息的序列化和反序列化
     */
    public Optional<KafkaPrincipalSerde> principalSerde() {
        return authenticator.principalSerde();
    }

    /**
     * 执行传输层握手和认证过程
     * 
     * 该方法处理两个主要步骤：
     * 1. 传输层握手：对于启用了客户端认证的SSL连接，通过{@link TransportLayer#handshake()}执行认证
     * 2. SASL认证：通过{@link Authenticator#authenticate()}执行SASL认证
     *
     * @throws AuthenticationException 当认证失败时抛出
     * @throws IOException 当发生I/O错误时抛出
     */
    public void prepare() throws AuthenticationException, IOException {
        // 标记是否正在进行认证过程
        boolean authenticating = false;
        try {
            // 如果传输层未就绪，执行握手
            if (!transportLayer.ready())
                transportLayer.handshake();
            // 如果传输层就绪且认证未完成，执行认证
            if (transportLayer.ready() && !authenticator.complete()) {
                authenticating = true;
                authenticator.authenticate();
            }
        } catch (AuthenticationException e) {
            // 处理认证异常，通知客户端以便终止操作而不重试
            // 其他错误在Selector中作为网络异常处理
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            // 设置通道状态为认证失败
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            if (authenticating) {
                // 如果在认证过程中失败，延迟关闭通道
                delayCloseOnAuthenticationFailure();
                throw new DelayedResponseAuthenticationException(e);
            }
            throw e;
        }
        // 如果通道已就绪，更新认证成功计数和状态
        if (ready()) {
            ++successfulAuthentications;
            state = ChannelState.READY;
        }
    }

    /**
     * 断开通道连接
     * 
     * 该方法执行以下操作：
     * 1. 标记通道为已断开状态
     * 2. 如果有远程地址信息，更新通道状态以包含更多信息
     * 3. 断开底层传输层连接
     */
    public void disconnect() {
        // 标记通道为已断开状态
        disconnected = true;
        // 如果通道未连接且有远程地址信息，更新状态以包含更多信息
        if (state == ChannelState.NOT_CONNECTED && remoteAddress != null) {
            state = new ChannelState(ChannelState.State.NOT_CONNECTED, remoteAddress.toString());
        }
        // 断开底层传输层连接
        transportLayer.disconnect();
    }

    /**
     * 设置通道状态
     * 
     * @param state 新的通道状态
     */
    public void state(ChannelState state) {
        this.state = state;
    }

    /**
     * 获取当前通道状态
     * 
     * @return 当前的通道状态
     */
    public ChannelState state() {
        return this.state;
    }

    /**
     * 完成通道的连接过程
     * 
     * 该方法执行以下操作：
     * 1. 在调用finishConnect()之前获取远程地址，因为如果连接被拒绝，之后将无法访问该地址
     * 2. 尝试完成底层传输层的连接
     * 3. 如果连接成功，根据通道状态设置相应的状态：
     *    - 如果通道已就绪（传输层就绪且认证完成），设置为READY状态
     *    - 如果有远程地址信息但未完成认证，设置为AUTHENTICATE状态并包含远程地址信息
     *    - 否则设置为基本的AUTHENTICATE状态
     *
     * @return 如果连接成功完成则返回true，否则返回false
     * @throws IOException 如果在连接过程中发生I/O错误
     */
    public boolean finishConnect() throws IOException {
        // 在finishConnect()调用前获取远程地址，因为如果连接被拒绝后将无法访问
        SocketChannel socketChannel = transportLayer.socketChannel();
        if (socketChannel != null) {
            remoteAddress = socketChannel.getRemoteAddress();
        }
        // 完成底层传输层的连接
        boolean connected = transportLayer.finishConnect();
        if (connected) {
            if (ready()) {
                // 如果通道已就绪（传输层就绪且认证完成），设置为READY状态
                state = ChannelState.READY;
            } else if (remoteAddress != null) {
                // 如果有远程地址信息但未完成认证，设置为AUTHENTICATE状态并包含远程地址信息
                state = new ChannelState(ChannelState.State.AUTHENTICATE, remoteAddress.toString());
            } else {
                // 设置为基本的AUTHENTICATE状态
                state = ChannelState.AUTHENTICATE;
            }
        }
        return connected;
    }

    /**
     * 检查通道是否已连接
     * 
     * @return 如果底层传输层已连接则返回true，否则返回false
     */
    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    /**
     * 获取通道的唯一标识符
     * 
     * @return 返回在创建通道时指定的唯一标识符
     */
    public String id() {
        return id;
    }

    /**
     * 获取通道关联的选择键
     * 
     * @return 返回底层传输层的SelectionKey，用于NIO操作
     */
    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * 将通道设置为静音状态
     * 
     * 该方法执行以下操作：
     * 1. 检查当前通道是否处于未静音状态
     * 2. 如果是未静音状态且通道未断开连接，则：
     *    - 从底层传输层移除读操作兴趣
     *    - 将通道状态设置为静音
     * 
     * 注意：外部静音操作应通过selector进行，以确保正确的状态处理
     */
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            // 如果通道未断开连接，移除读操作兴趣
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            // 设置通道为静音状态
            muteState = ChannelMuteState.MUTED;
        }
    }

    /**
     * 尝试取消通道的静音状态
     * 
     * 该方法执行以下操作：
     * 1. 检查当前通道是否处于基本静音状态（MUTED）
     * 2. 如果是基本静音状态且通道未断开连接，则：
     *    - 向底层传输层添加读操作兴趣
     *    - 将通道状态设置为未静音
     * 3. 对于其他静音状态（MUTED_AND_*），此操作无效
     *
     * @return 如果调用后通道处于NOT_MUTED状态则返回true，否则返回false
     */
    boolean maybeUnmute() {
        if (muteState == ChannelMuteState.MUTED) {
            // 如果通道未断开连接，添加读操作兴趣
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            // 设置通道为未静音状态
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    /**
     * 处理通道静音相关事件并根据状态机转换静音状态
     * 
     * 该方法根据不同的事件类型执行相应的状态转换：
     * 1. REQUEST_RECEIVED（收到请求）：
     *    - 如果当前是MUTED状态，转换为MUTED_AND_RESPONSE_PENDING
     * 2. RESPONSE_SENT（响应已发送）：
     *    - 如果是MUTED_AND_RESPONSE_PENDING状态，转换为MUTED
     *    - 如果是MUTED_AND_THROTTLED_AND_RESPONSE_PENDING状态，转换为MUTED_AND_THROTTLED
     * 3. THROTTLE_STARTED（开始限流）：
     *    - 如果是MUTED_AND_RESPONSE_PENDING状态，转换为MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
     * 4. THROTTLE_ENDED（结束限流）：
     *    - 如果是MUTED_AND_THROTTLED状态，转换为MUTED
     *    - 如果是MUTED_AND_THROTTLED_AND_RESPONSE_PENDING状态，转换为MUTED_AND_RESPONSE_PENDING
     *
     * @param event 要处理的通道静音相关事件
     * @throws IllegalStateException 如果无法从当前状态转换到新状态
     */
    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                // 处理收到请求事件
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                // 处理响应已发送事件
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                // 处理开始限流事件
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                // 处理结束限流事件
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        // 如果状态未发生变化，说明当前状态无法处理该事件
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    /**
     * 获取当前通道的静音状态
     * 
     * @return 返回当前的ChannelMuteState枚举值
     */
    public ChannelMuteState muteState() {
        return muteState;
    }

    /**
     * 在认证失败时延迟关闭通道
     * 
     * 该方法执行以下操作：
     * 1. 从通道移除所有读写操作，直到调用{@link #completeCloseOnAuthenticationFailure()}完成通道关闭
     * 2. 这样做的目的是确保在完全关闭通道之前，可以正确处理认证失败的情况
     */
    private void delayCloseOnAuthenticationFailure() {
        // 移除写操作兴趣，暂停所有写操作
        transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 完成认证失败后的处理
     * 
     * 该方法在{@link #prepare()}失败时被调用，执行以下操作：
     * 1. 添加写操作兴趣，以便可以向客户端发送错误响应
     * 2. 调用认证器处理认证失败的情况
     * 
     * @throws IOException 如果在处理过程中发生I/O错误
     */
    void completeCloseOnAuthenticationFailure() throws IOException {
        // 添加写操作兴趣，以便发送错误响应
        transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        // 调用底层认证器处理认证失败
        authenticator.handleAuthenticationFailure();
    }

    /**
     * 检查通道是否处于静音状态
     * 
     * @return 如果通道已被显式静音（使用{@link KafkaChannel#mute()}）则返回true
     */
    public boolean isMuted() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    /**
     * 检查通道是否可以进入静音状态
     * 
     * 该方法在以下情况返回false：
     * 1. 当前没有接收请求（receive == null）
     * 2. 当前请求已成功分配所需内存
     * 3. 底层传输层未就绪
     * 
     * @return 如果通道可以进入静音状态则返回true
     */
    public boolean isInMutableState() {
        // 如果没有接收请求或已成功分配内存，则不需要静音
        if (receive == null || receive.memoryAllocated())
            return false;
        // 只有在传输层就绪时才能静音
        return transportLayer.ready();
    }

    /**
     * 检查通道是否就绪
     * 
     * 通道就绪需要满足两个条件：
     * 1. 底层传输层已就绪（完成握手）
     * 2. 认证过程已完成
     * 
     * @return 如果通道已就绪则返回true
     */
    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    /**
     * 检查是否有待发送的数据
     * 
     * @return 如果有待发送的数据则返回true
     */
    public boolean hasSend() {
        return send != null;
    }

    /**
     * 获取通道连接的远程地址
     * 
     * 该方法具有以下特点：
     * 1. 如果套接字从未连接过，返回null
     * 2. 如果套接字在关闭前已连接，即使关闭后也会返回之前连接的地址
     * 
     * @return 返回通道连接的远程InetAddress，如果从未连接则返回null
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    /**
     * 获取通道连接的远程端口
     * 
     * 该方法具有以下特点：
     * 1. 如果套接字从未连接过，返回0
     * 2. 如果套接字在关闭前已连接，即使关闭后也会返回之前连接的端口号
     * 
     * @return 返回通道连接的远程端口，如果从未连接则返回0
     */
    public int socketPort() {
        return transportLayer.socketChannel().socket().getPort();
    }

    /**
     * 获取套接字的描述信息
     * 
     * 如果远程地址不可用，则返回本地地址的字符串表示
     * 
     * @return 返回套接字的描述信息字符串
     */
    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 设置要发送的网络数据
     * 
     * 该方法执行以下操作：
     * 1. 检查是否有未完成的发送操作
     * 2. 设置新的发送数据
     * 3. 添加写操作兴趣
     * 
     * @param send 要发送的NetworkSend对象
     * @throws IllegalStateException 如果当前已有未完成的发送操作
     */
    public void setSend(NetworkSend send) {
        // 检查是否有未完成的发送操作
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
        // 设置新的发送数据
        this.send = send;
        // 添加写操作兴趣
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 尝试完成发送操作
     * 
     * 该方法执行以下操作：
     * 1. 检查发送操作是否完成
     * 2. 如果完成，清理发送状态并移除写操作兴趣
     * 
     * @return 如果发送完成则返回NetworkSend对象，否则返回null
     */
    public NetworkSend maybeCompleteSend() {
        if (send != null && send.completed()) {
            // 重置写操作状态
            midWrite = false;
            // 移除写操作兴趣
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            // 获取结果并清理发送对象
            NetworkSend result = send;
            send = null;
            return result;
        }
        return null;
    }

    /**
     * 从通道读取数据
     * 
     * 该方法执行以下操作：
     * 1. 如果没有接收对象，创建新的NetworkReceive实例
     * 2. 接收数据
     * 3. 检查内存分配状态，必要时静音通道
     * 
     * @return 返回接收到的字节数
     * @throws IOException 如果在读取过程中发生I/O错误
     */
    public long read() throws IOException {
        // 如果没有接收对象，创建新的实例
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }

        // 接收数据
        long bytesReceived = receive(this.receive);

        // 如果已知所需内存量但未分配成功，且通道可以静音，则进行静音
        if (this.receive.requiredMemoryAmountKnown() && !this.receive.memoryAllocated() && isInMutableState()) {
            // 内存池可能已耗尽，静音自身
            mute();
        }
        return bytesReceived;
    }

    /**
     * 获取当前的接收对象
     * 
     * @return 返回当前的NetworkReceive对象
     */
    public NetworkReceive currentReceive() {
        return receive;
    }

    /**
     * 尝试完成接收操作
     * 
     * 该方法执行以下操作：
     * 1. 检查接收是否完成
     * 2. 如果完成，重置缓冲区位置并清理接收状态
     * 
     * @return 如果接收完成则返回NetworkReceive对象，否则返回null
     */
    public NetworkReceive maybeCompleteReceive() {
        if (receive != null && receive.complete()) {
            // 重置缓冲区位置以便读取
            receive.payload().rewind();
            // 获取结果并清理接收对象
            NetworkReceive result = receive;
            receive = null;
            return result;
        }
        return null;
    }

    /**
     * 向通道写入数据
     * 
     * 该方法执行以下操作：
     * 1. 检查是否有数据需要发送
     * 2. 标记写入状态
     * 3. 执行实际的写入操作
     * 
     * @return 返回写入的字节数
     * @throws IOException 如果在写入过程中发生I/O错误
     */
    public long write() throws IOException {
        // 如果没有数据需要发送，直接返回0
        if (send == null)
            return 0;

        // 标记正在进行写操作
        midWrite = true;
        // 执行实际的写入操作
        return send.writeTo(transportLayer);
    }

    /**
     * 累加网络线程时间
     * 
     * 该方法用于跟踪通道在网络线程上花费的时间，
     * 在每次发送响应后读取并重置。
     * 
     * @param nanos 要累加的纳秒数
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    /**
     * 获取并重置此通道的累计网络线程时间
     * 
     * 该方法用于性能监控，跟踪通道在网络线程上的处理时间。
     * 每次调用后都会重置计数器，以便开始新的统计周期。
     * 
     * @return 返回通道的累计网络线程时间（纳秒），并将计数器重置为0
     */
    public long getAndResetNetworkThreadTimeNanos() {
        // 获取当前累计的网络线程时间
        long current = networkThreadTimeNanos;
        // 重置计数器为0，开始新的统计周期
        networkThreadTimeNanos = 0;
        return current;
    }

    /**
     * 从传输层读取数据到NetworkReceive对象中
     * 
     * 该方法处理实际的数据接收操作，包括SSL认证失败的处理。
     * 在TLSv1.3中，握手后消息可能会抛出SSL异常，这些异常被视为认证失败。
     *
     * @param receive 用于存储接收数据的NetworkReceive对象
     * @return 返回读取的字节数
     * @throws IOException 当发生I/O错误时抛出
     * @throws SslAuthenticationException 当SSL认证失败时抛出
     */
    private long receive(NetworkReceive receive) throws IOException {
        try {
            // 从传输层读取数据到NetworkReceive对象
            return receive.readFrom(transportLayer);
        } catch (SslAuthenticationException e) {
            // TLSv1.3中，握手后消息可能抛出SSL异常，这些异常被视为认证失败
            // 获取远程地址描述（如果可用）
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            // 设置通道状态为认证失败
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            throw e;
        }
    }

    /**
     * 检查传输层是否还有未读取的数据
     * 
     * 该方法用于确定底层传输层的中间缓冲区中是否还有待读取的数据。
     * 这对于确保所有数据都被完整处理很重要，特别是在非阻塞I/O操作中。
     *
     * @return 如果传输层的中间缓冲区中还有未读取的数据则返回true，否则返回false
     */
    public boolean hasBytesBuffered() {
        // 检查传输层是否有未读取的数据
        return transportLayer.hasBytesBuffered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaChannel that = (KafkaChannel) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return super.toString() + " id=" + id;
    }
    
    /**
     * 获取此实例成功认证的次数
     * 
     * 该计数器记录通道成功完成认证的次数。当启用重新认证功能并且至少成功执行过一次重新认证时，
     * 该值可能会大于1。这对于监控通道的认证状态和重认证行为很有帮助。
     * 
     * @return 返回此实例成功完成认证的次数
     */
    public int successfulAuthentications() {
        // 返回成功认证计数器的值
        return successfulAuthentications;
    }

    /**
     * 尝试开始服务器端重新认证过程
     * 
     * 该方法在以下条件全部满足时启动重新认证：
     * 1. 这是一个服务器端连接
     * 2. 连接有过期时间设置
     * 3. 距离上次重新认证开始时间已经过去至少1秒
     * 
     * 重新认证机制的设计考虑：
     * 1. 会话过期：通过设置过期时间来强制定期更新认证状态
     * 2. 限流保护：通过最小重认证间隔（1秒）防止频繁重认证请求
     * 3. 优雅处理：如果重认证条件不满足，请求会被当作普通SASL握手处理
     * 
     * @param saslHandshakeNetworkReceive 包含服务器接收到的{@code SaslHandshakeRequest}的{@link NetworkReceive}对象，
     *                                   用于启动重新认证
     * @param nowNanosSupplier 当前时间的提供者，返回纳秒级时间戳（与{@code System.nanoTime()}兼容），
     *                        仅用于相对时间比较
     * 
     * @return 如果满足所有重认证条件并开始重认证过程则返回true，否则返回false
     * @throws AuthenticationException 如果重认证因无效凭证或其他安全配置错误而失败
     * @throws IOException 如果读/写操作发生I/O错误
     * @throws IllegalStateException 如果通道不在"ready"状态
     */
    public boolean maybeBeginServerReauthentication(NetworkReceive saslHandshakeNetworkReceive,
            Supplier<Long> nowNanosSupplier) throws AuthenticationException, IOException {
        // 检查通道是否处于就绪状态
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should be \"ready\" when processing SASL Handshake for potential re-authentication");
        
        // 如果没有设置会话过期时间，则禁用重认证
        // 此时SASL握手请求会被当作普通请求处理，并向客户端返回失败结果
        // 由于正在处理接收到的数据包，无需检查通道是否处于静音状态
        if (authenticator.serverSessionExpirationTimeNanos() == null)
            return false;

        // 延迟获取当前时间，直到确实需要使用时才获取，以优化性能
        long nowNanos = nowNanosSupplier.get();

        // 检查是否满足最小重认证间隔要求（1秒）
        // 如果在1秒内尝试重认证，请求会被当作普通SASL握手处理，并向客户端返回失败结果
        if (lastReauthenticationStartNanos != 0
                && nowNanos - lastReauthenticationStartNanos < MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS)
            return false;

        // 更新最后重认证开始时间
        lastReauthenticationStartNanos = nowNanos;
        // 交换认证器并开始重认证过程
        swapAuthenticatorsAndBeginReauthentication(
                new ReauthenticationContext(authenticator, saslHandshakeNetworkReceive, nowNanos));
        return true;
    }

    /**
     * 尝试开始客户端重新认证过程
     * 
     * 该方法在以下条件全部满足时启动重新认证：
     * 1. 这是一个客户端连接
     * 2. 通道未处于静音状态
     * 3. 没有正在进行的写操作
     * 4. 已定义会话重认证时间且已过期
     * 
     * 重新认证机制的设计考虑：
     * 1. 状态检查：确保通道处于适当状态（未静音、无写操作）
     * 2. 时间控制：基于会话重认证时间决定是否需要重认证
     * 3. 资源清理：重认证开始前清理接收缓冲区
     * 
     * @param nowNanosSupplier 当前时间的提供者，返回纳秒级时间戳（与{@code System.nanoTime()}兼容），
     *                        仅用于相对时间比较
     * 
     * @return 如果满足所有重认证条件并开始重认证过程则返回true，否则返回false
     * @throws AuthenticationException 如果重认证因无效凭证或其他安全配置错误而失败
     * @throws IOException 如果读/写操作发生I/O错误
     * @throws IllegalStateException 如果通道不在"ready"状态
     */
    public boolean maybeBeginClientReauthentication(Supplier<Long> nowNanosSupplier)
            throws AuthenticationException, IOException {
        // 检查通道是否处于就绪状态
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should always be \"ready\" when it is checked for possible re-authentication");

        // 检查重认证的前置条件：
        // 1. 通道必须未静音
        // 2. 没有正在进行的写操作
        // 3. 已设置客户端会话重认证时间
        if (muteState != ChannelMuteState.NOT_MUTED || midWrite
                || authenticator.clientSessionReauthenticationTimeNanos() == null)
            return false;

        // 延迟获取当前时间，直到确实需要使用时才获取，以优化性能
        long nowNanos = nowNanosSupplier.get();

        // 检查是否已到达重认证时间
        if (nowNanos < authenticator.clientSessionReauthenticationTimeNanos())
            return false;

        // 交换认证器并开始重认证过程
        swapAuthenticatorsAndBeginReauthentication(new ReauthenticationContext(authenticator, receive, nowNanos));
        // 清理当前的接收缓冲区
        receive = null;
        return true;
    }
    
    /**
     * 获取此实例重新认证过程所花费的毫秒数
     * 
     * 该方法返回从此实例的角度观察到的重新认证过程所需的时间。
     * 服务器端观察到的值会比客户端观察到的值小，因为客户端会额外经历一个网络往返过程。
     * 
     * 应用场景：
     * 1. 监控重新认证性能
     * 2. 诊断认证延迟问题
     * 3. 系统性能调优
     * 
     * @return 如果适用，返回重新认证过程花费的毫秒数；否则返回null
     */
    public Long reauthenticationLatencyMs() {
        // 从认证器获取重新认证延迟时间
        return authenticator.reauthenticationLatencyMs();
    }

    /**
     * 检查服务器端认证会话是否已过期
     * 
     * 该方法用于服务器端通道，检查给定时间是否超过了会话的过期时间。
     * 这是实现会话安全性和资源清理的重要机制。
     * 
     * 应用场景：
     * 1. 定期检查会话有效性
     * 2. 强制过期会话重新认证
     * 3. 安全策略实施
     * 
     * @param nowNanos 当前时间（纳秒），通过{@code System.nanoTime()}获取
     * @return 如果是服务器端通道且当前时间超过会话过期时间则返回true，否则返回false
     */
    public boolean serverAuthenticationSessionExpired(long nowNanos) {
        // 获取服务器会话过期时间（纳秒）
        Long serverSessionExpirationTimeNanos = authenticator.serverSessionExpirationTimeNanos();
        // 如果过期时间存在且当前时间已超过过期时间，则返回true
        return serverSessionExpirationTimeNanos != null && nowNanos - serverSessionExpirationTimeNanos > 0;
    }
    
    /**
     * 获取重新认证过程中接收到的非重认证相关的响应
     * 
     * 该方法返回在重新认证过程中接收到的，但与重认证无关的客户端响应。
     * 这些响应对应于重新认证开始前发送的请求，这些请求是在通道成功认证时发送的，
     * 但响应在重新认证过程中到达。
     * 
     * 应用场景：
     * 1. 处理重认证期间的正常业务响应
     * 2. 确保请求-响应的连续性
     * 3. 避免响应丢失
     * 
     * @return 返回在重新认证过程中接收到的与重认证无关的{@link NetworkReceive}响应。
     *         返回值永远不为null，但可能为空
     */
    public Optional<NetworkReceive> pollResponseReceivedDuringReauthentication() {
        // 从认证器获取重认证过程中接收到的非重认证相关响应
        return authenticator.pollResponseReceivedDuringReauthentication();
    }
    
    /**
     * 检查连接的客户端是否支持重新认证
     * 
     * 该方法用于服务器端通道，检查已连接的客户端是否表明其支持重新认证功能。
     * 这对于实现无缝的会话续期和安全策略更新非常重要。
     * 
     * 应用场景：
     * 1. 动态会话管理
     * 2. 安全策略更新
     * 3. 向后兼容性检查
     * 
     * @return 如果是服务器端通道且连接的客户端支持重新认证则返回true，否则返回false
     */
    boolean connectedClientSupportsReauthentication() {
        // 检查客户端是否支持重新认证
        return authenticator.connectedClientSupportsReauthentication();
    }

    /**
     * 交换认证器并开始重新认证过程
     * 
     * 该方法负责替换当前的认证器并启动重新认证流程。新认证器负责关闭旧的认证器，
     * 这样可以确保资源的正确释放和状态的平滑过渡。
     * 
     * 应用场景：
     * 1. 会话凭证更新
     * 2. 安全策略变更
     * 3. 定期重新认证
     * 
     * @param reauthenticationContext 重新认证上下文，包含重新认证所需的信息
     * @throws IOException 如果在重新认证过程中发生I/O错误
     */
    private void swapAuthenticatorsAndBeginReauthentication(ReauthenticationContext reauthenticationContext)
            throws IOException {
        // 创建新的认证器实例（旧认证器的关闭由新认证器负责）
        authenticator = authenticatorCreator.get();
        // 使用新认证器开始重新认证过程
        authenticator.reauthenticate(reauthenticationContext);
    }

    /**
     * 获取通道元数据注册表
     * 
     * 该方法返回与此通道关联的元数据注册表，用于存储和管理通道相关的元数据信息。
     * 
     * 应用场景：
     * 1. 通道配置管理
     * 2. 监控指标收集
     * 3. 调试信息获取
     * 
     * @return 返回通道的元数据注册表实例
     */
    public ChannelMetadataRegistry channelMetadataRegistry() {
        // 返回通道的元数据注册表
        return metadataRegistry;
    }
}
