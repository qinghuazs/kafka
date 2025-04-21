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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 管理Kafka集群中每个节点的连接状态。
 * 该类实现了连接状态管理、重连机制和超时控制等核心功能。
 */
final class ClusterConnectionStates {
    // 重连退避算法的指数基数，每次重试时间间隔会是上次的2倍
    static final int RECONNECT_BACKOFF_EXP_BASE = 2;
    // 重连退避时间的随机抖动比例，用于避免多个客户端同时重连
    static final double RECONNECT_BACKOFF_JITTER = 0.2;
    // 连接建立超时的指数基数，每次超时时间会是上次的2倍
    static final int CONNECTION_SETUP_TIMEOUT_EXP_BASE = 2;
    // 连接建立超时的随机抖动比例
    static final double CONNECTION_SETUP_TIMEOUT_JITTER = 0.2;
    // 存储每个节点ID对应的连接状态
    private final Map<String, NodeConnectionState> nodeState;
    // 日志记录器
    private final Logger log;
    // 主机名解析器，用于将主机名解析为IP地址
    private final HostResolver hostResolver;
    // 当前正在建立连接的节点集合
    private final Set<String> connectingNodes;
    // 重连退避算法实现，用于计算重连等待时间
    private final ExponentialBackoff reconnectBackoff;
    // 连接建立超时控制，用于计算连接超时时间
    private final ExponentialBackoff connectionSetupTimeout;

    /**
     * 创建一个新的集群连接状态管理器
     * @param reconnectBackoffMs 初始重连等待时间（毫秒）
     * @param reconnectBackoffMaxMs 最大重连等待时间（毫秒）
     * @param connectionSetupTimeoutMs 初始连接建立超时时间（毫秒）
     * @param connectionSetupTimeoutMaxMs 最大连接建立超时时间（毫秒）
     * @param logContext 日志上下文
     * @param hostResolver 主机名解析器
     */
    public ClusterConnectionStates(long reconnectBackoffMs, long reconnectBackoffMaxMs,
                                   long connectionSetupTimeoutMs, long connectionSetupTimeoutMaxMs,
                                   LogContext logContext, HostResolver hostResolver) {
        // 初始化日志记录器
        this.log = logContext.logger(ClusterConnectionStates.class);
        // 创建重连退避算法实例，使用指数退避策略
        this.reconnectBackoff = new ExponentialBackoff(
                reconnectBackoffMs,
                RECONNECT_BACKOFF_EXP_BASE,
                reconnectBackoffMaxMs,
                RECONNECT_BACKOFF_JITTER);
        // 创建连接超时控制实例，同样使用指数退避策略
        this.connectionSetupTimeout = new ExponentialBackoff(
                connectionSetupTimeoutMs,
                CONNECTION_SETUP_TIMEOUT_EXP_BASE,
                connectionSetupTimeoutMaxMs,
                CONNECTION_SETUP_TIMEOUT_JITTER);
        // 初始化节点状态映射
        this.nodeState = new HashMap<>();
        // 初始化正在连接的节点集合
        this.connectingNodes = new HashSet<>();
        // 设置主机名解析器
        this.hostResolver = hostResolver;
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param id the connection id to check
     * @param now the current time in ms
     * @return true if we can initiate a new connection
     */
    /**
     * 判断是否可以对指定节点发起新的连接
     * 当节点未连接且已经等待足够的重连退避时间时，返回true
     * 
     * @param id 节点ID
     * @param now 当前时间戳（毫秒）
     * @return 如果可以发起新连接则返回true
     */
    public boolean canConnect(String id, long now) {
        // 获取节点的连接状态
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            // 如果节点状态不存在，说明是首次连接，可以直接连接
            return true;
        else
            // 只有当节点处于断开状态，且已经等待了足够的重连退避时间，才允许重新连接
            return state.state.isDisconnected() &&
                   now - state.lastConnectAttemptMs >= state.reconnectBackoffMs;
    }

    /**
     * 判断节点是否处于临时熔断状态（即在重连退避时间内）
     * 
     * @param id 节点ID
     * @param now 当前时间戳（毫秒）
     * @return 如果节点正在退避期则返回true
     */
    public boolean isBlackedOut(String id, long now) {
        // 获取节点的连接状态
        NodeConnectionState state = nodeState.get(id);
        // 节点存在且处于断开状态，并且还在重连退避时间内
        return state != null
                && state.state.isDisconnected()
                && now - state.lastConnectAttemptMs < state.reconnectBackoffMs;
    }

    /**
     * 计算在尝试发送数据前需要等待的时间
     * 根据节点的连接状态返回不同的等待时间：
     * - 正在连接：返回连接建立超时时间
     * - 已断开：返回重连退避剩余时间
     * - 已连接：返回无限等待时间（由其他事件唤醒）
     * 
     * @param id 节点ID
     * @param now 当前时间戳（毫秒）
     * @return 需要等待的毫秒数
     */
    public long connectionDelay(String id, long now) {
        // 获取节点的连接状态
        NodeConnectionState state = nodeState.get(id);
        if (state == null) return 0; // 节点不存在时无需等待

        if (state.state == ConnectionState.CONNECTING) {
            // 正在连接中，返回连接建立的超时时间
            return connectionSetupTimeoutMs(id);
        } else if (state.state.isDisconnected()) {
            // 已断开连接，计算重连退避的剩余等待时间
            long timeWaited = now - state.lastConnectAttemptMs;
            return Math.max(state.reconnectBackoffMs - timeWaited, 0);
        } else {
            // 已连接状态，返回无限等待
            // 因为其他事件（如连接建立或数据确认）会在数据可以发送时触发唤醒
            return Long.MAX_VALUE;
        }
    }

    /**
     * 检查指定节点是否正在建立连接
     * 
     * @param id 要检查的节点ID
     * @return 如果节点正在建立连接则返回true
     */
    public boolean isConnecting(String id) {
        // 获取节点的连接状态
        NodeConnectionState state = nodeState.get(id);
        // 节点存在且状态为CONNECTING时返回true
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * 检查节点是否处于连接准备阶段（正在建立连接或等待API版本信息）
     * 
     * @param id 要检查的节点ID
     * @return 如果节点正在建立连接或等待API版本信息则返回true
     */
    public boolean isPreparingConnection(String id) {
        // 获取节点的连接状态
        NodeConnectionState state = nodeState.get(id);
        // 节点存在且状态为CONNECTING或CHECKING_API_VERSIONS时返回true
        return state != null &&
                (state.state == ConnectionState.CONNECTING || state.state == ConnectionState.CHECKING_API_VERSIONS);
    }

    /**
     * 将指定节点的状态设置为正在连接，如果需要则切换到新的解析地址
     * 
     * @param id 连接的节点ID
     * @param now 当前时间戳（毫秒）
     * @param host 要连接的主机名，如果需要会在内部进行解析
     */
    public void connecting(String id, long now, String host) {
        // 获取节点的当前连接状态
        NodeConnectionState connectionState = nodeState.get(id);
        if (connectionState != null && connectionState.host().equals(host)) {
            // 如果节点已存在且主机名相同，更新连接尝试时间和状态
            connectionState.lastConnectAttemptMs = now;
            connectionState.state = ConnectionState.CONNECTING;
            // 切换到下一个解析的IP地址，如果所有地址都已尝试，则标记需要重新解析
            connectionState.moveToNextAddress();
            connectingNodes.add(id);
            return;
        } else if (connectionState != null) {
            // 如果节点存在但主机名发生变化，记录日志
            log.info("Hostname for node {} changed from {} to {}.", id, connectionState.host(), host);
        }

        // 如果节点不存在或主机名已改变，创建新的连接状态对象
        // 初始化重连退避和连接超时时间为基础值（无退避）
        nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now,
                reconnectBackoff.backoff(0), connectionSetupTimeout.backoff(0), host, hostResolver, log));
        connectingNodes.add(id);
    }

    /**
     * 获取指定连接的已解析IP地址，如果需要会触发DNS解析
     * 
     * @param id 连接的节点ID
     * @throws UnknownHostException 如果主机名无法解析则抛出此异常
     * @return 解析后的IP地址
     */
    public InetAddress currentAddress(String id) throws UnknownHostException {
        // 调用节点状态对象的currentAddress方法获取当前解析的IP地址
        return nodeState(id).currentAddress();
    }

    /**
     * 将指定节点的状态设置为断开连接状态，并更新相关计时器和状态
     * 
     * @param id 已断开连接的节点ID
     * @param now 当前时间戳（毫秒）
     */
    public void disconnected(String id, long now) {
        // 获取节点状态对象
        NodeConnectionState nodeState = nodeState(id);
        // 更新最后一次连接尝试时间
        nodeState.lastConnectAttemptMs = now;
        // 更新重连退避时间（指数增长）
        updateReconnectBackoff(nodeState);
        
        if (nodeState.state == ConnectionState.CONNECTING) {
            // 如果节点正在连接中断开，说明连接失败
            // 更新连接建立超时时间（指数增长）
            updateConnectionSetupTimeout(nodeState);
            // 从正在连接的节点集合中移除
            connectingNodes.remove(id);
        } else {
            // 如果是其他状态断开，重置连接建立超时时间
            resetConnectionSetupTimeout(nodeState);
            if (nodeState.state.isConnected()) {
                // 如果之前是已连接状态，清除已解析的地址缓存
                // 这样下次连接时会触发新的DNS解析，因为节点IP可能已经改变
                nodeState.clearAddresses();
            }
        }
        // 设置状态为断开连接
        nodeState.state = ConnectionState.DISCONNECTED;
    }

    /**
     * 设置节点的限流截止时间，在此时间之前该节点的连接将被限流
     * 
     * @param id 要限流的节点ID
     * @param throttleUntilTimeMs 限流截止时间（毫秒时间戳）
     */
    public void throttle(String id, long throttleUntilTimeMs) {
        // 获取节点状态
        NodeConnectionState state = nodeState.get(id);
        // 限流截止时间只能延长不能缩短，避免限流时间回退
        if (state != null && state.throttleUntilTimeMs < throttleUntilTimeMs) {
            state.throttleUntilTimeMs = throttleUntilTimeMs;
        }
    }

    /**
     * 获取节点当前剩余的限流等待时间
     * 
     * @param id 要检查的节点ID
     * @param now 当前时间戳（毫秒）
     * @return 如果节点处于限流中，返回剩余限流时间（毫秒）；否则返回0
     */
    public long throttleDelayMs(String id, long now) {
        // 获取节点状态
        NodeConnectionState state = nodeState.get(id);
        // 如果节点存在且还在限流时间内，返回剩余限流时间
        if (state != null && state.throttleUntilTimeMs > now) {
            return state.throttleUntilTimeMs - now;
        } else {
            return 0;
        }
    }

    /**
     * 计算在尝试发送数据前需要等待的时间
     * 综合考虑连接状态和限流状态，返回更大的等待时间
     * 
     * @param id 要检查的节点ID
     * @param now 当前时间戳（毫秒）
     * @return 需要等待的时间（毫秒）
     */
    public long pollDelayMs(String id, long now) {
        // 获取限流等待时间
        long throttleDelayMs = throttleDelayMs(id, now);
        // 如果节点已连接且处于限流中，返回限流等待时间
        if (isConnected(id) && throttleDelayMs > 0) {
            return throttleDelayMs;
        } else {
            // 否则返回连接状态相关的等待时间
            return connectionDelay(id, now);
        }
    }

    /**
     * 将节点状态设置为正在检查API版本
     * 这是连接建立过程中的一个中间状态，用于确认客户端和服务器支持的API版本兼容性
     * 
     * @param id 连接标识符
     */
    public void checkingApiVersions(String id) {
        // 获取节点状态
        NodeConnectionState nodeState = nodeState(id);
        // 设置状态为检查API版本
        nodeState.state = ConnectionState.CHECKING_API_VERSIONS;
        // 重置连接建立超时时间
        resetConnectionSetupTimeout(nodeState);
        // 从正在连接的节点集合中移除
        connectingNodes.remove(id);
    }

    /**
     * 将节点状态设置为就绪状态
     * 表示连接已完全建立，可以开始正常的数据传输
     * 
     * @param id 连接标识符
     */
    public void ready(String id) {
        // 获取节点状态
        NodeConnectionState nodeState = nodeState(id);
        // 设置状态为就绪
        nodeState.state = ConnectionState.READY;
        // 清除认证异常（如果之前有的话）
        nodeState.authenticationException = null;
        // 重置重连退避时间
        resetReconnectBackoff(nodeState);
        // 重置连接建立超时时间
        resetConnectionSetupTimeout(nodeState);
        // 从正在连接的节点集合中移除
        connectingNodes.remove(id);
    }

    /**
     * 将节点状态设置为认证失败状态
     * 这种情况通常发生在节点需要认证但认证过程失败时
     * 
     * @param id 连接标识符
     * @param now 当前时间戳（毫秒）
     * @param exception 导致认证失败的异常
     */
    public void authenticationFailed(String id, long now, AuthenticationException exception) {
        // 获取节点状态
        NodeConnectionState nodeState = nodeState(id);
        // 保存认证异常信息
        nodeState.authenticationException = exception;
        // 设置状态为认证失败
        nodeState.state = ConnectionState.AUTHENTICATION_FAILED;
        // 更新最后一次连接尝试时间
        nodeState.lastConnectAttemptMs = now;
        // 更新重连退避时间（指数增长）
        updateReconnectBackoff(nodeState);
    }

    /**
     * 检查指定节点是否处于就绪状态且未被限流
     * 就绪状态意味着节点已完成连接建立、认证和API版本协商等步骤，可以开始正常的数据传输
     *
     * @param id 要检查的节点ID
     * @param now 当前时间戳（毫秒）
     * @return 如果节点就绪且未被限流则返回true
     */
    public boolean isReady(String id, long now) {
        // 调用重载方法检查节点状态
        return isReady(nodeState.get(id), now);
    }

    /**
     * 内部方法：检查节点连接状态是否就绪且未被限流
     * 
     * @param state 节点的连接状态对象
     * @param now 当前时间戳（毫秒）
     * @return 如果节点就绪且未被限流则返回true
     */
    private boolean isReady(NodeConnectionState state, long now) {
        // 节点存在、状态为READY且已超过限流时间，则认为节点就绪
        return state != null && state.state == ConnectionState.READY && state.throttleUntilTimeMs <= now;
    }

    /**
     * 检查集群中是否至少有一个节点处于就绪状态且未被限流
     * 这个方法通常用于判断集群是否可用于数据传输
     *
     * @param now 当前时间戳（毫秒）
     * @return 如果至少有一个节点就绪且未被限流则返回true
     */
    public boolean hasReadyNodes(long now) {
        // 遍历所有节点，检查是否有就绪的节点
        for (Map.Entry<String, NodeConnectionState> entry : nodeState.entrySet()) {
            if (isReady(entry.getValue(), now)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查指定节点是否已建立连接
     * 连接建立意味着TCP连接已成功，但不一定完成认证和API版本协商
     * 
     * @param id 要检查的节点ID
     * @return 如果节点已连接则返回true
     */
    public boolean isConnected(String id) {
        // 获取节点状态并检查是否已连接
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isConnected();
    }

    /**
     * 检查指定节点是否处于断开连接状态
     * 断开状态可能是由于连接失败、认证失败或主动断开等原因导致
     * 
     * @param id 要检查的节点ID
     * @return 如果节点已断开连接则返回true
     */
    public boolean isDisconnected(String id) {
        // 获取节点状态并检查是否已断开
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isDisconnected();
    }

    /**
     * 获取节点的认证异常信息
     * 如果节点在认证过程中发生错误，这个方法将返回相关的异常信息
     * 
     * @param id 要检查的节点ID
     * @return 如果发生认证错误则返回异常对象，否则返回null
     */
    public AuthenticationException authenticationException(String id) {
        // 获取节点状态并返回认证异常（如果有）
        NodeConnectionState state = nodeState.get(id);
        return state != null ? state.authenticationException : null;
    }

    /**
     * 重置节点的重连退避计数器和时间
     * 当节点成功连接后，需要重置这些值以便下次断开时从初始状态开始计算退避时间
     *
     * @param nodeState 要更新的节点状态对象
     */
    private void resetReconnectBackoff(NodeConnectionState nodeState) {
        // 重置失败次数计数器
        nodeState.failedAttempts = 0;
        // 重置重连退避时间为初始值
        nodeState.reconnectBackoffMs = reconnectBackoff.backoff(0);
    }

    /**
     * 重置节点的连接建立超时计数器和时间
     * 当开始新的连接尝试时，需要重置这些值以便使用初始超时时间
     *
     * @param nodeState 要更新的节点状态对象
     */
    private void resetConnectionSetupTimeout(NodeConnectionState nodeState) {
        // 重置连接尝试失败次数
        nodeState.failedConnectAttempts = 0;
        // 重置连接建立超时时间为初始值
        nodeState.connectionSetupTimeoutMs = connectionSetupTimeout.backoff(0);
    }

    /**
     * 更新节点的重连退避时间
     * 使用指数退避算法计算下次重连等待时间，退避时间会随着失败次数增加而指数增长
     * 计算公式：reconnect.backoff.ms * 2^(failures - 1) * (1 ± 20%随机抖动)
     * 最大不超过 reconnect.backoff.max.ms
     *
     * @param nodeState 要更新的节点状态对象
     */
    private void updateReconnectBackoff(NodeConnectionState nodeState) {
        // 根据当前失败次数计算新的退避时间
        nodeState.reconnectBackoffMs = reconnectBackoff.backoff(nodeState.failedAttempts);
        // 增加失败次数计数
        nodeState.failedAttempts++;
    }

    /**
     * 更新节点的连接建立超时时间
     * 使用指数退避算法计算下次连接尝试的超时时间，超时时间会随着失败次数增加而指数增长
     * 计算公式：socket.connection.setup.timeout.ms * 2^failures * (1 ± 20%随机抖动)
     * 最大不超过 socket.connection.setup.timeout.max.ms
     *
     * @param nodeState 要更新的节点状态对象
     */
    private void updateConnectionSetupTimeout(NodeConnectionState nodeState) {
        // 增加连接尝试失败次数
        nodeState.failedConnectAttempts++;
        // 根据更新后的失败次数计算新的超时时间
        nodeState.connectionSetupTimeoutMs = connectionSetupTimeout.backoff(nodeState.failedConnectAttempts);
    }

    /**
     * 从连接状态跟踪器中移除指定节点
     * 这个方法与disconnected方法的主要区别在于对connectionDelay的影响：
     * - remove后connectionDelay将返回0
     * - disconnected后会考虑reconnectBackoffMs的值
     * 
     * @param id 要移除的节点ID
     */
    public void remove(String id) {
        nodeState.remove(id);
        connectingNodes.remove(id);
    }

    /**
     * 获取指定连接的当前状态
     * 该方法返回节点的连接状态，如CONNECTING、READY、DISCONNECTED等
     * 
     * @param id 要查询的连接ID
     * @return 连接的当前状态
     */
    public ConnectionState connectionState(String id) {
        // 通过nodeState方法获取节点状态对象，并返回其状态值
        return nodeState(id).state;
    }

    /**
     * 获取指定节点的连接状态对象
     * 该方法是一个内部工具方法，用于获取节点的完整状态信息
     * 
     * @param id 要获取状态的连接ID
     * @throws IllegalStateException 如果找不到指定ID的节点状态
     */
    private NodeConnectionState nodeState(String id) {
        // 从状态映射中获取节点状态
        NodeConnectionState state = this.nodeState.get(id);
        // 如果状态不存在，抛出异常
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }

    /**
     * 获取当前正在建立连接的节点ID集合
     * 该方法主要用于测试目的，用于验证连接状态管理的正确性
     * 
     * @return 处于CONNECTING状态的节点ID集合
     */
    // package private for testing only
    Set<String> connectingNodes() {
        // 返回正在连接的节点集合
        return this.connectingNodes;
    }

    /**
     * 获取节点最近一次尝试连接的时间戳
     * 用于判断重连时机和计算连接超时
     * 
     * @param id 要查询的连接ID
     * @return 最后一次连接尝试的时间戳（毫秒），如果节点不存在则返回0
     */
    public long lastConnectAttemptMs(String id) {
        // 获取节点状态
        NodeConnectionState nodeState = this.nodeState.get(id);
        // 如果节点不存在返回0，否则返回最后连接尝试时间
        return nodeState == null ? 0 : nodeState.lastConnectAttemptMs;
    }

    /**
     * 获取节点当前的连接建立超时时间
     * 该值基于socket.connection.setup.timeout配置，可能会随着重试次数指数增长
     * 
     * @param id 要查询的连接ID
     * @return 当前的连接建立超时时间（毫秒）
     */
    public long connectionSetupTimeoutMs(String id) {
        // 获取节点状态并返回其连接建立超时时间
        NodeConnectionState nodeState = this.nodeState(id);
        return nodeState.connectionSetupTimeoutMs;
    }

    /**
     * 检查指定节点的连接是否已超时
     * 通过比较当前时间与最后连接尝试时间的差值来判断是否超过了超时限制
     * 
     * @param id 要检查的连接ID
     * @param now 当前时间戳（毫秒）
     * @return 如果连接已超时则返回true
     * @throws IllegalStateException 如果节点不在CONNECTING状态
     */
    public boolean isConnectionSetupTimeout(String id, long now) {
        // 获取节点状态
        NodeConnectionState nodeState = this.nodeState(id);
        // 如果节点不在连接中状态，抛出异常
        if (nodeState.state != ConnectionState.CONNECTING)
            throw new IllegalStateException("Node " + id + " is not in connecting state");
        // 判断是否超过了连接建立超时时间
        return now - lastConnectAttemptMs(id) > connectionSetupTimeoutMs(id);
    }

    /**
     * 获取所有连接建立已超时的节点列表
     * 遍历所有正在连接的节点，返回其中已超时的节点ID列表
     * 
     * @param now 当前时间戳（毫秒）
     * @return 已超时的节点ID列表
     */
    public List<String> nodesWithConnectionSetupTimeout(long now) {
        // 使用Stream API过滤出已超时的节点
        return connectingNodes.stream()
            // 对每个正在连接的节点检查是否超时
            .filter(id -> isConnectionSetupTimeout(id, now))
            // 收集超时的节点ID到列表中
            .collect(Collectors.toList());
    }

    /**
     * 节点连接状态类，用于管理与单个Kafka节点的连接状态和相关信息。
     * 该类维护了节点的连接状态、重试机制、DNS解析和地址选择等核心功能。
     */
    private static class NodeConnectionState {
        // 节点的主机名
        private final String host;
        // 主机名解析器，用于将主机名解析为IP地址
        private final HostResolver hostResolver;
        // 日志记录器
        private final Logger log;

        // 当前连接状态（DISCONNECTED、CONNECTING、READY等）
        ConnectionState state;
        // 认证失败时的异常信息
        AuthenticationException authenticationException;
        // 最后一次尝试连接的时间戳（毫秒）
        long lastConnectAttemptMs;
        // 连接失败的总次数
        long failedAttempts;
        // 当前连接尝试失败的次数
        long failedConnectAttempts;
        // 重连退避时间（毫秒），随着失败次数增加而增加
        long reconnectBackoffMs;
        // 连接建立超时时间（毫秒），随着失败次数增加而增加
        long connectionSetupTimeoutMs;
        // 节点限流截止时间，如果当前时间小于此值则表示节点正在被限流
        long throttleUntilTimeMs;
        // 已解析的IP地址列表
        private List<InetAddress> addresses;
        // 当前使用的IP地址索引
        private int addressIndex;
        // 上次尝试连接的IP地址，用于避免连续使用同一个失败的地址
        private InetAddress lastAttemptedAddress;

        /**
         * 创建一个新的节点连接状态实例
         * 
         * @param state 初始连接状态
         * @param lastConnectAttemptMs 最后一次连接尝试时间
         * @param reconnectBackoffMs 初始重连退避时间
         * @param connectionSetupTimeoutMs 初始连接超时时间
         * @param host 节点主机名
         * @param hostResolver 主机名解析器
         * @param log 日志记录器
         */
        private NodeConnectionState(ConnectionState state, long lastConnectAttemptMs, long reconnectBackoffMs,
                long connectionSetupTimeoutMs, String host, HostResolver hostResolver, Logger log) {
            // 设置初始连接状态
            this.state = state;
            // 初始化为空地址列表，首次使用时会触发DNS解析
            this.addresses = Collections.emptyList();
            // 设置地址索引为-1，表示尚未开始使用地址列表
            this.addressIndex = -1;
            // 初始化认证异常为null
            this.authenticationException = null;
            // 记录最后一次连接尝试时间
            this.lastConnectAttemptMs = lastConnectAttemptMs;
            // 初始化失败计数器
            this.failedAttempts = 0;
            // 设置初始重连退避时间
            this.reconnectBackoffMs = reconnectBackoffMs;
            // 设置初始连接超时时间
            this.connectionSetupTimeoutMs = connectionSetupTimeoutMs;
            // 初始化限流时间为0，表示不限流
            this.throttleUntilTimeMs = 0;
            // 保存节点主机名
            this.host = host;
            // 设置主机名解析器
            this.hostResolver = hostResolver;
            // 设置日志记录器
            this.log = log;
        }

        /**
         * 获取节点的主机名
         * @return 主机名字符串
         */
        public String host() {
            return host;
        }

        /**
         * 获取当前选择的IP地址，如果需要则解析主机名
         * 该方法实现了地址选择和DNS解析的核心逻辑：
         * 1. 首次调用时会触发DNS解析
         * 2. 记录使用过的地址，避免重复使用可能有问题的地址
         * 
         * @return 当前选择的IP地址
         * @throws UnknownHostException 如果主机名解析失败
         */
        private InetAddress currentAddress() throws UnknownHostException {
            // 如果地址列表为空，触发DNS解析
            if (addresses.isEmpty()) {
                resolveAddresses();
            }

            // 获取当前索引对应的IP地址
            // 记录最后使用的地址，用于在重新解析时避免立即重用可能有问题的地址
            InetAddress currentAddress = addresses.get(addressIndex);
            lastAttemptedAddress = currentAddress;
            return currentAddress;
        }

        /**
         * 切换到下一个可用的IP地址
         * 如果所有地址都已尝试过，则清空地址列表以触发重新解析
         * 这个机制确保了在连接失败时可以尝试所有可用的IP地址
         */
        private void moveToNextAddress() {
            // 如果地址列表为空，直接返回
            // 下次调用currentAddress时会触发解析
            if (addresses.isEmpty())
                return;

            // 循环切换到下一个地址
            addressIndex = (addressIndex + 1) % addresses.size();
            // 如果已经尝试了所有地址，清空列表以触发重新解析
            if (addressIndex == 0)
                clearAddresses();
        }

        /**
         * 解析主机名获取IP地址列表
         * 该方法实现了智能的地址选择策略：
         * 1. 使用DNS解析获取所有可用IP地址
         * 2. 避免立即重用最后失败的地址
         * 
         * @throws UnknownHostException 如果主机名解析失败
         */
        private void resolveAddresses() throws UnknownHostException {
            // 解析主机名获取IP地址列表
            addresses = ClientUtils.resolve(host, hostResolver);
            // 记录调试日志
            if (log.isDebugEnabled()) {
                log.debug("已将主机名 {} 解析为地址列表 {}", host, addresses);
            }
            // 重置地址索引
            addressIndex = 0;

            // 如果有多个地址，且第一个地址是上次失败的地址
            // 则跳过该地址，从下一个地址开始尝试
            // 这样可以避免立即重用可能有问题的地址
            if (addresses.size() > 1 && addresses.get(addressIndex).equals(lastAttemptedAddress)) {
                addressIndex++;
            }
        }

        /**
         * 清空已解析的地址列表
         * 这将触发下次调用currentAddress时重新进行DNS解析
         */
        private void clearAddresses() {
            addresses = Collections.emptyList();
        }

        /**
         * 返回节点连接状态的字符串表示
         * 包含当前状态、最后连接时间、失败次数等关键信息
         */
        public String toString() {
            return "NodeConnectionState(" +
                "state=" + state + ", " +
                "lastConnectAttemptMs=" + lastConnectAttemptMs + ", " +
                "failedAttempts=" + failedAttempts + ", " +
                "failedConnectAttempts=" + failedConnectAttempts + ", " +
                "throttleUntilTimeMs=" + throttleUntilTimeMs + ")";
        }
    }
}
