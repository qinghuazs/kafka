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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CorrelationIdMismatchException;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 用于异步网络I/O的网络客户端，是实现用户端生产者和消费者客户端的内部类。
 * <p>
 * 此类不是线程安全的!
 */
public class NetworkClient implements KafkaClient {

    /* 客户端状态枚举 */
    private enum State {
        ACTIVE,  // 活跃状态，正常工作
        CLOSING, // 正在关闭
        CLOSED   // 已关闭
    }

    /* 用于记录日志的Logger实例 */
    private final Logger log;

    /* 用于执行网络I/O操作的选择器 */
    private final Selectable selector;

    /* 元数据更新器，负责更新和维护Kafka集群的元数据信息 */
    private final MetadataUpdater metadataUpdater;

    /* 用于生成随机偏移量的随机数生成器 */
    private final Random randOffset;

    /* 维护每个节点的连接状态 */
    private final ClusterConnectionStates connectionStates;

    /* 当前正在发送或等待响应的请求集合 */
    private final InFlightRequests inFlightRequests;

    /* Socket发送缓冲区大小(字节) */
    private final int socketSendBuffer;

    /* Socket接收缓冲区大小(字节) */
    private final int socketReceiveBuffer;

    /* 用于在请求中标识此客户端的客户端ID */
    private final String clientId;

    /* 发送请求时使用的当前关联ID */
    private int correlation;

    /* 单个请求等待服务器确认的默认超时时间(毫秒) */
    private final int defaultRequestTimeoutMs;

    /* 重试创建到服务器连接前的等待时间(毫秒) */
    private final long reconnectBackoffMs;

    /* 从尝试获取元数据开始到客户端重新引导的超时时间 */
    private final long rebootstrapTriggerMs;

    /* 元数据恢复策略 */
    private final MetadataRecoveryStrategy metadataRecoveryStrategy;

    /* 用于获取系统时间的Time实例 */
    private final Time time;

    /**
     * 首次连接到broker时是否发送ApiVersionRequest请求
     * 用于发现broker支持的API版本
     */
    private final boolean discoverBrokerVersions;

    /* 维护各个API版本的信息 */
    private final ApiVersions apiVersions;

    /* 需要获取API版本信息的节点集合 */
    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    /* 已中止的发送请求列表 */
    private final List<ClientResponse> abortedSends = new LinkedList<>();

    /* 用于监控限流时间的传感器 */
    private final Sensor throttleTimeSensor;

    /* 客户端当前状态的原子引用 */
    private final AtomicReference<State> state;

    /* 遥测数据发送器 */
    private final TelemetrySender telemetrySender;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(selector,
             metadata,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             logContext,
             Long.MAX_VALUE,
             metadataRecoveryStrategy);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext,
                         long rebootstrapTriggerMs,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(null,
                metadata,
                selector,
                clientId,
                maxInFlightRequestsPerConnection,
                reconnectBackoffMs,
                reconnectBackoffMax,
                socketSendBuffer,
                socketReceiveBuffer,
                defaultRequestTimeoutMs,
                connectionSetupTimeoutMs,
                connectionSetupTimeoutMaxMs,
                time,
                discoverBrokerVersions,
                apiVersions,
                null,
                logContext,
                new DefaultHostResolver(),
                null,
                rebootstrapTriggerMs,
                metadataRecoveryStrategy);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             throttleTimeSensor,
             logContext,
             new DefaultHostResolver(),
             null,
             Long.MAX_VALUE,
             metadataRecoveryStrategy);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(metadataUpdater,
             null,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext,
             new DefaultHostResolver(),
             null,
             Long.MAX_VALUE,
             metadataRecoveryStrategy);
    }

    public NetworkClient(MetadataUpdater metadataUpdater,
                         Metadata metadata,
                         Selectable selector,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         HostResolver hostResolver,
                         ClientTelemetrySender clientTelemetrySender,
                         long rebootstrapTriggerMs,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(
                reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext, hostResolver);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
        this.state = new AtomicReference<>(State.ACTIVE);
        this.telemetrySender = (clientTelemetrySender != null) ? new TelemetrySender(clientTelemetrySender) : null;
        this.rebootstrapTriggerMs = rebootstrapTriggerMs;
        this.metadataRecoveryStrategy = metadataRecoveryStrategy;
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        // 检查是否已经准备就绪可以发送数据
        if (isReady(node, now))
            return true;

        // 如果当前可以建立连接，则初始化连接
        if (connectionStates.canConnect(node.idString(), now))
            // 如果我们想要向一个节点发送数据但还没有连接，则初始化一个连接
            initiateConnect(node, now);

        return false;
    }

    // 用于测试的可见方法，检查是否可以连接到指定节点
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * 断开与特定节点的连接（如果存在连接）
     * 该连接上的所有待处理的ClientRequest都将收到断开连接的通知
     *
     * @param nodeId 节点ID
     */
    @Override
    public void disconnect(String nodeId) {
        // 如果节点已经处于断开连接状态，则记录调试日志并返回
        if (connectionStates.isDisconnected(nodeId)) {
            log.debug("Client requested disconnect from node {}, which is already disconnected", nodeId);
            return;
        }

        // 记录断开连接的信息日志
        log.info("Client requested disconnect from node {}", nodeId);
        // 关闭与节点的连接
        selector.close(nodeId);
        // 获取当前时间戳
        long now = time.milliseconds();
        // 取消该节点上所有正在处理的请求
        cancelInFlightRequests(nodeId, now, abortedSends, false);
        // 更新连接状态为已断开
        connectionStates.disconnected(nodeId, now);
    }

    // 取消节点上所有正在处理的请求
    private void cancelInFlightRequests(String nodeId,
                                        long now,
                                        Collection<ClientResponse> responses,
                                        boolean timedOut) {
        // 清除并获取该节点的所有正在处理的请求
        Iterable<InFlightRequest> inFlightRequests = this.inFlightRequests.clearAll(nodeId);
        // 遍历所有请求进行处理
        for (InFlightRequest request : inFlightRequests) {
            // 根据日志级别记录不同详细程度的日志信息
            if (log.isDebugEnabled()) {
                log.debug("Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                        "(elapsed time since creation: {}ms, elapsed time since send: {}ms, throttle time: {}ms, request timeout: {}ms): {}",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.throttleTimeMs(), request.requestTimeoutMs, request.request);
            } else {
                log.info("Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                        "(elapsed time since creation: {}ms, elapsed time since send: {}ms, throttle time: {}ms, request timeout: {}ms)",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.throttleTimeMs(), request.requestTimeoutMs);
            }

            // 处理非内部请求
            if (!request.isInternalRequest) {
                if (responses != null) {
                    ClientResponse clientResponse;
                    // 根据是超时还是断开连接创建不同的响应
                    if (timedOut)
                        clientResponse = request.timedOut(now);
                    else
                        clientResponse = request.disconnected(now);
                    // 将响应添加到响应集合中
                    responses.add(clientResponse);
                }
            // 处理元数据请求失败的情况
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, Optional.empty());
            // 处理遥测API请求失败的情况
            } else if (isTelemetryApi(request.header.apiKey()) && telemetrySender != null) {
                telemetrySender.handleFailedRequest(request.header.apiKey(), null);
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        log.info("Client requested connection close from node {}", nodeId);
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, null, false);
        connectionStates.remove(nodeId);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
    }

    /**
     * 根据连接状态返回在尝试发送数据之前需要等待的毫秒数
     * 当断开连接时，这个值会考虑重连的退避时间
     * 当正在连接或已连接时，这个值会处理慢速/停滞的连接
     *
     * @param node 要检查的节点
     * @param now 当前时间戳
     * @return 需要等待的毫秒数
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    // 如果正在进行限流，返回剩余的限流延迟毫秒数，否则返回0
    // 这个方法用于测试
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * 基于连接延迟和限流延迟返回轮询延迟的毫秒数
     * @param node 要检查的连接
     * @param now 当前时间(毫秒)
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    /**
     * 根据连接状态检查节点的连接是否已失败
     * 这种连接失败通常是暂时的，可以在下一次调用{@link #ready(org.apache.kafka.common.Node, long)}时恢复
     * 但在某些情况下需要捕获和处理这些暂时性的失败
     *
     * @param node 要检查的节点
     * @return 如果连接已失败且节点已断开连接则返回true
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * 根据连接状态检查与此节点的认证是否失败
     * 认证失败会直接传播，不会进行任何重试
     *
     * @param node 要检查的节点
     * @return 如果认证失败则返回AuthenticationException，否则返回null
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * 检查具有给定ID的节点是否准备好发送更多请求
     *
     * @param node 要检查的节点
     * @param now 当前时间(毫秒)
     * @return 如果节点已准备好则返回true
     */
    @Override
    public boolean isReady(Node node, long now) {
        // 如果我们需要立即更新元数据，则声明所有请求未就绪
        // 以使元数据请求成为第一优先级
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now the current timestamp
     */
    private boolean canSendRequest(String node, long now) {
        // 检查连接状态是否就绪、通道是否就绪、是否可以发送更多请求
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
            inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    // 包级私有方法，用于测试
    // 发送内部元数据请求
    void sendInternalMetadataRequest(MetadataRequest.Builder builder, String nodeConnectionId, long now) {
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        doSend(clientRequest, true, now);
    }

    // 执行实际的请求发送操作
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        // 确保客户端处于活动状态
        ensureActive();
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // 如果这个请求来自NetworkClient外部，验证我们是否可以发送数据
            // 如果请求是内部的，我们相信内部代码已经做了这个验证
            // 对于某些内部请求，验证会略有不同
            // (例如，ApiVersionsRequests可以在READY状态之前发送)
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        // 获取请求构建器
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            // 获取节点的API版本信息
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // 注意：如果versionInfo为null，我们没有服务器版本信息
            // 这种情况会发生在发送初始ApiVersionRequest(用于获取版本信息本身)时
            // 当discoverBrokerVersions设置为false时也会出现这种情况
            if (versionInfo == null) {
                // 使用最新允许的版本
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                // 使用最新可用的版本
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // build调用也可能抛出UnsupportedVersionException
            // 如果有必要的字段在所选版本中无法表示
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // 如果版本不支持，跳过通过网络发送请求
            // 而是简单地将其添加到本地中止请求队列中
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            // 创建客户端响应
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);

            // 根据请求类型处理不支持的版本异常
            if (!isInternalRequest)
                // 非内部请求：添加到中止发送列表
                abortedSends.add(clientResponse);
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                // 元数据请求：通知元数据更新器处理失败
                metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
            else if (isTelemetryApi(clientRequest.apiKey()) && telemetrySender != null)
                // 遥测API请求：通知遥测发送器处理失败
                telemetrySender.handleFailedRequest(clientRequest.apiKey(), unsupportedVersionException);
        }
    }

    /**
     * 执行请求发送的核心方法
     * @param clientRequest 客户端请求对象，包含目标节点、超时时间等信息
     * @param isInternalRequest 是否为内部请求（如元数据请求）
     * @param now 当前时间戳
     * @param request 具体的请求内容
     */
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        // 获取目标节点的标识符
        String destination = clientRequest.destination();
        // 根据请求版本创建请求头，包含协议版本、客户端ID、相关ID等信息
        RequestHeader header = clientRequest.makeHeader(request.version());
        // 如果启用了调试日志，记录详细的请求信息
        if (log.isDebugEnabled()) {
            log.debug("Sending {} request with header {} and timeout {} to node {}: {}",
                clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request);
        }
        // 将请求和请求头序列化为可发送的格式
        Send send = request.toSend(header);
        // 创建一个在途请求对象，用于跟踪请求的状态和响应
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        // 将在途请求添加到跟踪集合中
        this.inFlightRequests.add(inFlightRequest);
        // 通过网络选择器发送请求到目标节点
        selector.send(new NetworkSend(clientRequest.destination(), send));
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        ensureActive();

        if (!abortedSends.isEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            List<ClientResponse> responses = new ArrayList<>();
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        long telemetryTimeout = telemetrySender != null ? telemetrySender.maybeUpdate(now) : Integer.MAX_VALUE;
        try {
            this.selector.poll(Utils.min(timeout, metadataTimeout, telemetryTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleCompletedSends(responses, updatedNow);
        handleCompletedReceives(responses, updatedNow);
        handleDisconnections(responses, updatedNow);
        handleConnections();
        handleInitiateApiVersionRequests(updatedNow);
        handleTimedOutConnections(responses, updatedNow);
        handleTimedOutRequests(responses, updatedNow);
        handleRebootstrap(responses, updatedNow);
        completeResponses(responses);

        return responses;
    }

    /**
     * 完成响应处理，调用每个响应的完成回调
     */
    private void completeResponses(List<ClientResponse> responses) {
        // 遍历所有响应
        for (ClientResponse response : responses) {
            try {
                // 调用响应的完成回调
                response.onComplete();
            } catch (Exception e) {
                // 记录回调执行过程中的未捕获异常
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * 获取正在处理的请求数量
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    /**
     * 检查是否有正在处理的请求
     */
    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * 获取指定节点上正在处理的请求数量
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    /**
     * 检查指定节点是否有正在处理的请求
     */
    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    /**
     * 检查是否有准备就绪可以发送数据的节点
     */
    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * 如果客户端正在等待I/O，唤醒它
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * 初始化客户端关闭过程
     */
    @Override
    public void initiateClose() {
        // 尝试将状态从ACTIVE变为CLOSING
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            // 唤醒可能阻塞的I/O操作
            wakeup();
        }
    }

    /**
     * 检查客户端是否处于活跃状态
     */
    @Override
    public boolean active() {
        return state.get() == State.ACTIVE;
    }

    /**
     * 确保客户端处于活跃状态，否则抛出异常
     */
    private void ensureActive() {
        if (!active())
            throw new DisconnectException("NetworkClient is no longer active, state is " + state);
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        // 尝试将状态从ACTIVE变为CLOSING
        state.compareAndSet(State.ACTIVE, State.CLOSING);
        // 尝试将状态从CLOSING变为CLOSED
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            // 关闭选择器
            this.selector.close();
            // 关闭元数据更新器
            this.metadataUpdater.close();
            // 如果存在遥测发送器，关闭它
            if (telemetrySender != null)
                telemetrySender.close();
        } else {
            // 记录重复关闭的警告
            log.warn("Attempting to close NetworkClient that has already been closed.");
        }
    }

    /**
     * 选择具有最少未完成请求且至少符合连接条件的节点。此方法会优先选择:
     * 1. 已有连接的节点
     * 2. 如果所有现有连接都在使用中，可能会选择一个尚未建立连接的节点
     * 3. 如果没有现有连接，会优先选择最近尝试连接时间最早的节点
     * 4. 永远不会选择在重连退避期内或正在被限流的节点
     *
     * @return 具有最少在途请求的节点信息
     */
    @Override
    public LeastLoadedNode leastLoadedNode(long now) {
        // 获取集群中的所有节点列表
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes in the Kafka cluster");
        // 初始化最小在途请求数为最大整数值
        int inflight = Integer.MAX_VALUE;

        // 用于记录找到的不同状态的节点
        Node foundConnecting = null;  // 正在建立连接的节点
        Node foundCanConnect = null;  // 可以建立连接的节点
        Node foundReady = null;       // 已就绪可发送请求的节点

        // 标记是否至少有一个连接就绪
        boolean atLeastOneConnectionReady = false;

        // 生成随机偏移量，用于在节点间实现负载均衡
        int offset = this.randOffset.nextInt(nodes.size());
        // 遍历所有节点
        for (int i = 0; i < nodes.size(); i++) {
            // 使用随机偏移确保不总是从第一个节点开始检查
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);

            // 检查是否有至少一个连接就绪
            if (!atLeastOneConnectionReady
                    && connectionStates.isReady(node.idString(), now)
                    && selector.isChannelReady(node.idString())) {
                atLeastOneConnectionReady = true;
            }

            // 检查是否可以向该节点发送请求
            if (canSendRequest(node.idString(), now)) {
                // 获取该节点当前的在途请求数
                int currInflight = this.inFlightRequests.count(node.idString());
                if (currInflight == 0) {
                    // 如果找到一个已建立连接且没有在途请求的节点，直接返回
                    log.trace("Found least loaded node {} connected with no in-flight requests", node);
                    return new LeastLoadedNode(node, true);
                } else if (currInflight < inflight) {
                    // 如果当前节点的在途请求数小于已知最小值，更新记录
                    inflight = currInflight;
                    foundReady = node;
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                // 记录正在准备连接的节点
                foundConnecting = node;
            } else if (canConnect(node, now)) {
                // 如果节点可以建立连接，选择最近尝试连接时间最早的节点
                if (foundCanConnect == null ||
                        this.connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                                this.connectionStates.lastConnectAttemptMs(node.idString())) {
                    foundCanConnect = node;
                }
            } else {
                // 该节点既不能发送请求也不能建立连接，从选择中排除
                log.trace("Removing node {} from least loaded node selection since it is neither ready " +
                        "for sending or connecting", node);
            }
        }

        // 按优先级返回节点：已就绪 > 正在连接 > 可以连接 > 无可用节点
        if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight);
            return new LeastLoadedNode(foundReady, atLeastOneConnectionReady);
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting);
            return new LeastLoadedNode(foundConnecting, atLeastOneConnectionReady);
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect);
            return new LeastLoadedNode(foundCanConnect, atLeastOneConnectionReady);
        } else {
            log.trace("Least loaded node selection failed to find an available node");
            return new LeastLoadedNode(null, atLeastOneConnectionReady);
        }
    }

    /**
     * 解析服务器响应的静态方法
     * 
     * @param responseBuffer 包含响应数据的ByteBuffer
     * @param requestHeader 原始请求的请求头
     * @return 解析后的AbstractResponse对象
     */
    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        try {
            // 调用AbstractResponse的静态方法解析响应数据
            return AbstractResponse.parseResponse(responseBuffer, requestHeader);
        } catch (BufferUnderflowException e) {
            // 如果在读取响应数据时发生缓冲区下溢，抛出模式异常
            throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e);
        } catch (CorrelationIdMismatchException e) {
            // 检查是否是SASL认证相关的请求
            if (SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                && !SaslClientAuthenticator.isReserved(e.responseCorrelationId()))
                // 如果是SASL请求但响应的correlationId不在SASL保留范围内，抛出模式异常
                throw new SchemaException("The response is unrelated to Sasl request since its correlation id is "
                    + e.responseCorrelationId() + " and the reserved range for Sasl request is [ "
                    + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + ","
                    + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]");
            else {
                // 其他correlationId不匹配的情况，直接抛出原异常
                throw e;
            }
        }
    }

    /**
     * 处理节点断开连接的后续操作
     *
     * @param responses 用于存储响应结果的列表，断开连接后的请求响应会被添加到这个列表中
     * @param nodeId 断开连接的节点ID
     * @param now 当前时间戳(毫秒)
     * @param disconnectState 断开连接时的通道状态，包含断开原因和远程地址信息
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        processDisconnection(responses, nodeId, now, disconnectState, false);
    }

    /**
     * 处理节点超时导致的断开连接
     * 当请求超时或连接建立超时时，会调用此方法处理断开连接的后续操作
     *
     * @param responses 用于存储响应结果的列表，断开连接后的请求响应会被添加到这个列表中
     * @param nodeId 断开连接的节点ID
     * @param now 当前时间戳(毫秒)
     */
    private void processTimeoutDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE, true);
    }

    /**
     * 处理节点断开连接的核心逻辑
     * 主要完成以下工作:
     * 1. 更新节点的连接状态
     * 2. 清理节点相关的API版本信息
     * 3. 根据断开原因记录相应级别的日志
     * 4. 处理该节点上未完成的请求
     * 5. 通知元数据更新器处理断开事件
     *
     * @param responses 用于存储响应结果的列表，断开连接后的请求响应会被添加到这个列表中
     * @param nodeId 断开连接的节点ID
     * @param now 当前时间戳(毫秒)
     * @param disconnectState 断开连接时的通道状态，包含断开原因和远程地址信息
     * @param timedOut 是否是由于超时导致的断开连接
     */
    /**
     * 处理节点断开连接的后续操作
     *
     * @param responses 用于存储响应的列表
     * @param nodeId 断开连接的节点ID
     * @param now 当前时间戳
     * @param disconnectState 断开连接时的通道状态
     * @param timedOut 是否因超时而断开连接
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState,
                                      boolean timedOut) {
        // 更新连接状态为已断开
        connectionStates.disconnected(nodeId, now);
        // 移除该节点的API版本信息
        apiVersions.remove(nodeId);
        // 移除该节点的API版本获取请求
        nodesNeedingApiVersionsFetch.remove(nodeId);
        
        // 根据断开连接时的状态进行不同处理
        switch (disconnectState.state()) {
            case AUTHENTICATION_FAILED:
                // 认证失败的情况
                AuthenticationException exception = disconnectState.exception();
                // 更新连接状态为认证失败
                connectionStates.authenticationFailed(nodeId, now, exception);
                // 记录错误日志
                log.error("Connection to node {} ({}) failed authentication due to: {}", nodeId,
                    disconnectState.remoteAddress(), exception.getMessage());
                break;
            case AUTHENTICATE:
                // 认证过程中断开的情况，记录警告日志
                log.warn("Connection to node {} ({}) terminated during authentication. This may happen " +
                    "due to any of the following reasons: (1) Firewall blocking Kafka TLS " +
                    "traffic (eg it may only allow HTTPS traffic), (2) Transient network issue.",
                    nodeId, disconnectState.remoteAddress());
                break;
            case NOT_CONNECTED:
                // 无法建立连接的情况，记录警告日志
                log.warn("Connection to node {} ({}) could not be established. Node may not be available.", nodeId, disconnectState.remoteAddress());
                break;
            default:
                // 其他断开连接状态在Selector中以debug级别记录
                break;
        }

        // 取消该节点上所有正在处理的请求
        cancelInFlightRequests(nodeId, now, responses, timedOut);
        // 通知元数据更新器处理服务器断开连接事件
        metadataUpdater.handleServerDisconnect(now, nodeId, Optional.ofNullable(disconnectState.exception()));
    }

    /**
     * 遍历所有正在处理的请求，检查并处理超时的请求。
     * 如果请求超时，将终止与该节点的连接并作为断开连接处理。
     *
     * @param responses 存储断开连接完成的响应列表
     * @param now 当前时间戳
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        // 获取所有包含超时请求的节点ID列表
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        for (String nodeId : nodeIds) {
            // 关闭与节点的连接
            this.selector.close(nodeId);
            log.info("Disconnecting from node {} due to request timeout.", nodeId);
            // 处理超时导致的断开连接
            processTimeoutDisconnection(responses, nodeId, now);
        }
    }

    /**
     * 处理已中止的发送请求
     * 
     * @param responses 响应列表，用于添加已中止的请求
     */
    private void handleAbortedSends(List<ClientResponse> responses) {
        // 将所有已中止的发送请求添加到响应列表中
        responses.addAll(abortedSends);
        // 清空已中止的发送请求列表
        abortedSends.clear();
    }

    /**
     * 处理套接字通道连接超时。当连接状态停留在CONNECTING状态超过超时时间时触发，
     * 超时时间由ClusterConnectionStates.NodeConnectionState指定。
     *
     * @param responses 用于更新的响应列表
     * @param now 当前时间戳
     */
    private void handleTimedOutConnections(List<ClientResponse> responses, long now) {
        // 获取连接建立超时的节点列表
        List<String> nodes = connectionStates.nodesWithConnectionSetupTimeout(now);
        for (String nodeId : nodes) {
            // 关闭与节点的连接
            this.selector.close(nodeId);
            log.info(
                "Disconnecting from node {} due to socket connection setup timeout. " +
                "The timeout value is {} ms.",
                nodeId,
                connectionStates.connectionSetupTimeoutMs(nodeId));
            // 处理超时导致的断开连接
            processTimeoutDisconnection(responses, nodeId, now);
        }
    }

    /**
     * 处理已完成的请求发送。特别是当不需要响应时，将请求标记为完成。
     *
     * @param responses 用于更新的响应列表
     * @param now 当前时间戳
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // 如果不需要响应，则在发送完成时返回
        for (NetworkSend send : this.selector.completedSends()) {
            // 获取发送到目标节点的最后一个请求
            InFlightRequest request = this.inFlightRequests.lastSent(send.destinationId());
            if (!request.expectResponse) {
                // 完成最后发送的请求
                this.inFlightRequests.completeLastSent(send.destinationId());
                // 添加完成的响应
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * 如果节点的响应包含非零限流延迟，且已为该节点启用客户端限流，
     * 则对连接进行指定延迟的限流。
     *
     * @param response 响应对象
     * @param apiVersion API版本
     * @param nodeId 节点ID
     * @param now 当前时间戳
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        // 获取响应中的限流时间
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            // 增加节点的限流时间
            inFlightRequests.incrementThrottleTime(nodeId, throttleTimeMs);
            // 设置节点的限流状态
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                      now + throttleTimeMs);
        }
    }

    /**
     * 处理已完成的接收操作，并使用接收到的响应更新响应列表
     *
     * @param responses 用于更新的响应列表
     * @param now 当前时间戳
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        // 遍历所有已完成的接收操作
        for (NetworkReceive receive : this.selector.completedReceives()) {
            // 获取响应来源节点
            String source = receive.source();
            // 完成下一个请求并获取该请求
            InFlightRequest req = inFlightRequests.completeNext(source);

            // 解析响应数据
            AbstractResponse response = parseResponse(receive.payload(), req.header);
            // 如果存在限流时间传感器，记录限流时间
            if (throttleTimeSensor != null)
                throttleTimeSensor.record(response.throttleTimeMs(), now);

            // 记录调试日志
            if (log.isDebugEnabled()) {
                log.debug("Received {} response from node {} for request with header {}: {}",
                    req.header.apiKey(), req.destination, req.header, response);
            }

            // 如果响应包含限流延迟，对连接进行限流
            maybeThrottle(response, req.header.apiVersion(), req.destination, now);
            
            // 根据不同类型的响应进行处理
            if (req.isInternalRequest && response instanceof MetadataResponse)
                // 处理元数据响应
                metadataUpdater.handleSuccessfulResponse(req.header, now, (MetadataResponse) response);
            else if (req.isInternalRequest && response instanceof ApiVersionsResponse)
                // 处理API版本响应
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) response);
            else if (req.isInternalRequest && response instanceof GetTelemetrySubscriptionsResponse)
                // 处理遥测订阅响应
                telemetrySender.handleResponse((GetTelemetrySubscriptionsResponse) response);
            else if (req.isInternalRequest && response instanceof PushTelemetryResponse)
                // 处理推送遥测响应
                telemetrySender.handleResponse((PushTelemetryResponse) response);
            else
                // 处理其他响应
                responses.add(req.completed(response, now));
        }
    }

    /**
     * 处理API版本响应
     *
     * @param responses 响应列表
     * @param req 正在处理的请求
     * @param now 当前时间戳
     * @param apiVersionsResponse API版本响应对象
     */
    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        // 获取目标节点ID
        final String node = req.destination;
        // 检查响应是否包含错误
        if (apiVersionsResponse.data().errorCode() != Errors.NONE.code()) {
            // 如果是版本0请求或错误不是不支持的版本，则断开连接
            if (req.request.version() == 0 || apiVersionsResponse.data().errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        Errors.forCode(apiVersionsResponse.data().errorCode()), node, req.header.correlationId());
                this.selector.close(node);
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                // 从Kafka 2.4开始，当返回UNSUPPORTED_VERSION错误时，ApiKeys字段会包含支持的ApiVersionsRequest版本
                // 如果未提供，客户端将回退到版本0
                short maxApiVersion = 0;
                if (apiVersionsResponse.data().apiKeys().size() > 0) {
                    ApiVersion apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion();
                    }
                }
                // 将节点添加到需要获取API版本的节点集合中
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            }
            return;
        }
        
        // 创建节点API版本信息对象
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(
            apiVersionsResponse.data().apiKeys(),
            apiVersionsResponse.data().supportedFeatures(),
            apiVersionsResponse.data().finalizedFeatures(),
            apiVersionsResponse.data().finalizedFeaturesEpoch());
        // 更新节点的API版本信息
        apiVersions.update(node, nodeVersionInfo);
        // 将连接状态设置为就绪
        this.connectionStates.ready(node);
        // 记录调试日志
        log.debug("Node {} has finalized features epoch: {}, finalized features: {}, supported features: {}, ZK migration ready: {}, API versions: {}.",
                node, apiVersionsResponse.data().finalizedFeaturesEpoch(), apiVersionsResponse.data().finalizedFeatures(),
                apiVersionsResponse.data().supportedFeatures(), apiVersionsResponse.data().zkMigrationReady(), nodeVersionInfo);
    }

    /**
     * 处理所有已断开的连接
     *
     * @param responses 断开连接时完成的响应列表
     * @param now 当前时间戳
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        // 遍历所有已断开连接的节点及其状态
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();  // 获取节点ID
            ChannelState channelState = entry.getValue();  // 获取连接状态
            if (channelState == ChannelState.EXPIRED) {  // 如果是因为空闲超时导致的断开
                log.debug("Idle connection to node {} disconnected.", node);
            } else {  // 其他原因导致的断开
                log.info("Node {} disconnected.", node);
            }
            processDisconnection(responses, node, now, channelState);  // 处理断开连接的后续操作
        }
    }

    /**
     * 记录所有新完成的连接
     */
    private void handleConnections() {
        // 遍历所有新建立连接的节点
        for (String node : this.selector.connected()) {
            // 注意：即使连接已建立，也可能还不能发送请求
            // 例如，如果启用了SSL，SSL握手会在连接建立后进行
            // 因此，在尝试使用此连接发送数据之前，仍需要检查isChannelReady
            if (discoverBrokerVersions) {  // 如果需要发现broker版本
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());  // 将节点加入需要获取API版本的列表
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {  // 如果不需要发现broker版本
                this.connectionStates.ready(node);  // 直接将连接状态设置为就绪
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    /**
     * 处理发起API版本请求
     * @param now 当前时间戳
     */
    private void handleInitiateApiVersionRequests(long now) {
        // 遍历所有需要获取API版本的节点
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();  // 获取节点ID
            // 如果通道已就绪且可以发送更多请求
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                // 仅当ApiVersionsRequest已排队准备发送时，才将连接状态转换为CHECKING_API_VERSIONS
                // 否则，如果通道未就绪，客户端可能会永远停留在CHECKING_API_VERSIONS状态
                this.connectionStates.checkingApiVersions(node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();  // 获取请求构建器
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);  // 创建客户端请求
                doSend(clientRequest, true, now);  // 发送请求
                iter.remove();  // 从待处理列表中移除该节点
            }
        }
    }

    /**
     * 处理客户端重新引导
     * @param responses 响应列表
     * @param now 当前时间戳
     */
    private void handleRebootstrap(List<ClientResponse> responses, long now) {
        // 如果元数据恢复策略为重新引导且需要重新引导
        if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP && metadataUpdater.needsRebootstrap(now, rebootstrapTriggerMs)) {
            // 遍历所有节点
            this.metadataUpdater.fetchNodes().forEach(node -> {
                String nodeId = node.idString();  // 获取节点ID
                this.selector.close(nodeId);  // 关闭与节点的连接
                // 如果节点正在连接或已连接
                if (connectionStates.isConnecting(nodeId) || connectionStates.isConnected(nodeId)) {
                    log.info("Disconnecting from node {} due to client rebootstrap.", nodeId);
                    processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);  // 处理断开连接
                }
            });
            metadataUpdater.rebootstrap(now);  // 执行重新引导
        }
    }

    /**
     * 初始化与指定节点的连接
     * @param node 要连接的节点
     * @param now 当前时间戳（毫秒）
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();  // 获取节点连接ID
        try {
            connectionStates.connecting(nodeConnectionId, now, node.host());  // 更新连接状态为正在连接
            InetAddress address = connectionStates.currentAddress(nodeConnectionId);  // 获取当前节点的网络地址
            log.debug("Initiating connection to node {} using address {}", node, address);
            // 通过选择器建立连接，设置发送和接收缓冲区大小
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(address, node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);
        } catch (IOException e) {
            log.warn("Error connecting to node {}", node, e);  // 记录连接错误
            connectionStates.disconnected(nodeConnectionId, now);  // 更新连接状态为已断开
            // 通知元数据更新器连接失败
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, Optional.empty());
        }
    }

    /**
     * 检查是否有任何节点正在建立连接
     * @return 如果有至少一个连接正在建立则返回true
     */
    private boolean isAnyNodeConnecting() {
        // 遍历所有节点，检查是否有节点处于正在连接状态
        for (Node node : metadataUpdater.fetchNodes()) {
            if (connectionStates.isConnecting(node.idString())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断给定的ApiKey是否属于遥测API
     * @param apiKey API键
     * @return 如果是遥测API则返回true
     */
    private boolean isTelemetryApi(ApiKeys apiKey) {
        return apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS || apiKey == ApiKeys.PUSH_TELEMETRY;
    }

    /**
     * 默认元数据更新器实现类，负责管理和更新Kafka集群的元数据信息
     */
    class DefaultMetadataUpdater implements MetadataUpdater {

        /* 当前集群的元数据信息 */
        private final Metadata metadata;

        /* 当前正在进行的元数据请求信息，如果没有请求则为null */
        private InProgressData inProgress;

        /*
         * 开始尝试获取元数据的时间戳(以毫秒为单位)。
         * 如果为空，表示尚未请求元数据。
         * 这个时间戳用于判断是否需要触发重新引导：
         * 如果从这个时间开始到现在超过了配置的重新引导触发间隔，就会触发重新引导。
         * 设置为Optional.of(0L)可以立即强制进行重新引导。
         */
        private Optional<Long> metadataAttemptStartMs = Optional.empty();


        /**
         * 构造函数
         * @param metadata 元数据对象
         */
        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.inProgress = null;
        }

        /**
         * 获取当前集群中的所有节点列表
         */
        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        /**
         * 检查是否需要更新元数据
         * @param now 当前时间戳
         * @return 如果需要更新则返回true
         */
        @Override
        public boolean isUpdateDue(long now) {
            return !hasFetchInProgress() && this.metadata.timeToNextUpdate(now) == 0;
        }

        /**
         * 检查是否有正在进行的元数据请求
         */
        private boolean hasFetchInProgress() {
            return inProgress != null;
        }

        /**
         * 尝试更新元数据
         * @param now 当前时间戳
         * @return 下一次可以尝试更新的时间间隔(毫秒)
         */
        @Override
        public long maybeUpdate(long now) {
            // 检查是否需要更新元数据
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            long waitForMetadataFetch = hasFetchInProgress() ? defaultRequestTimeoutMs : 0;

            // 计算元数据更新超时时间
            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // 如果是首次尝试获取元数据，记录开始时间
            if (metadataAttemptStartMs.isEmpty())
                metadataAttemptStartMs = Optional.of(now);

            // 获取负载最小的节点，用于发送元数据请求
            // 注意：此方法的行为和poll()超时计算高度相关
            LeastLoadedNode leastLoadedNode = leastLoadedNode(now);

            // 如果配置了重新引导策略且没有可用节点，执行重新引导
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP
                    && !leastLoadedNode.hasNodeAvailableOrConnectionReady()) {
                rebootstrap(now);

                leastLoadedNode = leastLoadedNode(now);
            }

            // 如果没有可用节点，放弃发送元数据请求
            if (leastLoadedNode.node() == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            // 尝试向选中的节点发送元数据请求
            return maybeUpdate(now, leastLoadedNode.node());
        }

        /**
         * 处理服务器断开连接的情况
         * @param now 当前时间戳
         * @param destinationId 目标节点ID
         * @param maybeFatalException 可能的致命异常
         */
        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            Cluster cluster = metadata.fetch();
            // 如果是引导服务器配置错误导致的连接被拒绝，或者是安全配置错误导致的认证失败，
            // processDisconnection会生成警告。
            // 下面的警告处理的是成功建立了到broker的连接，但在获取元数据之前就断开的情况。
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destinationId);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            // 如果在需要更新元数据时发生断开连接，将其视为更新失败
            // 以便正确执行退避策略
            if (isUpdateDue(now))
                handleFailedRequest(now, Optional.empty());

            // 如果有致命异常，通知元数据组件
            maybeFatalException.ifPresent(metadata::fatalError);

            // 断开连接可能是由于元数据过期导致的，请求更新元数据
            metadata.requestUpdate(false);
        }

        /**
         * 处理请求失败的情况
         * @param now 当前时间戳
         * @param maybeFatalException 可能的致命异常
         */
        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            // 如果有致命异常，通知元数据组件
            maybeFatalException.ifPresent(metadata::fatalError);
            // 标记元数据更新失败
            metadata.failedUpdate(now);
            // 清除正在进行的请求状态
            inProgress = null;
        }

        /**
         * 处理元数据请求成功响应
         * @param requestHeader 请求头
         * @param now 当前时间戳
         * @param response 元数据响应
         */
        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            // 检查是否有分区的leader缺少监听器，记录最多10个这样的分区用于诊断broker配置问题
            // 这可能是由于动态添加了监听器导致的临时问题
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                topicMetadata.partitionMetadata().stream()
                    .filter(partitionMetadata -> partitionMetadata.error == Errors.LISTENER_NOT_FOUND)
                    .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                .collect(Collectors.toList());
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // 检查是否有主题的元数据更新失败
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("The metadata response from the cluster reported a recoverable issue with correlation id {} : {}", requestHeader.correlationId(), errors);

            // 处理服务器要求重新引导的情况
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP && response.topLevelError() == Errors.REBOOTSTRAP_REQUIRED) {
                log.info("Rebootstrap requested by server.");
                initiateRebootstrap();
            } 
            // 处理空元数据响应的情况
            else if (response.brokers().isEmpty()) {
                // 当与broker的启动阶段通信时，可能会收到空的元数据集，这种情况需要稍后重试
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now);
            } 
            // 正常更新元数据
            else {
                this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
                metadataAttemptStartMs = Optional.empty();
            }

            // 清除正在进行的请求状态
            inProgress = null;
        }

        /**
         * 检查是否需要重新引导
         * @param now 当前时间戳
         * @param rebootstrapTriggerMs 触发重新引导的时间间隔
         */
        @Override
        public boolean needsRebootstrap(long now, long rebootstrapTriggerMs) {
            return metadataAttemptStartMs.filter(startMs -> now - startMs > rebootstrapTriggerMs).isPresent();
        }

        /**
         * 执行重新引导操作
         * @param now 当前时间戳
         */
        @Override
        public void rebootstrap(long now) {
            metadata.rebootstrap();
            metadataAttemptStartMs = Optional.of(now);
        }

        /**
         * 关闭元数据更新器
         */
        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * 初始化重新引导操作
         */
        private void initiateRebootstrap() {
            metadataAttemptStartMs = Optional.of(0L); // 强制立即进行重新引导
        }

        /**
         * 如果可以发送请求，则添加一个元数据请求到发送列表
         * @param now 当前时间戳
         * @param node 目标节点
         * @return 下一次可以尝试的时间间隔
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            // 如果可以发送请求，创建并发送元数据请求
            if (canSendRequest(nodeConnectionId, now)) {
                Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
                MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
                return defaultRequestTimeoutMs;
            }

            // 如果有正在建立的连接，等待其完成
            // 这可以防止客户端在之前的连接尝试未完成时不必要地连接到其他节点
            if (isAnyNodeConnecting()) {
                // 严格来说这里应该返回"连接超时"，但由于没有这样的应用层配置，
                // 使用重连退避时间代替
                return reconnectBackoffMs;
            }

            // 如果可以建立新连接，初始化连接
            if (connectionStates.canConnect(nodeConnectionId, now)) {
                log.debug("Initialize connection to node {} for sending metadata request", node);
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // 已连接但不能发送更多请求，或者正在连接中
            // 在这两种情况下，我们只需要等待网络事件通知我们所选连接可能再次可用
            return Long.MAX_VALUE;
        }

        /**
         * 保存正在进行的元数据请求的信息
         */
        public class InProgressData {
            /* 请求版本号 */
            public final int requestVersion;
            /* 是否是部分更新 */
            public final boolean isPartialUpdate;

            private InProgressData(int requestVersion, boolean isPartialUpdate) {
                this.requestVersion = requestVersion;
                this.isPartialUpdate = isPartialUpdate;
            }
        }

    }

    /**
     * 遥测数据发送器内部类，负责管理和发送客户端遥测数据到Kafka集群
     */
    class TelemetrySender {

        /* 实际执行遥测数据发送的组件 */
        private final ClientTelemetrySender clientTelemetrySender;
        /* 当前选中用于发送遥测数据的broker节点，会尽可能长时间复用同一个节点 */
        private Node stickyNode;

        /**
         * 构造遥测数据发送器
         * 
         * @param clientTelemetrySender 实际执行遥测数据发送的组件
         */
        public TelemetrySender(ClientTelemetrySender clientTelemetrySender) {
            this.clientTelemetrySender = clientTelemetrySender;
        }

        /**
         * 尝试更新遥测数据，如果条件满足则发送遥测请求
         * 
         * @param now 当前时间戳
         * @return 下一次可以发送遥测数据的时间间隔(毫秒)
         */
        public long maybeUpdate(long now) {
            // 检查是否到达下一次更新时间
            long timeToNextUpdate = clientTelemetrySender.timeToNextUpdate(defaultRequestTimeoutMs);
            if (timeToNextUpdate > 0)
                return timeToNextUpdate;

            // 根据KIP-714，尽可能长时间地复用同一个broker节点
            if (stickyNode == null) {
                // 选择负载最小的节点
                stickyNode = leastLoadedNode(now).node();
                if (stickyNode == null) {
                    log.debug("Give up sending telemetry request since no node is available");
                    return reconnectBackoffMs;
                }
            }

            return maybeUpdate(now, stickyNode);
        }

        /**
         * 尝试向指定节点发送遥测数据
         * 
         * @param now 当前时间戳
         * @param node 目标broker节点
         * @return 下一次可以发送遥测数据的时间间隔(毫秒)
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            // 检查是否可以向该节点发送请求
            if (canSendRequest(nodeConnectionId, now)) {
                // 创建遥测请求
                Optional<AbstractRequest.Builder<?>> requestOpt = clientTelemetrySender.createRequest();

                if (requestOpt.isEmpty())
                    return Long.MAX_VALUE;

                // 发送遥测请求
                AbstractRequest.Builder<?> request = requestOpt.get();
                ClientRequest clientRequest = newClientRequest(nodeConnectionId, request, now, true);
                doSend(clientRequest, true, now);
                return defaultRequestTimeoutMs;
            } else {
                // 如果无法向当前节点发送请求，清除sticky节点，下次循环时选择新的节点
                stickyNode = null;
            }

            // 如果有正在建立的连接，等待其完成，避免不必要的新连接
            if (isAnyNodeConnecting())
                return reconnectBackoffMs;

            // 检查是否可以建立新连接
            if (connectionStates.canConnect(nodeConnectionId, now)) {
                log.debug("Initialize connection to node {} for sending telemetry request", node);
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // 等待网络事件通知连接可用
            return Long.MAX_VALUE;
        }

        /**
         * 处理遥测订阅响应
         * 
         * @param response 遥测订阅响应
         */
        public void handleResponse(GetTelemetrySubscriptionsResponse response) {
            clientTelemetrySender.handleResponse(response);
        }

        /**
         * 处理推送遥测数据响应
         * 
         * @param response 推送遥测数据响应
         */
        public void handleResponse(PushTelemetryResponse response) {
            clientTelemetrySender.handleResponse(response);
        }

        /**
         * 处理遥测请求失败的情况
         * 
         * @param apiKey 失败的API类型
         * @param maybeFatalException 可能的致命异常
         */
        public void handleFailedRequest(ApiKeys apiKey, KafkaException maybeFatalException) {
            if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS)
                clientTelemetrySender.handleFailedGetTelemetrySubscriptionsRequest(maybeFatalException);
            else if (apiKey == ApiKeys.PUSH_TELEMETRY)
                clientTelemetrySender.handleFailedPushTelemetryRequest(maybeFatalException);
            else
                throw new IllegalStateException("Invalid api key for failed telemetry request");
        }

        /**
         * 关闭遥测发送器，释放相关资源
         */
        public void close() {
            try {
                clientTelemetrySender.close();
            } catch (Exception exception) {
                log.error("Failed to close client telemetry sender", exception);
            }
        }
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    // visible for testing
    int nextCorrelationId() {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1;
        }
        return correlation++;
    }

    // visible for testing
    Node telemetryConnectedNode() {
        return telemetrySender.stickyNode;
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final AbstractRequest request;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;
        final long requestTimeoutMs;
        long throttleTimeMs;

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                 clientRequest.requestTimeoutMs(),
                 clientRequest.createdTimeMs(),
                 clientRequest.destination(),
                 clientRequest.callback(),
                 clientRequest.expectResponse(),
                 isInternalRequest,
                 request,
                 send,
                 sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        public long timeElapsedSinceSendMs(long currentTimeMs) {
            return Math.max(0, currentTimeMs - sendTimeMs);
        }

        public long throttleTimeMs() {
            return throttleTimeMs;
        }

        public long timeElapsedSinceCreateMs(long currentTimeMs) {
            return Math.max(0, currentTimeMs - createdTimeMs);
        }

        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        public ClientResponse timedOut(long timeMs) {
            // A timed out request is considered disconnected as well
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, true, null, null, null);
        }

        public ClientResponse disconnected(long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, null, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }

        public void incrementThrottleTime(long throttleTimeMs) {
            this.throttleTimeMs = throttleTimeMs + this.throttleTimeMs;
        }
    }

}
