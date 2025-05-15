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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * {@link org.apache.kafka.clients.NetworkClient} 的包装器，用于处理网络轮询和发送操作。
 * 应用场景：在消费者客户端中，封装底层的网络通信逻辑，简化上层代码对网络操作的调用。
 * 设计考虑：通过委托模式，将网络相关的复杂性隔离在 NetworkClientDelegate 中，使得消费者核心逻辑更清晰。
 */
public class NetworkClientDelegate implements AutoCloseable {

    // Kafka 客户端，用于实际的网络通信
    private final KafkaClient client;
    // 后台事件处理器，用于异步处理事件，例如错误事件
    private final BackgroundEventHandler backgroundEventHandler;
    // 元数据，维护集群的元数据信息，如 broker、主题、分区等
    private final Metadata metadata;
    // 时间工具，用于获取当前时间、计算超时等
    private final Time time;
    // 日志记录器，用于记录此类操作的日志
    private final Logger log;
    // 请求超时时间（毫秒）
    private final int requestTimeoutMs;
    // 未发送请求的队列，存储等待发送的请求
    private final Queue<UnsentRequest> unsentRequests;
    // 重试退避时间（毫秒），在请求失败后等待一段时间再重试
    private final long retryBackoffMs;
    // 可选的元数据错误，如果元数据更新失败，则存储异常信息
    private Optional<Exception> metadataError;
    // 是否通过错误队列通知元数据错误
    private final boolean notifyMetadataErrorsViaErrorQueue;
    // 异步消费者指标，用于收集和报告与异步操作相关的消费者指标
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    /**
     * NetworkClientDelegate 的构造函数。
     * 应用场景：创建 NetworkClientDelegate 实例时调用，通常在消费者客户端初始化过程中。
     * 实现细节：初始化所有 final 字段，并从配置中读取相关参数。
     * 设计考虑：通过构造函数注入依赖项，方便测试和配置。
     *
     * @param time 时间工具
     * @param config 消费者配置
     * @param logContext 日志上下文
     * @param client Kafka 客户端
     * @param metadata 元数据
     * @param backgroundEventHandler 后台事件处理器
     * @param notifyMetadataErrorsViaErrorQueue 是否通过错误队列通知元数据错误
     * @param asyncConsumerMetrics 异步消费者指标
     */
    public NetworkClientDelegate(
            final Time time, // 传入时间工具实例
            final ConsumerConfig config, // 传入消费者配置
            final LogContext logContext, // 传入日志上下文
            final KafkaClient client, // 传入 Kafka 客户端实例
            final Metadata metadata, // 传入元数据实例
            final BackgroundEventHandler backgroundEventHandler, // 传入后台事件处理器实例
            final boolean notifyMetadataErrorsViaErrorQueue, // 传入是否通过错误队列通知元数据错误的标志
            final AsyncConsumerMetrics asyncConsumerMetrics) { // 传入异步消费者指标实例
        // 初始化时间工具
        this.time = time;
        // 初始化 Kafka 客户端
        this.client = client;
        // 初始化元数据
        this.metadata = metadata;
        // 初始化后台事件处理器
        this.backgroundEventHandler = backgroundEventHandler;
        // 初始化日志记录器，使用当前类的名称
        this.log = logContext.logger(getClass());
        // 初始化未发送请求队列为一个 ArrayDeque 实例
        this.unsentRequests = new ArrayDeque<>();
        // 从配置中获取请求超时时间
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        // 从配置中获取重试退避时间
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        // 初始化元数据错误为 Optional.empty()
        this.metadataError = Optional.empty();
        // 初始化是否通过错误队列通知元数据错误的标志
        this.notifyMetadataErrorsViaErrorQueue = notifyMetadataErrorsViaErrorQueue;
        // 初始化异步消费者指标
        this.asyncConsumerMetrics = asyncConsumerMetrics;
    }

    // 仅用于测试，返回未发送请求的队列
    // 应用场景：在单元测试中，可以访问和检查未发送的请求。
    // 实现细节：直接返回 unsentRequests 字段。
    // 设计考虑：提供一个包可见的方法，方便测试，同时不暴露给公共 API。
    Queue<UnsentRequest> unsentRequests() {
        // 返回未发送请求的队列
        return unsentRequests;
    }

    /**
     * 获取当前正在处理中的请求数量。
     * 应用场景：监控消费者客户端的网络负载情况。
     * 实现细节：调用 KafkaClient 的 inFlightRequestCount() 方法。
     * 设计考虑：将获取在途请求数的逻辑委托给 KafkaClient。
     *
     * @return 当前正在处理中的请求数量
     */
    public int inflightRequestCount() {
        // 调用 client 的 inFlightRequestCount 方法获取在途请求数
        return client.inFlightRequestCount();
    }

    /**
     * 检查节点是否已断开连接并且无法立即重新连接（即，在断开连接后的重新连接退避窗口中）。
     * 应用场景：在发送请求前，判断目标节点是否可用，避免向不可用节点发送请求。
     * 实现细节：调用 NetworkClientUtils.isUnavailable 方法进行判断。
     * 设计考虑：复用 NetworkClientUtils 中的工具方法，保持逻辑一致性。
     *
     * @param node 要检查可用性的 {@link Node} 节点
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    public boolean isUnavailable(Node node) {
        // 调用 NetworkClientUtils.isUnavailable 方法判断节点是否不可用
        return NetworkClientUtils.isUnavailable(client, node, time);
    }

    /**
     * 检查给定节点上是否存在身份验证错误，如果存在则抛出异常。
     * 应用场景：在与节点交互之前，检查该节点是否有身份验证失败的历史记录，确保操作的有效性。
     * 设计考虑：将身份验证失败的检查逻辑委托给 NetworkClientUtils，保持代码的模块化和复用性。
     *
     * @param node 要检查先前 {@link AuthenticationException} 的 {@link Node}；如果找到则抛出
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    public void maybeThrowAuthFailure(Node node) {
        // 调用 NetworkClientUtils 的静态方法来检查并可能抛出节点上的身份验证失败。
        // client 是当前的 KafkaClient 实例，node 是要检查的目标节点。
        NetworkClientUtils.maybeThrowAuthFailure(client, node);
    }

    /**
     * 如果当前可能，则发起连接。这主要用于重置套接字的失败状态。
     * 应用场景：当需要显式尝试与某个节点建立连接时，例如在检测到连接失败后，希望主动重试连接。
     * 设计考虑：同样将连接尝试的逻辑委托给 NetworkClientUtils，以便统一处理连接相关的操作。
     *
     * @param node 要连接的节点
     */
    public void tryConnect(Node node) {
        // 调用 NetworkClientUtils 的静态方法尝试连接到指定的节点。
        // client 是当前的 KafkaClient 实例，node 是目标节点，time 用于记录连接尝试的时间。
        NetworkClientUtils.tryConnect(client, node, time);
    }

    /**
     * 返回已发送请求的响应。此方法将尝试发送未发送的请求，轮询响应，并检查断开连接的节点。
     * 应用场景：这是网络交互的核心方法，在消费者的主循环中被调用，用于驱动请求的发送和响应的处理。
     * 设计考虑：将发送、轮询、错误处理和连接检查等多个步骤组合在一起，形成一个完整的网络交互周期。
     *
     * @param timeoutMs     超时时间（毫秒），指示 poll 操作最长阻塞时间
     * @param currentTimeMs 当前时间（毫秒），用于各种超时和时间相关的计算
     */
    public void poll(final long timeoutMs, final long currentTimeMs) {
        // 尝试发送队列中所有未发送的请求。
        trySend(currentTimeMs);

        // 初始化轮询超时时间为传入的 timeoutMs。
        long pollTimeoutMs = timeoutMs;
        // 如果存在未发送的请求，则将轮询超时时间限制为 retryBackoffMs 和当前 pollTimeoutMs 中的较小值。
        // 这是为了确保即使有未发送的请求，也不会因为长时间等待响应而阻塞太久，而是会更快地进入下一轮尝试发送。
        if (!unsentRequests.isEmpty()) {
            pollTimeoutMs = Math.min(retryBackoffMs, pollTimeoutMs);
        }
        // 调用底层 KafkaClient 的 poll 方法，实际执行网络 I/O 操作，发送请求并接收响应。
        this.client.poll(pollTimeoutMs, currentTimeMs);
        // 检查并可能传播元数据相关的错误。
        maybePropagateMetadataError();
        // 检查并处理已断开连接的节点上的未发送请求。
        checkDisconnects(currentTimeMs);
        // 记录未发送请求队列的大小指标。
        asyncConsumerMetrics.recordUnsentRequestsQueueSize(unsentRequests.size(), currentTimeMs);
    }

    /**
     * 检查元数据中是否存在任何异常，如果存在，则根据配置决定是通过后台错误队列通知还是直接记录错误。
     * 应用场景：在 poll 操作后，检查元数据更新过程中是否发生了错误，并进行相应的处理。
     * 设计考虑：提供两种错误通知机制，一种是通过事件队列异步处理，另一种是直接记录，增加了灵活性。
     */
    private void maybePropagateMetadataError() {
        try {
            // 尝试让元数据对象抛出其内部积累的任何异常。
            metadata.maybeThrowAnyException();
        } catch (Exception e) {
            // 如果捕获到异常，则根据配置进行处理。
            if (notifyMetadataErrorsViaErrorQueue) {
                // 如果配置为通过错误队列通知，则将错误包装成 ErrorEvent 添加到后台事件处理器。
                backgroundEventHandler.add(new ErrorEvent(e));
            } else {
                // 否则，将错误存储在 metadataError 字段中。
                metadataError = Optional.of(e);
            }
        }
    }

    /**
     * 如果至少有一个正在进行中的请求或未发送的请求，则返回 true。
     * 应用场景：用于判断当前是否有待处理的网络请求，例如在关闭消费者或执行某些清理操作前，可以检查此状态。
     * 设计考虑：提供一个便捷的方法来快速了解网络客户端的繁忙程度。
     * @return 如果有任何待处理的请求，则返回 true；否则返回 false。
     */
    public boolean hasAnyPendingRequests() {
        // 检查 KafkaClient 是否有正在进行中的请求 (已发送但未收到响应)
        // 或者检查未发送请求队列是否不为空。
        return client.hasInFlightRequests() || !unsentRequests.isEmpty();
    }

    /**
     * 尝试发送 unsentRequest 队列中的请求。如果请求没有分配节点，它将查找负载最低的节点，
     * 并在下一次 {@code poll()} 中重试。如果请求已过期，则会抛出 {@link TimeoutException}。
     * 应用场景：在每次 poll 操作开始时，遍历并尝试发送所有待处理的请求。
     * 设计考虑：处理请求的超时、选择目标节点以及实际发送逻辑，是请求发送的核心调度部分。
     * @param currentTimeMs 当前时间（毫秒），用于检查请求是否超时和更新计时器。
     */
    private void trySend(final long currentTimeMs) {
        // 获取未发送请求队列的迭代器。
        Iterator<UnsentRequest> iterator = unsentRequests.iterator();
        // 遍历所有未发送的请求。
        while (iterator.hasNext()) {
            // 获取下一个未发送的请求。
            UnsentRequest unsent = iterator.next();
            // 更新请求的计时器，以反映当前时间。
            unsent.timer.update(currentTimeMs);
            // 检查请求是否已超时。
            if (unsent.timer.isExpired()) {
                // 如果请求已超时，则从队列中移除。
                iterator.remove();
                // 记录请求在未发送队列中的停留时间。
                asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - unsent.enqueueTimeMs());
                // 调用请求处理器的 onFailure 方法，通知请求因超时而失败。
                unsent.handler.onFailure(currentTimeMs, new TimeoutException(
                    "Failed to send request after " + unsent.timer.timeoutMs() + " ms."));
                // 继续处理下一个未发送的请求。
                continue;
            }

            // 尝试实际发送请求，如果发送不成功（例如节点不可用）。
            if (!doSend(unsent, currentTimeMs)) {
                // 如果发送失败，则继续重试直到超时。当前迭代中不移除该请求，它将在后续的 trySend 调用中被再次尝试。
                continue;
            }
            // 如果请求发送成功，则从队列中移除。
            iterator.remove();
            // 记录请求在未发送队列中的停留时间。
            asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - unsent.enqueueTimeMs());
        }
    }

    /**
     * 实际执行发送单个未发送请求的操作。
     * 应用场景：被 {@link #trySend(long)} 方法调用，用于处理单个请求的发送逻辑，包括节点选择和就绪状态检查。
     * 设计考虑：将单个请求的发送细节封装在此方法中，使 {@link #trySend(long)} 的逻辑更清晰。
     * @param r 要发送的未发送请求 {@link UnsentRequest}。
     * @param currentTimeMs 当前时间（毫秒）。
     * @return 如果请求已成功发送或已准备好发送，则返回 true；如果由于节点不可用或未就绪而无法发送，则返回 false。
     */
    boolean doSend(final UnsentRequest r, final long currentTimeMs) {
        // 获取请求的目标节点。如果请求中指定了节点，则使用该节点；否则，从 KafkaClient 获取当前负载最低的节点。
        Node node = r.node.orElse(client.leastLoadedNode(currentTimeMs).node());
        // 如果找不到可用的节点，或者选定的节点当前不可用（例如，正在进行重新连接退避）。
        if (node == null || nodeUnavailable(node)) {
            // 记录调试信息，表明没有可用的 broker 来发送请求，并将重试。
            log.debug("No broker available to send the request: {}. Retrying.", r);
            // 返回 false，表示请求未发送成功，将在后续尝试。
            return false;
        }
        // 根据未发送请求的信息和选定的节点，构建一个 ClientRequest 对象。
        ClientRequest request = makeClientRequest(r, node, currentTimeMs);
        // 检查 KafkaClient 是否已准备好向该节点发送请求（例如，连接是否已建立且可用）。
        if (!client.ready(node, currentTimeMs)) {
            // 如果节点尚未准备好，则将请求重新排队。该请求将在事件循环的下一次迭代中处理。
            // 记录调试信息。
            log.debug("Node is not ready, handle the request in the next event loop: node={}, request={}", node, r);
            // 返回 false，表示请求未发送成功，将在后续尝试。
            return false;
        }
        // 调用 KafkaClient 的 send 方法，将请求发送到目标节点。
        client.send(request, currentTimeMs);
        // 返回 true，表示请求已成功发送。
        return true;
    }

    /**
     * 检查未发送请求队列中是否有请求的目标节点已断开连接。
     * 如果发现某个请求的目标节点连接失败，则将该请求从队列中移除，并以认证异常（如果存在）或通用连接失败来通知其处理器。
     * 应用场景：在每次 poll 操作后，清理那些因节点连接问题而无法发送的请求。
     * 设计考虑：主动检查并处理因节点断开连接而无法发送的请求，避免这些请求无限期地停留在队列中。
     * @param currentTimeMs 当前时间（毫秒），用于回调处理器。
     */
    protected void checkDisconnects(final long currentTimeMs) {
        // 检查未发送请求的连接。如果无法连接，则断开已断开连接的节点。
        // 获取未发送请求队列的迭代器。
        Iterator<UnsentRequest> iter = unsentRequests.iterator();
        // 遍历所有未发送的请求。
        while (iter.hasNext()) {
            // 获取下一个未发送的请求。
            UnsentRequest u = iter.next();
            // 检查请求是否已指定目标节点，并且该节点与 KafkaClient 的连接是否失败。
            if (u.node.isPresent() && client.connectionFailed(u.node.get())) {
                // 如果连接失败，则从队列中移除该请求。
                iter.remove();
                // 记录请求在未发送队列中的停留时间。
                asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - u.enqueueTimeMs());
                // 获取与该节点相关的身份验证异常（如果存在）。
                AuthenticationException authenticationException = client.authenticationException(u.node.get());
                // 调用请求处理器的 onFailure 方法，通知请求因连接失败而失败。
                // 如果存在身份验证异常，则使用该异常；否则，通常意味着其他类型的连接问题。
                u.handler.onFailure(currentTimeMs, authenticationException);
            }
        }
    }

    /**
     * 创建一个客户端请求。
     * 应用场景：当需要将一个 {@link UnsentRequest} 转换为可以发送给 Kafka broker 的 {@link ClientRequest} 时调用此方法。
     * 实现细节：使用 KafkaClient 的 newClientRequest 方法构建请求，并设置请求的节点、构建器、当前时间、是否需要响应、剩余超时时间和回调处理器。
     * 设计考虑：封装了 ClientRequest 的创建逻辑，使得上层调用更简洁。
     *
     * @param unsent 未发送的请求对象，包含请求构建器和回调处理器等信息。
     * @param node 目标 Kafka 节点。
     * @param currentTimeMs 当前时间戳（毫秒）。
     * @return 构建好的客户端请求对象。
     */
    private ClientRequest makeClientRequest(
        final UnsentRequest unsent, // 未发送的请求对象
        final Node node, // 目标节点
        final long currentTimeMs // 当前时间戳
    ) {
        // 调用 KafkaClient 的 newClientRequest 方法创建新的客户端请求
        return client.newClientRequest(
            node.idString(), // 目标节点的 ID 字符串
            unsent.requestBuilder, // 未发送请求中的请求构建器
            currentTimeMs, // 当前时间戳
            true, // 表示需要响应
            (int) unsent.timer.remainingMs(), // 请求的剩余超时时间（毫秒），从 UnsentRequest 的计时器获取
            unsent.handler // 未发送请求中的回调处理器
        );
    }
    
    /**
     * 获取并清除元数据错误。
     * 应用场景：当需要检查是否存在元数据相关的错误，并在获取后清除该错误状态时调用。
     * 实现细节：返回当前的 metadataError，并将其重置为 Optional.empty()。
     * 设计考虑：提供一个原子操作来获取并清除错误，避免重复处理同一个错误。
     *
     * @return 包含元数据错误的 Optional<Exception>，如果不存在错误则为空 Optional。
     */
    public Optional<Exception> getAndClearMetadataError() {
        // 将当前的元数据错误赋值给局部变量 metadataError
        Optional<Exception> metadataError = this.metadataError;
        // 将成员变量 this.metadataError 重置为空，表示错误已被处理或获取
        this.metadataError = Optional.empty();
        // 返回之前存储的元数据错误
        return metadataError;
    }

    /**
     * 获取当前负载最小的节点。
     * 应用场景：当需要选择一个最优的节点发送请求时，例如在没有特定目标节点的情况下发送元数据请求。
     * 实现细节：调用 KafkaClient 的 leastLoadedNode 方法，并传入当前时间戳。
     * 设计考虑：将选择负载最小节点的逻辑委托给 KafkaClient，它内部维护了节点的连接状态和负载信息。
     *
     * @return 负载最小的 Kafka 节点；如果没有可用节点，则可能返回 null。
     */
    public Node leastLoadedNode() {
        // 调用 KafkaClient 的 leastLoadedNode 方法，传入当前时间（通过 time.milliseconds() 获取）
        // .node() 获取 ClientState 中封装的 Node 对象
        return this.client.leastLoadedNode(time.milliseconds()).node();
    }

    /**
     * 唤醒 KafkaClient。
     * 应用场景：当需要中断 KafkaClient 当前的阻塞操作（如 poll）时调用。
     * 实现细节：直接调用 KafkaClient 的 wakeup 方法。
     * 设计考虑：提供一个简单的接口来触发底层客户端的唤醒机制。
     */
    public void wakeup() {
        // 调用 KafkaClient 的 wakeup 方法，这将中断任何正在进行的网络 I/O 操作
        client.wakeup();
    }

    /**
     * 检查节点是否已断开连接并且无法立即重新连接（即，在断开连接后的重新连接退避窗口中）。
     * 应用场景：在尝试向某个节点发送请求之前，判断该节点是否因为之前的连接失败而处于退避状态。
     * 实现细节：
     * 1. 调用 `client.connectionFailed(node)` 检查与该节点的连接是否失败过。
     * 2. 调用 `client.connectionDelay(node, time.milliseconds())` 获取到该节点的连接退避时间。
     * 3. 如果连接失败过且退避时间大于0，则认为节点当前不可用。
     * 设计考虑：这个方法封装了判断节点是否因退避而不可用的逻辑，使得调用方可以简单地判断节点状态。
     *
     * @param node 要检查的节点。
     * @return 如果节点不可用（处于重新连接退避窗口），则返回 true；否则返回 false。
     */
    public boolean nodeUnavailable(final Node node) {
        // 检查与指定节点的连接是否失败 (client.connectionFailed(node))
        // 并且，到该节点的连接延迟（退避时间）是否大于0 (client.connectionDelay(node, time.milliseconds()) > 0)
        // 如果两者都为 true，则表示该节点当前不可用
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    /**
     * 关闭 NetworkClientDelegate。
     * 应用场景：当消费者客户端关闭时，需要关闭底层的 KafkaClient 以释放网络资源。
     * 实现细节：直接调用 KafkaClient 的 close 方法。
     * 设计考虑：实现 AutoCloseable 接口，方便使用 try-with-resources 语句管理资源。
     *
     * @throws IOException 如果关闭过程中发生 I/O 错误。
     */
    public void close() throws IOException {
        // 调用 KafkaClient 的 close 方法，关闭网络连接并释放相关资源
        this.client.close();
    }

    /**
     * 将 {@link PollResult} 中的所有未发送请求添加到待发送队列，并返回下次轮询的时间间隔。
     * 应用场景：当上层模块（如 RequestManager）轮询后产生了一批待发送的请求和下次轮询的建议时间时调用。
     * 实现细节：
     * 1. 校验 `pollResult` 不为 null。
     * 2. 调用另一个 `addAll` 方法将 `pollResult` 中的 `unsentRequests` 添加到内部队列。
     * 3. 返回 `pollResult` 中的 `timeUntilNextPollMs`。
     * 设计考虑：提供一个便捷的方法来处理 `PollResult` 对象，简化调用方的逻辑。
     *
     * @param pollResult 包含未发送请求和下次轮询时间的轮询结果对象。
     * @return 下次轮询前需要等待的时间（毫秒）。
     */
    public long addAll(PollResult pollResult) {
        // 确保 pollResult 不为 null，否则抛出 NullPointerException
        Objects.requireNonNull(pollResult);
        // 调用重载的 addAll 方法，将 pollResult 中的未发送请求列表添加到待发送队列
        addAll(pollResult.unsentRequests);
        // 返回 pollResult 中指定的下次轮询前需要等待的时间
        return pollResult.timeUntilNextPollMs;
    }

    /**
     * 将一批未发送的请求添加到待发送队列。
     * 应用场景：当有多个 {@link UnsentRequest} 需要被发送时，可以一次性将它们全部加入队列。
     * 实现细节：
     * 1. 校验 `requests` 列表不为 null。
     * 2. 如果列表不为空，则遍历列表，对每个请求调用 `add` 方法。
     * 设计考虑：提供批量添加的接口，避免多次调用 `add` 方法的开销和复杂性。
     *
     * @param requests 包含多个未发送请求的列表。
     */
    public void addAll(final List<UnsentRequest> requests) {
        // 确保 requests 列表不为 null，否则抛出 NullPointerException
        Objects.requireNonNull(requests);
        // 如果请求列表不为空
        if (!requests.isEmpty()) {
            // 遍历请求列表中的每个请求，并调用 add 方法将其添加到待发送队列
            requests.forEach(this::add);
        }
    }

    /**
     * 将单个未发送的请求添加到待发送队列。
     * 应用场景：当有一个 {@link UnsentRequest} 需要被发送时调用。
     * 实现细节：
     * 1. 校验请求 `r` 不为 null。
     * 2. 为请求 `r` 设置计时器，使用当前的 `time` 对象和配置的 `requestTimeoutMs`。
     * 3. 设置请求 `r` 的入队时间为当前时间戳。
     * 4. 将请求 `r` 添加到 `unsentRequests` 队列中。
     * 设计考虑：这是添加请求的基本方法，负责初始化请求的超时和入队时间等元信息。
     *
     * @param r 要添加的未发送请求对象。
     */
    public void add(final UnsentRequest r) {
        // 确保请求 r 不为 null，否则抛出 NullPointerException
        Objects.requireNonNull(r);
        // 为请求 r 设置计时器，使用当前的时间工具 this.time 和请求超时时间 this.requestTimeoutMs
        r.setTimer(this.time, this.requestTimeoutMs);
        // 设置请求 r 的入队时间为当前时间戳
        r.setEnqueueTimeMs(time.milliseconds());
        // 将请求 r 添加到未发送请求队列 unsentRequests 中
        unsentRequests.add(r);
    }

    /**
     * 表示 {@link RequestManager#poll(long)} 方法返回的结果的静态内部类。
     * 它封装了下次轮询前需要等待的时间以及一批需要发送的请求。
     * 应用场景：作为 RequestManager 轮询操作的统一返回类型，方便上层模块（如 NetworkClientDelegate）处理。
     * 设计考虑：将轮询结果的两个核心信息（等待时间和待发送请求）聚合在一起，提高代码可读性和可维护性。
     */
    public static class PollResult {
        /**
         * 表示一个常量，指示轮询操作应无限期等待，直到有事件发生。
         * 应用场景：当 RequestManager 希望 NetworkClientDelegate 在没有请求时阻塞等待，而不是立即返回。
         */
        public static final long WAIT_FOREVER = Long.MAX_VALUE;
        /**
         * 表示一个空的轮询结果，指示没有待发送的请求，并且应无限期等待下次轮询。
         * 应用场景：当 RequestManager 轮询后发现没有新的请求需要发送，且没有明确的下次轮询时间时，可以返回此实例。
         */
        public static final PollResult EMPTY = new PollResult(WAIT_FOREVER);
        /**
         * 下次轮询前需要等待的时间（毫秒）。
         * 应用场景：指示 NetworkClientDelegate 在处理完当前批次的请求后，应等待多久再进行下一次轮询。
         * 如果为 {@link #WAIT_FOREVER}，则表示无限期等待。
         */
        public final long timeUntilNextPollMs;
        /**
         * 本次轮询产生的一批未发送的请求。
         * 应用场景：包含所有需要通过 NetworkClientDelegate 发送出去的请求。
         * 该列表是不可修改的，以确保其内容在创建后不会被意外更改。
         */
        public final List<UnsentRequest> unsentRequests;

        /**
         * PollResult 的构造函数。
         * 应用场景：创建一个包含下次轮询时间和待发送请求列表的 PollResult 实例。
         * 实现细节：将传入的 `unsentRequests` 列表包装成不可修改的列表，以防止后续意外修改。
         * 设计考虑：提供一个标准的构造方式，并确保 `unsentRequests` 的不可变性。
         *
         * @param timeUntilNextPollMs 下次轮询前需要等待的时间（毫秒）。
         * @param unsentRequests 未发送的请求列表。
         */
        public PollResult(final long timeUntilNextPollMs, final List<UnsentRequest> unsentRequests) {
            // 初始化下次轮询前需要等待的时间
            this.timeUntilNextPollMs = timeUntilNextPollMs;
            // 初始化未发送的请求列表，并使用 Collections.unmodifiableList 包装以确保其不可修改
            this.unsentRequests = Collections.unmodifiableList(unsentRequests);
        }

        /**
         * PollResult 的构造函数，使用默认的 {@link #WAIT_FOREVER} 作为下次轮询时间。
         * 应用场景：当只关心待发送的请求列表，而下次轮询时间不确定或希望无限期等待时使用。
         * 实现细节：调用另一个构造函数，将 `timeUntilNextPollMs` 设置为 `WAIT_FOREVER`。
         * 设计考虑：提供一个便捷的构造函数，简化仅有请求列表时的创建过程。
         *
         * @param unsentRequests 未发送的请求列表。
         */
        public PollResult(final List<UnsentRequest> unsentRequests) {
            // 调用另一个构造函数，将下次轮询时间设置为 WAIT_FOREVER，并传入未发送的请求列表
            this(WAIT_FOREVER, unsentRequests);
        }

        /**
         * PollResult 的构造函数，用于单个未发送的请求，并使用默认的 {@link #WAIT_FOREVER} 作为下次轮询时间。
         * 应用场景：当只有一个待发送的请求时使用。
         * 实现细节：将单个请求包装成一个只包含该请求的列表，然后调用另一个构造函数。
         * 设计考虑：为处理单个请求的场景提供便利。
         *
         * @param unsentRequest 单个未发送的请求。
         */
        public PollResult(final UnsentRequest unsentRequest) {
            // 调用另一个构造函数，将单个未发送的请求包装成一个列表 (Collections.singletonList)
            // 并将下次轮询时间设置为 WAIT_FOREVER (通过调用 this(List) 间接实现)
            this(Collections.singletonList(unsentRequest));
        }

        /**
         * PollResult 的构造函数，用于指定下次轮询时间，但没有待发送的请求。
         * 应用场景：当没有请求需要立即发送，但希望在指定时间后再次轮询时使用。
         * 实现细节：调用另一个构造函数，将 `unsentRequests` 设置为空列表。
         * 设计考虑：提供一个构造函数，用于只需要指定下次轮询时间的场景。
         *
         * @param timeUntilNextPollMs 下次轮询前需要等待的时间（毫秒）。
         */
        public PollResult(final long timeUntilNextPollMs) {
            // 调用另一个构造函数，传入指定的下次轮询时间，并将未发送的请求列表设置为空列表 (Collections.emptyList())
            this(timeUntilNextPollMs, Collections.emptyList());
        }
    }

    /**
     * 代表一个尚未发送的请求。
     * 应用场景：当需要异步发送请求时，可以将请求封装成 UnsentRequest 对象，放入待发送队列中。
     * 实现细节：包含请求构建器、响应处理器、目标节点等信息。
     * 设计考虑：将请求的元数据和处理逻辑封装在一起，方便管理和跟踪请求状态。
     */
    public static class UnsentRequest {
        /**
         * 请求构建器，用于构建实际的 Kafka 请求。
         * 应用场景：在发送请求前，需要使用此构建器创建具体的请求对象。
         * 实现细节：这是一个泛型字段，可以构建不同类型的请求。
         * 设计考虑：使用构建器模式，可以灵活地配置请求参数。
         */
        private final AbstractRequest.Builder<?> requestBuilder;
        /**
         * Future 完成处理器，用于处理请求的响应或异常。
         * 应用场景：当请求完成后，通过此处理器获取响应结果或处理发生的错误。
         * 实现细节：内部包含一个 CompletableFuture，用于异步获取结果。
         * 设计考虑：使用 CompletableFuture 可以方便地进行异步编程和回调处理。
         */
        private final FutureCompletionHandler handler;
        /**
         * 目标节点，表示请求将发送到哪个 Kafka 节点。
         * 如果为空，则表示可以选择任意一个可用的节点。
         * 应用场景：指定请求的目标 broker，或者在不关心特定 broker 时允许随机选择。
         * 实现细节：使用 Optional 类型，可以清晰地表示节点是否已指定。
         * 设计考虑：提供灵活性，允许指定目标节点或由客户端自动选择。
         */
        private final Optional<Node> node; // 如果为空，则表示可以选择随机节点

        /**
         * 计时器，用于跟踪请求的超时。
         * 应用场景：确保请求在指定的时间内得到响应，否则视为超时。
         * 实现细节：在请求发送前设置，并在轮询过程中检查是否超时。
         * 设计考虑：通过计时器管理请求的生命周期，避免无限等待。
         */
        private Timer timer;
        /**
         * 请求加入 unsentRequests 队列的时间戳（毫秒）。
         * 注意：这不是请求在队列中的持续时间。
         * 应用场景：用于记录请求的入队时间，可能用于监控或调试。
         * 实现细节：在请求被添加到待发送队列时设置。
         * 设计考虑：提供一个时间戳，用于分析请求的处理流程。
         */
        private long enqueueTimeMs; // 请求被加入 unsentRequests 队列的时间，而不是在队列中的持续时间。

        /**
         * UnsentRequest 的构造函数。
         * 应用场景：创建一个新的未发送请求对象。
         * 实现细节：初始化请求构建器、目标节点和响应处理器。
         * 设计考虑：通过构造函数传入必要的参数，确保对象在创建时处于有效状态。
         *
         * @param requestBuilder 请求构建器，不能为空。
         * @param node 目标节点，可以为空（表示随机选择节点）。
         */
        public UnsentRequest(final AbstractRequest.Builder<?> requestBuilder, // 请求构建器实例
                             final Optional<Node> node) { // 目标节点，可能为空
            // 检查 requestBuilder 是否为 null，如果是则抛出 NullPointerException
            Objects.requireNonNull(requestBuilder);
            // 初始化请求构建器字段
            this.requestBuilder = requestBuilder;
            // 初始化目标节点字段
            this.node = node;
            // 初始化 FutureCompletionHandler 字段，用于处理请求的完成状态
            this.handler = new FutureCompletionHandler();
        }

        /**
         * 设置请求的计时器。
         * 应用场景：在请求准备发送前，设置其超时计时器。
         * 实现细节：使用传入的 Time 对象和请求超时毫秒数创建并设置计时器。
         * 设计考虑：允许外部控制请求的超时时间。
         *
         * @param time Time 对象，用于创建计时器。
         * @param requestTimeoutMs 请求超时时间（毫秒）。
         */
        void setTimer(final Time time, final long requestTimeoutMs) {
            // 使用 time 对象和 requestTimeoutMs 创建一个新的计时器并赋值给 timer 字段
            this.timer = time.timer(requestTimeoutMs);
        }

        /**
         * 获取请求的计时器。
         * 应用场景：检查请求的超时状态。
         * 实现细节：返回内部的 timer 字段。
         * 设计考虑：提供对计时器的访问，以便外部可以查询其状态。
         *
         * @return 请求的计时器。
         */
        Timer timer() {
            // 返回 timer 字段
            return timer;
        }

        /**
         * 设置请求加入 {@link NetworkClientDelegate#unsentRequests} 队列的时间。
         * 应用场景：当请求被添加到待发送队列时调用此方法。
         * 实现细节：将传入的时间戳赋值给 enqueueTimeMs 字段。
         * 设计考虑：记录请求进入队列的精确时间。
         *
         * @param enqueueTimeMs 请求加入队列的时间戳（毫秒）。
         */
        private void setEnqueueTimeMs(final long enqueueTimeMs) {
            // 将传入的 enqueueTimeMs 赋值给 this.enqueueTimeMs 字段
            this.enqueueTimeMs = enqueueTimeMs;
        }

        /**
         * 返回请求加入 {@link NetworkClientDelegate#unsentRequests} 队列的时间。
         * 应用场景：获取请求入队的时间，用于分析或监控。
         * 实现细节：返回 enqueueTimeMs 字段的值。
         * 设计考虑：提供对入队时间的访问。
         *
         * @return 请求加入队列的时间戳（毫秒）。
         */
        private long enqueueTimeMs() {
            // 返回 enqueueTimeMs 字段的值
            return enqueueTimeMs;
        }

        /**
         * 获取与此请求关联的 CompletableFuture。
         * 应用场景：异步等待请求的完成并获取响应。
         * 实现细节：返回 handler 内部的 future 对象。
         * 设计考虑：通过 CompletableFuture 支持异步编程模型。
         *
         * @return 表示请求结果的 CompletableFuture。
         */
        CompletableFuture<ClientResponse> future() {
            // 返回 handler 对象中的 future 成员
            return handler.future;
        }

        /**
         * 获取请求的 FutureCompletionHandler。
         * 应用场景：需要直接操作响应处理器时使用。
         * 实现细节：返回 handler 字段。
         * 设计考虑：提供对底层响应处理器的访问。
         *
         * @return 请求的 FutureCompletionHandler。
         */
        FutureCompletionHandler handler() {
            // 返回 handler 字段
            return handler;
        }

        /**
         * 当请求完成时注册一个回调函数。
         * 应用场景：在请求处理完毕（成功或失败）后执行自定义逻辑。
         * 实现细节：调用 handler 内部 future 的 whenComplete 方法。
         * 设计考虑：提供链式调用，方便注册回调并返回自身。
         *
         * @param callback 当请求完成时执行的回调函数，接受 ClientResponse 和 Throwable 作为参数。
         * @return 当前 UnsentRequest 对象，支持链式调用。
         */
        UnsentRequest whenComplete(BiConsumer<ClientResponse, Throwable> callback) {
            // 在 handler.future 上注册一个完成时的回调
            handler.future().whenComplete(callback);
            // 返回当前 UnsentRequest 对象，以便进行链式调用
            return this;
        }

        /**
         * 获取请求构建器。
         * 应用场景：需要访问请求的构建参数或重新构建请求时使用。
         * 实现细节：返回 requestBuilder 字段。
         * 设计考虑：提供对请求构建器的访问。
         *
         * @return 请求构建器。
         */
        AbstractRequest.Builder<?> requestBuilder() {
            // 返回 requestBuilder 字段
            return requestBuilder;
        }

        /**
         * 获取目标节点。
         * 应用场景：确定请求将发送到哪个节点。
         * 实现细节：返回 node 字段。
         * 设计考虑：提供对目标节点信息的访问。
         *
         * @return 包含目标节点的 Optional 对象，如果未指定则为空。
         */
        Optional<Node> node() {
            // 返回 node 字段
            return node;
        }

        @Override
        public String toString() {
            String remainingMs;

            if (timer != null) {
                timer.update();
                remainingMs = String.valueOf(timer.remainingMs());
            } else {
                remainingMs = "<not set>";
            }

            return "UnsentRequest{" +
                    "requestBuilder=" + requestBuilder +
                    ", handler=" + handler +
                    ", node=" + node +
                    ", remainingMs=" + remainingMs +
                    '}';
        }
    }

    /**
     * FutureCompletionHandler 是一个实现了 RequestCompletionHandler 接口的静态内部类。
     * 应用场景：用于处理异步请求的完成，无论是成功还是失败，并将结果或异常设置到关联的 CompletableFuture 中。
     * 设计考虑：通过 CompletableFuture 提供了现代化的异步编程模型，使得调用方可以方便地处理异步操作的结果。
     */
    public static class FutureCompletionHandler implements RequestCompletionHandler {

        // 响应完成时间（毫秒）
        private long responseCompletionTimeMs;
        // 用于存储客户端响应的 CompletableFuture 对象，调用方可以通过此 future 获取异步操作的结果。
        private final CompletableFuture<ClientResponse> future;

        /**
         * FutureCompletionHandler 的构造函数。
         * 应用场景：在创建一个新的异步请求时，会创建一个 FutureCompletionHandler 实例来处理该请求的完成。
         * 实现细节：初始化一个 CompletableFuture 对象。
         */
        FutureCompletionHandler() {
            // 初始化 CompletableFuture，用于异步传递请求结果
            future = new CompletableFuture<>();
        }

        /**
         * 当请求失败时调用此方法。
         * 应用场景：网络错误、服务器错误或请求超时等导致请求无法成功完成时。
         * 实现细节：记录响应完成时间，并根据传入的异常 e 来完成 future。
         * 如果 e 为 null，则使用 DisconnectException.INSTANCE 来完成 future，表示连接断开。
         *
         * @param currentTimeMs 请求失败时的时间戳（毫秒）。
         * @param e 导致失败的运行时异常；如果是因为连接断开等非特定异常，则可能为 null。
         */
        public void onFailure(final long currentTimeMs, final RuntimeException e) {
            // 记录响应完成的时间戳
            this.responseCompletionTimeMs = currentTimeMs;
            // 检查传入的异常是否为 null
            if (e != null) {
                // 如果异常不为 null，则使用该异常使 future 异常完成
                this.future.completeExceptionally(e);
            } else {
                // 如果异常为 null（通常表示连接断开），则使用预定义的 DisconnectException 实例使 future 异常完成
                this.future.completeExceptionally(DisconnectException.INSTANCE);
            }
        }

        /**
         * 获取响应完成的时间（毫秒）。
         * 应用场景：用于记录或分析请求的处理耗时。
         *
         * @return 响应完成的时间戳（毫秒）。
         */
        public long completionTimeMs() {
            // 返回记录的响应完成时间
            return responseCompletionTimeMs;
        }

        /**
         * 当请求成功完成时调用此方法。
         * 应用场景：当 Kafka 客户端成功接收到来自服务器的响应时。
         * 实现细节：
         * 1. 获取响应接收时间。
         * 2. 检查响应中是否包含认证异常、断开连接标志或版本不匹配错误，如果存在任一情况，则调用 onFailure 处理。
         * 3. 如果响应正常，则记录响应完成时间，并使用该响应完成 future。
         *
         * @param response 客户端接收到的响应对象。
         */
        @Override
        public void onComplete(final ClientResponse response) {
            // 获取响应的接收时间戳
            long completionTimeMs = response.receivedTimeMs();
            // 检查响应中是否包含认证异常
            if (response.authenticationException() != null) {
                // 如果存在认证异常，则调用 onFailure 方法，并传入认证异常
                onFailure(completionTimeMs, response.authenticationException());
            // 检查响应是否表示连接已断开
            } else if (response.wasDisconnected()) {
                // 如果连接已断开，则调用 onFailure 方法，并传入 DisconnectException 实例
                onFailure(completionTimeMs, DisconnectException.INSTANCE);
            // 检查响应中是否包含版本不匹配错误
            } else if (response.versionMismatch() != null) {
                // 如果存在版本不匹配错误，则调用 onFailure 方法，并传入版本不匹配异常
                onFailure(completionTimeMs, response.versionMismatch());
            } else {
                // 如果响应正常，没有上述错误
                // 记录响应完成时间
                responseCompletionTimeMs = completionTimeMs;
                // 使用正常的响应使 future 完成
                this.future.complete(response);
            }
        }

        /**
         * 获取与此处理器关联的 CompletableFuture。
         * 应用场景：调用方使用此方法获取 future 对象，以便异步地等待和处理请求结果。
         *
         * @return 关联的 {@link CompletableFuture<ClientResponse>} 对象。
         */
        public CompletableFuture<ClientResponse> future() {
            // 返回持有的 CompletableFuture 实例
            return future;
        }
    }

    /**
     * 创建一个 {@link Supplier}，用于在 {@link ConsumerNetworkThread} 调用期间延迟创建 NetworkClientDelegate。
     * 应用场景：当需要延迟初始化 NetworkClientDelegate 实例时使用，例如在消费者网络线程的上下文中创建，以确保正确的线程局部性或资源管理。
     * 设计考虑：使用 Supplier 模式可以推迟对象的创建，直到实际需要时才进行，这有助于优化资源使用和启动时间。
     *           CachedSupplier 进一步优化，确保 create 方法只被调用一次，返回缓存的实例。
     *
     * @param time 时间工具。
     * @param logContext 日志上下文。
     * @param metadata 消费者元数据。
     * @param config 消费者配置。
     * @param apiVersions API 版本信息。
     * @param metrics 指标收集器。
     * @param throttleTimeSensor 节流时间传感器。
     * @param clientTelemetrySender 客户端遥测数据发送器。
     * @param backgroundEventHandler 后台事件处理器。
     * @param notifyMetadataErrorsViaErrorQueue 是否通过错误队列通知元数据错误。
     * @param asyncConsumerMetrics 异步消费者指标。
     * @return 一个 {@link Supplier<NetworkClientDelegate>} 实例，用于创建 NetworkClientDelegate。
     */
    public static Supplier<NetworkClientDelegate> supplier(final Time time, // 时间工具实例
                                                           final LogContext logContext, // 日志上下文实例
                                                           final ConsumerMetadata metadata, // 消费者元数据实例
                                                           final ConsumerConfig config, // 消费者配置实例
                                                           final ApiVersions apiVersions, // API 版本信息实例
                                                           final Metrics metrics, // 指标收集器实例
                                                           final Sensor throttleTimeSensor, // 节流时间传感器实例
                                                           final ClientTelemetrySender clientTelemetrySender, // 客户端遥测数据发送器实例
                                                           final BackgroundEventHandler backgroundEventHandler, // 后台事件处理器实例
                                                           final boolean notifyMetadataErrorsViaErrorQueue, // 是否通过错误队列通知元数据错误的标志
                                                           final AsyncConsumerMetrics asyncConsumerMetrics) { // 异步消费者指标实例
        // 返回一个新的 CachedSupplier 实例，它是一个 Supplier 的实现
        return new CachedSupplier<>() {
            /**
             * 创建 NetworkClientDelegate 实例。
             * 应用场景：当 Supplier 的 get() 方法首次被调用时，此方法会被执行以创建 NetworkClientDelegate。
             * 实现细节：
             * 1. 使用 ClientUtils.createNetworkClient 创建一个 KafkaClient 实例。
             * 2. 使用创建的 KafkaClient 和其他传入的参数构造一个新的 NetworkClientDelegate 实例。
             * 设计考虑：将 NetworkClientDelegate 的创建逻辑封装在此方法中，使得 supplier 方法更简洁。
             *
             * @return 新创建的 NetworkClientDelegate 实例。
             */
            @Override
            protected NetworkClientDelegate create() {
                // 使用 ClientUtils 工具类创建一个 KafkaClient 实例
                KafkaClient client = ClientUtils.createNetworkClient(config, // 消费者配置
                        metrics, // 指标收集器
                        CONSUMER_METRIC_GROUP_PREFIX, // 消费者指标组前缀
                        logContext, // 日志上下文
                        apiVersions, // API 版本信息
                        time, // 时间工具
                        CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION, // 每个连接的最大未完成请求数
                        metadata, // 消费者元数据
                        throttleTimeSensor, // 节流时间传感器
                        clientTelemetrySender); // 客户端遥测数据发送器
                // 使用创建的 KafkaClient 和其他参数，构造并返回一个新的 NetworkClientDelegate 实例
                return new NetworkClientDelegate(time, config, logContext, client, metadata, backgroundEventHandler, notifyMetadataErrorsViaErrorQueue, asyncConsumerMetrics);
            }
        };
    }
}
