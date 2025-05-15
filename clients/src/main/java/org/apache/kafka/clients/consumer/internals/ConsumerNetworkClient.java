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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 消费者对网络层的高级访问接口，提供对请求 Future 的基本支持。此类是线程安全的，
 * 但不为响应回调提供同步。这保证了在调用回调时不会持有锁。
 * 应用场景：作为 Kafka 消费者与 Broker 通信的核心组件，封装了请求发送、响应处理、连接管理等网络操作。
 * 实现细节：通过 KafkaClient 进行底层的网络通信，使用 RequestFuture 来异步处理请求结果。
 * 设计考虑：线程安全是核心考虑点，通过内部锁和原子变量保证并发访问的正确性。响应回调的无锁执行是为了避免死锁。
 */
public class ConsumerNetworkClient implements Closeable { // 定义 ConsumerNetworkClient 类，实现 Closeable 接口，表示该资源可以被关闭
    // 最大轮询超时时间（毫秒）
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // 此类的可变状态受对象监视器保护（不包括下面的 wakeup 标志和请求完成队列）。
    // 日志记录器，用于记录操作和错误信息
    private final Logger log;
    // Kafka 客户端，用于实际的网络通信
    private final KafkaClient client;
    // 未发送的请求队列，用于暂存待发送的请求
    private final UnsentRequests unsent = new UnsentRequests();
    // 元数据，存储 Kafka 集群的元数据信息
    private final Metadata metadata;
    // 时间工具，用于获取当前时间等
    private final Time time;
    // 重试退避时间（毫秒），请求失败后的重试间隔
    private final long retryBackoffMs;
    // 最大轮询超时时间（毫秒），poll 操作的最大阻塞时间
    private final int maxPollTimeoutMs;
    // 请求超时时间（毫秒），发送请求后等待响应的超时时间
    private final int requestTimeoutMs;
    // 原子布尔值，指示唤醒功能是否被禁用
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    // 我们不需要高吞吐量，因此使用公平锁以避免饥饿。
    // 可重入锁，用于保护共享资源，确保线程安全，true 表示公平锁
    private final ReentrantLock lock = new ReentrantLock(true);

    // 当请求完成时，它们会在调用之前传输到此队列。目的是避免在持有此对象的监视器时调用它们，这可能会导致死锁。
    // 待处理的完成回调队列，用于存储已完成请求的回调处理器，避免在持有锁时执行回调
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    // 待断开连接的节点队列
    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // 此标志允许客户端安全地被唤醒，而无需等待上面的锁。它是原子的，以避免需要获取上面的锁才能并发启用它。
    // 原子布尔值，用于唤醒客户端，避免在 poll 操作中长时间阻塞
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    /**
     * ConsumerNetworkClient 的构造函数。
     * 应用场景：在创建 KafkaConsumer 实例时，会间接创建 ConsumerNetworkClient 用于网络通信。
     * 实现细节：初始化 ConsumerNetworkClient 的各个字段，包括日志记录器、KafkaClient、元数据、时间工具以及各种超时和退避参数。
     * 设计考虑：通过构造函数注入依赖项，使得 ConsumerNetworkClient 的配置更加灵活。
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param client Kafka 客户端实例，负责底层网络通信
     * @param metadata Kafka 集群元数据信息
     * @param time 时间工具类，用于获取当前时间等
     * @param retryBackoffMs 请求失败后的重试退避时间（毫秒）
     * @param requestTimeoutMs 发送请求后等待响应的默认超时时间（毫秒）
     * @param maxPollTimeoutMs poll 操作的最大阻塞时间（毫秒）
     */
    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 int requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        // 初始化日志记录器，使用传入的 logContext 和当前类名
        this.log = logContext.logger(ConsumerNetworkClient.class);
        // 设置 KafkaClient 实例
        this.client = client;
        // 设置 Metadata 实例
        this.metadata = metadata;
        // 设置 Time 实例
        this.time = time;
        // 设置重试退避时间
        this.retryBackoffMs = retryBackoffMs;
        // 设置最大轮询超时时间，取传入值和静态最大值中的较小者，防止设置过大
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        // 设置默认请求超时时间
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /**
     * 获取默认的请求超时时间（毫秒）。
     * 应用场景：当发送请求时未指定超时时间，则使用此默认值。
     * 实现细节：直接返回 requestTimeoutMs 字段的值。
     * 设计考虑：提供一个便捷的方法获取配置的默认超时时间。
     * @return 默认请求超时时间（毫秒）
     */
    public int defaultRequestTimeoutMs() {
        // 返回构造函数中设置的请求超时时间
        return requestTimeoutMs;
    }

    /**
     * 使用默认超时时间发送请求。参见 {@link #send(Node, AbstractRequest.Builder, int)}。
     * 应用场景：当调用者希望使用配置的默认超时时间发送请求时使用此方法。
     * 实现细节：内部调用另一个 send 方法，并传入 requestTimeoutMs 作为超时参数。
     * 设计考虑：提供一个重载方法，简化使用默认超时时间的调用。
     *
     * @param node 请求的目标节点
     * @param requestBuilder 请求的构建器
     * @return 表示发送结果的 Future 对象
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        // 调用另一个 send 方法，使用成员变量 requestTimeoutMs 作为超时时间
        return send(node, requestBuilder, requestTimeoutMs);
    }

    /**
     * 发送一个新请求。请注意，在调用 {@link #poll(Timer)} 的某个变体之前，请求实际上不会在网络上传输。
     * 此时，请求要么成功传输，要么失败。
     * 使用返回的 future 来获取发送的结果。请注意，无需在 {@link ClientResponse} 对象上显式检查断开连接；
     * 相反，future 将因 {@link DisconnectException} 而失败。
     * 应用场景：消费者需要向 Broker 发送各种请求（如 FetchRequest, HeartbeatRequest 等）时调用此方法。
     * 实现细节：
     * 1. 获取当前时间。
     * 2. 创建一个 RequestFutureCompletionHandler 用于处理请求完成后的回调。
     * 3. 使用 KafkaClient 创建一个 ClientRequest 对象，包含了目标节点、请求构建器、当前时间、是否需要响应、请求超时时间和完成处理器。
     * 4. 将创建的 ClientRequest 放入 unsent 队列中，等待 poll 方法将其发送出去。
     * 5. 唤醒 KafkaClient，以防其在 poll 操作中阻塞，确保新加入的请求能被及时处理和发送。
     * 6. 返回 RequestFutureCompletionHandler 中的 future 对象，调用者可以通过此 future 异步获取请求结果。
     * 设计考虑：
     * - 异步发送：请求的实际发送是在 poll 方法中进行的，send 方法只是将请求加入队列，实现了异步化。
     * - Future 模式：通过返回 Future 对象，调用者可以非阻塞地等待请求结果。
     * - 超时控制：允许为每个请求指定超时时间。
     * - 错误处理：连接断开等网络问题会通过 Future 的失败状态体现。
     *
     * @param node 请求的目标节点
     * @param requestBuilder 请求内容的构建器
     * @param requestTimeoutMs 等待响应的最大时间（毫秒），超时则断开套接字并取消请求。如果套接字因任何原因断开，请求可能会更早被取消。
     * @return 一个 future，指示发送的结果。
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              AbstractRequest.Builder<?> requestBuilder,
                                              int requestTimeoutMs) {
        // 获取当前时间戳
        long now = time.milliseconds();
        // 创建请求完成处理器，用于异步处理响应
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        // 创建客户端请求对象
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
            requestTimeoutMs, completionHandler);
        // 将请求放入未发送队列
        unsent.put(node, clientRequest);

        // 唤醒客户端，以防其在 poll 中阻塞，以便我们可以发送排队的请求
        client.wakeup();
        // 返回与此请求关联的 Future
        return completionHandler.future;
    }

    /**
     * 获取当前负载最小的节点。
     * 应用场景：当需要选择一个 Broker 发送请求（通常是元数据请求或不针对特定分区的请求）时，可以选择负载最小的节点以实现负载均衡。
     * 实现细节：
     * 1. 获取锁，保护对 KafkaClient 内部状态的访问。
     * 2. 调用 KafkaClient 的 leastLoadedNode 方法获取负载最小的节点信息。
     * 3. 返回获取到的节点。
     * 4. 在 finally 块中释放锁，确保锁总是被释放。
     * 设计考虑：通过锁保证线程安全，选择负载最小的节点有助于提高系统整体性能和稳定性。
     * @return 负载最小的节点；如果没有可用节点，则可能返回 null 或抛出异常，具体取决于 KafkaClient 的实现。
     */
    public Node leastLoadedNode() {
        // 加锁以保护对 client 状态的并发访问
        lock.lock();
        try {
            // 调用 KafkaClient 的 leastLoadedNode 方法获取当前时间下负载最小的节点
            // .node() 方法从 ClientState（通常是 leastLoadedNode 返回的类型）中提取 Node 对象
            return client.leastLoadedNode(time.milliseconds()).node();
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }
    }

    /**
     * 检查在给定的时间点是否有可用的（已连接且未处于退避状态的）节点。
     * 应用场景：在尝试发送请求前，可以调用此方法检查网络连接状况，避免向不可用的节点发送请求。
     * 实现细节：
     * 1. 获取锁，保护对 KafkaClient 内部状态的访问。
     * 2. 调用 KafkaClient 的 hasReadyNodes 方法检查是否有就绪节点。
     * 3. 返回检查结果。
     * 4. 在 finally 块中释放锁，确保锁总是被释放。
     * 设计考虑：通过锁保证线程安全，提供一种快速检查网络连接状态的方式。
     * @param now 当前时间戳（毫秒）
     * @return 如果至少有一个节点已准备好接收请求，则返回 true；否则返回 false。
     */
    public boolean hasReadyNodes(long now) {
        // 加锁以保护对 client 状态的并发访问
        lock.lock();
        try {
            // 调用 KafkaClient 的 hasReadyNodes 方法检查在当前时间是否有就绪的节点
            return client.hasReadyNodes(now);
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }
    }

    /**
     * 在超时时间内阻塞等待元数据刷新。
     * 应用场景：当消费者需要最新的集群元数据（例如，发现新的分区或 Broker）时，会调用此方法。
     * 实现细节：
     * 1. 请求元数据更新，并获取当前元数据版本号。
     * 2. 在一个循环中调用 poll 方法，该方法会尝试发送待处理的请求（可能包括元数据请求）并处理网络IO。
     * 3. 循环条件是元数据版本未改变且计时器未超时。
     * 4. 如果元数据版本在超时前发生变化，则表示更新成功。
     * 设计考虑：通过版本号来判断元数据是否已更新，避免了复杂的比较逻辑。使用 Timer 控制超时，防止无限期阻塞。
     *
     * @param timer 计时器，用于控制等待的超时时间
     * @return 如果更新成功则返回 true，否则返回 false。
     */
    public boolean awaitMetadataUpdate(Timer timer) {
        // 请求元数据更新，但不强制立即发送请求，返回当前的元数据版本号
        int version = this.metadata.requestUpdate(false);
        // 循环直到元数据版本发生变化或计时器超时
        do {
            // 调用 poll 方法处理网络IO和请求，这可能会触发元数据更新
            poll(timer);
            // 检查元数据版本是否已更新，以及计时器是否尚未过期
        } while (this.metadata.updateVersion() == version && timer.notExpired());
        // 如果元数据版本大于初始版本，则表示更新成功
        return this.metadata.updateVersion() > version;
    }

    /**
     * 确保我们的元数据是最新的（如果期望更新，此方法将阻塞直到更新完成）。
     * 应用场景：在执行依赖最新元数据的操作（如分配分区）之前，调用此方法确保元数据是最新的。
     * 实现细节：
     * 1. 检查元数据是否已请求更新，或者是否已到达下一次更新的时间。
     * 2. 如果需要更新，则调用 awaitMetadataUpdate 方法阻塞等待更新完成。
     * 3. 如果元数据已经是新的，则直接返回 true。
     * 设计考虑：通过检查更新请求标志和下次更新时间，避免不必要的阻塞等待。
     *
     * @param timer 计时器，用于控制等待的超时时间
     * @return 如果元数据已是最新或成功更新，则返回 true；否则返回 false
     */
    boolean ensureFreshMetadata(Timer timer) {
        // 检查元数据是否已请求更新，或者根据当前时间判断是否需要立即更新元数据
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0) {
            // 如果需要更新，则调用 awaitMetadataUpdate 方法等待元数据更新完成
            return awaitMetadataUpdate(timer);
        } else {
            // 元数据已经是新的，无需更新
            return true;
        }
    }

    /**
     * 唤醒一个活动的 poll 操作。这将导致轮询线程在当前 poll（如果活动）或下一个 poll 时抛出异常。
     * 应用场景：当消费者在另一个线程中被关闭或需要中断时，可以调用此方法来唤醒阻塞的 poll 操作。
     * 实现细节：
     * 1. 设置 wakeup 标志为 true。
     * 2. 调用底层 KafkaClient 的 wakeup 方法，这通常会中断 Selector 的 select 操作。
     * 设计考虑：wakeup 操作需要是线程安全的，因为它通常从不同于轮询线程的线程调用。通过原子变量和底层客户端的线程安全 wakeup 实现这一点。
     */
    public void wakeup() {
        // wakeup 在不持有客户端锁的情况下应该是安全的，因为它只是委托给 Selector 的 wakeup，而 Selector 的 wakeup 是线程安全的
        // 记录收到用户唤醒的调试信息
        log.debug("Received user wakeup");
        // 将 wakeup 原子标志设置为 true，以便 poll 方法可以检测到唤醒请求
        this.wakeup.set(true);
        // 调用底层 KafkaClient 的 wakeup 方法，这将中断网络选择器的操作
        this.client.wakeup();
    }

    /**
     * 无限期阻塞，直到给定的请求 future 完成。
     * 应用场景：当需要同步等待某个特定请求完成时使用，例如发送一个关键请求后必须等待其结果。
     * 实现细节：
     * 1. 创建一个最大超时时间的计时器 (Long.MAX_VALUE)。
     * 2. 在一个循环中调用另一个 poll 方法，传入该计时器和 future。
     * 3. 循环直到 future 完成。
     * 设计考虑：提供一个便捷的方法来无限期等待 future，底层依赖带有计时器的 poll 方法。
     *
     * @param future 要等待的请求 future。
     * @throws WakeupException 如果从另一个线程调用了 {@link #wakeup()}
     * @throws InterruptException 如果调用线程被中断
     */
    public void poll(RequestFuture<?> future) {
        // 循环直到 future 完成
        while (!future.isDone())
            // 调用另一个 poll 方法，使用一个永不超时的计时器 (Long.MAX_VALUE) 和给定的 future
            poll(time.timer(Long.MAX_VALUE), future);
    }

    /**
     * 阻塞直到提供的请求 future 完成或超时已过。
     * 应用场景：当需要等待特定请求在一定时间内完成时使用。
     * 实现细节：调用另一个重载的 poll 方法，并将 disableWakeup 设置为 false。
     * 设计考虑：提供一个默认行为（即不禁用唤醒）的便捷方法。
     *
     * @param future 要等待的请求 future
     * @param timer 限制此方法阻塞时间的计时器
     * @return 如果 future 完成则返回 true，否则返回 false
     * @throws WakeupException 如果从另一个线程调用了 {@link #wakeup()}
     * @throws InterruptException 如果调用线程被中断
     */
    public boolean poll(RequestFuture<?> future, Timer timer) {
        // 调用另一个 poll 方法，传入 future、timer，并将 disableWakeup 设置为 false（即允许唤醒）
        return poll(future, timer, false);
    }

    /**
     * 阻塞直到提供的请求 future 完成或超时已过。
     * 应用场景：这是等待特定请求完成的核心逻辑之一，允许控制是否检查唤醒。
     * 实现细节：
     * 1. 在一个 do-while 循环中调用另一个 poll 方法（该方法处理实际的网络IO和请求发送/接收）。
     * 2. 循环条件是 future 尚未完成且计时器未超时。
     * 3. 返回 future 是否完成。
     * 设计考虑：通过循环调用 poll(Timer, PollCondition, boolean) 来驱动网络事件处理，直到目标 future 完成或超时。
     *
     * @param future 要等待的请求 future
     * @param timer 限制此方法阻塞时间的计时器
     * @param disableWakeup 如果为 true，则不检查唤醒，否则检查
     *
     * @return 如果 future 完成则返回 true，否则返回 false
     * @throws WakeupException 如果从另一个线程调用了 {@link #wakeup()} 并且 `disableWakeup` 为 false
     * @throws InterruptException 如果调用线程被中断
     */
    public boolean poll(RequestFuture<?> future, Timer timer, boolean disableWakeup) {
        // 循环直到 future 完成或计时器超时
        do {
            // 调用核心的 poll 方法，传入计时器、future 作为 PollCondition，以及 disableWakeup 标志
            // 这个 poll 方法会处理网络IO，并可能完成 future
            poll(timer, future, disableWakeup);
            // 检查 future 是否已完成以及计时器是否尚未过期
        } while (!future.isDone() && timer.notExpired());
        // 返回 future 是否已完成
        return future.isDone();
    }

    /**
     * 轮询任何网络 IO。
     * 应用场景：消费者主循环中调用此方法来发送待处理请求、接收响应、处理连接事件等。
     * 实现细节：调用另一个重载的 poll 方法，并将 pollCondition 设置为 null。
     * 设计考虑：提供一个简单的 poll 接口，不带特定的阻塞条件。
     *
     * @param timer 限制此方法阻塞时间的计时器
     * @throws WakeupException 如果从另一个线程调用了 {@link #wakeup()}
     * @throws InterruptException 如果调用线程被中断
     */
    public void poll(Timer timer) {
        // 调用另一个 poll 方法，传入计时器和 null 作为 PollCondition（表示没有特定的外部阻塞条件）
        poll(timer, null);
    }

    /**
     * 轮询任何网络 IO。
     * 应用场景：消费者主循环中调用此方法，可以提供一个 PollCondition 来决定是否应该阻塞等待。
     * 实现细节：调用另一个重载的 poll 方法，并将 disableWakeup 设置为 false。
     * 设计考虑：允许调用者通过 PollCondition 控制 poll 的阻塞行为。
     *
     * @param timer 限制此方法阻塞时间的计时器
     * @param pollCondition 可为 null 的阻塞条件
     */
    public void poll(Timer timer, PollCondition pollCondition) {
        // 调用核心的 poll 方法，传入计时器、pollCondition，并将 disableWakeup 设置为 false（即允许唤醒）
        poll(timer, pollCondition, false);
    }

    /**
     * 轮询任何网络 IO。
     * 这是 ConsumerNetworkClient 中最核心的轮询方法，负责驱动所有网络活动和请求处理。
     * 应用场景：被其他 poll 方法以及消费者主循环间接调用，是消费者与 Kafka Broker 交互的心脏。
     * 实现细节：
     * 1.  首先，触发待处理的已完成请求的回调 (firePendingCompletedRequests)。这确保了在进入锁之前处理完上次 poll 可能已完成的请求。
     * 2.  获取锁 (lock.lock()) 以保护共享状态。
     * 3.  在 try 块中：
     *     a. 处理待处理的异步断开连接 (handlePendingDisconnects)。
     *     b. 尝试发送所有可以立即发送的请求 (trySend)，并获取下次尝试发送的最小延迟时间 pollDelayMs。
     *     c. 检查是否需要阻塞等待网络IO：
     *        - 如果没有待完成的回调 (pendingCompletion.isEmpty()) 并且 pollCondition 为 null 或指示应该阻塞 (pollCondition.shouldBlock())，则进行阻塞式 poll。
     *        - 计算 poll 的超时时间：取 timer.remainingMs() 和 pollDelayMs 中的较小值。如果当前没有正在传输的请求 (client.inFlightRequestCount() == 0)，则 poll 超时时间还会与 retryBackoffMs 比较，取更小值，避免在没有请求时长时间阻塞。
     *        - 调用底层 KafkaClient 的 poll 方法进行实际的网络IO操作。
     *        - 否则（如果 pendingCompletion 不为空，或 pollCondition 指示不应阻塞），则进行非阻塞式 poll (client.poll(0, ...))。
     *     d. 更新计时器 (timer.update())。
     *     e. 检查并处理网络断开 (checkDisconnects)。这必须在 client.poll 之后立即进行，因为后续对 client.ready() 的调用会重置断开状态。
     *     f. 如果未禁用唤醒 (disableWakeup 为 false)，则检查并触发唤醒 (maybeTriggerWakeup)。这使得在检查断开后，回调可以在下一次 poll 时准备好被触发。
     *     g. 检查并抛出中断异常 (maybeThrowInterruptException)。
     *     h. 再次尝试发送请求 (trySend)，因为在 poll 期间可能已清空缓冲区或完成了连接。
     *     i. 使发送失败且已超时的请求失败 (failExpiredRequests)。
     *     j. 清理未发送请求的集合 (unsent.clean())，防止其无限增长。
     * 4.  在 finally 块中释放锁 (lock.unlock())。
     * 5.  再次调用 firePendingCompletedRequests()，这次在锁外部调用，以避免潜在的死锁（如果回调处理器需要获取其他锁）。
     * 6.  检查元数据是否有任何异常需要抛出 (metadata.maybeThrowAnyException())。
     * 设计考虑：
     * - 线程安全：通过 ReentrantLock 保护关键代码段。
     * - 职责分离：将实际的网络IO委托给 KafkaClient。
     * - 唤醒机制：允许从其他线程中断 poll 操作。
     * - 超时管理：通过 Timer 和各种超时参数精确控制阻塞时间。
     * - 错误处理：处理网络断开、请求超时、中断等异常情况。
     * - 回调处理：将回调的执行移到锁外部，防止死锁。
     * - 资源管理：清理未发送的请求，防止内存泄漏。
     *
     * @param timer 限制此方法阻塞时间的计时器
     * @param pollCondition 可为 null 的阻塞条件，用于指示 poll 是否应该阻塞等待，或者是否有特定的 future 需要等待
     * @param disableWakeup 如果为 TRUE，则禁用触发唤醒
     */
    public void poll(Timer timer, PollCondition pollCondition, boolean disableWakeup) {
        // 如果我们唤醒了上一次对 poll 的调用，可能需要调用一些处理器
        firePendingCompletedRequests();

        // 获取锁以保护共享状态
        lock.lock();
        try {
            // 在尝试任何发送之前处理异步断开连接
            handlePendingDisconnects();

            // 发送我们现在可以发送的所有请求，并获取下次尝试发送的延迟时间
            long pollDelayMs = trySend(timer.currentTimeMs());

            // 检查调用者是否仍然需要轮询。请注意，如果在调用 shouldBlock() 之后（由于触发了完成处理器）
            // 预期的完成条件得到满足，客户端将被唤醒。
            if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {
                // 如果没有正在传输的请求，则阻塞时间不要超过重试退避时间
                long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
                // 如果当前没有正在传输的请求 (in-flight requests)
                if (client.inFlightRequestCount() == 0)
                    // 则 poll 的超时时间也应小于等于重试退避时间，避免在空闲时长时间阻塞
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);
                // 调用底层 KafkaClient 的 poll 方法进行网络IO，阻塞 pollTimeout 毫秒
                client.poll(pollTimeout, timer.currentTimeMs());
            } else {
                // 如果有待处理的完成回调，或者 pollCondition 指示不应阻塞，则进行非阻塞轮询
                client.poll(0, timer.currentTimeMs());
            }
            // 更新计时器的状态
            timer.update();

            // 处理任何断开连接，使活动请求失败。请注意，必须在 poll 之后立即检查断开连接，
            // 因为任何后续对 client.ready() 的调用都将重置断开状态
            checkDisconnects(timer.currentTimeMs());
            // 如果未禁用唤醒
            if (!disableWakeup) {
                // 在检查断开连接后触发唤醒，以便回调可以在下一次调用 poll() 时准备好被触发
                maybeTriggerWakeup();
            }
            // 如果此线程被中断，则抛出 InterruptException
            maybeThrowInterruptException();

            // 再次尝试发送请求，因为在轮询过程中缓冲区可能已被清除或连接已完成
            trySend(timer.currentTimeMs());

            // 使无法发送且已过期的请求失败
            failExpiredRequests(timer.currentTimeMs());

            // 清理未发送请求的集合，以防止映射无限增长
            unsent.clean();
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }

        // 在没有锁的情况下调用，以避免潜在的死锁（如果处理器需要获取锁）
        firePendingCompletedRequests();

        // 检查元数据中是否有任何累积的异常需要抛出
        metadata.maybeThrowAnyException();
    }

    /**
     * 轮询网络IO并立即返回。此操作不会触发唤醒。
     * 应用场景：当需要检查网络活动但不想阻塞或触发唤醒逻辑时使用，例如在关闭前的清理操作。
     * 实现细节：调用内部的 poll 方法，超时时间设置为0，表示不等待，并且 noWakeup 参数设置为 true。
     * 设计考虑：提供一个非阻塞的轮询方式，避免不必要的唤醒，提高效率。
     */
    public void pollNoWakeup() {
        // 调用 poll 方法，设置超时时间为0 (立即返回)，不进行唤醒
        poll(time.timer(0), null, true);
    }

    /**
     * 尽力轮询网络IO，仅尝试传输准备就绪的请求。
     * 不检查任何待处理请求或元数据错误，因此不应抛出任何异常，
     * 也不会触发唤醒或中断异常。
     * 应用场景：在不关心响应或错误处理，只想尽快发送已准备好的请求时使用，例如一些后台的、容错性较高的批量发送场景。
     * 实现细节：
     * 1. 创建一个超时时间为0的计时器。
     * 2. 加锁以保证线程安全。
     * 3. 调用 trySend 尝试发送当前所有可发送的请求。
     * 4. 调用底层 KafkaClient 的 poll 方法，超时时间为0，仅处理已发送请求的响应和网络事件，不阻塞。
     * 5. 释放锁。
     * 设计考虑：提供一个“即发即弃”的发送机制，专注于发送操作，简化错误处理和唤醒逻辑，适用于特定场景。
     */
    public void transmitSends() {
        // 创建一个超时时间为0的计时器，表示不等待
        Timer timer = time.timer(0);

        // 不尝试处理任何断开连接、先前的请求失败、元数据异常等；
        // 只尝试一次并立即返回
        lock.lock(); // 获取锁，保证线程安全
        try {
            // 发送所有现在可以发送的请求
            trySend(timer.currentTimeMs()); // 尝试发送待发送队列中的请求

            // 调用底层客户端的 poll 方法，超时时间为0，处理网络IO事件
            client.poll(0, timer.currentTimeMs());
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 阻塞直到来自给定节点的所有待处理请求都已完成。
     * 应用场景：在需要确保某个特定节点上的所有操作都完成后再继续执行后续逻辑时使用，例如同步等待某个关键节点的响应。
     * 实现细节：
     * 1. 使用 while 循环，条件是该节点仍有待处理请求 (hasPendingRequests(node)) 并且计时器未超时 (timer.notExpired())。
     * 2. 在循环内部调用 poll(timer) 方法，该方法会处理网络IO、发送请求、处理响应，并根据计时器进行阻塞。
     * 3. 循环结束后，再次调用 hasPendingRequests(node) 检查是否所有请求都已完成，并返回取反结果。
     * 设计考虑：提供一种同步等待机制，通过计时器控制最大阻塞时间，避免无限等待。
     * @param node 要等待请求的节点
     * @param timer 限制此方法阻塞时间的计时器
     * @return 如果所有请求都已完成，则返回 true；如果计时器先超时，则返回 false
     */
    public boolean awaitPendingRequests(Node node, Timer timer) {
        // 当指定节点仍有待处理请求且计时器未超时时，持续轮询
        while (hasPendingRequests(node) && timer.notExpired()) {
            // 调用 poll 方法处理网络IO和请求，会根据 timer 进行阻塞
            poll(timer);
        }
        // 返回指定节点是否已无待处理请求
        return !hasPendingRequests(node);
    }

    /**
     * 获取到给定节点的待处理请求数量。这包括已传输的请求（即正在传输中的请求）和等待传输的请求。
     * 应用场景：监控特定节点的负载情况，或者在进行某些操作前检查节点是否有积压请求。
     * 实现细节：
     * 1. 加锁以保证线程安全地访问 unsent 队列和 KafkaClient 的内部状态。
     * 2. 获取 unsent 队列中目标节点的请求数量 (unsent.requestCount(node))。
     * 3. 获取 KafkaClient 中正在发往目标节点的请求数量 (client.inFlightRequestCount(node.idString()))。
     * 4. 两者相加即为总的待处理请求数。
     * 5. 释放锁。
     * 设计考虑：区分了未发送和已发送在途的请求，提供了对特定节点请求积压情况的准确度量。
     * @param node 相关节点
     * @return 待处理请求的数量
     */
    public int pendingRequestCount(Node node) {
        lock.lock(); // 获取锁，保证线程安全
        try {
            // 返回未发送队列中该节点的请求数与客户端中正在发往该节点的请求数之和
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 检查到给定节点是否有待处理请求。这包括已传输的请求（即正在传输中的请求）和等待传输的请求。
     * 应用场景：在发送新请求前，判断目标节点是否繁忙；或者在等待节点响应时，作为循环条件的一部分。
     * 实现细节：
     * 1. 首先检查 unsent 队列中是否有目标节点的请求，如果有，则直接返回 true (这是一个无锁的快速路径检查)。
     * 2. 如果 unsent 队列中没有，则加锁。
     * 3. 检查 KafkaClient 中是否有正在发往目标节点的请求 (client.hasInFlightRequests(node.idString()))。
     * 4. 释放锁并返回结果。
     * 设计考虑：通过先检查 unsent 队列（通常无锁或锁粒度更小）来优化性能，减少不必要的加锁操作。
     * @param node 相关节点
     * @return 一个布尔值，指示是否存在待处理请求
     */
    public boolean hasPendingRequests(Node node) {
        // 首先检查未发送队列中是否有该节点的请求，这是一个优化，避免不必要的加锁
        if (unsent.hasRequests(node))
            return true; // 如果未发送队列中有，则肯定有待处理请求
        lock.lock(); // 获取锁，保证线程安全
        try {
            // 检查底层客户端是否有正在发往该节点的请求
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 获取所有节点的待处理请求总数。这包括已传输的请求（即正在传输中的请求）和等待传输的请求。
     * 应用场景：全局监控消费者的网络负载，或者作为某些全局操作（如关闭客户端）前的检查条件。
     * 实现细节：
     * 1. 加锁以保证线程安全。
     * 2. 获取 unsent 队列中所有节点的请求总数 (unsent.requestCount())。
     * 3. 获取 KafkaClient 中所有正在传输的请求总数 (client.inFlightRequestCount())。
     * 4. 两者相加。
     * 5. 释放锁。
     * 设计考虑：提供一个全局的请求积压视图。
     * @return 待处理请求的总数
     */
    public int pendingRequestCount() {
        lock.lock(); // 获取锁，保证线程安全
        try {
            // 返回未发送队列中的总请求数与客户端中正在传输的总请求数之和
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 检查是否存在任何待处理请求。这包括已传输的请求（即正在传输中的请求）和等待传输的请求。
     * 应用场景：在执行可能影响网络状态的操作（如元数据更新）前，检查是否有正在进行的网络活动。
     * 实现细节：
     * 1. 首先检查 unsent 队列中是否有任何请求，如果有，则直接返回 true (优化性能)。
     * 2. 如果 unsent 队列中没有，则加锁。
     * 3. 检查 KafkaClient 中是否有任何正在传输的请求 (client.hasInFlightRequests())。
     * 4. 释放锁并返回结果。
     * 设计考虑：与 `hasPendingRequests(Node node)` 类似，通过先检查 unsent 队列来优化性能。
     * @return 一个布尔值，指示是否存在待处理请求
     */
    public boolean hasPendingRequests() {
        // 首先检查未发送队列中是否有任何请求，这是一个优化
        if (unsent.hasRequests())
            return true; // 如果未发送队列中有请求，则肯定有待处理请求
        lock.lock(); // 获取锁，保证线程安全
        try {
            // 检查底层客户端是否有正在传输的请求
            return client.hasInFlightRequests();
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 触发待处理的已完成请求的回调。
     * 应用场景：在 poll 方法中，当网络IO处理完毕后，此方法被调用来执行那些已经收到响应并标记为完成的请求的回调逻辑。
     * 实现细节：
     * 1. 初始化一个标志 `completedRequestsFired` 为 false。
     * 2. 无限循环 (for (;;)) 从 `pendingCompletion` 队列中取出已完成的请求回调处理器 (`RequestFutureCompletionHandler`)。
     * 3. 如果队列为空 (`completionHandler == null`)，则跳出循环。
     * 4. 调用 `completionHandler.fireCompletion()` 来执行实际的回调逻辑 (通常是设置 Future 的结果或异常)。
     * 5. 将 `completedRequestsFired` 设置为 true。
     * 6. 循环结束后，如果 `completedRequestsFired` 为 true (即至少有一个回调被触发)，则调用 `client.wakeup()`。
     * 设计考虑：
     * - 将回调的执行与持有主锁的逻辑分离，避免在持有锁时执行可能耗时或阻塞的回调，防止死锁。
     * - `pendingCompletion` 是一个并发队列，允许在其他线程中安全地添加已完成的回调。
     * - 唤醒客户端 (`client.wakeup()`) 是为了确保如果外部有线程正在等待这些 Future 完成 (例如通过 `future.get()`)，它们能够被及时唤醒。
     */
    private void firePendingCompletedRequests() {
        // 标记是否有已完成的请求被触发
        boolean completedRequestsFired = false;
        // 无限循环，直到待处理完成队列为空
        for (;;) {
            // 从待处理完成队列中取出一个回调处理器
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            // 如果队列为空，则跳出循环
            if (completionHandler == null)
                break;

            // 执行回调处理器的完成逻辑
            completionHandler.fireCompletion();
            // 标记已触发回调
            completedRequestsFired = true;
        }

        // 如果触发了任何已完成请求的回调，则唤醒客户端
        // 以防客户端在轮询此 future 的完成时阻塞
        if (completedRequestsFired)
            client.wakeup();
    }

    /**
     * 检查与未发送请求相关的连接是否已断开。
     * 应用场景：在每次 poll 操作中，主动检查那些尚未发送的请求，如果其目标节点的连接已经失败，则立即将这些请求标记为失败。
     * 实现细节：
     * 1. 遍历 `unsent` 队列中涉及的所有节点。
     * 2. 对每个节点，调用 `client.connectionFailed(node)` 检查与该节点的连接是否已失败。
     * 3. 如果连接失败：
     *    a. 从 `unsent` 队列中移除该节点的所有请求 (`unsent.remove(node)`)。这一步很重要，要在调用回调之前执行，以避免回调中可能再次遍历 `unsent` 列表导致的问题（例如协调器故障处理）。
     *    b. 遍历这些被移除的请求。
     *    c. 获取请求的回调处理器 (`RequestFutureCompletionHandler`)。
     *    d. 获取与该节点相关的认证异常 (如果存在)。
     *    e. 创建一个 `ClientResponse` 对象，标记为已断开连接 (`disconnected=true`)，并包含认证异常（如果有）。
     *    f. 调用回调处理器的 `onComplete` 方法，将此 `ClientResponse` 作为结果，从而使对应的 Future 失败。
     * 设计考虑：
     * - 主动失败未发送的请求：对于那些目标节点已断开的未发送请求，没有必要等待它们超时，应尽快失败，以便上层逻辑能够及时处理。
     * - NetworkClient 处理已发送请求的断开：注释中提到，影响已发送请求的断开由 NetworkClient 处理，此方法专注于未发送的请求。
     * - 回调前的移除：先从 `unsent` 队列移除再调用回调，是为了防止并发修改或递归遍历问题。
     * @param now 当前时间戳，用于创建 ClientResponse
     */
    private void checkDisconnects(long now) {
        // 任何影响已传输请求的断开连接将由 NetworkClient 处理，
        // 因此我们只需要检查任何未发送请求的连接是否已断开；
        // 如果已断开，则我们完成相应的 future 并设置 ClientResponse 中的断开标志
        // 遍历所有有未发送请求的节点
        for (Node node : unsent.nodes()) {
            // 检查客户端与该节点的连接是否失败
            if (client.connectionFailed(node)) {
                // 在调用请求回调之前删除条目，以避免回调处理协调器故障时再次遍历未发送列表。
                // 移除该节点所有未发送的请求
                Collection<ClientRequest> requests = unsent.remove(node);
                // 遍历这些被移除的请求
                for (ClientRequest request : requests) {
                    // 获取请求的回调处理器
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    // 获取该节点的认证异常（如果有）
                    AuthenticationException authenticationException = client.authenticationException(node);
                    // 使用表示断开连接的 ClientResponse 完成回调
                    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                            request.callback(), request.destination(), request.createdTimeMs(), now, true, // true 表示已断开连接
                            null, authenticationException, null));
                }
            }
        }
    }

    /**
     * 处理待断开连接的节点。
     * 应用场景：当外部调用 `disconnectAsync` 请求断开与某个节点的连接时，该节点的请求会被异步地加入 `pendingDisconnects` 队列。
     * 此方法在 `poll` 循环中被调用，以实际执行这些断开操作。
     * 实现细节：
     * 1. 加锁以保证对 `pendingDisconnects` 队列和 `unsent` 队列操作的线程安全。
     * 2. 循环从 `pendingDisconnects` 队列中取出待断开的节点。
     * 3. 如果队列为空，则跳出循环。
     * 4. 对取出的节点，调用 `failUnsentRequests(node, DisconnectException.INSTANCE)`，将该节点上所有未发送的请求标记为因断开连接而失败。
     * 5. 调用 `client.disconnect(node.idString())`，通知底层 KafkaClient 断开与该节点的连接。
     * 6. 释放锁。
     * 设计考虑：
     * - 异步断开：`disconnectAsync` 只是将断开请求入队，实际的断开操作在此方法中执行，避免了 `disconnectAsync` 的调用者长时间阻塞。
     * - 失败未发送请求：在断开连接之前，确保与该节点相关的未发送请求被正确处理（标记为失败）。
     */
    private void handlePendingDisconnects() {
        lock.lock(); // 获取锁，保证线程安全
        try {
            // 无限循环，直到待断开连接队列为空
            while (true) {
                // 从待断开连接队列中取出一个节点
                Node node = pendingDisconnects.poll();
                // 如果队列为空，则跳出循环
                if (node == null)
                    break;

                // 使该节点上所有未发送的请求失败，原因为断开连接
                failUnsentRequests(node, DisconnectException.INSTANCE);
                // 通知底层客户端断开与该节点的连接
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 异步请求断开与指定节点的连接。
     * 应用场景：当消费者逻辑判断需要主动断开与某个 Broker 节点的连接时（例如，节点出现故障或不再需要与该节点通信），可以调用此方法。
     * 实现细节：
     * 1. 将指定的 `node` 添加到 `pendingDisconnects` 并发队列中。
     * 2. 调用 `client.wakeup()` 唤醒可能在 `poll` 中阻塞的客户端线程，以便及时处理 `pendingDisconnects` 队列中的断开请求。
     * 设计考虑：
     * - 异步执行：断开连接的操作可能涉及网络IO，将其设计为异步可以避免调用者阻塞。
     * - 队列化处理：通过 `pendingDisconnects` 队列，将断开请求暂存，由 `poll` 循环中的 `handlePendingDisconnects` 方法统一处理，保证了操作的顺序和线程安全。
     * @param node 要断开连接的节点
     */
    public void disconnectAsync(Node node) {
        // 将节点添加到待断开连接队列
        pendingDisconnects.offer(node);
        // 唤醒客户端，以便处理这个断开请求
        client.wakeup();
    }

    /**
     * 使所有已超时的未发送请求失败。
     * 应用场景：在每次 `poll` 操作中，检查 `unsent` 队列中是否有请求因为等待发送时间过长而超时。
     * 实现细节：
     * 1. 调用 `unsent.removeExpiredRequests(now)` 方法，该方法会根据当前时间 `now` 和每个请求的创建时间及超时设置，找出所有已超时的未发送请求，并将它们从 `unsent` 队列中移除。
     * 2. 遍历这些被移除的超时请求。
     * 3. 对每个请求，获取其回调处理器 (`RequestFutureCompletionHandler`)。
     * 4. 调用回调处理器的 `onFailure` 方法，传入一个 `TimeoutException`，并附带超时的详细信息（例如，请求在多少毫秒后发送失败）。这将使对应的 Future 对象以超时异常结束。
     * 设计考虑：
     * - 主动超时：对于长时间未能发送出去的请求，应主动将其标记为超时失败，而不是无限期等待，这有助于及时释放资源并通知上层逻辑。
     * - 清晰的异常信息：通过 `TimeoutException` 明确告知调用者请求失败的原因是超时。
     * @param now 当前时间戳，用于判断请求是否超时
     */
    private void failExpiredRequests(long now) {
        // 清除所有已过期的未发送请求，并使它们对应的 future 失败
        // 从未发送队列中移除所有已超时的请求
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now);
        // 遍历这些超时的请求
        for (ClientRequest request : expiredRequests) {
            // 获取请求的回调处理器
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            // 使用 TimeoutException 使回调失败
            handler.onFailure(new TimeoutException("Failed to send request after " + request.requestTimeoutMs() + " ms."));
        }
    }

    /**
     * 使发送到指定节点的未发送请求失败。
     * 应用场景：当与某个节点的连接出现问题（例如，节点宕机、网络分区），或者在关闭客户端时，需要将所有排队等待发送到该节点的请求标记为失败。
     * 实现细节：
     * 1. 获取锁以保证线程安全。
     * 2. 从 `unsent` 集合中移除指定节点的所有未发送请求。
     * 3. 遍历这些未发送的请求。
     * 4. 获取每个请求的回调处理器（`RequestFutureCompletionHandler`）。
     * 5. 调用回调处理器的 `onFailure` 方法，传入指定的异常，从而使对应的 `RequestFuture` 失败。
     * 设计考虑：
     * - 线程安全：使用 `lock` 确保对 `unsent` 集合的并发访问是安全的。
     * - 及时失败：快速失败这些请求可以避免消费者长时间等待，并允许上层逻辑进行相应的错误处理或重试。
     * @param node 目标节点，该节点上的未发送请求将失败
     * @param e 导致请求失败的运行时异常
     */
    private void failUnsentRequests(Node node, RuntimeException e) {
        // 清除发往节点的未发送请求，并使它们对应的 future 失败
        lock.lock(); // 获取锁，保护 unsent 集合的访问
        try {
            // 从 unsent 集合中移除并获取指定节点的所有未发送请求
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            // 遍历所有未发送的请求
            for (ClientRequest unsentRequest : unsentRequests) {
                // 获取请求的回调处理器，这里强制转换为 RequestFutureCompletionHandler
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                // 调用处理器的 onFailure 方法，将异常传递给 Future，使其失败
                handler.onFailure(e);
            }
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    // 仅用于测试
    /**
     * 尝试发送当前可以发送的请求。
     * 应用场景：在 `poll` 方法的核心循环中被调用，用于检查并发送 `unsent` 队列中所有满足发送条件的请求。
     * 实现细节：
     * 1. 初始化 `pollDelayMs` 为最大轮询超时时间，这个值将用于计算下一次 `poll` 的阻塞时间。
     * 2. 遍历 `unsent` 队列中的所有目标节点。
     * 3. 对每个节点，获取其未发送请求的迭代器。
     * 4. 如果该节点有未发送的请求，更新 `pollDelayMs` 为当前值与客户端到该节点的 `pollDelayMs` 中的较小者。
     *    `client.pollDelayMs(node, now)` 会考虑连接状态、退避时间等因素，返回到下次可以尝试向该节点发送请求的延迟。
     * 5. 遍历该节点的所有未发送请求：
     *    a. 获取下一个请求。
     *    b. 检查客户端是否已准备好向该节点发送请求（`client.ready(node, now)`）。
     *    c. 如果准备好了，则通过 `client.send(request, now)` 发送请求，并从迭代器中移除该请求。
     *    d. 如果未准备好（例如，连接正在建立中，或处于退避期），则跳出当前节点的请求发送循环，尝试下一个节点。
     * 6. 返回计算得到的 `pollDelayMs`，这个值将影响 `KafkaClient.poll` 的阻塞时间。
     * 设计考虑：
     * - 批量发送：尝试发送所有可发送的请求，提高效率。
     * - 动态调整轮询延迟：根据节点状态和请求情况，动态计算下一次轮询的延迟时间，避免不必要的CPU空转或过长的等待。
     * - 节点状态检查：`client.ready()` 封装了复杂的节点状态判断逻辑。
     * @param now 当前时间戳（毫秒）
     * @return 下一次轮询操作可以安全阻塞的最短延迟时间（毫秒）
     */
    long trySend(long now) {
        // 初始化轮询延迟为配置的最大轮询超时时间
        long pollDelayMs = maxPollTimeoutMs;

        // 发送任何现在可以发送的请求
        // 遍历所有有未发送请求的节点
        for (Node node : unsent.nodes()) {
            // 获取该节点未发送请求的迭代器
            Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            // 如果该节点存在未发送的请求
            if (iterator.hasNext())
                // 更新 pollDelayMs，取当前 pollDelayMs 和 client.pollDelayMs(node, now) 中的较小值
                // client.pollDelayMs 返回到下次可以尝试向该节点发送请求的延迟
                pollDelayMs = Math.min(pollDelayMs, client.pollDelayMs(node, now));

            // 遍历当前节点的所有未发送请求
            while (iterator.hasNext()) {
                // 获取下一个请求
                ClientRequest request = iterator.next();
                // 检查客户端是否准备好向该节点发送数据
                if (client.ready(node, now)) {
                    // 如果准备好，则发送请求
                    client.send(request, now);
                    // 从迭代器中移除已发送的请求
                    iterator.remove();
                } else {
                    // 如果当前节点未准备好，则尝试下一个节点
                    break;
                }
            }
        }
        // 返回计算出的轮询延迟时间
        return pollDelayMs;
    }

    /**
     * 检查是否需要触发唤醒操作。
     * 应用场景：在长时间阻塞的 `poll` 操作中，如果其他线程调用了 `wakeup()` 方法，此方法用于检测唤醒信号并抛出 `WakeupException`，从而中断阻塞。
     * 实现细节：
     * 1. 检查唤醒功能是否未被禁用 (`!wakeupDisabled.get()`)。
     * 2. 检查唤醒标志是否被设置 (`wakeup.get()`)。
     * 3. 如果两个条件都满足，则记录调试日志，将唤醒标志重置为 `false`，并抛出 `WakeupException`。
     * 设计考虑：
     * - 及时响应唤醒：允许外部线程中断消费者的阻塞操作，例如在关闭消费者时。
     * - 原子操作：`wakeupDisabled` 和 `wakeup` 都是原子类型，确保多线程访问的正确性。
     * - 一次性唤醒：抛出异常后，`wakeup` 标志被重置，避免后续不必要的唤醒。
     */
    public void maybeTriggerWakeup() {
        // 检查唤醒功能是否启用，并且 wakeup 标志是否被设置
        if (!wakeupDisabled.get() && wakeup.get()) {
            // 记录调试信息，表明是响应用户唤醒而抛出 WakeupException
            log.debug("Raising WakeupException in response to user wakeup");
            // 重置 wakeup 标志为 false，避免重复触发
            wakeup.set(false);
            // 抛出 WakeupException 中断当前操作
            throw new WakeupException();
        }
    }

    /**
     * 检查当前线程是否被中断，如果是，则抛出 {@link InterruptException}。
     * 应用场景：在关键的循环或阻塞操作之前或之后调用，以响应线程中断信号，确保消费者能够优雅地停止。
     * 实现细节：
     * 1. 调用 `Thread.interrupted()` 检查当前线程的中断状态，并清除中断状态。
     * 2. 如果线程已被中断，则构造一个新的 `InterruptedException` 并包装在 `InterruptException` 中抛出。
     * 设计考虑：
     * - 及时响应中断：确保消费者在接收到中断信号时能够快速停止，释放资源。
     * - 标准中断处理：遵循 Java 的标准线程中断模式。
     */
    private void maybeThrowInterruptException() {
        // 检查当前线程是否已被中断（注意：Thread.interrupted() 会清除中断状态）
        if (Thread.interrupted()) {
            // 如果线程被中断，则抛出 InterruptException，包装原始的 InterruptedException
            throw new InterruptException(new InterruptedException());
        }
    }

    /**
     * 禁用唤醒功能。
     * 应用场景：在某些特定的、不希望被外部 `wakeup()` 调用打断的临界区操作中，可以临时禁用唤醒功能。
     * 例如，在执行一些必须完成的清理操作时。
     * 实现细节：
     * 将原子布尔值 `wakeupDisabled` 设置为 `true`。
     * 设计考虑：
     * - 提供控制机制：允许在特定情况下阻止唤醒，以保证操作的原子性或完整性。
     * - 原子操作：`wakeupDisabled` 是原子类型，保证设置操作的线程安全。
     */
    public void disableWakeups() {
        // 将 wakeupDisabled 原子地设置为 true，禁用唤醒功能
        wakeupDisabled.set(true);
    }

    /**
     * 关闭消费者网络客户端。
     * 应用场景：当消费者不再需要与 Kafka 集群通信时（例如，应用程序关闭），调用此方法释放网络资源。
     * 实现细节：
     * 1. 获取锁以确保关闭操作的线程安全。
     * 2. 调用底层 `KafkaClient` 的 `close()` 方法，关闭所有网络连接并释放相关资源。
     * 3. 在 `finally` 块中释放锁。
     * 设计考虑：
     * - 资源释放：确保网络连接等资源得到正确释放，防止资源泄漏。
     * - 线程安全：使用锁保护关闭过程，避免并发问题。
     * @throws IOException 如果在关闭底层客户端时发生 I/O 错误
     */
    @Override
    public void close() throws IOException {
        // 获取锁，确保关闭操作的线程安全
        lock.lock();
        try {
            // 调用底层 KafkaClient 的 close 方法关闭网络连接和资源
            client.close();
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }
    }


    /**
     * 检查节点是否已断开连接并且无法立即重新连接（即，如果它在断开连接后的重新连接退避窗口中）。
     * 应用场景：在尝试向某个节点发送请求之前，可以调用此方法判断该节点当前是否可用，以避免不必要的连接尝试或快速失败。
     * 实现细节：
     * 1. 获取锁以保证线程安全地访问客户端状态。
     * 2. 调用 `NetworkClientUtils.isUnavailable(client, node, time)` 方法进行判断。
     *    该工具方法会检查节点的连接状态以及是否处于连接失败后的退避期。
     * 3. 在 `finally` 块中释放锁。
     * 设计考虑：
     * - 避免无效尝试：通过检查节点可用性，可以减少向不可用节点发送请求的尝试。
     * - 封装复杂性：`NetworkClientUtils.isUnavailable` 封装了判断节点是否可用的具体逻辑。
     * @param node 要检查的节点
     * @return 如果节点不可用，则返回 `true`；否则返回 `false`
     */
    public boolean isUnavailable(Node node) {
        // 获取锁，确保线程安全地访问客户端状态
        lock.lock();
        try {
            // 调用 NetworkClientUtils.isUnavailable 方法检查节点是否断开并且在重连退避期
            return NetworkClientUtils.isUnavailable(client, node, time);
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }
    }

    /**
     * 检查给定节点上是否存在身份验证错误，如果存在则引发异常。
     * 应用场景：在与节点交互后，特别是在连接建立或请求发送后，调用此方法检查是否发生了身份验证失败。
     * 实现细节：
     * 1. 获取锁以保证线程安全地访问客户端状态。
     * 2. 调用 `NetworkClientUtils.maybeThrowAuthFailure(client, node)` 方法。
     *    该工具方法会检查与指定节点的连接是否存在身份验证错误，如果存在，则抛出相应的 `AuthenticationException`。
     * 3. 在 `finally` 块中释放锁。
     * 设计考虑：
     * - 早期错误检测：及时发现并抛出认证失败异常，便于上层应用处理。
     * - 封装认证检查逻辑：`NetworkClientUtils.maybeThrowAuthFailure` 封装了具体的认证错误检查和异常抛出逻辑。
     * @param node 要检查的节点
     * @throws AuthenticationException 如果在与指定节点的连接上检测到身份验证错误
     */
    public void maybeThrowAuthFailure(Node node) {
        // 获取锁，确保线程安全地访问客户端状态
        lock.lock();
        try {
            // 调用 NetworkClientUtils.maybeThrowAuthFailure 方法检查并可能抛出认证失败异常
            NetworkClientUtils.maybeThrowAuthFailure(client, node);
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }
    }

    /**
     * 如果当前可能，则发起连接。这仅对于重置套接字的失败状态真正有用。
     * 如果有实际的请求要发送，则应使用 {@link #send(Node, AbstractRequest.Builder)}。
     * 应用场景：当需要主动尝试与某个节点建立连接，或者在连接失败后希望重置连接状态并尝试重新连接时使用。
     *          通常，发送请求时会自动处理连接，但此方法提供了一个显式触发连接尝试的途径。
     * 实现细节：
     * 1. 获取锁以保证线程安全地操作客户端连接状态。
     * 2. 调用 `NetworkClientUtils.tryConnect(client, node, time)` 方法。
     *    该工具方法会尝试向指定节点发起连接（如果尚未连接或连接已断开）。
     * 3. 在 `finally` 块中释放锁。
     * 设计考虑：
     * - 主动连接控制：提供一种手动触发连接尝试的机制。
     * - 重置失败状态：有助于在连接失败后，清除之前的失败标记并尝试新的连接。
     * @param node 要连接的节点
     */
    public void tryConnect(Node node) {
        // 获取锁，确保线程安全地操作客户端连接状态
        lock.lock();
        try {
            // 调用 NetworkClientUtils.tryConnect 方法尝试连接到指定节点
            NetworkClientUtils.tryConnect(client, node, time);
        } finally {
            // 确保在任何情况下都释放锁
            lock.unlock();
        }
    }

    /**
     * 请求的 Future 完成处理器，实现了 {@link RequestCompletionHandler} 接口。
     * 应用场景：作为每个通过 `ConsumerNetworkClient` 发送的异步请求的回调处理器。
     *          当底层 `KafkaClient` 完成一个请求（成功或失败）时，会调用此处理器的 `onComplete` 或 `onFailure` 方法。
     * 实现细节：
     * - 持有一个 `RequestFuture<ClientResponse>` 对象，用于将请求的结果（成功响应或异常）传递给调用者。
     * - `onFailure` 和 `onComplete` 方法被调用时，会记录异常或响应，并将自身添加到 `pendingCompletion` 队列中，
     *   等待 `ConsumerNetworkClient` 的 `poll` 方法后续处理并触发 `future` 的完成。
     * - `fireCompletion` 方法根据记录的响应或异常，来完成（成功或失败）内部的 `future`。
     * 设计考虑：
     * - 解耦：将请求的发送与结果的处理解耦，`ConsumerNetworkClient` 负责发送和轮询，`RequestFutureCompletionHandler` 负责具体的结果通知。
     * - 延迟完成：结果不是立即在 `onComplete/onFailure` 中触发 `future`，而是先加入队列，由主轮询线程统一处理，避免在 `KafkaClient` 的 I/O 线程中执行过多逻辑或持有锁。
     */
    private class RequestFutureCompletionHandler implements RequestCompletionHandler {
        // 与此回调关联的 RequestFuture，用于将结果传递给请求的发起者
        private final RequestFuture<ClientResponse> future;
        // 存储收到的客户端响应，如果请求成功
        private ClientResponse response;
        // 存储发生的运行时异常，如果请求失败
        private RuntimeException e;

        /**
         * 构造函数，初始化一个 RequestFutureCompletionHandler。
         * 实现细节：创建一个新的 {@link RequestFuture} 实例，用于异步地接收请求结果。
         */
        private RequestFutureCompletionHandler() {
            // 初始化一个 RequestFuture，调用者将通过这个 future 获取响应或错误
            this.future = new RequestFuture<>();
        }

        /**
         * 触发 future 的完成逻辑。
         * 应用场景：在 `ConsumerNetworkClient` 的 `poll` 方法中，从 `pendingCompletion` 队列中取出此处理器后调用。
         * 实现细节：
         * 1. 如果存在异常 `e`，则用该异常使 `future` 失败。
         * 2. 否则，检查响应 `response` 中是否包含认证异常、断开连接信息或版本不匹配错误，并相应地使 `future` 失败。
         * 3. 如果以上都不是，则用成功的响应 `response` 完成 `future`。
         * 设计考虑：
         * - 统一处理点：将所有可能的完成情况（成功、各种错误）集中在此处理，更新 `future` 的状态。
         * - 错误优先级：按顺序检查不同类型的错误。
         */
        public void fireCompletion() {
            // 如果之前记录了异常 e
            if (e != null) {
                // 使用该异常使 future 失败
                future.raise(e);
            // 否则，如果响应中包含认证异常
            } else if (response.authenticationException() != null) {
                // 使用认证异常使 future 失败
                future.raise(response.authenticationException());
            // 否则，如果响应表明连接已断开
            } else if (response.wasDisconnected()) {
                // 记录调试日志，表明请求因节点断开而被取消
                log.debug("Cancelled request with header {} due to node {} being disconnected",
                        response.requestHeader(), response.destination());
                // 使用 DisconnectException 实例使 future 失败
                future.raise(DisconnectException.INSTANCE);
            // 否则，如果响应中包含版本不匹配错误
            } else if (response.versionMismatch() != null) {
                // 使用版本不匹配异常使 future 失败
                future.raise(response.versionMismatch());
            // 否则，表示请求成功
            } else {
                // 使用收到的响应完成 future
                future.complete(response);
            }
        }

        /**
         * 当请求失败时由 KafkaClient 调用。
         * 实现细节：
         * 1. 将传入的运行时异常 `e` 存储在此处理器实例中。
         * 2. 将此处理器自身添加到 `pendingCompletion` 队列中，等待后续处理。
         * @param e 导致请求失败的运行时异常
         */
        public void onFailure(RuntimeException e) {
            // 记录下发生的异常
            this.e = e;
            // 将当前完成处理器添加到待处理完成队列中，由 poll 循环稍后处理
            pendingCompletion.add(this);
        }

        /**
         * 当请求成功完成并收到响应时由 KafkaClient 调用。
         * 实现细节：
         * 1. 将传入的客户端响应 `response` 存储在此处理器实例中。
         * 2. 将此处理器自身添加到 `pendingCompletion` 队列中，等待后续处理。
         * @param response 从服务端收到的客户端响应
         */
        @Override
        public void onComplete(ClientResponse response) {
            // 记录下收到的响应
            this.response = response;
            // 将当前完成处理器添加到待处理完成队列中，由 poll 循环稍后处理
            pendingCompletion.add(this);
        }
    }

    /**
     * @interface PollCondition
     * @brief 定义轮询条件检查的接口。
     * @details
     * 应用场景：在多线程环境中调用 poll 方法时，调用者等待的条件可能在 poll 调用之前就已经满足。
     * 因此，引入此接口将条件检查尽可能推近到 poll 的调用点。
     * 实现细节：检查将在持有用于保护对 {@link org.apache.kafka.clients.NetworkClient} 并发访问的锁时进行。
     * 设计考虑：这意味着如果回调必须获取额外的锁，实现必须非常小心锁的顺序，以避免死锁。
     */
    public interface PollCondition { // 定义 PollCondition 接口
        /**
         * @brief 检查调用者是否仍在等待 IO 事件。
         * @details
         * 应用场景：在 poll 操作决定是否阻塞等待网络 IO 事件之前，会调用此方法来判断条件是否已满足。
         * 实现细节：具体的实现类需要根据自身的逻辑判断是否需要阻塞。
         * 设计考虑：允许 poll 操作根据外部条件动态决定是否阻塞，提高灵活性。
         * @return 如果调用者仍在等待 IO 事件，则返回 true；否则返回 false。
         */
        boolean shouldBlock(); // 声明 shouldBlock 方法，返回一个布尔值
    }

    /**
     * @class UnsentRequests
     * @brief 一个线程安全的辅助类，用于按节点保存尚未发送的请求。
     * @details
     * 应用场景：在 ConsumerNetworkClient 中，需要一个地方暂存待发送到特定节点的请求，直到 poll 操作实际发送它们。
     * 实现细节：内部使用 ConcurrentMap 来存储每个节点对应的请求队列 (ConcurrentLinkedQueue)。
     * 设计考虑：
     * - 线程安全：使用 ConcurrentMap 和 ConcurrentLinkedQueue 保证了多线程环境下的安全访问。
     * - 性能：选择并发集合是为了减少锁竞争，提高并发性能。
     * - 职责分离：将未发送请求的管理逻辑封装在此类中，使 ConsumerNetworkClient 的代码更清晰。
     */
    private static final class UnsentRequests { // 定义一个私有的静态内部类 UnsentRequests，用于管理未发送的请求
        /**
         * @brief 未发送请求的存储结构。
         * @details 使用 ConcurrentMap，键是目标节点 {@link Node}，值是该节点对应的 {@link ClientRequest} 请求队列 (ConcurrentLinkedQueue)。
         * ConcurrentLinkedQueue 是一个线程安全的队列，适合多生产者单消费者的场景。
         */
        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent; // 声明一个 ConcurrentMap 类型的成员变量 unsent，用于存储未发送的请求，键是 Node，值是 ClientRequest 的并发链式队列

        /**
         * @brief UnsentRequests 的构造函数。
         * @details
         * 应用场景：当创建 ConsumerNetworkClient 实例时，会间接创建 UnsentRequests 实例来管理未发送的请求。
         * 实现细节：初始化 unsent 字段为一个新的 ConcurrentHashMap。
         * 设计考虑：默认构造函数，简单直接。
         */
        private UnsentRequests() { // UnsentRequests 的私有构造函数
            unsent = new ConcurrentHashMap<>(); // 初始化 unsent 为一个新的 ConcurrentHashMap 实例
        }

        /**
         * @brief 将一个请求放入指定节点的未发送队列中。
         * @details
         * 应用场景：当 ConsumerNetworkClient 的 send 方法被调用时，会将 ClientRequest 添加到此处的队列中。
         * 实现细节：
         * 1. 使用 synchronized(unsent) 来保护对 unsent 映射的并发访问，特别是防止在添加请求时，队列被并发移除。
         * 2. 使用 unsent.computeIfAbsent(node, key -> new ConcurrentLinkedQueue<>()) 原子地获取或创建指定节点的请求队列。
         *    如果节点对应的队列不存在，则创建一个新的 ConcurrentLinkedQueue 并放入 map 中。
         * 3. 将请求添加到获取到的队列中。
         * 设计考虑：
         * - 线程安全：通过 synchronized 块和 ConcurrentMap 的原子操作来保证线程安全。
         * - 惰性初始化：节点的请求队列在第一次向该节点添加请求时才会被创建。
         * @param node 目标节点
         * @param request 要添加的客户端请求
         */
        public void put(Node node, ClientRequest request) { // 定义 put 方法，用于将请求添加到指定节点的队列中
            // 该锁保护 put 操作，防止节点队列被并发移除
            synchronized (unsent) { // 同步块，锁对象是 unsent，确保线程安全
                // 获取或创建指定节点的请求队列。如果节点不存在，则创建一个新的 ConcurrentLinkedQueue
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.computeIfAbsent(node, key -> new ConcurrentLinkedQueue<>());
                requests.add(request); // 将请求添加到队列中
            }
        }

        /**
         * @brief 获取指定节点当前未发送的请求数量。
         * @details
         * 应用场景：用于监控或调试，了解特定节点待发送请求的积压情况。
         * 实现细节：
         * 1. 从 unsent 映射中获取指定节点的请求队列。
         * 2. 如果队列不存在（即该节点没有未发送的请求），则返回 0。
         * 3. 否则，返回队列的大小。
         * 设计考虑：这是一个只读操作，ConcurrentMap 的 get 和 ConcurrentLinkedQueue 的 size 操作本身是线程安全的，所以不需要额外的同步。
         * @param node 目标节点
         * @return 指定节点未发送的请求数量
         */
        public int requestCount(Node node) { // 定义 requestCount 方法，获取指定节点的请求数量
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node); // 从 unsent Map 中获取指定节点的请求队列
            return requests == null ? 0 : requests.size(); // 如果队列为 null，返回 0，否则返回队列的大小
        }

        /**
         * @brief 获取所有节点当前未发送的总请求数量。
         * @details
         * 应用场景：用于整体监控未发送请求的总量。
         * 实现细节：
         * 1. 初始化总数为 0。
         * 2. 遍历 unsent 映射中的所有值（即所有节点的请求队列）。
         * 3. 将每个队列的大小累加到总数中。
         * 4. 返回总数。
         * 设计考虑：遍历 unsent.values() 和调用每个队列的 size() 都是线程安全的。但需要注意，在遍历过程中，其他线程可能正在修改队列，
         * 所以这个总数是一个近似值，反映了调用该方法时的一个快照。
         * @return 所有节点未发送的总请求数量
         */
        public int requestCount() { // 定义 requestCount 方法，获取所有未发送请求的总数
            int total = 0; // 初始化总数为 0
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) // 遍历 unsent Map 中的所有值（即所有请求队列）
                total += requests.size(); // 将当前队列的大小累加到 total
            return total; // 返回总数
        }

        /**
         * @brief 检查指定节点是否有未发送的请求。
         * @details
         * 应用场景：在决定是否需要为某个节点准备发送操作时使用。
         * 实现细节：
         * 1. 获取指定节点的请求队列。
         * 2. 如果队列存在且不为空，则返回 true。
         * 3. 否则返回 false。
         * 设计考虑：与 requestCount(Node) 类似，这是一个线程安全的只读操作。
         * @param node 目标节点
         * @return 如果指定节点有未发送的请求，则返回 true；否则返回 false。
         */
        public boolean hasRequests(Node node) { // 定义 hasRequests 方法，检查指定节点是否有未发送的请求
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node); // 获取指定节点的请求队列
            return requests != null && !requests.isEmpty(); // 如果队列不为 null 且不为空，则返回 true，否则返回 false
        }

        /**
         * @brief 检查是否有任何节点存在未发送的请求。
         * @details
         * 应用场景：在 poll 循环中，判断是否需要进行网络发送操作。
         * 实现细节：
         * 1. 遍历 unsent 映射中的所有请求队列。
         * 2. 如果发现任何一个队列不为空，则立即返回 true。
         * 3. 如果遍历完所有队列都为空，则返回 false。
         * 设计考虑：这是一个优化的检查，一旦发现有请求就返回，不必遍历所有队列。
         * @return 如果有任何未发送的请求，则返回 true；否则返回 false。
         */
        public boolean hasRequests() { // 定义 hasRequests 方法，检查是否有任何未发送的请求
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) // 遍历所有请求队列
                if (!requests.isEmpty()) // 如果当前队列不为空
                    return true; // 立即返回 true
            return false; // 如果所有队列都为空，返回 false
        }

        /**
         * @brief 移除并返回所有已超时的请求。
         * @details
         * 应用场景：在每次 poll 操作之前，清理掉那些已经等待太久而超时的请求，避免发送它们。
         * 实现细节：
         * 1. 创建一个列表 expiredRequests 用于存放超时的请求。
         * 2. 遍历 unsent 映射中的所有请求队列。
         * 3. 对每个队列，获取其迭代器。
         * 4. 遍历队列中的请求：
         *    a. 计算请求已创建的时间 (elapsedMs)。
         *    b. 如果 elapsedMs 大于请求的超时时间 request.requestTimeoutMs()，则将该请求添加到 expiredRequests 列表中，并从当前队列中移除它。
         *    c. 由于请求是按创建时间顺序添加到队列的，如果当前请求未超时，则其后的请求也不会超时，因此可以 break 内部循环，处理下一个节点的队列。
         * 5. 返回包含所有超时请求的列表。
         * 设计考虑：
         * - 效率：对每个队列，一旦遇到未超时的请求就停止检查，因为后续请求的创建时间更晚。
         * - 线程安全：ConcurrentLinkedQueue 的迭代器是弱一致性的，并且支持在迭代时通过迭代器的 remove 方法安全地移除元素。
         *   但是，这个方法本身没有外部同步，如果 ConsumerNetworkClient 中的其他操作（如 poll）并发地修改这些队列，可能会有竞态条件。
         *   通常，removeExpiredRequests 会在持有 ConsumerNetworkClient 主锁的情况下被调用，以确保一致性。
         * @param now 当前时间戳（毫秒）
         * @return 已移除的超时请求集合
         */
        private Collection<ClientRequest> removeExpiredRequests(long now) { // 定义 removeExpiredRequests 方法，移除并返回超时的请求
            List<ClientRequest> expiredRequests = new ArrayList<>(); // 创建一个列表用于存储超时的请求
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) { // 遍历所有节点的请求队列
                Iterator<ClientRequest> requestIterator = requests.iterator(); // 获取当前队列的迭代器
                while (requestIterator.hasNext()) { // 遍历队列中的请求
                    ClientRequest request = requestIterator.next(); // 获取下一个请求
                    long elapsedMs = Math.max(0, now - request.createdTimeMs()); // 计算请求自创建以来经过的时间（毫秒），确保不为负
                    if (elapsedMs > request.requestTimeoutMs()) { // 如果经过的时间大于请求的超时时间
                        expiredRequests.add(request); // 将该请求添加到超时请求列表
                        requestIterator.remove(); // 从当前队列中移除该请求
                    } else
                        // 由于请求是按时间顺序排列的，如果当前请求未超时，则后续请求也不会超时
                        break; // 跳出内部循环，处理下一个节点的队列
                }
            }
            return expiredRequests; // 返回超时请求的集合
        }

        /**
         * @brief 清理 unsent 映射，移除那些请求队列为空的节点条目。
         * @details
         * 应用场景：在某些操作（如节点断开连接后）之后，清理不再需要的空队列，以释放资源。
         * 实现细节：
         * 1. 使用 synchronized(unsent) 块来确保在清理过程中，没有新的请求被添加到正在被检查或移除的队列中，
         *    或者一个队列在从映射中移除后又被其他线程修改。
         * 2. 使用 unsent.values().removeIf(ConcurrentLinkedQueue::isEmpty) 来移除所有值（请求队列）为空的条目。
         * 设计考虑：
         * - 线程安全：通过 synchronized 块保证操作的原子性和一致性。
         * - 资源管理：及时清理空队列有助于减少内存占用。
         */
        public void clean() { // 定义 clean 方法，用于清理空的请求队列
            // 该锁保护移除操作，防止并发的 put 操作在队列从 map 中移除后又修改该队列
            synchronized (unsent) { // 同步块，锁对象是 unsent
                // 移除 unsent Map 中所有值（即 ConcurrentLinkedQueue）为空的条目
                unsent.values().removeIf(ConcurrentLinkedQueue::isEmpty);
            }
        }

        /**
         * @brief 移除并返回指定节点的所有未发送请求。
         * @details
         * 应用场景：当一个节点断开连接或不再需要向其发送请求时，调用此方法清空该节点的所有待处理请求。
         * 实现细节：
         * 1. 使用 synchronized(unsent) 块来确保在移除节点队列的过程中，没有新的请求被添加到该队列，
         *    或者该队列在从映射中移除后又被其他线程修改。
         * 2. 调用 unsent.remove(node) 从映射中移除指定节点的条目，并返回其关联的请求队列。
         * 3. 如果返回的队列为 null（即该节点原本就没有未发送的请求），则返回一个空的集合。
         * 4. 否则，返回获取到的请求队列。
         * 设计考虑：
         * - 线程安全：通过 synchronized 块保证操作的原子性和一致性。
         * - 完整性：一次性移除节点的所有请求。
         * @param node 要移除请求的目标节点
         * @return 指定节点的所有未发送请求的集合；如果节点没有请求，则返回空集合。
         */
        public Collection<ClientRequest> remove(Node node) { // 定义 remove 方法，移除并返回指定节点的所有请求
            // 该锁保护移除操作，防止并发的 put 操作在队列从 map 中移除后又修改该队列
            synchronized (unsent) { // 同步块，锁对象是 unsent
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node); // 从 unsent Map 中移除指定节点的条目，并获取其请求队列
                return requests == null ? Collections.emptyList() : requests; // 如果队列为 null，返回空列表，否则返回该队列
            }
        }

        /**
         * @brief 获取指定节点未发送请求队列的迭代器。
         * @details
         * 应用场景：当需要遍历某个特定节点的所有未发送请求时使用，例如在 poll 操作中准备发送请求。
         * 实现细节：
         * 1. 从 unsent 映射中获取指定节点的请求队列。
         * 2. 如果队列为 null，则返回一个空的迭代器。
         * 3. 否则，返回该队列的迭代器。
         * 设计考虑：
         * - 线程安全：ConcurrentLinkedQueue 的 iterator() 方法返回的迭代器是弱一致性的，可以在迭代时进行并发修改（但迭代器本身可能不反映这些修改）。
         *   通常，获取迭代器后对队列的遍历和操作会在持有外部锁（如 ConsumerNetworkClient 的主锁）的情况下进行，以确保数据的一致性视图。
         * @param node 目标节点
         * @return 指定节点未发送请求队列的迭代器；如果节点没有请求，则返回空迭代器。
         */
        public Iterator<ClientRequest> requestIterator(Node node) { // 定义 requestIterator 方法，获取指定节点请求队列的迭代器
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node); // 获取指定节点的请求队列
            return requests == null ? Collections.emptyIterator() : requests.iterator(); // 如果队列为 null，返回空迭代器，否则返回队列的迭代器
        }

        /**
         * @brief 获取当前所有存在未发送请求的节点集合。
         * @details
         * 应用场景：当需要知道哪些节点当前有待发送的请求时使用，例如在 poll 循环中确定需要向哪些节点发送数据。
         * 实现细节：直接返回 unsent 映射的键集 (keySet)。
         * 设计考虑：
         * - 线程安全：ConcurrentHashMap 的 keySet() 方法返回的是一个动态视图，对它的迭代是线程安全的。
         * @return 包含所有有未发送请求的节点的集合。
         */
        public Collection<Node> nodes() { // 定义 nodes 方法，获取所有有未发送请求的节点
            return unsent.keySet(); // 返回 unsent Map 的键集，即所有节点的集合
        }
    }

}
