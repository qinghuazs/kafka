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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * 该类负责根据以下标准确定发送下一个 {@link FindCoordinatorRequest} 的时机:
 * <p/>
 * 1. 是否存在现有协调器。
 * 2. 是否存在正在处理的请求。
 * 3. 退避计时器是否已过期。
 * {@link NetworkClientDelegate.PollResult} 包含一个等待计时器或一个 {@link NetworkClientDelegate.UnsentRequest} 的单例列表。
 * <p/>
 * {@link FindCoordinatorRequest} 将由 {@link #onResponse(long, FindCoordinatorResponse)} 回调处理，
 * 该回调随后调用 {@code onResponse} 来处理异常和响应。请注意，在收到故障时，协调器节点将被标记为 {@code null}。
 *
 * 应用场景:
 * 当消费者客户端需要与 Kafka 集群中的协调器（通常是 GroupCoordinator）进行通信以执行诸如加入消费组、提交偏移量、发送心跳等操作时，
 * 此类用于管理查找和连接到正确协调器的过程。
 *
 * 设计考虑:
 * - **状态管理**: 内部维护了协调器的状态（已知、未知、正在查找等）以及请求的退避状态，以避免过于频繁地发送查找请求。
 * - **错误处理**: 能够处理查找协调器过程中可能发生的各种错误，并在必要时进行重试。
 * - **解耦**: 将协调器查找逻辑与具体的消费者操作（如心跳、偏移量提交）解耦，使得其他需要协调器的组件可以复用此逻辑。
 */
public class CoordinatorRequestManager implements RequestManager {
    /**
     * 协调器断开连接日志记录间隔（毫秒）。
     * 用于控制在协调器持续不可用时，记录警告日志的频率。
     */
    private static final long COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS = 60 * 1000;
    /**
     * 日志记录器，用于记录该类的操作和状态信息。
     */
    private final Logger log;
    /**
     * 消费者组ID。
     * 协调器是针对特定消费者组的，因此需要此ID来查找正确的协调器。
     */
    private final String groupId;

    /**
     * 协调器请求状态。
     * 用于管理发送 {@link FindCoordinatorRequest} 的状态，包括退避逻辑和重试次数。
     */
    private final RequestState coordinatorRequestState;
    /**
     * 标记协调器未知的时间戳（毫秒）。
     * 初始值为-1L，表示协调器当前是已知的或从未尝试连接过。
     * 当协调器连接失败或断开时，会更新此时间戳。
     * 用于在协调器长时间无法连接后开始记录警告日志。
     */
    private long timeMarkedUnknownMs = -1L;
    /**
     * 协调器总计断开连接的分钟数。
     * 用于在日志中报告协调器持续不可用的时间。
     */
    private long totalDisconnectedMin = 0;
    /**
     * 标志位，指示该管理器是否正在关闭。
     * 如果为 true，则不再发送新的查找协调器请求。
     */
    private boolean closing = false;
    /**
     * 当前已知的协调器节点。
     * 如果为 null，表示当前没有已知的协调器，或者协调器已断开连接。
     */
    private Node coordinator;
    /**
     * 保存收到的最新致命错误。
     * 它被暴露出来，以便需要协调器的管理器可以访问它并采取适当的操作。
     * 例如:
     * - {@code AbstractHeartbeatRequestManager} 将错误事件传播到应用程序线程。
     * - {@code CommitRequestManager} 会使待处理的请求失败。
     */
    private Optional<Throwable> fatalError = Optional.empty();

    /**
     * 构造函数。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param retryBackoffMs 查找协调器请求的初始重试退避时间（毫秒）。
     * @param retryBackoffMaxMs 查找协调器请求的最大重试退避时间（毫秒）。
     * @param groupId 消费者组ID。
     *
     * 应用场景:
     * 在创建消费者实例时，会创建此管理器来负责与协调器的通信。
     *
     * 设计考虑:
     * - 传入退避参数允许灵活配置重试行为。
     * - groupId 是必需的，因为协调器是按组管理的。
     */
    public CoordinatorRequestManager(
        final LogContext logContext,
        final long retryBackoffMs,
        final long retryBackoffMaxMs,
        final String groupId
    ) {
        // 确保 groupId 不为 null，否则无法查找协调器
        Objects.requireNonNull(groupId);
        // 初始化日志记录器
        this.log = logContext.logger(this.getClass());
        // 设置消费者组ID
        this.groupId = groupId;
        // 初始化协调器请求状态管理器，用于控制 FindCoordinatorRequest 的发送频率和重试逻辑
        this.coordinatorRequestState = new RequestState(
                logContext, // 日志上下文
                CoordinatorRequestManager.class.getSimpleName(), // 用于日志记录的请求状态名称
                retryBackoffMs, // 初始重试退避时间
                retryBackoffMaxMs // 最大重试退避时间
        );
    }

    /**
     * 通知该管理器关闭。
     * 将 {@code closing} 标志设置为 true，这将阻止后续的协调器查找请求。
     *
     * 应用场景:
     * 当消费者关闭时，会调用此方法来停止协调器相关的活动。
     */
    @Override
    public void signalClose() {
        // 将 closing 标志设置为 true，表示管理器正在关闭
        closing = true;
    }

    /**
     * 轮询以确定是否需要发送 {@link FindCoordinatorRequest}。
     * 如果我们不需要发现协调器（例如，协调器已知或正在关闭），此方法将返回一个包含 {@link Long#MAX_VALUE} 退避时间和空列表的 {@link NetworkClientDelegate.PollResult}。
     * 如果我们仍在从先前的尝试中进行退避，此方法将返回一个包含剩余退避时间和空列表的 {@link NetworkClientDelegate.PollResult}。
     * 否则，此方法将返回一个包含单个 {@link NetworkClientDelegate.UnsentRequest} 和 {@link Long#MAX_VALUE} 退避时间的 {@link NetworkClientDelegate.PollResult}。
     * 请注意，此方法不涉及任何实际的网络IO，它仅确定我们是否需要发送新请求。
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @return {@link NetworkClientDelegate.PollResult}。此返回值不会为 {@code null}。
     *
     * 应用场景:
     * 在消费者的主轮询循环中，会定期调用此方法来检查是否需要查找协调器。
     *
     * 设计考虑:
     * - **非阻塞**: 此方法本身不执行网络操作，只是决策是否需要发送请求。
     * - **退避逻辑**: 集成了退避机制，避免在连接失败时过于频繁地重试。
     * - **状态驱动**: 根据当前协调器的状态（已知/未知）和请求状态（是否可以发送）来决定行为。
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // 如果正在关闭或协调器已经找到，则不需要发送查找请求
        if (closing || this.coordinator != null)
            // 返回一个空的 PollResult，表示没有请求要发送，并且退避时间为最大值
            return EMPTY;

        // 检查是否可以发送查找协调器的请求（即退避时间已过且没有正在进行的请求）
        if (coordinatorRequestState.canSendRequest(currentTimeMs)) {
            // 如果可以发送，则创建一个 FindCoordinatorRequest
            NetworkClientDelegate.UnsentRequest request = makeFindCoordinatorRequest(currentTimeMs);
            // 返回包含该请求的 PollResult
            return new NetworkClientDelegate.PollResult(request);
        }

        // 如果当前不能发送请求（例如，仍在退避期），则返回一个包含剩余退避时间的 PollResult
        return new NetworkClientDelegate.PollResult(coordinatorRequestState.remainingBackoffMs(currentTimeMs));
    }

    /**
     * 创建一个 {@link FindCoordinatorRequest}。
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @return {@link NetworkClientDelegate.UnsentRequest} 包含构建好的查找协调器请求。
     *
     * 应用场景:
     * 当 {@link #poll(long)} 方法确定需要发送查找协调器请求时，会调用此方法来实际构建请求。
     *
     * 设计考虑:
     * - **请求构建**: 封装了 {@link FindCoordinatorRequest} 的构建逻辑，包括设置协调器类型和组ID。
     * - **回调处理**: 为请求设置了完成时的回调逻辑，用于处理成功响应或失败异常。
     */
    NetworkClientDelegate.UnsentRequest makeFindCoordinatorRequest(final long currentTimeMs) {
        // 标记协调器请求状态为已尝试发送，并更新上次发送时间
        coordinatorRequestState.onSendAttempt(currentTimeMs);
        // 创建 FindCoordinatorRequest 的请求数据
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                // 设置协调器类型为 GROUP，因为这是用于消费者组的协调器
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                // 设置要查找的协调器的键，即消费者组ID
                .setKey(this.groupId);
        // 创建一个未发送的请求对象
        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
            // 使用请求数据构建 FindCoordinatorRequest.Builder
            new FindCoordinatorRequest.Builder(data),
            // 此处 Optional.empty() 表示没有特定的目标节点，请求将发送到随机节点或引导节点
            Optional.empty()
        );

        // 为请求设置完成时的回调逻辑
        return unsentRequest.whenComplete((clientResponse, throwable) -> {
            // 清除之前可能存在的致命错误信息
            getAndClearFatalError();
            // 检查请求是否成功收到响应
            if (clientResponse != null) {
                // 如果收到响应，将其转换为 FindCoordinatorResponse 类型
                FindCoordinatorResponse response = (FindCoordinatorResponse) clientResponse.responseBody();
                // 调用 onResponse 方法处理响应
                onResponse(clientResponse.receivedTimeMs(), response);
            } else {
                // 如果请求失败（例如，发生网络错误或超时），调用 onFailedResponse 方法处理异常
                // unsentRequest.handler().completionTimeMs() 获取请求完成的时间戳
                onFailedResponse(unsentRequest.handler().completionTimeMs(), throwable);
            }
        });
    }

    /**
     * 处理当前协调器的断开连接。
     * 此方法检查给定的异常是否是 {@link DisconnectException} 的实例。
     * 如果是，则将协调器标记为未知，表示客户端应尝试发现新的协调器。对于任何其他异常类型，不执行任何操作。
     *
     * @param exception     要处理的异常，该异常是作为请求响应的一部分接收的。
     * @param currentTimeMs 当前时间（毫秒）。
     *
     * 应用场景:
     * 当与协调器的连接因网络问题或其他原因中断时，调用此方法来更新协调器状态并准备重新发现。
     *
     * 设计考虑:
     * - 只处理 {@link DisconnectException} 类型的异常，因为这是明确表示连接断开的信号。
     * - 其他类型的异常可能表示其他问题，不应在此处简单地将协调器标记为未知。
     */
    public void handleCoordinatorDisconnect(Throwable exception, long currentTimeMs) {
        // 检查异常是否为 DisconnectException 的实例
        if (exception instanceof DisconnectException) {
            // 如果是 DisconnectException，则调用 markCoordinatorUnknown 方法将协调器标记为未知
            // 并传递异常消息和当前时间作为参数
            markCoordinatorUnknown(exception.getMessage(), currentTimeMs);
        }
        // 如果异常不是 DisconnectException 类型，则不执行任何操作
    }

    /**
     * 当检测到断开连接时，将协调器标记为“未知”（即 {@code null}）。此检测可能通过以下两种路径之一发生：
     *
     * <ol>
     *     <li>协调器已被发现，但随后断开连接</li>
     *     <li>协调器尚未被发现和/或连接</li>
     * </ol>
     *
     * @param cause         将协调器标记为未知的原因的字符串说明。
     * @param currentTimeMs 当前时间（毫秒）。
     *
     * 应用场景:
     * - 在与协调器的连接丢失时调用（例如，收到 DisconnectException）。
     * - 在查找协调器请求失败时调用。
     * - 在初始启动或协调器信息失效后，需要重新发现协调器时。
     *
     * 设计考虑:
     * - 记录协调器变为未知的时间戳，用于后续的日志记录和可能的超时逻辑。
     * - 如果协调器之前是已知的，会记录一条 INFO 级别的日志，说明协调器不可用以及原因。
     * - 如果协调器之前就是未知的（例如，在持续断开连接期间），则会根据配置的间隔记录 DEBUG 级别的日志，报告持续断开的时间。
     * - 将协调器实例设置为 null，表示当前没有已知的协调器。
     */
    public void markCoordinatorUnknown(final String cause, final long currentTimeMs) {
        // 如果协调器之前是已知的 (coordinator != null)，或者这是第一次将协调器标记为未知 (timeMarkedUnknownMs == -1)
        if (coordinator != null || timeMarkedUnknownMs == -1) {
            // 更新协调器被标记为未知的时间戳
            timeMarkedUnknownMs = currentTimeMs;
            // 重置总断开连接分钟数，因为这是一个新的断开事件或首次标记未知
            totalDisconnectedMin = 0;
        }

        // 检查协调器之前是否已知
        if (coordinator != null) {
            // 如果协调器之前是已知的，记录一条 INFO 级别的日志，说明协调器现在不可用以及原因
            log.info(
                "组协调器 {} 由于原因: {} 而不可用或无效。将尝试重新发现。",
                coordinator,
                cause
            );
            // 将协调器实例设置为 null，表示当前没有已知的协调器
            coordinator = null;
        } else {
            // 如果协调器之前就是未知的（例如，在持续断开连接期间）
            // 计算从上次标记未知到现在的持续断开时间
            long durationOfOngoingDisconnectMs = Math.max(0, currentTimeMs - timeMarkedUnknownMs);
            // 将持续断开时间转换为分钟数（基于日志记录间隔）
            long currDisconnectMin = durationOfOngoingDisconnectMs / COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS;
            // 如果当前的断开分钟数超过了已记录的总断开分钟数（意味着达到了新的日志记录阈值）
            if (currDisconnectMin > totalDisconnectedMin) {
                // 记录一条 DEBUG 级别的日志，报告消费者已与组协调器断开连接的持续时间
                log.debug("消费者已与组协调器断开连接 {} 毫秒", durationOfOngoingDisconnectMs);
                // 更新总断开分钟数
                totalDisconnectedMin = currDisconnectMin;
            }
        }
    }

    /**
     * 处理成功的 FindCoordinator 响应。
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @param coordinator   从响应中获取的协调器信息。
     *
     * 应用场景:
     * 当 {@link FindCoordinatorRequest} 成功返回并且找到了协调器时调用此方法。
     *
     * 设计考虑:
     * - 使用 `Integer.MAX_VALUE - coordinator.nodeId()` 作为协调器连接ID，是为了在底层的网络客户端层为协调器创建单独的连接，
     *   避免与其他 broker 的连接混淆或冲突。
     * - 更新本地的协调器节点信息。
     * - 记录 INFO 级别的日志，表明已发现组协调器。
     * - 通知 `coordinatorRequestState` 本次尝试成功，这会重置退避计时器。
     */
    private void onSuccessfulResponse(
        final long currentTimeMs,
        final FindCoordinatorResponseData.Coordinator coordinator
    ) {
        // 使用 MAX_VALUE - node.id 作为协调器连接ID，以允许在底层网络客户端层为协调器建立单独的连接
        // 这样做可以避免与常规数据 broker 的连接产生潜在的冲突或管理上的混淆
        int coordinatorConnectionId = Integer.MAX_VALUE - coordinator.nodeId();
        // 创建一个新的 Node 对象来表示发现的协调器
        this.coordinator = new Node(
                coordinatorConnectionId, // 协调器的连接ID
                coordinator.host(),      // 协调器的主机名
                coordinator.port()       // 协调器的端口号
        );
        // 记录一条 INFO 级别的日志，表明已成功发现组协调器及其信息
        log.info("已发现组协调器 {}", this.coordinator);
        // 通知 coordinatorRequestState 本次查找协调器的尝试成功
        // 这通常会重置退避计时器，以便在需要时可以立即发送下一个请求
        coordinatorRequestState.onSuccessfulAttempt(currentTimeMs);
    }

    /**
     * 处理失败的 FindCoordinator 响应或请求本身发生的异常。
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @param exception     导致失败的异常。
     *
     * 应用场景:
     * - 当 {@link FindCoordinatorRequest} 返回的响应中包含错误码时调用。
     * - 当发送 {@link FindCoordinatorRequest} 或处理其响应时发生运行时异常时调用。
     *
     * 设计考虑:
     * - 首先通知 `coordinatorRequestState` 本次尝试失败，这会启动或增加退避计时器。
     * - 调用 `markCoordinatorUnknown` 将协调器标记为未知，因为查找失败了。
     * - 根据异常类型进行不同的处理：
     *   - 如果是可重试异常 ({@link RetriableException})，记录 DEBUG 日志并返回，等待下次重试。
     *   - 如果是组授权失败 ({@link Errors#GROUP_AUTHORIZATION_FAILED})，记录 DEBUG 日志，并设置一个特定的 {@link GroupAuthorizationException} 为致命错误。
     *   - 对于其他类型的异常，视为致命错误，记录 WARN 日志，并将该异常设置为致命错误。
     * - 致命错误 (`fatalError`) 会被其他管理器（如心跳管理器或提交管理器）检查，并可能导致消费者停止或向应用程序抛出异常。
     */
    private void onFailedResponse(final long currentTimeMs, final Throwable exception) {
        // 通知 coordinatorRequestState 本次查找协调器的尝试失败
        // 这通常会启动或增加退避计时器，以便在下次尝试前等待一段时间
        coordinatorRequestState.onFailedAttempt(currentTimeMs);
        // 将协调器标记为未知，因为查找协调器的请求失败了
        markCoordinatorUnknown("FindCoordinator 请求因异常失败", currentTimeMs);

        // 检查异常是否为可重试异常
        if (exception instanceof RetriableException) {
            // 如果是可重试异常，记录 DEBUG 级别的日志并返回
            // 管理器将在退避期过后自动重试
            log.debug("FindCoordinator 请求由于可重试异常失败", exception);
            return;
        }

        // 检查异常是否为组授权失败错误
        if (exception == Errors.GROUP_AUTHORIZATION_FAILED.exception()) {
            // 如果是组授权失败，记录 DEBUG 级别的日志
            log.debug("FindCoordinator 请求由于授权错误 {} 失败", exception.getMessage());
            // 创建一个 GroupAuthorizationException，并将其设置为致命错误
            KafkaException groupAuthorizationException = GroupAuthorizationException.forGroupId(this.groupId);
            fatalError = Optional.of(groupAuthorizationException);
            return;
        }

        // 对于其他类型的异常，视为致命错误
        // 记录 WARN 级别的日志
        log.warn("FindCoordinator 请求由于致命异常失败", exception);
        // 将该异常设置为致命错误
        fatalError = Optional.of(exception);
    }

    /**
     * 在 {@link FindCoordinatorRequest} 的 future 成功返回后处理响应。
     * 此方法仍必须解开响应对象以检查协议错误。
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @param response      查找协调器的响应。如果抛出异常，则为 null。
     *
     * 应用场景:
     * 当网络客户端成功接收到 {@link FindCoordinatorResponse} 后，会调用此方法。
     * 这是处理协调器查找结果的主要入口点。
     *
     * 设计考虑:
     * - 首先从响应中按 groupId 提取协调器信息。
     * - 如果响应中没有包含预期的协调器部分，则认为是非法状态，调用 `onFailedResponse` 处理。
     * - 获取协调器节点信息后，检查其错误码：
     *   - 如果错误码不是 `Errors.NONE`，表示查找失败，调用 `onFailedResponse` 并传入对应的异常。
     *   - 如果错误码是 `Errors.NONE`，表示查找成功，调用 `onSuccessfulResponse` 处理。
     */
    private void onResponse(
        final long currentTimeMs,
        final FindCoordinatorResponse response
    ) {
        // 处理运行时异常（例如，如果 response 为 null，这里会抛出 NullPointerException，但通常调用者会确保 response 不为 null）
        // 从响应中根据 groupId 查找对应的协调器信息
        Optional<FindCoordinatorResponseData.Coordinator> coordinatorOpt = response.coordinatorByKey(this.groupId);
        // 检查是否找到了协调器信息
        if (coordinatorOpt.isEmpty()) {
            // 如果响应中没有包含指定 groupId 的协调器部分，则构造错误消息
            String msg = String.format("响应中未包含 groupId: %s 的预期协调器部分", this.groupId);
            // 调用 onFailedResponse 处理此错误，将其视为非法状态异常
            onFailedResponse(currentTimeMs, new IllegalStateException(msg));
            // 处理完毕，返回
            return;
        }

        // 获取协调器节点信息
        FindCoordinatorResponseData.Coordinator node = coordinatorOpt.get();
        // 检查协调器节点返回的错误码
        if (node.errorCode() != Errors.NONE.code()) {
            // 如果错误码不是 NONE，表示查找协调器时发生了错误
            // 将错误码转换为对应的异常，并调用 onFailedResponse 处理
            onFailedResponse(currentTimeMs, Errors.forCode(node.errorCode()).exception());
            // 处理完毕，返回
            return;
        }
        // 如果错误码为 NONE，表示成功找到协调器
        // 调用 onSuccessfulResponse 处理成功的响应
        onSuccessfulResponse(currentTimeMs, node);
    }

    /**
     * 返回当前协调器节点。
     *
     * @return 当前协调器节点。如果协调器未知或未连接，则返回 {@link Optional#empty()}。
     *
     * 应用场景:
     * 其他管理器（如心跳管理器、偏移量提交管理器）需要知道当前的协调器节点以便向其发送请求时，会调用此方法。
     *
     * 设计考虑:
     * - 使用 {@link Optional} 来优雅地处理协调器可能为 `null` 的情况。
     */
    public Optional<Node> coordinator() {
        // 返回一个 Optional 对象，其中包含当前的协调器节点
        // 如果 this.coordinator 为 null（即协调器未知），则 Optional.ofNullable 会返回一个空的 Optional
        return Optional.ofNullable(this.coordinator);
    }
    /**
     * 获取并清除最近发生的致命错误。
     * 此方法用于一次性获取错误，获取后错误状态将被清除。
     *
     * @return 包含致命错误的 {@link Optional}，如果没有致命错误，则返回 {@link Optional#empty()}。
     *
     * 应用场景:
     * - 消费者主循环或相关管理器在检测到协调器查找失败后，可能会调用此方法来获取具体的错误原因，
     *   并决定是否需要向上传播异常或采取其他恢复措施。
     * - 例如，心跳管理器可能会获取此错误并将其传播到应用程序线程。
     *
     * 设计考虑:
     * - “获取并清除”模式确保错误只被处理一次。
     * - 使用 {@link Optional} 来表示可能不存在的错误。
     */
    public Optional<Throwable> getAndClearFatalError() {
        // 将当前存储的 fatalError 赋值给一个局部变量
        Optional<Throwable> fatalErrorToReturn = this.fatalError;
        // 清除当前存储的 fatalError，将其设置为空的 Optional
        this.fatalError = Optional.empty();
        // 返回之前存储的 fatalError
        return fatalErrorToReturn;
    }

    /**
     * 返回最近发生的致命错误，但不清除它。
     * 此方法用于检查是否存在致命错误，而不会改变错误状态。
     *
     * @return 包含致命错误的 {@link Optional}，如果没有致命错误，则返回 {@link Optional#empty()}。
     *
     * 应用场景:
     * - 用于轮询检查是否存在致命错误，而不立即处理它。
     * - 某些组件可能需要多次检查错误状态。
     *
     * 设计考虑:
     * - 与 `getAndClearFatalError` 不同，此方法是非破坏性的读取。
     * - 使用 {@link Optional} 来表示可能不存在的错误。
     */
    public Optional<Throwable> fatalError() {
        // 返回当前存储的 fatalError
        // 此方法不会清除 fatalError 状态
        return fatalError;
    }
}
