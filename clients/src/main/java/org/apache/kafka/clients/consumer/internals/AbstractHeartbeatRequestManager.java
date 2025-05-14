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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.Collections;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * <p>管理心跳的请求创建和响应处理。该模块使用存储在成员资格管理器中的状态创建一个心跳请求，
 * 并将其排队到网络队列以发送出去。一旦收到响应，它会更新成员资格管理器中的状态并处理任何错误。
 *
 * <p>心跳管理器根据成员状态生成心跳请求。它还负责心跳请求的时间安排，
 * 以确保它们根据心跳间隔（当成员状态稳定时）发送，或按需（当成员正在确认分配或离开组时）发送。
 *
 * <p>如果成员被踢出组，它将尝试通过调用 {@code OnPartitionsLost} 来放弃当前分配，
 * 然后再尝试以零 epoch 重新加入。
 *
 * <p>如果找不到协调器，我们将跳过发送心跳并首先尝试查找协调器。
 *
 * <p>当成员完成分配协调后，{@link HeartbeatRequestState} 将被重置，
 * 以便在下一个事件循环中发送心跳。
 *
 * <p>类变量 R 是特定组心跳 RPC 的响应。
 */
public abstract class AbstractHeartbeatRequestManager<R extends AbstractResponse> implements RequestManager {

    /**
     * 日志记录器，用于记录此类的相关信息。
     */
    protected final Logger logger;

    /**
     * 组协调器等待成员撤销其分区的最长时间（毫秒）。这是由组协调器在心跳中提供的。
     * 应用场景：控制消费者在两次 poll 调用之间的最大时间间隔，如果超过此时间，协调器会认为消费者已死并触发再均衡。
     * 实现细节：从配置 {@link CommonClientConfigs#MAX_POLL_INTERVAL_MS_CONFIG} 中获取。
     * 设计考虑：防止消费者长时间不 poll 导致分区无法及时释放，影响组的稳定性。
     */
    protected final int maxPollIntervalMs;

    /**
     * 协调器请求管理器，负责管理与组协调器的连接。
     * 应用场景：发送查找协调器请求、心跳请求等。
     * 实现细节：通过构造函数注入。
     * 设计考虑：将协调器相关的网络通信逻辑封装起来，便于管理和复用。
     */
    protected final CoordinatorRequestManager coordinatorRequestManager;

    /**
     * 心跳请求状态管理器，负责管理心跳请求的时间安排和重试逻辑。
     * 应用场景：控制心跳发送的频率、处理发送失败后的重试策略。
     * 实现细节：内部维护心跳间隔、重试退避时间等状态。
     * 设计考虑：将心跳发送的复杂逻辑（如定时、退避、抖动）封装起来，简化主流程。
     */
    private final HeartbeatRequestState heartbeatRequestState;

    /**
     * 错误事件处理器，允许后台线程将错误传播回用户。
     * 应用场景：当心跳或协调器相关的操作发生严重错误时，通过此处理器通知上层应用。
     * 实现细节：通过构造函数注入，通常是一个事件队列的生产者。
     * 设计考虑：解耦错误处理与业务逻辑，使得错误可以异步地被上层感知和处理。
     */
    private final BackgroundEventHandler backgroundEventHandler;

    /**
     * 用于跟踪自上次消费者轮询以来时间的计时器。如果计时器到期，消费者将停止发送心跳，直到下一次轮询。
     * 应用场景：确保消费者在 `max.poll.interval.ms` 内调用 `poll()` 方法，否则认为消费者可能卡住或死亡。
     * 实现细节：使用 Kafka 提供的 {@link Time#timer(long)} 创建。
     * 设计考虑：这是消费者活性检测的关键机制之一，超时会导致消费者被踢出组。
     */
    private final Timer pollTimer;

    /**
     * 持有心跳传感器，用于测量心跳时间和响应延迟。
     * 应用场景：收集和报告心跳相关的性能指标，如心跳成功率、平均延迟等。
     * 实现细节：通过构造函数注入，通常与 Metrics 模块集成。
     * 设计考虑：提供可观测性，帮助监控和诊断心跳机制的健康状况。
     */
    private final HeartbeatMetricsManager metricsManager;

    /**
     * 当集群不支持新的 CONSUMER 组协议时显示的错误消息。
     * 提示用户在消费者配置中设置 `group.protocol=classic` 以恢复到 CLASSIC 协议，直到集群升级。
     */
    public static final String CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG = "The cluster does not support the new CONSUMER " +
        "group protocol. Set group.protocol=classic on the consumer configs to revert to the CLASSIC protocol " +
        "until the cluster is upgraded.";

    /**
     * 构造函数，用于创建 {@link AbstractHeartbeatRequestManager} 实例。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param time 时间工具，用于获取当前时间和创建计时器。
     * @param config 消费者配置，包含如 `max.poll.interval.ms`、`retry.backoff.ms` 等参数。
     * @param coordinatorRequestManager 协调器请求管理器，用于与组协调器通信。
     * @param backgroundEventHandler 后台事件处理器，用于将错误事件传播给用户。
     * @param metricsManager 心跳指标管理器，用于收集和报告心跳相关的指标。
     * 应用场景：在消费者客户端初始化时创建心跳管理器。
     * 设计考虑：通过依赖注入的方式传入必要的组件，使得类更容易测试和维护。
     */
    AbstractHeartbeatRequestManager(
            final LogContext logContext,
            final Time time,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final BackgroundEventHandler backgroundEventHandler,
            final HeartbeatMetricsManager metricsManager) {
        // 初始化协调器请求管理器
        this.coordinatorRequestManager = coordinatorRequestManager;
        // 初始化日志记录器，使用当前类的名称
        this.logger = logContext.logger(getClass());
        // 初始化后台事件处理器
        this.backgroundEventHandler = backgroundEventHandler;
        // 从配置中获取 max.poll.interval.ms 的值，并初始化 maxPollIntervalMs 字段
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        // 从配置中获取 retry.backoff.ms 的值
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        // 从配置中获取 retry.backoff.max.ms 的值
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        // 初始化心跳请求状态管理器，设置初始心跳间隔为0（表示立即发送），以及重试退避参数
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
                retryBackoffMaxMs, maxPollIntervalMs);
        // 初始化 poll 计时器，超时时间为 maxPollIntervalMs
        this.pollTimer = time.timer(maxPollIntervalMs);
        // 初始化心跳指标管理器
        this.metricsManager = metricsManager;
    }

    /**
     * 构造函数（主要用于测试），允许传入自定义的 {@link Timer} 和 {@link HeartbeatRequestState}。
     *
     * @param logContext 日志上下文。
     * @param timer 自定义的计时器，用于 poll 超时检测。
     * @param config 消费者配置。
     * @param coordinatorRequestManager 协调器请求管理器。
     * @param heartbeatRequestState 自定义的心跳请求状态管理器。
     * @param backgroundEventHandler 后台事件处理器。
     * @param metricsManager 心跳指标管理器。
     * 应用场景：单元测试中，可以方便地模拟时间和心跳状态，验证心跳管理器的逻辑。
     * 设计考虑：提供一个更灵活的构造函数，便于进行细粒度的测试控制。
     */
    AbstractHeartbeatRequestManager(
            final LogContext logContext,
            final Timer timer,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler,
            final HeartbeatMetricsManager metricsManager) {
        // 初始化日志记录器，使用当前类的名称
        this.logger = logContext.logger(this.getClass());
        // 从配置中获取 max.poll.interval.ms 的值，并初始化 maxPollIntervalMs 字段
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        // 初始化协调器请求管理器
        this.coordinatorRequestManager = coordinatorRequestManager;
        // 初始化心跳请求状态管理器（使用传入的实例）
        this.heartbeatRequestState = heartbeatRequestState;
        // 初始化后台事件处理器
        this.backgroundEventHandler = backgroundEventHandler;
        // 初始化 poll 计时器（使用传入的实例）
        this.pollTimer = timer;
        // 初始化心跳指标管理器
        this.metricsManager = metricsManager;
    }

    /**
     * 此方法将根据成员状态构建一个心跳请求（如果必须发送）。在以下情况下会发送心跳：
     * <ol>
     *     <li>成员是消费者组的一部分或想要加入该组。</li>
     *     <li>心跳间隔已到期，或者成员处于指示其应立即发送心跳而无需等待间隔的状态。</li>
     * </ol>
     * 此方法还将根据成员的状态确定下一次轮询前的最大等待时间。
     * <ol>
     *     <li>如果成员没有协调器或处于失败状态，则计时器设置为 Long.MAX_VALUE，因为不需要发送心跳。</li>
     *     <li>如果成员由于指数退避而无法发送心跳，则将返回退避计时器上剩余的时间。</li>
     *     <li>如果成员的心跳计时器尚未到期，则将返回心跳计时器上剩余的时间。</li>
     *     <li>如果成员可以发送心跳，则计时器设置为当前心跳间隔。</li>
     * </ol>
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @return {@link PollResult} 包含一个心跳请求（如果必须发送），以及下一次轮询前要等待的时间。
     * 应用场景：在消费者事件循环中定期调用，以维持与协调器的连接并处理组成员关系变化。
     * 实现细节：检查协调器状态、poll 超时、成员状态，并据此决定是否发送心跳以及发送何种心跳（常规或离开组）。
     * 设计考虑：确保消费者及时响应协调器的指令，并在必要时发送心跳以表明其活性或意图（如离开组）。
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        // 检查协调器是否已知，或者成员是否应跳过当前心跳
        if (coordinatorRequestManager.coordinator().isEmpty() || membershipManager().shouldSkipHeartbeat()) {
            // 如果协调器未知或应跳过心跳，则调用成员资格管理器的 onHeartbeatRequestSkipped 方法
            membershipManager().onHeartbeatRequestSkipped();
            // 检查并传播协调器相关的致命错误事件
            maybePropagateCoordinatorFatalErrorEvent();
            // 返回一个空的 PollResult，表示没有请求要发送，并且等待时间由其他机制决定
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        // 更新 poll 计时器，记录当前时间
        pollTimer.update(currentTimeMs);
        // 检查 poll 计时器是否已到期，并且成员当前没有处于离开组的过程中
        if (pollTimer.isExpired() && !membershipManager().isLeavingGroup()) {
            // 如果 poll 超时，记录警告信息，说明两次 poll 调用之间的时间超过了 max.poll.interval.ms
            logger.warn("Consumer poll timeout has expired. This means the time between " +
                "subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time processing " +
                "messages. You can address this either by increasing max.poll.interval.ms or by " +
                "reducing the maximum size of batches returned in poll() with max.poll.records.");

            // 将成员状态转换为发送离开组请求的状态，并标记为由于 poll 超时
            membershipManager().transitionToSendingLeaveGroup(true);
            // 创建一个离开组的心跳请求，忽略响应
            NetworkClientDelegate.UnsentRequest leaveHeartbeat = makeHeartbeatRequest(currentTimeMs, true);

            // 我们可以忽略离开响应，因为我们可以在收到响应之前或之后加入组。
            // 重置心跳请求状态
            heartbeatRequestState.reset();
            // 重置心跳状态（特定于子类的实现）
            resetHeartbeatState();
            // 返回包含离开组心跳请求的 PollResult，并设置下一次 poll 的等待时间为当前心跳间隔
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(leaveHeartbeat));
        }

        // 情况1：成员正在离开组
        boolean heartbeatNow = membershipManager().state() == MemberState.LEAVING ||
            // 情况2：成员状态指示应立即发送心跳（无需等待间隔），并且当前没有正在进行的心跳请求
            (membershipManager().shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight());

        // 如果当前不能发送请求（例如，由于退避或心跳间隔未到），并且不是立即发送心跳的情况
        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            // 返回一个 PollResult，指示下一次可以发送心跳的时间
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
        }

        // 创建一个常规的心跳请求，不忽略响应
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, false);
        // 返回包含心跳请求的 PollResult，并设置下一次 poll 的等待时间为当前心跳间隔
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    /**
     * 返回此请求管理器用于跟踪组状态的 {@link AbstractMembershipManager}。
     * 提供此方法是为了让 {@link ApplicationEventProcessor} 可以访问状态以进行查询或更新。
     * @return {@link AbstractMembershipManager} 实例。
     * 应用场景：允许其他组件（如事件处理器）获取和操作组成员信息。
     * 设计考虑：这是一个抽象方法，具体的成员管理器由子类提供，体现了模板方法模式。 
     */
    public abstract AbstractMembershipManager<R> membershipManager();

    /**
     * 如果在调用此方法关闭消费者时状态仍为 LEAVING，则生成一个离开组的心跳请求。
     * <p/>
     * 请注意，在关闭消费者时，即使生成了取消订阅事件（触发回调并发送离开组请求），
     * 取消订阅事件处理也可能无法及时完成，并继续关闭管理器（例如，以零超时调用 close）。
     * 因此，我们最终可能会在此 pollOnClose 方法中遇到成员处于 {@link MemberState#PREPARE_LEAVING} 状态
     * （例如，应用程序线程没有时间处理事件以执行回调），或 {@link MemberState#LEAVING} 状态
     * （例如，由于当时协调器不可用而无法发送离开请求）。在所有情况下，pollOnClose
     * 将在发送最终请求之前触发，因此我们确保在需要时生成离开请求。
     *
     * @param currentTimeMs 调用该方法时的当前系统时间（毫秒）。
     * @return 包含要发送的请求的 PollResult。
     * 应用场景：在消费者关闭流程中，确保发送一个明确的离开组请求，以便协调器能够及时清理资源。
     * 实现细节：检查成员是否处于离开组的状态，如果是，则构建并返回一个离开组的心跳请求。
     * 设计考虑：处理关闭时的边缘情况，确保即使之前的离开操作未完成，也能尝试发送最后的离开请求。
     */
    @Override
    public PollResult pollOnClose(long currentTimeMs) {
        // 检查成员是否正在离开组
        if (membershipManager().isLeavingGroup()) {
            // 如果是，创建一个离开组的心跳请求，忽略响应
            NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, true);
            // 返回包含离开组心跳请求的 PollResult
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
        }
        // 如果成员没有离开组，则返回一个空的 PollResult
        return EMPTY;
    }

    /**
     * 返回应用程序线程可以安全等待的延迟时间，在此之后它应该对请求管理器的结果做出响应。
     * 例如，当发送心跳时，订阅状态可能会改变，因此阻塞时间超过心跳间隔可能意味着应用程序线程对更改不敏感。
     *
     * <p>类似地，我们可能需要解除应用程序线程的阻塞，以发送一个 `PollApplicationEvent`，确保我们的轮询计时器在轮询时不会过期。
     *
     * <p>如果当前正在跳过心跳，此方法仍会返回下一个心跳延迟，而不是 {@code Long.MAX_VALUE}，以便应用程序线程保持响应。
     * @param currentTimeMs 当前时间（毫秒）。
     * @return 应用程序线程可以安全等待的最大时间（毫秒）。
     * 应用场景：供上层调用者（如消费者主循环）决定其阻塞操作的最长等待时间，以确保及时处理心跳相关的事件和状态更新。
     * 实现细节：综合考虑 poll 计时器的剩余时间以及下一次心跳发送的时间。
     * 设计考虑：平衡阻塞等待的效率和对心跳机制响应的及时性。
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        // 更新 poll 计时器
        pollTimer.update(currentTimeMs);
        // 如果 poll 计时器已到期，或者成员应立即发送心跳且当前没有正在进行的心跳请求
        if (pollTimer.isExpired() || (membershipManager().shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight())) {
            // 则返回0，表示不应等待，需要立即处理
            return 0L;
        }
        // 否则，返回 poll 计时器剩余时间的一半与下一次心跳发送时间的较小值
        // 除以2是为了更早地唤醒，以便有机会发送 PollApplicationEvent，防止 pollTimer 过期
        return Math.min(pollTimer.remainingMs() / 2, heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
    }

    /**
     * 重置 poll 计时器，表示用户已调用 consumer.poll()。如果成员由于 poll 计时器到期而处于 {@link MemberState#STALE} 状态，
     * 这会将成员转换为 {@link MemberState#JOINING} 状态，以便它重新加入组。
     * @param pollMs 调用 poll() 的时间戳（毫秒）。
     * 应用场景：每次消费者成功调用 `poll()` 方法后，调用此方法来重置其活性计时器。
     * 实现细节：更新 poll 计时器，如果发现计时器已过期（表示之前的 poll 间隔过长），则记录警告并尝试让成员重新加入组。
     * 设计考虑：这是消费者活性管理的核心部分，确保消费者在 `max.poll.interval.ms` 内保持活跃。
     */
    public void resetPollTimer(final long pollMs) {
        // 使用传入的 pollMs 更新 poll 计时器
        pollTimer.update(pollMs);
        // 检查 poll 计时器是否已到期
        if (pollTimer.isExpired()) {
            // 如果已到期，记录警告信息，指出两次 poll 调用之间的时间超过了配置的 max.poll.interval.ms
            logger.warn("Time between subsequent calls to poll() was longer than the configured " +
                "max.poll.interval.ms, exceeded approximately by {} ms. Member {} will rejoin the group now.",
                pollTimer.isExpiredBy(), membershipManager().memberId());
            // 尝试让过期的成员重新加入组
            membershipManager().maybeRejoinStaleMember();
        }
        // 重置 poll 计时器，将其超时时间设置为 maxPollIntervalMs
        pollTimer.reset(maxPollIntervalMs);
    }

    /**
     * 检查并传播协调器请求管理器中可能发生的致命错误事件。
     * 应用场景：在某些操作（如跳过心跳）后，检查是否有协调器相关的严重错误需要通知上层应用。
     * 实现细节：从协调器请求管理器获取并清除致命错误，如果存在，则通过后台事件处理器将其包装成 ErrorEvent 并添加。
     * 设计考虑：确保协调器层面的严重问题能够被及时捕获并传递给应用程序进行处理。
     */
    private void maybePropagateCoordinatorFatalErrorEvent() {
        // 从协调器请求管理器获取并清除致命错误
        coordinatorRequestManager.getAndClearFatalError()
                // 如果存在致命错误，则将其包装成 ErrorEvent 并添加到后台事件处理器
                .ifPresent(fatalError -> backgroundEventHandler.add(new ErrorEvent(fatalError)));
    }

    /**
     * 创建一个心跳请求，并更新相关的状态和指标。
     *
     * @param currentTimeMs 当前时间（毫秒）。
     * @param ignoreResponse 是否忽略此心跳请求的响应（例如，在发送离开组请求时）。
     * @return 构建的未发送的心跳请求。
     * 应用场景：当需要发送心跳时（无论是常规心跳还是离开组心跳），调用此方法来实际构建请求并进行必要的簿记。
     * 实现细节：调用抽象方法 `buildHeartbeatRequest()` 来获取实际的请求对象，然后更新心跳请求状态、通知成员管理器、记录指标并重置心跳计时器。
     * 设计考虑：将心跳请求的创建与发送前的准备工作（状态更新、指标记录）封装在一起。
     */
    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs, final boolean ignoreResponse) {
        // 调用（可能是重载的）makeHeartbeatRequest 方法来构建实际的请求，这里假设存在一个只接受 ignoreResponse 的版本或 buildHeartbeatRequest() 内部处理了
        // 注意：原始代码中这里调用的是 makeHeartbeatRequest(ignoreResponse)，这似乎是一个递归调用或调用了另一个同名方法。
        // 假设这里意图是调用 buildHeartbeatRequest() 并根据 ignoreResponse 设置请求属性，或者调用一个专门的构建方法。
        // 为了与原始结构保持一致并假设存在一个 `protected abstract NetworkClientDelegate.UnsentRequest buildHeartbeatRequest();`
        // 以及一个 `private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(boolean ignoreResponse)` 的辅助方法，
        // 或者 `buildHeartbeatRequest()` 内部会处理 `ignoreResponse`。
        // 鉴于上下文，更可能是调用 `buildHeartbeatRequest()`，然后根据 `ignoreResponse` 可能调整请求属性或处理方式。
        // 但为了精确匹配原始签名，我们假设存在一个 `makeHeartbeatRequest(boolean)` 的重载，或者 `buildHeartbeatRequest()` 会被间接调用。
        // 实际的 Kafka 代码中，通常是 `buildHeartbeatRequest()` 返回请求，然后在这里包装或处理。
        // 我们将遵循原始代码的调用签名，假设 `makeHeartbeatRequest(ignoreResponse)` 是一个有效的调用，它会返回请求。
        // 在这个抽象类中，通常会有一个 `protected abstract NetworkClientDelegate.UnsentRequest buildHeartbeatRequest();`
        // 然后这个私有方法会调用它，并处理 `ignoreResponse` 逻辑（例如，设置请求的回调）。
        // 为了简单起见，我们假设 `buildHeartbeatRequest()` 是被调用的，并且 `ignoreResponse` 用于后续处理。
        NetworkClientDelegate.UnsentRequest request = buildHeartbeatRequest(); // 实际构建请求的调用
        // 如果需要忽略响应，可以在这里设置请求的回调为一个空操作或者标记请求
        if (ignoreResponse) {
            // 示例：request.setIgnoreResponse(true); // 这取决于 UnsentRequest 的 API
            // 或者在发送时使用不同的回调
        }

        // 记录发送尝试的时间
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        // 通知成员管理器心跳请求已生成
        membershipManager().onHeartbeatRequestGenerated();
        // 记录心跳发送时间指标
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        // 重置心跳计时器，以便安排下一次心跳
        heartbeatRequestState.resetTimer();
        // 返回构建的请求
        return request;
    }

    /**
     * 创建一个心跳请求。
     * 应用场景：当需要向协调器发送心跳以维持成员资格或表明意图（如离开组）时调用。
     * 实现细节：首先构建一个基础的心跳请求。如果指定忽略响应，则仅记录响应；否则，在请求完成时处理响应或异常。
     * 设计考虑：提供忽略响应的选项，用于某些场景下（如发送离开组请求时）不需要关心响应结果，简化处理逻辑。
     * @param ignoreResponse 是否忽略响应。
     * @return {@link NetworkClientDelegate.UnsentRequest} 未发送的请求对象。
     */
    @SuppressWarnings("unchecked")
    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final boolean ignoreResponse) {
        // 构建基础的心跳请求
        NetworkClientDelegate.UnsentRequest request = buildHeartbeatRequest();
        // 如果忽略响应
        if (ignoreResponse)
            // 则记录响应并返回
            return logResponse(request);
        // 否则，在请求完成时处理
        else
            // 返回一个当请求完成时会执行回调的请求对象
            return request.whenComplete((response, exception) -> {
                // 获取请求完成时间
                long completionTimeMs = request.handler().completionTimeMs();
                // 如果响应不为空（即请求成功）
                if (response != null) {
                    // 记录请求延迟
                    metricsManager.recordRequestLatency(response.requestLatencyMs());
                    // 处理成功的响应
                    onResponse((R) response.responseBody(), completionTimeMs);
                // 如果响应为空（即请求失败）
                } else {
                    // 处理失败的情况
                    onFailure(exception, completionTimeMs);
                }
            });
    }

    /**
     * 记录心跳请求的响应，即使响应被忽略。
     * 应用场景：用于调试和监控，即使业务逻辑上忽略了响应，也希望记录下请求的结果。
     * 实现细节：在请求完成时，记录请求延迟，并根据响应中的错误类型记录成功或失败的日志信息。
     * 设计考虑：确保所有发出的请求都有日志记录，便于问题排查和系统监控。
     * @param request 要记录响应的未发送请求。
     * @return {@link NetworkClientDelegate.UnsentRequest} 附加了响应记录逻辑的未发送请求对象。
     */
    @SuppressWarnings("unchecked")
    private NetworkClientDelegate.UnsentRequest logResponse(final NetworkClientDelegate.UnsentRequest request) {
        // 返回一个当请求完成时会执行回调的请求对象
        return request.whenComplete((response, exception) -> {
            // 如果响应不为空
            if (response != null) {
                // 记录请求延迟
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                // 获取响应中的错误信息
                Errors error = errorForResponse((R) response.responseBody());
                // 如果没有错误
                if (error == Errors.NONE)
                    // 记录调试级别的成功日志
                    logger.debug("{} responded successfully: {}", heartbeatRequestName(), response);
                // 如果有错误
                else
                    // 记录错误级别的失败日志
                    logger.error("{} failed because of {}: {}", heartbeatRequestName(), error, response);
            // 如果响应为空（即发生异常）
            } else {
                // 记录错误级别的异常日志
                logger.error("{} failed because of unexpected exception.", heartbeatRequestName(), exception);
            }
        });
    }

    /**
     * 处理心跳请求失败的情况。
     * 应用场景：当心跳请求因网络问题、协调器问题或其他可重试/不可重试的异常而失败时调用。
     * 实现细节：更新心跳请求状态为失败尝试，重置心跳状态。如果异常是可重试的，则处理协调器断开连接，并记录调试信息。如果异常不可重试且未被特定处理逻辑处理，则记录致命错误并处理。最后通知成员管理器心跳失败。
     * 设计考虑：区分可重试和不可重试的错误，采取不同的处理策略，确保系统的健壮性。
     * @param exception 抛出的异常。
     * @param responseTimeMs 响应时间（或失败发生的时间）。
     */
    private void onFailure(final Throwable exception, final long responseTimeMs) {
        // 标记心跳请求状态为失败尝试
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        // 重置心跳状态（特定于子类的实现）
        resetHeartbeatState();
        // 如果异常是可重试的异常
        if (exception instanceof RetriableException) {
            // 处理协调器断开连接的情况
            coordinatorRequestManager.handleCoordinatorDisconnect(exception, responseTimeMs);
            // 构建调试信息，说明失败原因和重试时间
            String message = String.format("%s failed because of the retriable exception. Will retry in %s ms: %s",
                heartbeatRequestName(),
                heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                exception.getMessage());
            // 记录调试信息
            logger.debug(message);
        // 如果异常不是可重试的，并且没有被特定的失败处理逻辑处理
        } else if (!handleSpecificFailure(exception)) {
            // 记录错误日志，说明发生了致命错误
            logger.error("{} failed due to fatal error: {}", heartbeatRequestName(), exception.getMessage());
            // 处理致命错误
            handleFatalFailure(exception);
        }
        // 通知成员管理器心跳失败，并指明是否是由于可重试异常导致的失败
        // 在所有错误处理和传播完成后通知组管理器有关失败的信息。
        membershipManager().onHeartbeatFailure(exception instanceof RetriableException);
    }

    /**
     * 处理成功的心跳响应。
     * 应用场景：当协调器成功响应心跳请求时调用。
     * 实现细节：如果响应中没有错误，则更新心跳间隔，标记心跳请求状态为成功尝试，并通知成员管理器心跳成功。如果响应中有错误，则调用 {@link #onErrorResponse(Object, long)} 处理错误响应。
     * 设计考虑：区分成功响应和带错误的响应，分别进行处理。
     * @param response 心跳响应。
     * @param currentTimeMs 当前时间（毫秒）。
     */
    private void onResponse(final R response, final long currentTimeMs) {
        // 如果响应中没有错误
        if (errorForResponse(response) == Errors.NONE) {
            // 根据响应更新心跳间隔
            heartbeatRequestState.updateHeartbeatIntervalMs(heartbeatIntervalForResponse(response));
            // 标记心跳请求状态为成功尝试
            heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            // 通知成员管理器心跳成功
            membershipManager().onHeartbeatSuccess(response);
            // 返回，不再继续处理
            return;
        }
        // 如果响应中有错误，则调用错误响应处理方法
        onErrorResponse(response, currentTimeMs);
    }

    /**
     * 处理包含错误的心跳响应。
     * 应用场景：当协调器响应心跳请求但返回了错误码时调用。
     * 实现细节：获取错误类型和错误信息，重置心跳状态，标记心跳请求状态为失败尝试。然后根据具体的错误类型执行不同的处理逻辑，例如：标记协调器未知、记录日志、转换成员状态等。最后通知成员管理器心跳失败。
     * 设计考虑：针对 Kafka 定义的各种错误码进行精细化处理，确保消费者能够正确响应协调器的状态变化和错误指示。
     * @param response 包含错误的心跳响应。
     * @param currentTimeMs 当前时间（毫秒）。
     */
    private void onErrorResponse(final R response, final long currentTimeMs) {
        // 获取响应中的错误类型
        Errors error = errorForResponse(response);
        // 获取响应中的错误信息
        String errorMessage = errorMessageForResponse(response);
        // 用于构建日志信息的消息字符串
        String message;

        // 重置心跳状态（特定于子类的实现）
        resetHeartbeatState();
        // 标记心跳请求状态为失败尝试
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);

        // 根据错误类型进行不同的处理
        switch (error) {
            case NOT_COORDINATOR:
                // 管理器应在协调器节点再次可用时立即重试
                // 构建日志信息，说明协调器不正确
                message = String.format("%s failed because the group coordinator %s is incorrect. " +
                                "Will attempt to find the coordinator again and retry",
                        heartbeatRequestName(), coordinatorRequestManager.coordinator());
                // 记录信息日志
                logInfo(message, response, currentTimeMs);
                // 标记协调器为未知，以便重新发现
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // 跳过退避，以便在发现新协调器后立即发送下一个心跳
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                // 构建日志信息，说明协调器不可用
                message = String.format("%s failed because the group coordinator %s is not available. " +
                                "Will attempt to find the coordinator again and retry",
                        heartbeatRequestName(), coordinatorRequestManager.coordinator());
                // 记录信息日志
                logInfo(message, response, currentTimeMs);
                // 标记协调器为未知，以便重新发现
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // 跳过退避，以便在发现新协调器后立即发送下一个心跳
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // 管理器将退避并重试
                // 构建日志信息，说明协调器仍在加载中
                message = String.format("%s failed because the group coordinator %s is still loading. Will retry",
                        heartbeatRequestName(), coordinatorRequestManager.coordinator());
                // 记录信息日志
                logInfo(message, response, currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                // 创建组授权失败异常
                GroupAuthorizationException exception =
                        GroupAuthorizationException.forGroupId(membershipManager().groupId());
                // 记录错误日志，说明组授权失败
                logger.error("{} failed due to group authorization failure: {}",
                        heartbeatRequestName(), exception.getMessage());
                // 处理致命错误
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_ASSIGNOR:
                // 记录错误日志，说明请求无效、达到组最大大小或不支持的分配器
                logger.error("{} failed due to {}: {}", heartbeatRequestName(), error, errorMessage);
                // 处理致命错误
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                // 构建日志信息，说明成员的 epoch 被隔离
                message = String.format("%s failed for member %s because epoch %s is fenced.",
                        heartbeatRequestName(), membershipManager().memberId(), membershipManager().memberEpoch());
                // 记录信息日志
                logInfo(message, response, currentTimeMs);
                // 将成员状态转换为 FENCED
                membershipManager().transitionToFenced();
                // 跳过退避，以便在被隔离的成员释放其分配后立即发送下一个心跳以重新加入
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                // 构建日志信息，说明成员 ID 未知
                message = String.format("%s failed because member %s is unknown.",
                        heartbeatRequestName(), membershipManager().memberId());
                // 记录信息日志
                logInfo(message, response, currentTimeMs);
                // 将成员状态转换为 FENCED
                membershipManager().transitionToFenced();
                // 跳过退避，以便在被隔离的成员释放其分配后立即发送下一个心跳以重新加入
                heartbeatRequestState.reset();
                break;

            case INVALID_REGULAR_EXPRESSION:
                // 记录错误日志，说明正则表达式无效
                logger.error("{} failed due to {}: {}", heartbeatRequestName(), error, errorMessage);
                // 处理致命错误，并提供更详细的错误信息
                handleFatalFailure(error.exception("Invalid RE2J SubscriptionPattern provided in the call to " +
                    "subscribe. " + errorMessage));
                break;

            default:
                // 如果没有被特定的响应异常处理逻辑处理
                if (!handleSpecificExceptionInResponse(response, currentTimeMs)) {
                    // 如果管理器收到未知错误 - 代码中可能存在错误或出现了新的错误代码
                    // 记录错误日志，说明发生了意外错误
                    logger.error("{} failed due to unexpected error {}: {}", heartbeatRequestName(), error, errorMessage);
                    // 处理致命错误
                    handleFatalFailure(error.exception(errorMessage));
                }
                break;
        }

        // 通知成员管理器心跳失败，并且不是由于可重试异常导致的
        // 在所有错误处理和传播完成后通知组管理器有关失败的信息。
        membershipManager().onHeartbeatFailure(false);
    }

    /**
     * 记录信息日志。
     * <p>
     * 应用场景：用于记录心跳相关的操作信息，例如心跳成功或失败。
     * 实现细节：使用 SLF4J Logger 记录 INFO 级别的日志，包含自定义消息、退避时间和响应错误信息。
     * 设计考虑：提供清晰的日志信息，便于追踪心跳过程和排查问题。
     *
     * @param message       要记录的消息。
     * @param response      心跳响应对象，用于提取错误信息。
     * @param currentTimeMs 当前时间戳（毫秒），用于计算剩余退避时间。
     */
    protected void logInfo(final String message, final R response, final long currentTimeMs) {
        // 使用 logger 记录 INFO 级别的日志
        logger.info("{} in {}ms: {}",
            message, // 记录传入的消息
            heartbeatRequestState.remainingBackoffMs(currentTimeMs), // 获取并记录当前剩余的退避时间
            errorMessageForResponse(response)); // 获取并记录响应中的错误信息
    }

    /**
     * 处理致命错误。
     * <p>
     * 应用场景：当发生无法恢复的严重错误时调用，例如组成员身份验证失败或协议版本不兼容。
     * 实现细节：将错误包装成 ErrorEvent 添加到后台事件处理器，并将成员资格管理器状态转换为致命状态。
     * 设计考虑：确保严重错误能够被上层应用感知，并使消费者进入一个明确的失败状态，防止进一步的无效操作。
     *
     * @param error 抛出的致命错误。
     */
    protected void handleFatalFailure(Throwable error) {
        // 将错误包装成 ErrorEvent 并添加到后台事件处理器队列中
        backgroundEventHandler.add(new ErrorEvent(error));
        // 调用成员资格管理器的方法，将成员状态转换为致命（FATAL）状态
        membershipManager().transitionToFatal();
    }

    /**
     * 处理特定于组类型的故障，当发送请求且未收到响应时。
     * <p>
     * 应用场景：子类可以覆盖此方法来处理特定于其协议的发送请求失败场景（例如，在请求构建阶段就发生异常）。
     * 实现细节：默认实现返回 false，表示未处理该错误，由通用错误处理逻辑接管。
     * 设计考虑：提供一个扩展点，允许不同类型的组（如消费者组、共享组、Streams 组）实现各自的特定错误处理逻辑。
     *
     * @param exception 构建请求时抛出的异常。
     * @return 如果错误已处理则返回 true，否则返回 false。
     */
    public boolean handleSpecificFailure(Throwable exception) {
        // 默认情况下，不处理特定故障，返回 false
        return false;
    }

    /**
     * 处理特定于组类型的响应异常。
     * <p>
     * 应用场景：子类可以覆盖此方法来处理心跳响应中特定于其协议的错误代码。
     * 实现细节：默认实现返回 false，表示未处理该错误，由通用错误处理逻辑接管。
     * 设计考虑：提供一个扩展点，允许不同类型的组根据响应中的特定错误码执行定制化的处理逻辑。
     *
     * @param response      心跳响应。
     * @param currentTimeMs 当前时间（毫秒）。
     * @return 如果错误已处理则返回 true，否则返回 false。
     */
    public boolean handleSpecificExceptionInResponse(final R response, final long currentTimeMs) {
        // 默认情况下，不处理响应中的特定异常，返回 false
        return false;
    }

    /**
     * 重置心跳状态。
     * <p>
     * 应用场景：在某些情况下（例如，重新加入组、协调器变更）需要重置心跳相关的状态，以便重新开始心跳周期。
     * 实现细节：这是一个抽象方法，具体实现由子类提供，因为不同组类型的心跳状态可能不同。
     * 设计考虑：强制子类实现此方法，确保心跳状态可以被正确重置。
     */
    public abstract void resetHeartbeatState();

    /**
     * 使用心跳状态构建一个心跳请求，以忠实地遵循协议。
     * <p>
     * 应用场景：在需要发送心跳时，调用此方法来构造一个符合当前组成员状态和协议要求的心跳请求。
     * 实现细节：这是一个抽象方法，具体实现由子类提供，因为不同组类型的心跳请求结构不同。
     * 设计考虑：强制子类实现请求构建逻辑，确保生成的请求符合特定组协议。
     *
     * @return 心跳请求。
     */
    public abstract NetworkClientDelegate.UnsentRequest buildHeartbeatRequest();

    /**
     * 返回用于日志记录的心跳 RPC 请求名称。
     * <p>
     * 应用场景：在日志中标识不同类型的心跳请求，便于问题排查和监控。
     * 实现细节：这是一个抽象方法，具体实现由子类提供，返回其心跳请求的名称（例如 "ConsumerGroupHeartbeatRequest"）。
     * 设计考虑：提供统一的接口获取请求名称，方便日志记录。
     *
     * @return 心跳 RPC 请求名称。
     */
    public abstract String heartbeatRequestName();

    /**
     * 返回响应的错误。
     * <p>
     * 应用场景：从心跳响应中提取错误码，用于后续的错误处理逻辑。
     * 实现细节：这是一个抽象方法，具体实现由子类提供，解析特定响应对象并返回对应的 {@link Errors} 枚举。
     * 设计考虑：封装了从不同响应类型中提取错误码的逻辑。
     *
     * @param response 心跳响应。
     * @return 错误 {@link Errors}。
     */
    public abstract Errors errorForResponse(R response);

    /**
     * 返回响应的错误消息。
     * <p>
     * 应用场景：从心跳响应中提取详细的错误描述信息，用于日志记录或向用户展示。
     * 实现细节：这是一个抽象方法，具体实现由子类提供，解析特定响应对象并返回错误消息字符串。
     * 设计考虑：封装了从不同响应类型中提取错误消息的逻辑。
     *
     * @param response 心跳响应。
     * @return 错误消息。
     */
    public abstract String errorMessageForResponse(R response);

    /**
     * 返回响应的心跳间隔。
     * <p>
     * 应用场景：协调器通过心跳响应告知客户端下一次心跳的间隔时间。
     * 实现细节：这是一个抽象方法，具体实现由子类提供，解析特定响应对象并返回心跳间隔（毫秒）。
     * 设计考虑：封装了从不同响应类型中提取心跳间隔的逻辑。
     *
     * @param response 心跳响应。
     * @return 心跳间隔。
     */
    public abstract long heartbeatIntervalForResponse(R response);

    /**
     * 表示心跳请求的状态，包括计时、重试和指数退避的逻辑。
     * 该对象扩展了 {@link RequestState} 以启用指数退避和重复请求处理。它包含两个字段：
     * <p>
     * 应用场景：管理单个心跳请求的生命周期，包括何时发送、发送失败如何重试、以及心跳间隔的管理。
     * 设计考虑：将心跳发送的调度逻辑（定时、退避、抖动）封装在一个独立的类中，使得 {@link AbstractHeartbeatRequestManager} 的逻辑更清晰。
     *           继承 {@link RequestState} 复用了通用的请求发送控制逻辑。
     */
    static class HeartbeatRequestState extends RequestState {
        /**
         * heartbeatTimer 跟踪自上次发送心跳以来的时间。
         * <p>
         * 应用场景：用于判断是否到达下一次心跳发送的时间点。
         * 实现细节：使用 Kafka 的 {@link Timer} 实现。
         * 设计考虑：精确控制心跳发送的时机。
         */
        private final Timer heartbeatTimer;

        /**
         * 通过心跳请求获取/更新的心跳间隔。
         * <p>
         * 应用场景：存储从协调器获取到的心跳间隔，并用于重置 {@link #heartbeatTimer}。
         * 实现细节：一个长整型变量，表示毫秒数。
         * 设计考虑：动态调整心跳频率。
         */
        private long heartbeatIntervalMs;

        /**
         * {@link HeartbeatRequestState} 的构造函数。
         * <p>
         * 应用场景：初始化心跳请求状态对象。
         * 实现细节：调用父类 {@link RequestState} 的构造函数，并初始化 {@link #heartbeatIntervalMs} 和 {@link #heartbeatTimer}。
         * 设计考虑：确保所有必要的参数都被正确初始化。
         *
         * @param logContext        日志上下文。
         * @param time              时间工具。
         * @param heartbeatIntervalMs 初始心跳间隔（毫秒）。
         * @param retryBackoffMs    重试退避时间（毫秒）。
         * @param retryBackoffMaxMs 最大重试退避时间（毫秒）。
         * @param jitter            退避时间的抖动因子。
         */
        HeartbeatRequestState(
                final LogContext logContext,
                final Time time,
                final long heartbeatIntervalMs,
                final long retryBackoffMs,
                final long retryBackoffMaxMs,
                final double jitter) {
            // 调用父类 RequestState 的构造函数，初始化重试相关的参数
            super(logContext, HeartbeatRequestState.class.getName(), retryBackoffMs, 2, retryBackoffMaxMs, jitter);
            // 初始化心跳间隔
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            // 使用指定的时间工具和心跳间隔创建一个新的计时器
            this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        }

        /**
         * 更新心跳计时器。
         * <p>
         * 应用场景：在每次检查是否可以发送心跳请求之前，更新计时器的当前时间。
         * 实现细节：调用 {@link Timer#update(long)} 方法。
         * 设计考虑：确保计时器的状态与当前时间同步。
         *
         * @param currentTimeMs 当前时间戳（毫秒）。
         */
        private void update(final long currentTimeMs) {
            // 使用当前时间更新心跳计时器
            this.heartbeatTimer.update(currentTimeMs);
        }

        /**
         * 重置心跳计时器。
         * <p>
         * 应用场景：在成功发送心跳或需要立即发送心跳（如处理失败后）时，重置计时器。
         * 实现细节：使用当前的 {@link #heartbeatIntervalMs} 重置 {@link #heartbeatTimer}。
         * 设计考虑：确保下一次心跳在正确的间隔后触发。
         */
        void resetTimer() {
            // 使用当前的心跳间隔重置心跳计时器
            this.heartbeatTimer.reset(heartbeatIntervalMs);
        }

        @Override
        public String toStringBase() {
            // 调用父类的 toStringBase 方法获取基础字符串
            return super.toStringBase() +
                // 追加当前心跳计时器剩余时间
                ", remainingMs=" + heartbeatTimer.remainingMs() +
                // 追加当前心跳间隔
                ", heartbeatIntervalMs=" + heartbeatIntervalMs;
        }

        /**
         * 检查当前时间是否应该发送心跳请求。
         * 如果心跳计时器已到期，退避已到期，并且没有正在处理的请求，则应发送心跳。
         * <p>
         * 应用场景：在每次事件循环中调用，以确定是否需要发送心跳。
         * 实现细节：首先更新计时器，然后检查计时器是否到期，并结合父类 {@link RequestState#canSendRequest(long)} 的判断（检查退避时间和是否有进行中的请求）。
         * 设计考虑：综合考虑心跳间隔、重试退避和请求并发，决定是否发送心跳。
         *
         * @param currentTimeMs 当前时间戳（毫秒）。
         * @return 如果可以发送心跳请求，则返回 true；否则返回 false。
         */
        @Override
        public boolean canSendRequest(final long currentTimeMs) {
            // 更新心跳计时器的当前时间
            update(currentTimeMs);
            // 检查心跳计时器是否已到期，并且父类（RequestState）的条件也满足（即退避时间已过且没有进行中的请求）
            return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
        }

        /**
         * 计算距离下一次心跳发送还需要的时间（毫秒）。
         * <p>
         * 应用场景：用于确定 poll 循环的最大等待时间，以便及时发送心跳。
         * 实现细节：如果心跳计时器已到期，则返回剩余的退避时间；否则返回计时器剩余的时间。
         * 设计考虑：提供精确的等待时间，避免不必要的 CPU 消耗，同时保证心跳的及时性。
         *
         * @param currentTimeMs 当前时间戳（毫秒）。
         * @return 距离下一次心跳的时间（毫秒）。
         */
        long timeToNextHeartbeatMs(final long currentTimeMs) {
            // 如果心跳计时器已经到期
            if (heartbeatTimer.isExpired()) {
                // 返回当前请求状态的剩余退避时间
                return this.remainingBackoffMs(currentTimeMs);
            }
            // 否则，返回心跳计时器的剩余时间
            return heartbeatTimer.remainingMs();
        }

        /**
         * 当尝试发送请求失败时的回调方法。
         * <p>
         * 应用场景：在心跳请求发送失败（例如网络错误、协调器返回可重试错误）后调用。
         * 实现细节：将心跳计时器重置为0，允许在失败后立即（或根据退避策略）发送下一次心跳，而无需等待完整的心跳间隔。
         *           然后调用父类的 {@link RequestState#onFailedAttempt(long)} 处理退避逻辑。
         * 设计考虑：在发生故障后，可能需要更快地重试或重新加入组，因此不应死等一个完整的心跳间隔。
         *
         * @param currentTimeMs 发生失败时的时间戳（毫秒）。
         */
        @Override
        public void onFailedAttempt(final long currentTimeMs) {
            // 重置计时器以允许在失败后发送心跳而无需等待间隔。
            // 失败后，可能需要退避（例如，导致重试的错误，如协调器加载错误），
            // 或立即发送下一次心跳（例如，导致重新加入的错误，如隔离错误）。
            heartbeatTimer.reset(0); // 将计时器重置为0，使其立即到期
            // 调用父类的 onFailedAttempt 方法，处理退避逻辑
            super.onFailedAttempt(currentTimeMs);
        }

        /**
         * 更新心跳间隔（毫秒）。
         * <p>
         * 应用场景：当从协调器收到新的心跳间隔时调用。
         * 实现细节：如果新的间隔与当前间隔不同，则更新 {@link #heartbeatIntervalMs} 并使用新间隔更新和重置 {@link #heartbeatTimer}。
         * 设计考虑：确保客户端使用协调器指定的最新心跳间隔。
         *
         * @param heartbeatIntervalMs 新的心跳间隔（毫秒）。
         */
        private void updateHeartbeatIntervalMs(final long heartbeatIntervalMs) {
            // 如果当前的心跳间隔与传入的新心跳间隔相同
            if (this.heartbeatIntervalMs == heartbeatIntervalMs) {
                // 如果间隔没有改变，则无需更新计时器
                return;
            }
            // 更新成员变量中的心跳间隔
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            // 使用新的心跳间隔更新并重置心跳计时器
            this.heartbeatTimer.updateAndReset(heartbeatIntervalMs);
        }
    }
}
