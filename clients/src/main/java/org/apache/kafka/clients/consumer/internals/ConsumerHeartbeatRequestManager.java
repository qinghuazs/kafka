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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.REGEX_RESOLUTION_NOT_SUPPORTED_MSG;

/**
 * 这是消费者组的心跳请求管理器。
 *
 * <p>更多详情请参见 {@link AbstractHeartbeatRequestManager}。</p>
 * 应用场景：此类负责管理消费者客户端与 Kafka 集群之间的心跳通信，确保消费者组成员的活性和协调。
 * 实现细节：它继承了 {@link AbstractHeartbeatRequestManager} 的通用心跳逻辑，并针对消费者组的特性进行了定制。
 * 设计考虑：通过专门的管理器处理心跳，可以将心跳逻辑与消费者核心逻辑解耦，提高代码的可维护性和可测试性。
 */
public class ConsumerHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ConsumerGroupHeartbeatResponse> {

    /**
     * 消费者组的成员资格管理器。
     * 应用场景：跟踪和管理消费者在组内的成员信息，如成员ID、年代等。
     * 实现细节：这是一个 final 字段，在构造时初始化。
     * 设计考虑：将成员资格管理的职责委托给专门的类，符合单一职责原则。
     */
    private final ConsumerMembershipManager membershipManager;

    /**
     * HeartbeatState 管理心跳请求的正确构建。
     * 应用场景：根据当前消费者的状态（如订阅信息、成员信息）构建心跳请求体。
     * 实现细节：这是一个 final 字段，在构造时初始化。
     * 设计考虑：将心跳请求的构建逻辑封装在 HeartbeatState 中，使得 ConsumerHeartbeatRequestManager 更侧重于流程控制。
     */
    private final HeartbeatState heartbeatState;

    /**
     * ConsumerHeartbeatRequestManager 的构造函数。
     *
     * @param logContext 日志上下文，用于记录日志。
     * @param time 时间工具，用于获取当前时间等操作。
     * @param config 消费者配置信息。
     * @param coordinatorRequestManager 协调器请求管理器，用于与协调器通信。
     * @param subscriptions 订阅状态，表示消费者当前的订阅信息。
     * @param membershipManager 消费者成员资格管理器。
     * @param backgroundEventHandler 后台事件处理器，用于处理后台事件。
     * @param metrics 指标收集器，用于收集性能指标。
     * 应用场景：在消费者客户端初始化时创建 ConsumerHeartbeatRequestManager 实例。
     * 实现细节：调用父类构造函数，并初始化 membershipManager 和 heartbeatState。
     * 设计考虑：通过依赖注入的方式传入必要的组件，使得类更容易测试和维护。
     */
    public ConsumerHeartbeatRequestManager(
            final LogContext logContext, // 日志上下文
            final Time time, // 时间工具
            final ConsumerConfig config, // 消费者配置
            final CoordinatorRequestManager coordinatorRequestManager, // 协调器请求管理器
            final SubscriptionState subscriptions, // 订阅状态
            final ConsumerMembershipManager membershipManager, // 消费者成员资格管理器
            final BackgroundEventHandler backgroundEventHandler, // 后台事件处理器
            final Metrics metrics) { // 指标收集器
        // 调用父类的构造函数，传入通用参数和心跳指标管理器
        super(logContext, time, config, coordinatorRequestManager, backgroundEventHandler,
            new HeartbeatMetricsManager(metrics));
        // 初始化消费者成员资格管理器
        this.membershipManager = membershipManager;
        // 初始化心跳状态管理器，传入订阅状态、成员资格管理器和最大轮询间隔时间
        this.heartbeatState = new HeartbeatState(subscriptions, membershipManager, maxPollIntervalMs);
    }

    // 仅用于测试
    /**
     * ConsumerHeartbeatRequestManager 的构造函数（主要用于测试）。
     *
     * @param logContext 日志上下文。
     * @param timer 计时器，用于控制心跳发送等。
     * @param config 消费者配置信息。
     * @param coordinatorRequestManager 协调器请求管理器。
     * @param membershipManager 消费者成员资格管理器。
     * @param heartbeatState 心跳状态管理器。
     * @param heartbeatRequestState 心跳请求状态，管理心跳请求的发送逻辑。
     * @param backgroundEventHandler 后台事件处理器。
     * @param metrics 指标收集器。
     * 应用场景：在单元测试中，可以方便地注入 mock 对象或自定义状态，以验证特定逻辑。
     * 实现细节：调用父类构造函数，并初始化 membershipManager 和 heartbeatState。
     * 设计考虑：提供一个更灵活的构造函数，便于进行细粒度的测试控制。
     */
    ConsumerHeartbeatRequestManager(
            final LogContext logContext, // 日志上下文
            final Timer timer, // 计时器
            final ConsumerConfig config, // 消费者配置
            final CoordinatorRequestManager coordinatorRequestManager, // 协调器请求管理器
            final ConsumerMembershipManager membershipManager, // 消费者成员资格管理器
            final HeartbeatState heartbeatState, // 心跳状态管理器
            final AbstractHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState, // 心跳请求状态
            final BackgroundEventHandler backgroundEventHandler, // 后台事件处理器
            final Metrics metrics) { // 指标收集器
        // 调用父类的构造函数，传入通用参数和心跳指标管理器
        super(logContext, timer, config, coordinatorRequestManager, heartbeatRequestState, backgroundEventHandler,
            new HeartbeatMetricsManager(metrics));
        // 初始化消费者成员资格管理器
        this.membershipManager = membershipManager;
        // 初始化心跳状态管理器
        this.heartbeatState = heartbeatState;
    }

    /**
     * {@inheritDoc}
     * 处理特定的心跳失败情况。
     * 应用场景：当发送心跳请求或处理心跳响应过程中发生特定类型的异常时，此方法被调用。
     * 实现细节：主要处理 {@link UnsupportedVersionException}，例如当 Broker 不支持消费者协议或正则表达式解析时。
     * 设计考虑：将特定异常的处理逻辑集中在此方法中，使得主处理流程更清晰。
     * @param exception 发生的异常。
     * @return 如果错误已处理，则返回 true；否则返回 false。
     */
    @Override
    public boolean handleSpecificFailure(Throwable exception) {
        // 初始化错误处理标记为 false
        boolean errorHandled = false;
        // 获取异常信息
        String errorMessage = exception.getMessage();
        // 检查异常是否为 UnsupportedVersionException 的实例
        if (exception instanceof UnsupportedVersionException) {
            // 默认错误消息为消费者协议不支持
            String message = CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG;
            // 如果错误消息与正则表达式解析不支持的消息相同
            if (errorMessage.equals(REGEX_RESOLUTION_NOT_SUPPORTED_MSG)) {
                // 更新错误消息为正则表达式解析不支持
                message = REGEX_RESOLUTION_NOT_SUPPORTED_MSG;
                // 记录错误日志，指出正则表达式解析不支持
                logger.error("{} regex resolution not supported: {}", heartbeatRequestName(), message);
            } else {
                // 记录错误日志，指出因版本不支持导致发送请求失败
                logger.error("{} failed due to unsupported version while sending request: {}", heartbeatRequestName(), errorMessage);
            }
            // 处理严重故障，包装原始异常并传递新的错误消息
            handleFatalFailure(new UnsupportedVersionException(message, exception));
            // 标记错误已处理
            errorHandled = true;
        }
        // 返回错误处理标记
        return errorHandled;
    }

    /**
     * {@inheritDoc}
     * 处理心跳响应中的特定异常情况。
     * 应用场景：当收到 Broker 的心跳响应，并且响应中包含特定的错误代码时，此方法被调用。
     * 实现细节：根据响应中的错误代码（如 UNSUPPORTED_VERSION, UNRELEASED_INSTANCE_ID, FENCED_INSTANCE_ID）执行相应的处理逻辑。
     * 设计考虑：将响应中特定错误的逻辑与通用错误处理分开，提高代码可读性和可维护性。
     * @param response 心跳响应。
     * @param currentTimeMs 当前时间（毫秒）。
     * @return 如果错误已处理，则返回 true；否则返回 false。
     */
    @Override
    public boolean handleSpecificExceptionInResponse(final ConsumerGroupHeartbeatResponse response, final long currentTimeMs) {
        // 从响应中获取错误类型
        Errors error = errorForResponse(response);
        // 从响应中获取错误消息
        String errorMessage = errorMessageForResponse(response);
        // 初始化错误处理标记
        boolean errorHandled;

        // 根据错误类型进行处理
        switch (error) {
            // Broker 响应心跳不支持，意味着新协议未启用，因此传播自定义消息。
            // 注意：协议完全不支持的情况应该在客户端构建请求和检查支持的 API 时失败（在 onFailure 中处理）。
            case UNSUPPORTED_VERSION:
                // 记录错误日志，指出 Broker 端响应版本不支持
                logger.error("{} failed due to unsupported version response on broker side: {}",
                    heartbeatRequestName(), CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG);
                // 处理严重故障，使用预定义的消费者协议不支持消息
                handleFatalFailure(error.exception(CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG));
                // 标记错误已处理
                errorHandled = true;
                break;

            case UNRELEASED_INSTANCE_ID:
                // 记录错误日志，指出由于未释放的实例 ID 导致失败
                logger.error("{} failed due to unreleased instance id {}: {}",
                    heartbeatRequestName(), membershipManager.groupInstanceId().orElse("null"), errorMessage);
                // 处理严重故障，使用响应中的错误消息
                handleFatalFailure(error.exception(errorMessage));
                // 标记错误已处理
                errorHandled = true;
                break;

            case FENCED_INSTANCE_ID:
                // 记录错误日志，指出由于实例 ID 被隔离导致失败
                logger.error("{} failed due to fenced instance id {}: {}. " +
                        "This is expected in the case that the member was removed from the group " +
                        "by an admin client, and another member joined using the same group instance id.",
                    heartbeatRequestName(), membershipManager.groupInstanceId().orElse("null"), errorMessage);
                // 处理严重故障，使用响应中的错误消息
                handleFatalFailure(error.exception(errorMessage));
                // 标记错误已处理
                errorHandled = true;
                break;

            default:
                // 对于其他错误类型，标记为未处理
                errorHandled = false;
        }
        // 返回错误处理标记
        return errorHandled;
    }

    /**
     * {@inheritDoc}
     * 重置心跳状态。
     * 应用场景：当需要清除之前的心跳发送状态时调用，例如在重新加入组或发生特定错误后。
     * 实现细节：调用内部 {@link HeartbeatState} 对象的 reset 方法。
     * 设计考虑：提供一个明确的接口来重置心跳相关的内部状态，确保后续心跳请求的正确性。
     */
    @Override
    public void resetHeartbeatState() {
        // 调用 HeartbeatState 实例的 reset 方法，清空已发送字段的记录。
        heartbeatState.reset();
    }

    /**
     * {@inheritDoc}
     * 构建消费者组心跳请求。
     * 应用场景：在需要向协调器发送心跳以维持成员资格或同步状态时调用。
     * 实现细节：使用 {@link HeartbeatState} 构建请求数据，并将其包装在 {@link NetworkClientDelegate.UnsentRequest} 中。
     * 设计考虑：将心跳请求的构建逻辑委托给 {@link HeartbeatState}，并通过 {@link CoordinatorRequestManager} 获取目标协调器节点。
     * @return 一个包含心跳请求的 {@link NetworkClientDelegate.UnsentRequest} 对象，如果当前不需要发送心跳则返回 null。
     */
    @Override
    public NetworkClientDelegate.UnsentRequest buildHeartbeatRequest() {
        // 创建一个新的未发送请求对象
        return new NetworkClientDelegate.UnsentRequest(
            // 使用 HeartbeatState 构建的请求数据来创建一个新的 ConsumerGroupHeartbeatRequest 构建器
            new ConsumerGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            // 从协调器请求管理器获取当前协调器节点
            coordinatorRequestManager.coordinator());
    }

    /**
     * {@inheritDoc}
     * 获取心跳请求的名称。
     * 应用场景：用于日志记录和度量指标，以标识此类心跳请求。
     * @return 心跳请求的名称字符串，固定为 "ConsumerGroupHeartbeatRequest"。
     */
    @Override
    public String heartbeatRequestName() {
        // 返回消费者组心跳请求的名称
        return "ConsumerGroupHeartbeatRequest";
    }

    /**
     * {@inheritDoc}
     * 从心跳响应中提取错误类型。
     * 应用场景：处理心跳响应时，用于判断响应是否包含错误以及错误的具体类型。
     * @param response 消费者组心跳响应对象。
     * @return 对应的 {@link Errors}枚举值。
     */
    @Override
    public Errors errorForResponse(ConsumerGroupHeartbeatResponse response) {
        // 从响应数据中获取错误码，并将其转换为 Errors 枚举类型
        return Errors.forCode(response.data().errorCode());
    }

    /**
     * {@inheritDoc}
     * 从心跳响应中提取错误消息。
     * 应用场景：当心跳响应包含错误时，用于获取详细的错误描述信息。
     * @param response 消费者组心跳响应对象。
     * @return 错误消息字符串，如果响应中没有错误消息，则可能为 null。
     */
    @Override
    public String errorMessageForResponse(ConsumerGroupHeartbeatResponse response) {
        // 从响应数据中获取错误消息字符串
        return response.data().errorMessage();
    }

    /**
     * {@inheritDoc}
     * 从心跳响应中提取心跳间隔时间。
     * 应用场景：协调器通过心跳响应告知客户端下一次心跳应该在多久之后发送。
     * @param response 消费者组心跳响应对象。
     * @return 心跳间隔时间（毫秒）。
     */
    @Override
    public long heartbeatIntervalForResponse(ConsumerGroupHeartbeatResponse response) {
        // 从响应数据中获取心跳间隔时间（毫秒）
        return response.data().heartbeatIntervalMs();
    }

    /**
     * {@inheritDoc}
     * 获取消费者成员资格管理器。
     * 应用场景：用于访问和管理消费者的成员信息，如成员ID、年代(epoch)等。
     * @return {@link ConsumerMembershipManager} 实例。
     */
    @Override
    public ConsumerMembershipManager membershipManager() {
        // 返回持有的 ConsumerMembershipManager 实例
        return membershipManager;
    }

    /**
     * 正确构建心跳请求，确保所有信息都按照协议发送，
     * 但后续请求不会发送未更改的信息。这对于确保协调成功完成非常重要。
     * 应用场景：此类封装了构建心跳请求时所需的状态和逻辑，特别是关于哪些字段需要发送的决策逻辑。
     * 实现细节：它跟踪已发送的字段，并仅在字段值发生变化或成员首次加入时才发送它们。
     * 设计考虑：通过将此逻辑封装在一个单独的类中，可以使 {@link ConsumerHeartbeatRequestManager} 的代码更简洁，并提高可测试性。
     */
    static class HeartbeatState {
        /**
         * 消费者的订阅状态，包含当前订阅的主题和模式。
         * 应用场景：用于获取消费者订阅的主题列表和订阅模式，这些信息可能需要在心跳中发送给协调器。
         */
        private final SubscriptionState subscriptions;
        /**
         * 消费者成员资格管理器，管理消费者的成员ID、年代(epoch)、实例ID等信息。
         * 应用场景：用于获取构建心跳请求所必需的成员身份信息。
         */
        private final ConsumerMembershipManager membershipManager;
        /**
         * 再均衡超时时间（毫秒）。
         * 应用场景：在心跳请求中发送给协调器，告知协调器在触发再均衡时等待该成员的最长时间。
         */
        private final int rebalanceTimeoutMs;
        /**
         * 记录最近一次心跳请求中已发送的字段值。
         * 应用场景：用于比较当前状态与上次发送的状态，以决定哪些字段需要在本次心跳中发送，从而减少不必要的网络传输。
         */
        private final SentFields sentFields;

        /**
         * HeartbeatState 的构造函数。
         * @param subscriptions 消费者的订阅状态。
         * @param membershipManager 消费者成员资格管理器。
         * @param rebalanceTimeoutMs 再均衡超时时间（毫秒）。
         * 应用场景：在创建 {@link ConsumerHeartbeatRequestManager} 时，会创建此 {@link HeartbeatState} 实例来管理心跳请求的构建。
         * 设计考虑：通过构造函数注入依赖，确保 {@link HeartbeatState} 拥有构建请求所需的所有信息。
         */
        public HeartbeatState(
                final SubscriptionState subscriptions, // 消费者的订阅状态
                final ConsumerMembershipManager membershipManager, // 消费者成员资格管理器
                final int rebalanceTimeoutMs) { // 再均衡超时时间（毫秒）
            // 初始化订阅状态
            this.subscriptions = subscriptions;
            // 初始化成员资格管理器
            this.membershipManager = membershipManager;
            // 初始化再均衡超时时间
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            // 创建一个新的 SentFields 实例，用于跟踪已发送的字段
            this.sentFields = new SentFields();
        }


        /**
         * 重置已发送字段的状态。
         * 应用场景：当心跳状态需要完全重置时调用，例如，在成员重新加入组或发生某些类型的错误后，
         *          确保下一次心跳请求会发送所有必要的信息。
         * 实现细节：调用内部 {@link SentFields} 对象的 reset 方法。
         */
        public void reset() {
            // 调用 SentFields 实例的 reset 方法，将所有已发送字段的记录清除
            sentFields.reset();
        }

        /**
         * 构建消费者组心跳请求的数据部分。
         * 应用场景：在每次准备发送心跳时调用，根据当前消费者的状态和上次发送的状态，决定哪些信息需要包含在请求中。
         * 实现细节：此方法会填充 {@link ConsumerGroupHeartbeatRequestData} 对象的各个字段。
         *          一些字段（如 GroupId, MemberId, MemberEpoch）总是发送。
         *          其他字段（如 RebalanceTimeoutMs, SubscribedTopicNames, SubscribedTopicRegex, ServerAssignor, TopicPartitions）
         *          仅在成员首次加入组 (JOINING 状态) 或其值自上次心跳以来发生更改时发送。
         * 设计考虑：通过这种方式，可以最小化网络传输的数据量，仅发送必要的信息。
         * @return {@link ConsumerGroupHeartbeatRequestData} 对象，包含了本次心跳请求所需发送的数据。
         */
        public ConsumerGroupHeartbeatRequestData buildRequestData() {
            // 创建一个新的心跳请求数据对象
            ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();

            // GroupId - 总是发送
            // 设置消费者组ID
            data.setGroupId(membershipManager.groupId());

            // MemberId - 总是发送，它将在消费者启动时生成。
            // 设置成员ID
            data.setMemberId(membershipManager.memberId());

            // MemberEpoch - 总是发送
            // 设置成员年代（epoch）
            data.setMemberEpoch(membershipManager.memberEpoch());

            // InstanceId - 如果存在则设置
            // 如果成员有关联的实例ID，则设置它
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

            // 判断是否需要发送所有字段，当成员状态为 JOINING 时为 true
            boolean sendAllFields = membershipManager.state() == MemberState.JOINING;

            // RebalanceTimeoutMs - 仅在加入组时或自上次心跳以来发生更改时发送
            // 如果需要发送所有字段，或者当前的 rebalanceTimeoutMs 与上次发送的不同
            if (sendAllFields || sentFields.rebalanceTimeoutMs != rebalanceTimeoutMs) {
                // 设置再均衡超时时间
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                // 更新已发送字段中的 rebalanceTimeoutMs
                sentFields.rebalanceTimeoutMs = rebalanceTimeoutMs;
            }

            // SubscribedTopicNames - 仅在自上次心跳以来发生更改时发送
            // 获取当前订阅的主题名称集合
            TreeSet<String> subscribedTopicNames = new TreeSet<>(this.subscriptions.subscription());
            // 如果需要发送所有字段，或者当前订阅的主题名称与上次发送的不同
            if (sendAllFields || !subscribedTopicNames.equals(sentFields.subscribedTopicNames)) {
                // 设置订阅的主题名称列表
                data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
                // 更新已发送字段中的 subscribedTopicNames
                sentFields.subscribedTopicNames = subscribedTopicNames;
            }

            // SubscribedTopicRegex - 仅在自上次心跳以来发生更改时发送。
            // 发送空字符串表示需要移除已订阅的模式。
            // 获取当前的订阅模式
            SubscriptionPattern pattern = subscriptions.subscriptionPattern();
            // 判断订阅模式是否已更新
            boolean patternUpdated = !Objects.equals(pattern, sentFields.pattern);
            // 如果 (需要发送所有字段且模式不为null) 或者模式已更新
            if ((sendAllFields && pattern != null) || patternUpdated) {
                // 设置订阅的主题正则表达式，如果模式为null则设置为空字符串
                data.setSubscribedTopicRegex((pattern != null) ? pattern.pattern() : "");
                // 更新已发送字段中的 pattern
                sentFields.pattern = pattern;
            }

            // ServerAssignor - 在加入组时或自上次心跳以来发生更改时发送
            // 如果成员资格管理器中存在服务器端分配器
            this.membershipManager.serverAssignor().ifPresent(serverAssignor -> {
                // 如果需要发送所有字段，或者当前的服务器分配器与上次发送的不同
                if (sendAllFields || !serverAssignor.equals(sentFields.serverAssignor)) {
                    // 设置服务器分配器
                    data.setServerAssignor(serverAssignor);
                    // 更新已发送字段中的 serverAssignor
                    sentFields.serverAssignor = serverAssignor;
                }
            });

            // TopicPartitions - 在加入组时或在从服务器接收到新分配并协调后的第一次心跳时发送。
            // 这是通过在本地分配（包括其本地年代，尽管本地年代不在心跳中发送）发生更改时重新发送主题分区来确保的。
            // 获取当前的本地分配信息
            AbstractMembershipManager.LocalAssignment local = membershipManager.currentAssignment();
            // 如果需要发送所有字段，或者当前的本地分配与上次发送的不同
            if (sendAllFields || !local.equals(sentFields.localAssignment)) {
                // 构建主题分区列表
                List<ConsumerGroupHeartbeatRequestData.TopicPartitions> topicPartitions =
                        buildTopicPartitionsList(local.partitions);
                // 设置主题分区信息
                data.setTopicPartitions(topicPartitions);
                // 更新已发送字段中的 localAssignment
                sentFields.localAssignment = local;
            }

            // 返回构建好的心跳请求数据
            return data;
        }

        /**
         * 根据主题ID到分区集合的映射构建主题分区列表。
         * 应用场景：当需要向协调器报告消费者当前拥有的分区分配时，此方法用于将内部数据结构转换为协议所需的格式。
         * @param topicIdPartitions 一个映射，键是主题ID ({@link Uuid})，值是该主题下已分配给此消费者的分区号的有序集合。
         * @return 一个 {@link ConsumerGroupHeartbeatRequestData.TopicPartitions} 对象的列表，每个对象代表一个主题及其分区。
         */
        private List<ConsumerGroupHeartbeatRequestData.TopicPartitions> buildTopicPartitionsList(Map<Uuid, SortedSet<Integer>> topicIdPartitions) {
            // 将 topicIdPartitions 映射的条目流式处理
            return topicIdPartitions.entrySet().stream().map(
                    // 对每个条目（主题ID -> 分区集）
                    entry -> new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        // 创建一个新的 TopicPartitions 对象，并设置主题ID
                        .setTopicId(entry.getKey())
                        // 设置该主题的分区列表（将 SortedSet<Integer> 转换为 ArrayList<Integer>）
                        .setPartitions(new ArrayList<>(entry.getValue())))
                // 将处理后的 TopicPartitions 对象收集到一个列表中
                .collect(Collectors.toList());
        }

        /**
         * 记录在最近一次心跳请求中发送的 ConsumerHeartbeatRequest 的字段值。
         * 应用场景：此类用于跟踪哪些可选字段已在先前的心跳中发送，以避免在值未更改时重复发送，从而优化网络带宽。
         * 实现细节：它包含与 {@link ConsumerGroupHeartbeatRequestData} 中可省略字段对应的成员变量。
         * 设计考虑：通过一个专门的类来管理已发送字段的状态，可以使 {@link HeartbeatState#buildRequestData()} 的逻辑更清晰。
         */
        static class SentFields {
            /**
             * 最近一次发送的再均衡超时时间（毫秒）。默认为 -1，表示尚未发送或已重置。
             */
            private int rebalanceTimeoutMs = -1;
            /**
             * 最近一次发送的已订阅主题名称集合。默认为 null，表示尚未发送或已重置。
             */
            private TreeSet<String> subscribedTopicNames = null;
            /**
             * 最近一次发送的订阅模式。默认为 null，表示尚未发送或已重置。
             */
            private SubscriptionPattern pattern = null;
            /**
             * 最近一次发送的服务器端分配器名称。默认为 null，表示尚未发送或已重置。
             */
            private String serverAssignor = null;
            /**
             * 最近一次发送的本地分配信息。默认为 null，表示尚未发送或已重置。
             */
            private AbstractMembershipManager.LocalAssignment localAssignment = null;

            /**
             * SentFields 的默认构造函数。
             * 应用场景：在创建 {@link HeartbeatState} 时，会隐式或显式创建此 {@link SentFields} 实例。
             */
            SentFields() {
                // 构造函数体为空，字段使用其默认初始化值
            }

            /**
             * 重置所有已发送字段的记录状态。
             * 应用场景：当需要强制在下一次心跳中发送所有可选字段时调用，例如在 {@link HeartbeatState#reset()} 中。
             * 实现细节：将所有跟踪的字段值恢复到其初始状态（通常是 null 或特定标记值如 -1）。
             */
            void reset() {
                // 将已订阅主题名称集合重置为 null
                subscribedTopicNames = null;
                // 将再均衡超时时间重置为 -1
                rebalanceTimeoutMs = -1;
                // 将服务器分配器重置为 null
                serverAssignor = null;
                // 将本地分配信息重置为 null
                localAssignment = null;
                // 将订阅模式重置为 null
                pattern = null;
            }
        }
    }
}
