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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 一个在{@link ConsumerNetworkThread 网络线程}中创建和执行的{@link EventProcessor}，
 * 用于处理应用线程生成的{@link ApplicationEvent 应用事件}。
 * 
 * 该处理器是Kafka消费者客户端的核心组件之一，负责处理各种消费者事件，包括：
 * 1. 提交位移（同步/异步）
 * 2. 消费者轮询
 * 3. 主题订阅和分区分配
 * 4. 元数据更新
 * 5. 消费者组成员管理
 * 6. 共享消费相关操作
 */
public class ApplicationEventProcessor implements EventProcessor<ApplicationEvent> {

    /** 用于记录日志的Logger实例 */
    private final Logger log;
    /** 消费者元数据，包含broker、topic等信息 */
    private final ConsumerMetadata metadata;
    /** 订阅状态管理器，维护分区订阅和位移信息 */
    private final SubscriptionState subscriptions;
    /** 请求管理器集合，包含各种类型的请求管理器 */
    private final RequestManagers requestManagers;
    /** 元数据版本快照，用于追踪元数据更新 */
    private int metadataVersionSnapshot;

    /**
     * ApplicationEventProcessor的构造函数
     * 
     * @param logContext 日志上下文，用于创建日志记录器
     * @param requestManagers 请求管理器集合，用于管理各种类型的请求
     * @param metadata 消费者元数据，包含集群、主题等信息
     * @param subscriptions 订阅状态管理器，维护分区订阅和消费位移
     */
    public ApplicationEventProcessor(final LogContext logContext,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata,
                                     final SubscriptionState subscriptions) {
        // 初始化日志记录器
        this.log = logContext.logger(ApplicationEventProcessor.class);
        // 设置请求管理器集合
        this.requestManagers = requestManagers;
        // 设置消费者元数据
        this.metadata = metadata;
        // 设置订阅状态管理器
        this.subscriptions = subscriptions;
        // 初始化元数据版本快照
        this.metadataVersionSnapshot = metadata.updateVersion();
    }

    /**
     * 处理应用线程生成的各种事件。该方法是事件处理的核心入口，根据事件类型分发到对应的处理方法。
     * 
     * 支持的事件类型包括：
     * 1. 位移提交相关事件
     *    - COMMIT_ASYNC: 异步提交位移
     *    - COMMIT_SYNC: 同步提交位移
     *    - FETCH_COMMITTED_OFFSETS: 获取已提交的位移
     * 
     * 2. 消费者轮询和位置管理事件
     *    - POLL: 消费者轮询操作
     *    - CHECK_AND_UPDATE_POSITIONS: 检查和更新消费位置
     *    - LIST_OFFSETS: 获取指定时间戳的位移
     *    - RESET_OFFSET: 重置消费位置
     * 
     * 3. 主题订阅和分区分配事件
     *    - ASSIGNMENT_CHANGE: 分区分配变更
     *    - TOPIC_SUBSCRIPTION_CHANGE: 主题订阅变更
     *    - TOPIC_PATTERN_SUBSCRIPTION_CHANGE: 基于模式的主题订阅变更
     *    - TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE: 基于RE2J模式的主题订阅变更
     *    - UNSUBSCRIBE: 取消订阅
     * 
     * 4. 元数据管理事件
     *    - TOPIC_METADATA: 获取指定主题的元数据
     *    - ALL_TOPICS_METADATA: 获取所有主题的元数据
     *    - UPDATE_SUBSCRIPTION_METADATA: 更新订阅的元数据
     * 
     * 5. 消费者组管理事件
     *    - CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED: 重平衡监听器回调完成
     *    - COMMIT_ON_CLOSE: 关闭时提交位移
     *    - LEAVE_GROUP_ON_CLOSE: 关闭时离开消费者组
     *    - STOP_FIND_COORDINATOR_ON_CLOSE: 关闭时停止查找协调器
     * 
     * 6. 共享消费相关事件
     *    - CREATE_FETCH_REQUESTS: 创建获取请求
     *    - SHARE_FETCH: 共享消费获取
     *    - SHARE_ACKNOWLEDGE_SYNC: 同步确认共享消费
     *    - SHARE_ACKNOWLEDGE_ASYNC: 异步确认共享消费
     *    - SHARE_SUBSCRIPTION_CHANGE: 共享消费订阅变更
     *    - SHARE_UNSUBSCRIBE: 取消共享消费订阅
     *    - SHARE_ACKNOWLEDGE_ON_CLOSE: 关闭时确认共享消费
     *    - SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION: 注册共享消费确认回调
     * 
     * 7. 分区管理事件
     *    - SEEK_UNVALIDATED: 未验证的位移调整
     *    - PAUSE_PARTITIONS: 暂停分区
     *    - RESUME_PARTITIONS: 恢复分区
     *    - CURRENT_LAG: 获取当前消费延迟
     *
     * @param event 需要处理的应用事件
     */
    @SuppressWarnings({"CyclomaticComplexity"})
    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT_ASYNC:
                process((AsyncCommitEvent) event);
                return;

            case COMMIT_SYNC:
                process((SyncCommitEvent) event);
                return;

            case POLL:
                process((PollEvent) event);
                return;

            case FETCH_COMMITTED_OFFSETS:
                process((FetchCommittedOffsetsEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataEvent) event);
                return;

            case ALL_TOPICS_METADATA:
                process((AllTopicsMetadataEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsEvent) event);
                return;

            case RESET_OFFSET:
                process((ResetOffsetEvent) event);
                return;

            case CHECK_AND_UPDATE_POSITIONS:
                process((CheckAndUpdatePositionsEvent) event);
                return;

            case TOPIC_SUBSCRIPTION_CHANGE:
                process((TopicSubscriptionChangeEvent) event);
                return;

            case TOPIC_PATTERN_SUBSCRIPTION_CHANGE:
                process((TopicPatternSubscriptionChangeEvent) event);
                return;

            case TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE:
                process((TopicRe2JPatternSubscriptionChangeEvent) event);
                return;

            case UPDATE_SUBSCRIPTION_METADATA:
                process((UpdatePatternSubscriptionEvent) event);
                return;

            case UNSUBSCRIBE:
                process((UnsubscribeEvent) event);
                return;

            case CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED:
                process((ConsumerRebalanceListenerCallbackCompletedEvent) event);
                return;

            case COMMIT_ON_CLOSE:
                process((CommitOnCloseEvent) event);
                return;

            case LEAVE_GROUP_ON_CLOSE:
                process((LeaveGroupOnCloseEvent) event);
                return;

            case STOP_FIND_COORDINATOR_ON_CLOSE:
                process((StopFindCoordinatorOnCloseEvent) event);
                return;

            case CREATE_FETCH_REQUESTS:
                process((CreateFetchRequestsEvent) event);
                return;

            case SHARE_FETCH:
                process((ShareFetchEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_SYNC:
                process((ShareAcknowledgeSyncEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_ASYNC:
                process((ShareAcknowledgeAsyncEvent) event);
                return;

            case SHARE_SUBSCRIPTION_CHANGE:
                process((ShareSubscriptionChangeEvent) event);
                return;

            case SHARE_UNSUBSCRIBE:
                process((ShareUnsubscribeEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_ON_CLOSE:
                process((ShareAcknowledgeOnCloseEvent) event);
                return;

            case SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION:
                process((ShareAcknowledgementCommitCallbackRegistrationEvent) event);
                return;

            case SEEK_UNVALIDATED:
                process((SeekUnvalidatedEvent) event);
                return;

            case PAUSE_PARTITIONS:
                process((PausePartitionsEvent) event);
                return;

            case RESUME_PARTITIONS:
                process((ResumePartitionsEvent) event);
                return;

            case CURRENT_LAG:
                process((CurrentLagEvent) event);
                return;

            default:
                log.warn("Application event type {} was not expected", event.type());
        }
    }

    /**
     * 处理消费者轮询事件
     * 
     * 该方法主要完成以下工作：
     * 1. 如果存在提交管理器，则：
     *    - 更新自动提交定时器
     *    - 通知消费者组成员管理器进行轮询
     *    - 重置轮询定时器
     * 2. 如果不存在提交管理器，则：
     *    - 通知共享心跳管理器进行轮询
     *    - 重置轮询定时器
     *
     * @param event 轮询事件，包含轮询时间间隔
     */
    private void process(final PollEvent event) {
        if (requestManagers.commitRequestManager.isPresent()) {
            // 更新自动提交定时器
            requestManagers.commitRequestManager.ifPresent(m -> m.updateAutoCommitTimer(event.pollTimeMs()));
            // 处理消费者组心跳
            requestManagers.consumerHeartbeatRequestManager.ifPresent(hrm -> {
                hrm.membershipManager().onConsumerPoll();
                hrm.resetPollTimer(event.pollTimeMs());
            });
        } else {
            // 处理共享消费心跳
            requestManagers.shareHeartbeatRequestManager.ifPresent(hrm -> {
                hrm.membershipManager().onConsumerPoll();
                hrm.resetPollTimer(event.pollTimeMs());
            });
        }
    }

    /**
     * 处理创建获取请求事件
     * 
     * 该方法通过FetchRequestManager创建新的获取请求，用于从Kafka服务器获取消息。
     * 创建完成后，将结果通过CompletableFuture返回给调用者。
     *
     * @param event 创建获取请求事件
     */
    private void process(final CreateFetchRequestsEvent event) {
        CompletableFuture<Void> future = requestManagers.fetchRequestManager.createFetchRequests();
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理异步提交位移事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查CommitRequestManager是否可用
     * 2. 如果不可用，返回异常
     * 3. 如果可用，则：
     *    - 获取CommitRequestManager实例
     *    - 异步提交位移
     *    - 处理提交结果
     *
     * @param event 异步提交事件，包含要提交的位移信息
     */
    private void process(final AsyncCommitEvent event) {
        if (requestManagers.commitRequestManager.isEmpty()) {
            event.future().completeExceptionally(new KafkaException("Unable to async commit " +
                "offset because the CommitRequestManager is not available. Check if group.id was set correctly"));
            return;
        }

        try {
            // 获取提交管理器并执行异步提交
            CommitRequestManager manager = requestManagers.commitRequestManager.get();
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.commitAsync(event.offsets());
            future.whenComplete(complete(event.future()));
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理同步提交位移事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查CommitRequestManager是否可用
     * 2. 如果不可用，返回异常
     * 3. 如果可用，则：
     *    - 获取CommitRequestManager实例
     *    - 同步提交位移，等待直到超时或完成
     *    - 处理提交结果
     *
     * @param event 同步提交事件，包含要提交的位移信息和截止时间
     */
    private void process(final SyncCommitEvent event) {
        if (requestManagers.commitRequestManager.isEmpty()) {
            event.future().completeExceptionally(new KafkaException("Unable to sync commit " +
                "offset because the CommitRequestManager is not available. Check if group.id was set correctly"));
            return;
        }

        try {
            // 获取提交管理器并执行同步提交
            CommitRequestManager manager = requestManagers.commitRequestManager.get();
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.commitSync(event.offsets(), event.deadlineMs());
            future.whenComplete(complete(event.future()));
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理获取已提交位移事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查CommitRequestManager是否可用，如果不可用则返回异常
     * 2. 通过CommitRequestManager获取指定分区的已提交位移
     * 3. 异步处理获取结果
     *
     * @param event 获取已提交位移事件，包含要查询的分区和截止时间
     */
    private void process(final FetchCommittedOffsetsEvent event) {
        // 检查CommitRequestManager是否可用，不可用时返回异常
        if (requestManagers.commitRequestManager.isEmpty()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommitRequestManager is not available. Check if group.id was set correctly"));
            return;
        }
        // 获取CommitRequestManager实例
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        // 异步获取指定分区的已提交位移
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.fetchOffsets(event.partitions(), event.deadlineMs());
        // 处理获取结果
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理分区分配变更事件
     * 
     * 该方法主要完成以下工作：
     * 1. 如果启用了自动提交，则：
     *    - 更新自动提交定时器
     *    - 尝试执行异步提交
     * 2. 更新分区分配信息
     * 3. 如果有新主题，请求更新元数据
     * 
     * 注意：这里触发的异步提交如果失败不会重试
     *
     * @param event 分区分配变更事件，包含新分配的分区列表
     */
    private void process(final AssignmentChangeEvent event) {
        // 如果启用了自动提交，更新定时器并尝试异步提交
        if (requestManagers.commitRequestManager.isPresent()) {
            CommitRequestManager manager = requestManagers.commitRequestManager.get();
            manager.updateAutoCommitTimer(event.currentTimeMs());
            manager.maybeAutoCommitAsync();
        }

        // 记录分配的分区信息
        log.info("Assigned to partition(s): {}", event.partitions().stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
        try {
            // 更新订阅状态中的分区分配信息，如果有新主题则请求更新元数据
            if (subscriptions.assignFromUser(new HashSet<>(event.partitions())))
                metadata.requestUpdateForNewTopics();

            // 完成事件处理
            event.future().complete(null);
        } catch (Exception e) {
            // 处理异常情况
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理获取位移事件
     * 
     * 该方法主要完成以下工作：
     * 1. 根据给定的时间戳获取对应的位移信息
     * 2. 异步处理获取结果
     *
     * @param event 获取位移事件，包含要查询的分区和时间戳信息
     */
    private void process(final ListOffsetsEvent event) {
        // 通过OffsetsRequestManager异步获取指定时间戳对应的位移
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> future =
            requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(), event.requireTimestamps());
        // 处理获取结果
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理主题订阅变更事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查消费者组成员管理器是否可用
     * 2. 更新订阅的主题列表
     * 3. 如果有新主题，请求更新元数据
     * 4. 通知成员管理器订阅已更新
     * 
     * 这将使消费者：
     * - 如果还不是组成员，则加入消费者组
     * - 如果已经是组成员，则在下次poll时发送更新后的订阅信息
     *
     * @param event 主题订阅变更事件，包含新的主题列表和监听器
     */
    private void process(final TopicSubscriptionChangeEvent event) {
        // 检查消费者组成员管理器是否可用
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            event.future().complete(null);
            return;
        }

        try {
            // 更新订阅的主题列表，如果有新主题则请求更新元数据
            if (subscriptions.subscribe(event.topics(), event.listener()))
                this.metadataVersionSnapshot = metadata.requestUpdateForNewTopics();

            // 通知成员管理器订阅已更新，这将触发加入组或更新订阅信息
            requestManagers.consumerHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
            // 完成事件处理
            event.future().complete(null);
        } catch (Exception e) {
            // 处理异常情况
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理主题订阅模式变更事件
     * 
     * 该方法主要完成以下工作：
     * 1. 更新客户端中的订阅状态，保存新的订阅模式
     * 2. 根据最新的元数据评估模式，找到匹配的主题
     * 3. 在下次轮询时向broker发送更新后的订阅信息
     *    (如果还不是消费者组成员，则加入组)
     *
     * @param event 主题订阅模式变更事件，包含新的订阅模式和监听器
     */
    private void process(final TopicPatternSubscriptionChangeEvent event) {
        try {
            // 使用新的模式和监听器更新订阅状态
            subscriptions.subscribe(event.pattern(), event.listener());
            // 请求更新元数据以获取新主题信息
            metadata.requestUpdateForNewTopics();
            // 根据最新元数据更新模式订阅
            updatePatternSubscription(metadata.fetch());
            // 完成事件处理
            event.future().complete(null);
        } catch (Exception e) {
            // 处理异常情况
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理RE2J模式订阅变更事件
     * 
     * 该方法主要完成以下工作：
     * 1. 更新客户端中的订阅状态，保存新的RE2J订阅模式
     * 2. 在下次轮询时让消费者发送更新后的模式
     *    (如果还不是消费者组成员，则加入组)
     * 
     * 注意：此方法不会评估模式，而是直接将模式传递给broker
     *
     * @param event RE2J模式订阅变更事件，包含新的订阅模式和监听器
     */
    private void process(final TopicRe2JPatternSubscriptionChangeEvent event) {
        // 检查消费者组成员管理器是否可用
        if (requestManagers.consumerMembershipManager.isEmpty()) {
            event.future().completeExceptionally(
                new KafkaException("MembershipManager is not available when processing a subscribe event"));
            return;
        }
        try {
            // 使用新的RE2J模式和监听器更新订阅状态
            subscriptions.subscribe(event.pattern(), event.listener());
            // 通知成员管理器订阅已更新
            requestManagers.consumerMembershipManager.get().onSubscriptionUpdated();
            // 完成事件处理
            event.future().complete(null);
        } catch (Exception e) {
            // 处理异常情况
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理订阅模式更新事件
     * 
     * 该方法主要完成以下工作：
     * 1. 仅在元数据发生变化时，使用最新的主题元数据重新评估订阅的正则表达式
     * 2. 让消费者在下次轮询时发送更新后的订阅信息
     *
     * @param event 订阅模式更新事件
     */
    private void process(final UpdatePatternSubscriptionEvent event) {
        // 如果没有基于模式的订阅，直接返回
        if (!subscriptions.hasPatternSubscription()) {
            return;
        }
        // 检查元数据是否有更新
        if (this.metadataVersionSnapshot < metadata.updateVersion()) {
            // 更新元数据版本快照
            this.metadataVersionSnapshot = metadata.updateVersion();
            // 使用最新元数据更新模式订阅
            updatePatternSubscription(metadata.fetch());
        }
        // 完成事件处理
        event.future().complete(null);
    }

    /**
     * 处理取消订阅事件
     * 
     * 该方法主要完成以下工作：
     * 1. 让消费者释放其分配的分区
     * 2. 发送请求离开消费者组
     *
     * @param event 取消订阅事件，包含一个Future对象，该对象将在以下情况完成：
     *             - 释放分区分配的回调执行完成
     *             - 离开消费者组的请求已发送
     */
    private void process(final UnsubscribeEvent event) {
        // 检查消费者组心跳请求管理器是否可用
        if (requestManagers.consumerHeartbeatRequestManager.isPresent()) {
            // 通过成员管理器发送离开组请求
            CompletableFuture<Void> future = requestManagers.consumerHeartbeatRequestManager.get().membershipManager().leaveGroup();
            // 处理请求完成后的回调
            future.whenComplete(complete(event.future()));
        } else {
            // 如果消费者没有使用组管理功能，仍需要清除所有可能的分配
            subscriptions.unsubscribe();
            // 完成事件处理
            event.future().complete(null);
        }
    }

    /**
     * 处理重置位移事件
     * 
     * 该方法主要完成以下工作：
     * 1. 确定需要重置位移的分区列表
     * 2. 使用指定的重置策略请求重置这些分区的位移
     *
     * @param event 重置位移事件，包含要重置的分区和重置策略
     */
    private void process(final ResetOffsetEvent event) {
        try {
            // 如果未指定分区，则使用所有已分配的分区
            Collection<TopicPartition> parts = event.topicPartitions().isEmpty() ?
                    subscriptions.assignedPartitions() : event.topicPartitions();
            // 请求重置指定分区的位移
            subscriptions.requestOffsetReset(parts, event.offsetResetStrategy());
            // 完成事件处理
            event.future().complete(null);
        } catch (Exception e) {
            // 处理异常情况
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理检查和更新位置事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查所有已分配分区是否都有获取位置
     * 2. 如果存在缺失的位置，则获取位移并用它们更新订阅状态中的位置
     *
     * @param event 检查和更新位置事件，包含操作截止时间
     */
    private void process(final CheckAndUpdatePositionsEvent event) {
        // 通过位移请求管理器更新获取位置
        CompletableFuture<Boolean> future = requestManagers.offsetsRequestManager.updateFetchPositions(event.deadlineMs());
        // 处理更新完成后的回调
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理主题元数据事件
     * 
     * 该方法主要完成以下工作：
     * 1. 通过主题元数据请求管理器获取指定主题的元数据信息
     * 2. 异步处理获取结果
     *
     * @param event 主题元数据事件，包含要查询的主题和截止时间
     */
    private void process(final TopicMetadataEvent event) {
        // 通过主题元数据请求管理器请求指定主题的元数据
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestTopicMetadata(event.topic(), event.deadlineMs());
        // 处理请求完成后的回调
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理所有主题元数据事件
     * 
     * 该方法主要完成以下工作：
     * 1. 通过主题元数据请求管理器获取所有主题的元数据信息
     * 2. 异步处理获取结果
     *
     * @param event 所有主题元数据事件，包含操作截止时间
     */
    private void process(final AllTopicsMetadataEvent event) {
        // 通过主题元数据请求管理器请求所有主题的元数据
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestAllTopicsMetadata(event.deadlineMs());
        // 处理请求完成后的回调
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理消费者重平衡监听器回调完成事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查消费者组心跳请求管理器是否可用
     * 2. 通知成员管理器重平衡监听器回调已完成
     *
     * @param event 消费者重平衡监听器回调完成事件
     */
    private void process(final ConsumerRebalanceListenerCallbackCompletedEvent event) {
        // 检查消费者组心跳请求管理器是否可用
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            // 记录警告日志，表示无法发送回调执行通知
            log.warn(
                "An internal error occurred; the group membership manager was not present, so the notification of the {} callback execution could not be sent",
                event.methodName()
            );
            return;
        }
        // 通知成员管理器重平衡监听器回调已完成
        requestManagers.consumerHeartbeatRequestManager.get().membershipManager().consumerRebalanceListenerCallbackCompleted(event);
    }

    /**
     * 处理消费者关闭时的提交位移事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查CommitRequestManager是否可用
     * 2. 如果可用，通知CommitRequestManager准备关闭
     * 3. 如果不可用，直接返回
     *
     * @param event 关闭时提交位移事件
     */
    private void process(@SuppressWarnings("unused") final CommitOnCloseEvent event) {
        // 检查CommitRequestManager是否可用，不可用时直接返回
        if (requestManagers.commitRequestManager.isEmpty())
            return;
        // 记录日志，表示即将关闭CommitRequestManager
        log.debug("Signal CommitRequestManager closing");
        // 通知CommitRequestManager准备关闭
        requestManagers.commitRequestManager.get().signalClose();
    }

    /**
     * 处理消费者关闭时离开消费者组的事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查ConsumerMembershipManager是否可用
     * 2. 如果可用，通知ConsumerMembershipManager离开消费者组
     * 3. 如果不可用，直接返回
     * 4. 处理离开组的异步结果
     *
     * @param event 关闭时离开消费者组事件
     */
    private void process(final LeaveGroupOnCloseEvent event) {
        // 检查ConsumerMembershipManager是否可用，不可用时直接返回
        if (requestManagers.consumerMembershipManager.isEmpty())
            return;

        // 记录日志，表示消费者即将离开消费者组
        log.debug("Signal the ConsumerMembershipManager to leave the consumer group since the consumer is closing");
        // 通知ConsumerMembershipManager离开消费者组，并获取异步结果
        CompletableFuture<Void> future = requestManagers.consumerMembershipManager.get().leaveGroupOnClose();
        // 处理离开组的异步结果
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理消费者关闭时停止查找协调器的事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查CoordinatorRequestManager是否可用
     * 2. 如果可用，通知CoordinatorRequestManager准备关闭
     *
     * @param event 关闭时停止查找协调器事件
     */
    private void process(@SuppressWarnings("unused") final StopFindCoordinatorOnCloseEvent event) {
        // 如果CoordinatorRequestManager可用，通知其准备关闭
        requestManagers.coordinatorRequestManager.ifPresent(manager -> {
            log.debug("Signal CoordinatorRequestManager closing");
            manager.signalClose();
        });
    }

    /**
     * 处理共享消费获取事件，通知共享消费请求管理器获取更多记录
     * 
     * 该方法主要完成以下工作：
     * 1. 检查ShareConsumeRequestManager是否可用
     * 2. 如果可用，通知其获取更多记录
     *
     * @param event 共享消费获取事件，包含确认信息映射
     */
    private void process(final ShareFetchEvent event) {
        // 如果ShareConsumeRequestManager可用，通知其获取更多记录
        requestManagers.shareConsumeRequestManager.ifPresent(scrm -> scrm.fetch(event.acknowledgementsMap()));
    }

    /**
     * 处理同步确认共享消费事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查ShareConsumeRequestManager是否可用
     * 2. 如果可用，同步提交确认信息
     * 3. 处理提交的异步结果
     *
     * @param event 同步确认共享消费事件，包含确认信息映射和截止时间
     */
    private void process(final ShareAcknowledgeSyncEvent event) {
        // 检查ShareConsumeRequestManager是否可用，不可用时直接返回
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            return;
        }

        // 获取ShareConsumeRequestManager实例
        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        // 同步提交确认信息，并获取异步结果
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future =
                manager.commitSync(event.acknowledgementsMap(), event.deadlineMs());
        // 处理提交的异步结果
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理异步确认共享消费事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查ShareConsumeRequestManager是否可用
     * 2. 如果可用，异步提交确认信息
     *
     * @param event 异步确认共享消费事件，包含确认信息映射
     */
    private void process(final ShareAcknowledgeAsyncEvent event) {
        // 检查ShareConsumeRequestManager是否可用，不可用时直接返回
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            return;
        }

        // 获取ShareConsumeRequestManager实例
        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        // 异步提交确认信息
        manager.commitAsync(event.acknowledgementsMap());
    }

    /**
     * 处理共享消费订阅变更事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查ShareHeartbeatRequestManager是否可用
     * 2. 如果不可用，返回异常
     * 3. 更新订阅信息，如果有新主题则请求更新元数据
     * 4. 通知成员管理器订阅已更新
     *
     * @param event 共享消费订阅变更事件，包含新的主题列表
     */
    private void process(final ShareSubscriptionChangeEvent event) {
        // 检查ShareHeartbeatRequestManager是否可用，不可用时返回异常
        if (requestManagers.shareHeartbeatRequestManager.isEmpty()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing a subscribe event");
            event.future().completeExceptionally(error);
            return;
        }

        // 更新订阅信息，如果有新主题则请求更新元数据
        if (subscriptions.subscribeToShareGroup(event.topics()))
            metadata.requestUpdateForNewTopics();

        // 通知成员管理器订阅已更新
        requestManagers.shareHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();

        // 完成事件处理
        event.future().complete(null);
    }

    /**
     * 处理取消共享消费订阅事件
     * 
     * 该方法主要完成以下工作：
     * 1. 检查ShareHeartbeatRequestManager是否可用
     * 2. 如果不可用，返回异常
     * 3. 取消所有订阅
     * 4. 通知成员管理器离开组
     * 5. 处理离开组的异步结果
     *
     * @param event 取消共享消费订阅事件
     */
    private void process(final ShareUnsubscribeEvent event) {
        // 检查ShareHeartbeatRequestManager是否可用，不可用时返回异常
        if (requestManagers.shareHeartbeatRequestManager.isEmpty()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an unsubscribe event");
            event.future().completeExceptionally(error);
            return;
        }

        // 取消所有订阅
        subscriptions.unsubscribe();

        // 通知成员管理器离开组，并获取异步结果
        CompletableFuture<Void> future = requestManagers.shareHeartbeatRequestManager.get().membershipManager().leaveGroup();
        // 处理离开组的异步结果，在心跳发送时完成
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理消费者关闭事件。该方法会使消费者完成所有待处理的确认操作。
     * 
     * 该方法主要完成以下工作：
     * 1. 检查共享消费请求管理器是否可用
     * 2. 调用管理器的acknowledgeOnClose方法处理待确认的消息
     * 3. 异步处理确认结果
     *
     * @param event 关闭时确认事件，包含一个在确认响应完成时完成的Future
     */
    private void process(final ShareAcknowledgeOnCloseEvent event) {
        // 检查共享消费请求管理器是否可用，不可用时返回异常
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an acknowledge-on-close event");
            event.future().completeExceptionally(error);
            return;
        }

        // 获取共享消费请求管理器并执行关闭时的确认操作
        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        CompletableFuture<Void> future = manager.acknowledgeOnClose(event.acknowledgementsMap(), event.deadlineMs());
        // 处理确认结果
        future.whenComplete(complete(event.future()));
    }

    /**
     * 处理确认提交回调处理器配置事件。该方法用于设置用户是否配置了回调处理器。
     * 
     * 该方法主要完成以下工作：
     * 1. 检查共享消费请求管理器是否可用
     * 2. 更新管理器中的回调处理器配置状态
     *
     * @param event 包含回调处理器配置状态的事件
     */
    private void process(final ShareAcknowledgementCommitCallbackRegistrationEvent event) {
        // 如果共享消费请求管理器不可用，直接返回
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            return;
        }

        // 获取共享消费请求管理器并更新回调处理器的注册状态
        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        manager.setAcknowledgementCommitCallbackRegistered(event.isCallbackRegistered());
    }

    /**
     * 处理未验证的位移调整事件。该方法用于在不验证位移有效性的情况下调整消费位置。
     * 
     * 该方法主要完成以下工作：
     * 1. 更新分区的最新Epoch信息（如果有）
     * 2. 创建新的消费位置
     * 3. 更新订阅状态中的位移信息
     *
     * @param event 未验证的位移调整事件
     */
    private void process(final SeekUnvalidatedEvent event) {
        try {
            // 如果有Epoch信息，更新元数据中的最新Epoch
            event.offsetEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(event.partition(), epoch));
            // 创建新的消费位置
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                event.offset(),
                event.offsetEpoch(),
                metadata.currentLeader(event.partition())
            );
            // 更新订阅状态中的位移信息
            subscriptions.seekUnvalidated(event.partition(), newPosition);
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理暂停分区事件。该方法用于暂停指定分区的消费操作。
     * 
     * 该方法主要完成以下工作：
     * 1. 获取要暂停的分区列表
     * 2. 遍历分区列表，逐个暂停分区
     * 3. 更新订阅状态
     *
     * @param event 暂停分区事件
     */
    private void process(final PausePartitionsEvent event) {
        try {
            // 获取要暂停的分区列表
            Collection<TopicPartition> partitions = event.partitions();
            log.debug("Pausing partitions {}", partitions);

            // 遍历分区列表，逐个暂停分区
            for (TopicPartition partition : partitions) {
                subscriptions.pause(partition);
            }

            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理恢复分区事件。该方法用于恢复之前暂停的分区的消费操作。
     * 
     * 该方法主要完成以下工作：
     * 1. 获取要恢复的分区列表
     * 2. 遍历分区列表，逐个恢复分区
     * 3. 更新订阅状态
     *
     * @param event 恢复分区事件
     */
    private void process(final ResumePartitionsEvent event) {
        try {
            // 获取要恢复的分区列表
            Collection<TopicPartition> partitions = event.partitions();
            log.debug("Resuming partitions {}", partitions);

            // 遍历分区列表，逐个恢复分区
            for (TopicPartition partition : partitions) {
                subscriptions.resume(partition);
            }

            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 处理获取当前消费延迟事件。该方法用于计算指定分区的消费延迟（消息堆积量）。
     * 
     * 该方法主要完成以下工作：
     * 1. 获取分区的当前消费延迟
     * 2. 如果无法获取延迟信息：
     *    - 检查是否需要获取日志末尾位移
     *    - 发起获取位移的请求
     * 3. 返回延迟信息
     *
     * @param event 获取当前消费延迟事件
     */
    private void process(final CurrentLagEvent event) {
        try {
            // 获取目标分区和隔离级别
            final TopicPartition topicPartition = event.partition();
            final IsolationLevel isolationLevel = event.isolationLevel();
            // 获取分区的当前消费延迟
            final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

            final OptionalLong lagOpt;
            if (lag == null) {
                // 如果无法获取日志末尾位移且没有进行中的位移请求
                if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                    // 记录日志并请求获取日志末尾位移
                    log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                    subscriptions.requestPartitionEndOffset(topicPartition);

                    // 创建获取最新位移的请求
                    Map<TopicPartition, Long> timestampToSearch = Collections.singletonMap(
                        topicPartition,
                        ListOffsetsRequest.LATEST_TIMESTAMP
                    );

                    // 发送获取位移请求
                    requestManagers.offsetsRequestManager.fetchOffsets(timestampToSearch, false);
                }

                // 如果无法获取延迟信息，返回空
                lagOpt = OptionalLong.empty();
            } else {
                // 如果成功获取延迟信息，包装后返回
                lagOpt = OptionalLong.of(lag);
            }

            event.future().complete(lagOpt);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * 创建一个用于处理CompletableFuture完成回调的BiConsumer
     * 
     * 该方法主要用于统一处理异步操作的完成结果，包括：
     * 1. 正常完成时设置返回值
     * 2. 异常完成时设置异常信息
     *
     * @param <T> CompletableFuture的结果类型
     * @param b 需要处理完成回调的CompletableFuture实例
     * @return 处理完成回调的BiConsumer实例
     */
    private <T> BiConsumer<? super T, ? super Throwable> complete(final CompletableFuture<T> b) {
        return (value, exception) -> {
            // 如果存在异常，则将Future标记为异常完成
            if (exception != null)
                b.completeExceptionally(exception);
            // 如果正常完成，则设置返回值
            else
                b.complete(value);
        };
    }

    /**
     * 创建一个延迟初始化的ApplicationEventProcessor实例的Supplier
     * 
     * 该方法使用CachedSupplier实现延迟创建，主要用于：
     * 1. 确保ApplicationEventProcessor在ConsumerNetworkThread中按需创建
     * 2. 避免过早初始化导致的资源浪费
     * 3. 保证线程安全的实例创建
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param metadata 消费者元数据，包含集群信息
     * @param subscriptions 订阅状态管理器
     * @param requestManagersSupplier 请求管理器的提供者
     * @return 用于创建ApplicationEventProcessor的Supplier实例
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final SubscriptionState subscriptions,
                                                               final Supplier<RequestManagers> requestManagersSupplier) {
        return new CachedSupplier<>() {
            @Override
            protected ApplicationEventProcessor create() {
                // 获取请求管理器实例
                RequestManagers requestManagers = requestManagersSupplier.get();
                // 创建并返回ApplicationEventProcessor实例
                return new ApplicationEventProcessor(
                        logContext,
                        requestManagers,
                        metadata,
                        subscriptions
                );
            }
        };
    }

    /**
     * 根据消费者订阅的正则表达式模式更新主题订阅列表
     * 
     * 该方法主要完成以下工作：
     * 1. 检查消费者组成员管理器是否可用
     * 2. 根据正则表达式过滤匹配的主题
     * 3. 更新订阅状态
     * 4. 触发元数据更新（如果有新主题）
     * 5. 通知成员管理器订阅已更新
     *
     * @param cluster 用于获取主题列表的集群信息
     */
    private void updatePatternSubscription(Cluster cluster) {
        // 检查消费者组成员管理器是否可用
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        // 根据正则表达式过滤匹配的主题
        final Set<String> topicsToSubscribe = cluster.topics().stream()
            .filter(subscriptions::matchesSubscribedPattern)
            .collect(Collectors.toSet());
        // 更新订阅状态，如果有新主题则请求更新元数据
        if (subscriptions.subscribeFromPattern(topicsToSubscribe)) {
            this.metadataVersionSnapshot = metadata.requestUpdateForNewTopics();
        }
        // 通知成员管理器订阅已更新
        // 如果还不是组成员则加入组，如果已经是组成员则在下次poll时发送更新后的订阅信息
        // 注意：即使没有主题匹配正则表达式，也会执行这一步，以确保成员在需要时加入组（带有空订阅）
        requestManagers.consumerHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
    }

    /**
     * 获取当前元数据版本快照
     * 该方法主要用于测试，用于验证元数据更新是否正确执行
     *
     * @return 当前的元数据版本号
     */
    // Visible for testing
    int metadataVersionSnapshot() {
        return metadataVersionSnapshot;
    }
}
