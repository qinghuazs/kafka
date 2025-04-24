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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * 一个用于跟踪消费者的主题、分区和偏移量的类。分区可以通过以下两种方式之一被"分配"：
 * 1. 通过{@link #assignFromUser(Set)}直接分配(手动分配)
 * 2. 通过{@link #assignFromSubscribed(Collection)}从订阅中自动分配
 * <p>
 * 分区被分配后，在使用{@link #seekValidated(TopicPartition, FetchPosition)}设置其初始位置之前，
 * 该分区不被视为"可获取的"。可获取的分区会跟踪一个位置，该位置是已返回给用户的最后一个偏移量。
 * 你可以通过{@link #pause(TopicPartition)}暂停从分区获取数据，而不影响已消费的位置。
 * 分区将保持不可获取状态，直到使用{@link #resume(TopicPartition)}恢复。
 * 你也可以通过{@link #isPaused(TopicPartition)}独立查询暂停状态。
 * <p>
 * 注意：当分区分配发生变化时（无论是用户直接更改还是通过组重平衡），暂停状态和已消费的位置都不会被保留。
 * <p>
 * 线程安全：此类是线程安全的。
 */
public class SubscriptionState {
    // 当尝试同时订阅主题、分区和模式时的异常消息
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    // 日志记录器
    private final Logger log;

    /**
     * 订阅类型枚举，定义了所有可能的订阅状态：
     * - NONE: 未订阅状态
     * - AUTO_TOPICS: 自动订阅指定主题
     * - AUTO_PATTERN: 自动订阅匹配正则表达式的主题
     * - AUTO_PATTERN_RE2J: 使用Re2J引擎的正则表达式订阅
     * - USER_ASSIGNED: 用户手动分配分区
     * - AUTO_TOPICS_SHARE: 共享消费组的主题订阅
     */
    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, AUTO_PATTERN_RE2J, USER_ASSIGNED, AUTO_TOPICS_SHARE
    }

    // 当前的订阅类型
    private SubscriptionType subscriptionType;

    // 用户请求的正则表达式模式
    private Pattern subscribedPattern;

    // 用户请求的Re2J正则表达式模式
    private SubscriptionPattern subscribedRe2JPattern;

    // 用户请求订阅的主题列表
    private Set<String> subscription;

    // 消费组订阅的主题列表
    // 对于消费组的leader来说，这可能包含一些不在subscription中的主题
    // 因为leader负责检测需要触发组重平衡的元数据变化
    private Set<String> groupSubscription;

    // 当前分配的分区，注意分区的顺序很重要(详见FetchBuilder)
    private final PartitionStates<TopicPartitionState> assignment;

    // 默认的偏移量重置策略
    private final AutoOffsetResetStrategy defaultResetStrategy;

    // 当分配发生变化时要调用的用户提供的监听器
    private Optional<ConsumerRebalanceListener> rebalanceListener = Optional.empty();

    // 分配ID，每次分配变化时递增
    private int assignmentId = 0;

    /**
     * 重写toString方法，返回订阅状态的字符串表示
     * 包含订阅类型、订阅模式、订阅主题列表、组订阅、默认重置策略和分区分配信息
     */
    @Override
    public synchronized String toString() {
        return "SubscriptionState{" +
            "type=" + subscriptionType +
            ", subscribedPattern=" + subscribedPatternInUse() +
            ", subscription=" + String.join(",", subscription) +
            ", groupSubscription=" + String.join(",", groupSubscription) +
            ", defaultResetStrategy=" + defaultResetStrategy +
            ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
    }

    /**
     * 获取当前使用的订阅模式
     * @return 如果是Re2J模式返回Re2J模式对象，如果是普通正则模式返回Pattern对象，否则返回null
     */
    private Object subscribedPatternInUse() {
        if (subscriptionType == SubscriptionType.AUTO_PATTERN_RE2J)
            return subscribedRe2JPattern;
        if (subscriptionType == SubscriptionType.AUTO_PATTERN)
            return subscribedPattern;
        return null;
    }

    /**
     * 返回订阅状态的友好字符串表示
     * 根据不同的订阅类型返回不同格式的描述字符串
     */
    public synchronized String prettyString() {
        switch (subscriptionType) {
            case NONE:
                return "None";
            case AUTO_TOPICS:
                return "Subscribe(" + String.join(",", subscription) + ")";
            case AUTO_PATTERN:
                return "Subscribe(" + subscribedPattern + ")";
            case AUTO_PATTERN_RE2J:
                return "Subscribe(" + subscribedRe2JPattern + ")";
            case USER_ASSIGNED:
                return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
            case AUTO_TOPICS_SHARE:
                return "Subscribe to Share Group(" + String.join(",", subscription) + ")";
            default:
                throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
        }
    }

    /**
     * 构造函数，初始化订阅状态对象
     * @param logContext 日志上下文
     * @param defaultResetStrategy 默认的偏移量重置策略
     */
    public SubscriptionState(LogContext logContext, AutoOffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        // 使用TreeSet以获得更好的日志输出效果
        this.subscription = new TreeSet<>();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscribedRe2JPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * 获取当前的分配ID
     * 这是一个单调递增的ID，每次分配变化时都会增加
     * 可用于检查分配是否发生了变化
     *
     * @return 当前的分配ID
     */
    synchronized int assignmentId() {
        return assignmentId;
    }

    /**
     * 设置或验证订阅类型
     * 如果当前类型为NONE，则设置新类型
     * 如果当前已有类型，则验证新类型是否与现有类型相同
     * 
     * @param type 要设置的订阅类型
     * @throws IllegalStateException 如果尝试设置与现有类型不同的类型
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    /**
     * 订阅指定的主题集合
     * 
     * @param topics 要订阅的主题集合
     * @param listener 再平衡监听器
     * @return 如果订阅发生变化返回true，否则返回false
     */
    public synchronized boolean subscribe(Set<String> topics, Optional<ConsumerRebalanceListener> listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        return changeSubscription(topics);
    }

    /**
     * 使用正则表达式模式订阅主题
     * 
     * @param pattern 主题匹配的正则表达式
     * @param listener 再平衡监听器
     */
    public synchronized void subscribe(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_PATTERN);
        this.subscribedPattern = pattern;
    }

    /**
     * 使用Re2J正则表达式模式订阅主题
     * 
     * @param pattern Re2J主题匹配模式
     * @param listener 再平衡监听器
     */
    public synchronized void subscribe(SubscriptionPattern pattern, Optional<ConsumerRebalanceListener> listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_PATTERN_RE2J);
        this.subscribedRe2JPattern = pattern;
    }

    /**
     * 从正则表达式模式订阅更新实际的主题订阅
     * 
     * @param topics 匹配模式的主题集合
     * @return 如果订阅发生变化返回true，否则返回false
     * @throws IllegalArgumentException 如果当前不是正则表达式订阅模式
     */
    public synchronized boolean subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        return changeSubscription(topics);
    }

    /**
     * 订阅共享消费组的主题
     * 
     * @param topics 要订阅的主题集合
     * @return 如果订阅发生变化返回true，否则返回false
     */
    public synchronized boolean subscribeToShareGroup(Set<String> topics) {
        registerRebalanceListener(Optional.empty());
        setSubscriptionType(SubscriptionType.AUTO_TOPICS_SHARE);
        return changeSubscription(topics);
    }

    /**
     * 更新订阅的主题集合
     * 
     * @param topicsToSubscribe 新的主题集合
     * @return 如果订阅发生变化返回true，否则返回false
     */
    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe))
            return false;

        subscription = topicsToSubscribe;
        return true;
    }

    /**
     * 设置当前消费组的订阅
     * 这个方法由消费组的leader使用，以确保它能接收到消费组所有感兴趣主题的元数据更新
     * 
     * 应用场景：
     * 1. 在消费组重平衡过程中，leader需要获取所有成员订阅的主题元数据
     * 2. leader需要监控这些主题的元数据变化，以决定是否需要触发新的重平衡
     *
     * @param topics 消费组订阅的所有主题集合
     * @return 如果消费组订阅包含了本地未订阅的主题则返回true
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        // 检查是否处于自动分配分区模式，如果不是则抛出异常
        if (!hasAutoAssignedPartitions())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        // 更新消费组的订阅主题集合
        groupSubscription = new HashSet<>(topics);
        // 检查本地订阅是否包含了所有消费组订阅的主题
        return !subscription.containsAll(groupSubscription);
    }

    /**
     * 重置消费组的订阅，使其只包含当前消费者订阅的主题
     * 
     * 应用场景：
     * 1. 当消费者离开消费组时
     * 2. 当消费组leader角色发生变更时
     * 3. 当需要清理旧的消费组订阅信息时
     */
    synchronized void resetGroupSubscription() {
        // 将消费组订阅设置为空集合
        groupSubscription = Collections.emptySet();
    }

    /**
     * 更改分区分配为用户指定的分区集合
     * 这与{@link #assignFromSubscribed(Collection)}不同，后者的分区是从订阅的主题中自动分配的
     * 
     * 应用场景：
     * 1. 用户手动指定要消费的分区
     * 2. 实现自定义的分区分配策略
     * 3. 需要精确控制分区消费的场景
     *
     * @param partitions 用户指定要分配的分区集合
     * @return 如果分配发生变化返回true，否则返回false
     */
    public synchronized boolean assignFromUser(Set<TopicPartition> partitions) {
        // 设置订阅类型为用户手动分配
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        // 如果分配的分区集合没有变化，则返回false
        if (this.assignment.partitionSet().equals(partitions))
            return false;

        // 增加分配ID，标识新的分配
        assignmentId++;

        // 更新订阅的主题集合
        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            // 获取分区的状态，如果不存在则创建新的状态
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null)
                state = new TopicPartitionState();
            partitionToState.put(partition, state);

            // 将分区对应的主题添加到手动订阅主题集合
            manualSubscribedTopics.add(partition.topic());
        }

        // 更新分区分配状态
        this.assignment.set(partitionToState);
        // 更新订阅主题并返回是否发生变化
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * 检查使用经典消费组协议时收到的分区分配是否与订阅匹配
     * 注意：这里只考虑subscribedPattern，因为这个功能只在经典协议下使用
     * 经典协议不支持subscribedRe2JPattern
     * 
     * 应用场景：
     * 1. 验证分配的分区是否都来自已订阅的主题
     * 2. 防止消费者接收到未订阅主题的分区
     * 3. 在消费组重平衡时进行分配验证
     *
     * @param assignments 要检查的分区分配
     * @return 如果分配与订阅匹配返回true，否则返回false
     */
    public synchronized boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        for (TopicPartition topicPartition : assignments) {
            // 如果使用了正则表达式订阅
            if (this.subscribedPattern != null) {
                // 检查分区的主题是否匹配订阅模式
                if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                        topicPartition,
                        this.subscribedPattern);

                    return false;
                }
            } else {
                // 检查分区的主题是否在订阅列表中
                if (!this.subscription.contains(topicPartition.topic())) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 更改分区分配为协调器返回的指定分区集合
     * 这与{@link #assignFromUser(Set)}不同，后者直接使用用户输入设置分配
     * 
     * 应用场景：
     * 1. 消费组重平衡后接收新的分区分配
     * 2. 动态分区分配变更
     * 3. 自动分配模式下的分区分配更新
     *
     * @param assignments 要分配的分区集合
     * @throws IllegalArgumentException 如果在手动分配模式下尝试动态分配分区
     */
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        // 检查是否处于自动分配模式
        if (!this.hasAutoAssignedPartitions())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        // 为每个分配的分区创建或获取状态
        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            TopicPartitionState state = this.assignment.stateValue(tp);
            if (state == null)
                state = new TopicPartitionState();
            assignedPartitionStates.put(tp, state);
        }

        // 增加分配ID并更新分区分配
        assignmentId++;
        this.assignment.set(assignedPartitionStates);
    }

    /**
     * 注册再平衡监听器
     * 
     * 应用场景：
     * 1. 在订阅主题时设置监听器
     * 2. 在分区分配变化时接收通知
     * 3. 在再平衡开始和结束时执行自定义逻辑
     *
     * @param listener 要注册的再平衡监听器
     */
    private void registerRebalanceListener(Optional<ConsumerRebalanceListener> listener) {
        this.rebalanceListener = Objects.requireNonNull(listener, "RebalanceListener cannot be null");
    }

    /**
     * 检查是否使用了正则表达式订阅模式
     * 
     * 应用场景：
     * 1. 确定当前的订阅类型
     * 2. 在处理主题匹配时判断是否需要使用正则匹配
     * 3. 在更新订阅时决定使用哪种逻辑
     *
     * @return 如果使用了正则表达式订阅返回true
     */
    public synchronized boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    /**
     * 检查是否没有任何订阅或用户分配
     * 
     * 应用场景：
     * 1. 检查消费者是否处于初始状态
     * 2. 在进行订阅或分配操作前进行状态检查
     * 3. 判断是否需要执行初始化逻辑
     *
     * @return 如果没有订阅或分配返回true
     */
    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    /**
     * 取消所有订阅
     * 
     * 应用场景：
     * 1. 消费者需要重新设置订阅时
     * 2. 消费者关闭时清理订阅状态
     * 3. 需要重置消费者状态时
     */
    public synchronized void unsubscribe() {
        // 清空所有订阅和分配信息
        this.subscription = Collections.emptySet();
        this.groupSubscription = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        // 增加分配ID表示状态变化
        this.assignmentId++;
    }

    /**
     * 检查主题是否匹配订阅的模式
     * 
     * 应用场景：
     * 1. 在动态发现新主题时检查是否需要订阅
     * 2. 在消费组重平衡时验证分配的主题是否符合订阅模式
     * 3. 在元数据更新时过滤不匹配的主题
     *
     * @param topic 要检查的主题名称
     * @return 如果使用了模式订阅且主题匹配该模式则返回true，否则返回false
     */
    public synchronized boolean matchesSubscribedPattern(String topic) {
        // 获取当前的订阅模式
        Pattern pattern = this.subscribedPattern;
        // 如果使用了模式订阅且模式不为空，则检查主题是否匹配
        if (hasPatternSubscription() && pattern != null)
            return pattern.matcher(topic).matches();
        return false;
    }

    /**
     * 获取当前订阅的主题集合
     * 
     * 应用场景：
     * 1. 获取消费者当前订阅的所有主题
     * 2. 在重平衡时确定需要分配的主题范围
     * 3. 在元数据更新时确定需要获取元数据的主题
     *
     * @return 如果是自动分配分区模式则返回订阅的主题集合，否则返回空集合
     */
    public synchronized Set<String> subscription() {
        // 只有在自动分配分区模式下才返回订阅集合
        if (hasAutoAssignedPartitions())
            return this.subscription;
        return Collections.emptySet();
    }

    /**
     * 获取当前使用的Re2J兼容的订阅模式
     * 
     * 应用场景：
     * 1. 在使用Re2J引擎进行主题匹配时获取模式
     * 2. 在需要序列化订阅信息时获取模式信息
     * 3. 在调试和日志记录时获取当前的订阅模式
     *
     * @return 通过{@link #subscribe(SubscriptionPattern, Optional)}设置的Re2J模式，
     *         如果没有使用Re2J模式则返回null
     */
    public synchronized SubscriptionPattern subscriptionPattern() {
        // 只有在使用Re2J模式订阅时才返回模式对象
        if (hasRe2JPatternSubscription())
            return this.subscribedRe2JPattern;
        return null;
    }

    /**
     * 检查是否使用了Re2J模式进行订阅
     * 
     * 应用场景：
     * 1. 确定使用哪种正则引擎进行主题匹配
     * 2. 在序列化订阅信息时判断模式类型
     * 3. 在日志记录时区分订阅模式类型
     *
     * @return 如果使用Re2J模式订阅返回true，否则返回false
     */
    public synchronized boolean hasRe2JPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN_RE2J;
    }

    /**
     * 获取当前已暂停的分区集合
     * 
     * 应用场景：
     * 1. 获取所有暂停消费的分区
     * 2. 在重平衡时保存暂停状态
     * 3. 在恢复消费时确定哪些分区需要继续暂停
     *
     * @return 当前已暂停的分区集合
     */
    public synchronized Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused);
    }

    /**
     * 获取需要元数据的订阅主题集合
     * 
     * 应用场景：
     * 1. 消费组leader获取所有成员的订阅主题，用于分区分配
     * 2. 普通成员获取自己的订阅主题，用于检测元数据变化
     * 3. 在重平衡时确定需要获取元数据的主题范围
     *
     * 实现细节：
     * - 如果没有组订阅信息，返回个人订阅集合
     * - 如果组订阅包含所有个人订阅，返回组订阅集合
     * - 否则返回组订阅和个人订阅的并集，确保包含所有需要的主题
     *
     * @return 如果是消费组leader则返回组内所有订阅主题的并集，
     *         否则返回与{@link #subscription()}相同的集合
     */
    synchronized Set<String> metadataTopics() {
        if (groupSubscription.isEmpty())
            return subscription;
        else if (groupSubscription.containsAll(subscription))
            return groupSubscription;
        else {
            // 当订阅发生变化时，groupSubscription可能过时，
            // 确保返回新的订阅主题
            Set<String> topics = new HashSet<>(groupSubscription);
            topics.addAll(subscription);
            return topics;
        }
    }

    /**
     * 检查是否需要指定主题的元数据
     * 
     * 应用场景：
     * 1. 确定是否需要获取某个主题的元数据
     * 2. 在元数据更新时过滤不相关的主题
     * 3. 优化元数据请求，只获取必要的主题信息
     *
     * @param topic 要检查的主题
     * @return 如果主题在个人订阅或组订阅中返回true，否则返回false
     */
    synchronized boolean needsMetadata(String topic) {
        return subscription.contains(topic) || groupSubscription.contains(topic);
    }

    /**
     * 获取分区的分配状态，如果分区未分配则抛出异常
     * 
     * 应用场景：
     * 1. 在进行位置查找和更新操作前验证分区状态
     * 2. 确保只对已分配的分区进行操作
     * 3. 及时发现和报告分区分配问题
     *
     * @param tp 要获取状态的主题分区
     * @return 分区的状态对象
     * @throws IllegalStateException 如果分区未被分配
     */
    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    /**
     * 获取分区的分配状态，如果分区未分配则返回null
     * 
     * 应用场景：
     * 1. 在不确定分区是否分配时安全地获取状态
     * 2. 在批量操作时处理可能未分配的分区
     * 3. 在状态检查时避免异常处理
     *
     * @param tp 要获取状态的主题分区
     * @return 分区的状态对象，如果分区未分配则返回null
     */
    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    /**
     * 设置分区的已验证获取位置
     * 
     * 应用场景：
     * 1. 在确认位置有效后更新消费位置
     * 2. 在重置位置后设置新的起始位置
     * 3. 在恢复消费时设置已验证的位置
     *
     * @param tp 要设置位置的主题分区
     * @param position 要设置的获取位置
     */
    public synchronized void seekValidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekValidated(position);
    }

    /**
     * 将分区的位置设置到指定的偏移量
     * 
     * 应用场景：
     * 1. 手动设置消费起始位置
     * 2. 实现消费位置的跳转
     * 3. 在出现问题时重置消费位置
     *
     * @param tp 要设置位置的主题分区
     * @param offset 要设置的偏移量
     */
    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
    }

    /**
     * 设置分区的未验证获取位置
     * 
     * 应用场景：
     * 1. 在位置验证前临时设置位置
     * 2. 在重置过程中设置初始位置
     * 3. 在恢复过程中设置待验证的位置
     *
     * @param tp 要设置位置的主题分区
     * @param position 要设置的获取位置
     */
    public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekUnvalidated(position);
    }

    /**
     * 尝试设置分区的未验证获取位置，但只在特定条件满足时执行
     * 
     * 应用场景：
     * 1. 在自动重置位置时安全地更新位置
     * 2. 在重平衡后根据需要重置位置
     * 3. 在错误恢复时有条件地更新位置
     *
     * 实现细节：
     * - 检查分区是否仍然分配
     * - 检查是否仍然需要重置
     * - 验证重置策略是否匹配
     * - 符合条件时更新位置并记录日志
     *
     * @param tp 要设置位置的主题分区
     * @param position 要设置的获取位置
     * @param requestedResetStrategy 请求的重置策略
     */
    synchronized void maybeSeekUnvalidated(TopicPartition tp, FetchPosition position, AutoOffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != null && !requestedResetStrategy.equals(state.resetStrategy)) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to position {}.", tp, position);
            state.seekUnvalidated(position);
        }
    }

    /**
     * 获取当前分配的分区集合的可修改副本
     * 
     * 应用场景：
     * 1. 获取当前消费者负责的所有分区
     * 2. 在重平衡时保存当前分配信息
     * 3. 在状态报告时获取分配信息
     *
     * @return 当前分配的分区集合的可修改副本
     */
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(this.assignment.partitionSet());
    }

    /**
     * 获取当前分配的分区列表的可修改副本
     * 
     * 应用场景：
     * 1. 需要有序处理分区时获取分区列表
     * 2. 在批量操作时获取分区列表
     * 3. 在需要保持分区顺序的场景使用
     *
     * @return 当前分配的分区列表的可修改副本
     */
    public synchronized List<TopicPartition> assignedPartitionsList() {
        return new ArrayList<>(this.assignment.partitionSet());
    }

    /**
     * 以线程安全的方式获取已分配分区的数量
     * 
     * 应用场景：
     * 1. 监控消费者分区分配状态
     * 2. 判断是否需要触发再平衡
     * 3. 负载均衡决策
     *
     * @return 已分配的分区数量
     */
    synchronized int numAssignedPartitions() {
        // 返回当前分配的分区数量
        return this.assignment.size();
    }

    /**
     * 获取当前可获取数据的分区列表
     * 该方法主要用于测试，但也展示了如何高效地过滤可获取的分区
     * 
     * 应用场景：
     * 1. 获取当前可以拉取数据的分区列表
     * 2. 在消费者拉取循环中使用
     * 3. 测试分区状态管理
     *
     * @param isAvailable 判断分区是否可用的谓词函数
     * @return 可获取数据的分区列表
     */
    public synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        // 由于这是获取数据的热路径，我们使用显式迭代而不是Stream API以提高性能
        List<TopicPartition> result = new ArrayList<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            // 首先进行快速检查以避免不必要的谓词评估
            // 如果是共享消费组类型或分区状态为可获取，且分区可用，则添加到结果列表
            if ((subscriptionType.equals(SubscriptionType.AUTO_TOPICS_SHARE) || topicPartitionState.isFetchable())
                    && isAvailable.test(topicPartition)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    /**
     * 检查当前是否使用自动分区分配模式
     * 
     * 应用场景：
     * 1. 判断是否需要进行消费组协调
     * 2. 确定分区分配策略
     * 3. 验证操作是否允许
     *
     * @return 如果使用自动分配模式返回true
     */
    public synchronized boolean hasAutoAssignedPartitions() {
        // 检查订阅类型是否为自动分配类型（包括普通主题订阅、模式订阅和共享组订阅）
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN
                || this.subscriptionType == SubscriptionType.AUTO_TOPICS_SHARE || this.subscriptionType == SubscriptionType.AUTO_PATTERN_RE2J;
    }

    /**
     * 检查指定主题是否通过Re2J模式订阅并已分配分区
     * 
     * 应用场景：
     * 1. 验证主题是否匹配Re2J订阅模式
     * 2. 处理动态发现的主题
     * 3. 元数据更新时的主题验证
     *
     * @param topic 要检查的主题名称
     * @return 如果主题通过Re2J模式订阅并已分配分区则返回true
     */
    public synchronized boolean isAssignedFromRe2j(String topic) {
        // 如果不是Re2J模式订阅，直接返回false
        if (!hasRe2JPatternSubscription()) {
            return false;
        }

        // 遍历所有已分配的分区，检查是否存在该主题的分区
        for (TopicPartition topicPartition : assignment.partitionSet()) {
            if (topicPartition.topic().equals(topic)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 设置分区的消费位置
     * 
     * 应用场景：
     * 1. 手动设置消费位置
     * 2. 恢复已提交的偏移量
     * 3. 实现自定义的位置管理策略
     *
     * @param tp 目标主题分区
     * @param position 要设置的消费位置
     */
    public synchronized void position(TopicPartition tp, FetchPosition position) {
        // 获取分区状态并更新位置
        assignedState(tp).position(position);
    }

    /**
     * 尝试为当前leader验证分区位置
     * 如果leader支持可用的OffsetsForLeaderEpoch API版本，则进入位置验证状态
     * 如果leader不支持该API，则直接完成位置验证
     * 
     * 应用场景：
     * 1. 消费者启动时的位置验证
     * 2. leader变更后的位置验证
     * 3. 检测和处理日志截断
     *
     * @param apiVersions 支持的API版本信息
     * @param tp 要验证的主题分区
     * @param leaderAndEpoch leader及其纪元信息
     * @return 如果进入位置验证状态返回true
     */
    public synchronized boolean maybeValidatePositionForCurrentLeader(ApiVersions apiVersions,
                                                                      TopicPartition tp,
                                                                      Metadata.LeaderAndEpoch leaderAndEpoch) {
        // 获取分区状态，如果分区未分配则跳过验证
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("跳过验证未分配分区 {} 的位置", tp);
            return false;
        }

        // 如果存在leader，检查其API版本支持
        if (leaderAndEpoch.leader.isPresent()) {
            NodeApiVersions nodeApiVersions = apiVersions.get(leaderAndEpoch.leader.get().idString());
            if (nodeApiVersions == null || hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                // 如果broker支持所需的API版本，进行位置验证
                return state.maybeValidatePosition(leaderAndEpoch);
            } else {
                // 如果broker不支持新版本的OffsetsForLeaderEpoch，跳过验证
                state.updatePositionLeaderNoValidation(leaderAndEpoch);
                return false;
            }
        } else {
            // 如果没有leader信息，仍然尝试验证位置
            return state.maybeValidatePosition(leaderAndEpoch);
        }
    }

    /**
     * 尝试使用OffsetForLeaderEpoch请求返回的结束偏移量完成验证
     * 
     * 应用场景：
     * 1. 处理位置验证的响应
     * 2. 检测和处理日志截断
     * 3. 实现位置重置策略
     *
     * @param tp 主题分区
     * @param requestPosition 请求时的位置
     * @param epochEndOffset 返回的纪元结束偏移量
     * @return 如果检测到日志截断且没有设置重置策略，返回截断详情
     */
    public synchronized Optional<LogTruncation> maybeCompleteValidation(TopicPartition tp,
                                                                        FetchPosition requestPosition,
                                                                        EpochEndOffset epochEndOffset) {
        // 获取分区状态
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            // 如果分区未分配，记录日志并跳过
            log.debug("跳过未分配分区 {} 的验证完成处理", tp);
        } else if (!state.awaitingValidation()) {
            // 如果分区不再等待验证，记录日志并跳过
            log.debug("跳过不再期望验证的分区 {} 的验证完成处理", tp);
        } else {
            // 获取当前位置
            SubscriptionState.FetchPosition currentPosition = state.position;
            if (!currentPosition.equals(requestPosition)) {
                // 如果当前位置与请求时的位置不匹配，记录日志并跳过
                log.debug("跳过分区 {} 的验证完成处理，因为当前位置 {} 与发送请求时的位置 {} 不匹配",
                          tp, currentPosition, requestPosition);
            } else if (epochEndOffset.endOffset() == UNDEFINED_EPOCH_OFFSET ||
                        epochEndOffset.leaderEpoch() == UNDEFINED_EPOCH) {
                // 如果返回未定义的偏移量或纪元
                if (hasDefaultOffsetResetPolicy()) {
                    // 如果有默认重置策略，请求重置偏移量
                    log.info("在位置 {} 检测到分区 {} 的日志截断，重置偏移量",
                             tp, currentPosition);
                    requestOffsetReset(tp);
                } else {
                    // 如果没有重置策略，记录警告并返回截断信息
                    log.warn("在位置 {} 检测到分区 {} 的日志截断，但未设置重置策略",
                             tp, currentPosition);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.empty()));
                }
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                // 如果返回的结束偏移量小于当前位置（检测到截断）
                if (hasDefaultOffsetResetPolicy()) {
                    // 如果有默认重置策略，创建新位置并重置
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("在位置 {} 检测到分区 {} 的日志截断，重置偏移量到已知的第一个分歧位置 {}",
                             tp, currentPosition, newPosition);
                    state.seekValidated(newPosition);
                } else {
                    // 如果没有重置策略，记录警告并返回截断信息
                    OffsetAndMetadata divergentOffset = new OffsetAndMetadata(epochEndOffset.endOffset(),
                        Optional.of(epochEndOffset.leaderEpoch()), null);
                    log.warn("在位置 {} 检测到分区 {} 的日志截断（broker的结束偏移量是 {}），但未设置重置策略",
                             tp, currentPosition, divergentOffset);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.of(divergentOffset)));
                }
            } else {
                // 如果没有检测到截断，完成验证
                state.completeValidation();
            }
        }

        return Optional.empty();
    }

    /**
     * 检查分区是否正在等待位置验证
     * 
     * 应用场景：
     * 1. 检查位置验证状态
     * 2. 决定是否需要发送验证请求
     * 3. 跟踪验证进度
     *
     * @param tp 要检查的主题分区
     * @return 如果分区正在等待验证返回true
     */
    public synchronized boolean awaitingValidation(TopicPartition tp) {
        return assignedState(tp).awaitingValidation();
    }

    /**
     * 完成分区的位置验证
     * 
     * 应用场景：
     * 1. 验证成功后的状态更新
     * 2. 手动完成验证过程
     * 3. 错误恢复时的状态重置
     *
     * @param tp 要完成验证的主题分区
     */
    public synchronized void completeValidation(TopicPartition tp) {
        assignedState(tp).completeValidation();
    }

    /**
     * 获取分区的有效位置
     * 
     * 应用场景：
     * 1. 获取已验证的消费位置
     * 2. 恢复中断的消费
     * 3. 位置检查点处理
     *
     * @param tp 主题分区
     * @return 分区的有效位置
     */
    public synchronized FetchPosition validPosition(TopicPartition tp) {
        return assignedState(tp).validPosition();
    }

    /**
     * 获取分区的当前位置
     * 
     * 应用场景：
     * 1. 获取当前消费进度
     * 2. 位置监控和报告
     * 3. 位置保存和恢复
     *
     * @param tp 主题分区
     * @return 分区的当前位置
     */
    public synchronized FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    /**
     * 获取分区的当前位置，如果分区未分配则返回null
     * 
     * 应用场景：
     * 1. 安全地获取位置信息
     * 2. 处理可能未分配的分区
     * 3. 位置查询和验证
     *
     * @param tp 主题分区
     * @return 分区的当前位置，如果分区未分配则返回null
     */
    public synchronized FetchPosition positionOrNull(TopicPartition tp) {
        // 获取分区状态，如果未分配则返回null
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            return null;
        }
        return assignedState(tp).position;
    }

    /**
     * 计算指定分区的消费延迟（消息堆积量）
     * 
     * 应用场景：
     * 1. 监控消费进度，检测消费延迟
     * 2. 实现背压机制，根据延迟调整消费速率
     * 3. 告警系统中用于检测消息堆积
     *
     * @param tp 要计算延迟的主题分区
     * @param isolationLevel 事务隔离级别，决定使用哪个偏移量作为上界
     * @return 消费延迟值（未消费的消息数），如果无法计算则返回null
     */
    public synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        // 获取分区的状态信息
        TopicPartitionState topicPartitionState = assignedState(tp);
        // 如果没有消费位置，无法计算延迟
        if (topicPartitionState.position == null) {
            return null;
        } else if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            // 事务消费模式：使用最后稳定偏移量计算延迟
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        } else {
            // 非事务消费模式：使用高水位计算延迟
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
        }
    }

    /**
     * 获取分区的结束偏移量
     * 
     * 应用场景：
     * 1. 计算分区的总消息数
     * 2. 判断是否已经消费到分区末尾
     * 3. 在事务消费场景中确定可以安全消费的范围
     *
     * @param tp 要查询的主题分区
     * @param isolationLevel 事务隔离级别，决定返回哪个类型的结束偏移量
     * @return 分区的结束偏移量，对于READ_COMMITTED模式返回最后稳定偏移量，否则返回高水位标记
     */
    public synchronized Long partitionEndOffset(TopicPartition tp, IsolationLevel isolationLevel) {
        // 获取分区的状态信息
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            // 事务消费模式：返回最后稳定偏移量（所有已完成事务的最大偏移量）
            return topicPartitionState.lastStableOffset;
        } else {
            // 非事务消费模式：返回高水位标记（已复制到所有ISR的最大偏移量）
            return topicPartitionState.highWatermark;
        }
    }

    /**
     * 请求获取分区的结束偏移量
     * 
     * 应用场景：
     * 1. 在消费者初始化时获取分区的最新位置
     * 2. 在消费者重置位置时获取分区的边界
     * 3. 在需要重新同步消费位置时使用
     *
     * @param tp 要请求结束偏移量的主题分区
     */
    public synchronized void requestPartitionEndOffset(TopicPartition tp) {
        // 获取分区的状态信息
        TopicPartitionState topicPartitionState = assignedState(tp);
        // 标记该分区需要获取结束偏移量
        topicPartitionState.requestEndOffset();
    }

    /**
     * 检查是否已经请求获取指定分区的结束偏移量
     * 
     * 应用场景：
     * 1. 避免重复请求分区的结束偏移量
     * 2. 在获取分区元数据时进行状态检查
     * 3. 用于跟踪偏移量请求的处理状态
     *
     * @param tp 要检查的主题分区
     * @return 如果已经请求过该分区的结束偏移量则返回true，否则返回false
     */
    public synchronized boolean partitionEndOffsetRequested(TopicPartition tp) {
        // 获取分区的状态信息
        TopicPartitionState topicPartitionState = assignedState(tp);
        // 返回是否已请求结束偏移量的状态
        return topicPartitionState.endOffsetRequested();
    }

    /**
     * 计算分区的消费延迟（消费位置与日志起始偏移量之间的差值）
     * 
     * 应用场景：
     * 1. 监控消费进度，评估消费延迟情况
     * 2. 实现消费延迟告警
     * 3. 负载均衡时的分区分配决策
     *
     * @param tp 要计算延迟的主题分区
     * @return 如果有日志起始偏移量，返回消费位置与起始偏移量的差值；否则返回null
     */
    synchronized Long partitionLead(TopicPartition tp) {
        // 获取分区的状态信息
        TopicPartitionState topicPartitionState = assignedState(tp);
        // 如果没有日志起始偏移量则返回null，否则返回消费位置与起始偏移量的差值
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    /**
     * 更新分区的高水位标记
     * 
     * 应用场景：
     * 1. 消费者获取新的消息后更新高水位
     * 2. 确保消费者不会读取未完全提交的消息
     * 3. 实现事务隔离级别的读取保证
     *
     * @param tp 要更新的主题分区
     * @param highWatermark 新的高水位值
     */
    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        // 获取分区状态并更新高水位
        assignedState(tp).highWatermark(highWatermark);
    }

    /**
     * 尝试更新分区的高水位标记，仅在分区已分配时更新
     * 
     * 应用场景：
     * 1. 在不确定分区是否分配时安全地更新高水位
     * 2. 避免对未分配分区进行操作
     * 3. 在重平衡过程中的安全更新
     *
     * @param tp 要更新的主题分区
     * @param highWatermark 新的高水位值
     * @return 如果分区已分配且更新成功返回true，否则返回false
     */
    synchronized boolean tryUpdatingHighWatermark(TopicPartition tp, long highWatermark) {
        // 获取分区状态，可能返回null
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            // 分区已分配，更新高水位并返回true
            assignedState(tp).highWatermark(highWatermark);
            return true;
        }
        return false;
    }

    /**
     * 尝试更新分区的日志起始偏移量，仅在分区已分配时更新
     * 
     * 应用场景：
     * 1. 日志压缩或清理后更新起始偏移量
     * 2. 处理日志截断情况
     * 3. 重置消费位置时的边界检查
     *
     * @param tp 要更新的主题分区
     * @param logStartOffset 新的日志起始偏移量
     * @return 如果分区已分配且更新成功返回true，否则返回false
     */
    synchronized boolean tryUpdatingLogStartOffset(TopicPartition tp, long logStartOffset) {
        // 获取分区状态，可能返回null
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            // 分区已分配，更新日志起始偏移量并返回true
            assignedState(tp).logStartOffset(logStartOffset);
            return true;
        }
        return false;
    }

    /**
     * 更新分区的最后稳定偏移量（LSO）
     * 
     * 应用场景：
     * 1. 事务消费时更新最后稳定偏移量
     * 2. 确保事务隔离性
     * 3. 防止读取未提交的事务消息
     *
     * @param tp 要更新的主题分区
     * @param lastStableOffset 新的最后稳定偏移量
     */
    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        // 获取分区状态并更新最后稳定偏移量
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    /**
     * 尝试更新分区的最后稳定偏移量，仅在分区已分配时更新
     * 
     * 应用场景：
     * 1. 在不确定分区是否分配时安全地更新LSO
     * 2. 事务消费场景下的安全更新
     * 3. 重平衡过程中的状态更新
     *
     * @param tp 要更新的主题分区
     * @param lastStableOffset 新的最后稳定偏移量
     * @return 如果分区已分配且更新成功返回true，否则返回false
     */
    synchronized boolean tryUpdatingLastStableOffset(TopicPartition tp, long lastStableOffset) {
        // 获取分区状态，可能返回null
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            // 分区已分配，更新最后稳定偏移量并返回true
            assignedState(tp).lastStableOffset(lastStableOffset);
            return true;
        }
        return false;
    }

    /**
     * 设置首选的读取副本，并设置租约超时时间
     * 超时后，该副本将不再有效，{@link #preferredReadReplica(TopicPartition, long)}将返回空结果
     * 
     * 应用场景：
     * 1. 就近读取优化，从最近的副本读取数据
     * 2. 负载均衡，分散读取压力
     * 3. 网络拓扑优化
     *
     * @param tp 要设置的主题分区
     * @param preferredReadReplicaId 首选的读取副本ID
     * @param timeMs 副本租约的有效期时间戳
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, LongSupplier timeMs) {
        // 获取分区状态并更新首选读取副本
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * 尝试设置首选的读取副本，仅在分区已分配时更新
     * 超时后，该副本将不再有效，{@link #preferredReadReplica(TopicPartition, long)}将返回空结果
     * 
     * 应用场景：
     * 1. 在不确定分区是否分配时安全地更新首选副本
     * 2. 动态调整读取策略
     * 3. 故障转移场景
     *
     * @param tp 要设置的主题分区
     * @param preferredReadReplicaId 首选的读取副本ID
     * @param timeMs 副本租约的有效期时间戳
     * @return 如果分区已分配且更新成功返回true，否则返回false
     */
    public synchronized boolean tryUpdatingPreferredReadReplica(TopicPartition tp,
                                                             int preferredReadReplicaId,
                                                             LongSupplier timeMs) {
        // 获取分区状态，可能返回null
        final TopicPartitionState state = assignedStateOrNull(tp);
        if (state != null) {
            // 分区已分配，更新首选读取副本并返回true
            assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
            return true;
        }
        return false;
    }

    /**
     * 获取首选的读取副本
     * 
     * 应用场景：
     * 1. 确定从哪个副本读取数据
     * 2. 实现最近副本读取策略
     * 3. 优化读取性能
     *
     * @param tp 要查询的主题分区
     * @param timeMs 当前时间戳
     * @return 如果设置了有效的首选副本则返回该副本ID，否则返回空
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        // 获取分区状态，可能返回null
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            // 返回首选读取副本，如果已过期则返回空
            return topicPartitionState.preferredReadReplica(timeMs);
        }
    }

    /**
     * 清除首选的读取副本设置，这将导致获取器返回从leader获取数据
     * 
     * 应用场景：
     * 1. 副本失效时清除设置
     * 2. 手动切换回leader读取
     * 3. 故障恢复场景
     *
     * @param tp 要清除设置的主题分区
     * @return 如果之前设置了首选副本则返回该副本ID，否则返回空
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        // 获取分区状态，可能返回null
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            // 清除首选读取副本设置并返回之前的设置
            return topicPartitionState.clearPreferredReadReplica();
        }
    }

    /**
     * 获取所有已消费的分区的偏移量和元数据
     * 
     * 应用场景：
     * 1. 提交消费位移
     * 2. 检查点保存
     * 3. 消费进度监控
     *
     * @return 包含所有已消费分区的偏移量和元数据的映射
     */
    public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        // 创建结果映射
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        // 遍历所有分配的分区
        assignment.forEach((topicPartition, partitionState) -> {
            // 只包含有效位置的分区
            if (partitionState.hasValidPosition())
                allConsumed.put(topicPartition, new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.offsetEpoch, ""));
        });
        return allConsumed;
    }

    /**
     * 请求重置指定分区的偏移量
     * 
     * 应用场景：
     * 1. 手动重置消费位置
     * 2. 处理无效偏移量
     * 3. 重新消费数据
     *
     * @param partition 要重置的分区
     * @param offsetResetStrategy 偏移量重置策略
     */
    public synchronized void requestOffsetReset(TopicPartition partition, AutoOffsetResetStrategy offsetResetStrategy) {
        // 获取分区状态并请求重置
        assignedState(partition).reset(offsetResetStrategy);
    }

    /**
     * 请求重置多个分区的偏移量
     * 
     * 应用场景：
     * 1. 批量重置消费位置
     * 2. 消费组重平衡后的位置重置
     * 3. 错误恢复场景
     *
     * @param partitions 要重置的分区集合
     * @param offsetResetStrategy 偏移量重置策略
     */
    public synchronized void requestOffsetReset(Collection<TopicPartition> partitions, AutoOffsetResetStrategy offsetResetStrategy) {
        // 遍历所有分区并请求重置
        partitions.forEach(tp -> {
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
            assignedState(tp).reset(offsetResetStrategy);
        });
    }

    /**
     * 使用默认策略请求重置分区的偏移量
     * 
     * 应用场景：
     * 1. 简化的重置操作
     * 2. 使用配置的默认策略
     * 3. 自动错误恢复
     *
     * @param partition 要重置的分区
     */
    public void requestOffsetReset(TopicPartition partition) {
        // 使用默认重置策略请求重置
        requestOffsetReset(partition, defaultResetStrategy);
    }

    /**
     * 如果分区已分配，则请求重置其偏移量
     * 
     * 应用场景：
     * 1. 安全的重置操作
     * 2. 重平衡过程中的位置重置
     * 3. 条件性重置场景
     *
     * @param partition 要重置的分区
     */
    public synchronized void requestOffsetResetIfPartitionAssigned(TopicPartition partition) {
        // 获取分区状态，可能返回null
        final TopicPartitionState state = assignedStateOrNull(partition);
        if (state != null) {
            // 分区已分配，使用默认策略重置
            state.reset(defaultResetStrategy);
        }
    }

    /**
     * 设置分区下次允许重试的时间
     * 
     * 应用场景：
     * 1. 实现重试退避
     * 2. 限制重试频率
     * 3. 错误恢复策略
     *
     * @param partitions 要设置的分区集合
     * @param nextAllowResetTimeMs 下次允许重试的时间戳
     */
    synchronized void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        // 遍历所有分区并设置下次允许重试时间
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    /**
     * 检查是否有默认的偏移量重置策略
     * 
     * 应用场景：
     * 1. 验证重置策略配置
     * 2. 错误处理决策
     * 3. 功能可用性检查
     *
     * @return 如果设置了默认重置策略（不为NONE）返回true
     */
    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != AutoOffsetResetStrategy.NONE;
    }

    /**
     * 检查分区是否需要重置偏移量
     * 
     * 应用场景：
     * 1. 消费前的状态检查
     * 2. 错误恢复检查
     * 3. 位置重置流程控制
     *
     * @param partition 要检查的分区
     * @return 如果分区正在等待重置返回true
     */
    public synchronized boolean isOffsetResetNeeded(TopicPartition partition) {
        // 检查分区是否正在等待重置
        return assignedState(partition).awaitingReset();
    }

    /**
     * 获取指定分区的偏移量重置策略
     * 
     * 应用场景：
     * 1. 当分区需要重置消费位置时，确定使用哪种重置策略
     * 2. 在处理新分配的分区时，确定初始消费位置
     * 3. 处理无效偏移量时的恢复策略
     *
     * @param partition 要查询的主题分区
     * @return 该分区的偏移量重置策略
     */
    public synchronized AutoOffsetResetStrategy resetStrategy(TopicPartition partition) {
        // 从分区状态中获取重置策略
        return assignedState(partition).resetStrategy();
    }

    /**
     * 检查所有分配的分区是否都有有效的获取位置
     * 
     * 应用场景：
     * 1. 在开始消费之前验证所有分区是否准备就绪
     * 2. 在重平衡后检查新分配的分区状态
     * 3. 确保消费者可以开始获取消息
     *
     * @return 如果所有分区都有有效的获取位置返回true，否则返回false
     */
    public synchronized boolean hasAllFetchPositions() {
        // 由于这是获取的热路径，我们使用迭代器而不是java.util.stream API以提高性能
        Iterator<TopicPartitionState> it = assignment.stateIterator();
        while (it.hasNext()) {
            // 检查每个分区是否有有效位置
            if (!it.next().hasValidPosition()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取所有需要初始化的分区集合
     * 
     * 应用场景：
     * 1. 在消费者启动时确定需要初始化位置的分区
     * 2. 在重平衡后处理新分配的分区
     * 3. 重置特定分区的消费位置
     *
     * @return 需要初始化的分区集合
     */
    public synchronized Set<TopicPartition> initializingPartitions() {
        // 使用shouldInitialize条件过滤需要初始化的分区
        return collectPartitions(TopicPartitionState::shouldInitialize);
    }

    /**
     * 根据给定的过滤条件收集符合条件的分区
     * 
     * 应用场景：
     * 1. 筛选特定状态的分区
     * 2. 批量处理符合条件的分区
     * 3. 用于各种分区状态查询操作
     *
     * @param filter 用于过滤分区的谓词函数
     * @return 符合过滤条件的分区集合
     */
    private Set<TopicPartition> collectPartitions(Predicate<TopicPartitionState> filter) {
        Set<TopicPartition> result = new HashSet<>();
        // 遍历所有分配的分区，将符合条件的分区添加到结果集
        assignment.forEach((topicPartition, topicPartitionState) -> {
            if (filter.test(topicPartitionState)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    /**
     * 使用配置的重置策略重置需要初始化位置的分区
     * 
     * 应用场景：
     * 1. 消费者首次启动时初始化分区位置
     * 2. 处理无效偏移量的情况
     * 3. 手动重置特定分区的消费位置
     *
     * @param initPartitionsToInclude 要包含在重置中的初始化分区的过滤器
     * @throws NoOffsetForPartitionException 如果有分区需要位置但没有配置重置策略
     */
    public synchronized void resetInitializingPositions(Predicate<TopicPartition> initPartitionsToInclude) {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        // 遍历所有分配的分区
        assignment.forEach((tp, partitionState) -> {
            // 检查分区是否需要初始化且满足包含条件
            if (partitionState.shouldInitialize() && initPartitionsToInclude.test(tp)) {
                if (defaultResetStrategy == AutoOffsetResetStrategy.NONE)
                    // 如果没有配置重置策略，将分区添加到无偏移量集合
                    partitionsWithNoOffsets.add(tp);
                else
                    // 使用默认重置策略请求重置分区位置
                    requestOffsetReset(tp);
            }
        });

        // 如果有分区没有偏移量且没有重置策略，抛出异常
        if (!partitionsWithNoOffsets.isEmpty())
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
    }

    /**
     * 重置所有需要初始化的分区的位置
     * 
     * 应用场景：
     * 1. 批量重置所有需要初始化的分区
     * 2. 在消费者重启时重置位置
     * 3. 手动触发全局位置重置
     */
    public synchronized void resetInitializingPositions() {
        // 重置所有分区，不进行过滤
        resetInitializingPositions(tp -> true);
    }

    /**
     * 获取当前需要重置位置且不在重试等待状态的分区集合
     * 
     * 应用场景：
     * 1. 定期检查需要重置的分区
     * 2. 处理之前重置失败的分区
     * 3. 重试机制的实现
     *
     * @param nowMs 当前时间戳
     * @return 需要重置的分区集合
     */
    public synchronized Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        // 收集等待重置且不在重试等待状态的分区
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs));
    }

    /**
     * 获取当前需要验证位置且不在重试等待状态的分区集合
     * 
     * 应用场景：
     * 1. 定期验证分区位置的有效性
     * 2. 处理之前验证失败的分区
     * 3. 位置验证重试机制的实现
     *
     * @param nowMs 当前时间戳
     * @return 需要验证的分区集合
     */
    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        // 收集等待验证且不在重试等待状态的分区
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs));
    }

    /**
     * 检查指定的分区是否已被分配给该消费者
     * 
     * 应用场景：
     * 1. 验证分区操作的合法性
     * 2. 检查分区的所有权
     * 3. 在执行分区操作前进行权限检查
     *
     * @param tp 要检查的主题分区
     * @return 如果分区已被分配返回true
     */
    public synchronized boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    /**
     * 检查指定的分区是否处于暂停状态
     * 
     * 应用场景：
     * 1. 控制分区的消费流量
     * 2. 实现背压机制
     * 3. 临时停止特定分区的消费
     *
     * @param tp 要检查的主题分区
     * @return 如果分区已暂停返回true
     */
    public synchronized boolean isPaused(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isPaused();
    }

    /**
     * 检查指定的分区是否可以获取消息
     * 
     * 应用场景：
     * 1. 在获取消息前检查分区状态
     * 2. 控制消息获取的流程
     * 3. 实现消费者的流量控制
     *
     * @param tp 要检查的主题分区
     * @return 如果分区可以获取消息返回true
     */
    synchronized boolean isFetchable(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isFetchable();
    }

    /**
     * 检查指定的分区是否有有效的消费位置
     * 
     * 应用场景：
     * 1. 验证分区是否准备好进行消费
     * 2. 检查位置初始化状态
     * 3. 在开始消费前进行状态检查
     *
     * @param tp 要检查的主题分区
     * @return 如果分区有有效的消费位置返回true
     */
    public synchronized boolean hasValidPosition(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.hasValidPosition();
    }

    /**
     * 暂停指定分区的消息获取
     * 
     * 应用场景：
     * 1. 实现消费者的背压机制
     * 2. 临时停止处理特定分区的消息
     * 3. 控制消费速率
     *
     * @param tp 要暂停的主题分区
     */
    public synchronized void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    /**
     * 标记指定分区集合为待撤销状态
     * 
     * 应用场景：
     * 1. 在重平衡开始时标记将要被撤销的分区
     * 2. 准备分区所有权转移
     * 3. 实现平滑的分区迁移
     *
     * @param tps 要标记的分区集合
     */
    public synchronized void markPendingRevocation(Set<TopicPartition> tps) {
        tps.forEach(tp -> assignedState(tp).markPendingRevocation());
    }

    /**
     * 标记分区是否正在等待onPartitionsAssigned回调
     * 该方法用于测试目的
     * 
     * 应用场景：
     * 1. 在测试中模拟分区分配过程
     * 2. 验证异步分配的行为
     * 3. 调试分区分配的状态转换
     *
     * @param tps 要标记的分区集合
     * @param pendingOnAssignedCallback 是否等待回调
     */
    synchronized void markPendingOnAssignedCallback(Collection<TopicPartition> tps,
                                                    boolean pendingOnAssignedCallback) {
        tps.forEach(tp -> assignedState(tp).markPendingOnAssignedCallback(pendingOnAssignedCallback));
    }

    /**
     * 更新分区分配并标记新分配的分区等待onPartitionsAssigned回调
     * 
     * 应用场景：
     * 1. 在异步消费者中处理新的分区分配
     * 2. 确保分区在回调完成前不可获取
     * 3. 实现平滑的分区转换
     *
     * @param fullAssignment 完整的分区分配集合，包括已有和新增的分区
     * @param addedPartitions 新增的分区子集，这些分区在回调完成前不可获取
     */
    public synchronized void assignFromSubscribedAwaitingCallback(Collection<TopicPartition> fullAssignment,
                                                                  Collection<TopicPartition> addedPartitions) {
        // 更新分区分配
        assignFromSubscribed(fullAssignment);
        // 标记新分配的分区等待回调
        markPendingOnAssignedCallback(addedPartitions, true);
    }

    /**
     * 启用之前等待onPartitionsAssigned回调完成的分区
     * 
     * 应用场景：
     * 1. 在异步消费者中完成分区分配的回调处理
     * 2. 使分区可以开始获取消息
     * 3. 完成分区的状态转换
     *
     * @param partitions 要启用的分区集合
     */
    public synchronized void enablePartitionsAwaitingCallback(Collection<TopicPartition> partitions) {
        // 标记分区不再等待回调
        markPendingOnAssignedCallback(partitions, false);
    }

    /**
     * 恢复指定分区的消息获取
     * 
     * 应用场景：
     * 1. 重新开始处理之前暂停的分区
     * 2. 在背压解除后恢复消费
     * 3. 动态调整消费行为
     *
     * @param tp 要恢复的主题分区
     */
    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    /**
     * 处理请求失败的分区，设置下次重试时间
     * 
     * 应用场景：
     * 1. 处理网络请求失败
     * 2. 实现重试机制
     * 3. 错误恢复处理
     *
     * @param partitions 失败的分区集合
     * @param nextRetryTimeMs 下次重试的时间戳
     */
    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions) {
            // 请求失败时，分配可能已经改变，此时我们会忽略该分区
            final TopicPartitionState state = assignedStateOrNull(partition);
            if (state != null)
                state.requestFailed(nextRetryTimeMs);
        }
    }

    /**
     * 将指定的分区移动到分配队列的末尾
     * 
     * 应用场景：
     * 1. 调整分区处理的优先级
     * 2. 实现轮询策略
     * 3. 负载均衡优化
     *
     * @param tp 要移动的主题分区
     */
    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    /**
     * 获取再平衡监听器
     * 
     * 应用场景：
     * 1. 在重平衡过程中通知用户
     * 2. 允许用户处理分区分配变化
     * 3. 实现自定义的重平衡行为
     *
     * @return 再平衡监听器的Optional包装
     */
    public synchronized Optional<ConsumerRebalanceListener> rebalanceListener() {
        return rebalanceListener;
    }

    /**
     * TopicPartitionState类用于维护每个主题分区的状态信息
     * 包含了分区的获取状态、位置信息、水位线、暂停状态等关键信息
     * 
     * 应用场景：
     * 1. 跟踪分区的消费进度和状态
     * 2. 管理分区的暂停和恢复操作
     * 3. 处理分区位置的验证和重置
     * 4. 管理首选副本的读取
     */
    private static class TopicPartitionState {

        // 分区的获取状态(INITIALIZING、FETCHING、AWAIT_RESET、AWAIT_VALIDATION)
        private FetchState fetchState;
        // 最后消费的位置信息，包含偏移量和leader epoch
        private FetchPosition position;

        // 上次获取操作返回的高水位线，表示消费者可以读取到的最大偏移量
        private Long highWatermark;
        // 日志的起始偏移量，表示最早可用的消息偏移量
        private Long logStartOffset;
        // 最后稳定的偏移量，用于事务性消息
        private Long lastStableOffset;
        // 分区是否被用户暂停消费
        private boolean paused;
        // 分区是否正在等待撤销（在再平衡过程中）
        private boolean pendingRevocation;
        // 分区是否正在等待onAssigned回调执行
        private boolean pendingOnAssignedCallback;
        // 当需要重置偏移量时使用的策略（earliest或latest）
        private AutoOffsetResetStrategy resetStrategy;
        // 下次重试的时间戳
        private Long nextRetryTimeMs;
        // 首选的读取副本ID
        private Integer preferredReadReplica;
        // 首选读取副本的过期时间
        private Long preferredReadReplicaExpireTimeMs;
        // 是否已请求获取分区结束偏移量
        private boolean endOffsetRequested;
        
        /**
         * 构造函数，初始化分区状态
         * 将所有状态设置为初始值，包括：
         * - 未暂停
         * - 未等待撤销
         * - 未等待onAssigned回调
         * - 未请求结束偏移量
         * - 获取状态为INITIALIZING
         * - 其他字段设为null
         */
        TopicPartitionState() {
            this.paused = false;
            this.pendingRevocation = false;
            this.pendingOnAssignedCallback = false;
            this.endOffsetRequested = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        /**
         * 检查是否已请求获取分区的结束偏移量
         * 
         * 应用场景：
         * - 在消费者需要知道分区最新消息位置时使用
         * - 用于判断是否需要重新请求结束偏移量
         * 
         * @return 如果已请求结束偏移量返回true，否则返回false
         */
        public boolean endOffsetRequested() {
            return endOffsetRequested;
        }

        /**
         * 标记已请求获取分区的结束偏移量
         * 
         * 应用场景：
         * - 当消费者请求获取分区最新消息位置时调用
         * - 在需要更新分区结束位置时使用
         */
        public void requestEndOffset() {
            endOffsetRequested = true;
        }

        /**
         * 转换分区的获取状态
         * 
         * 实现细节：
         * 1. 尝试将当前状态转换为新状态
         * 2. 如果转换成功：
         *   - 更新状态
         *   - 执行转换后的回调操作
         *   - 检查位置信息的有效性
         * 3. 如果新状态不需要位置信息，则清除位置
         * 
         * 应用场景：
         * - 在分区状态需要改变时使用（如从INITIALIZING到FETCHING）
         * - 在重置或验证位置时使用
         * 
         * @param newState 目标状态
         * @param runIfTransitioned 状态转换成功后要执行的操作
         */
        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            FetchState nextState = this.fetchState.transitionTo(newState);
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                runIfTransitioned.run();
                if (this.position == null && nextState.requiresPosition()) {
                    throw new IllegalStateException("Transitioned subscription state to " + nextState + ", but position is null");
                } else if (!nextState.requiresPosition()) {
                    this.position = null;
                }
            }
        }

        /**
         * 获取首选的读取副本ID
         * 
         * 实现细节：
         * 1. 检查首选副本是否过期
         * 2. 如果过期，清除首选副本并返回空
         * 3. 如果未过期，返回当前的首选副本
         * 
         * 应用场景：
         * - 在进行数据读取时，确定应该从哪个副本读取数据
         * - 支持就近读取，提高读取性能
         * - 实现读负载均衡
         * 
         * @param timeMs 当前时间戳
         * @return 首选副本ID，如果没有或已过期则返回空
         */
        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        /**
         * 更新首选的读取副本
         * 
         * 实现细节：
         * 1. 检查是否需要更新（当前无首选副本或首选副本发生变化）
         * 2. 如果需要更新，设置新的首选副本和过期时间
         * 
         * 应用场景：
         * - 发现更优的副本节点时更新首选副本
         * - 当前首选副本不可用需要切换时
         * - 实现动态的读负载均衡
         * 
         * @param preferredReadReplica 新的首选副本ID
         * @param timeMs 用于获取过期时间的时间戳提供者
         */
        private void updatePreferredReadReplica(int preferredReadReplica, LongSupplier timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.getAsLong();
            }
        }

        /**
         * 清除首选的读取副本
         * 
         * 实现细节：
         * 1. 如果存在首选副本，保存其ID
         * 2. 清除首选副本和过期时间
         * 3. 返回被清除的副本ID
         * 
         * 应用场景：
         * - 首选副本不可用时
         * - 需要重新选择首选副本时
         * - 分区重新分配时重置首选副本
         * 
         * @return 被清除的副本ID，如果没有则返回空
         */
        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        /**
         * 重置分区的消费位置
         * 
         * 实现细节：
         * 1. 将分区状态转换为AWAIT_RESET
         * 2. 设置新的重置策略
         * 3. 清除重试时间
         * 
         * 应用场景：
         * - 当没有有效的消费位置时（如新分区）
         * - 当消费位置无效需要重置时
         * - 当用户手动请求重置位置时
         * 
         * @param strategy 重置策略（earliest或latest）
         */
        private void reset(AutoOffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * 检查位置是否存在且需要验证，如果需要则进入AWAIT_VALIDATION状态
         * 同时会使用当前leader和epoch更新位置信息
         * 
         * 实现细节：
         * 1. 如果当前状态是AWAIT_RESET，不需要验证
         * 2. 如果没有leader信息，不能验证
         * 3. 如果位置存在且leader发生变化，创建新位置并验证
         * 4. 清除首选读取副本
         * 
         * 应用场景：
         * - leader发生变化时验证位置有效性
         * - 确保消费位置与当前leader匹配
         * - 防止消费到无效数据
         *
         * @param currentLeaderAndEpoch 用于比较的leader和epoch信息
         * @return 如果位置正在等待验证返回true
         */
        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (currentLeaderAndEpoch.leader.isEmpty()) {
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * 对于旧版本的API，无法执行位置验证，直接转换到FETCHING状态
         * 
         * 实现细节：
         * 1. 检查位置是否存在
         * 2. 使用新的leader信息创建新位置
         * 3. 直接转换到FETCHING状态
         * 
         * 应用场景：
         * - 与旧版本broker兼容
         * - 不支持leader epoch验证时使用
         * 
         * @param currentLeaderAndEpoch 当前的leader和epoch信息
         */
        private void updatePositionLeaderNoValidation(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (position != null) {
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * 验证消费位置
         * 
         * 实现细节：
         * 1. 检查是否有完整的epoch信息
         * 2. 如果有，进入AWAIT_VALIDATION状态等待验证
         * 3. 如果没有，直接进入FETCHING状态
         * 
         * 应用场景：
         * - 确保消费位置的有效性
         * - 处理leader变更场景
         * - 防止消费到截断的数据
         * 
         * @param position 要验证的位置信息
         */
        private void validatePosition(FetchPosition position) {
            if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {
                transitionState(FetchStates.AWAIT_VALIDATION, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            } else {
                // 如果没有epoch信息，则跳过验证
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * 完成位置验证并进入获取状态
         * 
         * 实现细节：
         * 1. 检查是否有位置信息
         * 2. 如果有，转换到FETCHING状态
         * 3. 清除重试时间
         * 
         * 应用场景：
         * - 位置验证成功后继续消费
         * - 重新开始数据获取
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> this.nextRetryTimeMs = null);
            }
        }

        /**
         * 检查是否正在等待位置验证
         * 
         * 应用场景：
         * - 判断是否可以开始获取数据
         * - 等待验证完成
         * 
         * @return 如果正在等待验证返回true
         */
        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * 检查是否正在等待重试延迟
         * 
         * 应用场景：
         * - 控制重试频率
         * - 实现退避策略
         * 
         * @param nowMs 当前时间戳
         * @return 如果还在重试等待期返回true
         */
        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        /**
         * 检查是否正在等待位置重置
         * 
         * 应用场景：
         * - 判断是否需要执行位置重置
         * - 等待重置完成
         * 
         * @return 如果正在等待重置返回true
         */
        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        /**
         * 设置下次允许重试的时间
         * 
         * 应用场景：
         * - 实现重试间隔控制
         * - 防止频繁重试
         * 
         * @param nextAllowedRetryTimeMs 下次允许重试的时间戳
         */
        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        /**
         * 设置请求失败的重试时间
         * 
         * 应用场景：
         * 1. 处理获取数据请求失败的情况
         * 2. 实现退避重试机制
         * 3. 避免频繁重试导致资源浪费
         */
        private void requestFailed(long nextAllowedRetryTimeMs) {
            // 设置下次允许重试的时间戳
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        /**
         * 检查是否有有效的消费位置
         * 
         * 应用场景：
         * 1. 在开始获取数据前验证位置
         * 2. 判断是否需要重置位置
         * 3. 确保消费位置的有效性
         */
        private boolean hasValidPosition() {
            // 通过当前获取状态判断位置是否有效
            return fetchState.hasValidPosition();
        }

        private boolean hasPosition() {
            return position != null;
        }

        private boolean isPaused() {
            return paused;
        }

        /**
         * 设置已验证的消费位置
         * 
         * 应用场景：
         * 1. 设置初始消费位置
         * 2. 手动调整消费位置
         * 3. 重置消费位置后设置新位置
         */
        private void seekValidated(FetchPosition position) {
            // 转换到FETCHING状态并更新位置信息
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * 设置未验证的消费位置
         * 
         * 应用场景：
         * 1. 手动seek操作时设置新位置
         * 2. 需要验证新位置的有效性
         * 3. 处理位置变更请求
         */
        private void seekUnvalidated(FetchPosition fetchPosition) {
            // 先设置位置，然后触发验证
            seekValidated(fetchPosition);
            validatePosition(fetchPosition);
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private FetchPosition validPosition() {
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        private void pause() {
            this.paused = true;
        }

        private void markPendingRevocation() {
            this.pendingRevocation = true;
        }

        private void markPendingOnAssignedCallback(boolean pendingOnAssignedCallback) {
            this.pendingOnAssignedCallback = pendingOnAssignedCallback;
        }

        private void resume() {
            this.paused = false;
        }

        /**
         * True if the partition is in {@link FetchStates#INITIALIZING} state. While in this
         * state, a position for the partition can be retrieved (based on committed offsets or
         * partitions offsets).
         * Note that retrieving a position does not mean that we can start fetching from the
         * partition (see {@link #isFetchable()})
         */
        private boolean shouldInitialize() {
            return fetchState.equals(FetchStates.INITIALIZING);
        }

        private boolean isFetchable() {
            return !paused && !pendingRevocation && !pendingOnAssignedCallback && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
            this.endOffsetRequested = false;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
            this.endOffsetRequested = false;
        }

        private AutoOffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    /**
     * The fetch state of a partition. This class is used to determine valid state transitions and expose the some of
     * the behavior of the current fetch state. Actual state variables are stored in the {@link TopicPartitionState}.
     */
    interface FetchState {
        default FetchState transitionTo(FetchState newState) {
            if (validTransitions().contains(newState)) {
                return newState;
            } else {
                return this;
            }
        }

        /**
         * Return the valid states which this state can transition to
         */
        Collection<FetchState> validTransitions();

        /**
         * Test if this state requires a position to be set
         */
        boolean requiresPosition();

        /**
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        boolean hasValidPosition();
    }

    /**
     * An enumeration of all the possible fetch states. The state transitions are encoded in the values returned by
     * {@link FetchState#validTransitions}.
     */
    enum FetchStates implements FetchState {
        INITIALIZING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        FETCHING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return true;
            }
        },

        AWAIT_RESET() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        AWAIT_VALIDATION() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        }
    }

    /**
     * Represents the position of a partition subscription.
     *
     * This includes the offset and epoch from the last record in
     * the batch from a FetchResponse. It also includes the leader epoch at the time the batch was consumed.
     */
    public static class FetchPosition {
        public final long offset;
        final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }

    public static class LogTruncation {
        public final TopicPartition topicPartition;
        public final FetchPosition fetchPosition;
        public final Optional<OffsetAndMetadata> divergentOffsetOpt;

        public LogTruncation(TopicPartition topicPartition,
                             FetchPosition fetchPosition,
                             Optional<OffsetAndMetadata> divergentOffsetOpt) {
            this.topicPartition = topicPartition;
            this.fetchPosition = fetchPosition;
            this.divergentOffsetOpt = divergentOffsetOpt;
        }

        @Override
        public String toString() {
            StringBuilder bldr = new StringBuilder()
                .append("(partition=")
                .append(topicPartition)
                .append(", fetchOffset=")
                .append(fetchPosition.offset)
                .append(", fetchEpoch=")
                .append(fetchPosition.offsetEpoch);

            if (divergentOffsetOpt.isPresent()) {
                OffsetAndMetadata divergentOffset = divergentOffsetOpt.get();
                bldr.append(", divergentOffset=")
                    .append(divergentOffset.offset())
                    .append(", divergentEpoch=")
                    .append(divergentOffset.leaderEpoch());
            } else {
                bldr.append(", divergentOffset=unknown")
                    .append(", divergentEpoch=unknown");
            }

            return bldr.append(")").toString();

        }
    }
}
