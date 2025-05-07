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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.utils.LogContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 消费者元数据类
 * 扩展了基础的Metadata类，提供了特定于消费者的元数据管理功能
 */
public class ConsumerMetadata extends Metadata {
    /**
     * 是否包含内部主题
     * 控制是否在元数据中包含Kafka的内部主题
     */
    private final boolean includeInternalTopics;
    
    /**
     * 是否允许自动创建主题
     * 控制在请求不存在的主题时是否自动创建
     */
    private final boolean allowAutoTopicCreation;
    
    /**
     * 订阅状态
     * 维护消费者的主题订阅信息
     */
    private final SubscriptionState subscription;
    
    /**
     * 临时主题集合
     * 存储临时需要获取元数据的主题
     */
    private final Set<String> transientTopics;

    /**
     * 构造函数
     * 使用详细参数初始化消费者元数据
     *
     * @param refreshBackoffMs 刷新回退时间（毫秒）
     * @param refreshBackoffMaxMs 最大刷新回退时间（毫秒）
     * @param metadataExpireMs 元数据过期时间（毫秒）
     * @param includeInternalTopics 是否包含内部主题
     * @param allowAutoTopicCreation 是否允许自动创建主题
     * @param subscription 订阅状态
     * @param logContext 日志上下文
     * @param clusterResourceListeners 集群资源监听器
     */
    public ConsumerMetadata(long refreshBackoffMs,
                            long refreshBackoffMaxMs,
                            long metadataExpireMs,
                            boolean includeInternalTopics,
                            boolean allowAutoTopicCreation,
                            SubscriptionState subscription,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners) {
        // 调用父类构造函数，初始化基本元数据属性
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        // 设置是否包含内部主题
        this.includeInternalTopics = includeInternalTopics;
        // 设置是否允许自动创建主题
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        // 设置订阅状态
        this.subscription = subscription;
        // 初始化临时主题集合
        this.transientTopics = new HashSet<>();
    }

    /**
     * 使用消费者配置初始化的构造函数
     *
     * @param config 消费者配置
     * @param subscriptions 订阅状态
     * @param logContext 日志上下文
     * @param clusterResourceListeners 集群资源监听器
     */
    public ConsumerMetadata(ConsumerConfig config,
                            SubscriptionState subscriptions,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners) {
        // 从配置中获取参数并调用主构造函数
        this(config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                subscriptions,
                logContext,
                clusterResourceListeners);
    }

    /**
     * 获取是否允许自动创建主题的标志
     *
     * @return 如果允许自动创建主题返回true，否则返回false
     */
    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    /**
     * 创建新的元数据请求构建器
     * 根据订阅状态构建适当的元数据请求
     *
     * @return 元数据请求构建器
     */
    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        // 如果存在模式订阅，返回所有主题的请求构建器
        if (subscription.hasPatternSubscription() || subscription.hasRe2JPatternSubscription())
            return MetadataRequest.Builder.allTopics();
        // 创建主题列表
        List<String> topics = new ArrayList<>();
        // 添加订阅的主题
        topics.addAll(subscription.metadataTopics());
        // 添加临时主题
        topics.addAll(transientTopics);
        // 返回指定主题的请求构建器
        return new MetadataRequest.Builder(topics, allowAutoTopicCreation);
    }

    /**
     * 添加临时主题
     * 将主题添加到临时主题集合并在需要时请求更新
     *
     * @param topics 要添加的主题集合
     */
    synchronized void addTransientTopics(Set<String> topics) {
        // 添加主题到临时集合
        this.transientTopics.addAll(topics);
        // 如果当前元数据不包含所有主题，请求更新
        if (!fetch().topics().containsAll(topics))
            requestUpdateForNewTopics();
    }

    /**
     * 清除所有临时主题
     */
    synchronized void clearTransientTopics() {
        // 清空临时主题集合
        this.transientTopics.clear();
    }

    /**
     * 确定是否保留指定的主题
     *
     * @param topic 主题名称
     * @param isInternal 是否是内部主题
     * @param nowMs 当前时间戳（毫秒）
     * @return 如果应该保留主题返回true，否则返回false
     */
    @Override
    protected synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        // 如果是临时主题或需要元数据，返回true
        if (transientTopics.contains(topic) || subscription.needsMetadata(topic))
            return true;

        // 如果是内部主题且不包含内部主题，返回false
        if (isInternal && !includeInternalTopics)
            return false;

        // 如果主题匹配订阅模式或是从Re2j分配的，返回true
        return subscription.matchesSubscribedPattern(topic) || subscription.isAssignedFromRe2j(topic);
    }
}
