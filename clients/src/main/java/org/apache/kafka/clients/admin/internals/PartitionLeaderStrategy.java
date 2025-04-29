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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 针对分区leader节点的API的基础驱动实现。
 * 该类主要用于管理客户端查找和管理Kafka主题分区的leader副本。
 */
public class PartitionLeaderStrategy implements AdminApiLookupStrategy<TopicPartition> {
    /**
     * 单一请求作用域常量
     * 由于元数据请求可以任意分组主题分区，所以所有请求可以共享同一个请求上下文
     */
    private static final ApiRequestScope SINGLE_REQUEST_SCOPE = new ApiRequestScope() {
    };

    /**
     * 日志记录器实例
     */
    private final Logger log;

    /**
     * 是否容忍未知主题
     * 当设置为true时，遇到未知主题不会立即失败，而是继续处理其他主题
     */
    private final boolean tolerateUnknownTopics;

    /**
     * 创建一个新的PartitionLeaderStrategy实例
     * 默认容忍未知主题(tolerateUnknownTopics=true)
     *
     * @param logContext 日志上下文对象
     */
    public PartitionLeaderStrategy(LogContext logContext) {
        this(logContext, true);
    }

    /**
     * 创建一个新的PartitionLeaderStrategy实例
     *
     * @param logContext 日志上下文对象
     * @param tolerateUnknownTopics 是否容忍未知主题
     */
    public PartitionLeaderStrategy(LogContext logContext, boolean tolerateUnknownTopics) {
        // 初始化日志记录器，使用当前类作为日志类别
        this.log = logContext.logger(PartitionLeaderStrategy.class);
        // 设置是否容忍未知主题的标志
        this.tolerateUnknownTopics = tolerateUnknownTopics;
    }

    /**
     * 获取给定主题分区的查找作用域
     * 由于元数据请求可以任意分组主题分区，所以返回单一共享的请求作用域
     *
     * @param key 主题分区对象
     * @return 请求作用域
     */
    @Override
    public ApiRequestScope lookupScope(TopicPartition key) {
        // 元数据请求可以任意分组主题分区，所以它们可以共享相同的请求上下文
        return SINGLE_REQUEST_SCOPE;
    }

    /**
     * 构建元数据请求
     * 为指定的主题分区集合创建一个元数据请求构建器
     *
     * @param partitions 需要查询元数据的主题分区集合
     * @return 元数据请求构建器
     */
    @Override
    public MetadataRequest.Builder buildRequest(Set<TopicPartition> partitions) {
        // 创建元数据请求数据对象
        MetadataRequestData request = new MetadataRequestData();
        // 禁用自动创建主题功能
        request.setAllowAutoTopicCreation(false);
        // 从分区集合中提取不重复的主题名称，并为每个主题创建请求
        partitions.stream().map(TopicPartition::topic).distinct().forEach(topic ->
            request.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(topic))
        );
        // 返回元数据请求构建器
        return new MetadataRequest.Builder(request);
    }

    /**
     * 处理主题级别的错误
     * 根据不同的错误类型采取相应的处理策略
     *
     * @param topic 发生错误的主题名称
     * @param topicError 主题级别的错误类型
     * @param requestPartitions 请求的分区集合
     * @param failed 用于存储失败信息的映射
     */
    @SuppressWarnings("fallthrough")
    private void handleTopicError(
        String topic,
        Errors topicError,
        Set<TopicPartition> requestPartitions,
        Map<TopicPartition, Throwable> failed
    ) {
        switch (topicError) {
            case UNKNOWN_TOPIC_OR_PARTITION:
                // 如果不允许未知主题，则将所有相关分区标记为失败
                if (!tolerateUnknownTopics) {
                    log.error("Received unknown topic error for topic {}", topic, topicError.exception());
                    failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> topicError.exception(
                            "Failed to fetch metadata for partition " + tp + " because metadata for topic `" + topic + "` could not be found"));
                    break;
                }
                // 如果允许未知主题，则故意fall through到下一个case
            case LEADER_NOT_AVAILABLE:
            case BROKER_NOT_AVAILABLE:
                // 对于临时性错误，记录日志并准备重试
                log.debug("Metadata request for topic {} returned topic-level error {}. Will retry",
                    topic, topicError);
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                // 主题授权失败，记录错误并标记所有相关分区为授权失败
                log.error("Received authorization failure for topic {} in `Metadata` response", topic,
                    topicError.exception());
                failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> new TopicAuthorizationException(
                    "Failed to fetch metadata for partition " + tp + " due to topic authorization failure",
                    Collections.singleton(topic)));
                break;

            case INVALID_TOPIC_EXCEPTION:
                // 主题名称无效，记录错误并标记所有相关分区为无效
                log.error("Received invalid topic error for topic {} in `Metadata` response", topic,
                    topicError.exception());
                failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> new InvalidTopicException(
                    "Failed to fetch metadata for partition " + tp + " due to invalid topic `" + topic + "`",
                    Collections.singleton(topic)));
                break;

            default:
                // 处理未预期的错误，记录错误并标记所有相关分区为失败
                log.error("Received unexpected error for topic {} in `Metadata` response", topic,
                    topicError.exception());
                failAllPartitionsForTopic(topic, requestPartitions, failed, tp -> topicError.exception(
                    "Failed to fetch metadata for partition " + tp + " due to unexpected error for topic `" + topic + "`"));
        }
    }

    /**
     * 将指定主题的所有分区标记为失败
     * 
     * @param topic 主题名称
     * @param partitions 所有分区的集合
     * @param failed 用于存储失败信息的映射
     * @param exceptionGenerator 异常生成器函数，用于为每个分区生成对应的异常
     */
    private void failAllPartitionsForTopic(
        String topic,
        Set<TopicPartition> partitions,
        Map<TopicPartition, Throwable> failed,
        Function<TopicPartition, Throwable> exceptionGenerator
    ) {
        // 过滤出属于指定主题的分区，并为每个分区生成异常信息
        partitions.stream().filter(tp -> tp.topic().equals(topic)).forEach(tp ->
            failed.put(tp, exceptionGenerator.apply(tp))
        );
    }

    /**
     * 处理分区级别的错误
     * 
     * @param topicPartition 发生错误的主题分区
     * @param partitionError 分区级别的错误类型
     * @param failed 用于存储失败信息的映射
     */
    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors partitionError,
        Map<TopicPartition, Throwable> failed
    ) {
        switch (partitionError) {
            // 对于以下临时性错误，记录日志并准备重试
            case NOT_LEADER_OR_FOLLOWER:  // 节点不是leader或follower
            case REPLICA_NOT_AVAILABLE:    // 副本不可用
            case LEADER_NOT_AVAILABLE:     // leader不可用
            case BROKER_NOT_AVAILABLE:     // broker不可用
            case KAFKA_STORAGE_ERROR:      // Kafka存储错误
            case UNKNOWN_TOPIC_OR_PARTITION: // 未知的主题或分区
                log.debug("Metadata request for partition {} returned partition-level error {}. Will retry",
                    topicPartition, partitionError);
                break;

            default:
                // 处理未预期的错误，记录错误并标记分区为失败
                log.error("Received unexpected error for partition {} in `Metadata` response",
                    topicPartition, partitionError.exception());
                failed.put(topicPartition, partitionError.exception(
                    "Unexpected error during metadata lookup for " + topicPartition));
        }
    }

    /**
     * 处理元数据响应
     * 解析响应中的主题和分区信息，处理各种错误情况，并返回查找结果
     *
     * @param requestPartitions 请求的分区集合
     * @param abstractResponse 服务器返回的响应
     * @return 包含失败和成功映射的查找结果
     */
    @Override
    public LookupResult<TopicPartition> handleResponse(
        Set<TopicPartition> requestPartitions,
        AbstractResponse abstractResponse
    ) {
        // 将抽象响应转换为元数据响应
        MetadataResponse response = (MetadataResponse) abstractResponse;
        // 用于存储失败的分区及其异常
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        // 用于存储成功的分区及其leader broker ID
        Map<TopicPartition, Integer> mapped = new HashMap<>();

        // 遍历响应中的所有主题
        for (MetadataResponseData.MetadataResponseTopic topicMetadata : response.data().topics()) {
            String topic = topicMetadata.name();
            // 获取主题级别的错误码
            Errors topicError = Errors.forCode(topicMetadata.errorCode());
            if (topicError != Errors.NONE) {
                // 如果存在主题级别的错误，交给错误处理器处理
                handleTopicError(topic, topicError, requestPartitions, failed);
                continue;
            }

            // 遍历主题下的所有分区
            for (MetadataResponseData.MetadataResponsePartition partitionMetadata : topicMetadata.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic, partitionMetadata.partitionIndex());
                // 获取分区级别的错误码
                Errors partitionError = Errors.forCode(partitionMetadata.errorCode());

                // 过滤掉不在请求列表中的分区
                if (!requestPartitions.contains(topicPartition)) {
                    // 元数据响应总是返回请求主题的所有分区，所以需要过滤掉不感兴趣的分区
                    continue;
                }

                // 处理分区级别的错误
                if (partitionError != Errors.NONE) {
                    handlePartitionError(topicPartition, partitionError, failed);
                    continue;
                }

                // 获取leader broker ID并进行处理
                int leaderId = partitionMetadata.leaderId();
                if (leaderId >= 0) {
                    // 如果leader ID有效，添加到映射中
                    mapped.put(topicPartition, leaderId);
                } else {
                    // 如果leader未知，记录日志准备重试
                    log.debug("Metadata request for {} returned no error, but the leader is unknown. Will retry",
                        topicPartition);
                }
            }
        }
        return new LookupResult<>(failed, mapped);
    }

    /**
     * PartitionLeaderFuture类用于管理分区leader的查找结果
     * 它维护了一个预先获取的分区到broker ID的映射，可以用来优化请求
     * 这个映射会在处理请求过程中保持更新
     * 这对于需要重复使用PartitionLeaderStrategy的场景特别有用
     * 比如连续多次调用Admin#listOffsets方法
     *
     * @param <V> Future完成时返回的值类型
     */
    public static class PartitionLeaderFuture<V> implements AdminApiFuture<TopicPartition, V> {
        // 请求的所有分区键
        private final Set<TopicPartition> requestKeys;
        // 分区leader的缓存映射
        private final Map<TopicPartition, Integer> partitionLeaderCache;
        // 每个分区对应的Future对象
        private final Map<TopicPartition, KafkaFuture<V>> futures;

        /**
         * 创建一个新的PartitionLeaderFuture实例
         *
         * @param requestKeys 请求的分区集合
         * @param partitionLeaderCache 分区leader的缓存映射
         */
        public PartitionLeaderFuture(Set<TopicPartition> requestKeys, Map<TopicPartition, Integer> partitionLeaderCache) {
            this.requestKeys = requestKeys;
            this.partitionLeaderCache = partitionLeaderCache;
            // 为每个请求的分区创建一个Future
            this.futures = requestKeys.stream().collect(Collectors.toUnmodifiableMap(
                Function.identity(),
                k -> new KafkaFutureImpl<>()
            ));
        }

        /**
         * 获取所有需要查找的分区键
         */
        @Override
        public Set<TopicPartition> lookupKeys() {
            return futures.keySet();
        }

        /**
         * 获取缓存中不存在的分区键
         * 这些分区需要重新查找leader信息
         */
        @Override
        public Set<TopicPartition> uncachedLookupKeys() {
            Set<TopicPartition> keys = new HashSet<>();
            requestKeys.forEach(tp -> {
                if (!partitionLeaderCache.containsKey(tp)) {
                    keys.add(tp);
                }
            });
            return keys;
        }

        /**
         * 获取缓存中已有的分区到broker ID的映射
         */
        @Override
        public Map<TopicPartition, Integer> cachedKeyBrokerIdMapping() {
            Map<TopicPartition, Integer> mapping = new HashMap<>();
            requestKeys.forEach(tp -> {
                Integer brokerId = partitionLeaderCache.get(tp);
                if (brokerId != null) {
                    mapping.put(tp, brokerId);
                }
            });
            return mapping;
        }

        /**
         * 获取所有分区的Future对象
         */
        public Map<TopicPartition, KafkaFuture<V>> all() {
            return futures;
        }

        /**
         * 批量完成Future
         */
        @Override
        public void complete(Map<TopicPartition, V> values) {
            values.forEach(this::complete);
        }

        /**
         * 完成单个分区的Future
         */
        private void complete(TopicPartition key, V value) {
            futureOrThrow(key).complete(value);
        }

        /**
         * 更新leader缓存映射
         */
        @Override
        public void completeLookup(Map<TopicPartition, Integer> brokerIdMapping) {
            partitionLeaderCache.putAll(brokerIdMapping);
        }

        /**
         * 批量处理异常完成
         */
        @Override
        public void completeExceptionally(Map<TopicPartition, Throwable> errors) {
            errors.forEach(this::completeExceptionally);
        }

        /**
         * 处理单个分区的异常完成
         * 移除缓存并标记Future为异常完成
         */
        private void completeExceptionally(TopicPartition key, Throwable t) {
            partitionLeaderCache.remove(key);
            futureOrThrow(key).completeExceptionally(t);
        }

        /**
         * 获取指定分区的Future，如果不存在则抛出异常
         */
        private KafkaFutureImpl<V> futureOrThrow(TopicPartition key) {
            // 类型转换是安全的，因为我们初始化时只使用KafkaFutureImpl
            KafkaFutureImpl<V> future = (KafkaFutureImpl<V>) futures.get(key);
            if (future == null) {
                throw new IllegalArgumentException("Attempt to complete future for " + key +
                    ", which was not requested");
            } else {
                return future;
            }
        }
    }
}
