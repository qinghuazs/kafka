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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * <p>
 * 管理主题元数据请求的状态。当请求准备好发送时，此管理器返回一个
 * {@link NetworkClientDelegate.PollResult}。具体来说，此管理器处理以下用户API调用：
 * </p>
 * <ul>
 * <li>listTopics - 列出所有主题</li>
 * <li>partitionsFor - 获取指定主题的分区信息</li>
 * </ul>
 * <p>
 * 管理器在发送新请求之前会检查{@link TopicMetadataRequestState}的状态，
 * 以防止在未完成上一次重试退避时发送新请求。
 * 一旦请求成功完成或超时，其对应的条目将被移除。
 * </p>
 */

public class TopicMetadataRequestManager implements RequestManager {
    // 用于时间相关操作的工具类
    private final Time time;
    // 是否允许自动创建主题的配置标志
    private final boolean allowAutoTopicCreation;
    // 正在处理中的元数据请求列表
    private final List<TopicMetadataRequestState> inflightRequests;
    // 重试请求的基础退避时间（毫秒）
    private final long retryBackoffMs;
    // 重试请求的最大退避时间（毫秒）
    private final long retryBackoffMaxMs;
    // 日志记录器
    private final Logger log;
    // 日志上下文
    private final LogContext logContext;

    /**
     * 构造函数，初始化主题元数据请求管理器
     * @param context 日志上下文
     * @param time 时间工具类
     * @param config 消费者配置
     */
    public TopicMetadataRequestManager(final LogContext context, final Time time, final ConsumerConfig config) {
        // 初始化日志相关组件
        logContext = context;
        log = logContext.logger(getClass());
        this.time = time;
        // 初始化请求列表为空链表
        inflightRequests = new LinkedList<>();
        // 从配置中获取重试相关参数
        retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        // 获取是否允许自动创建主题的配置
        allowAutoTopicCreation = config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG);
    }

    /**
     * 轮询并处理元数据请求
     * @param currentTimeMs 当前时间戳（毫秒）
     * @return 包含待发送请求的轮询结果
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // 清理已超时的请求
        List<TopicMetadataRequestState> expiredRequests = inflightRequests.stream()
                .filter(TimedRequestState::isExpired)
                .collect(Collectors.toList());
        // 处理每个超时请求
        expiredRequests.forEach(TopicMetadataRequestState::expire);

        // 获取所有可以发送的请求
        List<NetworkClientDelegate.UnsentRequest> requests = inflightRequests.stream()
            .map(req -> req.send(currentTimeMs)) // 尝试发送每个请求
            .filter(Optional::isPresent) // 过滤出可以发送的请求
            .map(Optional::get) // 获取请求实例
            .collect(Collectors.toList());

        // 如果没有待发送的请求则返回空结果，否则创建新的轮询结果
        return requests.isEmpty() ? EMPTY : new NetworkClientDelegate.PollResult(0, requests);
    }

    /**
     * 请求获取所有主题的元数据信息
     * @param deadlineMs 请求截止时间（毫秒）
     * @return 包含所有主题元数据的Future对象
     */
    public CompletableFuture<Map<String, List<PartitionInfo>>> requestAllTopicsMetadata(final long deadlineMs) {
        // 创建新的请求状态对象
        TopicMetadataRequestState newRequest = new TopicMetadataRequestState(
                logContext,
                deadlineMs,
                retryBackoffMs,
                retryBackoffMaxMs);
        // 将请求添加到处理队列
        inflightRequests.add(newRequest);
        return newRequest.future;
    }

    /**
     * 请求获取指定主题的元数据信息
     * @param topic 要查询的主题名称
     * @param deadlineMs 请求截止时间（毫秒）
     * @return 包含指定主题元数据的Future对象
     */
    public CompletableFuture<Map<String, List<PartitionInfo>>> requestTopicMetadata(final String topic, final long deadlineMs) {
        // 创建新的请求状态对象
        TopicMetadataRequestState newRequest = new TopicMetadataRequestState(
                logContext,
                topic,
                deadlineMs,
                retryBackoffMs,
                retryBackoffMaxMs);
        // 将请求添加到处理队列
        inflightRequests.add(newRequest);
        return newRequest.future;
    }

    /**
     * 获取当前正在处理的请求列表（用于测试）
     * @return 正在处理的请求列表
     */
    List<TopicMetadataRequestState> inflightRequests() {
        return inflightRequests;
    }

    /**
     * 主题元数据请求状态类，继承自TimedRequestState
     * 用于跟踪和管理单个主题元数据请求的生命周期
     */
    class TopicMetadataRequestState extends TimedRequestState {
        // 请求的主题名称，如果是请求所有主题则为null
        private final String topic;
        // 标识是否是请求所有主题的元数据
        private final boolean allTopics;
        // 用于异步返回请求结果的Future对象
        CompletableFuture<Map<String, List<PartitionInfo>>> future;

        /**
         * 构造函数 - 用于请求所有主题的元数据
         * @param logContext 日志上下文
         * @param deadlineMs 请求截止时间
         * @param retryBackoffMs 重试基础退避时间
         * @param retryBackoffMaxMs 重试最大退避时间
         */
        public TopicMetadataRequestState(final LogContext logContext,
                                         final long deadlineMs,
                                         final long retryBackoffMs,
                                         final long retryBackoffMaxMs) {
            super(logContext, TopicMetadataRequestState.class.getSimpleName(), retryBackoffMs,
                    retryBackoffMaxMs, deadlineTimer(time, deadlineMs));
            future = new CompletableFuture<>();
            this.topic = null;
            this.allTopics = true;
        }

        /**
         * 构造函数 - 用于请求指定主题的元数据
         * @param logContext 日志上下文
         * @param topic 要请求的主题名称
         * @param deadlineMs 请求截止时间
         * @param retryBackoffMs 重试基础退避时间
         * @param retryBackoffMaxMs 重试最大退避时间
         */
        public TopicMetadataRequestState(final LogContext logContext,
                                         final String topic,
                                         final long deadlineMs,
                                         final long retryBackoffMs,
                                         final long retryBackoffMaxMs) {
            super(logContext, TopicMetadataRequestState.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, deadlineTimer(time, deadlineMs));
            future = new CompletableFuture<>();
            this.topic = topic;
            this.allTopics = false;
        }

        /**
         * 准备并发送元数据请求
         * @param currentTimeMs 当前时间戳
         * @return 如果可以发送请求则返回UnsentRequest，否则返回空
         */
        private Optional<NetworkClientDelegate.UnsentRequest> send(final long currentTimeMs) {
            // 检查是否可以发送请求（考虑重试退避时间）
            if (!canSendRequest(currentTimeMs)) {
                return Optional.empty();
            }
            // 记录发送尝试
            onSendAttempt(currentTimeMs);

            // 根据请求类型（所有主题或单个主题）创建对应的请求构建器
            final MetadataRequest.Builder request = allTopics
                ? MetadataRequest.Builder.allTopics()
                : new MetadataRequest.Builder(Collections.singletonList(topic), allowAutoTopicCreation);

            return Optional.of(createUnsentRequest(request));
        }

        /**
         * 处理请求超时
         */
        private void expire() {
            completeFutureAndRemoveRequest(
                    new TimeoutException("Timeout expired while fetching topic metadata"));
        }

        /**
         * 创建未发送的请求对象
         * @param request 元数据请求构建器
         * @return 未发送的请求对象
         */
        private NetworkClientDelegate.UnsentRequest createUnsentRequest(
                final MetadataRequest.Builder request) {
            // 创建未发送的请求实例
            NetworkClientDelegate.UnsentRequest unsent = new NetworkClientDelegate.UnsentRequest(
                request,
                Optional.empty());

            // 设置请求完成时的回调处理
            return unsent.whenComplete((response, exception) -> {
                if (response == null) {
                    handleError(exception, unsent.handler().completionTimeMs());
                } else {
                    handleResponse(response);
                }
            });
        }

        /**
         * 处理请求错误
         * @param exception 异常信息
         * @param completionTimeMs 完成时间戳
         */
        private void handleError(final Throwable exception,
                                 final long completionTimeMs) {
            if (exception instanceof RetriableException) {
                // 对于可重试的异常，如果未超时则重试，否则完成请求
                if (isExpired()) {
                    completeFutureAndRemoveRequest(new TimeoutException("Timeout expired while fetching topic metadata"));
                } else {
                    onFailedAttempt(completionTimeMs);
                }
            } else {
                // 对于不可重试的异常，直接完成请求
                completeFutureAndRemoveRequest(exception);
            }
        }

        /**
         * 处理服务器响应
         * @param response 服务器响应
         */
        private void handleResponse(final ClientResponse response) {
            try {
                // 解析元数据响应并完成Future
                Map<String, List<PartitionInfo>> res = handleTopicMetadataResponse((MetadataResponse) response.responseBody());
                future.complete(res);
                inflightRequests.remove(this);
            } catch (Exception e) {
                // 处理响应解析过程中的错误
                handleError(e, response.receivedTimeMs());
            }
        }

        /**
         * 完成Future并从请求队列中移除当前请求
         * @param throwable 异常信息
         */
        private void completeFutureAndRemoveRequest(final Throwable throwable) {
            future.completeExceptionally(throwable);
            inflightRequests.remove(this);
        }

        /**
         * 处理主题元数据响应
         * @param response 元数据响应
         * @return 主题分区信息映射
         */
        private Map<String, List<PartitionInfo>> handleTopicMetadataResponse(final MetadataResponse response) {
            // 构建集群元数据
            Cluster cluster = response.buildCluster();

            // 检查未授权的主题
            final Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
            if (!unauthorizedTopics.isEmpty())
                throw new TopicAuthorizationException(unauthorizedTopics);

            // 处理响应中的错误
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty()) {
                log.debug("Topic metadata fetch included errors: {}", errors);

                for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                    String topic = errorEntry.getKey();
                    Errors error = errorEntry.getValue();

                    // 处理不同类型的错误
                    if (error == Errors.INVALID_TOPIC_EXCEPTION)
                        throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                    else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                        // 对于未知主题，跳过处理
                        continue;
                    else if (error.exception() instanceof RetriableException)
                        throw error.exception();
                    else
                        throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                            error.exception());
                }
            }

            // 构建主题分区信息映射
            HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
            for (String topic : cluster.topics())
                topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
            return topicsPartitionInfos;
        }

        /**
         * 获取请求的主题名称
         * @return 主题名称
         */
        public String topic() {
            return topic;
        }
    }
}
