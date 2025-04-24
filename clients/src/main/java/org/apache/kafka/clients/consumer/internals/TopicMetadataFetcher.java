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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link TopicMetadataFetcher} 负责获取给定主题集合的 {@link PartitionInfo} 分区信息。
 * 所有方法都会在提供的 {@link Timer timeout} 超时时间内阻塞执行。
 */
public class TopicMetadataFetcher {

    // 日志记录器，用于记录元数据获取过程中的关键信息
    private final Logger log;
    // 消费者网络客户端，用于发送元数据请求和接收响应
    private final ConsumerNetworkClient client;
    // 指数退避策略，用于在请求失败时控制重试间隔
    private final ExponentialBackoff retryBackoff;

    /**
     * 构造函数，初始化主题元数据获取器
     * @param logContext 日志上下文，用于创建日志记录器
     * @param client 消费者网络客户端，用于网络通信
     * @param retryBackoffMs 初始重试退避时间（毫秒）
     * @param retryBackoffMaxMs 最大重试退避时间（毫秒）
     */
    public TopicMetadataFetcher(LogContext logContext, ConsumerNetworkClient client, long retryBackoffMs, long retryBackoffMaxMs) {
        // 初始化日志记录器
        this.log = logContext.logger(getClass());
        // 设置网络客户端
        this.client = client;
        // 创建指数退避策略，用于控制重试间隔
        this.retryBackoff = new ExponentialBackoff(retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
    }

    /**
     * 获取集群中指定主题的 {@link PartitionInfo 分区信息}，如果主题不存在则返回 {@code null}。
     *
     * @param topic 要查询的主题名称
     * @param allowAutoTopicCreation 是否允许自动创建主题
     * @param timer 限制方法阻塞时间的计时器
     * @return 主题的 {@link PartitionInfo 分区信息} {@link List 列表}，如果主题不存在则返回 {@code null}
     */
    public List<PartitionInfo> getTopicMetadata(String topic, boolean allowAutoTopicCreation, Timer timer) {
        // 创建针对单个主题的元数据请求
        MetadataRequest.Builder request = new MetadataRequest.Builder(Collections.singletonList(topic), allowAutoTopicCreation);
        // 获取主题元数据并返回指定主题的分区信息
        Map<String, List<PartitionInfo>> topicMetadata = getTopicMetadata(request, timer);
        return topicMetadata.get(topic);
    }

    /**
     * 获取集群中所有主题的 {@link PartitionInfo 分区信息}。
     *
     * @param timer 限制方法阻塞时间的计时器
     * @return 包含所有主题及其 {@link PartitionInfo 分区信息} 的映射
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
        // 创建获取所有主题元数据的请求
        MetadataRequest.Builder request = MetadataRequest.Builder.allTopics();
        // 执行请求并返回所有主题的元数据
        return getTopicMetadata(request, timer);
    }

    /**
     * 获取Kafka集群中所有主题的元数据信息。
     *
     * @param request 要发送的元数据请求
     * @param timer 限制方法阻塞时间的计时器
     * @return 包含主题及其分区信息的映射
     */
    private Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer) {
        // 如果没有请求任何主题，则直接返回空映射，避免不必要的网络请求
        if (!request.isAllTopics() && request.emptyTopicList())
            return Collections.emptyMap();

        // 记录重试次数
        long attempts = 0L;
        do {
            // 发送元数据请求并获取Future对象
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            // 轮询等待响应
            client.poll(future, timer);

            // 如果请求失败且不可重试，则抛出异常
            if (future.failed() && !future.isRetriable())
                throw future.exception();

            // 如果请求成功
            if (future.succeeded()) {
                // 获取响应并构建集群信息
                MetadataResponse response = (MetadataResponse) future.value().responseBody();
                Cluster cluster = response.buildCluster();

                // 检查未授权的主题
                Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
                if (!unauthorizedTopics.isEmpty())
                    throw new TopicAuthorizationException(unauthorizedTopics);

                // 标记是否需要重试
                boolean shouldRetry = false;
                Map<String, Errors> errors = response.errors();
                if (!errors.isEmpty()) {
                    // 如果存在错误，需要检查是致命错误还是可重试错误
                    log.debug("Topic metadata fetch included errors: {}", errors);

                    // 遍历处理每个错误
                    for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                        String topic = errorEntry.getKey();
                        Errors error = errorEntry.getValue();

                        // 处理不同类型的错误
                        if (error == Errors.INVALID_TOPIC_EXCEPTION)
                            // 主题名称无效，抛出异常
                            throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                            // 主题不存在，继续处理下一个主题
                            continue;
                        else if (error.exception() instanceof RetriableException)
                            // 可重试的错误，标记需要重试
                            shouldRetry = true;
                        else
                            // 其他未预期的错误，抛出异常
                            throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                                    error.exception());
                    }
                }

                // 如果不需要重试，返回主题分区信息
                if (!shouldRetry) {
                    HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
                    // 收集所有主题的分区信息
                    for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            // 等待一段时间后重试
            timer.sleep(retryBackoff.backoff(attempts++));
        } while (timer.notExpired());

        // 超时异常
        throw new TimeoutException("Timeout expired while fetching topic metadata");
    }

    /**
     * 异步向Kafka集群中负载最小的节点发送元数据请求
     * @param request 元数据请求构建器
     * @return 指示元数据请求发送结果的Future对象
     */
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest.Builder request) {
        // 获取负载最小的节点
        final Node node = client.leastLoadedNode();
        // 如果没有可用的节点，返回错误Future
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            // 向选定的节点发送请求
            return client.send(node, request);
    }

}
