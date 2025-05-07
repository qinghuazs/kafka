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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * 该类管理与Kafka代理的数据获取过程。
 * <p>
 * 线程安全性：
 * Fetcher的请求和响应可能由不同的线程处理，因为心跳线程可能会处理响应。
 * 其他操作是单线程的，仅由轮询消费者的线程调用。
 * <ul>
 *     <li>如果响应处理器访问Fetcher的任何共享状态（如FetchSessionHandler），
 *     所有对该状态的访问必须在Fetcher实例上同步。</li>
 *     <li>如果响应处理器访问协调器的任何共享状态（如SubscriptionState），
 *     假定调用者已在协调器实例上同步了所有对该状态的访问。</li>
 *     <li>任何时候每个节点最多只能有一个待处理请求。跟踪带有待处理请求的节点，
 *     并在处理响应后更新。这确保了在一个线程上处理响应时更新的任何状态（如epoch）
 *     在另一个线程上创建后续请求时都是可见的。</li>
 * </ul>
 */
public class Fetcher<K, V> extends AbstractFetch {

    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 消费者网络客户端
     * 用于处理与Kafka代理的网络通信
     */
    private final ConsumerNetworkClient client;

    /**
     * 获取数据收集器
     * 用于收集和处理从代理获取的数据
     */
    private final FetchCollector<K, V> fetchCollector;

    /**
     * 构造函数
     * 初始化获取器的所有必要组件
     *
     * @param logContext 日志上下文
     * @param client 消费者网络客户端
     * @param metadata 消费者元数据
     * @param subscriptions 订阅状态
     * @param fetchConfig 获取配置
     * @param deserializers 反序列化器
     * @param metricsManager 度量管理器
     * @param time 时间服务
     * @param apiVersions API版本信息
     */
    public Fetcher(LogContext logContext,
                   ConsumerNetworkClient client,
                   ConsumerMetadata metadata,
                   SubscriptionState subscriptions,
                   FetchConfig fetchConfig,
                   Deserializers<K, V> deserializers,
                   FetchMetricsManager metricsManager,
                   Time time,
                   ApiVersions apiVersions) {
        // 调用父类构造函数，初始化基本组件
        super(logContext, metadata, subscriptions, fetchConfig, new FetchBuffer(logContext), metricsManager, time, apiVersions);
        // 初始化日志记录器
        this.log = logContext.logger(Fetcher.class);
        // 设置网络客户端
        this.client = client;
        // 创建获取数据收集器
        this.fetchCollector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers,
                metricsManager,
                time);
    }

    /**
     * 检查节点是否不可用
     *
     * @param node 要检查的节点
     * @return 如果节点不可用返回true
     */
    @Override
    protected boolean isUnavailable(Node node) {
        // 委托给客户端检查节点可用性
        return client.isUnavailable(node);
    }

    /**
     * 检查并可能抛出认证失败异常
     *
     * @param node 要检查的节点
     */
    @Override
    protected void maybeThrowAuthFailure(Node node) {
        // 委托给客户端检查认证失败
        client.maybeThrowAuthFailure(node);
    }

    /**
     * 清除未分配分区的缓冲数据
     *
     * @param assignedPartitions 已分配的分区集合
     */
    public void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions) {
        // 仅保留已分配分区的数据
        fetchBuffer.retainAll(new HashSet<>(assignedPartitions));
    }

    /**
     * 为任何已分配分区的节点设置获取请求
     * 这些节点还没有正在进行的获取或待处理的获取数据
     *
     * @return 发送的获取请求数量
     */
    public synchronized int sendFetches() {
        // 准备获取请求
        final Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests = prepareFetchRequests();
        // 发送获取请求并设置成功和失败的处理器
        sendFetchesInternal(
                fetchRequests,
                (fetchTarget, data, clientResponse) -> {
                    synchronized (Fetcher.this) {
                        handleFetchSuccess(fetchTarget, data, clientResponse);
                    }
                },
                (fetchTarget, data, error) -> {
                    synchronized (Fetcher.this) {
                        handleFetchFailure(fetchTarget, data, error);
                    }
                });
        // 返回发送的请求数量
        return fetchRequests.size();
    }

    /**
     * 可能关闭获取会话
     * 在指定的时间限制内尝试关闭所有获取会话
     *
     * @param timer 用于强制执行时间限制的计时器
     */
    protected void maybeCloseFetchSessions(final Timer timer) {
        // 发送关闭获取会话的请求
        final List<RequestFuture<ClientResponse>> requestFutures = sendFetchesInternal(
                prepareCloseFetchSessionRequests(),
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );

        // 轮询确保请求已写入套接字
        // 等待直到计时器过期或所有请求都收到响应
        while (timer.notExpired() && !requestFutures.stream().allMatch(RequestFuture::isDone)) {
            client.poll(timer, null, true);
            timer.update();
        }

        // 检查是否所有请求都完成了
        if (!requestFutures.stream().allMatch(RequestFuture::isDone)) {
            // 在完成所有future之前超时了
            // 这是可以接受的，因为我们不想在这里阻塞关闭
            log.debug("All requests couldn't be sent in the specific timeout period {}ms. " +
                    "This may result in unnecessary fetch sessions at the broker. Consider increasing the timeout passed for " +
                    "KafkaConsumer.close(Duration timeout)", timer.timeoutMs());
        }
    }

    /**
     * 收集获取的数据
     *
     * @return 包含获取数据的Fetch对象
     */
    public Fetch<K, V> collectFetch() {
        // 从获取缓冲区收集数据
        return fetchCollector.collectFetch(fetchBuffer);
    }

    /**
     * 内部关闭方法
     * 由close(Timer)调用，由IdempotentCloser保护，确保只在首次调用close()方法时执行一次
     *
     * @param timer 用于强制执行时间限制的计时器
     */
    protected synchronized void closeInternal(Timer timer) {
        // 禁用唤醒，因为我们已经在关闭了
        client.disableWakeups();
        // 尝试关闭获取会话
        maybeCloseFetchSessions(timer);
        // 调用父类的关闭方法
        super.closeInternal(timer);
    }

    /**
     * 内部发送获取请求的方法
     * 创建获取请求，将其排队/发送，并为响应添加回调
     *
     * @param fetchRequests 节点到其请求数据的映射
     * @param successHandler 成功响应的处理器
     * @param errorHandler 失败响应的处理器
     * @return 回调的Future列表
     */
    private List<RequestFuture<ClientResponse>> sendFetchesInternal(Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests,
                                                                    ResponseHandler<ClientResponse> successHandler,
                                                                    ResponseHandler<Throwable> errorHandler) {
        // 创建请求Future列表
        final List<RequestFuture<ClientResponse>> requestFutures = new ArrayList<>();

        // 处理每个获取请求
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequests.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            // 创建获取请求构建器
            final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
            // 发送请求并获取响应Future
            final RequestFuture<ClientResponse> responseFuture = client.send(fetchTarget, request);

            // 添加响应监听器
            responseFuture.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ClientResponse resp) {
                    // 处理成功响应
                    successHandler.handle(fetchTarget, data, resp);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // 处理失败响应
                    errorHandler.handle(fetchTarget, data, e);
                }
            });

            // 将Future添加到列表
            requestFutures.add(responseFuture);
        }

        return requestFutures;
    }
}