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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * FetchRequestManager负责根据用户的主题订阅/分区分配，
 * 为可获取的分区（{@link SubscriptionState#fetchablePartitions(Predicate)}）
 * 生成获取请求（{@link FetchRequest}）。
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    /**
     * 网络客户端代理
     * 用于处理与Kafka代理的网络通信
     */
    private final NetworkClientDelegate networkClientDelegate;

    /**
     * 待处理的获取请求Future
     * 用于跟踪当前正在进行的获取请求
     */
    private CompletableFuture<Void> pendingFetchRequestFuture;

    /**
     * 构造函数
     * 初始化获取请求管理器的所有必要组件
     *
     * @param logContext 日志上下文
     * @param time 时间服务
     * @param metadata 消费者元数据
     * @param subscriptions 订阅状态
     * @param fetchConfig 获取配置
     * @param fetchBuffer 获取缓冲区
     * @param metricsManager 度量管理器
     * @param networkClientDelegate 网络客户端代理
     * @param apiVersions API版本信息
     */
    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig fetchConfig,
                        final FetchBuffer fetchBuffer,
                        final FetchMetricsManager metricsManager,
                        final NetworkClientDelegate networkClientDelegate,
                        final ApiVersions apiVersions) {
        // 调用父类构造函数初始化基本组件
        super(logContext, metadata, subscriptions, fetchConfig, fetchBuffer, metricsManager, time, apiVersions);
        // 设置网络客户端代理
        this.networkClientDelegate = networkClientDelegate;
    }

    /**
     * 检查节点是否不可用
     *
     * @param node 要检查的节点
     * @return 如果节点不可用返回true
     */
    @Override
    protected boolean isUnavailable(Node node) {
        // 委托给网络客户端代理检查节点可用性
        return networkClientDelegate.isUnavailable(node);
    }

    /**
     * 检查并可能抛出认证失败异常
     *
     * @param node 要检查的节点
     */
    @Override
    protected void maybeThrowAuthFailure(Node node) {
        // 委托给网络客户端代理检查认证失败
        networkClientDelegate.maybeThrowAuthFailure(node);
    }

    /**
     * 创建获取请求
     * 通知消费者需要为代理节点创建请求以获取下一批记录
     *
     * @see CreateFetchRequestsEvent
     * @return 调用者可以等待的Future，确保请求已创建
     */
    public CompletableFuture<Void> createFetchRequests() {
        // 创建新的CompletableFuture
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (pendingFetchRequestFuture != null) {
            // 如果存在待处理的获取请求，将新创建的future链接到待处理的future
            pendingFetchRequestFuture.whenComplete((value, exception) -> {
                if (exception != null) {
                    // 如果有异常，使新future异常完成
                    future.completeExceptionally(exception);
                } else {
                    // 否则正常完成新future
                    future.complete(value);
                }
            });
        } else {
            // 如果没有待处理的请求，设置新future为待处理
            pendingFetchRequestFuture = future;
        }

        return future;
    }

    /**
     * 轮询获取请求
     * 
     * @param currentTimeMs 当前时间戳
     * @return 轮询结果
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        // 使用内部轮询方法处理常规获取请求
        return pollInternal(
            this::prepareFetchRequests,
            this::handleFetchSuccess,
            this::handleFetchFailure
        );
    }

    /**
     * 关闭时的轮询操作
     * 
     * @param currentTimeMs 当前时间戳
     * @return 轮询结果
     */
    @Override
    public PollResult pollOnClose(long currentTimeMs) {
        // 创建待处理的获取请求，这是pollInternal创建请求所必需的
        createFetchRequests();

        // 使用内部轮询方法处理关闭时的获取会话请求
        return pollInternal(
                this::prepareCloseFetchSessionRequests,
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );
    }

    /**
     * 创建包含零个或多个获取请求的轮询结果
     *
     * @param fetchRequestPreparer 获取请求准备器，生成节点到其获取请求数据的映射
     * @param successHandler 成功响应的处理器
     * @param errorHandler 失败响应的处理器
     * @return 轮询结果
     */
    private PollResult pollInternal(FetchRequestPreparer fetchRequestPreparer,
                                    ResponseHandler<ClientResponse> successHandler,
                                    ResponseHandler<Throwable> errorHandler) {
        // 如果没有待处理的获取请求，返回空结果
        if (pendingFetchRequestFuture == null) {
            return PollResult.EMPTY;
        }

        try {
            // 准备获取请求
            Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests = fetchRequestPreparer.prepare();

            // 将获取请求转换为未发送的请求列表
            List<UnsentRequest> requests = fetchRequests.entrySet().stream().map(entry -> {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                // 创建获取请求构建器
                final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
                // 创建响应处理器
                final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                    if (error != null)
                        errorHandler.handle(fetchTarget, data, error);
                    else
                        successHandler.handle(fetchTarget, data, clientResponse);
                };

                // 创建未发送的请求并添加完成处理器
                return new UnsentRequest(request, Optional.of(fetchTarget)).whenComplete(responseHandler);
            }).collect(Collectors.toList());

            // 完成待处理的future
            pendingFetchRequestFuture.complete(null);
            return new PollResult(requests);
        } catch (Throwable t) {
            // 异常处理：返回空结果而不是重新抛出异常
            // 因为从RequestManager.poll()方法抛出的任何异常都会中断其他请求管理器的轮询
            pendingFetchRequestFuture.completeExceptionally(t);
            return PollResult.EMPTY;
        } finally {
            // 清除待处理的future
            pendingFetchRequestFuture = null;
        }
    }

    /**
     * 获取请求准备器接口
     * 简单的函数式接口，用于传递方法引用以提高可读性
     */
    @FunctionalInterface
    protected interface FetchRequestPreparer {
        /**
         * 准备获取请求数据
         * @return 节点到其获取请求数据的映射
         */
        Map<Node, FetchSessionHandler.FetchRequestData> prepare();
    }
}
