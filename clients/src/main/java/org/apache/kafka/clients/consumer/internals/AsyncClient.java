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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

/**
 * Kafka消费者客户端的异步网络通信基类。
 * 该类使用泛型设计，支持不同类型的请求和响应处理：
 * @param <T1> 请求数据的类型
 * @param <Req> 具体的请求类型，必须是AbstractRequest的子类
 * @param <Resp> 对应的响应类型，必须是AbstractResponse的子类
 * @param <T2> 处理响应后返回的结果类型
 */
public abstract class AsyncClient<T1, Req extends AbstractRequest, Resp extends AbstractResponse, T2> {

    // 日志记录器，用于记录异步通信过程中的关键信息
    private final Logger log;
    // 消费者网络客户端，负责实际的网络通信
    private final ConsumerNetworkClient client;

    /**
     * 构造函数
     * @param client 消费者网络客户端实例，用于执行实际的网络请求
     * @param logContext 日志上下文，用于创建特定类的日志记录器
     */
    AsyncClient(ConsumerNetworkClient client, LogContext logContext) {
        this.client = client;
        this.log = logContext.logger(getClass());
    }

    /**
     * 发送异步请求并处理响应
     * @param node 目标节点，表示要发送请求的Kafka broker
     * @param requestData 请求数据，类型为T1
     * @return 返回RequestFuture对象，包含类型为T2的响应结果
     */
    public RequestFuture<T2> sendAsyncRequest(Node node, T1 requestData) {
        // 调用子类实现的prepareRequest方法，准备具体的请求
        AbstractRequest.Builder<Req> requestBuilder = prepareRequest(node, requestData);

        // 通过网络客户端发送请求，并使用RequestFutureAdapter处理响应
        return client.send(node, requestBuilder).compose(new RequestFutureAdapter<>() {
            @Override
            @SuppressWarnings("unchecked")
            public void onSuccess(ClientResponse value, RequestFuture<T2> future) {
                Resp resp;
                try {
                    // 尝试将响应体转换为预期的响应类型
                    resp = (Resp) value.responseBody();
                } catch (ClassCastException cce) {
                    // 类型转换失败时记录错误并通知Future
                    log.error("Could not cast response body", cce);
                    future.raise(cce);
                    return;
                }
                // 记录收到的响应信息
                log.trace("Received {} {} from broker {}", resp.getClass().getSimpleName(), resp, node);
                try {
                    // 调用子类实现的handleResponse方法处理响应，并完成Future
                    future.complete(handleResponse(node, requestData, resp));
                } catch (RuntimeException e) {
                    // 处理响应过程中发生异常时，如果Future未完成则通知异常
                    if (!future.isDone()) {
                        future.raise(e);
                    }
                }
            }

        });
    }

    /**
     * 获取日志记录器实例
     * @return 当前类使用的Logger实例
     */
    protected Logger logger() {
        return log;
    }

    /**
     * 准备请求的抽象方法，子类必须实现此方法来构建具体的请求
     * @param node 目标节点
     * @param requestData 请求数据
     * @return 返回请求构建器
     */
    protected abstract AbstractRequest.Builder<Req> prepareRequest(Node node, T1 requestData);

    /**
     * 处理响应的抽象方法，子类必须实现此方法来处理服务器的响应
     * @param node 目标节点
     * @param requestData 原始请求数据
     * @param response 服务器的响应
     * @return 返回处理后的结果
     */
    protected abstract T2 handleResponse(Node node, T1 requestData, Resp response);
}
