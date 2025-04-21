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
package org.apache.kafka.clients;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * 发送到服务器的请求。包含网络发送信息和客户端级别的元数据。
 */
public final class ClientRequest {

    // 目标服务器的标识符
    private final String destination;
    // 请求构建器，用于创建具体的请求内容
    private final AbstractRequest.Builder<?> requestBuilder;
    // 请求的关联ID，用于匹配请求和响应
    private final int correlationId;
    // 客户端ID，用于在服务器端识别客户端
    private final String clientId;
    // 请求创建时的时间戳（毫秒）
    private final long createdTimeMs;
    // 是否期望收到响应
    private final boolean expectResponse;
    // 请求超时时间（毫秒）
    private final int requestTimeoutMs;
    // 请求完成时的回调处理器
    private final RequestCompletionHandler callback;

    /**
     * @param destination The brokerId to send the request to
     * @param requestBuilder The builder for the request to make
     * @param correlationId The correlation id for this client request
     * @param clientId The client ID to use for the header
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(String destination,
                         AbstractRequest.Builder<?> requestBuilder,
                         int correlationId,
                         String clientId,
                         long createdTimeMs,
                         boolean expectResponse,
                         int requestTimeoutMs,
                         RequestCompletionHandler callback) {
        this.destination = destination;
        this.requestBuilder = requestBuilder;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.createdTimeMs = createdTimeMs;
        this.expectResponse = expectResponse;
        this.requestTimeoutMs = requestTimeoutMs;
        this.callback = callback;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", destination=" + destination +
            ", correlationId=" + correlationId +
            ", clientId=" + clientId +
            ", createdTimeMs=" + createdTimeMs +
            ", requestBuilder=" + requestBuilder +
            ")";
    }

    /**
     * 检查是否期望收到响应
     * @return 如果期望收到响应返回true，否则返回false
     */
    public boolean expectResponse() {
        return expectResponse;
    }

    /**
     * 获取请求的API类型
     * @return 请求的API类型枚举值
     */
    public ApiKeys apiKey() {
        return requestBuilder.apiKey();
    }

    /**
     * 创建请求头
     * @param version API版本号
     * @return 包含请求元数据的请求头对象
     */
    public RequestHeader makeHeader(short version) {
        ApiKeys requestApiKey = apiKey();
        return new RequestHeader(
            new RequestHeaderData()
                .setRequestApiKey(requestApiKey.id)
                .setRequestApiVersion(version)
                .setClientId(clientId)
                .setCorrelationId(correlationId),
            requestApiKey.requestHeaderVersion(version));
    }

    /**
     * 获取请求构建器
     * @return 用于构建请求的构建器对象
     */
    public AbstractRequest.Builder<?> requestBuilder() {
        return requestBuilder;
    }

    /**
     * 获取目标服务器标识符
     * @return 目标服务器的标识符
     */
    public String destination() {
        return destination;
    }

    /**
     * 获取请求完成回调处理器
     * @return 回调处理器对象
     */
    public RequestCompletionHandler callback() {
        return callback;
    }

    /**
     * 获取请求创建时间
     * @return 请求创建时的时间戳（毫秒）
     */
    public long createdTimeMs() {
        return createdTimeMs;
    }

    /**
     * 获取请求关联ID
     * @return 用于关联请求和响应的ID
     */
    public int correlationId() {
        return correlationId;
    }

    /**
     * 获取请求超时时间
     * @return 请求的超时时间（毫秒）
     */
    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }
}
