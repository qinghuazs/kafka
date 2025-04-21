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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * 服务器的响应。包含响应体以及原始请求的相关元数据。
 */
public class ClientResponse {

    // 原始请求的请求头
    private final RequestHeader requestHeader;
    // 请求完成时的回调处理器
    private final RequestCompletionHandler callback;
    // 目标服务器的标识符
    private final String destination;
    // 接收响应的时间戳（毫秒）
    private final long receivedTimeMs;
    // 请求延迟时间（毫秒）
    private final long latencyMs;
    // 客户端是否在完全读取响应之前断开连接
    private final boolean disconnected;
    // 是否因超时而断开连接
    private final boolean timedOut;
    // API版本不匹配异常
    private final UnsupportedVersionException versionMismatch;
    // 认证异常
    private final AuthenticationException authenticationException;
    // 响应体
    private final AbstractResponse responseBody;

    /**
     * @param requestHeader The header of the corresponding request
     * @param callback The callback to be invoked
     * @param destination The node the corresponding request was sent to
     * @param createdTimeMs The unix timestamp when the corresponding request was created
     * @param receivedTimeMs The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param versionMismatch Whether there was a version mismatch that prevented sending the request.
     * @param responseBody The response contents (or null) if we disconnected, no response was expected,
     *                     or if there was a version mismatch.
     */
    public ClientResponse(RequestHeader requestHeader,
                          RequestCompletionHandler callback,
                          String destination,
                          long createdTimeMs,
                          long receivedTimeMs,
                          boolean disconnected,
                          UnsupportedVersionException versionMismatch,
                          AuthenticationException authenticationException,
                          AbstractResponse responseBody) {
        this(requestHeader,
             callback,
             destination,
             createdTimeMs,
             receivedTimeMs,
             disconnected,
             false,
             versionMismatch,
             authenticationException,
             responseBody);
    }

    /**
     * @param requestHeader The header of the corresponding request
     * @param callback The callback to be invoked
     * @param destination The node the corresponding request was sent to
     * @param createdTimeMs The unix timestamp when the corresponding request was created
     * @param receivedTimeMs The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param timedOut Whether the client was disconnected because of a timeout; when setting this
     *                 to <code>true</code>, <code>disconnected</code> must be <code>true</code>
     *                 or an {@link IllegalStateException} will be thrown
     * @param versionMismatch Whether there was a version mismatch that prevented sending the request.
     * @param responseBody The response contents (or null) if we disconnected, no response was expected,
     *                     or if there was a version mismatch.
     */
    public ClientResponse(RequestHeader requestHeader,
                          RequestCompletionHandler callback,
                          String destination,
                          long createdTimeMs,
                          long receivedTimeMs,
                          boolean disconnected,
                          boolean timedOut,
                          UnsupportedVersionException versionMismatch,
                          AuthenticationException authenticationException,
                          AbstractResponse responseBody) {
        if (!disconnected && timedOut)
            throw new IllegalStateException("The client response can't be in the state of connected, yet timed out");

        this.requestHeader = requestHeader;
        this.callback = callback;
        this.destination = destination;
        this.receivedTimeMs = receivedTimeMs;
        this.latencyMs = receivedTimeMs - createdTimeMs;
        this.disconnected = disconnected;
        this.timedOut = timedOut;
        this.versionMismatch = versionMismatch;
        this.authenticationException = authenticationException;
        this.responseBody = responseBody;
    }

    /**
     * 获取响应接收时间
     * @return 接收响应的时间戳（毫秒）
     */
    public long receivedTimeMs() {
        return receivedTimeMs;
    }

    /**
     * 检查是否在读取响应过程中断开连接
     * @return 如果连接断开返回true，否则返回false
     */
    public boolean wasDisconnected() {
        return disconnected;
    }

    /**
     * 检查是否因超时而断开连接
     * @return 如果因超时断开返回true，否则返回false
     */
    public boolean wasTimedOut() {
        return timedOut;
    }

    /**
     * 获取API版本不匹配异常
     * @return 版本不匹配异常对象，如果没有版本不匹配则为null
     */
    public UnsupportedVersionException versionMismatch() {
        return versionMismatch;
    }

    /**
     * 获取认证异常
     * @return 认证异常对象，如果没有认证错误则为null
     */
    public AuthenticationException authenticationException() {
        return authenticationException;
    }

    /**
     * 获取请求头
     * @return 原始请求的请求头对象
     */
    public RequestHeader requestHeader() {
        return requestHeader;
    }

    /**
     * 获取目标服务器标识符
     * @return 目标服务器的标识符
     */
    public String destination() {
        return destination;
    }

    /**
     * 获取响应体
     * @return 响应的具体内容
     */
    public AbstractResponse responseBody() {
        return responseBody;
    }

    /**
     * 检查是否包含响应体
     * @return 如果响应体不为null返回true，否则返回false
     */
    public boolean hasResponse() {
        return responseBody != null;
    }

    /**
     * 获取请求延迟时间
     * @return 从发送请求到接收响应的时间间隔（毫秒）
     */
    public long requestLatencyMs() {
        return latencyMs;
    }

    /**
     * 执行请求完成回调
     * 如果设置了回调处理器，则调用其onComplete方法
     */
    public void onComplete() {
        if (callback != null)
            callback.onComplete(this);
    }

    @Override
    public String toString() {
        return "ClientResponse(receivedTimeMs=" + receivedTimeMs +
               ", latencyMs=" +
               latencyMs +
               ", disconnected=" +
               disconnected +
               ", timedOut=" +
               timedOut +
               ", requestHeader=" +
               requestHeader +
               ", responseBody=" +
               responseBody +
               ")";
    }

}
