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

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

/**
 * 请求状态类
 * 用于管理请求的状态、重试逻辑和退避策略
 * 
 * 应用场景：
 * 1. 管理网络请求的生命周期
 * 2. 实现指数退避重试机制
 * 3. 控制请求发送时机
 * 4. 处理请求失败和重试
 */
class RequestState {
    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 请求所有者标识
     */
    protected final String owner;

    /**
     * 重试退避指数基数，默认为2
     * 用于计算指数退避时间
     */
    static final int RETRY_BACKOFF_EXP_BASE = 2;

    /**
     * 重试退避抖动因子，默认为0.2
     * 用于在退避时间中添加随机性，避免多个客户端同时重试
     */
    static final double RETRY_BACKOFF_JITTER = 0.2;

    /**
     * 指数退避计算器
     * 用于计算重试间隔时间
     */
    protected final ExponentialBackoff exponentialBackoff;

    /**
     * 最近一次发送请求的时间戳（毫秒）
     */
    protected long lastSentMs = -1;

    /**
     * 最近一次接收响应的时间戳（毫秒）
     */
    protected long lastReceivedMs = -1;

    /**
     * 当前请求的尝试次数
     */
    protected int numAttempts = 0;

    /**
     * 当前的退避时间（毫秒）
     */
    protected long backoffMs = 0;

    /**
     * 标识是否有请求正在进行中
     */
    private boolean requestInFlight = false;

    /**
     * 构造函数
     * 创建一个新的请求状态实例
     *
     * @param logContext 日志上下文
     * @param owner 请求所有者标识
     * @param retryBackoffMs 初始重试退避时间
     * @param retryBackoffMaxMs 最大重试退避时间
     */
    public RequestState(final LogContext logContext,
                        final String owner,
                        final long retryBackoffMs,
                        final long retryBackoffMaxMs) {
        // 初始化日志记录器
        this.log = logContext.logger(RequestState.class);
        // 设置请求所有者
        this.owner = owner;
        // 创建指数退避计算器，使用默认的指数基数和抖动因子
        this.exponentialBackoff = new ExponentialBackoff(
                retryBackoffMs,
                RETRY_BACKOFF_EXP_BASE,
                retryBackoffMaxMs,
                RETRY_BACKOFF_JITTER);
    }

    /**
     * 测试用构造函数
     * 允许自定义所有退避参数
     */
    RequestState(final LogContext logContext,
                 final String owner,
                 final long retryBackoffMs,
                 final int retryBackoffExpBase,
                 final long retryBackoffMaxMs,
                 final double jitter) {
        // 初始化日志记录器
        this.log = logContext.logger(RequestState.class);
        // 设置请求所有者
        this.owner = owner;
        // 创建指数退避计算器，使用自定义参数
        this.exponentialBackoff = new ExponentialBackoff(
                retryBackoffMs,
                retryBackoffExpBase,
                retryBackoffMaxMs,
                jitter);
    }

    /**
     * 重置请求状态
     * 将所有状态恢复到初始值，允许立即发送新请求
     */
    public void reset() {
        // 清除进行中的请求标志
        this.requestInFlight = false;
        // 重置最后发送时间
        this.lastSentMs = -1;
        // 重置最后接收时间
        this.lastReceivedMs = -1;
        // 重置尝试次数
        this.numAttempts = 0;
        // 重置退避时间为最小值
        this.backoffMs = exponentialBackoff.backoff(0);
    }

    /**
     * 检查是否可以发送新请求
     * 
     * @param currentTimeMs 当前时间戳（毫秒）
     * @return 如果可以发送新请求则返回true
     */
    public boolean canSendRequest(final long currentTimeMs) {
        // 检查是否有请求正在进行中
        if (requestInFlight()) {
            // 记录跟踪日志
            log.trace("An inflight request already exists for {}", this);
            return false;
        }

        // 计算剩余的退避时间
        long remainingBackoffMs = remainingBackoffMs(currentTimeMs);

        // 如果没有剩余退避时间，允许发送请求
        if (remainingBackoffMs <= 0) {
            return true;
        } else {
            // 记录剩余等待时间
            log.trace("{} ms remain before another request should be sent for {}", remainingBackoffMs, this);
            return false;
        }
    }

    /**
     * 检查是否有请求正在进行中
     * 
     * @return 如果有请求正在进行中则返回true
     */
    public boolean requestInFlight() {
        return requestInFlight;
    }

    /**
     * 记录发送请求的尝试
     * 
     * @param currentTimeMs 当前时间戳（毫秒）
     */
    public void onSendAttempt(final long currentTimeMs) {
        // 设置请求进行中标志
        this.requestInFlight = true;
        // 更新最后发送时间
        this.lastSentMs = currentTimeMs;
    }

    /**
     * 处理请求发送成功的回调
     * 重置尝试次数，但保持最小退避时间
     * 
     * @param currentTimeMs 当前时间戳（毫秒）
     */
    public void onSuccessfulAttempt(final long currentTimeMs) {
        // 清除请求进行中标志
        this.requestInFlight = false;
        // 更新最后接收时间
        this.lastReceivedMs = currentTimeMs;
        // 重置退避时间为最小值
        this.backoffMs = exponentialBackoff.backoff(0);
        // 重置尝试次数
        this.numAttempts = 0;
    }

    /**
     * 处理请求发送失败的回调
     * 增加尝试次数并更新退避时间
     * 
     * @param currentTimeMs 当前时间戳（毫秒）
     */
    public void onFailedAttempt(final long currentTimeMs) {
        // 清除请求进行中标志
        this.requestInFlight = false;
        // 更新最后接收时间
        this.lastReceivedMs = currentTimeMs;
        // 根据当前尝试次数计算新的退避时间
        this.backoffMs = exponentialBackoff.backoff(numAttempts);
        // 增加尝试次数
        this.numAttempts++;
    }

    /**
     * 计算剩余的退避时间
     * 
     * @param currentTimeMs 当前时间戳（毫秒）
     * @return 剩余的退避时间（毫秒）
     */
    long remainingBackoffMs(final long currentTimeMs) {
        // 计算自上次接收响应后经过的时间
        long timeSinceLastReceiveMs = currentTimeMs - this.lastReceivedMs;
        // 返回剩余的退避时间，不小于0
        return Math.max(0, backoffMs - timeSinceLastReceiveMs);
    }

    /**
     * This method appends the instance variables together in a simple String of comma-separated key value pairs.
     * This allows subclasses to include these values and not have to duplicate each variable, helping to prevent
     * any variables from being omitted when new ones are added.
     *
     * @return String version of instance variables.
     */
    protected String toStringBase() {
        return "owner='" + owner + '\'' +
                ", exponentialBackoff=" + exponentialBackoff +
                ", lastSentMs=" + lastSentMs +
                ", lastReceivedMs=" + lastReceivedMs +
                ", numAttempts=" + numAttempts +
                ", backoffMs=" + backoffMs +
                ", requestInFlight=" + requestInFlight;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + toStringBase() + '}';
    }
}
