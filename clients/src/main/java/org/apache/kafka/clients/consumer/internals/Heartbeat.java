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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

/**
 * 一个用于管理消费者组成员与协调器之间心跳的辅助类
 * 
 * 该类负责以下功能：
 * 1. 维护心跳定时器，确保定期向协调器发送心跳
 * 2. 跟踪会话超时，防止消费者组成员被踢出组
 * 3. 管理消费者轮询超时，确保消费者正常消费消息
 * 4. 处理心跳失败的重试机制
 */
public final class Heartbeat {
    // 消费者最大轮询间隔，超过此时间未调用poll()方法将被认为消费者已死亡
    private final int maxPollIntervalMs;
    // 消费者组重平衡配置，包含心跳间隔、会话超时等参数
    private final GroupRebalanceConfig rebalanceConfig;
    // 时间工具类，用于获取当前时间和创建定时器
    private final Time time;
    // 心跳定时器，用于控制发送心跳的时间间隔
    private final Timer heartbeatTimer;
    // 会话定时器，用于检测会话是否超时
    private final Timer sessionTimer;
    // 轮询定时器，用于检测消费者是否及时调用poll()方法
    private final Timer pollTimer;
    // 日志记录器
    private final Logger log;
    // 指数退避重试机制，用于心跳失败时的重试间隔控制
    private final ExponentialBackoff retryBackoff;

    // 上次发送心跳的时间戳
    private volatile long lastHeartbeatSend = 0L;
    // 标识当前是否有正在处理的心跳请求
    private volatile boolean heartbeatInFlight = false;
    // 心跳重试次数，用于计算退避时间
    private volatile long heartbeatAttempts = 0L;

    /**
     * 心跳管理器的构造函数
     * 
     * @param config 消费者组重平衡配置，包含心跳间隔、会话超时等参数
     * @param time 时间工具类实例
     * @throws IllegalArgumentException 当心跳间隔大于等于会话超时时抛出异常
     */
    public Heartbeat(GroupRebalanceConfig config,
                     Time time) {
        // 确保心跳间隔小于会话超时，否则可能在会话超时前无法发送心跳
        if (config.heartbeatIntervalMs >= config.sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");
        this.rebalanceConfig = config;
        this.time = time;
        // 初始化各个定时器
        this.heartbeatTimer = time.timer(config.heartbeatIntervalMs);
        this.sessionTimer = time.timer(config.sessionTimeoutMs);
        this.maxPollIntervalMs = config.rebalanceTimeoutMs;
        this.pollTimer = time.timer(maxPollIntervalMs);
        // 创建指数退避重试机制，用于心跳失败时的重试
        this.retryBackoff = new ExponentialBackoff(rebalanceConfig.retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                rebalanceConfig.retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);

        // 初始化日志记录器，包含消费者组ID信息
        final LogContext logContext = new LogContext("[Heartbeat groupID=" + config.groupId + "] ");
        this.log = logContext.logger(getClass());
    }

    /**
     * 更新所有定时器的当前时间
     * 
     * @param now 当前时间戳
     */
    private void update(long now) {
        heartbeatTimer.update(now);
        sessionTimer.update(now);
        pollTimer.update(now);
    }

    /**
     * 记录消费者的poll调用，重置轮询超时定时器
     * 
     * @param now 当前时间戳
     */
    public void poll(long now) {
        update(now);
        pollTimer.reset(maxPollIntervalMs);
    }

    /**
     * 检查是否有正在处理的心跳请求
     * 
     * @return 如果有正在处理的心跳请求返回true，否则返回false
     */
    boolean hasInflight() {
        return heartbeatInFlight;
    }

    /**
     * 发送心跳请求时调用，更新相关状态
     * 
     * @param now 当前时间戳
     */
    void sentHeartbeat(long now) {
        // 记录发送时间和状态
        lastHeartbeatSend = now;
        heartbeatInFlight = true;
        update(now);
        // 重置心跳定时器，准备下一次心跳
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs);

        if (log.isTraceEnabled()) {
            log.trace("Sending heartbeat request with {}ms remaining on timer", heartbeatTimer.remainingMs());
        }
    }

    /**
     * 处理心跳失败的情况，使用指数退避策略重试
     */
    void failHeartbeat() {
        update(time.milliseconds());
        heartbeatInFlight = false;
        // 使用指数退避算法计算下一次重试的间隔时间
        heartbeatTimer.reset(retryBackoff.backoff(heartbeatAttempts++));

        log.trace("Heartbeat failed, reset the timer to {}ms remaining", heartbeatTimer.remainingMs());
    }

    /**
     * 处理心跳响应成功的情况
     * 重置心跳状态和会话定时器
     */
    void receiveHeartbeat() {
        update(time.milliseconds());
        // 清除心跳处理中状态
        heartbeatInFlight = false;
        // 重置重试次数
        heartbeatAttempts = 0L;
        // 重置会话定时器，表示与协调器的连接正常
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
    }

    /**
     * 检查是否需要发送新的心跳
     * 
     * @param now 当前时间戳
     * @return 如果心跳定时器已过期返回true，表示需要发送新的心跳
     */
    boolean shouldHeartbeat(long now) {
        update(now);
        return heartbeatTimer.isExpired();
    }
    
    /**
     * 获取上次发送心跳的时间戳
     * 
     * @return 上次发送心跳的时间戳
     */
    long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    /**
     * 计算距离下一次需要发送心跳的剩余时间
     * 
     * @param now 当前时间戳
     * @return 距离下一次心跳的毫秒数
     */
    long timeToNextHeartbeat(long now) {
        update(now);
        return heartbeatTimer.remainingMs();
    }

    /**
     * 检查会话是否已超时
     * 如果超时，表示消费者可能已经被协调器认为已死亡
     * 
     * @param now 当前时间戳
     * @return 如果会话已超时返回true
     */
    boolean sessionTimeoutExpired(long now) {
        update(now);
        return sessionTimer.isExpired();
    }

    /**
     * 重置所有超时计时器
     * 通常在重新加入消费者组或重平衡后调用
     */
    void resetTimeouts() {
        update(time.milliseconds());
        // 重置会话超时、轮询超时和心跳间隔
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
        pollTimer.reset(maxPollIntervalMs);
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs);
    }

    /**
     * 仅重置会话超时计时器
     * 通常在收到协调器的响应但不需要重置其他计时器时调用
     */
    void resetSessionTimeout() {
        update(time.milliseconds());
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs);
    }

    /**
     * 检查消费者的轮询操作是否已超时
     * 如果超时，表示消费者可能已经停止处理消息
     * 
     * @param now 当前时间戳
     * @return 如果轮询已超时返回true
     */
    boolean pollTimeoutExpired(long now) {
        update(now);
        return pollTimer.isExpired();
    }

    /**
     * 获取最后一次poll()调用的时间
     * 
     * @return 最后一次poll()调用的时间戳
     */
    long lastPollTime() {
        return pollTimer.currentTimeMs();
    }
}
