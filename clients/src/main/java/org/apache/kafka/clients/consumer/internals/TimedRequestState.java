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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

/**
 * TimedRequestState类扩展了RequestState类的功能，通过添加Timer来跟踪请求的过期时间。
 * 在Kafka中，这个类主要用于管理需要超时控制的请求状态，例如消费者的心跳请求、组成员请求等。
 * 它不仅继承了RequestState的重试机制，还增加了精确的超时控制功能。
 *
 * @see RequestState 基础请求状态类，提供重试和退避机制
 * @see Timer 计时器接口，用于跟踪时间和判断过期
 */
public class TimedRequestState extends RequestState {

    /**
     * 计时器实例，用于跟踪请求的过期时间
     * 这个timer是final的，确保一旦初始化就不能被修改，保证了时间跟踪的一致性
     */
    private final Timer timer;

    /**
     * 创建一个新的TimedRequestState实例
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param owner 请求所有者的标识符，用于日志和调试
     * @param retryBackoffMs 重试的初始退避时间（毫秒）
     * @param retryBackoffMaxMs 重试的最大退避时间（毫秒）
     * @param timer 用于跟踪请求过期时间的计时器实例
     */
    public TimedRequestState(final LogContext logContext,
                             final String owner,
                             final long retryBackoffMs,
                             final long retryBackoffMaxMs,
                             final Timer timer) {
        super(logContext, owner, retryBackoffMs, retryBackoffMaxMs);
        this.timer = timer;
    }

    /**
     * 创建一个新的TimedRequestState实例，支持更细粒度的退避控制
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param owner 请求所有者的标识符，用于日志和调试
     * @param retryBackoffMs 重试的初始退避时间（毫秒）
     * @param retryBackoffExpBase 指数退避的基数，用于计算下一次退避时间
     * @param retryBackoffMaxMs 重试的最大退避时间（毫秒）
     * @param jitter 退避时间的随机抖动因子，用于避免多个请求同时重试
     * @param timer 用于跟踪请求过期时间的计时器实例
     */
    public TimedRequestState(final LogContext logContext,
                             final String owner,
                             final long retryBackoffMs,
                             final int retryBackoffExpBase,
                             final long retryBackoffMaxMs,
                             final double jitter,
                             final Timer timer) {
        super(logContext, owner, retryBackoffMs, retryBackoffExpBase, retryBackoffMaxMs, jitter);
        this.timer = timer;
    }

    /**
     * 检查请求是否已过期
     * 
     * @return 如果请求已过期返回true，否则返回false
     * 实现细节：首先更新计时器状态，然后检查是否过期
     */
    public boolean isExpired() {
        timer.update();
        return timer.isExpired();
    }

    /**
     * 获取距离请求过期还剩余的毫秒数
     * 
     * @return 剩余的毫秒数，如果已过期则返回0
     * 实现细节：首先更新计时器状态，然后获取剩余时间
     */
    public long remainingMs() {
        timer.update();
        return timer.remainingMs();
    }

    /**
     * 创建一个截止时间计时器
     * 这个静态方法用于创建一个新的计时器，该计时器将在指定的截止时间到达时过期
     *
     * @param time 时间接口实例，用于获取当前时间和创建计时器
     * @param deadlineMs 截止时间（毫秒级时间戳）
     * @return 新创建的计时器实例
     * 实现细节：计算当前时间到截止时间的差值，确保不会出现负值，然后创建计时器
     */
    public static Timer deadlineTimer(final Time time, final long deadlineMs) {
        long diff = Math.max(0, deadlineMs - time.milliseconds());
        return time.timer(diff);
    }


    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", remainingMs=" + remainingMs();
    }
}
