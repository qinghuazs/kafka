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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * {@code CompletableEvent}是一个接口，同时被{@link CompletableApplicationEvent}和{@link CompletableBackgroundEvent}
 * 用于通用的处理和逻辑实现。这个接口允许调用者获取与事件相关的{@link #future() future}对象和事件的
 * {@link #deadlineMs() 过期时间戳}。
 * 
 * 应用场景：
 * 1. 在Kafka消费者客户端中处理异步操作，如提交偏移量、加入消费者组等
 * 2. 提供统一的事件完成状态跟踪机制
 * 3. 支持超时控制，避免事件长时间未完成导致资源泄露
 *
 * @param <T> 事件完成时的返回类型
 */
public interface CompletableEvent<T> {

    /**
     * 返回与此事件关联的{@link CompletableFuture future}对象。每个事件都有其相关的执行逻辑。
     * 事件可以通过以下三种方式之一完成：
     *
     * <ul>
     *     <li>
     *         成功：当事件逻辑成功完成时，该事件生成的数据（如果有）将传递给{@link CompletableFuture#complete(Object)}。
     *         如果泛型类型被指定为{@link Void}，则提供{@code null}值。
     *         例如：提交偏移量成功、成功加入消费者组等。</li>
     *     <li>
     *         错误：当事件逻辑产生错误时，错误将传递给{@link CompletableFuture#completeExceptionally(Throwable)}。
     *         例如：网络异常、broker不可用等。
     *     </li>
     *     <li>
     *         超时：当执行事件逻辑的时间超过{@link #deadlineMs() 截止时间}时，将创建一个{@link TimeoutException}实例
     *         并传递给{@link CompletableFuture#completeExceptionally(Throwable)}。当消费者关闭时，如果事件仍未完成，
     *         也会发生这种情况。
     *         例如：提交偏移量超时、加入消费者组超时等。
     *     </li>
     * </ul>
     *
     * @return 调用者可以在该Future上阻塞或查询完成状态
     *
     * @see CompletableEventReaper 用于管理和清理超时事件的组件
     */
    CompletableFuture<T> future();

    /**
     * 表示事件特定执行必须完成的绝对时钟时间的截止时间。这不是一个超时值，而是一个绝对时间点。
     * 当<em>超过</em>这个时间点后，将使用{@link TimeoutException}实例调用
     * {@link CompletableFuture#completeExceptionally(Throwable)}。
     *
     * 设计考虑：
     * 1. 使用绝对时间而不是相对时间，避免系统时间调整带来的影响
     * 2. 便于事件清理器统一管理和处理超时事件
     * 3. 支持不同类型事件设置不同的截止时间
     *
     * @return 事件必须完成的绝对时间（毫秒）
     *
     * @see CompletableEventReaper 事件清理器会定期检查并处理超过截止时间的事件
     */
    long deadlineMs();

    /**
     * 基于{@link Timer#currentTimeMs()}和{@link Timer#remainingMs()}计算截止时间戳。
     * 
     * 实现细节：
     * 1. 使用Timer对象获取当前时间和剩余时间
     * 2. 确保Timer对象不为null
     * 3. 调用基础计算方法计算最终截止时间
     *
     * @param timer 定时器对象，提供当前时间和剩余时间
     *
     * @return 事件应该完成的绝对时间（毫秒）
     */
    static long calculateDeadlineMs(final Timer timer) {
        requireNonNull(timer);
        return calculateDeadlineMs(timer.currentTimeMs(), timer.remainingMs());
    }

    /**
     * 基于{@link Timer#currentTimeMs()}和{@link Duration#toMillis()}计算截止时间戳。
     * 
     * 实现细节：
     * 1. 使用Time对象获取当前时间
     * 2. 将Duration转换为毫秒
     * 3. 确保参数不为null
     * 4. 调用基础计算方法计算最终截止时间
     *
     * @param time     时间提供者对象
     * @param duration 持续时间对象
     *
     * @return 事件应该完成的绝对时间（毫秒）
     */
    static long calculateDeadlineMs(final Time time, final Duration duration) {
        return calculateDeadlineMs(requireNonNull(time).milliseconds(), requireNonNull(duration).toMillis());
    }

    /**
     * 基于{@link Timer#currentTimeMs()}和超时时间计算截止时间戳。
     * 
     * 实现细节：
     * 1. 使用Time对象获取当前时间
     * 2. 确保Time对象不为null
     * 3. 调用基础计算方法计算最终截止时间
     *
     * @param time      时间提供者对象
     * @param timeoutMs 超时时间（毫秒）
     *
     * @return 事件应该完成的绝对时间（毫秒）
     */
    static long calculateDeadlineMs(final Time time, final long timeoutMs) {
        return calculateDeadlineMs(requireNonNull(time).milliseconds(), timeoutMs);
    }

    /**
     * 基于当前时间和超时时间计算截止时间戳。这是最基础的计算方法，其他计算方法最终都会调用此方法。
     * 
     * 实现细节：
     * 1. 检查是否会发生时间溢出
     * 2. 如果会溢出，返回Long.MAX_VALUE
     * 3. 否则返回当前时间加上超时时间
     *
     * @param currentTimeMs 当前时间（毫秒）
     * @param timeoutMs     超时时间（毫秒）
     *
     * @return 事件应该完成的绝对时间（毫秒）
     */
    static long calculateDeadlineMs(final long currentTimeMs, final long timeoutMs) {
        if (currentTimeMs > Long.MAX_VALUE - timeoutMs)
            return Long.MAX_VALUE;
        else
            return currentTimeMs + timeoutMs;
    }
}
