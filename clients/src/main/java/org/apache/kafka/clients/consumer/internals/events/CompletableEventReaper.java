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

import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * {@code CompletableEventReaper} 负责跟踪带有时间限制的事件（{@link CompletableEvent time-bound events}），
 * 并移除那些超过其截止时间（{@link CompletableEvent#deadlineMs() deadline}）的事件（除非它们已经完成）。
 * 这个机制被 {@link AsyncKafkaConsumer} 用来强制执行用户在其API调用中提供的超时时间
 * （例如 {@link AsyncKafkaConsumer#commitSync(Duration)}）。
 *
 * 应用场景：
 * 1. 管理Kafka消费者客户端中的异步操作超时，如提交偏移量、加入消费者组等
 * 2. 清理已完成或超时的事件，避免资源泄露
 * 3. 提供统一的事件完成状态跟踪机制
 *
 * 设计考虑：
 * 1. 使用ArrayList存储待跟踪的事件，支持快速遍历和删除操作
 * 2. 通过定期检查机制及时处理超时事件
 * 3. 在消费者关闭时确保所有未完成事件得到适当处理
 */
public class CompletableEventReaper {

    /**
     * 日志记录器，用于记录事件处理过程中的重要信息
     */
    private final Logger log;

    /**
     * 跟踪的事件列表，这些事件都是可能过期的候选事件
     * 使用ArrayList实现，支持快速遍历和按条件删除操作
     */
    private final List<CompletableEvent<?>> tracked;

    /**
     * 构造函数，初始化事件清理器
     * 
     * 实现细节：
     * 1. 使用提供的LogContext创建特定于此类的日志记录器
     * 2. 初始化空的事件跟踪列表
     *
     * @param logContext 日志上下文对象，用于创建日志记录器
     */
    public CompletableEventReaper(LogContext logContext) {
        this.log = logContext.logger(CompletableEventReaper.class);
        this.tracked = new ArrayList<>();
    }

    /**
     * 添加新的事件到跟踪列表中，以便后续完成或过期处理
     * 
     * 实现细节：
     * 1. 使用Objects.requireNonNull确保事件不为null
     * 2. 将事件添加到跟踪列表中
     *
     * @param event 要跟踪的事件，不能为null
     */
    public void add(CompletableEvent<?> event) {
        tracked.add(Objects.requireNonNull(event, "Event to track must be non-null"));
    }

    /**
     * 执行两步处理过程来"完成"已过期或正常完成的事件：
     *
     * <ol>
     *     <li>
     *         对于每个超过其截止时间的事件，创建一个TimeoutException实例，
     *         并通过CompletableFuture.completeExceptionally方法传递异常。
     *     </li>
     *     <li>
     *         对于每个已经处于完成状态的事件，将其从跟踪列表中移除。
     *     </li>
     * </ol>
     *
     * 应用场景：
     * 1. 定期检查和处理超时事件
     * 2. 清理已完成的事件，释放资源
     * 3. 确保异步操作不会无限期挂起
     *
     * 设计考虑：
     * 1. 使用函数式编程方式处理事件流
     * 2. 分两步处理以确保正确的状态转换
     * 3. 通过日志记录关键操作信息
     *
     * @param currentTimeMs 当前时间（毫秒），用于与事件的过期时间进行比较
     * @return 过期的事件数量
     */
    public long reap(long currentTimeMs) {
        Consumer<CompletableEvent<?>> expireEvent = event -> {
            long pastDueMs = currentTimeMs - event.deadlineMs();
            TimeoutException error = new TimeoutException(String.format("%s was %s ms past its expiration of %s", event.getClass().getSimpleName(), pastDueMs, event.deadlineMs()));

            if (event.future().completeExceptionally(error)) {
                log.debug("Event {} completed exceptionally since its expiration of {} passed {} ms ago", event, event.deadlineMs(), pastDueMs);
            } else {
                log.trace("Event {} not completed exceptionally since it was previously completed", event);
            }
        };

        // First, complete (exceptionally) any events that have passed their deadline AND aren't already complete.
        long count = tracked.stream()
            .filter(e -> !e.future().isDone())
            .filter(e -> currentTimeMs >= e.deadlineMs())
            .peek(expireEvent)
            .count();
        // Second, remove any events that are already complete, just to make sure we don't hold references. This will
        // include any events that finished successfully as well as any events we just completed exceptionally above.
        tracked.removeIf(e -> e.future().isDone());

        return count;
    }

    /**
     * 在消费者关闭时处理所有未完成的事件
     * 
     * 应用场景：
     * 1. 消费者关闭时的清理工作
     * 2. 确保所有事件得到适当处理，避免资源泄露
     * 3. 处理已跟踪和未跟踪的事件
     *
     * 实现细节：
     * 1. 不考虑事件的截止时间，直接将所有未完成事件标记为异常完成
     * 2. 分别处理已跟踪的事件和新提供的事件队列
     * 3. 清空所有事件列表
     *
     * 设计考虑：
     * 1. 使用Stream API进行高效的事件处理
     * 2. 通过日志记录重要的状态变化
     * 3. 确保资源的完全释放
     *
     * @param events 需要处理的事件队列，这些事件尚未被跟踪
     * @return 处理的事件总数（已跟踪的过期事件数 + 队列中的过期事件数）
     */
    public long reap(Collection<?> events) {
        Objects.requireNonNull(events, "Event queue to reap must be non-null");

        Consumer<CompletableEvent<?>> expireEvent = event -> {
            TimeoutException error = new TimeoutException(String.format("%s could not be completed before the consumer closed", event.getClass().getSimpleName()));

            if (event.future().completeExceptionally(error)) {
                log.debug("Event {} completed exceptionally since the consumer is closing", event);
            } else {
                log.trace("Event {} not completed exceptionally since it was completed prior to the consumer closing", event);
            }
        };

        long trackedExpiredCount = tracked.stream()
            .filter(e -> !e.future().isDone())
            .peek(expireEvent)
            .count();
        tracked.clear();

        long eventExpiredCount = events.stream()
            .filter(e -> e instanceof CompletableEvent<?>)
            .map(e -> (CompletableEvent<?>) e)
            .filter(e -> !e.future().isDone())
            .peek(expireEvent)
            .count();
        events.clear();
        return trackedExpiredCount + eventExpiredCount;
    }

    /**
     * 获取当前跟踪的事件数量
     * 
     * @return 跟踪列表中的事件数量
     */
    public int size() {
        return tracked.size();
    }

    /**
     * 检查指定事件是否在跟踪列表中
     * 
     * 实现细节：
     * 1. 首先检查事件是否为null
     * 2. 使用List.contains方法检查事件是否在跟踪列表中
     *
     * @param event 要检查的事件
     * @return 如果事件在跟踪列表中返回true，否则返回false
     */
    public boolean contains(CompletableEvent<?> event) {
        return event != null && tracked.contains(event);
    }

    /**
     * 获取所有未完成的事件列表
     * 
     * 实现细节：
     * 1. 使用Stream API过滤未完成的事件
     * 2. 将结果收集到新的List中
     *
     * @return 包含所有未完成事件的新列表
     */
    public List<CompletableEvent<?>> uncompletedEvents() {
        return tracked.stream()
                .filter(e -> !e.future().isDone())
                .collect(Collectors.toList());
    }
    
}
