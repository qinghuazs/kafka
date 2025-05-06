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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * 后台线程Runnable，用于消费{@link ApplicationEvent}并产生{@link BackgroundEvent}。
 * 它使用事件循环来消费和产生事件，并轮询网络客户端以处理网络I/O。
 */
public class ConsumerNetworkThread extends KafkaThread implements Closeable {

    // 用于测试的可见字段
    // 最大轮询超时时间，5秒
    static final long MAX_POLL_TIMEOUT_MS = 5000;
    // 后台线程名称
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";
    // 时间工具类
    private final Time time;
    // 日志记录器
    private final Logger log;
    // 应用事件队列，用于存储待处理的应用事件
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    // 可完成事件收割器，用于处理过期事件
    private final CompletableEventReaper applicationEventReaper;
    // 应用事件处理器供应商
    private final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier;
    // 网络客户端代理供应商
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    // 请求管理器供应商
    private final Supplier<RequestManagers> requestManagersSupplier;
    // 异步消费者指标收集器
    private final AsyncConsumerMetrics asyncConsumerMetrics;
    // 应用事件处理器实例
    private ApplicationEventProcessor applicationEventProcessor;
    // 网络客户端代理实例
    private NetworkClientDelegate networkClientDelegate;
    // 请求管理器实例
    private RequestManagers requestManagers;
    // 线程运行状态标志
    private volatile boolean running;
    // 幂等关闭器，确保资源只被关闭一次
    private final IdempotentCloser closer = new IdempotentCloser();
    // 关闭超时时间
    private volatile Duration closeTimeout = Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS);
    // 缓存的最大等待时间
    private volatile long cachedMaximumTimeToWait = MAX_POLL_TIMEOUT_MS;
    // 上次轮询时间戳
    private long lastPollTimeMs = 0L;

    /**
     * 构造函数，初始化消费者网络线程
     * 
     * @param logContext 日志上下文，用于创建日志记录器
     * @param time 时间工具类，用于获取时间戳和创建定时器
     * @param applicationEventQueue 应用事件队列，存储待处理的应用事件
     * @param applicationEventReaper 事件收割器，用于处理过期事件
     * @param applicationEventProcessorSupplier 应用事件处理器供应商
     * @param networkClientDelegateSupplier 网络客户端代理供应商
     * @param requestManagersSupplier 请求管理器供应商
     * @param asyncConsumerMetrics 异步消费者指标收集器
     */
    public ConsumerNetworkThread(LogContext logContext,
                                 Time time,
                                 BlockingQueue<ApplicationEvent> applicationEventQueue,
                                 CompletableEventReaper applicationEventReaper,
                                 Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                 Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                 Supplier<RequestManagers> requestManagersSupplier,
                                 AsyncConsumerMetrics asyncConsumerMetrics) {
        // 调用父类构造函数，设置线程名称和守护线程标志
        super(BACKGROUND_THREAD_NAME, true);
        this.time = time;
        this.log = logContext.logger(getClass());
        this.applicationEventQueue = applicationEventQueue;
        this.applicationEventReaper = applicationEventReaper;
        this.applicationEventProcessorSupplier = applicationEventProcessorSupplier;
        this.networkClientDelegateSupplier = networkClientDelegateSupplier;
        this.requestManagersSupplier = requestManagersSupplier;
        this.running = true;
        this.asyncConsumerMetrics = asyncConsumerMetrics;
    }

    /**
     * 线程运行方法，实现事件循环处理逻辑
     * 1. 初始化必要的资源
     * 2. 在running为true时持续执行事件处理
     * 3. 捕获并记录异常，但继续运行
     * 4. 最后清理资源
     */
    @Override
    public void run() {
        try {
            log.debug("Consumer network thread started");

            // 等待安全进入后台网络线程后再初始化这些对象
            initializeResources();

            // 主事件循环
            while (running) {
                try {
                    // 执行一次事件处理循环
                    runOnce();
                } catch (final Throwable e) {
                    // 记录异常但继续运行
                    log.error("Unexpected error caught in consumer network thread", e);
                }
            }
        } finally {
            // 线程结束时清理资源
            cleanup();
        }
    }

    /**
     * 初始化资源
     * 1. 创建应用事件处理器实例
     * 2. 创建网络客户端代理实例
     * 3. 创建请求管理器实例
     */
    void initializeResources() {
        applicationEventProcessor = applicationEventProcessorSupplier.get();
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
    }

    /**
     * 轮询并处理{@link ApplicationEvent 应用事件}。执行以下任务：
     *
     * <ol>
     *     <li>
     *         通过{@link ApplicationEventProcessor}从应用线程的事件队列中提取并处理所有事件
     *     </li>
     *     <li>
     *         遍历{@link RequestManager}列表并调用{@link RequestManager#poll(long)}获取
     *         {@link NetworkClientDelegate.UnsentRequest}列表和网络轮询时间
     *     </li>
     *     <li>
     *         通过{@link NetworkClientDelegate#addAll(List)}准备发送每个{@link AbstractRequest.Builder 请求}
     *     </li>
     *     <li>
     *         通过{@link KafkaClient#poll(long, long)}轮询客户端以发送请求并获取可用的响应
     *     </li>
     * </ol>
     */
    void runOnce() {
        // 处理应用事件队列中的事件
        processApplicationEvents();

        // 获取当前时间戳
        final long currentTimeMs = time.milliseconds();
        // 记录两次轮询之间的时间间隔
        if (lastPollTimeMs != 0L) {
            asyncConsumerMetrics.recordTimeBetweenNetworkThreadPoll(currentTimeMs - lastPollTimeMs);
        }
        lastPollTimeMs = currentTimeMs;

        // 计算网络轮询等待时间
        // 1. 遍历所有请求管理器
        // 2. 对每个管理器执行poll操作获取未发送的请求
        // 3. 将请求添加到网络客户端代理
        // 4. 取所有轮询时间的最小值，但不超过MAX_POLL_TIMEOUT_MS
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(rm -> rm.poll(currentTimeMs))
                .map(networkClientDelegate::addAll)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        // 执行网络轮询，发送请求并接收响应
        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);

        // 更新缓存的最大等待时间
        // 取所有请求管理器返回的最大等待时间的最小值
        cachedMaximumTimeToWait = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(rm -> rm.maximumTimeToWait(currentTimeMs))
                .reduce(Long.MAX_VALUE, Math::min);

        // 处理过期的应用事件
        reapExpiredApplicationEvents(currentTimeMs);
        // 获取未完成的事件列表
        List<CompletableEvent<?>> uncompletedEvents = applicationEventReaper.uncompletedEvents();
        // 检查元数据错误并处理
        maybeFailOnMetadataError(uncompletedEvents);
    }

    /**
     * 处理应用线程产生的事件
     * 1. 从事件队列中提取所有事件
     * 2. 记录事件处理的相关指标
     * 3. 对每个事件进行处理：
     *    - 如果是可完成事件，添加到收割器并检查元数据错误
     *    - 使用事件处理器处理事件
     *    - 捕获并记录处理过程中的异常
     */
    private void processApplicationEvents() {
        // 创建临时列表存储待处理的事件
        LinkedList<ApplicationEvent> events = new LinkedList<>();
        // 从队列中提取所有事件
        applicationEventQueue.drainTo(events);
        if (events.isEmpty())
            return;

        // 记录队列大小指标
        asyncConsumerMetrics.recordApplicationEventQueueSize(0);
        long startMs = time.milliseconds();
        
        // 处理每个事件
        for (ApplicationEvent event : events) {
            // 记录事件在队列中的等待时间
            asyncConsumerMetrics.recordApplicationEventQueueTime(time.milliseconds() - event.enqueuedMs());
            try {
                // 处理可完成事件
                if (event instanceof CompletableEvent) {
                    // 添加到收割器
                    applicationEventReaper.add((CompletableEvent<?>) event);
                    // Check if there are any metadata errors and fail the CompletableEvent if an error is present.
                    // This call is meant to handle "immediately completed events" which may not enter the awaiting state,
                    // so metadata errors need to be checked and handled right away.
                    maybeFailOnMetadataError(List.of((CompletableEvent<?>) event));
                }
                applicationEventProcessor.process(event);
            } catch (Throwable t) {
                log.warn("Error processing event {}", t.getMessage(), t);
            }
        }
        asyncConsumerMetrics.recordApplicationEventQueueProcessingTime(time.milliseconds() - startMs);
    }

    /**
     * 处理已过期的事件。这个清理步骤只应在网络I/O线程至少调用过一次
     * {@link NetworkClientDelegate#poll(long, long) poll}之后执行，
     * 这样可以确保每个事件在检查超时之前至少有一次机会满足网络请求。
     * 
     * @param currentTimeMs 当前时间戳，用于判断事件是否过期
     */
    private void reapExpiredApplicationEvents(long currentTimeMs) {
        // 使用事件收割器清理过期事件，并记录过期事件数量
        asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(currentTimeMs));
    }

    /**
     * 在消费者关闭时执行必要的网络I/O操作：
     *
     * <ol>
     *     <li>
     *         遍历{@link RequestManager}列表并调用{@link RequestManager#pollOnClose(long)}
     *         获取{@link NetworkClientDelegate.UnsentRequest}列表和网络轮询时间
     *     </li>
     *     <li>
     *         通过{@link NetworkClientDelegate#addAll(List)}准备发送每个
     *         {@link AbstractRequest.Builder 请求}
     *     </li>
     *     <li>
     *         通过{@link KafkaClient#poll(long, long) 轮询客户端}发送请求并
     *         获取可用的响应
     *     </li>
     *     <li>
     *         只要{@link Timer#notExpired() 定时器未过期}，就持续
     *         {@link KafkaClient#poll(long, long) 轮询客户端}以获取响应
     *     </li>
     * </ol>
     * 
     * @param requestManagers 请求管理器集合
     * @param networkClientDelegate 网络客户端代理
     * @param currentTimeMs 当前时间戳
     */
    // 用于测试
    static void runAtClose(final Collection<Optional<? extends RequestManager>> requestManagers,
                           final NetworkClientDelegate networkClientDelegate,
                           final long currentTimeMs) {
        // 处理关闭时的待发送请求
        requestManagers.stream()
                .filter(Optional::isPresent)  // 过滤出存在的请求管理器
                .map(Optional::get)          // 获取请求管理器实例
                .map(rm -> rm.pollOnClose(currentTimeMs))  // 获取关闭时需要发送的请求
                .forEach(networkClientDelegate::addAll);   // 将请求添加到网络客户端代理
    }

    /**
     * 检查消费者网络线程是否正在运行
     * 
     * @return 如果线程正在运行返回true，否则返回false
     */
    public boolean isRunning() {
        // 返回线程运行状态标志
        return running;
    }

    /**
     * 唤醒网络客户端，中断当前的轮询操作
     * 这个方法通常在需要立即处理某些操作（如关闭）时调用
     */
    public void wakeup() {
        // 网络客户端可能为空，因为initializeResources方法可能还未被调用
        if (networkClientDelegate != null)
            // 唤醒网络客户端，中断当前的轮询操作
            networkClientDelegate.wakeup();
    }

    /**
     * 返回应用线程在需要响应请求管理器结果之前可以安全等待的延迟时间
     * 例如，当发送心跳时订阅状态可能会改变，如果阻塞时间超过心跳间隔，
     * 应用线程可能无法及时响应这些变化
     * 
     * 由于此方法由应用线程调用，不允许直接访问提供信息的请求管理器
     * 因此，消费者网络线程会定期缓存来自请求管理器的信息，然后可以通过此方法安全地读取
     * 
     * @return 最大延迟时间（毫秒）
     */
    public long maximumTimeToWait() {
        // 返回缓存的最大等待时间
        return cachedMaximumTimeToWait;
    }

    /**
     * 使用默认超时时间关闭消费者网络线程
     */
    @Override
    public void close() {
        // 使用默认的关闭超时时间调用close方法
        close(closeTimeout);
    }

    /**
     * 使用指定的超时时间关闭消费者网络线程
     * 
     * @param timeout 等待网络线程关闭的超时时间
     */
    public void close(final Duration timeout) {
        // 确保超时参数不为空
        Objects.requireNonNull(timeout, "Close timeout for consumer network thread must be non-null");

        // 使用幂等关闭器确保资源只被关闭一次
        closer.close(
                // 执行实际的关闭操作
                () -> closeInternal(timeout),
                // 如果已经关闭，则记录警告日志
                () -> log.warn("The consumer network thread was already closed")
        );
    }

    /**
     * 启动关闭过程
     * 
     * 此方法由应用线程调用，但资源由网络线程拥有。因此，我们不会在应用线程上
     * 立即关闭这些资源。相反，我们只是更新应用线程上的内部状态。当网络线程下次
     * {@link #run() 执行其循环}时，它会注意到这个状态，停止处理任何进一步的事件，
     * 并开始{@link #cleanup() 关闭其资源}
     * 
     * 此方法将等待（即阻塞应用线程）最多给定超时时间的持续时间，以给网络线程
     * 时间进行清理关闭
     * 
     * @param timeout 等待网络线程关闭资源的时间上限
     */
    private void closeInternal(final Duration timeout) {
        // 将超时时间转换为毫秒
        long timeoutMs = timeout.toMillis();
        // 记录关闭信号的跟踪日志
        log.trace("Signaling the consumer network thread to close in {}ms", timeoutMs);
        // 设置运行标志为false
        running = false;
        // 更新关闭超时时间
        closeTimeout = timeout;
        // 唤醒网络线程
        wakeup();

        try {
            // 等待网络线程完成
            join();
        } catch (InterruptedException e) {
            // 如果等待被中断，记录错误日志
            log.error("Interrupted while waiting for consumer network thread to complete", e);
        }
    }

    /**
     * 最后一次检查未发送队列，并持续轮询直到所有请求都已发送或定时器超时
     * 
     * @param timer 用于控制轮询超时的定时器
     */
    private void sendUnsentRequests(final Timer timer) {
        // 如果没有待处理的请求，直接返回
        if (!networkClientDelegate.hasAnyPendingRequests())
            return;

        // 持续轮询直到所有请求发送完成或定时器超时
        do {
            // 执行一次网络轮询
            networkClientDelegate.poll(timer.remainingMs(), timer.currentTimeMs());
            // 更新定时器
            timer.update();
        } while (timer.notExpired() && networkClientDelegate.hasAnyPendingRequests());

        // 如果仍有未发送的请求，记录警告日志
        if (networkClientDelegate.hasAnyPendingRequests()) {
            log.warn("Close timeout of {} ms expired before the consumer network thread was able " +
                "to complete pending requests. Inflight request count: {}, Unsent request count: {}",
                timer.timeoutMs(), networkClientDelegate.inflightRequestCount(), networkClientDelegate.unsentRequests().size());
        }
    }

    /**
     * 清理消费者网络线程的资源
     * 该方法在线程关闭时执行，负责：
     * 1. 处理关闭时的待发送请求
     * 2. 清理过期的应用事件
     * 3. 安全关闭各种资源
     * 
     * 设计考虑：
     * - 使用Timer控制关闭超时，避免长时间阻塞
     * - 即使出现异常也会继续执行清理，确保资源被释放
     * - 使用closeQuietly方法安全关闭资源，避免抛出异常
     */
    void cleanup() {
        // 记录开始关闭的跟踪日志
        log.trace("Closing the consumer network thread");
        // 创建关闭超时定时器
        Timer timer = time.timer(closeTimeout);
        try {
            // 处理关闭时的待发送请求
            runAtClose(requestManagers.entries(), networkClientDelegate, time.milliseconds());
        } catch (Exception e) {
            // 记录异常但继续执行关闭流程
            log.error("Unexpected error during shutdown. Proceed with closing.", e);
        } finally {
            // 尝试发送所有未发送的请求
            sendUnsentRequests(timer);
            // 清理过期的应用事件并记录数量
            asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(applicationEventQueue));

            // 安全关闭请求管理器
            closeQuietly(requestManagers, "request managers");
            // 安全关闭网络客户端代理
            closeQuietly(networkClientDelegate, "network client delegate");
            // 记录关闭完成的调试日志
            log.debug("Closed the consumer network thread");
        }
    }

    /**
     * 检查元数据错误并将错误传播给需要订阅元数据的未完成事件
     * 
     * 设计考虑：
     * - 只处理需要订阅元数据的CompletableApplicationEvent
     * - 使用Stream API进行事件过滤和转换，提高代码可读性
     * - 通过future的completeExceptionally方法传播错误，确保事件能正确处理失败情况
     * 
     * @param events 需要检查的可完成事件列表
     */
    private void maybeFailOnMetadataError(List<CompletableEvent<?>> events) {
        // 过滤出需要订阅元数据的CompletableApplicationEvent
        List<? extends CompletableApplicationEvent<?>> subscriptionMetadataEvent = events.stream()
                // 首先过滤出CompletableApplicationEvent类型的事件
                .filter(e -> e instanceof CompletableApplicationEvent<?>)
                // 将事件转换为CompletableApplicationEvent类型
                .map(e -> (CompletableApplicationEvent<?>) e)
                // 再过滤出需要订阅元数据的事件
                .filter(CompletableApplicationEvent::requireSubscriptionMetadata)
                // 收集到列表中
                .collect(Collectors.toList());
        
        // 如果没有需要处理的事件，直接返回
        if (subscriptionMetadataEvent.isEmpty())
            return;

        // 获取并清除元数据错误，如果存在错误则传播给所有事件
        networkClientDelegate.getAndClearMetadataError().ifPresent(metadataError ->
                subscriptionMetadataEvent.forEach(event -> event.future().completeExceptionally(metadataError))
        );
    }
}
