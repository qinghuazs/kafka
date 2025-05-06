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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * 应用事件处理器，负责接收来自应用线程的{@link ApplicationEvent 应用事件}，
 * 这些事件随后可以被{@link ConsumerNetworkThread 网络线程}中的{@link ApplicationEventProcessor}读取和处理。
 * 
 * 该类是Kafka消费者客户端中的核心组件之一，主要用于处理异步事件，实现应用线程和网络线程之间的解耦。
 * 它通过阻塞队列来传递事件，确保事件能够被可靠地传递和处理。
 */
public class ApplicationEventHandler implements Closeable {

    // 用于记录日志的Logger实例
    private final Logger log;
    // 用于获取系统时间的工具类
    private final Time time;
    // 存储应用事件的阻塞队列，用于实现应用线程和网络线程之间的事件传递
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    // 负责网络I/O操作的消费者网络线程
    private final ConsumerNetworkThread networkThread;
    // 用于确保资源只被关闭一次的幂等关闭器
    private final IdempotentCloser closer = new IdempotentCloser();
    // 异步消费者指标收集器，用于监控和统计事件处理相关的指标
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    /**
     * 构造函数，初始化应用事件处理器的各个组件
     * 
     * @param logContext 日志上下文，用于创建日志记录器
     * @param time 时间工具类，用于获取系统时间
     * @param applicationEventQueue 应用事件队列，用于存储待处理的事件
     * @param applicationEventReaper 完成事件收割器，用于清理已完成的事件
     * @param applicationEventProcessorSupplier 应用事件处理器的供应商，提供事件处理器实例
     * @param networkClientDelegateSupplier 网络客户端代理的供应商，提供网络通信功能
     * @param requestManagersSupplier 请求管理器的供应商，提供请求管理功能
     * @param asyncConsumerMetrics 异步消费者指标收集器，用于性能监控
     */
    public ApplicationEventHandler(final LogContext logContext,
                                   final Time time,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final CompletableEventReaper applicationEventReaper,
                                   final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                   final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                   final Supplier<RequestManagers> requestManagersSupplier,
                                   final AsyncConsumerMetrics asyncConsumerMetrics) {
        // 初始化日志记录器
        this.log = logContext.logger(ApplicationEventHandler.class);
        // 初始化时间工具类
        this.time = time;
        // 初始化应用事件队列
        this.applicationEventQueue = applicationEventQueue;
        // 初始化指标收集器
        this.asyncConsumerMetrics = asyncConsumerMetrics;
        // 创建并启动网络线程
        this.networkThread = new ConsumerNetworkThread(logContext,
                time,
                applicationEventQueue,
                applicationEventReaper,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier,
                asyncConsumerMetrics);
        this.networkThread.start();
    }

    /**
     * 将应用事件添加到处理器中，并唤醒网络I/O线程进行处理
     * 
     * 该方法的主要职责：
     * 1. 验证事件的有效性
     * 2. 记录事件入队时间
     * 3. 更新队列大小指标
     * 4. 将事件添加到队列
     * 5. 唤醒网络线程处理事件
     *
     * @param event 由应用线程创建的{@link ApplicationEvent 应用事件}
     */
    public void add(final ApplicationEvent event) {
        // 确保事件对象不为空
        Objects.requireNonNull(event, "ApplicationEvent provided to add must be non-null");
        // 设置事件入队时间戳
        event.setEnqueuedMs(time.milliseconds());
        // 在实际添加事件到队列之前记录更新后的队列大小
        // 这样做是为了避免竞态条件（后台线程在持续从队列中移除事件）
        asyncConsumerMetrics.recordApplicationEventQueueSize(applicationEventQueue.size() + 1);
        // 将事件添加到队列中
        applicationEventQueue.add(event);
        // 唤醒网络线程处理新事件
        wakeupNetworkThread();
    }

    /**
     * 唤醒{@link ConsumerNetworkThread 网络I/O线程}以从队列中获取并处理下一个事件
     * 
     * 该方法通常在新事件被添加到队列后调用，用于通知网络线程及时处理新事件
     */
    public void wakeupNetworkThread() {
        // 调用网络线程的唤醒方法
        networkThread.wakeup();
    }

    /**
     * 返回应用线程在需要响应请求管理器结果之前可以安全等待的延迟时间
     * 
     * 这个方法的主要用途是确保应用线程能够及时响应状态变化。例如：
     * - 当发送心跳时，订阅状态可能发生变化
     * - 如果阻塞时间超过心跳间隔，应用线程可能无法及时响应这些变化
     * 
     * 应用场景：
     * 1. 用于控制轮询超时时间
     * 2. 确保消费者能够及时处理组协调和心跳等关键操作
     * 3. 在长时间操作中提供合适的检查点
     *
     * @return 最大等待时间（毫秒）
     */
    public long maximumTimeToWait() {
        // 从网络线程获取最大等待时间
        return networkThread.maximumTimeToWait();
    }

    /**
     * 添加一个{@link CompletableApplicationEvent 可完成的应用事件}到处理器并等待其结果
     * 
     * 该方法的特点：
     * 1. 同步阻塞：会阻塞等待事件处理完成
     * 2. 结果返回：成功时返回事件处理结果，失败时抛出异常
     * 3. 中断处理：支持线程中断机制
     * 
     * 应用场景：
     * - 需要立即获知操作结果的同步操作
     * - 事务性操作或需要保证顺序的操作
     * - 需要等待远程操作完成的场景
     *
     * @param event 由轮询线程创建的{@link CompletableApplicationEvent 可完成的应用事件}
     * @return 事件处理的结果值
     * @param <T> 事件返回值的类型
     * @throws InterruptException 如果等待过程中线程被中断
     */
    public <T> T addAndGet(final CompletableApplicationEvent<T> event) {
        // 确保事件对象不为空
        Objects.requireNonNull(event, "CompletableApplicationEvent provided to addAndGet must be non-null");
        // 添加事件到队列
        add(event);
        // 在开始等待之前检查线程是否被中断，确保即使在不需要等待的情况下也能传播异常
        // （事件可能在添加到队列和尝试获取结果之间的时间内就完成了）
        if (Thread.interrupted()) {
            throw new InterruptException("Interrupted waiting for results for application event " + event);
        }
        // 获取并返回事件的处理结果
        return ConsumerUtils.getResult(event.future());
    }

    /**
     * 关闭事件处理器，使用默认的超时时间（零）
     * 实现自{@link Closeable}接口
     */
    @Override
    public void close() {
        // 调用带超时参数的关闭方法，超时时间设为0
        close(Duration.ZERO);
    }

    /**
     * 使用指定的超时时间关闭事件处理器
     * 
     * 该方法的主要职责：
     * 1. 确保资源的幂等关闭（每个资源只会被关闭一次）
     * 2. 安全关闭网络线程
     * 3. 处理重复关闭的情况
     *
     * @param timeout 等待资源关闭的最大时间
     */
    public void close(final Duration timeout) {
        // 使用幂等关闭器执行关闭操作
        closer.close(
                // 主要关闭操作：安静地关闭网络线程
                () -> Utils.closeQuietly(() -> networkThread.close(timeout), "consumer network thread"),
                // 如果已经关闭，则记录警告日志
                () -> log.warn("The application event handler was already closed")
        );
    }
}
