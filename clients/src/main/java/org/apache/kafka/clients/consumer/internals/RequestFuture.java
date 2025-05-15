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

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Timer;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 来自{@link ConsumerNetworkClient}的异步请求结果。使用{@link ConsumerNetworkClient#poll(Timer)}
 * （及其变体）来完成请求future。使用{@link #isDone()}检查future是否完成，使用
 * {@link #succeeded()}检查请求是否成功完成。典型用法如下：
 *
 * <pre>
 *     RequestFuture<ClientResponse> future = client.send(api, request);
 *     client.poll(future);
 *
 *     if (future.succeeded()) {
 *         ClientResponse response = future.value();
 *         // 处理响应
 *     } else {
 *         throw future.exception();
 *     }
 * </pre>
 *
 * 应用场景：
 * 1. 异步网络请求：处理Kafka消费者与服务器之间的异步通信
 * 2. 请求结果追踪：跟踪异步请求的完成状态和结果
 * 3. 错误处理：统一管理请求过程中的异常情况
 * 4. 监听器模式：支持对请求完成事件的异步监听
 *
 * @param <T> 结果的返回类型（如果没有响应可以是Void）
 */
public class RequestFuture<T> implements ConsumerNetworkClient.PollCondition {

    /**
     * 表示请求未完成的标记对象
     * 用于AtomicReference的初始值，代表异步操作尚未完成
     */
    private static final Object INCOMPLETE_SENTINEL = new Object();

    /**
     * 存储异步操作的结果
     * - 初始值为INCOMPLETE_SENTINEL，表示未完成
     * - 成功完成时存储类型为T的结果值
     * - 失败时存储RuntimeException异常对象
     */
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE_SENTINEL);

    /**
     * 存储请求完成时需要通知的监听器队列
     * 使用ConcurrentLinkedQueue确保线程安全
     * 监听器在请求成功或失败时被触发
     */
    private final ConcurrentLinkedQueue<RequestFutureListener<T>> listeners = new ConcurrentLinkedQueue<>();

    /**
     * 用于等待请求完成的闭锁
     * 当请求完成（无论成功还是失败）时，计数器减为0
     * 用于支持同步等待操作
     */
    private final CountDownLatch completedLatch = new CountDownLatch(1);

    /**
     * 检查响应是否已准备好处理
     * 
     * 实现说明：
     * - 通过检查result是否等于INCOMPLETE_SENTINEL来判断请求是否完成
     * - 使用AtomicReference保证线程安全的状态检查
     * 
     * @return 如果响应已就绪返回true，否则返回false
     */
    public boolean isDone() {
        return result.get() != INCOMPLETE_SENTINEL;
    }

    /**
     * 等待请求完成，支持超时机制
     * 
     * 实现说明：
     * - 利用CountDownLatch实现同步等待
     * - 支持超时时间设置，避免无限等待
     * - 可被中断，支持异常处理
     * 
     * @param timeout 等待超时时间
     * @param unit 时间单位
     * @return 如果在超时之前完成返回true，否则返回false
     * @throws InterruptedException 如果等待过程被中断
     */
    public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        return completedLatch.await(timeout, unit);
    }

    /**
     * 获取请求对应的结果值（仅在请求成功时可用）
     * 
     * 实现说明：
     * - 首先检查请求是否成功完成
     * - 使用类型转换将存储的结果转换为期望的类型
     * - 通过AtomicReference安全地访问结果值
     * 
     * @return 在{@link #complete(Object)}中设置的值
     * @throws IllegalStateException 如果future未完成或失败
     */
    @SuppressWarnings("unchecked")
    public T value() {
        if (!succeeded())
            throw new IllegalStateException("Attempt to retrieve value from future which hasn't successfully completed");
        return (T) result.get();
    }

    /**
     * 检查请求是否成功完成
     * 
     * 实现说明：
     * - 需要同时满足两个条件：
     *   1. 请求已完成（isDone()返回true）
     *   2. 请求未失败（failed()返回false）
     * 
     * @return 如果请求完成且成功则返回true
     */
    public boolean succeeded() {
        return isDone() && !failed();
    }

    /**
     * 检查请求是否失败
     * 
     * 实现说明：
     * - 通过检查result中存储的是否为RuntimeException来判断
     * - 使用instanceof进行类型检查
     * 
     * @return 如果请求以失败状态完成则返回true
     */
    public boolean failed() {
        return result.get() instanceof RuntimeException;
    }

    /**
     * 检查请求是否可重试
     * 
     * 实现说明：
     * - 这是一个便捷方法，用于检查异常是否为{@link RetriableException}类型
     * - 通过检查异常类型来判断请求是否可以重试
     * - 在重试机制中用于决定是否进行请求重试
     * 
     * @return 如果可以重试返回true，否则返回false
     * @throws IllegalStateException 如果future未完成或成功完成（没有异常）
     */
    public boolean isRetriable() {
        return exception() instanceof RetriableException;
    }

    /**
     * 获取失败结果中的异常（仅在请求失败时可用）
     * 
     * 实现说明：
     * - 首先验证请求是否确实失败
     * - 从AtomicReference中安全地获取异常对象
     * - 进行类型转换返回RuntimeException
     * 
     * @return 在{@link #raise(RuntimeException)}中设置的异常
     * @throws IllegalStateException 如果future未完成或成功完成
     */
    public RuntimeException exception() {
        if (!failed())
            throw new IllegalStateException("Attempt to retrieve exception from future which hasn't failed");
        return (RuntimeException) result.get();
    }

    /**
     * 成功完成请求
     * 
     * 实现说明：
     * - 参数校验：确保value不是RuntimeException类型
     * - 原子操作：使用CAS操作更新结果状态
     * - 完成处理：
     *   1. 设置结果值
     *   2. 触发成功事件通知监听器
     *   3. 释放完成闭锁
     * 
     * 调用此方法后：
     * - {@link #succeeded()}将返回true
     * - 可以通过{@link #value()}获取结果值
     * 
     * @param value 对应的结果值（如果没有可以为null）
     * @throws IllegalStateException 如果future已经完成
     * @throws IllegalArgumentException 如果参数是{@link RuntimeException}类型
     */
    public void complete(T value) {
        try {
            if (value instanceof RuntimeException)
                throw new IllegalArgumentException("The argument to complete can not be an instance of RuntimeException");

            if (!result.compareAndSet(INCOMPLETE_SENTINEL, value))
                throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
            fireSuccess();
        } finally {
            completedLatch.countDown();
        }
    }

    /**
     * 抛出异常，将请求标记为失败
     * 
     * 实现说明：
     * - 参数校验：确保异常对象不为null
     * - 原子操作：使用CAS操作更新结果状态
     * - 失败处理：
     *   1. 设置异常对象
     *   2. 触发失败事件通知监听器
     *   3. 释放完成闭锁
     * 
     * @param e 要传递给调用者的异常
     * @throws IllegalStateException 如果future已经完成
     * @throws IllegalArgumentException 如果异常参数为null
     */
    public void raise(RuntimeException e) {
        try {
            if (e == null)
                throw new IllegalArgumentException("The exception passed to raise must not be null");

            if (!result.compareAndSet(INCOMPLETE_SENTINEL, e))
                throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");

            fireFailure();
        } finally {
            completedLatch.countDown();
        }
    }

    /**
     * 抛出Kafka错误，将请求标记为失败
     * 
     * 实现说明：
     * - 将Kafka错误转换为对应的异常
     * - 调用raise(RuntimeException)处理异常
     * 
     * @param error 要传递给调用者的Kafka错误
     */
    public void raise(Errors error) {
        raise(error.exception());
    }

    /**
     * 触发成功事件，通知所有监听器
     * 
     * 实现说明：
     * - 获取成功的结果值
     * - 遍历并移除所有监听器
     * - 对每个监听器调用onSuccess方法
     * - 使用while循环确保处理所有监听器
     */
    private void fireSuccess() {
        T value = value();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null)
                break;
            listener.onSuccess(value);
        }
    }

    /**
     * 触发失败事件，通知所有监听器
     * 
     * 实现说明：
     * - 获取失败的异常对象
     * - 遍历并移除所有监听器
     * - 对每个监听器调用onFailure方法
     * - 使用while循环确保处理所有监听器
     */
    private void fireFailure() {
        RuntimeException exception = exception();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null)
                break;
            listener.onFailure(exception);
        }
    }

    /**
     * 添加一个在future完成时会被通知的监听器
     * 
     * 实现说明：
     * - 将监听器添加到并发队列中
     * - 如果future已经完成，立即触发对应的回调：
     *   - 失败状态：调用fireFailure()
     *   - 成功状态：调用fireSuccess()
     * - 支持在任何状态下添加监听器
     * 
     * @param listener 要添加的非空监听器
     */
    public void addListener(RequestFutureListener<T> listener) {
        this.listeners.add(listener);
        if (failed())
            fireFailure();
        else if (succeeded())
            fireSuccess();
    }

    /**
     * 将一种类型的请求future转换为另一种类型
     * 
     * 实现说明：
     * - 创建新的目标类型RequestFuture
     * - 添加监听器处理原始future的完成事件
     * - 使用适配器进行类型转换
     * - 支持链式异步操作
     * 
     * 应用场景：
     * - 在异步操作链中转换数据类型
     * - 支持复杂的异步处理流程
     * 
     * @param adapter 执行转换的适配器
     * @param <S> 转换后的future类型
     * @return 新的future实例
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        final RequestFuture<S> adapted = new RequestFuture<>();
        addListener(new RequestFutureListener<>() {
            @Override
            public void onSuccess(T value) {
                adapter.onSuccess(value, adapted);
            }

            @Override
            public void onFailure(RuntimeException e) {
                adapter.onFailure(e, adapted);
            }
        });
        return adapted;
    }

    /**
     * 将当前future的结果链接到另一个future
     * 
     * 实现说明：
     * - 添加监听器转发结果状态：
     *   - 成功时：将结果值传递给目标future
     *   - 失败时：将异常传递给目标future
     * - 实现future之间的结果传递
     * 
     * 应用场景：
     * - 构建异步操作链
     * - 实现请求结果的传递和组合
     * 
     * @param future 要链接到的目标future
     */
    public void chain(final RequestFuture<T> future) {
        addListener(new RequestFutureListener<>() {
            @Override
            public void onSuccess(T value) {
                future.complete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                future.raise(e);
            }
        });
    }

    /**
     * 创建一个已失败的RequestFuture
     * 
     * 实现说明：
     * - 创建新的future实例
     * - 立即将其标记为失败状态
     * - 设置指定的异常
     * 
     * @param e 失败原因的异常
     * @param <T> future的结果类型
     * @return 已失败的future实例
     */
    public static <T> RequestFuture<T> failure(RuntimeException e) {
        RequestFuture<T> future = new RequestFuture<>();
        future.raise(e);
        return future;
    }

    /**
     * 创建一个已成功完成的void类型RequestFuture
     * 
     * 实现说明：
     * - 创建新的Void类型future实例
     * - 立即将其标记为成功完成
     * - 结果值为null
     * 
     * @return 已成功完成的void类型future
     */
    public static RequestFuture<Void> voidSuccess() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        return future;
    }

    /**
     * 创建一个表示协调器不可用的失败future
     * 
     * 实现说明：
     * - 使用COORDINATOR_NOT_AVAILABLE错误创建失败的future
     * - 用于处理消费者组协调器不可用的情况
     * 
     * @param <T> future的结果类型
     * @return 包含协调器不可用错误的失败future
     */
    public static <T> RequestFuture<T> coordinatorNotAvailable() {
        return failure(Errors.COORDINATOR_NOT_AVAILABLE.exception());
    }

    /**
     * 创建一个表示没有可用代理的失败future
     * 
     * 实现说明：
     * - 使用NoAvailableBrokersException创建失败的future
     * - 用于处理Kafka集群中没有可用代理的情况
     * 
     * @param <T> future的结果类型
     * @return 包含无可用代理异常的失败future
     */
    public static <T> RequestFuture<T> noBrokersAvailable() {
        return failure(new NoAvailableBrokersException());
    }

    /**
     * 实现ConsumerNetworkClient.PollCondition接口的方法
     * 用于确定是否需要阻塞等待请求完成
     * 
     * 实现说明：
     * - 当future未完成时返回true，表示需要继续阻塞
     * - 当future完成时返回false，表示可以继续处理
     * 
     * @return 如果请求未完成返回true，否则返回false
     */
    @Override
    public boolean shouldBlock() {
        return !isDone();
    }
}
