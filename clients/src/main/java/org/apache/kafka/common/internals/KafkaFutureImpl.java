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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一个灵活的Future实现，支持方法链式调用和其他异步编程模式。
 * 
 * 设计考虑：
 * 1. 封装性：通过组合KafkaCompletableFuture来实现功能，而不是直接继承CompletableFuture
 * 2. 安全性：通过isDependant标记来区分是否为依赖Future，影响异常处理行为
 * 3. 异常处理：保持与KafkaFuture API的一致性，同时处理与CompletableFuture的异常处理差异
 * 
 * 应用场景：
 * 1. Kafka异步API操作的返回值
 * 2. 支持链式调用的异步操作流程
 * 3. 需要严格控制Future完成方式的场景
 */
public class KafkaFutureImpl<T> extends KafkaFuture<T> {

    /**
     * 底层的KafkaCompletableFuture实例，用于实际的异步操作处理
     * 选择组合而不是继承CompletableFuture，以便更好地控制Future的完成方式
     */
    private final KafkaCompletableFuture<T> completableFuture;

    /**
     * 标记这个Future是否是一个依赖Future
     * true表示这个Future是由其他Future通过thenApply等方法派生出来的
     * 这会影响某些方法（如isCancelled）的行为
     */
    private final boolean isDependant;

    /**
     * 创建一个新的非依赖KafkaFutureImpl实例
     * 实现细节：
     * - 设置isDependant为false表示这是一个独立的Future
     * - 创建新的KafkaCompletableFuture作为底层实现
     */
    public KafkaFutureImpl() {
        this(false, new KafkaCompletableFuture<>());
    }

    /**
     * 创建一个新的KafkaFutureImpl实例
     * 实现细节：
     * - 用于内部创建依赖Future时使用
     * - 直接使用提供的KafkaCompletableFuture实例
     */
    private KafkaFutureImpl(boolean isDependant, KafkaCompletableFuture<T> completableFuture) {
        this.isDependant = isDependant;
        this.completableFuture = completableFuture;
    }

    /**
     * 将此KafkaFuture转换为CompletionStage
     * 实现细节：
     * - 直接返回底层的completableFuture，因为它已经实现了CompletionStage接口
     */
    @Override
    public CompletionStage<T> toCompletionStage() {
        return completableFuture;
    }

    /**
     * 返回一个新的KafkaFuture，当此Future正常完成时，将使用此Future的结果作为参数执行提供的函数
     * 
     * 实现细节：
     * 1. 使用completableFuture.thenApply创建转换后的Future
     * 2. 包装异常处理逻辑以保持与KafkaFuture的行为一致
     * 3. 将结果转换为KafkaFutureImpl并标记为依赖Future
     */
    @Override
    public <R> KafkaFuture<R> thenApply(BaseFunction<T, R> function) {
        CompletableFuture<R> appliedFuture = completableFuture.thenApply(value -> {
            try {
                // 执行用户提供的转换函数
                return function.apply(value);
            } catch (Throwable t) {
                if (t instanceof CompletionException) {
                    // 为了保持KafkaFuture的异常处理行为，需要额外包装一层CompletionException
                    throw new CompletionException(t);
                } else {
                    throw t;
                }
            }
        });
        // 创建新的依赖Future并确保它是KafkaCompletableFuture类型
        return new KafkaFutureImpl<>(true, toKafkaCompletableFuture(appliedFuture));
    }

    /**
     * 将CompletableFuture转换为KafkaCompletableFuture
     * 
     * 实现细节：
     * 1. 如果已经是KafkaCompletableFuture则直接返回
     * 2. 否则创建新的KafkaCompletableFuture并设置完成回调
     * 3. 在回调中正确处理正常完成和异常完成的情况
     */
    private static <U> KafkaCompletableFuture<U> toKafkaCompletableFuture(CompletableFuture<U> completableFuture) {
        if (completableFuture instanceof KafkaCompletableFuture) {
            return (KafkaCompletableFuture<U>) completableFuture;
        } else {
            final KafkaCompletableFuture<U> result = new KafkaCompletableFuture<>();
            completableFuture.whenComplete((x, y) -> {
                if (y != null) {
                    // 异常完成的情况
                    result.kafkaCompleteExceptionally(y);
                } else {
                    // 正常完成的情况
                    result.kafkaComplete(x);
                }
            });
            return result;
        }
    }

    /**
     * 返回一个新的KafkaFuture，当此Future完成时执行给定的操作
     * 
     * 实现细节：
     * 1. 使用completableFuture.whenComplete注册完成回调
     * 2. 在回调中执行用户提供的BiConsumer
     * 3. 处理回调中可能抛出的异常
     */
    @Override
    public KafkaFuture<T> whenComplete(final BiConsumer<? super T, ? super Throwable> biConsumer) {
        CompletableFuture<T> tCompletableFuture = completableFuture.whenComplete((java.util.function.BiConsumer<? super T, ? super Throwable>) (a, b) -> {
            try {
                // 执行用户提供的完成回调
                biConsumer.accept(a, b);
            } catch (Throwable t) {
                if (t instanceof CompletionException) {
                    throw new CompletionException(t);
                } else {
                    throw t;
                }
            }
        });
        // 创建新的依赖Future
        return new KafkaFutureImpl<>(true, toKafkaCompletableFuture(tCompletableFuture));
    }

    /**
     * 使用给定的值完成此Future
     * 
     * 实现细节：
     * - 通过底层的KafkaCompletableFuture完成操作
     */
    @Override
    public boolean complete(T newValue) {
        return completableFuture.kafkaComplete(newValue);
    }

    /**
     * 使用给定的异常完成此Future
     * 
     * 实现细节：
     * - 处理CompletionException的特殊情况，确保异常链的完整性
     * - 通过底层的KafkaCompletableFuture完成操作
     */
    @Override
    public boolean completeExceptionally(Throwable newException) {
        // 为了避免丢失异常链中的第一个CompletionException，需要额外包装
        return completableFuture.kafkaCompleteExceptionally(
                newException instanceof CompletionException ? new CompletionException(newException) : newException);
    }

    /**
     * 如果尚未完成，则使用CancellationException完成此Future
     * 所有未完成的依赖Future也将异常完成，异常为由此CancellationException引起的CompletionException
     * 
     * 实现细节：
     * - 直接委托给底层的KafkaCompletableFuture处理取消操作
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return completableFuture.cancel(mayInterruptIfRunning);
    }

    /**
     * 处理KafkaFuture的历史API和CompletableFuture API之间的差异：
     * 1. CompletableFuture#get()不会将CancellationException包装在ExecutionException中（KafkaFuture也不会）
     * 2. CompletableFuture#get()总是将CompletionException的cause包装在ExecutionException中
     *    （而KafkaFuture不会这样做）
     *
     * KafkaFuture的语义是：所有异常完成（通过completeExceptionally()或来自依赖Future的异常）
     * 都表现为ExecutionException，这可以通过get()和getNow()观察到。
     * 
     * 实现细节：
     * - 检查异常是否为CancellationException
     * - 如果是则直接抛出，保持与KafkaFuture的行为一致
     */
    private void maybeThrowCancellationException(Throwable cause) {
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        }
    }

    /**
     * 等待Future完成并返回结果
     * 
     * 实现细节：
     * 1. 调用底层completableFuture的get方法
     * 2. 处理Java 23中CancellationException的特殊情况
     * 3. 保持与KafkaFuture的异常处理行为一致
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return completableFuture.get();
            // 在Java 23中，当CompletableFuture被取消时，get()会抛出一个包装了CancellationException的CancellationException
            // 因此我们需要解包它以保持KafkaFuture的行为一致
            // 参见 https://bugs.openjdk.org/browse/JDK-8331987
        } catch (ExecutionException | CancellationException e) {
            maybeThrowCancellationException(e.getCause());
            throw e;
        }
    }

    /**
     * 等待Future在指定时间内完成并返回结果
     * 
     * 实现细节：
     * 1. 使用超时参数调用底层completableFuture的get方法
     * 2. 处理Java 23中CancellationException的特殊情况
     * 3. 保持与KafkaFuture的异常处理行为一致
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        try {
            return completableFuture.get(timeout, unit);
            // 在Java 23中，当CompletableFuture被取消时，get()会抛出一个包装了CancellationException的CancellationException
            // 因此我们需要解包它以保持KafkaFuture的行为一致
            // 参见 https://bugs.openjdk.org/browse/JDK-8331987
        } catch (ExecutionException | CancellationException e) {
            maybeThrowCancellationException(e.getCause());
            throw e;
        }
    }

    /**
     * 如果Future已完成则返回结果值（或抛出遇到的异常），否则返回给定的默认值
     * 
     * 实现细节：
     * 1. 尝试从底层completableFuture获取当前值
     * 2. 处理各种异常情况，特别是Java 23中的CancellationException
     * 3. 确保异常处理符合KafkaFuture的API约定
     */
    @Override
    public T getNow(T valueIfAbsent) throws ExecutionException {
        try {
            return completableFuture.getNow(valueIfAbsent);
        } catch (CancellationException e) {
            // 在Java 23中，当CompletableFuture被取消时，getNow()会抛出一个包装了CancellationException的CancellationException
            // 而在Java 23之前的版本中，它会直接抛出CompletionException
            // 参见 https://bugs.openjdk.org/browse/JDK-8331987
            if (e.getCause() instanceof CancellationException) {
                throw (CancellationException) e.getCause();
            } else {
                throw e;
            }
        } catch (CompletionException e) {
            maybeThrowCancellationException(e.getCause());
            // 注意，与CompletableFuture#get()抛出ExecutionException不同，CompletableFuture#getNow()
            // 抛出CompletionException，因此需要重新包装以符合KafkaFuture API的要求
            throw new ExecutionException(e.getCause());
        }
    }

    /**
     * 检查此Future是否在正常完成前被取消
     * 
     * 实现细节：
     * 1. 对于依赖Future，需要特殊处理以保持历史行为
     * 2. 检查异常类型来判断是否被取消
     * 3. 非依赖Future直接使用底层completableFuture的isCancelled方法
     */
    @Override
    public boolean isCancelled() {
        if (isDependant) {
            // 对于依赖Future，直接返回CompletableFuture.isCancelled()会破坏历史的KafkaFuture行为
            // 因为CompletableFuture#isCancelled()只检查异常是否为CancellationException
            // 而实际上它会是一个包装了CancellationException的CompletionException
            try {
                completableFuture.getNow(null);
                return false;
            } catch (Exception e) {
                return e instanceof CompletionException
                        && e.getCause() instanceof CancellationException;
            }
        } else {
            return completableFuture.isCancelled();
        }
    }

    /**
     * 检查此Future是否以任何方式异常完成
     * 
     * 实现细节：
     * - 直接委托给底层completableFuture的isCompletedExceptionally方法
     */
    @Override
    public boolean isCompletedExceptionally() {
        return completableFuture.isCompletedExceptionally();
    }

    /**
     * 检查此Future是否以任何方式完成：正常完成、异常完成或通过取消完成
     * 
     * 实现细节：
     * - 直接委托给底层completableFuture的isDone方法
     */
    @Override
    public boolean isDone() {
        return completableFuture.isDone();
    }

    @Override
    public String toString() {
        T value = null;
        Throwable exception = null;
        try {
            value = completableFuture.getNow(null);
        } catch (CancellationException e) {
            // In Java 23, When a CompletableFuture is cancelled, getNow() will throw a CancellationException wrapping a 
            // CancellationException. whereas in Java < 23, it throws a CompletionException directly.
            // see https://bugs.openjdk.org/browse/JDK-8331987
            if (e.getCause() instanceof CancellationException) {
                exception = e.getCause();
            } else { 
                exception = e;
            }
        } catch (CompletionException e) {
            exception = e.getCause();
        } catch (Exception e) {
            exception = e;
        }
        return String.format("KafkaFuture{value=%s,exception=%s,done=%b}", value, exception, exception != null || value != null);
    }
}
