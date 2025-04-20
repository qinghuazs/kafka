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
package org.apache.kafka.common;

import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一个灵活的Future实现，支持方法链式调用和其他异步编程模式。
 * 
 * <h3>与 {@code CompletionStage} 的关系</h3>
 * <p>可以通过调用 {@link #toCompletionStage()} 方法从 {@code KafkaFuture} 实例获取
 * {@code CompletionStage}。
 * 在将 {@link KafkaFuture#whenComplete(BiConsumer)} 或 {@link KafkaFuture#thenApply(BaseFunction)} 转换为
 * {@link CompletableFuture#whenComplete(java.util.function.BiConsumer)} 或
 * {@link CompletableFuture#thenApply(java.util.function.Function)} 时需要注意，返回的
 * {@code KafkaFuture} 在失败时会抛出 {@code ExecutionException}，而 {@code CompletionStage} 则会
 * 抛出 {@code CompletionException}。
 */
public abstract class KafkaFuture<T> implements Future<T> {
    /**
     * 一个函数式接口，接收A类型的参数并返回B类型的结果。
     * 用于在异步操作完成时对结果进行转换。
     */
    @FunctionalInterface
    public interface BaseFunction<A, B> {
        B apply(A a);
    }

    /**
     * 一个函数式接口，用于消费两个不同类型的对象。
     * 主要用于处理异步操作完成时的结果和可能的异常。
     */
    @FunctionalInterface
    public interface BiConsumer<A, B> {
        void accept(A a, B b);
    }

    /** 
     * 返回一个已经完成的KafkaFuture，其结果值为给定的value。
     * 这是一个工具方法，用于快速创建一个已完成状态的Future。
     */
    public static <U> KafkaFuture<U> completedFuture(U value) {
        KafkaFuture<U> future = new KafkaFutureImpl<>();
        future.complete(value);
        return future;
    }

    /** 
     * 返回一个新的KafkaFuture，该Future在所有给定的futures都完成时完成。
     * 如果任何一个future抛出异常，返回的future将返回该异常。
     * 如果多个futures抛出异常，将随机选择其中一个异常返回。
     */
    public static KafkaFuture<Void> allOf(KafkaFuture<?>... futures) {
        // 创建一个新的KafkaFutureImpl作为结果
        KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        CompletableFuture.allOf(Arrays.stream(futures)
                .map(kafkaFuture -> {
                    // 安全转换，因为KafkaFuture的唯一子类是KafkaFutureImpl，
                    // 其toCompletionStage()方法总是返回CompletableFuture
                    return (CompletableFuture<?>) kafkaFuture.toCompletionStage();
                })
                .toArray(CompletableFuture[]::new)).whenComplete((value, ex) -> {
                    if (ex == null) {
                        // 如果没有异常发生，正常完成结果Future
                        result.complete(value);
                    } else {
                        // 需要解包CompletableFuture.allOf()引入的CompletionException
                        result.completeExceptionally(ex.getCause());
                    }
                });

        return result;
    }

    /**
     * 获取一个与此KafkaFuture具有相同完成属性的CompletionStage。
     * 返回的实例将在此future完成时以相同的方式完成（具有相同的结果或异常）。
     *
     * 对返回的实例调用toCompletableFuture()将得到一个CompletableFuture，
     * 但在该CompletableFuture实例上调用完成方法（complete()和complete*()族中的其他方法
     * 以及obtrude*()族）将导致抛出UnsupportedOperationException。
     * 与"最小"CompletableFuture不同，CompletableFuture中未从CompletionStage继承的
     * get*()和其他方法将正常工作。
     *
     * 如果你想阻塞等待KafkaFuture完成，应该使用get()、get(long, TimeUnit)或
     * getNow(Object)，而不是调用.toCompletionStage().toCompletableFuture().get()等。
     *
     * @since Kafka 3.0
     */
    public abstract CompletionStage<T> toCompletionStage();

    /**
     * 返回一个新的KafkaFuture，当此future正常完成时，将使用此future的结果作为参数
     * 执行提供的函数。
     *
     * 该函数可能由调用thenApply的线程执行，也可能由完成future的线程执行。
     */
    public abstract <R> KafkaFuture<R> thenApply(BaseFunction<T, R> function);

    /**
     * 返回一个新的KafkaFuture，该Future具有与此future相同的结果或异常，
     * 并在此future完成时执行给定的操作。
     *
     * 当此future完成时，将使用此future的结果（如果没有则为null）和异常
     * （如果没有则为null）作为参数调用给定的操作。
     *
     * 返回的future在操作返回时完成。
     * 提供的操作不应抛出异常。但是，如果确实抛出异常，则适用以下规则：
     * 如果此future正常完成但提供的操作抛出异常，则返回的future将以该操作的异常异常完成。
     * 或者，如果此future异常完成且提供的操作抛出异常，则返回的future将以此future的异常完成。
     *
     * 该操作可能由调用whenComplete的线程执行，也可能由完成future的线程执行。
     *
     * @param action 要执行的操作
     * @return 新的future
     */
    public abstract KafkaFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

    /**
     * 如果尚未完成，则将get()和相关方法返回的值设置为给定值。
     */
    protected abstract boolean complete(T newValue);

    /**
     * 如果尚未完成，则导致get()和相关方法的调用抛出给定的异常。
     */
    protected abstract boolean completeExceptionally(Throwable newException);

    /**
     * 如果尚未完成，则使用CancellationException完成此future。
     * 尚未完成的依赖future也将异常完成，异常为由此CancellationException
     * 引起的CompletionException。
     */
    @Override
    public abstract boolean cancel(boolean mayInterruptIfRunning);

    /**
     * 如有必要，等待此future完成，然后返回其结果。
     */
    @Override
    public abstract T get() throws InterruptedException, ExecutionException;

    /**
     * 如有必要，最多等待指定的时间以使此future完成，然后返回其结果（如果可用）。
     */
    @Override
    public abstract T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
        TimeoutException;

    /**
     * 如果已完成，则返回结果值（或抛出遇到的任何异常），否则返回给定的valueIfAbsent。
     */
    public abstract T getNow(T valueIfAbsent) throws InterruptedException, ExecutionException;

    /**
     * 如果此CompletableFuture在正常完成之前被取消，则返回true。
     */
    @Override
    public abstract boolean isCancelled();

    /**
     * 如果此CompletableFuture以任何方式异常完成，则返回true。
     */
    public abstract boolean isCompletedExceptionally();

    /**
     * 如果以任何方式完成：正常完成、异常完成或通过取消完成，则返回true。
     */
    @Override
    public abstract boolean isDone();
}
