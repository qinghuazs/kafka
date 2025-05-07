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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 这是一个内部类，继承自CompletableFuture。其存在的主要目的是为了解决CompletableFuture暴露了complete()、
 * completeExceptionally()等方法而可能导致的安全问题。因为这些方法如果被用户代码直接调用，可能会错误地完成
 * 一个从Kafka API返回给客户端应用程序的KafkaFuture。
 * 
 * 设计考虑：
 * 1. 安全性：通过重写所有可能导致Future完成的公共方法，确保只有Kafka内部代码可以完成Future
 * 2. 封装性：提供kafkaComplete和kafkaCompleteExceptionally方法供内部使用
 * 3. 异常处理：所有外部调用完成方法的尝试都会抛出UnsupportedOperationException
 * 
 * 应用场景：
 * 1. 在Kafka的异步API操作中，返回给用户的Future需要防止被用户代码意外或错误地完成
 * 2. 在需要确保Future只能由Kafka内部代码完成的场景下使用
 * 
 * @param <T> Future值的类型参数
 */
public class KafkaCompletableFuture<T> extends CompletableFuture<T> {

    /**
     * 正常完成这个Future。这个方法仅供Kafka客户端内部使用，不应被用户代码调用。
     * 
     * 实现细节：
     * - 通过调用父类CompletableFuture的complete方法来完成Future
     * - 方法的可见性被限制为包级私有，防止外部代码访问
     * 
     * @param value Future的结果值
     * @return 如果这次调用导致CompletableFuture转换为完成状态则返回true，否则返回false
     */
    boolean kafkaComplete(T value) {
        return super.complete(value);
    }

    /**
     * 以异常方式完成这个Future。这个方法仅供Kafka客户端内部使用，不应被用户代码调用。
     * 
     * 实现细节：
     * - 通过调用父类CompletableFuture的completeExceptionally方法来完成Future
     * - 方法的可见性被限制为包级私有，防止外部代码访问
     * 
     * @param throwable 用于完成Future的异常对象
     * @return 如果这次调用导致CompletableFuture转换为完成状态则返回true，否则返回false
     */
    boolean kafkaCompleteExceptionally(Throwable throwable) {
        return super.completeExceptionally(throwable);
    }

    /**
     * 重写父类的complete方法，禁止外部代码直接完成Future
     * 
     * @param value 尝试设置的完成值
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public boolean complete(T value) {
        throw erroneousCompletionException();
    }

    /**
     * 重写父类的completeExceptionally方法，禁止外部代码直接以异常方式完成Future
     * 
     * @param ex 尝试设置的异常对象
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw erroneousCompletionException();
    }

    /**
     * 重写父类的obtrudeValue方法，禁止外部代码强制设置Future的值
     * 
     * @param value 尝试强制设置的值
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public void obtrudeValue(T value) {
        throw erroneousCompletionException();
    }

    /**
     * 重写父类的obtrudeException方法，禁止外部代码强制设置Future的异常
     * 
     * @param ex 尝试强制设置的异常
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public void obtrudeException(Throwable ex) {
        throw erroneousCompletionException();
    }

    /**
     * 重写父类的newIncompleteFuture方法，确保创建的新Future实例也是KafkaCompletableFuture类型
     * 
     * 实现细节：
     * - 返回一个新的KafkaCompletableFuture实例而不是普通的CompletableFuture
     * - 这确保了所有派生的Future操作都保持相同的安全限制
     * 
     * @param <U> 新Future的值类型
     * @return 新的未完成的KafkaCompletableFuture实例
     */
    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new KafkaCompletableFuture<>();
    }

    /**
     * 重写父类的completeAsync方法，禁止外部代码使用异步方式完成Future
     * 
     * @param supplier 尝试提供完成值的供应者
     * @param executor 尝试执行完成操作的执行器
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        throw erroneousCompletionException();
    }

    /**
     * 重写父类的completeAsync方法，禁止外部代码使用异步方式完成Future
     * 
     * @param supplier 尝试提供完成值的供应者
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        throw erroneousCompletionException();
    }

    /**
     * 重写父类的completeOnTimeout方法，禁止外部代码设置超时完成
     * 
     * @param value 尝试在超时时设置的值
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 当尝试调用此方法时总是抛出此异常
     */
    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        throw erroneousCompletionException();
    }

    /**
     * 创建一个UnsupportedOperationException异常实例，用于所有尝试完成Future的非法操作
     * 
     * 实现细节：
     * - 返回统一的异常信息，说明用户代码不应该完成来自Kafka客户端的Future
     * - 作为私有方法，只在类内部使用
     * 
     * @return 包含说明性消息的UnsupportedOperationException实例
     */
    private UnsupportedOperationException erroneousCompletionException() {
        return new UnsupportedOperationException("User code should not complete futures returned from Kafka clients");
    }
}
