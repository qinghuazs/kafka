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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName;
import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 该事件表示应用线程已经执行了 {@link ConsumerRebalanceListener} 回调。
 * 如果回调执行过程中抛出了错误，该错误会被包含在事件中，以便其他事件监听器可以获知。
 *
 * 应用场景：
 * 1. 在消费者组再平衡过程中，当分区分配发生变化时，ConsumerRebalanceListener会被调用
 * 2. 该事件用于通知其他组件回调执行的结果，包括是否成功完成以及可能发生的错误
 * 3. 支持异步操作的完成状态跟踪
 */
public class ConsumerRebalanceListenerCallbackCompletedEvent extends ApplicationEvent {

    /**
     * 表示被执行的ConsumerRebalanceListener回调方法的名称
     * 可能是onPartitionsAssigned或onPartitionsRevoked
     */
    private final ConsumerRebalanceListenerMethodName methodName;

    /**
     * 用于跟踪回调方法执行的异步操作的完成状态
     * 当回调方法执行完成时，该Future会被完成
     */
    private final CompletableFuture<Void> future;

    /**
     * 存储回调执行过程中可能发生的Kafka异常
     * 如果执行成功则为空，发生错误时包含具体异常信息
     */
    private final Optional<KafkaException> error;

    /**
     * 创建一个新的ConsumerRebalanceListenerCallbackCompletedEvent实例
     *
     * @param methodName 被执行的回调方法名称，不能为null
     * @param future 异步操作的Future对象，不能为null
     * @param error 可能发生的异常信息，不能为null（但可以是空Optional）
     */
    public ConsumerRebalanceListenerCallbackCompletedEvent(final ConsumerRebalanceListenerMethodName methodName,
                                                           final CompletableFuture<Void> future,
                                                           final Optional<KafkaException> error) {
        // 调用父类构造函数，设置事件类型为CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED
        super(Type.CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED);
        // 确保所有参数都不为null，否则抛出NullPointerException
        this.methodName = Objects.requireNonNull(methodName);
        this.future = Objects.requireNonNull(future);
        this.error = Objects.requireNonNull(error);
    }

    /**
     * 获取被执行的回调方法名称
     * @return 回调方法名称
     */
    public ConsumerRebalanceListenerMethodName methodName() {
        return methodName;
    }

    /**
     * 获取异步操作的Future对象
     * @return 用于跟踪回调执行状态的Future
     */
    public CompletableFuture<Void> future() {
        return future;
    }

    /**
     * 获取回调执行过程中可能发生的异常
     * @return 包含异常信息的Optional对象
     */
    public Optional<KafkaException> error() {
        return error;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", methodName=" + methodName +
                ", future=" + future +
                ", error=" + error;
    }
}
