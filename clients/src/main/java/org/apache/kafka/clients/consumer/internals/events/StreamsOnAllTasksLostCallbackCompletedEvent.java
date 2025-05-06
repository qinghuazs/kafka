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

import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Streams所有任务丢失回调完成事件类
 * 该类用于在Kafka Streams中所有任务丢失后的回调处理完成时触发
 * 继承自ApplicationEvent基类，表示这是一个应用层级的事件
 */
public class StreamsOnAllTasksLostCallbackCompletedEvent extends ApplicationEvent {

    /**
     * 用于异步处理回调完成的Future对象
     * 类型为CompletableFuture<Void>表示不需要返回值
     */
    private final CompletableFuture<Void> future;

    /**
     * 可选的Kafka异常对象
     * 用于存储回调处理过程中可能发生的异常
     */
    private final Optional<KafkaException> error;

    /**
     * 构造函数，初始化Streams所有任务丢失回调完成事件
     *
     * @param future 异步处理的Future对象
     * @param error 可选的Kafka异常对象
     */
    public StreamsOnAllTasksLostCallbackCompletedEvent(final CompletableFuture<Void> future,
                                                       final Optional<KafkaException> error) {
        // 调用父类构造函数，指定事件类型为STREAMS_ON_ALL_TASKS_LOST_CALLBACK_COMPLETED
        super(Type.STREAMS_ON_ALL_TASKS_LOST_CALLBACK_COMPLETED);
        // 使用Objects.requireNonNull确保future参数不为null
        this.future = Objects.requireNonNull(future);
        // 使用Objects.requireNonNull确保error参数不为null
        this.error = Objects.requireNonNull(error);
    }

    /**
     * 获取异步处理的Future对象
     *
     * @return 返回CompletableFuture对象
     */
    public CompletableFuture<Void> future() {
        return future;
    }

    /**
     * 获取可能发生的Kafka异常
     *
     * @return 返回Optional包装的KafkaException对象
     */
    public Optional<KafkaException> error() {
        return error;
    }

    /**
     * 重写toString方法的基础实现，添加future和error信息
     */
    @Override
    protected String toStringBase() {
        return super.toStringBase() +
            ", future=" + future +
            ", error=" + error;
    }
}
