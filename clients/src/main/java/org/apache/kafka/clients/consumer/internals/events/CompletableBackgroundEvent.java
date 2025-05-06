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

import java.util.concurrent.CompletableFuture;

/**
 * 可完成的后台事件基类，用于处理需要异步完成的后台任务。
 * 该类继承自BackgroundEvent并实现CompletableEvent接口，提供了异步操作的完成状态跟踪和超时控制机制。
 *
 * 应用场景：
 * 1. 处理Kafka消费者客户端中的后台异步操作，如心跳检测、组成员管理等
 * 2. 支持后台任务的结果获取和超时控制
 * 3. 提供统一的事件完成状态跟踪机制
 *
 * 设计考虑：
 * 1. 使用CompletableFuture支持异步操作的完成状态跟踪
 * 2. 通过deadlineMs控制操作超时，避免事件长时间未完成导致资源泄露
 * 3. 提供抽象类实现，便于不同类型的后台事件扩展
 *
 * @param <T> 事件完成时的返回值类型
 */
public abstract class CompletableBackgroundEvent<T> extends BackgroundEvent implements CompletableEvent<T> {

    /**
     * 用于跟踪事件完成状态的Future对象
     * 当事件完成时，可以通过该对象获取结果或异常信息
     */
    private final CompletableFuture<T> future;

    /**
     * 事件的截止时间（毫秒），表示事件必须在该时间点之前完成
     * 注意：这是一个绝对时间点，而不是超时时间间隔
     */
    private final long deadlineMs;

    /**
     * 构造函数，初始化事件类型和截止时间
     * 
     * 实现细节：
     * 1. 调用父类构造函数设置事件类型
     * 2. 创建新的CompletableFuture对象用于状态跟踪
     * 3. 设置事件截止时间
     *
     * @param type 事件类型，用于标识不同种类的后台事件
     * @param deadlineMs 事件的绝对截止时间（毫秒）
     */
    protected CompletableBackgroundEvent(final Type type, final long deadlineMs) {
        super(type);
        this.future = new CompletableFuture<>();
        this.deadlineMs = deadlineMs;
    }

    /**
     * 获取与事件关联的CompletableFuture对象
     * 
     * 实现细节：
     * 返回在构造函数中创建的future对象，用于跟踪事件的完成状态
     *
     * @return 用于跟踪事件完成状态的CompletableFuture对象
     */
    @Override
    public CompletableFuture<T> future() {
        return future;
    }

    /**
     * 获取事件的截止时间
     * 
     * 实现细节：
     * 返回在构造函数中设置的deadlineMs值，表示事件的绝对截止时间
     *
     * @return 事件的绝对截止时间（毫秒）
     */
    @Override
    public long deadlineMs() {
        return deadlineMs;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", future=" + future + ", deadlineMs=" + deadlineMs;
    }
}
