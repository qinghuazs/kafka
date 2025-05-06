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
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * 这是由{@link ConsumerNetworkThread 网络线程}创建的事件的抽象定义类。
 * 该类作为Kafka消费者客户端中所有后台事件的基类，定义了事件的基本属性和行为。
 * 后台事件主要用于处理网络线程和应用线程之间的异步通信，包括错误处理、消费者再平衡、
 * 共享确认提交以及流处理任务的状态变更等场景。
 */
public abstract class BackgroundEvent {

    /**
     * 后台事件类型枚举，定义了所有可能的事件类型：
     * - ERROR: 错误事件，用于传递网络线程中发生的错误
     * - CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED: 需要触发消费者再平衡监听器回调
     * - SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK: 共享确认提交回调
     * - STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED: 需要触发流处理任务分配回调
     * - STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED: 需要触发流处理任务撤销回调
     * - STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED: 需要触发流处理所有任务丢失回调
     */
    public enum Type {
        ERROR,
        CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED,
        SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK,
        STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED,
        STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED,
        STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED
    }

    /**
     * 事件类型，标识当前事件属于哪种操作类型
     * 这是一个不可变字段，在事件创建时被初始化
     */
    private final Type type;

    /**
     * 事件的唯一标识符
     * 用于通过{@link #hashCode()}和{@link #equals(Object)}方法区分不同事件
     * 同时在调试时可用于日志消息中标识具体事件
     */
    private final Uuid id;

    /**
     * 事件入队列的时间戳（毫秒）
     * 该字段在事件创建后可以被修改，因此不应在hashCode或equals中使用
     * 主要用于跟踪事件在队列中的停留时间，有助于监控和性能分析
     */
    private long enqueuedMs;

    /**
     * 创建一个新的后台事件
     * @param type 事件类型，不能为null，否则会抛出NullPointerException
     */
    protected BackgroundEvent(Type type) {
        // 确保事件类型不为null
        this.type = Objects.requireNonNull(type);
        // 生成全局唯一的事件ID
        this.id = Uuid.randomUuid();
    }

    /**
     * 获取事件类型
     * @return 当前事件的类型
     */
    public Type type() {
        return type;
    }

    /**
     * 获取事件的唯一标识符
     * @return 事件的UUID
     */
    public Uuid id() {
        return id;
    }

    /**
     * 设置事件入队列的时间戳
     * @param enqueuedMs 入队列时间（毫秒）
     */
    public void setEnqueuedMs(long enqueuedMs) {
        this.enqueuedMs = enqueuedMs;
    }

    /**
     * 获取事件入队列的时间戳
     * @return 入队列时间（毫秒）
     */
    public long enqueuedMs() {
        return enqueuedMs;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackgroundEvent that = (BackgroundEvent) o;
        return type == that.type && id.equals(that.id);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, id);
    }

    protected String toStringBase() {
        return "type=" + type + ", id=" + id + ", enqueuedMs=" + enqueuedMs;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "{" + toStringBase() + "}";
    }
}
