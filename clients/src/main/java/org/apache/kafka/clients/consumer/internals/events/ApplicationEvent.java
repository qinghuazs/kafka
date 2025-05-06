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

import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ShareConsumerImpl;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * This is the abstract definition of the events created by the {@link AsyncKafkaConsumer} and
 * {@link ShareConsumerImpl} on the user's application thread.
 * 
 * 这是由AsyncKafkaConsumer和ShareConsumerImpl在用户的应用线程上创建的事件的抽象定义类。
 * 该类作为Kafka消费者客户端中所有应用事件的基类，定义了事件的基本属性和行为。
 */
public abstract class ApplicationEvent {

    /**
     * 事件类型枚举，定义了Kafka消费者可能触发的所有类型的事件
     * 主要包括以下几类：
     * 1. 提交相关：异步提交(COMMIT_ASYNC)、同步提交(COMMIT_SYNC)等
     * 2. 消费相关：拉取消息(POLL)、获取已提交偏移量(FETCH_COMMITTED_OFFSETS)等
     * 3. 元数据相关：主题元数据更新(TOPIC_METADATA)、订阅变更(TOPIC_SUBSCRIPTION_CHANGE)等
     * 4. 分区管理：分配变更(ASSIGNMENT_CHANGE)、暂停/恢复分区(PAUSE_PARTITIONS/RESUME_PARTITIONS)等
     * 5. 消费者组相关：离开组(LEAVE_GROUP_ON_CLOSE)、停止协调器(STOP_FIND_COORDINATOR_ON_CLOSE)等
     * 6. 共享消费相关：共享获取(SHARE_FETCH)、共享确认(SHARE_ACKNOWLEDGE_ASYNC/SYNC)等
     * 7. 流处理相关：任务分配/撤销/丢失回调完成事件等
     */
    public enum Type {
        COMMIT_ASYNC,              // 异步提交偏移量
        COMMIT_SYNC,               // 同步提交偏移量
        POLL,                      // 拉取消息
        FETCH_COMMITTED_OFFSETS,   // 获取已提交的偏移量
        NEW_TOPICS_METADATA_UPDATE,// 新主题元数据更新
        ASSIGNMENT_CHANGE,         // 分区分配变更
        LIST_OFFSETS,             // 列出偏移量
        CHECK_AND_UPDATE_POSITIONS,// 检查并更新位置
        RESET_OFFSET,             // 重置偏移量
        TOPIC_METADATA,           // 获取主题元数据
        ALL_TOPICS_METADATA,      // 获取所有主题元数据
        TOPIC_SUBSCRIPTION_CHANGE, // 主题订阅变更
        TOPIC_PATTERN_SUBSCRIPTION_CHANGE,    // 主题模式订阅变更
        TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE,// RE2J模式主题订阅变更
        UPDATE_SUBSCRIPTION_METADATA,         // 更新订阅元数据
        UNSUBSCRIBE,              // 取消订阅
        CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED,  // 消费者再平衡监听器回调完成
        COMMIT_ON_CLOSE,          // 关闭时提交
        CREATE_FETCH_REQUESTS,    // 创建获取请求
        LEAVE_GROUP_ON_CLOSE,     // 关闭时离开消费者组
        STOP_FIND_COORDINATOR_ON_CLOSE,      // 关闭时停止查找协调器
        PAUSE_PARTITIONS,         // 暂停分区
        RESUME_PARTITIONS,        // 恢复分区
        CURRENT_LAG,              // 当前消费延迟
        SHARE_FETCH,              // 共享消费获取
        SHARE_ACKNOWLEDGE_ASYNC,   // 异步共享确认
        SHARE_ACKNOWLEDGE_SYNC,    // 同步共享确认
        SHARE_SUBSCRIPTION_CHANGE, // 共享订阅变更
        SHARE_UNSUBSCRIBE,        // 共享取消订阅
        SHARE_ACKNOWLEDGE_ON_CLOSE,// 关闭时共享确认
        SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION,  // 共享确认提交回调注册
        SEEK_UNVALIDATED,         // 未验证的定位
        STREAMS_ON_TASKS_ASSIGNED_CALLBACK_COMPLETED,    // 流处理任务分配回调完成
        STREAMS_ON_TASKS_REVOKED_CALLBACK_COMPLETED,     // 流处理任务撤销回调完成
        STREAMS_ON_ALL_TASKS_LOST_CALLBACK_COMPLETED,    // 流处理所有任务丢失回调完成
    }

    /**
     * 事件类型，标识当前事件属于哪种操作类型
     * 这是一个不可变字段，在事件创建时被初始化
     */
    private final Type type;

    /**
     * This identifies a particular event. It is used to disambiguate events via {@link #hashCode()} and
     * {@link #equals(Object)} and can be used in log messages when debugging.
     * 
     * 事件的唯一标识符，用于通过hashCode()和equals()方法区分不同事件
     * 同时在调试时可用于日志消息中标识具体事件
     */
    private final Uuid id;

    /**
     * The time in milliseconds when this event was enqueued.
     * This field can be changed after the event is created, so it should not be used in hashCode or equals.
     * 
     * 事件入队列的时间戳（毫秒）
     * 该字段在事件创建后可以被修改，因此不应在hashCode或equals中使用
     */
    private long enqueuedMs;

    /**
     * 创建一个新的应用事件
     * @param type 事件类型，不能为null
     */
    protected ApplicationEvent(Type type) {
        // 确保事件类型不为null，否则抛出NullPointerException
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
        ApplicationEvent that = (ApplicationEvent) o;
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
