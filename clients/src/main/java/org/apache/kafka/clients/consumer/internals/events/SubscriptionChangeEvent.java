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

import java.util.Optional;

/**
 * Application event indicating that the subscription state has changed, triggered when a user
 * calls the subscribe API. This will make the consumer join a consumer group if not part of it
 * yet, or just send the updated subscription to the broker if it's already a member of the group.
 */
/**
 * 订阅状态变更事件类
 * 当用户调用subscribe API时触发此应用程序事件，表示订阅状态发生了变化
 * 如果消费者还不是消费者组的成员，这将使消费者加入消费者组
 * 如果消费者已经是组的成员，则只向broker发送更新的订阅信息
 */
public abstract class SubscriptionChangeEvent extends CompletableApplicationEvent<Void> {

    /**
     * 消费者再平衡监听器
     * 用于在分区分配发生变化时通知用户
     * 使用Optional包装，表示监听器可能不存在
     */
    private final Optional<ConsumerRebalanceListener> listener;

    /**
     * 构造函数，初始化订阅状态变更事件
     *
     * @param type 事件类型
     * @param listener 可选的再平衡监听器
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public SubscriptionChangeEvent(final Type type, final Optional<ConsumerRebalanceListener> listener, final long deadlineMs) {
        // 调用父类构造函数，传入事件类型和截止时间
        super(type, deadlineMs);
        // 初始化再平衡监听器
        this.listener = listener;
    }

    /**
     * 获取再平衡监听器
     *
     * @return 返回Optional包装的ConsumerRebalanceListener对象
     */
    public Optional<ConsumerRebalanceListener> listener() {
        // 返回再平衡监听器
        return listener;
    }

    /**
     * 重写toString方法的基础实现，添加listener信息
     */
    @Override
    protected String toStringBase() {
        // 调用父类的toStringBase方法，并附加listener信息
        return super.toStringBase() + ", listener=" + listener;
    }
}
