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
import org.apache.kafka.clients.consumer.SubscriptionPattern;

import java.util.Optional;

/**
 * Application event indicating triggered by a call to the subscribe API
 * providing a {@link SubscriptionPattern} (RE2J-compatible pattern).
 * This will make the consumer send the updated subscription to the
 * broker on the next poll, joining the group if it is not already part of it.
 */
/**
 * RE2J模式主题订阅变更事件类
 * 当用户调用subscribe API并提供RE2J兼容的订阅模式（SubscriptionPattern）时触发此应用程序事件
 * 这将使消费者在下次poll时向broker发送更新的订阅信息
 * 如果消费者还不是消费者组的成员，则会加入该组
 */
public class TopicRe2JPatternSubscriptionChangeEvent extends SubscriptionChangeEvent {
    
    /**
     * RE2J兼容的主题订阅模式
     * 使用SubscriptionPattern类型来定义主题匹配规则
     * 该字段为final，确保模式的不可变性
     */
    private final SubscriptionPattern pattern;

    /**
     * 构造函数，初始化RE2J模式主题订阅变更事件
     *
     * @param pattern RE2J兼容的订阅模式
     * @param listener 可选的再平衡监听器
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public TopicRe2JPatternSubscriptionChangeEvent(final SubscriptionPattern pattern,
                                                   final Optional<ConsumerRebalanceListener> listener,
                                                   final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE，并传入监听器和截止时间
        super(Type.TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE, listener, deadlineMs);
        // 初始化RE2J订阅模式
        this.pattern = pattern;
    }

    /**
     * 获取RE2J订阅模式
     *
     * @return 返回RE2J兼容的SubscriptionPattern对象
     */
    public SubscriptionPattern pattern() {
        // 返回RE2J订阅模式
        return pattern;
    }

    /**
     * 重写toString方法的基础实现，添加subscriptionPattern信息
     */
    @Override
    public String toStringBase() {
        // 调用父类的toStringBase方法，并附加subscriptionPattern信息
        return super.toStringBase() + ", subscriptionPattern=" + pattern;
    }
}
