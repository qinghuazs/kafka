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
import java.util.Set;

/**
 * Application event indicating that a user calls the subscribe API for new topics.
 * This will make the consumer join a consumer group if not part of it yet,
 * or just send the updated subscription to the broker on the next poll.
 */
/**
 * 主题订阅变更事件类
 * 当用户调用subscribe API订阅新主题时触发此应用程序事件
 * 如果消费者还不是消费者组的成员，这将使消费者加入消费者组
 * 如果消费者已经是组的成员，则在下次poll时向broker发送更新的订阅信息
 */
public class TopicSubscriptionChangeEvent extends SubscriptionChangeEvent {
    
    /**
     * 存储要订阅的主题集合
     * 使用Set<String>类型来存储主题名称列表
     * 该字段为final，确保主题集合的不可变性
     */
    private final Set<String> topics;

    /**
     * 构造函数，初始化主题订阅变更事件
     *
     * @param topics 要订阅的主题集合
     * @param listener 可选的再平衡监听器
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public TopicSubscriptionChangeEvent(final Set<String> topics, final Optional<ConsumerRebalanceListener> listener, final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为TOPIC_SUBSCRIPTION_CHANGE，并传入监听器和截止时间
        super(Type.TOPIC_SUBSCRIPTION_CHANGE, listener, deadlineMs);
        // 初始化主题集合
        this.topics = topics;
    }

    /**
     * 获取订阅的主题集合
     *
     * @return 返回主题名称的Set集合
     */
    public Set<String> topics() {
        // 返回主题集合
        return topics;
    }

    /**
     * 重写toString方法的基础实现，添加topics信息
     */
    @Override
    public String toStringBase() {
        // 调用父类的toStringBase方法，并附加topics信息
        return super.toStringBase() + ", topics=" + topics;
    }
}
