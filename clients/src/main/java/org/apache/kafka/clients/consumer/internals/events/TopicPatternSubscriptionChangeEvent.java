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
import java.util.regex.Pattern;

/**
 * Application event indicating that a user calls the subscribe API for a new pattern.
 * This will make the consumer join a consumer group if not part of it yet,
 * or just send the updated subscription to the broker on the next poll.
 */
/**
 * 主题模式订阅变更事件类
 * 当用户调用subscribe API使用新的模式进行订阅时触发此应用程序事件
 * 如果消费者还不是消费者组的成员，这将使消费者加入消费者组
 * 如果消费者已经是组的成员，则在下次poll时向broker发送更新的订阅信息
 */
public class TopicPatternSubscriptionChangeEvent extends SubscriptionChangeEvent {
    
    /**
     * 主题匹配模式
     * 使用正则表达式模式来匹配要订阅的主题
     * 该字段为final，确保模式的不可变性
     */
    private final Pattern pattern;

    /**
     * 构造函数，初始化主题模式订阅变更事件
     *
     * @param pattern 主题匹配模式
     * @param listener 可选的再平衡监听器
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public TopicPatternSubscriptionChangeEvent(final Pattern pattern, final Optional<ConsumerRebalanceListener> listener, final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为TOPIC_PATTERN_SUBSCRIPTION_CHANGE，并传入监听器和截止时间
        super(Type.TOPIC_PATTERN_SUBSCRIPTION_CHANGE, listener, deadlineMs);
        // 初始化主题匹配模式
        this.pattern = pattern;
    }

    /**
     * 获取主题匹配模式
     *
     * @return 返回用于匹配主题的Pattern对象
     */
    public Pattern pattern() {
        // 返回主题匹配模式
        return pattern;
    }

    /**
     * 重写toString方法的基础实现，添加pattern信息
     */
    @Override
    public String toStringBase() {
        // 调用父类的toStringBase方法，并附加pattern信息
        return super.toStringBase() + ", pattern=" + pattern;
    }
}
