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

import java.util.Collection;
import java.util.Set;

/**
 * Application event indicating that the subscription state has changed, triggered when a user
 * calls the subscribe API. This will make the consumer join a share group if not part of it
 * yet, or just send the updated subscription to the broker if it's already a member of the group.
 */
/**
 * 订阅状态变更事件类
 * 当用户调用subscribe API时触发此应用程序事件，表示订阅状态发生了变化
 * 如果消费者还不是共享组的成员，这将使消费者加入共享组
 * 如果消费者已经是组的成员，则只向broker发送更新的订阅信息
 */
public class ShareSubscriptionChangeEvent extends CompletableApplicationEvent<Void> {

    /**
     * 存储订阅的主题集合
     * 使用不可变Set确保线程安全
     * 包含所有要订阅的主题名称
     */
    private final Set<String> topics;

    /**
     * 构造函数，初始化订阅状态变更事件
     * 使用Long.MAX_VALUE作为截止时间，表示这个事件不会超时
     *
     * @param topics 要订阅的主题集合
     */
    public ShareSubscriptionChangeEvent(final Collection<String> topics) {
        // 调用父类构造函数，指定事件类型为SHARE_SUBSCRIPTION_CHANGE，设置最大截止时间
        super(Type.SHARE_SUBSCRIPTION_CHANGE, Long.MAX_VALUE);
        // 创建topics集合的不可变副本以确保线程安全
        this.topics = Set.copyOf(topics);
    }

    /**
     * 获取订阅的主题集合
     *
     * @return 返回不可变的主题集合
     */
    public Set<String> topics() {
        // 返回主题集合
        return topics;
    }

    /**
     * 重写toString方法的基础实现，添加topics信息
     */
    @Override
    protected String toStringBase() {
        // 调用父类的toStringBase方法，并附加topics信息
        return super.toStringBase() + ", topics=" + topics;
    }
}
