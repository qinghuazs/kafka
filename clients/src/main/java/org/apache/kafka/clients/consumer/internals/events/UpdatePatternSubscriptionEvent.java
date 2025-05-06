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

/**
 * Application event which is triggered as part of the consumer poll loop to update the pattern subscription
 * if metadata changed.
 */
/**
 * 模式订阅更新事件类
 * 该事件作为消费者poll循环的一部分被触发
 * 当元数据发生变化时，用于更新基于模式的订阅信息
 */
public class UpdatePatternSubscriptionEvent extends CompletableApplicationEvent<Void> {

    /**
     * 构造函数，初始化模式订阅更新事件
     *
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public UpdatePatternSubscriptionEvent(final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为UPDATE_SUBSCRIPTION_METADATA和截止时间
        super(Type.UPDATE_SUBSCRIPTION_METADATA, deadlineMs);
    }
}
