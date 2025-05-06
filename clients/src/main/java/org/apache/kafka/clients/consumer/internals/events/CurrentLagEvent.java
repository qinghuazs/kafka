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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * 当前消费延迟事件类，用于获取指定分区的消费延迟信息
 * 该事件继承自CompletableApplicationEvent，返回类型为OptionalLong，表示可选的延迟消息数量
 * 在Kafka消费者客户端中，用于监控消费进度，帮助识别潜在的消费积压问题
 */
public class CurrentLagEvent extends CompletableApplicationEvent<OptionalLong> {

    /**
     * 目标主题分区
     * 表示需要查询消费延迟的具体分区
     */
    private final TopicPartition partition;

    /**
     * 事务隔离级别
     * 用于指定读取消息的隔离级别，影响消费者可以看到的消息范围
     */
    private final IsolationLevel isolationLevel;

    /**
     * 创建一个新的当前消费延迟事件
     *
     * @param partition 目标主题分区，不能为null
     * @param isolationLevel 事务隔离级别，不能为null
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public CurrentLagEvent(final TopicPartition partition, final IsolationLevel isolationLevel, final long deadlineMs) {
        // 调用父类构造函数，设置事件类型为CURRENT_LAG和处理截止时间
        super(Type.CURRENT_LAG, deadlineMs);
        // 确保partition参数不为null，并赋值
        this.partition = Objects.requireNonNull(partition);
        // 确保isolationLevel参数不为null，并赋值
        this.isolationLevel = Objects.requireNonNull(isolationLevel);
    }

    /**
     * 获取目标主题分区
     *
     * @return 返回当前事件关联的主题分区
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * 获取事务隔离级别
     *
     * @return 返回当前事件使用的事务隔离级别
     */
    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", partition=" + partition + ", isolationLevel=" + isolationLevel;
    }
}
