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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * 分区偏移量重置事件类。
 * 该事件用于根据指定的策略重置分区的偏移量位置。实际的重置操作将在下一次通过
 * {@link KafkaConsumer#poll(Duration)} 或 {@link KafkaConsumer#position(TopicPartition)} 更新获取位置时执行。
 * 这个机制用于执行 {@link AsyncKafkaConsumer#seekToBeginning(Collection)} 和 
 * {@link AsyncKafkaConsumer#seekToEnd(Collection)} 操作。
 */
public class ResetOffsetEvent extends CompletableApplicationEvent<Void> {

    /**
     * 需要重置偏移量的主题分区集合。
     * 使用final修饰确保引用不可变，通过Collections.unmodifiableCollection确保集合内容不可修改，
     * 这样的设计保证了事件处理过程中分区集合的稳定性和线程安全性。
     */
    private final Collection<TopicPartition> topicPartitions;

    /**
     * 偏移量重置策略。
     * 定义了如何重置分区的偏移量，可以是移动到分区起始位置或结束位置。
     * 使用final修饰并通过Objects.requireNonNull确保策略不为空，保证重置操作的可靠性。
     */
    private final AutoOffsetResetStrategy offsetResetStrategy;

    /**
     * 创建一个分区偏移量重置事件实例。
     * 
     * 实现细节：
     * 1. 调用父类构造器，设置事件类型为RESET_OFFSET和处理截止时间
     * 2. 将分区集合转换为不可修改集合，确保线程安全
     * 3. 确保重置策略不为空，防止空指针异常
     *
     * @param topicPartitions 需要重置偏移量的分区集合
     * @param offsetResetStrategy 偏移量重置策略
     * @param deadline 事件处理的截止时间戳（毫秒）
     */
    public ResetOffsetEvent(Collection<TopicPartition> topicPartitions, AutoOffsetResetStrategy offsetResetStrategy, long deadline) {
        super(Type.RESET_OFFSET, deadline);
        this.topicPartitions = Collections.unmodifiableCollection(topicPartitions);
        this.offsetResetStrategy = Objects.requireNonNull(offsetResetStrategy);
    }

    /**
     * 获取需要重置偏移量的分区集合
     * 
     * @return 不可修改的分区集合
     */
    public Collection<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    /**
     * 获取偏移量重置策略
     * 
     * @return 当前设置的偏移量重置策略
     */
    public AutoOffsetResetStrategy offsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", topicPartitions=" + topicPartitions + ", offsetStrategy=" + offsetResetStrategy;
    }
}
