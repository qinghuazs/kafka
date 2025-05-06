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

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;

/**
 * 恢复分区消费事件类
 * 该事件用于通知Kafka消费者恢复指定分区的消费操作
 * 继承自CompletableApplicationEvent，支持异步完成通知机制
 */
public class ResumePartitionsEvent extends CompletableApplicationEvent<Void> {

    /**
     * 需要恢复消费的主题分区集合
     * 使用不可变集合确保线程安全，防止外部修改
     */
    private final Collection<TopicPartition> partitions;

    /**
     * 构造函数，创建一个新的恢复分区消费事件
     * 
     * 实现细节：
     * 1. 调用父类构造函数，设置事件类型为RESUME_PARTITIONS
     * 2. 使用Collections.unmodifiableCollection创建不可变分区集合，确保线程安全
     *
     * @param partitions 需要恢复消费的主题分区集合
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public ResumePartitionsEvent(final Collection<TopicPartition> partitions, final long deadlineMs) {
        super(Type.RESUME_PARTITIONS, deadlineMs);
        this.partitions = Collections.unmodifiableCollection(partitions);
    }

    /**
     * 获取需要恢复消费的主题分区集合
     * 
     * @return 不可变的主题分区集合
     */
    public Collection<TopicPartition> partitions() {
        return partitions;
    }

    /**
     * 重写toString方法的基础部分
     * 添加分区信息到字符串表示中
     * 
     * @return 包含事件详细信息的字符串
     */
    @Override
    public String toStringBase() {
        return super.toStringBase() + ", partitions=" + partitions;
    }
}
