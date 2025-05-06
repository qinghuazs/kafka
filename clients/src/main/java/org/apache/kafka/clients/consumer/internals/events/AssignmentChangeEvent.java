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
import java.util.Set;

public class AssignmentChangeEvent extends CompletableApplicationEvent<Void> {

    /**
     * 事件创建时的当前时间戳（毫秒）
     * 用于追踪事件的创建时间，便于分析分区分配的时序性
     */
    private final long currentTimeMs;

    /**
     * 需要变更分配的主题分区集合
     * 使用不可变集合确保线程安全，防止外部修改
     */
    private final Collection<TopicPartition> partitions;

    /**
     * 构造函数，创建一个新的分区分配变更事件
     * 
     * 实现细节：
     * 1. 调用父类构造函数，设置事件类型为ASSIGNMENT_CHANGE
     * 2. 记录事件创建时的时间戳
     * 3. 使用Set.copyOf创建不可变分区集合，确保线程安全
     *
     * @param currentTimeMs 事件创建时的当前时间戳（毫秒）
     * @param deadlineMs 事件处理的截止时间（毫秒）
     * @param partitions 需要变更分配的主题分区集合
     */
    public AssignmentChangeEvent(final long currentTimeMs, final long deadlineMs, final Collection<TopicPartition> partitions) {
        super(Type.ASSIGNMENT_CHANGE, deadlineMs);
        this.currentTimeMs = currentTimeMs;
        this.partitions = Set.copyOf(partitions);
    }

    /**
     * 获取事件创建时的时间戳
     * 
     * @return 事件创建时的时间戳（毫秒）
     */
    public long currentTimeMs() {
        return currentTimeMs;
    }

    /**
     * 获取需要变更分配的主题分区集合
     * 
     * @return 不可变的主题分区集合
     */
    public Collection<TopicPartition> partitions() {
        return partitions;
    }

    /**
     * 重写toString方法的基础部分
     * 添加时间戳和分区信息到字符串表示中
     * 
     * @return 包含事件详细信息的字符串
     */
    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", currentTimeMs=" + currentTimeMs + ", partitions=" + partitions;
    }
}
