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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * 提交偏移量事件的抽象基类，用于处理Kafka消费者的偏移量提交操作
 * 
 * 应用场景：
 * 1. 在消费者组中，用于同步或异步提交消费位移
 * 2. 支持批量提交多个分区的偏移量
 * 3. 确保提交的偏移量合法且不可变
 *
 * 设计考虑：
 * 1. 使用Optional包装偏移量Map，支持空值场景（提交所有已消费的偏移量）
 * 2. 通过不可变集合确保线程安全
 * 3. 继承CompletableApplicationEvent支持异步操作完成通知
 */
public abstract class CommitEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>> {

    /**
     * 待提交的分区偏移量映射
     * 
     * 数据结构：
     * - Key: TopicPartition，表示主题分区
     * - Value: OffsetAndMetadata，包含偏移量和元数据信息
     * 
     * 说明：
     * 1. 使用Optional包装，当为空时表示提交所有已消费的偏移量
     * 2. Map内容不可变，确保线程安全
     */
    private final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets;

    /**
     * 构造函数，初始化提交事件
     * 
     * 实现细节：
     * 1. 调用父类构造器设置事件类型和截止时间
     * 2. 验证并存储偏移量信息
     *
     * @param type 事件类型，区分同步/异步提交
     * @param offsets 待提交的偏移量映射，可为空
     * @param deadlineMs 操作截止时间（毫秒）
     */
    protected CommitEvent(final Type type, final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, final long deadlineMs) {
        super(type, deadlineMs);
        this.offsets = validate(offsets);
    }

    /**
     * 验证偏移量的合法性并返回不可变映射
     * 
     * 实现细节：
     * 1. 检查Optional是否为空
     * 2. 验证所有偏移量是否为非负数
     * 3. 将映射转换为不可变集合
     *
     * @param offsets 待验证的偏移量映射
     * @return 经过验证的不可变偏移量映射
     * @throws IllegalArgumentException 当存在负数偏移量时抛出
     */
    private static Optional<Map<TopicPartition, OffsetAndMetadata>> validate(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        if (offsets.isEmpty()) {
            return Optional.empty();
        }

        for (OffsetAndMetadata offsetAndMetadata : offsets.get().values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }

        return Optional.of(Collections.unmodifiableMap(offsets.get()));
    }

    /**
     * 获取待提交的偏移量映射
     * 
     * @return 不可变的偏移量映射，为空时表示提交所有已消费的偏移量
     */
    public Optional<Map<TopicPartition, OffsetAndMetadata>> offsets() {
        return offsets;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", offsets=" + offsets;
    }
}
