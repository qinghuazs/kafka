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

import java.util.Map;
import java.util.Optional;

/**
 * Event to commit offsets waiting for a response and retrying on expected retriable errors until
 * the timer expires. If no offsets are provided, this event will commit all consumed offsets.
 */
/**
 * 同步提交事件类
 * 该事件用于等待响应并提交偏移量，在预期的可重试错误发生时会进行重试，直到计时器过期
 * 如果没有提供偏移量，该事件将提交所有已消费的偏移量
 */
public class SyncCommitEvent extends CommitEvent {

    /**
     * 构造函数，初始化同步提交事件
     *
     * @param offsets 要提交的主题分区偏移量映射，使用Optional包装表示可选
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public SyncCommitEvent(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为COMMIT_SYNC，并传入偏移量和截止时间
        super(Type.COMMIT_SYNC, offsets, deadlineMs);
    }
}
