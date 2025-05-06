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

import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

/**
 * 未验证的位移调整事件类
 * 用于在后台线程执行 {@link SubscriptionState#seekUnvalidated(TopicPartition, SubscriptionState.FetchPosition)}
 * 操作。通过在后台线程中执行位移调整，可以避免在更新订阅状态时出现竞态条件。
 */
public class SeekUnvalidatedEvent extends CompletableApplicationEvent<Void> {
    /**
     * 需要调整位移的主题分区
     * 包含主题和分区信息，用于标识具体要操作的分区
     */
    private final TopicPartition partition;

    /**
     * 目标位移值
     * 表示要将消费位置调整到的具体偏移量
     */
    private final long offset;

    /**
     * 位移的纪元号（可选）
     * 用于确保位移调整的一致性，特别是在发生领导者选举或分区重分配时
     */
    private final Optional<Integer> offsetEpoch;

    /**
     * 构造函数，创建一个新的未验证位移调整事件
     * 
     * 实现细节：
     * 1. 调用父类构造函数，设置事件类型为SEEK_UNVALIDATED
     * 2. 初始化分区、位移值和纪元号信息
     *
     * @param deadlineMs 事件处理的截止时间（毫秒）
     * @param partition 目标主题分区
     * @param offset 目标位移值
     * @param offsetEpoch 位移的纪元号（可选）
     */
    public SeekUnvalidatedEvent(long deadlineMs, TopicPartition partition, long offset, Optional<Integer> offsetEpoch) {
        super(Type.SEEK_UNVALIDATED, deadlineMs);
        this.partition = partition;
        this.offset = offset;
        this.offsetEpoch = offsetEpoch;
    }

    /**
     * 获取需要调整位移的主题分区
     * 
     * @return 目标主题分区对象
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * 获取目标位移值
     * 
     * @return 要调整到的位移值
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取位移的纪元号
     * 
     * @return 位移纪元号（可选值）
     */
    public Optional<Integer> offsetEpoch() {
        return offsetEpoch;
    }

    /**
     * 重写toString方法的基础部分
     * 添加分区、位移值和纪元号信息到字符串表示中
     * 
     * @return 包含事件详细信息的字符串
     */
    @Override
    protected String toStringBase() {
        return super.toStringBase()
                + ", partition=" + partition
                + ", offset=" + offset
                + offsetEpoch.map(integer -> ", offsetEpoch=" + integer).orElse("");
    }
}
