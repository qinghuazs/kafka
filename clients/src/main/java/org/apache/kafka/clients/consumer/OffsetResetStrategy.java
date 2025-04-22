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
package org.apache.kafka.clients.consumer;

import java.util.Locale;

/**
 * Kafka消费者的偏移量重置策略枚举类。
 * 当消费者需要确定从哪个位置开始消费消息时（例如：第一次消费、偏移量过期或无效），将使用此策略。
 * 
 * @deprecated 自4.0版本起已弃用。请使用 {@link org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy} 代替。
 * 新版本提供了更灵活的偏移量重置机制，包括基于时间的重置策略。
 */
@Deprecated
public enum OffsetResetStrategy {
    /**
     * 将偏移量重置到分区中最新的位置（即最后提交的消息偏移量）。
     * 使用场景：
     * 1. 只关注最新的消息，不需要消费历史数据
     * 2. 实时监控或告警系统，只需要处理最新状态
     */
    LATEST,

    /**
     * 将偏移量重置到分区中最早的位置。
     * 使用场景：
     * 1. 需要从头开始处理所有历史数据
     * 2. 数据迁移或全量数据分析
     * 3. 消费者第一次订阅主题且希望不丢失任何消息
     */
    EARLIEST,

    /**
     * 当找不到消费者组的偏移量时，不自动重置偏移量，而是抛出异常。
     * 使用场景：
     * 1. 严格的消息处理要求，不允许自动重置偏移量
     * 2. 需要人工干预来处理偏移量丢失的情况
     * 3. 对数据一致性要求较高的业务场景
     */
    NONE;

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
