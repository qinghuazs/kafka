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

import java.time.Duration;

/**
 * 用于检查所有已分配分区是否都有获取位置的事件。如果存在缺失的位置信息，该事件会
 * 获取偏移量并在获得后更新位置。首先会尝试使用可用的已提交偏移量。如果没有可用的
 * 已提交偏移量，则会使用从领导者节点获取的分区偏移量。
 * <p/>
 * 事件完成时会返回一个布尔值，表示所有已分配的分区是否都有有效的获取位置
 * （基于 {@link SubscriptionState#hasAllFetchPositions()} 的结果）。
 *
 * 应用场景：
 * 1. 消费者组初始化时，需要确定每个分区的起始消费位置
 * 2. 分区重分配后，新分配的分区需要确定消费位置
 * 3. 手动位置重置（seek）后，需要验证和更新位置信息
 *
 * 设计考虑：
 * 1. 位置更新的优先级：已提交偏移量 > 领导者分区偏移量
 * 2. 异步操作：通过事件机制避免阻塞主线程
 * 3. 完整性检查：确保所有分区都有有效的消费位置
 */
public class CheckAndUpdatePositionsEvent extends CompletableApplicationEvent<Boolean> {

    /**
     * 构造函数，创建一个检查和更新位置的事件
     *
     * 实现细节：
     * 1. 调用父类构造函数，设置事件类型为CHECK_AND_UPDATE_POSITIONS
     * 2. 设置事件的截止时间，超过该时间事件将被视为超时
     *
     * @param deadlineMs 事件的截止时间（毫秒），表示事件必须在该时间点之前完成
     */
    public CheckAndUpdatePositionsEvent(long deadlineMs) {
        super(Type.CHECK_AND_UPDATE_POSITIONS, deadlineMs);
    }

    /**
     * 指示此事件执行时是否需要订阅元数据
     *
     * 实现细节：
     * 1. 重写父类方法，返回true表示需要订阅元数据
     * 2. 确保在 {@link org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#poll(Duration) poll} 
     *    或 {@link org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#position(TopicPartition) position} 
     *    过程中正确处理元数据错误
     *
     * 设计考虑：
     * 1. 元数据对于位置更新至关重要，因为需要知道分区的最新状态
     * 2. 通过要求元数据存在，可以避免在缺少必要信息时进行无效的位置更新
     *
     * @return true，表示此事件需要订阅元数据才能执行
     */
    @Override
    public boolean requireSubscriptionMetadata() {
        return true;
    }
}