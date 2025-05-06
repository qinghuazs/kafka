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
import java.util.Set;

/**
 * 获取已提交偏移量的事件类。
 * 该事件用于异步获取指定分区集合的已提交偏移量信息。作为Kafka消费者组位移管理的重要组成部分，
 * 它继承自CompletableApplicationEvent，支持异步操作完成后的回调处理。
 * 
 * 返回类型为Map<TopicPartition, OffsetAndMetadata>，其中：
 * - TopicPartition：表示具体的主题分区
 * - OffsetAndMetadata：包含已提交的偏移量及其元数据信息
 */
public class FetchCommittedOffsetsEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>> {

    /**
     * 需要获取已提交偏移量的分区集合。
     * 使用final修饰确保引用不可变，通过Collections.unmodifiableSet确保集合内容不可修改，
     * 这样的设计保证了事件处理过程中分区集合的稳定性和线程安全性。
     */
    private final Set<TopicPartition> partitions;

    /**
     * 创建一个获取已提交偏移量的事件实例
     * 
     * @param partitions 需要获取已提交偏移量的分区集合
     * @param deadlineMs 事件处理的截止时间戳（毫秒）
     */
    public FetchCommittedOffsetsEvent(final Set<TopicPartition> partitions, final long deadlineMs) {
        // 调用父类构造器，设置事件类型为FETCH_COMMITTED_OFFSETS和处理截止时间
        super(Type.FETCH_COMMITTED_OFFSETS, deadlineMs);
        // 将分区集合转换为不可修改的集合，确保事件处理过程中的数据一致性
        this.partitions = Collections.unmodifiableSet(partitions);
    }

    /**
     * 获取需要查询已提交偏移量的分区集合
     * 
     * @return 不可修改的分区集合
     */
    public Set<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", partitions=" + partitions;
    }
}
