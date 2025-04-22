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

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * 该异常表示某个分区没有存储的偏移量（offset）且没有定义重置策略的情况。
 * 在Kafka消费者组中，每个分区的消费位置都由offset来跟踪。当出现以下情况时会抛出此异常：
 * 1. 消费者组首次消费某个分区，且没有之前提交的offset
 * 2. 之前的offset已过期被删除
 * 3. 消费者配置中未设置auto.offset.reset属性来处理无offset情况
 */
public class NoOffsetForPartitionException extends InvalidOffsetException {

    // 序列化版本号，用于序列化和反序列化时的版本控制
    private static final long serialVersionUID = 1L;

    // 存储所有没有offset的分区集合
    private final Set<TopicPartition> partitions;

    /**
     * 单个分区无offset异常的构造函数
     * @param partition 没有定义offset的分区
     */
    public NoOffsetForPartitionException(TopicPartition partition) {
        super("Undefined offset with no reset policy for partition: " + partition);
        // 使用Collections.singleton创建单元素不可变集合
        this.partitions = Collections.singleton(partition);
    }

    /**
     * 多个分区无offset异常的构造函数
     * @param partitions 没有定义offset的分区集合
     */
    public NoOffsetForPartitionException(Collection<TopicPartition> partitions) {
        super("Undefined offset with no reset policy for partitions: " + partitions);
        // 创建分区集合的不可变副本
        this.partitions = Set.copyOf(partitions);
    }

    /**
     * 获取所有没有定义offset的分区集合
     * @return 返回没有offset的分区的不可变集合
     */
    public Set<TopicPartition> partitions() {
        return partitions;
    }

}
