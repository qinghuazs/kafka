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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 用于指定要查询的共享消费者组偏移量的配置类。通过{@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}方法使用。
 * 
 * 共享消费者组是Kafka中的一种特殊消费者组类型，允许多个消费者共同消费同一份数据。
 * 该类用于指定要查询哪些主题分区的偏移量信息，帮助监控和管理共享消费者组的消费进度。
 * 
 * 应用场景：
 * 1. 监控共享消费者组的消费进度
 * 2. 诊断消费延迟问题
 * 3. 手动管理偏移量
 * 
 * <p>
 * 该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsSpec {

    /**
     * 要查询偏移量的主题分区集合
     * 用于存储需要获取偏移量信息的TopicPartition对象列表
     */
    private Collection<TopicPartition> topicPartitions;

    /**
     * 设置要查询偏移量的主题分区集合
     * 
     * @param topicPartitions 主题分区集合，每个元素包含主题名称和分区号
     * @return 返回当前对象实例，支持链式调用
     */
    public ListShareGroupOffsetsSpec topicPartitions(Collection<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
        return this;
    }

    /**
     * 获取要查询偏移量的主题分区集合
     * 
     * @return 如果未设置则返回空列表，否则返回已设置的主题分区集合
     */
    public Collection<TopicPartition> topicPartitions() {
        return topicPartitions == null ? List.of() : topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ListShareGroupOffsetsSpec)) {
            return false;
        }
        ListShareGroupOffsetsSpec that = (ListShareGroupOffsetsSpec) o;
        return Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartitions);
    }

    @Override
    public String toString() {
        return "ListShareGroupOffsetsSpec(" +
            "topicPartitions=" + (topicPartitions != null ? topicPartitions : "null") +
            ')';
    }
}
