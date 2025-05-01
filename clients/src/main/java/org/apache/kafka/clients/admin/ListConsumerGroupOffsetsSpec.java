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
import java.util.Objects;

/**
 * 用于指定通过Admin#listConsumerGroupOffsets(java.util.Map)方法列出消费者组偏移量的配置类。
 * 
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsSpec {

    /**
     * 要列出偏移量的主题分区集合
     * 当该字段为null时，表示列出所有主题分区的偏移量
     */
    private Collection<TopicPartition> topicPartitions;

    /**
     * 设置要列出偏移量的主题分区集合
     * 如果设置为null，则包含所有主题分区
     *
     * @param topicPartitions 要包含的主题分区列表
     * @return 返回当前ListConsumerGroupOffsetSpec对象，支持方法链式调用
     */
    public ListConsumerGroupOffsetsSpec topicPartitions(Collection<TopicPartition> topicPartitions) {
        // 设置主题分区集合
        this.topicPartitions = topicPartitions;
        // 返回this以支持方法链式调用
        return this;
    }

    /**
     * 获取要列出偏移量的主题分区集合
     * 如果返回null，表示需要列出该消费者组的所有分区的偏移量
     * 
     * @return 返回主题分区集合，如果为null则表示包含所有分区
     */
    public Collection<TopicPartition> topicPartitions() {
        // 返回主题分区集合
        return topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ListConsumerGroupOffsetsSpec)) {
            return false;
        }
        ListConsumerGroupOffsetsSpec that = (ListConsumerGroupOffsetsSpec) o;
        return Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartitions);
    }

    @Override
    public String toString() {
        return "ListConsumerGroupOffsetsSpec(" +
                "topicPartitions=" + topicPartitions +
                ')';
    }
}
