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
package org.apache.kafka.common;

import java.util.Objects;

/**
 * 这个类表示一个主题分区的全局唯一标识符。它通过组合主题ID和分区信息来确保即使同名主题被重新创建，
 * 也能保持唯一性。这对于Kafka的数据一致性和分区管理非常重要。
 * 
 * 主题ID (topicId) 是一个全局唯一的标识符，即使主题被删除后重建，新建的主题也会获得一个新的ID。
 * 这样可以避免由于主题重建导致的数据混淆问题。
 */
public class TopicIdPartition {

    // 主题的全局唯一标识符，用于确保主题的唯一性，即使是同名主题重建也会有不同的ID
    private final Uuid topicId;
    // 主题分区对象，包含了主题名称和分区号的信息
    private final TopicPartition topicPartition;

    /**
     * 使用指定的主题ID和主题分区信息创建实例。
     *
     * @param topicId 主题的全局唯一标识符
     * @param topicPartition 包含主题名称和分区号的主题分区对象
     * @throws NullPointerException 如果topicId或topicPartition为null
     */
    public TopicIdPartition(Uuid topicId, TopicPartition topicPartition) {
        this.topicId = Objects.requireNonNull(topicId, "topicId can not be null");
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition can not be null");
    }

    /**
     * 使用主题ID、分区号和主题名称创建实例。
     *
     * @param topicId 主题的全局唯一标识符
     * @param partition 分区号
     * @param topic 主题名称，可以为null
     * @throws NullPointerException 如果topicId为null
     */
    public TopicIdPartition(Uuid topicId, int partition, String topic) {
        this.topicId = Objects.requireNonNull(topicId, "topicId can not be null");
        this.topicPartition = new TopicPartition(topic, partition);
    }

    /**
     * 获取主题的全局唯一标识符。
     *
     * @return 返回表示该主题的UUID
     */
    public Uuid topicId() {
        return topicId;
    }

    /**
     * 获取主题名称。
     *
     * @return 返回主题名称，如果未知则返回null
     */
    public String topic() {
        return topicPartition.topic();
    }

    /**
     * 获取分区号。
     *
     * @return 返回分区的编号
     */
    public int partition() {
        return topicPartition.partition();
    }

    /**
     * 获取主题分区对象。
     *
     * @return 返回包含主题名称和分区号的TopicPartition对象
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicIdPartition that = (TopicIdPartition) o;
        return topicId.equals(that.topicId) &&
               topicPartition.equals(that.topicPartition);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicId.hashCode();
        result = prime * result + topicPartition.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return topicId + ":" + topic() + "-" + partition();
    }
}
