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

import java.io.Serializable;
import java.util.Objects;


/**
 * 主题分区副本类，用于唯一标识Kafka集群中某个分区的副本
 * 包含三个关键信息：主题名称(topic)、分区号(partition)和broker ID
 * 这个类在Kafka的副本管理、故障转移等场景中被广泛使用
 */
public final class TopicPartitionReplica implements Serializable {

    // 缓存的哈希码，用于提高哈希表性能
    private int hash = 0;
    // broker ID，标识存储该副本的broker节点
    private final int brokerId;
    // 分区号，标识主题内的具体分区
    private final int partition;
    // 主题名称
    private final String topic;

    /**
     * 构造函数，创建一个主题分区副本实例
     * @param topic 主题名称，不能为null
     * @param partition 分区号
     * @param brokerId broker ID，标识副本所在的broker节点
     */
    public TopicPartitionReplica(String topic, int partition, int brokerId) {
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
        this.brokerId = brokerId;
    }

    /**
     * 获取主题名称
     * @return 返回当前副本所属的主题名称
     */
    public String topic() {
        return topic;
    }

    /**
     * 获取分区号
     * @return 返回当前副本所属的分区号
     */
    public int partition() {
        return partition;
    }

    /**
     * 获取broker ID
     * @return 返回存储该副本的broker节点ID
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * 计算哈希码
     * 使用缓存机制来提高性能，只在第一次调用时计算哈希值
     * 哈希算法考虑了主题名称、分区号和broker ID三个属性
     */
    @Override
    public int hashCode() {
        // 如果哈希值已经计算过，直接返回缓存的值
        if (hash != 0) {
            return hash;
        }
        // 使用质数31作为哈希计算的基数
        final int prime = 31;
        int result = 1;
        // 依次将主题名称、分区号和broker ID的哈希值计入结果
        result = prime * result + topic.hashCode();
        result = prime * result + partition;
        result = prime * result + brokerId;
        // 缓存计算出的哈希值
        this.hash = result;
        return result;
    }

    /**
     * 判断两个TopicPartitionReplica对象是否相等
     * 当且仅当主题名称、分区号和broker ID都相同时，两个对象才相等
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicPartitionReplica other = (TopicPartitionReplica) obj;
        return partition == other.partition && brokerId == other.brokerId && topic.equals(other.topic);
    }

    /**
     * 返回主题分区副本的字符串表示
     * 格式为：主题名称-分区号-broker ID
     */
    @Override
    public String toString() {
        return String.format("%s-%d-%d", topic, partition, brokerId);
    }
}
