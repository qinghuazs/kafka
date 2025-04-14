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
 * 主题分区类，用于唯一标识Kafka中的一个分区
 * 每个主题分区由主题名称(topic)和分区号(partition)组成
 * 这个类被广泛用于Kafka的各个组件中，用于跟踪和管理特定分区的消息
 */
public final class TopicPartition implements Serializable {
    // 序列化版本号，用于类的序列化和反序列化
    private static final long serialVersionUID = -613627415771699627L;

    // 缓存的哈希码，用于提高哈希表性能
    private int hash = 0;
    // 分区号，标识主题内的具体分区
    private final int partition;
    // 主题名称
    private final String topic;

    /**
     * 构造函数，创建一个主题分区实例
     * @param topic 主题名称
     * @param partition 分区号
     */
    public TopicPartition(String topic, int partition) {
        this.partition = partition;
        this.topic = topic;
    }

    /**
     * 获取分区号
     * @return 返回当前主题分区的分区号
     */
    public int partition() {
        return partition;
    }

    /**
     * 获取主题名称
     * @return 返回当前主题分区的主题名称
     */
    public String topic() {
        return topic;
    }

    /**
     * 计算哈希码
     * 使用缓存机制来提高性能，只在第一次调用时计算哈希值
     * 哈希算法考虑了主题名称和分区号
     */
    @Override
    public int hashCode() {
        // 如果哈希值已经计算过，直接返回缓存的值
        if (hash != 0)
            return hash;
        // 使用质数31作为哈希计算的基数
        final int prime = 31;
        // 初始化结果为基数加分区号
        int result = prime + partition;
        // 将主题名称的哈希值也计入结果
        result = prime * result + Objects.hashCode(topic);
        // 缓存计算出的哈希值
        this.hash = result;
        return result;
    }

    /**
     * 判断两个TopicPartition对象是否相等
     * 当且仅当主题名称和分区号都相同时，两个对象才相等
     */
    @Override
    public boolean equals(Object obj) {
        // 检查是否为同一个对象引用
        if (this == obj)
            return true;
        // 检查是否为null
        if (obj == null)
            return false;
        // 检查是否为同一个类
        if (getClass() != obj.getClass())
            return false;
        // 转换为TopicPartition类型
        TopicPartition other = (TopicPartition) obj;
        // 比较分区号和主题名称是否都相等
        return partition == other.partition && Objects.equals(topic, other.topic);
    }

    /**
     * 返回主题分区的字符串表示
     * 格式为：主题名称-分区号
     */
    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
