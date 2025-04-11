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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询(Round-Robin)分区器
 * 
 * 这是一个轮询分区策略的实现，用于将消息均匀地分布到所有可用的分区中。
 * 与默认分区器不同，该分区器完全忽略消息的key，不使用key的哈希值来选择分区。
 * 相反，它使用轮询计数器来依次选择每个分区，确保消息被均匀分布。
 * 
 * 主要特点：
 * 1. 消息分布均匀：通过轮询方式确保每个分区接收到相近数量的消息
 * 2. 并发安全：使用ConcurrentHashMap和AtomicInteger保证线程安全
 * 3. 容错处理：优先使用可用分区，在无可用分区时会选择不可用分区
 * 4. 按主题隔离：为每个主题维护独立的计数器，避免主题间互相影响
 */
public class RoundRobinPartitioner implements Partitioner {
    /**
     * 主题计数器映射表
     * - 使用ConcurrentHashMap确保线程安全的并发访问
     * - key为主题名称，value为该主题的轮询计数器
     * - 每个主题使用独立的AtomicInteger作为计数器，确保原子性操作
     */
    private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

    /**
     * 配置方法，本分区器不需要任何配置参数
     */
    public void configure(Map<String, ?> configs) {}

    /**
     * 为给定的记录计算目标分区
     * 
     * 实现步骤：
     * 1. 获取主题的下一个计数值
     * 2. 获取主题的可用分区列表
     * 3. 如果有可用分区，从中轮询选择一个
     * 4. 如果没有可用分区，从所有分区中轮询选择一个
     *
     * @param topic 主题名称
     * @param key 用于分区的键（可以为null）- 在轮询策略中被忽略
     * @param keyBytes 序列化后的键（可以为null）- 在轮询策略中被忽略
     * @param value 消息的值（可以为null）- 在轮询策略中被忽略
     * @param valueBytes 序列化后的值（可以为null）- 在轮询策略中被忽略
     * @param cluster 当前集群的元数据，用于获取分区信息
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int nextValue = nextValue(topic);
        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        if (!availablePartitions.isEmpty()) {
            int part = Utils.toPositive(nextValue) % availablePartitions.size();
            return availablePartitions.get(part).partition();
        } else {
            // no partitions are available, give a non-available partition
            int numPartitions = cluster.partitionsForTopic(topic).size();
            return Utils.toPositive(nextValue) % numPartitions;
        }
    }

    /**
     * 获取指定主题的下一个计数值
     * 
     * 实现说明：
     * 1. 使用computeIfAbsent确保线程安全地获取或创建计数器
     * 2. 如果主题不存在计数器，则创建一个从0开始的新计数器
     * 3. 使用getAndIncrement原子操作获取当前值并递增
     * 
     * @param topic 主题名称
     * @return 返回当前计数值（在递增之前的值）
     */
    private int nextValue(String topic) {
        AtomicInteger counter = topicCounterMap.computeIfAbsent(topic, k -> new AtomicInteger(0));
        return counter.getAndIncrement();
    }

    public void close() {}
}
