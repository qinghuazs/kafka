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
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 一个容器类，用于存储从Kafka服务器拉取的消息记录。
 * 该类按照主题分区组织消息记录，每个主题分区对应一个{@link ConsumerRecord}列表。
 * 这些记录是通过{@link Consumer#poll(java.time.Duration)}操作获取的。
 * 
 * 主要功能：
 * 1. 提供按分区和主题访问消息记录的方法
 * 2. 支持遍历所有消息记录
 * 3. 跟踪每个分区的下一个消费位置
 */
public class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {
    // 空记录集的静态实例，用于表示没有数据的情况
    public static final ConsumerRecords<Object, Object> EMPTY = new ConsumerRecords<>(Map.of(), Map.of());

    // 存储消息记录的主数据结构，key为主题分区，value为该分区的消息记录列表
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    // 存储每个分区的下一个消费位置和元数据信息
    private final Map<TopicPartition, OffsetAndMetadata> nextOffsets;

    /**
     * 已废弃的构造函数
     * @deprecated 从4.0版本开始废弃。请使用{@link #ConsumerRecords(Map, Map)}替代。
     */
    @Deprecated
    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        this(records, Map.of()); // 使用空的nextOffsets映射调用新的构造函数
    }

    /**
     * 创建一个新的ConsumerRecords实例
     * @param records 包含每个分区消息记录的映射
     * @param nextOffsets 包含每个分区下一个消费位置的映射
     */
    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records, final Map<TopicPartition, OffsetAndMetadata> nextOffsets) {
        this.records = records; // 存储消息记录映射
        this.nextOffsets = Map.copyOf(nextOffsets); // 创建nextOffsets的不可变副本
    }

    /**
     * 获取指定分区的所有消息记录
     * 
     * @param partition 目标分区
     * @return 返回指定分区的消息记录列表，如果分区不存在则返回空列表。返回的列表是不可修改的。
     */
    public List<ConsumerRecord<K, V>> records(TopicPartition partition) {
        List<ConsumerRecord<K, V>> recs = this.records.get(partition); // 获取分区的记录列表
        if (recs == null)
            return Collections.emptyList(); // 如果分区不存在，返回空列表
        else
            return Collections.unmodifiableList(recs); // 返回不可修改的记录列表
    }

    /**
     * 获取所有分区的下一个消费位置和元数据信息
     * 这些位置信息表示在当前poll调用中已经更新的消费位置
     * 
     * @return 返回一个不可变的映射，包含每个分区的下一个消费位置和元数据
     */
    public Map<TopicPartition, OffsetAndMetadata> nextOffsets() {
        return nextOffsets; // 返回存储的nextOffsets映射（已经是不可变的）
    }

    /**
     * 获取指定主题的所有消息记录
     * 这个方法会收集指定主题的所有分区的记录，并返回一个可以遍历所有记录的迭代器
     * 
     * @param topic 目标主题名称
     * @return 返回一个可以遍历指定主题所有消息记录的Iterable对象
     * @throws IllegalArgumentException 如果topic参数为null
     */
    public Iterable<ConsumerRecord<K, V>> records(String topic) {
        if (topic == null)
            throw new IllegalArgumentException("Topic must be non-null."); // 检查主题名称是否为null
        List<List<ConsumerRecord<K, V>>> recs = new ArrayList<>(); // 创建一个列表来存储所有匹配的分区记录
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : records.entrySet()) {
            if (entry.getKey().topic().equals(topic)) // 检查分区是否属于目标主题
                recs.add(entry.getValue()); // 添加匹配的分区记录列表
        }
        return new ConcatenatedIterable<>(recs); // 返回一个可以连续遍历所有记录的迭代器
    }

    /**
     * 获取当前记录集中包含数据的所有分区
     * 
     * @return 返回一个不可修改的分区集合，包含所有有数据的分区。如果没有数据，返回空集合
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(records.keySet()); // 返回records的键集合的不可修改视图
    }

    /**
     * 实现Iterable接口，提供遍历所有消息记录的迭代器
     * 
     * @return 返回一个可以遍历所有分区所有消息记录的迭代器
     */
    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return new ConcatenatedIterable<>(records.values()).iterator(); // 创建一个可以遍历所有分区记录的迭代器
    }

    /**
     * 获取所有主题的消息记录总数
     * 
     * @return 返回所有分区中消息记录的总数
     */
    public int count() {
        int count = 0;
        for (List<ConsumerRecord<K, V>> recs: this.records.values()) // 遍历所有分区的记录列表
            count += recs.size(); // 累加每个分区的记录数
        return count;
    }

    /**
     * 内部工具类，用于连接多个ConsumerRecord迭代器
     * 这个类可以将多个分区的记录列表连接成一个统一的迭代器，方便遍历所有记录
     */
    private static class ConcatenatedIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {

        // 存储多个可迭代对象的集合，每个可迭代对象代表一个分区的记录列表
        private final Iterable<? extends Iterable<ConsumerRecord<K, V>>> iterables;

        /**
         * 创建一个新的连接迭代器
         * @param iterables 要连接的多个迭代器集合
         */
        public ConcatenatedIterable(Iterable<? extends Iterable<ConsumerRecord<K, V>>> iterables) {
            this.iterables = iterables;
        }

        /**
         * 实现迭代器接口，提供一个可以连续遍历所有分区记录的迭代器
         * 
         * @return 返回一个抽象迭代器，可以顺序访问所有分区的所有记录
         */
        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<>() {
                // 外层迭代器，用于遍历所有分区的记录列表
                final Iterator<? extends Iterable<ConsumerRecord<K, V>>> iters = iterables.iterator();
                // 当前正在遍历的分区的记录迭代器
                Iterator<ConsumerRecord<K, V>> current;

                /**
                 * 获取下一个记录
                 * 如果当前迭代器已经遍历完，会自动切换到下一个分区的迭代器
                 */
                protected ConsumerRecord<K, V> makeNext() {
                    while (current == null || !current.hasNext()) { // 当前迭代器为空或已遍历完时
                        if (iters.hasNext())
                            current = iters.next().iterator(); // 切换到下一个分区的迭代器
                        else
                            return allDone(); // 所有分区都遍历完成
                    }
                    return current.next(); // 返回当前迭代器的下一个记录
                }
            };
        }
    }

    /**
     * 检查记录集是否为空
     * 
     * @return 如果没有任何记录返回true，否则返回false
     */
    public boolean isEmpty() {
        return records.isEmpty(); // 检查records映射是否为空
    }

    /**
     * 获取一个空的ConsumerRecords实例
     * 这是一个工具方法，用于快速创建空的记录集
     * 
     * @param <K> 消息键的类型
     * @param <V> 消息值的类型
     * @return 返回一个空的ConsumerRecords实例
     */
    @SuppressWarnings("unchecked")
    public static <K, V> ConsumerRecords<K, V> empty() {
        return (ConsumerRecords<K, V>) EMPTY; // 返回预定义的空记录集实例
    }

}
