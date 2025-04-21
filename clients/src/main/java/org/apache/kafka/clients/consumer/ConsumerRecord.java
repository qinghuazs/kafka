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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import java.util.Optional;

/**
 * Kafka消费者接收的记录，包含键值对数据
 * 同时包含以下元数据：
 * - 主题名称
 * - 分区号
 * - 偏移量（在分区中的位置）
 * - 时间戳（来自对应的ProducerRecord）
 */
public class ConsumerRecord<K, V> {
    // 表示没有时间戳的常量
    public static final long NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP;
    // 表示序列化大小未知的常量
    public static final int NULL_SIZE = -1;

    // 记录所属的主题
    private final String topic;
    // 记录所属的分区
    private final int partition;
    // 记录在分区中的偏移量
    private final long offset;
    // 记录的时间戳
    private final long timestamp;
    // 时间戳的类型（CREATE_TIME或LOG_APPEND_TIME）
    private final TimestampType timestampType;
    // 序列化后的key大小（字节）
    private final int serializedKeySize;
    // 序列化后的value大小（字节）
    private final int serializedValueSize;
    // 记录的头部信息
    private final Headers headers;
    // 记录的键
    private final K key;
    // 记录的值
    private final V value;
    // leader epoch（可选，用于检测数据丢失）
    private final Optional<Integer> leaderEpoch;
    // 投递计数（可选，用于共享消费组）
    private final Optional<Short> deliveryCount;

    /**
     * 创建一个消费记录（兼容Kafka 0.9版本的构造函数）
     * 该版本不支持时间戳和序列化元数据
     *
     * @param topic 记录所属的主题
     * @param partition 记录所属的分区
     * @param offset 记录在分区中的偏移量
     * @param key 记录的键（可以为null）
     * @param value 记录的值
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          K key,
                          V value) {
        this(topic, partition, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, NULL_SIZE, NULL_SIZE, key, value,
            new RecordHeaders(), Optional.empty());
    }

    /**
     * 创建一个完整的消费记录（不包含投递计数）
     *
     * @param topic 记录所属的主题
     * @param partition 记录所属的分区
     * @param offset 记录在分区中的偏移量
     * @param timestamp 记录的时间戳
     * @param timestampType 时间戳类型
     * @param serializedKeySize 序列化后的key大小
     * @param serializedValueSize 序列化后的value大小
     * @param key 记录的键（可以为null）
     * @param value 记录的值
     * @param headers 记录的头部信息
     * @param leaderEpoch leader epoch（可选）
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value,
                          Headers headers,
                          Optional<Integer> leaderEpoch) {
        this(topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value,
            headers, leaderEpoch, Optional.empty());
    }

    /**
     * 创建一个完整的消费记录（包含所有字段）
     *
     * @param topic 记录所属的主题
     * @param partition 记录所属的分区
     * @param offset 记录在分区中的偏移量
     * @param timestamp 记录的时间戳
     * @param timestampType 时间戳类型
     * @param serializedKeySize 序列化后的key大小
     * @param serializedValueSize 序列化后的value大小
     * @param key 记录的键（可以为null）
     * @param value 记录的值
     * @param headers 记录的头部信息
     * @param leaderEpoch leader epoch（可选）
     * @param deliveryCount 投递计数（可选）
     */
    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value,
                          Headers headers,
                          Optional<Integer> leaderEpoch,
                          Optional<Short> deliveryCount) {
        // 校验主题名称不能为null
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        // 校验头部信息不能为null
        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        // 初始化所有字段
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.leaderEpoch = leaderEpoch;
        this.deliveryCount = deliveryCount;
    }

    /**
     * 获取记录所属的主题（永远不会返回null）
     */
    public String topic() {
        return this.topic;
    }

    /**
     * 获取记录所属的分区号
     */
    public int partition() {
        return this.partition;
    }

    /**
     * 获取记录的头部信息（永远不会返回null）
     */
    public Headers headers() {
        return headers;
    }
    
    /**
     * 获取记录的键（如果没有指定键则返回null）
     */
    public K key() {
        return key;
    }

    /**
     * 获取记录的值
     */
    public V value() {
        return value;
    }

    /**
     * 获取记录在Kafka分区中的偏移量位置
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取记录的时间戳（从Unix纪元开始的毫秒数）
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * 获取记录的时间戳类型
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * 获取序列化后的键的大小（字节）
     * 如果键为null，返回-1
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * 获取序列化后的值的大小（字节）
     * 如果值为null，返回-1
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * 获取记录的leader epoch（如果可用）
     * @return leader epoch，对于旧版本的记录格式返回空
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * 获取记录的投递计数（如果可用）
     * 只有在共享消费组中才会计数
     * @return 投递计数，如果未计数则返回空
     */
    public Optional<Short> deliveryCount() {
        return deliveryCount;
    }

    /**
     * 将记录转换为字符串表示形式，包含所有字段信息
     */
    @Override
    public String toString() {
        return "ConsumerRecord(topic = " + topic
               + ", partition = " + partition
               + ", leaderEpoch = " + leaderEpoch.orElse(null)
               + ", offset = " + offset
               + ", " + timestampType + " = " + timestamp
               + ", deliveryCount = " + deliveryCount.orElse(null)
               + ", serialized key size = "  + serializedKeySize
               + ", serialized value size = " + serializedValueSize
               + ", headers = " + headers
               + ", key = " + key
               + ", value = " + value + ")";
    }
}
