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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Objects;

/**
 * 要发送到Kafka的键值对记录。包含以下组成部分：
 * 1. topic名称：记录将被发送到的主题（必需）
 * 2. partition编号：可选的分区号
 * 3. key和value：可选的键值对
 * <p>
 * 分区选择机制：
 * 1. 如果指定了有效的分区号，消息会被发送到指定分区
 * 2. 如果没有指定分区，但提供了key，会使用key的哈希值选择分区
 * 3. 如果既没有分区号也没有key，将以轮询方式分配分区
 * 注意：分区编号从0开始
 * <p>
 * 时间戳处理机制：
 * 1. 如果用户没有提供时间戳，生产者会使用当前时间作为记录的时间戳
 * 2. Kafka最终使用的时间戳取决于主题的时间戳类型配置：
 * <li>
 * 如果主题配置为使用 {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}，
 * broker将使用生产者记录中的时间戳
 * </li>
 * <li>
 * 如果主题配置为使用 {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}，
 * broker会用消息追加到日志时的本地时间覆盖生产者记录中的时间戳
 * </li>
 * <p>
 * 在上述两种情况下，实际使用的时间戳都会通过{@link RecordMetadata}返回给用户
 */
public class ProducerRecord<K, V> {

    // 记录要发送到的主题名称，不能为null
    private final String topic;
    // 记录要发送到的分区号，可以为null（由Kafka自动分配）
    private final Integer partition;
    // 消息头部信息，包含用户自定义的键值对元数据
    private final Headers headers;
    // 消息的键，可以为null
    private final K key;
    // 消息的实际内容
    private final V value;
    // 消息的时间戳（毫秒），可以为null（由生产者自动设置为当前时间）
    private final Long timestamp;

    /**
     * 创建一个带有指定时间戳的消息记录，将被发送到指定的主题和分区
     * 
     * @param topic 消息记录将被追加到的主题名称
     * @param partition 消息记录应该被发送到的分区号
     * @param timestamp 消息记录的时间戳（从epoch开始的毫秒数）。如果为null，生产者将使用System.currentTimeMillis()设置时间戳
     * @param key 消息记录中包含的键
     * @param value 消息记录的实际内容
     * @param headers 消息记录中包含的头部信息
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        // 校验topic不能为null
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        // 校验timestamp必须为null或非负数
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        // 校验partition必须为null或非负数
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        // 初始化所有字段
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        // 创建新的RecordHeaders对象来存储消息头
        this.headers = new RecordHeaders(headers);
    }

    /**
     * 创建一个带有指定时间戳的消息记录，将被发送到指定的主题和分区
     * 这个构造函数不包含消息头信息
     *
     * @param topic 消息记录将被追加到的主题名称
     * @param partition 消息记录应该被发送到的分区号
     * @param timestamp 消息记录的时间戳（从epoch开始的毫秒数）。如果为null，生产者将使用System.currentTimeMillis()设置时间戳
     * @param key 消息记录中包含的键
     * @param value 消息记录的实际内容
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, partition, timestamp, key, value, null);
    }

    /**
     * 创建一个消息记录，将被发送到指定的主题和分区
     * 这个构造函数包含消息头信息，但不指定时间戳（将使用生产者的当前时间）
     *
     * @param topic 消息记录将被追加到的主题名称
     * @param partition 消息记录应该被发送到的分区号
     * @param key 消息记录中包含的键
     * @param value 消息记录的实际内容
     * @param headers 消息记录中包含的头部信息
     */
    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this(topic, partition, null, key, value, headers);
    }
    
    /**
     * 创建一个消息记录，将被发送到指定的主题和分区
     * 这个构造函数不包含时间戳和消息头信息
     *
     * @param topic 消息记录将被追加到的主题名称
     * @param partition 消息记录应该被发送到的分区号
     * @param key 消息记录中包含的键
     * @param value 消息记录的实际内容
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, value, null);
    }
    
    /**
     * 创建一个消息记录，将被发送到指定的主题
     * 这个构造函数只指定主题、键和值，分区将由Kafka自动选择
     * 
     * @param topic 消息记录将被追加到的主题名称
     * @param key 消息记录中包含的键
     * @param value 消息记录的实际内容
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, key, value, null);
    }
    
    /**
     * 创建一个没有键的消息记录
     * 这是最简单的构造函数，只需要指定主题和值
     * 
     * @param topic 消息记录将被发送到的主题名称
     * @param value 消息记录的实际内容
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value, null);
    }

    /**
     * 获取消息记录将被发送到的主题名称
     * @return 主题名称
     */
    public String topic() {
        return topic;
    }

    /**
     * 获取消息记录的头部信息
     * @return 消息头对象，包含用户自定义的键值对元数据
     */
    public Headers headers() {
        return headers;
    }

    /**
     * 获取消息记录的键
     * @return 消息的键（如果没有指定键则返回null）
     */
    public K key() {
        return key;
    }

    /**
     * 获取消息记录的值
     * @return 消息的实际内容
     */
    public V value() {
        return value;
    }

    /**
     * 获取消息记录的时间戳
     * @return 时间戳，以毫秒为单位（从epoch开始）
     */
    public Long timestamp() {
        return timestamp;
    }

    /**
     * 获取消息记录将被发送到的分区号
     * @return 分区号（如果没有指定分区则返回null）
     */
    public Integer partition() {
        return partition;
    }

    /**
     * 将消息记录转换为字符串表示形式
     * 包含所有字段的值，null值会被显示为"null"
     * @return 消息记录的字符串表示
     */
    @Override
    public String toString() {
        String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", headers=" + headers + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ")";
    }

    /**
     * 判断两个消息记录是否相等
     * 所有字段都相等时返回true
     * @param o 要比较的对象
     * @return 如果两个消息记录相等则返回true，否则返回false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerRecord))
            return false;

        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;

        return Objects.equals(key, that.key) &&
            Objects.equals(partition, that.partition) &&
            Objects.equals(topic, that.topic) &&
            Objects.equals(headers, that.headers) &&
            Objects.equals(value, that.value) &&
            Objects.equals(timestamp, that.timestamp);
    }

    /**
     * 计算消息记录的哈希码
     * 使用所有字段的值计算哈希值
     * @return 消息记录的哈希码
     */
    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
