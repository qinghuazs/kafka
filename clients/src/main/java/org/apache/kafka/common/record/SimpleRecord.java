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
package org.apache.kafka.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Kafka记录的高级表示形式。在构建记录集时非常有用，因为它可以避免依赖特定的魔数版本。
 * 该类提供了一个简单的接口来创建和管理Kafka记录，包含了记录的关键组成部分：键值对、时间戳和头部信息。
 */
public class SimpleRecord {
    /**
     * 记录的键，使用ByteBuffer存储以支持高效的字节操作
     * 可以为null，表示没有键的记录
     */
    private final ByteBuffer key;

    /**
     * 记录的值，使用ByteBuffer存储以支持高效的字节操作
     * 包含实际的消息内容，可以为null
     */
    private final ByteBuffer value;

    /**
     * 记录的时间戳，表示记录创建或接收的时间
     * 以毫秒为单位的Unix时间戳
     */
    private final long timestamp;

    /**
     * 记录的头部信息数组，用于存储元数据
     * 不能为null，但可以是空数组
     */
    private final Header[] headers;

    /**
     * 创建一个完整的SimpleRecord实例
     * 
     * @param timestamp 记录的时间戳（毫秒）
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部信息数组
     * @throws NullPointerException 如果headers参数为null
     */
    public SimpleRecord(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 确保headers不为null，这是一个必需的安全检查
        Objects.requireNonNull(headers, "Headers must be non-null");
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    /**
     * 使用字节数组创建SimpleRecord实例
     * 
     * @param timestamp 记录的时间戳（毫秒）
     * @param key 记录的键的字节数组
     * @param value 记录的值的字节数组
     * @param headers 记录的头部信息数组
     */
    public SimpleRecord(long timestamp, byte[] key, byte[] value, Header[] headers) {
        // 将字节数组包装为ByteBuffer，支持null值
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    /**
     * 创建没有头部信息的SimpleRecord实例
     * 
     * @param timestamp 记录的时间戳（毫秒）
     * @param key 记录的键
     * @param value 记录的值
     */
    public SimpleRecord(long timestamp, ByteBuffer key, ByteBuffer value) {
        // 使用空头部数组创建记录
        this(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 使用字节数组创建没有头部信息的SimpleRecord实例
     * 
     * @param timestamp 记录的时间戳（毫秒）
     * @param key 记录的键的字节数组
     * @param value 记录的值的字节数组
     */
    public SimpleRecord(long timestamp, byte[] key, byte[] value) {
        // 将字节数组包装为ByteBuffer并使用空头部数组
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value));
    }

    /**
     * 创建只有值和时间戳的SimpleRecord实例
     * 
     * @param timestamp 记录的时间戳（毫秒）
     * @param value 记录的值的字节数组
     */
    public SimpleRecord(long timestamp, byte[] value) {
        // 创建没有键的记录
        this(timestamp, null, value);
    }

    /**
     * 创建最简单的SimpleRecord实例，只包含值
     * 
     * @param value 记录的值的字节数组
     */
    public SimpleRecord(byte[] value) {
        // 使用NO_TIMESTAMP作为默认时间戳
        this(RecordBatch.NO_TIMESTAMP, null, value);
    }

    /**
     * 创建只包含ByteBuffer值的SimpleRecord实例
     * 
     * @param value 记录的值
     */
    public SimpleRecord(ByteBuffer value) {
        // 使用NO_TIMESTAMP作为默认时间戳
        this(RecordBatch.NO_TIMESTAMP, null, value);
    }

    /**
     * 创建包含键值对的SimpleRecord实例
     * 
     * @param key 记录的键的字节数组
     * @param value 记录的值的字节数组
     */
    public SimpleRecord(byte[] key, byte[] value) {
        // 使用NO_TIMESTAMP作为默认时间戳
        this(RecordBatch.NO_TIMESTAMP, key, value);
    }

    /**
     * 从现有Record对象创建SimpleRecord实例
     * 
     * @param record 源Record对象
     */
    public SimpleRecord(Record record) {
        // 复制源记录的所有属性
        this(record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * 获取记录的键
     * 
     * @return 记录的键的ByteBuffer，如果记录没有键则返回null
     */
    public ByteBuffer key() {
        return key;
    }

    /**
     * 获取记录的值
     * 
     * @return 记录的值的ByteBuffer，如果记录没有值则返回null
     */
    public ByteBuffer value() {
        return value;
    }

    /**
     * 获取记录的时间戳
     * 
     * @return 记录的时间戳（毫秒），如果没有设置时间戳则返回RecordBatch.NO_TIMESTAMP
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * 获取记录的头部信息数组
     * 
     * @return 记录的头部信息数组，永远不会为null，但可能是空数组
     */
    public Header[] headers() {
        return headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SimpleRecord that = (SimpleRecord) o;
        return timestamp == that.timestamp &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    @Override
    public String toString() {
        return String.format("SimpleRecord(timestamp=%d, key=%d bytes, value=%d bytes)",
                timestamp(),
                key == null ? 0 : key.limit(),
                value == null ? 0 : value.limit());
    }
}
