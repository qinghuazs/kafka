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
package org.apache.kafka.common.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;

/**
 * 记录反序列化异常
 * 
 * 当消费者使用配置的反序列化器（{@link org.apache.kafka.common.serialization.Deserializer}）
 * 对接收到的记录进行反序列化时发生错误时抛出此异常。
 * 
 * 异常场景：
 * 1. 消息格式与反序列化器不匹配
 * 2. 消息数据损坏或不完整
 * 3. 自定义反序列化器实现错误
 * 
 * 异常包含了完整的消息上下文信息，便于：
 * - 定位问题消息的具体位置（主题分区和偏移量）
 * - 分析消息的原始内容（key和value的字节数据）
 * - 确定反序列化失败的具体组件（key或value）
 */
public class RecordDeserializationException extends SerializationException {

    private static final long serialVersionUID = 2L;

    /**
     * 反序列化异常来源枚举
     * 用于标识反序列化失败发生在消息的哪个部分
     */
    public enum DeserializationExceptionOrigin {
        /** 消息键反序列化失败 */
        KEY,
        /** 消息值反序列化失败 */
        VALUE
    }

    /** 反序列化异常的来源（KEY或VALUE） */
    private final DeserializationExceptionOrigin origin;
    /** 发生异常的主题分区 */
    private final TopicPartition partition;
    /** 发生异常的消息偏移量 */
    private final long offset;
    /** 消息的时间戳类型 */
    private final TimestampType timestampType;
    /** 消息的时间戳 */
    private final long timestamp;
    /** 消息键的原始字节缓冲区 */
    private final ByteBuffer keyBuffer;
    /** 消息值的原始字节缓冲区 */
    private final ByteBuffer valueBuffer;
    /** 消息的头部信息 */
    private final Headers headers;

    /**
     * 已废弃的构造函数
     * 
     * @deprecated 自3.9版本起废弃。请使用 {@link #RecordDeserializationException(DeserializationExceptionOrigin, TopicPartition, long, long, TimestampType, ByteBuffer, ByteBuffer, Headers, String, Throwable)} 替代。
     */
    @Deprecated
    public RecordDeserializationException(TopicPartition partition,
                                          long offset,
                                          String message,
                                          Throwable cause) {
        super(message, cause);
        this.origin = null;
        this.partition = partition;
        this.offset = offset;
        this.timestampType = TimestampType.NO_TIMESTAMP_TYPE;
        this.timestamp = ConsumerRecord.NO_TIMESTAMP;
        this.keyBuffer = null;
        this.valueBuffer = null;
        this.headers = null;
    }

    /**
     * 构造函数
     *
     * @param origin 反序列化异常的来源（KEY或VALUE）
     * @param partition 发生异常的主题分区
     * @param offset 发生异常的消息偏移量
     * @param timestamp 消息的时间戳
     * @param timestampType 消息的时间戳类型
     * @param keyBuffer 消息键的原始字节缓冲区
     * @param valueBuffer 消息值的原始字节缓冲区
     * @param headers 消息的头部信息
     * @param message 异常消息
     * @param cause 导致此异常的原始异常
     */
    public RecordDeserializationException(DeserializationExceptionOrigin origin,
                                          TopicPartition partition,
                                          long offset,
                                          long timestamp,
                                          TimestampType timestampType,
                                          ByteBuffer keyBuffer,
                                          ByteBuffer valueBuffer,
                                          Headers headers,
                                          String message,
                                          Throwable cause) {
        super(message, cause);
        this.origin = origin;
        this.offset = offset;
        this.timestampType = timestampType;
        this.timestamp = timestamp;
        this.partition = partition;
        this.keyBuffer = keyBuffer;
        this.valueBuffer = valueBuffer;
        this.headers = headers;
    }

    /**
     * 获取反序列化异常的来源
     *
     * @return 异常来源（KEY或VALUE）
     */
    public DeserializationExceptionOrigin origin() {
        return origin;
    }

    /**
     * 获取发生异常的主题分区
     *
     * @return 主题分区信息
     */
    public TopicPartition topicPartition() {
        return partition;
    }

    /**
     * 获取发生异常的消息偏移量
     *
     * @return 消息偏移量
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取消息的时间戳类型
     *
     * @return 时间戳类型
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * 获取消息的时间戳
     *
     * @return 消息时间戳
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * 获取消息键的原始字节缓冲区
     *
     * @return 消息键的字节缓冲区
     */
    public ByteBuffer keyBuffer() {
        return keyBuffer;
    }

    /**
     * 获取消息值的原始字节缓冲区
     *
     * @return 消息值的字节缓冲区
     */
    public ByteBuffer valueBuffer() {
        return valueBuffer;
    }

    /**
     * 获取消息的头部信息
     *
     * @return 消息头部
     */
    public Headers headers() {
        return headers;
    }
}
