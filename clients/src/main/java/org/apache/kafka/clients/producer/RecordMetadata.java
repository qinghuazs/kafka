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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;

/**
 * 已被服务器确认的消息记录的元数据信息
 * <br/>
 * 当生产者成功发送消息到Kafka服务器并收到确认后，会返回此元数据对象。
 * 它包含了消息的关键信息：
 * - 消息的目标主题和分区
 * - 消息在分区中的偏移量
 * - 消息的时间戳
 * - 消息的序列化后大小
 * <br/>
 * 这些信息对于以下场景非常有用：
 * - 消息追踪和定位
 * - 消息发送确认
 * - 性能监控和调优
 * - 消息大小统计
 */
public final class RecordMetadata {

    /**
     * 表示消息记录尚未分配分区时的分区值
     * <br/>
     * 当消息记录还未被分配到具体分区时（例如在消息发送过程中），
     * 使用此常量值(-1)表示分区未知。
     */
    public static final int UNKNOWN_PARTITION = -1;

    private final long offset;
    // 消息的时间戳
    // 时间戳的值取决于主题的时间戳类型配置：
    // 1. 如果使用LogAppendTime（日志追加时间），时间戳将是broker追加消息时的时间戳
    // 2. 如果使用CreateTime（创建时间），时间戳将是：
    //    - 如果用户在ProducerRecord中指定了时间戳，则使用用户指定的时间戳
    //    - 否则，使用生产者接收到消息时的本地时间
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final TopicPartition topicPartition;

    /**
     * 使用提供的参数创建一个新的RecordMetadata实例
     * <br/>
     * @param topicPartition 消息发送到的主题分区
     * @param baseOffset 消息批次的基础偏移量
     * @param batchIndex 消息在批次中的索引位置
     * @param timestamp 消息的时间戳
     * @param serializedKeySize 消息键序列化后的大小（字节）
     * @param serializedValueSize 消息值序列化后的大小（字节）
     */
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, int batchIndex, long timestamp,
                          int serializedKeySize, int serializedValueSize) {
        // ignore the batchIndex if the base offset is -1, since this indicates the offset is unknown
        this.offset = baseOffset == -1 ? baseOffset : baseOffset + batchIndex;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    /**
     * 判断元数据中是否包含有效的偏移量信息
     * <br/>
     * 在某些情况下，消息的偏移量可能无效，例如：
     * - 消息发送失败
     * - 服务器未返回偏移量
     * - 异步发送时尚未收到响应
     * 
     * @return 如果元数据中包含有效的偏移量则返回true，否则返回false
     */
    public boolean hasOffset() {
        return this.offset != ProduceResponse.INVALID_OFFSET;
    }

    /**
     * 获取消息记录在主题分区中的偏移量
     * <br/>
     * 偏移量是消息在分区中的唯一标识符，它是一个单调递增的整数。
     * 消费者可以通过偏移量来控制消息的消费位置。
     * 
     * @return 消息的偏移量，如果{@link #hasOffset()}返回false则返回-1
     */
    public long offset() {
        return this.offset;
    }

    /**
     * 判断元数据中是否包含有效的时间戳信息
     * <br/>
     * 时间戳可能在以下情况下无效：
     * - 较老版本的消息格式不包含时间戳
     * - 时间戳被显式设置为无效值
     * 
     * @return 如果存在有效的时间戳则返回true，否则返回false
     */
    public boolean hasTimestamp() {
        return this.timestamp != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * 获取消息记录的时间戳
     * <br/>
     * 时间戳的类型（CreateTime或LogAppendTime）取决于主题的配置：
     * - CreateTime：消息创建时间，由生产者设置
     * - LogAppendTime：消息追加时间，由broker设置
     * 
     * @return 消息的时间戳，如果{@link #hasTimestamp()}返回false则返回-1
     */
    public long timestamp() {
        return this.timestamp;
    }

    /**
     * 获取消息键序列化后的未压缩大小（字节）
     * <br/>
     * 此大小反映了消息键在网络传输前的实际大小，不包含压缩后的大小。
     * 可用于评估消息键的存储和网络开销。
     * 
     * @return 序列化后的消息键大小（字节），如果消息键为null则返回-1
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * 获取消息值序列化后的未压缩大小（字节）
     * <br/>
     * 此大小反映了消息值在网络传输前的实际大小，不包含压缩后的大小。
     * 可用于评估消息值的存储和网络开销。
     * 
     * @return 序列化后的消息值大小（字节），如果消息值为null则返回-1
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * 获取消息记录所属的主题名称
     * <br/>
     * 主题是Kafka中消息的逻辑分类，用于组织和管理相关的消息流。
     * 
     * @return 消息所属的主题名称
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * 获取消息记录所在的分区号
     * <br/>
     * 分区是主题下的物理存储单元，用于实现并行处理和数据分布。
     * 分区号从0开始，小于主题的总分区数。
     * 
     * @return 消息所在的分区号
     */
    public int partition() {
        return this.topicPartition.partition();
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
