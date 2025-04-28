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

import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;

/**
 * A mutable record batch is one that can be modified in place (without copying). This is used by the broker
 * to override certain fields in the batch before appending it to the log.
 * 
 * 可变记录批次接口，允许在不复制的情况下直接修改记录批次。这个接口主要被broker用来在将批次追加到日志之前
 * 覆盖批次中的某些字段。
 * 
 * 应用场景：
 * 1. 在消息写入日志之前修改消息的偏移量
 * 2. 更新消息的时间戳信息
 * 3. 设置分区leader的epoch值
 * 4. 高效地写入和读取记录批次
 */
public interface MutableRecordBatch extends RecordBatch {

    /**
     * Set the last offset of this batch.
     * @param offset The last offset to use
     * 
     * 设置此批次的最后偏移量
     * 在Kafka中，每个消息批次都有一个起始偏移量和结束偏移量，这个方法用于设置批次的结束偏移量
     * 通常在broker将消息写入日志之前调用，以确保消息的正确定位
     */
    void setLastOffset(long offset);

    /**
     * Set the max timestamp for this batch. When using log append time, this effectively overrides the individual
     * timestamps of all the records contained in the batch. To avoid recompression, the record fields are not updated
     * by this method, but clients ignore them if the timestamp time is log append time. Note that baseTimestamp is not
     * updated by this method.
     *
     * This typically requires re-computation of the batch's CRC.
     *
     * @param timestampType The timestamp type
     * @param maxTimestamp The maximum timestamp
     * 
     * 设置此批次的最大时间戳
     * 实现细节：
     * 1. 当使用日志追加时间时，此方法会覆盖批次中所有记录的单独时间戳
     * 2. 为了避免重新压缩，此方法不会更新记录字段本身
     * 3. 如果时间戳类型是日志追加时间，客户端会忽略记录中的原始时间戳
     * 4. 基础时间戳(baseTimestamp)不会被此方法更新
     * 5. 调用此方法通常需要重新计算批次的CRC校验和
     */
    void setMaxTimestamp(TimestampType timestampType, long maxTimestamp);

    /**
     * Set the partition leader epoch for this batch of records.
     * @param epoch The partition leader epoch to use
     * 
     * 设置此记录批次的分区leader epoch
     * 在Kafka的复制机制中，每个分区leader都有一个单调递增的epoch号
     * 这个值用于：
     * 1. 标识当前leader的版本
     * 2. 防止旧leader产生的消息被错误地追加到日志中
     * 3. 在故障恢复时确保数据一致性
     */
    void setPartitionLeaderEpoch(int epoch);

    /**
     * Write this record batch into an output stream.
     * @param outputStream The buffer to write the batch to
     * 
     * 将此记录批次写入输出流
     * 实现要点：
     * 1. 将整个批次的内容序列化到提供的ByteBufferOutputStream中
     * 2. 包括批次的元数据（如偏移量、时间戳等）和实际的记录数据
     * 3. 确保写入的数据格式符合Kafka的消息格式规范
     */
    void writeTo(ByteBufferOutputStream outputStream);

    /**
     * Return an iterator which skips parsing key, value and headers from the record stream, and therefore the resulted
     * {@code org.apache.kafka.common.record.Record}'s key and value fields would be empty. This iterator is used
     * when the read record's key and value are not needed and hence can save some byte buffer allocating / GC overhead.
     *
     * @return The closeable iterator
     * 
     * 返回一个跳过解析记录键值和头部的迭代器
     * 性能优化：
     * 1. 返回的Record对象中的key和value字段为空
     * 2. 适用于只需要读取记录元数据而不需要实际消息内容的场景
     * 3. 通过避免不必要的字节缓冲区分配，显著减少GC开销
     * 4. 使用BufferSupplier来管理临时缓冲区的分配和回收
     */
    CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier);
}
