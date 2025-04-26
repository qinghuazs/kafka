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

import org.apache.kafka.common.errors.CorruptRecordException;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/**
 * 基于字节缓冲区的日志输入流。
 * 该类通过返回底层字节缓冲区的切片来避免记录复制，提高读取效率。
 * 主要用于从ByteBuffer中读取Kafka消息记录批次，支持不同版本的消息格式。
 */
class ByteBufferLogInputStream implements LogInputStream<MutableRecordBatch> {
    // 底层字节缓冲区，存储实际的消息记录数据
    private final ByteBuffer buffer;
    // 允许的最大消息大小，用于验证记录大小是否超出限制
    private final int maxMessageSize;

    /**
     * 构造函数，初始化日志输入流
     * @param buffer 包含消息记录的字节缓冲区
     * @param maxMessageSize 允许的最大消息大小，用于记录大小验证
     */
    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    /**
     * 读取下一个消息记录批次
     * 实现步骤：
     * 1. 检查缓冲区剩余空间
     * 2. 获取并验证批次大小
     * 3. 读取魔数值确定消息版本
     * 4. 创建批次切片并更新缓冲区位置
     * 5. 根据消息版本返回对应的批次对象
     * 
     * @return 返回可变记录批次对象，如果没有更多数据或空间不足则返回null
     */
    public MutableRecordBatch nextBatch() {
        // 获取缓冲区中剩余的字节数
        int remaining = buffer.remaining();

        // 获取下一个批次的大小，包含了验证逻辑
        Integer batchSize = nextBatchSize();
        // 如果批次大小为null或剩余空间不足，返回null
        if (batchSize == null || remaining < batchSize)
            return null;

        // 读取魔数值，用于确定消息格式版本
        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);

        // 创建当前批次的切片，避免数据复制
        ByteBuffer batchSlice = buffer.slice();
        // 设置切片的限制为批次大小
        batchSlice.limit(batchSize);
        // 更新原缓冲区的位置
        buffer.position(buffer.position() + batchSize);

        // 根据魔数值选择适当的批次实现
        // 对于新版本的消息格式使用DefaultRecordBatch
        // 对于旧版本的消息格式使用ByteBufferLegacyRecordBatch
        if (magic > RecordBatch.MAGIC_VALUE_V1)
            return new DefaultRecordBatch(batchSlice);
        else
            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchSlice);
    }

    /**
     * 验证下一个批次的头部并返回批次大小
     * 实现步骤：
     * 1. 验证缓冲区剩余空间
     * 2. 读取并验证记录大小
     * 3. 检查魔数值的有效性
     * 4. 计算总的批次大小
     *
     * @return 返回包含LOG_OVERHEAD的下一个批次大小，如果缓冲区数据不足则返回null
     * @throws CorruptRecordException 当记录大小或魔数值无效时抛出异常
     */
    Integer nextBatchSize() throws CorruptRecordException {
        // 获取缓冲区中剩余的字节数
        int remaining = buffer.remaining();
        // 如果剩余空间小于日志开销，返回null
        if (remaining < LOG_OVERHEAD)
            return null;

        // 读取记录大小
        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);
        // 验证记录大小是否小于最小记录开销（使用V0版本的开销作为基准）
        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size %d is less than the minimum record overhead (%d)",
                    recordSize, LegacyRecord.RECORD_OVERHEAD_V0));
        // 验证记录大小是否超过最大消息大小限制
        if (recordSize > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size %d exceeds the largest allowable message size (%d).",
                    recordSize, maxMessageSize));

        // 检查是否有足够的字节来读取魔数值
        if (remaining < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        // 读取并验证魔数值的有效性
        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE)
            throw new CorruptRecordException("Invalid magic found in record: " + magic);

        // 返回总的批次大小（记录大小 + 日志开销）
        return recordSize + LOG_OVERHEAD;
    }
}
