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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 用于处理Kafka消息记录格式转换和兼容性的工具类。
 * 主要用于在不同版本的消息格式之间进行转换，确保消息在不同版本的客户端和服务器之间能够正确传输和处理。
 */
public class RecordsUtil {
    /**
     * 将消息批次转换为指定的消息格式版本。
     * 
     * 【功能说明】
     * 1. 用于将高版本的消息格式降级转换为低版本格式，以支持旧版本客户端的兼容性需求
     * 2. 特别处理了未压缩消息从v2及以上版本转换到v1或更低版本的情况
     * 
     * 【参数说明】
     * firstOffset参数仅在将未压缩的v2或更高版本转换为v1或更低版本时有意义，
     * 这是因为在v0和v1版本中，未压缩的记录不会被批处理（每个批次始终只有1条记录）。
     * 
     * 【特殊处理】
     * 当客户端请求v1格式的记录，且起始位置在v2格式未压缩批次的中间位置时，
     * 需要在转换过程中丢弃批次中的部分记录。这是为了保证某些版本的librdkafka客户端的正确性。
     * 
     * 【内存处理】
     * temporaryMemoryBytes计算假设在调用此方法之前，批次未被加载到堆内存中
     * （例如通过FileChannelRecordBatch类）。这种情况在broker中是常见的
     * （我们只在进行降级转换时才将记录加载到堆内存中），但在生产者中较少见。
     * 由于在生产者中进行降级转换的情况非常罕见，因此没有特别处理这种情况的复杂性。
     */
    protected static ConvertedRecords<MemoryRecords> downConvert(Iterable<? extends RecordBatch> batches, byte toMagic,
                                                                 long firstOffset, Time time) {
        // 维护批次及其解压缩后的记录，避免重复解压缩操作
        List<RecordBatchAndRecords> recordBatchAndRecordsList = new ArrayList<>();
        int totalSizeEstimate = 0;  // 估算转换后的总内存大小
        long startNanos = time.nanoseconds();  // 记录开始时间，用于性能统计

        // 遍历处理每个消息批次
        for (RecordBatch batch : batches) {
            // 当目标版本低于V2版本时的特殊处理
            if (toMagic < RecordBatch.MAGIC_VALUE_V2) {
                // 跳过控制类消息批次，因为低版本不支持
                if (batch.isControlBatch())
                    continue;

                // ZSTD压缩格式不支持降级转换
                if (batch.compressionType() == CompressionType.ZSTD)
                    throw new UnsupportedCompressionTypeException("Down-conversion of zstandard-compressed batches " +
                        "is not supported");
            }

            // 如果批次的版本号小于等于目标版本，无需转换
            if (batch.magic() <= toMagic) {
                totalSizeEstimate += batch.sizeInBytes();
                recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, null, null));
            } else {
                // 需要进行版本转换的情况
                List<Record> records = new ArrayList<>();
                for (Record record : batch) {
                    // 根据条件筛选需要保留的记录：
                    // 1. 目标版本高于V1
                    // 2. 批次是压缩的
                    // 3. 记录的偏移量大于等于首个偏移量
                    if (toMagic > RecordBatch.MAGIC_VALUE_V1 || batch.isCompressed() || record.offset() >= firstOffset)
                        records.add(record);
                }
                if (records.isEmpty())
                    continue;

                // 确定基准偏移量
                final long baseOffset;
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && toMagic >= RecordBatch.MAGIC_VALUE_V2)
                    // 如果源版本和目标版本都是V2及以上，使用批次的基准偏移量
                    baseOffset = batch.baseOffset();
                else
                    // 否则使用第一条记录的偏移量作为基准
                    baseOffset = records.get(0).offset();

                // 估算转换后的大小并添加到列表
                totalSizeEstimate += AbstractRecords.estimateSizeInBytes(toMagic, baseOffset, batch.compressionType(), records);
                recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, records, baseOffset));
            }
        }

        // 根据估算的大小分配内存缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(totalSizeEstimate);
        long temporaryMemoryBytes = 0;  // 记录临时使用的内存大小
        int numRecordsConverted = 0;    // 记录转换的消息数量

        // 遍历处理每个批次和记录对象
        for (RecordBatchAndRecords recordBatchAndRecords : recordBatchAndRecordsList) {
            // 累加批次大小到临时内存统计
            temporaryMemoryBytes += recordBatchAndRecords.batch.sizeInBytes();

            if (recordBatchAndRecords.batch.magic() <= toMagic) {
                // 如果批次版本小于等于目标版本，直接写入
                buffer = Utils.ensureCapacity(buffer, buffer.position() + recordBatchAndRecords.batch.sizeInBytes());
                recordBatchAndRecords.batch.writeTo(buffer);
            } else {
                // 需要转换的批次，调用转换方法
                MemoryRecordsBuilder builder = convertRecordBatch(toMagic, buffer, recordBatchAndRecords);
                buffer = builder.buffer();  // 获取可能扩容后的缓冲区
                // 累加统计信息
                temporaryMemoryBytes += builder.uncompressedBytesWritten();
                numRecordsConverted += builder.numRecords();
            }
        }

        // 准备缓冲区用于读取
        buffer.flip();
        // 创建包含转换统计信息的对象
        RecordValidationStats stats = new RecordValidationStats(temporaryMemoryBytes, numRecordsConverted,
                time.nanoseconds() - startNanos);
        return new ConvertedRecords<>(MemoryRecords.readableRecords(buffer), stats);
    }

    /**
     * 转换单个记录批次到指定的消息格式版本。
     * 返回的缓冲区可能与输入的不同（例如可能需要扩容）。
     * 
     * @param magic 目标消息格式版本
     * @param buffer 用于存储转换后记录的缓冲区
     * @param recordBatchAndRecords 待转换的批次及其记录
     * @return 包含转换后记录的构建器
     */
    private static MemoryRecordsBuilder convertRecordBatch(byte magic, ByteBuffer buffer, RecordBatchAndRecords recordBatchAndRecords) {
        RecordBatch batch = recordBatchAndRecords.batch;
        // 获取时间戳类型
        final TimestampType timestampType = batch.timestampType();
        // 确定日志追加时间，如果是LOG_APPEND_TIME类型，使用批次的最大时间戳，否则使用NO_TIMESTAMP
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;

        // 创建内存记录构建器，设置压缩类型、时间戳类型和基准偏移量
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, Compression.of(batch.compressionType()).build(),
                timestampType, recordBatchAndRecords.baseOffset, logAppendTime);

        // 遍历并转换每条记录
        for (Record record : recordBatchAndRecords.records) {
            // 降级转换记录。注意：转换到V0和V1版本时忽略头部信息，因为这些版本不支持
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                // V2及以上版本，保留完整记录信息
                builder.append(record);
            else
                // V0和V1版本，只保留基本字段
                builder.appendWithOffset(record.offset(), record.timestamp(), record.key(), record.value());
        }

        builder.close();  // 关闭构建器
        return builder;
    }


    /**
     * 用于在版本转换过程中保存批次信息的内部类。
     * 将批次、记录列表和基准偏移量打包在一起，便于统一处理和转换。
     */
    private static class RecordBatchAndRecords {
        // 原始的消息批次对象
        private final RecordBatch batch;
        // 批次中需要转换的记录列表，如果批次版本不需要转换则为null
        private final List<Record> records;
        // 批次的基准偏移量，用于确定消息在分区中的位置
        private final Long baseOffset;

        private RecordBatchAndRecords(RecordBatch batch, List<Record> records, Long baseOffset) {
            this.batch = batch;
            this.records = records;
            this.baseOffset = baseOffset;
        }
    }

}
