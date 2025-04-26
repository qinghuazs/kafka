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
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

/**
 * Kafka记录集合的抽象基类，提供了对记录批次和单条记录的访问接口。
 * 该类实现了Records接口，为不同格式版本的记录提供统一的访问方式。
 * 主要功能包括：
 * 1. 记录批次的遍历和访问
 * 2. 单条记录的迭代
 * 3. 记录大小的估算
 * 4. 版本兼容性检查
 */
public abstract class AbstractRecords implements Records {

    /**
     * 用于遍历所有记录的迭代器
     * 通过方法引用this::recordsIterator初始化，确保每次调用records()方法时都能获得一个新的迭代器
     */
    private final Iterable<Record> records = this::recordsIterator;

    /**
     * 检查所有批次是否都匹配指定的魔数版本
     * 
     * @param magic 要检查的魔数版本
     * @return 如果所有批次的魔数都匹配则返回true，否则返回false
     */
    @Override
    public boolean hasMatchingMagic(byte magic) {
        // 遍历所有批次，检查每个批次的魔数是否匹配
        for (RecordBatch batch : batches())
            if (batch.magic() != magic)
                return false;
        return true;
    }

    /**
     * 获取第一个记录批次
     * 
     * @return 如果存在则返回第一个记录批次，否则返回null
     */
    public RecordBatch firstBatch() {
        // 获取批次迭代器
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        // 如果没有下一个批次，返回null
        if (!iterator.hasNext())
            return null;

        // 返回第一个批次
        return iterator.next();
    }

    /**
     * 获取最后一个记录批次
     * 
     * @return 包含最后一个记录批次的Optional对象，如果没有批次则返回空Optional
     */
    @Override
    public Optional<RecordBatch> lastBatch() {
        // 获取批次迭代器
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        // 遍历所有批次，保存最后一个批次
        RecordBatch batch = null;
        while (iterator.hasNext())
            batch = iterator.next();

        // 将最后一个批次包装成Optional返回
        return Optional.ofNullable(batch);
    }

    /**
     * 获取用于遍历所有记录的迭代器
     * 这个方法会遍历所有批次，并返回其中所有记录的迭代器
     * 
     * @return 记录迭代器
     */
    @Override
    public Iterable<Record> records() {
        return records;
    }

    /**
     * 将记录集合转换为可发送的格式
     * 
     * @return 包装后的DefaultRecordsSend对象
     */
    @Override
    public DefaultRecordsSend<Records> toSend() {
        return new DefaultRecordsSend<>(this);
    }

    /**
     * 创建一个用于遍历所有记录的迭代器
     * 这个迭代器会遍历所有批次，并返回其中的所有记录
     * 
     * @return 记录迭代器
     */
    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<>() {
            // 批次迭代器
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            // 当前批次的记录迭代器
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                // 如果当前记录迭代器存在且还有下一个记录，返回下一个记录
                if (records != null && records.hasNext())
                    return records.next();

                // 如果还有下一个批次，获取该批次的记录迭代器，并递归调用makeNext
                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                // 如果没有更多记录，返回结束标记
                return allDone();
            }
        };
    }

    /**
     * 估算给定记录集合所需的字节数
     * 
     * @param magic 记录格式的魔数版本
     * @param baseOffset 基础偏移量
     * @param compressionType 压缩类型
     * @param records 记录集合
     * @return 估算的字节数
     */
    public static int estimateSizeInBytes(byte magic,
                                          long baseOffset,
                                          CompressionType compressionType,
                                          Iterable<Record> records) {
        int size = 0;
        // 对于V1及以下版本，累加每条记录的大小
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            // 对于V2及以上版本，使用DefaultRecordBatch的方法计算大小
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        // 根据压缩类型估算最终大小
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    /**
     * 估算给定简单记录集合所需的字节数
     * 
     * @param magic 记录格式的魔数版本
     * @param compressionType 压缩类型
     * @param records 简单记录集合
     * @return 估算的字节数
     */
    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<SimpleRecord> records) {
        int size = 0;
        // 对于V1及以下版本，累加每条记录的大小
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (SimpleRecord record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            // 对于V2及以上版本，使用DefaultRecordBatch的方法计算大小
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        // 根据压缩类型估算最终大小
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    /**
     * 根据压缩类型估算压缩后的字节数
     * 
     * @param size 原始大小
     * @param compressionType 压缩类型
     * @return 估算的压缩后字节数
     * 
     * 如果不使用压缩，直接返回原始大小
     * 如果使用压缩，返回原始大小的一半到64KB之间的值，且不小于1KB
     */
    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    /**
     * 获取存储给定字段的记录批次所需的最大字节数估计值
     * 
     * @param magic 记录格式的魔数版本
     * @param compressionType 压缩类型
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部信息
     * @return 记录批次的最大字节数估计值
     * 
     * 注意：这只是一个估计值，因为它没有考虑压缩算法带来的额外开销
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, byte[] key, byte[] value, Header[] headers) {
        // 将字节数组包装成ByteBuffer，然后调用重载方法进行计算
        return estimateSizeInBytesUpperBound(magic, compressionType, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    /**
     * 获取存储给定字段的记录批次所需的最大字节数估计值（ByteBuffer版本）
     * 
     * @param magic 记录格式的魔数版本
     * @param compressionType 压缩类型
     * @param key 记录的键（ByteBuffer格式）
     * @param value 记录的值（ByteBuffer格式）
     * @param headers 记录的头部信息
     * @return 记录批次的最大字节数估计值
     * 
     * 该方法根据不同的魔数版本和压缩类型计算大小：
     * 1. 对于V2及以上版本：使用DefaultRecordBatch的估算方法
     * 2. 对于V0/V1版本：
     *    - 有压缩：LOG_OVERHEAD + 记录开销 + 记录大小
     *    - 无压缩：LOG_OVERHEAD + 记录大小
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, ByteBuffer key,
                                                    ByteBuffer value, Header[] headers) {
        // 对于V2及以上版本的记录格式
        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            return DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers);
        // 对于V0/V1版本，且使用了压缩
        else if (compressionType != CompressionType.NONE)
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic) + LegacyRecord.recordSize(magic, key, value);
        // 对于V0/V1版本，且没有使用压缩
        else
            return Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
    }

    /**
     * Return the size of the record batch header.
     *
     * For V0 and V1 with no compression, it's unclear if Records.LOG_OVERHEAD or 0 should be chosen. There is no header
     * per batch, but a sequence of batches is preceded by the offset and size. This method returns `0` as it's what
     * `MemoryRecordsBuilder` requires.
     */
    public static int recordBatchHeaderSizeInBytes(byte magic, CompressionType compressionType) {
        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        } else if (compressionType != CompressionType.NONE) {
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic);
        } else {
            return 0;
        }
    }


}
