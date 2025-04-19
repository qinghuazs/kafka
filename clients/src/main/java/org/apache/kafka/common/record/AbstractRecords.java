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

public abstract class AbstractRecords implements Records {

    private final Iterable<Record> records = this::recordsIterator;

    @Override
    public boolean hasMatchingMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() != magic)
                return false;
        return true;
    }

    public RecordBatch firstBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        if (!iterator.hasNext())
            return null;

        return iterator.next();
    }

    @Override
    public Optional<RecordBatch> lastBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        RecordBatch batch = null;
        while (iterator.hasNext())
            batch = iterator.next();

        return Optional.ofNullable(batch);
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    @Override
    public Iterable<Record> records() {
        return records;
    }

    @Override
    public DefaultRecordsSend<Records> toSend() {
        return new DefaultRecordsSend<>(this);
    }

    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }

    public static int estimateSizeInBytes(byte magic,
                                          long baseOffset,
                                          CompressionType compressionType,
                                          Iterable<Record> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<SimpleRecord> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (SimpleRecord record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

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
