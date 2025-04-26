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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.LegacyFileChannelRecordBatch;
import org.apache.kafka.common.record.DefaultRecordBatch.DefaultFileChannelRecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Objects;

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/**
 * 一个由{@link FileChannel}支持的日志输入流。
 * 
 * 应用场景:
 * 1. 从Kafka日志文件中读取消息记录
 * 2. 支持不同版本的消息格式(V0/V1/V2)
 * 3. 提供记录批次的迭代访问功能
 * 4. 实现延迟加载,仅在需要时才读取记录数据
 */
public class FileLogInputStream implements LogInputStream<FileLogInputStream.FileChannelRecordBatch> {
    // 当前读取位置
    private int position;
    // 文件结束位置,不会读取超过此位置的数据
    private final int end;
    // 底层文件记录对象,提供对文件通道的访问
    private final FileRecords fileRecords;
    // 用于读取日志头部的缓冲区,大小为直到magic字段的头部大小
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /**
     * 创建一个新的文件日志输入流
     * 
     * @param records 底层的FileRecords实例,提供文件通道访问
     * @param start 开始读取的文件位置
     * @param end 结束读取的文件位置
     */
    FileLogInputStream(FileRecords records,
                       int start,
                       int end) {
        this.fileRecords = records;
        this.position = start;
        this.end = end;
    }

    /**
     * 读取下一个记录批次
     * 
     * 实现细节:
     * 1. 首先检查是否还有足够空间读取头部
     * 2. 读取并解析日志头部信息(偏移量、大小、magic值)
     * 3. 验证记录大小是否合法
     * 4. 根据magic值创建对应版本的记录批次对象
     * 5. 更新读取位置
     * 
     * @return 下一个FileChannelRecordBatch对象,如果到达文件末尾则返回null
     * @throws IOException 如果读取过程中发生I/O错误
     * @throws CorruptRecordException 如果发现损坏的记录
     */
    @Override
    public FileChannelRecordBatch nextBatch() throws IOException {
        // 获取底层文件通道
        FileChannel channel = fileRecords.channel();
        // 检查是否还有足够空间读取头部
        if (position >= end - HEADER_SIZE_UP_TO_MAGIC)
            return null;

        // 重置缓冲区并读取日志头部
        logHeaderBuffer.rewind();
        Utils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        // 解析头部信息
        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong(OFFSET_OFFSET);  // 获取记录偏移量
        int size = logHeaderBuffer.getInt(SIZE_OFFSET);      // 获取记录大小

        // 验证记录大小是否大于最小开销(使用V0版本的开销作为基准)
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Found record size %d smaller than minimum record " +
                            "overhead (%d) in file %s.", size, LegacyRecord.RECORD_OVERHEAD_V0, fileRecords.file()));

        // 检查是否有足够空间读取整个记录
        if (position > end - LOG_OVERHEAD - size)
            return null;

        // 获取magic值(消息格式版本)
        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        final FileChannelRecordBatch batch;

        // 根据magic值创建对应版本的记录批次对象
        if (magic < RecordBatch.MAGIC_VALUE_V2)
            batch = new LegacyFileChannelRecordBatch(offset, magic, fileRecords, position, size);  // 创建旧版本记录批次
        else
            batch = new DefaultFileChannelRecordBatch(offset, magic, fileRecords, position, size);  // 创建新版本记录批次

        // 更新读取位置
        position += batch.sizeInBytes();
        return batch;
    }

    /**
     * 由底层FileChannel支持的日志条目。这种设计允许在不需要将记录数据读入内存的情况下遍历记录批次,
     * 直到实际需要时才加载。缺点是当底层通道关闭时,条目通常将不再可读。
     * 
     * 应用场景:
     * 1. 延迟加载大型日志文件
     * 2. 支持不同版本的消息格式
     * 3. 提供记录批次的迭代访问
     * 4. 实现内存高效的日志读取
     */
    public abstract static class FileChannelRecordBatch extends AbstractRecordBatch {
        // 记录批次的起始偏移量
        protected final long offset;
        // 消息格式版本号
        protected final byte magic;
        // 底层文件记录对象
        protected final FileRecords fileRecords;
        // 批次在文件中的起始位置
        protected final int position;
        // 批次的大小(字节)
        protected final int batchSize;

        // 完整的记录批次对象(延迟加载)
        private RecordBatch fullBatch;
        // 记录批次的头部信息(延迟加载)
        private RecordBatch batchHeader;

        /**
         * 创建一个新的文件通道记录批次
         * 
         * @param offset 批次的起始偏移量
         * @param magic 消息格式版本号
         * @param fileRecords 底层文件记录对象
         * @param position 批次在文件中的位置
         * @param batchSize 批次的大小
         */
        FileChannelRecordBatch(long offset,
                               byte magic,
                               FileRecords fileRecords,
                               int position,
                               int batchSize) {
            this.offset = offset;
            this.magic = magic;
            this.fileRecords = fileRecords;
            this.position = position;
            this.batchSize = batchSize;
        }

        /**
         * 获取压缩类型
         * 通过加载批次头部信息获取
         */
        @Override
        public CompressionType compressionType() {
            return loadBatchHeader().compressionType();
        }

        /**
         * 获取时间戳类型
         * 通过加载批次头部信息获取
         */
        @Override
        public TimestampType timestampType() {
            return loadBatchHeader().timestampType();
        }

        /**
         * 获取校验和
         * 通过加载批次头部信息获取
         */
        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        /**
         * 获取最大时间戳
         * 通过加载批次头部信息获取
         */
        @Override
        public long maxTimestamp() {
            return loadBatchHeader().maxTimestamp();
        }

        /**
         * 获取批次在文件中的位置
         */
        public int position() {
            return position;
        }

        /**
         * 获取消息格式版本号
         */
        @Override
        public byte magic() {
            return magic;
        }

        /**
         * 获取记录迭代器
         * 通过加载完整批次获取
         */
        @Override
        public Iterator<Record> iterator() {
            return loadFullBatch().iterator();
        }

        /**
         * 获取流式记录迭代器
         * 通过加载完整批次获取,支持缓冲区重用
         */
        @Override
        public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
            return loadFullBatch().streamingIterator(bufferSupplier);
        }

        /**
         * 检查记录批次是否有效
         * 通过加载完整批次验证
         */
        @Override
        public boolean isValid() {
            return loadFullBatch().isValid();
        }

        /**
         * 确保记录批次有效
         * 如果无效则抛出异常
         */
        @Override
        public void ensureValid() {
            loadFullBatch().ensureValid();
        }

        /**
         * 获取批次的总大小(字节)
         * 包括日志开销和批次大小
         */
        @Override
        public int sizeInBytes() {
            return LOG_OVERHEAD + batchSize;
        }

        /**
         * 将批次数据写入缓冲区
         * 
         * @param buffer 目标缓冲区
         * @throws KafkaException 如果读取过程中发生I/O错误
         */
        @Override
        public void writeTo(ByteBuffer buffer) {
            FileChannel channel = fileRecords.channel();
            try {
                // 保存原始限制
                int limit = buffer.limit();
                // 设置新的限制以容纳批次数据
                buffer.limit(buffer.position() + sizeInBytes());
                // 从文件通道读取数据
                Utils.readFully(channel, buffer, position);
                // 恢复原始限制
                buffer.limit(limit);
            } catch (IOException e) {
                throw new KafkaException("Failed to read record batch at position " + position + " from " + fileRecords, e);
            }
        }

        /**
         * 将缓冲区数据转换为内存中的记录批次
         * 由具体的实现类提供转换逻辑
         */
        protected abstract RecordBatch toMemoryRecordBatch(ByteBuffer buffer);

        /**
         * 获取批次头部的大小
         * 由具体的实现类提供
         */
        protected abstract int headerSize();

        /**
         * 加载完整的记录批次
         * 如果尚未加载则从文件中读取
         */
        protected RecordBatch loadFullBatch() {
            if (fullBatch == null) {
                batchHeader = null;
                fullBatch = loadBatchWithSize(sizeInBytes(), "full record batch");
            }
            return fullBatch;
        }

        /**
         * 加载批次头部信息
         * 如果已加载完整批次则直接返回
         */
        protected RecordBatch loadBatchHeader() {
            if (fullBatch != null)
                return fullBatch;

            if (batchHeader == null)
                batchHeader = loadBatchWithSize(headerSize(), "record batch header");

            return batchHeader;
        }

        /**
         * 从文件中加载指定大小的批次数据
         * 
         * @param size 要加载的数据大小
         * @param description 描述信息(用于错误报告)
         * @return 加载的记录批次
         * @throws KafkaException 如果加载过程中发生I/O错误
         */
        private RecordBatch loadBatchWithSize(int size, String description) {
            FileChannel channel = fileRecords.channel();
            try {
                // 分配缓冲区
                ByteBuffer buffer = ByteBuffer.allocate(size);
                // 从文件通道读取数据
                Utils.readFullyOrFail(channel, buffer, position, description);
                buffer.rewind();
                // 转换为内存中的记录批次
                return toMemoryRecordBatch(buffer);
            } catch (IOException e) {
                throw new KafkaException("Failed to load record batch at position " + position + " from " + fileRecords, e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FileChannelRecordBatch that = (FileChannelRecordBatch) o;

            FileChannel channel = fileRecords == null ? null : fileRecords.channel();
            FileChannel thatChannel = that.fileRecords == null ? null : that.fileRecords.channel();

            return offset == that.offset &&
                    position == that.position &&
                    batchSize == that.batchSize &&
                    Objects.equals(channel, thatChannel);
        }

        @Override
        public int hashCode() {
            FileChannel channel = fileRecords == null ? null : fileRecords.channel();

            int result = Long.hashCode(offset);
            result = 31 * result + (channel != null ? channel.hashCode() : 0);
            result = 31 * result + position;
            result = 31 * result + batchSize;
            return result;
        }

        @Override
        public String toString() {
            return "FileChannelRecordBatch(magic: " + magic +
                    ", offset: " + offset +
                    ", size: " + batchSize + ")";
        }
    }
}
