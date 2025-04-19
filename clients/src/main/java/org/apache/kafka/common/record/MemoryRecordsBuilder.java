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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * 此类用于在内存中写入新的日志数据，是{@link MemoryRecords}的写入路径实现。
 * 它透明地处理压缩并提供追加新记录的方法，可能需要进行消息格式转换。
 * 
 * 在内存占用需要保持较低且记录追加停止到构建器关闭之间存在时间间隔的情况下（例如Producer），
 * 在追加停止时调用`closeForRecordAppends`非常重要。这将释放压缩缓冲区等资源
 * （对于LZ4压缩，这些资源可能高达64 KB）。
 */
public class MemoryRecordsBuilder implements AutoCloseable {
    // 压缩率估算因子，用于预估压缩后的数据大小
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    // 表示已关闭的输出流，当尝试写入时抛出异常
    private static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
        }
    });

    // 时间戳类型（CREATE_TIME或LOG_APPEND_TIME）
    private final TimestampType timestampType;
    // 压缩类型（如NONE、GZIP、SNAPPY、LZ4、ZSTD等）
    private final Compression compression;
    // 用于保存底层ByteBuffer的引用，以便写入记录批次头部和访问已写入的字节
    // ByteBufferOutputStream在现有缓冲区不够大时会分配新的ByteBuffer，
    // 因此直接持有对底层ByteBuffer的引用是不安全的
    private final ByteBufferOutputStream bufferStream;
    // 消息格式版本号
    private final byte magic;
    // 初始写入位置
    private final int initialPosition;
    // 基准偏移量，表示此批次第一条消息的偏移量
    private final long baseOffset;
    // 日志追加时间
    private final long logAppendTime;
    // 是否为控制批次（如事务标记）
    private final boolean isControlBatch;
    // 分区leader的epoch值
    // 分区leader的任期号,用于标识分区leader的版本,每当leader发生变更时递增,
    // 用于防止"脑裂"等异常情况
    private final int partitionLeaderEpoch;
    // 写入字节限制
    private final int writeLimit;
    // 批次头部大小（字节数）
    private final int batchHeaderSizeInBytes;
    // 删除时间界限（用于日志压缩）
    private final long deleteHorizonMs;

    // 使用保守的压缩率估计值，生产者会在追加记录前根据之前批次的统计信息覆盖此值
    private float estimatedCompressionRatio = 1.0F;

    // 用于追加记录的输出流，可能会即时压缩数据
    private DataOutputStream appendStream;
    // 是否为事务性消息
    private boolean isTransactional;
    // 生产者ID
    private long producerId;
    // 生产者epoch值
    private short producerEpoch;
    // 序列号基准值
    private int baseSequence;
    // 压缩前的记录总字节数（不包括头部）
    private int uncompressedRecordsSizeInBytes;
    // 记录数量
    private int numRecords;
    // 实际压缩率
    private float actualCompressionRatio;
    // 最大时间戳
    private long maxTimestamp;
    // 具有最大时间戳的记录的偏移量
    private long offsetOfMaxTimestamp = -1;
    // 最后一条记录的偏移量
    private Long lastOffset = null;
    // 基准时间戳
    private Long baseTimestamp = null;

    // 已构建的内存记录对象
    private MemoryRecords builtRecords;
    // 是否已中止
    private boolean aborted = false;

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
                                byte magic,
                                Compression compression,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit,
                                long deleteHorizonMs) {
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
            if (compression.type() == CompressionType.ZSTD)
                throw new IllegalArgumentException("ZStandard compression is not supported for magic " + magic);
            if (deleteHorizonMs != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Delete horizon timestamp is not supported for magic " + magic);
        }

        this.magic = magic;
        this.timestampType = timestampType;
        this.compression = compression;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.numRecords = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.isControlBatch = isControlBatch;
        this.deleteHorizonMs = deleteHorizonMs;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialPosition = bufferStream.position();
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compression.type());

        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        this.appendStream = new DataOutputStream(compression.wrapForOutput(this.bufferStream, magic));

        if (hasDeleteHorizonMs()) {
            this.baseTimestamp = deleteHorizonMs;
        }
    }

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
                                byte magic,
                                Compression compression,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        this(bufferStream, magic, compression, timestampType, baseOffset, logAppendTime, producerId,
             producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit,
             RecordBatch.NO_TIMESTAMP);
    }

    /**
     * Construct a new builder.
     *
     * @param buffer The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *               to fit the records appended)
     * @param magic The magic value to use
     * @param compression The compression codec to use
     * @param timestampType The desired timestamp type. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset The initial offset to use for
     * @param logAppendTime The log append time of this record set. Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param producerId The producer ID associated with the producer writing this record set
     * @param producerEpoch The epoch of the producer
     * @param baseSequence The sequence number of the first record in this set
     * @param isTransactional Whether or not the records are part of a transaction
     * @param isControlBatch Whether or not this is a control batch (e.g. for transaction markers)
     * @param partitionLeaderEpoch The epoch of the partition leader appending the record set to the log
     * @param writeLimit The desired limit on the total bytes for this record set (note that this can be exceeded
     *                   when compression is used since size estimates are rough, and in the case that the first
     *                   record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer,
                                byte magic,
                                Compression compression,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        this(new ByteBufferOutputStream(buffer), magic, compression, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public Compression compression() {
        return compression;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public final boolean hasDeleteHorizonMs() {
        return magic >= RecordBatch.MAGIC_VALUE_V2 && deleteHorizonMs >= 0L;
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }


    /**
     * There are three cases of finding max timestamp to return:
     * 1) version 0: The max timestamp is NO_TIMESTAMP (-1)
     * 2) LogAppendTime: All records have same timestamp, and so the max timestamp is equal to logAppendTime
     * 3) CreateTime: The max timestamp of record
     * <p>
     * Let's talk about OffsetOfMaxTimestamp. There are some paths that we don't try to find the OffsetOfMaxTimestamp
     * to avoid expensive records iteration. Those paths include follower append and index recovery. In order to
     * avoid inconsistent time index, we let all paths find shallowOffsetOfMaxTimestamp instead of OffsetOfMaxTimestamp.
     * <p>
     * Let's define the shallowOffsetOfMaxTimestamp: It is last offset of the batch having max timestamp. If there are
     * many batches having same max timestamp, we pick up the earliest batch.
     * <p>
     * There are five cases of finding shallowOffsetOfMaxTimestamp to return:
     * 1) version 0: It is always the -1
     * 2) LogAppendTime with single batch: It is the offset of last record
     * 3) LogAppendTime with many single-record batches: Those single-record batches have same max timestamp, so we return
     *                                                   the base offset, which is equal to the last offset of earliest batch
     * 4) CreateTime with single batch: We return offset of last record to follow the spec we mentioned above. Of course,
     *                                  we do have the OffsetOfMaxTimestamp for this case, but we want to make all paths
     *                                  find the shallowOffsetOfMaxTimestamp rather than offsetOfMaxTimestamp
     * 5) CreateTime with many single-record batches: Each batch is composed of single record, and hence offsetOfMaxTimestamp
     *                                                is equal to the last offset of earliest batch with max timestamp
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                // maxTimestamp => case 2
                // shallowOffsetOfMaxTimestamp => case 2
                return new RecordsInfo(logAppendTime, lastOffset);
            else
                // maxTimestamp => case 2
                // shallowOffsetOfMaxTimestamp => case 3
                return new RecordsInfo(logAppendTime, baseOffset);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            // maxTimestamp => case 1
            // shallowOffsetOfMaxTimestamp => case 1
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, -1);
        } else {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                // maxTimestamp => case 3
                // shallowOffsetOfMaxTimestamp => case 4
                return new RecordsInfo(maxTimestamp, lastOffset);
            else
                // maxTimestamp => case 3
                // shallowOffsetOfMaxTimestamp => case 5
                return new RecordsInfo(maxTimestamp, offsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    /**
     * 返回批次头部（始终未压缩）和记录（压缩前）的总大小。
     * 此方法用于获取写入的未压缩字节数，包括批次头部和记录数据。
     */
    public int uncompressedBytesWritten() {
        // 返回未压缩的记录大小和批次头部大小之和
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    /**
     * 设置生产者状态信息，包括生产者ID、epoch、序列号基准值和事务标志。
     * 这些信息用于实现精确一次语义和事务性消息。
     */
    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            // 当批次关闭时会分配序列号。如果向分区leader的ProduceRequest失败并可重试，
            // 批次会重新入队。此时不应再次设置状态，因为更改已发送到broker的批次的producerId和序列号
            // 可能会导致消息重复。
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        // 设置生产者ID、epoch、序列号基准值和事务标志
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    /**
     * 覆盖批次中最后一条记录的偏移量。
     * 只能在记录构建完成之前调用此方法。
     */
    public void overrideLastOffset(long lastOffset) {
        // 如果记录已经构建完成，则不允许覆盖最后的偏移量
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    /**
     * 释放记录追加所需的资源（如压缩缓冲区）。
     * 调用此方法后，只能更新RecordBatch头部。
     * 这个方法对于内存管理很重要，特别是在使用压缩时。
     */
    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                // 关闭追加流，释放相关资源
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                // 将追加流设置为已关闭状态
                appendStream = CLOSED_STREAM;
            }
        }
    }

    /**
     * 中止当前批次的构建过程。
     * 这会释放资源并将缓冲区位置重置到初始位置。
     */
    public void abort() {
        // 关闭记录追加
        closeForRecordAppends();
        // 重置缓冲区位置到初始位置
        buffer().position(initialPosition);
        // 标记批次已中止
        aborted = true;
    }

    /**
     * 重新打开批次并重写生产者状态。
     * 用于在需要重新处理批次时更新生产者相关信息。
     */
    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        // 不能重新打开已中止的批次
        if (aborted)
            throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        // 清除已构建的记录
        builtRecords = null;
        // 更新生产者状态信息
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    /**
     * 关闭记录构建器并完成批次构建。
     * 这个方法会处理压缩、写入批次头部，并创建最终的MemoryRecords对象。
     */
    public void close() {
        // 检查批次是否已中止
        if (aborted)
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");

        // 如果记录已经构建完成，直接返回
        if (builtRecords != null)
            return;

        // 验证生产者状态
        validateProducerState();

        // 关闭记录追加
        closeForRecordAppends();

        if (numRecords == 0L) {
            // 如果没有记录，重置缓冲区位置并返回空记录
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            // 根据消息格式版本选择不同的头部写入方式
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                // 计算新版本格式的实际压缩率
                this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            else if (compression.type() != CompressionType.NONE)
                // 计算旧版本格式的实际压缩率
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;

            // 创建最终的MemoryRecords对象
            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void validateProducerState() {
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence < 0 && !isControlBatch)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }
    }

    /**
     * Write the header to the default batch.
     * @return the written compressed bytes.
     */
    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);
        int size = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            maxTimestamp = logAppendTime;
        else
            maxTimestamp = this.maxTimestamp;

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compression.type(), timestampType,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                hasDeleteHorizonMs(), partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * 写入旧版本（legacy）批次的头部信息。
     * 该方法用于处理旧版本消息格式的批次头部写入，包括压缩和时间戳信息的处理。
     * 
     * @return 写入的压缩字节数
     */
    private int writeLegacyCompressedWrapperHeader() {
        // 确保记录批次可写入
        ensureOpenForRecordBatchWrite();
        // 获取底层缓冲区
        ByteBuffer buffer = bufferStream.buffer();
        // 保存当前位置
        int pos = buffer.position();
        // 将位置重置到批次开始处
        buffer.position(initialPosition);

        // 计算包装器大小（减去日志开销）
        int wrapperSize = pos - initialPosition - Records.LOG_OVERHEAD;
        // 计算实际压缩的字节数（减去记录开销）
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        // 写入旧版本批次头部（包含最后偏移量和大小信息）
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        // 根据时间戳类型确定使用的时间戳
        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        // 写入压缩记录头部（包含魔数、大小、时间戳、压缩类型等信息）
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compression.type(), timestampType);

        // 恢复缓冲区位置到原来的位置
        buffer.position(pos);
        // 返回压缩后的字节数
        return writtenCompressed;
    }

    /**
     * 在指定偏移量处追加一条新记录。
     * 该方法处理记录的追加操作，包括各种验证检查和格式版本的兼容性处理。
     * 
     * @param offset 记录的绝对偏移量
     * @param isControlRecord 是否为控制记录
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部信息数组
     */
    private void appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                  ByteBuffer value, Header[] headers) {
        try {
            // 验证控制记录只能追加到控制批次中
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            // 确保偏移量单调递增
            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %d following previous offset %d " +
                        "(Offsets must increase monotonically).", offset, lastOffset));

            // 验证时间戳的有效性
            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            // 检查消息格式版本是否支持记录头部
            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            // 如果基准时间戳未设置，则使用当前记录的时间戳
            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            // 根据消息格式版本选择不同的追加方式
            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                // 新版本格式：追加默认记录
                appendDefaultRecord(offset, timestamp, key, value, headers);
            } else {
                // 旧版本格式：追加传统记录
                appendLegacyRecord(offset, timestamp, key, value, magic);
            }
        } catch (IOException e) {
            // 处理I/O异常
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     */
    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * 在指定偏移量处追加一条新记录。
     * 这是最完整的非控制记录追加方法，支持指定消息头部。
     * 
     * @param offset 记录在日志缓冲区中的绝对偏移量
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部信息数组，如果有的话
     */
    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 调用内部方法追加记录，isControl参数为false表示这是普通记录而非控制记录
        appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * 在指定偏移量处追加一条新记录。
     * 这个重载方法接受byte数组形式的键值对，内部会将其包装为ByteBuffer。
     * 
     * @param offset 记录在日志缓冲区中的绝对偏移量
     * @param timestamp 记录的时间戳
     * @param key 记录的键（字节数组）
     * @param value 记录的值（字节数组）
     */
    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        // 将byte数组包装为ByteBuffer，并使用空消息头追加记录
        appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * 在指定偏移量处追加一条新记录。
     * 这个重载方法接受ByteBuffer形式的键值对，但不包含消息头。
     * 
     * @param offset 记录在日志缓冲区中的绝对偏移量
     * @param timestamp 记录的时间戳
     * @param key 记录的键（ByteBuffer）
     * @param value 记录的值（ByteBuffer）
     */
    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        // 使用空消息头追加记录
        appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 在指定偏移量处追加一条新记录。
     * 这个重载方法直接接受SimpleRecord对象，从中提取所需的所有字段。
     * 
     * @param offset 记录在日志缓冲区中的绝对偏移量
     * @param record 要追加的SimpleRecord对象
     */
    public void appendWithOffset(long offset, SimpleRecord record) {
        // 从SimpleRecord中提取所有必要字段并追加记录
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * 在指定偏移量处追加一条控制记录。
     * 控制记录用于特殊用途（如事务标记），其类型必须是已知的，否则会抛出异常。
     *
     * @param offset 记录在日志缓冲区中的绝对偏移量
     * @param record 要追加的SimpleRecord对象（作为控制记录）
     * @throws IllegalArgumentException 如果控制记录类型未知
     */
    public void appendControlRecordWithOffset(long offset, SimpleRecord record) {
        // 从记录的key中解析控制记录类型ID
        short typeId = ControlRecordType.parseTypeId(record.key());
        // 根据类型ID获取控制记录类型
        ControlRecordType type = ControlRecordType.fromTypeId(typeId);
        // 如果是未知的控制记录类型，抛出异常
        if (type == ControlRecordType.UNKNOWN)
            throw new IllegalArgumentException("Cannot append record with unknown control record type " + typeId);

        // 追加控制记录，isControl参数为true表示这是一个控制记录
        appendWithOffset(offset, true, record.timestamp(),
            record.key(), record.value(), record.headers());
    }

    /**
     * 在下一个连续偏移量处追加一条新记录。
     * 这是一个简化的方法重载，不包含消息头信息。
     * 
     * @param timestamp 记录的时间戳，表示消息的创建时间或日志追加时间
     * @param key 记录的键，用于消息分区和日志压缩
     * @param value 记录的值，即实际的消息内容
     */
    public void append(long timestamp, ByteBuffer key, ByteBuffer value) {
        // 调用包含空消息头的重载方法，Record.EMPTY_HEADERS是一个空的消息头数组
        append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 在下一个连续偏移量处追加一条新记录。
     * 这是主要的追加方法，支持完整的记录格式，包括消息头。
     * 
     * @param timestamp 记录的时间戳，表示消息的创建时间或日志追加时间
     * @param key 记录的键，用于消息分区和日志压缩
     * @param value 记录的值，即实际的消息内容
     * @param headers 记录的消息头数组，包含额外的元数据信息
     */
    public void append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 调用带偏移量的追加方法，使用nextSequentialOffset()获取下一个连续的偏移量
        appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * 在下一个连续偏移量处追加一条新记录。
     * 这是一个接受字节数组参数的便捷方法重载。
     * 
     * @param timestamp 记录的时间戳，表示消息的创建时间或日志追加时间
     * @param key 记录的键的字节数组形式
     * @param value 记录的值的字节数组形式
     */
    public void append(long timestamp, byte[] key, byte[] value) {
        // 将字节数组包装为ByteBuffer（允许为null），并使用空消息头追加记录
        append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * 在下一个连续的偏移量位置追加一条新记录。
     * 这是一个基础的追加方法，用于添加普通的消息记录。
     *
     * @param timestamp 记录的时间戳
     * @param key 记录的键（可以为null）
     * @param value 记录的值（可以为null）
     * @param headers 记录的头部信息（如果有的话）
     */
    public void append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        // 将key和value包装为ByteBuffer，并调用重载的append方法
        append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * 在下一个连续的偏移量位置追加一条新记录。
     * 这是一个便捷方法，直接接受SimpleRecord对象作为参数。
     *
     * @param record 要追加的记录对象
     */
    public void append(SimpleRecord record) {
        // 获取下一个连续偏移量并追加记录
        appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * 在下一个连续的偏移量位置追加一条控制记录。
     * 控制记录用于特殊的系统操作，如事务控制、分区leader变更等。
     *
     * @param timestamp 记录的时间戳
     * @param type 控制记录类型（不能为UNKNOWN）
     * @param value 控制记录的值
     */
    public void appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        // 获取控制记录的键结构
        Struct keyStruct = type.recordKey();
        // 分配一个新的ByteBuffer来存储键
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        // 将键结构写入ByteBuffer
        keyStruct.writeTo(key);
        // 翻转ByteBuffer，准备读取
        key.flip();
        // 使用空的头部信息追加控制记录
        appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 追加事务结束标记。
     * 用于标记事务的提交或中止状态。
     *
     * @param timestamp 记录的时间戳
     * @param marker 事务结束标记对象
     */
    public void appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        // 检查是否有有效的生产者ID
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        // 检查批次是否启用了事务
        if (!isTransactional)
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        // 序列化标记值
        ByteBuffer value = marker.serializeValue();
        // 追加控制记录
        appendControlRecord(timestamp, marker.controlType(), value);
    }

    /**
     * 追加leader变更消息。
     * 用于记录分区leader的变更信息。
     *
     * @param timestamp 记录的时间戳
     * @param leaderChangeMessage leader变更消息对象
     */
    public void appendLeaderChangeMessage(long timestamp, LeaderChangeMessage leaderChangeMessage) {
        // 检查分区leader的epoch是否有效
        if (partitionLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            throw new IllegalArgumentException("Partition leader epoch must be valid, but get " + partitionLeaderEpoch);
        }
        // 追加leader变更控制记录
        appendControlRecord(
            timestamp,
            ControlRecordType.LEADER_CHANGE,
            MessageUtil.toByteBuffer(leaderChangeMessage, ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION)
        );
    }

    /**
     * 追加快照头部消息。
     * 用于记录快照相关的元数据信息。
     *
     * @param timestamp 记录的时间戳
     * @param snapshotHeaderRecord 快照头部记录对象
     */
    public void appendSnapshotHeaderMessage(long timestamp, SnapshotHeaderRecord snapshotHeaderRecord) {
        // 追加快照头部控制记录
        appendControlRecord(
            timestamp,
            ControlRecordType.SNAPSHOT_HEADER,
            MessageUtil.toByteBuffer(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
        );
    }

    public void appendSnapshotFooterMessage(long timestamp, SnapshotFooterRecord snapshotHeaderRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.SNAPSHOT_FOOTER,
            MessageUtil.toByteBuffer(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION)
        );
    }

    public void appendKRaftVersionMessage(long timestamp, KRaftVersionRecord kraftVersionRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.KRAFT_VERSION,
            MessageUtil.toByteBuffer(kraftVersionRecord, ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
        );
    }

    public void appendVotersMessage(long timestamp, VotersRecord votersRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.KRAFT_VOTERS,
            MessageUtil.toByteBuffer(votersRecord, ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
        );
    }

    /**
     * Add a legacy record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a record without doing offset/magic validation (this should only be used in testing).
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, SimpleRecord record) throws IOException {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int offsetDelta = (int) (offset - baseOffset);
            long timestamp = record.timestamp();
            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            int sizeInBytes = DefaultRecord.writeTo(appendStream,
                offsetDelta,
                timestamp - baseTimestamp,
                record.key(),
                record.value(),
                record.headers());
            recordWritten(offset, timestamp, sizeInBytes);
        } else {
            LegacyRecord legacyRecord = LegacyRecord.create(magic,
                record.timestamp(),
                Utils.toNullableArray(record.key()),
                Utils.toNullableArray(record.value()));
            appendUncheckedWithOffset(offset, legacyRecord);
        }
    }

    /**
     * Append a record at the next sequential offset.
     * @param record the record to add
     */
    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a log record using a different offset
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended record.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    /**
     * 在下一个连续的偏移量位置追加记录。如果还没有追加过任何记录，则使用构建器的基准偏移量。
     * 这个方法主要用于追加旧版本格式的记录。
     * 
     * @param record 要添加的旧版本格式记录
     */
    public void append(LegacyRecord record) {
        // 调用appendWithOffset方法，使用下一个连续的偏移量追加记录
        appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * 追加默认格式的记录到内存中。
     * 这个方法处理新版本格式(magic >= 2)的记录追加，支持记录头部和更多的元数据。
     * 
     * @param offset 记录的绝对偏移量
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部数组
     * @throws IOException 如果写入过程中发生I/O错误
     */
    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        // 确保记录追加器处于打开状态
        ensureOpenForRecordAppend();
        // 计算相对于批次基准偏移量的偏移量增量
        int offsetDelta = (int) (offset - baseOffset);
        // 计算相对于批次基准时间戳的时间戳增量
        long timestampDelta = timestamp - baseTimestamp;
        // 将记录写入输出流，并获取写入的字节数
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        // 更新记录写入的统计信息
        recordWritten(offset, timestamp, sizeInBytes);
    }

    /**
     * 追加旧版本格式的记录到内存中。
     * 这个方法处理旧版本格式(magic < 2)的记录追加，维护与旧版本的兼容性。
     * 
     * @param offset 记录的绝对偏移量
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param magic 消息格式版本号
     * @return 记录的CRC校验值
     * @throws IOException 如果写入过程中发生I/O错误
     */
    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, byte magic) throws IOException {
        // 确保记录追加器处于打开状态
        ensureOpenForRecordAppend();

        // 计算记录的大小（不包括头部开销）
        int size = LegacyRecord.recordSize(magic, key, value);
        // 写入记录头部，包含内部偏移量和记录大小
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        // 如果使用日志追加时间，则使用配置的logAppendTime作为时间戳
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;
        // 写入记录内容并计算CRC校验值
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        // 更新记录写入的统计信息，包括日志开销
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    /**
     * 计算记录的内部偏移量。
     * 对于压缩的消息批次，需要使用相对偏移量；对于未压缩的消息，使用绝对偏移量。
     * 
     * @param offset 记录的绝对偏移量
     * @return 用于写入的内部偏移量
     */
    private long toInnerOffset(long offset) {
        // 对于magic > 0且启用压缩的情况，使用相对偏移量
        if (magic > 0 && compression.type() != CompressionType.NONE)
            return offset - baseOffset;
        // 其他情况使用绝对偏移量
        return offset;
    }

    /**
     * 更新记录写入后的各项统计信息。
     * 这个方法在每次写入记录后调用，用于维护批次的状态信息。
     * 
     * @param offset 记录的绝对偏移量
     * @param timestamp 记录的时间戳
     * @param size 记录的大小（字节数）
     */
    private void recordWritten(long offset, long timestamp, int size) {
        // 检查记录数量是否超过最大限制
        if (numRecords == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        // 检查偏移量增量是否超过最大限制
        if (offset - baseOffset > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset +
                    ", last offset: " + offset);

        // 更新记录计数
        numRecords += 1;
        // 更新未压缩的记录总大小
        uncompressedRecordsSizeInBytes += size;
        // 更新最后一条记录的偏移量
        lastOffset = offset;

        // 对于新版本格式的记录，更新最大时间戳及其对应的偏移量
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM)
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    /**
     * 获取已写入字节数的估算值(基于{@link CompressionType}中硬编码的估算因子)。
     * 此方法用于预估记录批次的总字节大小，以便进行内存分配和批次大小控制。
     * 
     * @return 估算的已写入字节数
     */
    private int estimatedBytesWritten() {
        // 如果不使用压缩，直接返回批次头部大小加上未压缩的记录大小
        if (compression.type() == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedRecordsSizeInBytes;
        } else {
            // 如果使用压缩，基于未压缩字节数估算写入到底层ByteBuffer的字节数
            // 计算公式: 批次头部大小 + (未压缩记录大小 * 预估压缩率 * 压缩率估算因子)
            // 其中:
            // - estimatedCompressionRatio: 基于历史压缩效果的预估压缩率
            // - COMPRESSION_RATE_ESTIMATION_FACTOR: 额外的安全系数(1.05)，用于应对压缩率波动
            return batchHeaderSizeInBytes + (int) (uncompressedRecordsSizeInBytes * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Set the estimated compression ratio for the memory records builder.
     */
    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    /**
     * 检查是否有足够空间添加包含给定key/value对的新记录。如果当前批次还没有任何记录，则返回true。
     * 
     * @param timestamp 记录的时间戳
     * @param key 记录的键（字节数组）
     * @param value 记录的值（字节数组）
     * @param headers 记录的头部信息
     * @return 如果有足够空间则返回true，否则返回false
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        // 将字节数组包装为ByteBuffer并调用重载方法
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * 检查是否有足够空间添加包含给定key/value对的新记录。如果当前批次还没有任何记录，则返回true。
     * 
     * 注意：返回值是基于写入压缩器的字节估算，当使用压缩时可能不准确。这种情况下，后续的追加操作
     * 可能会导致底层字节缓冲流进行动态缓冲区重新分配。
     *
     * @param timestamp 记录的时间戳
     * @param key 记录的键（ByteBuffer）
     * @param value 记录的值（ByteBuffer）
     * @param headers 记录的头部信息
     * @return 如果有足够空间则返回true，否则返回false
     */
    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 如果批次已满，直接返回false
        if (isFull())
            return false;

        // 如果是第一条记录，总是允许追加（ByteBufferOutputStream会根据需要自动增长）
        if (numRecords == 0)
            return true;

        final int recordSize;
        // 根据不同的魔数版本计算记录大小
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            // 旧版本格式：记录大小 = 日志开销 + 记录实际大小
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            // 新版本格式：计算偏移量增量和时间戳增量
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = baseTimestamp == null ? 0 : timestamp - baseTimestamp;
            // 使用DefaultRecord计算记录大小，包含所有元数据
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        // 保守估计：不考虑新记录的压缩效果
        // 检查写入限制是否足够容纳估算的字节数
        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    /**
     * Check if we have room for a given number of bytes.
     */
    public boolean hasRoomFor(int estimatedRecordsSize) {
        if (isFull()) return false;
        return this.writeLimit >= estimatedBytesWritten() + estimatedRecordsSize;
    }

    public int maxAllowedBytes() {
        return this.writeLimit - this.batchHeaderSizeInBytes;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    /**
     * 判断当前记录批次是否已满，满足以下任一条件时返回true：
     * 1. 追加流已关闭（appendStream == CLOSED_STREAM）
     * 2. 已添加至少一条记录（numRecords > 0）且预估的已写入字节数超过写入限制（writeLimit）
     * 
     * 特别说明：写入限制（writeLimit）仅在添加第一条记录后才会生效，这样设计是为了确保：
     * - 即使producer的batch.size设置为0（用于禁用批处理），我们依然能创建非空的批次
     * - 避免出现完全无法写入记录的情况
     * 
     * @return 如果批次已满返回true，否则返回false
     */
    public boolean isFull() {
        // 注意：写入限制仅在添加首条记录后才生效，这确保了即使在禁用批处理的情况下（producer的batch.size=0）
        // 我们也总能创建非空的批次
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    /**
     * 获取写入底层缓冲区的字节数估计值。
     * 在以下两种情况下，返回值是完全准确的：
     * 1. 记录集未被压缩
     * 2. 构建器已经被关闭
     * 
     * @return 估计的字节数
     */
    public int estimatedSizeInBytes() {
        // 如果记录已经构建完成，直接返回实际大小；否则返回估计值
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    /**
     * 获取记录批次的魔数版本。
     * 魔数用于标识消息格式的版本，不同版本支持不同的特性。
     * 
     * @return 魔数版本值
     */
    public byte magic() {
        return magic;
    }

    /**
     * 获取下一个顺序偏移量。
     * 如果当前没有最后一条记录的偏移量，则返回基准偏移量；
     * 否则返回最后一条记录偏移量加1。
     * 
     * @return 下一个顺序偏移量
     */
    private long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
    }

    /**
     * 记录信息类，用于存储记录批次的时间戳相关信息。
     * 包含最大时间戳和具有最大时间戳的记录的浅层偏移量。
     */
    public static class RecordsInfo {
        /** 记录批次中的最大时间戳 */
        public final long maxTimestamp;
        /** 
         * 具有最大时间戳的记录批次的最后一个偏移量。
         * 如果多个批次具有相同的最大时间戳，则选择最早的批次。
         */
        public final long shallowOffsetOfMaxTimestamp;

        /**
         * 构造记录信息对象
         * 
         * @param maxTimestamp 最大时间戳
         * @param shallowOffsetOfMaxTimestamp 具有最大时间戳的记录批次的最后一个偏移量
         */
        public RecordsInfo(long maxTimestamp,
                           long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    /**
     * Return the producer id of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}
