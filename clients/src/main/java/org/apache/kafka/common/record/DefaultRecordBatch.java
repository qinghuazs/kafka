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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Crc32C;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * 实现magic 2及以上版本的RecordBatch。记录批次的结构如下:
 *
 * RecordBatch =>
 *  BaseOffset => Int64                // 批次中第一条消息的偏移量
 *  Length => Int32                    // 整个批次的长度(字节)
 *  PartitionLeaderEpoch => Int32      // 分区leader的epoch值
 *  Magic => Int8                      // 消息格式版本号
 *  CRC => Uint32                      // 校验和
 *  Attributes => Int16                // 属性标记位
 *  LastOffsetDelta => Int32           // 最后一条消息相对于基准偏移量的差值,同时也用作最后序列号的差值
 *  BaseTimestamp => Int64             // 批次中第一条消息的时间戳
 *  MaxTimestamp => Int64              // 批次中最大的时间戳
 *  ProducerId => Int64                // 生产者ID
 *  ProducerEpoch => Int16             // 生产者epoch值
 *  BaseSequence => Int32              // 基准序列号
 *  RecordsCount => Int32              // 记录数量
 *  Records => [Record]                // 实际的记录数组
 *
 * 注意:当启用压缩时(见下面的attributes),压缩后的记录数据会直接跟在记录数量字段后面进行序列化。
 *
 * CRC校验覆盖从attributes到批次末尾的所有数据(即CRC之后的所有字节)。CRC位于magic byte之后,
 * 这意味着客户端必须先解析magic byte才能决定如何解释批次长度和magic byte之间的字节。
 * 分区leader epoch字段不包含在CRC计算中,以避免在broker接收到每个批次时都需要重新计算CRC。
 * CRC计算使用CRC-32C(Castagnoli)多项式。
 *
 * 关于日志压缩:
 * 与旧的消息格式不同,magic v2及以上版本在日志清理时会保留原始批次的第一个和最后一个偏移量/序列号。
 * 这是为了在重新加载日志时能够恢复生产者的状态。如果不保留最后的序列号,那么在分区leader失败后,
 * 一旦新leader从日志中重建生产者状态,预期的下一个序列号将不再与客户端写入的序列号同步。
 * 这会导致致命的OutOfOrderSequence错误。基准序列号必须保留用于重复检查:broker通过验证传入批次的
 * 第一个和最后一个序列号是否与该生产者的最后一个序列号匹配来检查重复的Produce请求。
 *
 * 注意:如果在压缩过程中批次中的所有记录都被删除,broker可能仍会保留一个空的批次头以保留上述生产者序列信息。
 * 这些空批次仅保留到对应的生产者写入新的序列号或由于缺乏活动而使producerId过期。
 *
 * 压缩后不需要保留原始批次的时间戳。因此BaseTimestamp字段在大多数情况下反映批次中第一条记录的时间戳。
 * 如果批次为空,BaseTimestamp将设置为-1(NO_TIMESTAMP)。如果delete horizon标志设置为1,
 * BaseTimestamp将设置为应删除墓碑记录和中止事务标记的时间。
 *
 * 类似地,如果时间戳类型是CREATE_TIME,MaxTimestamp字段反映当前记录的最大时间戳。
 * 对于LOG_APPEND_TIME,MaxTimestamp字段反映broker设置的时间戳并在压缩后保留。
 * 此外,空批次的MaxTimestamp始终保留变空之前的值。
 *
 * 当前的属性位如下:
 *
 *  ---------------------------------------------------------------------------------------------------------------------------
 *  | 未使用 (7-15) | 删除时间标记 (6) | 控制消息 (5) | 事务消息 (4) | 时间戳类型 (3) | 压缩类型 (0-2) |
 *  ---------------------------------------------------------------------------------------------------------------------------
 */
public class DefaultRecordBatch extends AbstractRecordBatch implements MutableRecordBatch {
    // 记录批次各字段在缓冲区中的偏移量和长度定义
    // 基准偏移量字段,占8字节,从缓冲区起始位置开始
    static final int BASE_OFFSET_OFFSET = 0;
    static final int BASE_OFFSET_LENGTH = 8;
    
    // 批次长度字段,占4字节
    static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int LENGTH_LENGTH = 4;
    
    // 分区leader epoch字段,占4字节
    static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
    
    // 消息格式版本号字段,占1字节
    static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
    static final int MAGIC_LENGTH = 1;
    
    // CRC校验和字段,占4字节
    public static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int CRC_LENGTH = 4;
    
    // 属性字段,占2字节
    static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    
    // 最后一条消息的偏移量增量,占4字节
    public static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    
    // 基准时间戳字段,占8字节
    static final int BASE_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int BASE_TIMESTAMP_LENGTH = 8;
    
    // 最大时间戳字段,占8字节
    static final int MAX_TIMESTAMP_OFFSET = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
    static final int MAX_TIMESTAMP_LENGTH = 8;
    
    // 生产者ID字段,占8字节
    static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
    static final int PRODUCER_ID_LENGTH = 8;
    
    // 生产者epoch字段,占2字节
    static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
    static final int PRODUCER_EPOCH_LENGTH = 2;
    
    // 基准序列号字段,占4字节
    static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
    static final int BASE_SEQUENCE_LENGTH = 4;
    
    // 记录数量字段,占4字节
    public static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
    static final int RECORDS_COUNT_LENGTH = 4;
    
    // 实际记录数据的起始位置
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    
    // 记录批次头部的总大小
    public static final int RECORD_BATCH_OVERHEAD = RECORDS_OFFSET;

    // 属性字段中各标志位的掩码定义
    // 压缩类型掩码(0-2位),支持8种压缩类型
    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    // 事务标志掩码(第4位),1表示事务消息
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
    // 控制消息标志掩码(第5位),1表示控制消息
    private static final int CONTROL_FLAG_MASK = 0x20;
    // 删除时间标记掩码(第6位),1表示设置了删除时间
    private static final byte DELETE_HORIZON_FLAG_MASK = 0x40;
    // 时间戳类型掩码(第3位),0表示CREATE_TIME,1表示LOG_APPEND_TIME
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    // 存储记录批次数据的字节缓冲区
    private final ByteBuffer buffer;

    /**
     * 构造函数,创建一个新的DefaultRecordBatch实例
     * 
     * @param buffer 包含记录批次数据的字节缓冲区
     */
    DefaultRecordBatch(ByteBuffer buffer) {
        this.buffer = buffer; // 初始化存储记录批次数据的缓冲区
    }

    /**
     * 获取记录批次的magic值(消息格式版本号)
     * 
     * @return magic值,表示消息格式版本
     */
    @Override
    public byte magic() {
        return buffer.get(MAGIC_OFFSET); // 从缓冲区指定位置读取magic值
    }

    /**
     * 验证记录批次的有效性
     * 检查批次大小是否合法以及CRC校验和是否匹配
     * 
     * @throws CorruptRecordException 如果记录批次损坏或无效
     */
    @Override
    public void ensureValid() {
        // 检查批次大小是否小于最小允许的开销
        if (sizeInBytes() < RECORD_BATCH_OVERHEAD)
            throw new CorruptRecordException("Record batch is corrupt (the size " + sizeInBytes() +
                    " is smaller than the minimum allowed overhead " + RECORD_BATCH_OVERHEAD + ")");

        // 验证CRC校验和
        if (!isValid())
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * 获取批次的基准时间戳
     * 用于计算记录的实际时间戳(通过时间戳增量)
     * 
     * @return 基准时间戳
     */
    public long baseTimestamp() {
        return buffer.getLong(BASE_TIMESTAMP_OFFSET); // 从缓冲区读取基准时间戳
    }

    /**
     * 获取批次中的最大时间戳
     * 
     * @return 批次中所有记录的最大时间戳
     */
    @Override
    public long maxTimestamp() {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET); // 从缓冲区读取最大时间戳
    }

    /**
     * 获取时间戳类型
     * 可以是CREATE_TIME(消息创建时间)或LOG_APPEND_TIME(消息追加时间)
     * 
     * @return 时间戳类型
     */
    @Override
    public TimestampType timestampType() {
        // 通过属性字段中的时间戳类型标志位判断
        return (attributes() & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

    /**
     * 获取批次的基准偏移量
     * 
     * @return 批次中第一条消息的偏移量
     */
    @Override
    public long baseOffset() {
        return buffer.getLong(BASE_OFFSET_OFFSET); // 从缓冲区读取基准偏移量
    }

    /**
     * 获取批次中最后一条消息的偏移量
     * 通过基准偏移量加上最后一条消息的偏移量增量计算
     * 
     * @return 最后一条消息的偏移量
     */
    @Override
    public long lastOffset() {
        return baseOffset() + lastOffsetDelta(); // 基准偏移量 + 最后消息的偏移量增量
    }

    /**
     * 获取生产者ID
     * 用于幂等性和事务特性
     * 
     * @return 生产者ID
     */
    @Override
    public long producerId() {
        return buffer.getLong(PRODUCER_ID_OFFSET); // 从缓冲区读取生产者ID
    }

    /**
     * 获取生产者epoch值
     * 用于处理生产者故障恢复
     * 
     * @return 生产者epoch值
     */
    @Override
    public short producerEpoch() {
        return buffer.getShort(PRODUCER_EPOCH_OFFSET); // 从缓冲区读取生产者epoch值
    }

    /**
     * 获取基准序列号
     * 用于消息排序和重复检测
     * 
     * @return 批次的基准序列号
     */
    @Override
    public int baseSequence() {
        return buffer.getInt(BASE_SEQUENCE_OFFSET); // 从缓冲区读取基准序列号
    }

    /**
     * 获取最后一条消息的偏移量增量
     * 
     * @return 最后一条消息相对于基准偏移量的增量值
     */
    private int lastOffsetDelta() {
        return buffer.getInt(LAST_OFFSET_DELTA_OFFSET); // 从缓冲区读取偏移量增量
    }

    /**
     * 获取批次中最后一条消息的序列号
     * 通过基准序列号和偏移量增量计算
     * 
     * @return 最后一条消息的序列号,如果没有序列号则返回NO_SEQUENCE
     */
    @Override
    public int lastSequence() {
        int baseSequence = baseSequence(); // 获取基准序列号
        // 如果没有序列号,返回NO_SEQUENCE
        if (baseSequence == RecordBatch.NO_SEQUENCE)
            return RecordBatch.NO_SEQUENCE;
        // 通过基准序列号和偏移量增量计算最后的序列号
        return incrementSequence(baseSequence, lastOffsetDelta());
    }

    /**
     * 获取记录批次的压缩类型
     * 通过属性字段的低3位(0-2位)确定使用的压缩算法
     * 
     * @return 压缩类型枚举值
     */
    @Override
    public CompressionType compressionType() {
        // 使用COMPRESSION_CODEC_MASK(0x07)提取属性字段的低3位,然后获取对应的压缩类型
        return CompressionType.forId(attributes() & COMPRESSION_CODEC_MASK);
    }

    /**
     * 计算记录批次的总大小(字节)
     * 包括日志开销(LOG_OVERHEAD)和批次长度
     * 
     * @return 记录批次的总字节数
     */
    @Override
    public int sizeInBytes() {
        // 总大小 = 日志开销 + 批次长度
        return LOG_OVERHEAD + buffer.getInt(LENGTH_OFFSET);
    }

    /**
     * 获取批次中的记录数量
     * 
     * @return 记录数量
     */
    private int count() {
        // 从缓冲区读取记录数量
        return buffer.getInt(RECORDS_COUNT_OFFSET);
    }

    /**
     * 获取批次中的记录数量,如果批次为空则返回null
     * 
     * @return 记录数量或null
     */
    @Override
    public Integer countOrNull() {
        return count(); // 直接返回记录数量
    }

    /**
     * 将记录批次写入ByteBuffer
     * 复制当前缓冲区的内容到目标缓冲区
     * 
     * @param buffer 目标ByteBuffer
     */
    @Override
    public void writeTo(ByteBuffer buffer) {
        // 复制当前缓冲区内容到目标缓冲区
        buffer.put(this.buffer.duplicate());
    }

    /**
     * 将记录批次写入ByteBufferOutputStream
     * 复制当前缓冲区的内容到输出流
     * 
     * @param outputStream 目标输出流
     */
    @Override
    public void writeTo(ByteBufferOutputStream outputStream) {
        // 复制当前缓冲区内容到输出流
        outputStream.write(this.buffer.duplicate());
    }

    /**
     * 检查记录批次是否是事务消息
     * 通过检查属性字段的事务标志位(第4位)判断
     * 
     * @return 如果是事务消息则返回true
     */
    @Override
    public boolean isTransactional() {
        // 检查事务标志位是否设置
        return (attributes() & TRANSACTIONAL_FLAG_MASK) > 0;
    }

    /**
     * 检查是否设置了删除时间标记
     * 通过检查属性字段的删除时间标志位(第6位)判断
     * 
     * @return 如果设置了删除时间则返回true
     */
    private boolean hasDeleteHorizonMs() {
        // 检查删除时间标志位是否设置
        return (attributes() & DELETE_HORIZON_FLAG_MASK) > 0;
    }

    /**
     * 获取删除时间戳
     * 如果设置了删除时间标记,则返回基准时间戳作为删除时间
     * 
     * @return 包含删除时间的OptionalLong,如果未设置则返回空
     */
    @Override
    public OptionalLong deleteHorizonMs() {
        if (hasDeleteHorizonMs())
            // 如果设置了删除时间标记,返回基准时间戳
            return OptionalLong.of(buffer.getLong(BASE_TIMESTAMP_OFFSET));
        else
            return OptionalLong.empty();
    }

    /**
     * 检查记录批次是否是控制消息
     * 通过检查属性字段的控制消息标志位(第5位)判断
     * 
     * @return 如果是控制消息则返回true
     */
    @Override
    public boolean isControlBatch() {
        // 检查控制消息标志位是否设置
        return (attributes() & CONTROL_FLAG_MASK) > 0;
    }

    /**
     * 获取分区leader的epoch值
     * 用于确保消息的一致性和处理leader切换
     * 
     * @return 分区leader的epoch值
     */
    @Override
    public int partitionLeaderEpoch() {
        // 从缓冲区读取分区leader epoch值
        return buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET);
    }

    /**
     * 创建用于读取记录数据的输入流
     * 如果记录批次使用了压缩,则返回解压缩后的输入流
     * 
     * @param bufferSupplier 用于分配临时缓冲区的供应器
     * @return 包含记录数据的输入流
     */
    public InputStream recordInputStream(BufferSupplier bufferSupplier) {
        // 复制缓冲区并将位置设置到记录数据的起始位置
        final ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        // 根据压缩类型创建相应的输入流
        return Compression.of(compressionType()).build().wrapForInput(buffer, magic(), bufferSupplier);
    }

    /**
     * 创建用于迭代压缩记录的迭代器
     * 支持跳过键值对的快速迭代模式
     * 
     * @param bufferSupplier 用于分配临时缓冲区的供应器
     * @param skipKeyValue 是否跳过键值对的读取
     * @return 记录迭代器
     */
    private CloseableIterator<Record> compressedIterator(BufferSupplier bufferSupplier, boolean skipKeyValue) {
        // 获取记录数据的输入流
        final InputStream inputStream = recordInputStream(bufferSupplier);

        if (skipKeyValue) {
            // 创建跳过键值对的迭代器
            return new StreamRecordIterator(inputStream) {
                @Override
                protected Record doReadRecord(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) throws IOException {
                    // 部分读取记录,跳过键值对
                    return DefaultRecord.readPartiallyFrom(inputStream, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                }
            };
        } else {
            // 创建完整读取记录的迭代器
            return new StreamRecordIterator(inputStream) {
                @Override
                protected Record doReadRecord(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) throws IOException {
                    // 完整读取记录,包括键值对
                    return DefaultRecord.readFrom(inputStream, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                }
            };
        }
    }

    /**
     * 创建未压缩记录的迭代器
     * 直接从ByteBuffer中读取记录,不需要解压缩
     * 
     * 应用场景:
     * 1. 处理未压缩的消息批次
     * 2. 提供高效的顺序读取
     * 
     * @return 未压缩记录的迭代器
     */
    private CloseableIterator<Record> uncompressedIterator() {
        // 复制缓冲区以避免影响原始数据
        final ByteBuffer buffer = this.buffer.duplicate();
        // 将位置设置到记录数据的起始位置
        buffer.position(RECORDS_OFFSET);
        return new RecordIterator() {
            @Override
            protected Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) {
                try {
                    // 从缓冲区读取下一条记录
                    return DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                } catch (BufferUnderflowException e) {
                    // 如果读取时发生缓冲区下溢,说明批次大小声明不正确
                    throw new InvalidRecordException("Incorrect declared batch size, premature EOF reached");
                }
            }
            @Override
            protected boolean ensureNoneRemaining() {
                // 检查是否还有剩余数据
                return !buffer.hasRemaining();
            }
            @Override
            public void close() {}
        };
    }

    /**
     * 获取记录批次的迭代器
     * 根据是否压缩选择不同的迭代策略
     * 
     * 应用场景:
     * 1. 遍历批次中的所有记录
     * 2. 处理压缩和未压缩的消息
     * 
     * @return 记录迭代器
     */
    @Override
    public Iterator<Record> iterator() {
        // 如果批次为空,返回空迭代器
        if (count() == 0)
            return Collections.emptyIterator();

        // 如果是未压缩的批次,使用未压缩迭代器
        if (!isCompressed())
            return uncompressedIterator();

        // 对于压缩的批次,由于无法确保底层压缩流会被关闭,
        // 这里先解压整个记录集。如果需要更低的内存占用,
        // 可以使用streamingIterator,但会增加复杂性
        try (CloseableIterator<Record> iterator = compressedIterator(BufferSupplier.NO_CACHING, false)) {
            // 创建列表存储解压后的记录
            List<Record> records = new ArrayList<>(count());
            while (iterator.hasNext())
                records.add(iterator.next());
            return records.iterator();
        }
    }

    /**
     * 创建跳过键值对的迭代器
     * 用于只需要读取记录元数据而不需要实际数据的场景
     * 
     * 应用场景:
     * 1. 日志验证时只需要验证记录的有效性
     * 2. 需要节省内存的场景
     * 
     * @param bufferSupplier 用于分配临时缓冲区的供应器
     * @return 可关闭的记录迭代器
     */
    @Override
    public CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier) {
        if (count() == 0) {
            return CloseableIterator.wrap(Collections.emptyIterator());
        }

        /*
         * 对于未压缩的迭代器,跳过键值对实际上并不值得,
         * 因为ByteBufferInputStream的skip()函数效率较低,
         * 它会分配新的字节数组,还不如直接读取数据
         */
        if (!isCompressed())
            return uncompressedIterator();

        // 使用可关闭的迭代器,这样调用者(如日志验证器)需要负责关闭它
        // 同时通过不提前解压整个记录集来节省内存占用
        return compressedIterator(bufferSupplier, true);
    }

    /**
     * 创建流式迭代器
     * 提供更低内存占用的迭代方式
     * 
     * 应用场景:
     * 1. 处理大型消息批次时需要控制内存使用
     * 2. 流式处理记录而不是一次性加载全部数据
     * 
     * @param bufferSupplier 用于分配临时缓冲区的供应器
     * @return 可关闭的记录迭代器
     */
    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        // 根据是否压缩选择相应的迭代器
        if (isCompressed())
            return compressedIterator(bufferSupplier, false);
        else
            return uncompressedIterator();
    }

    /**
     * 设置批次的最后偏移量
     * 通过调整基准偏移量来实现
     * 
     * 应用场景:
     * 1. 日志压缩时需要调整消息偏移量
     * 2. 消息转换时需要重新分配偏移量
     * 
     * @param offset 新的最后偏移量
     */
    @Override
    public void setLastOffset(long offset) {
        // 根据最后偏移量增量计算并设置新的基准偏移量
        buffer.putLong(BASE_OFFSET_OFFSET, offset - lastOffsetDelta());
    }

    /**
     * 设置批次的最大时间戳和时间戳类型
     * 同时更新相关的属性和CRC
     * 
     * 应用场景:
     * 1. 消息转换时需要更新时间戳
     * 2. 日志追加时设置新的时间戳
     * 
     * @param timestampType 时间戳类型
     * @param maxTimestamp 最大时间戳
     */
    @Override
    public void setMaxTimestamp(TimestampType timestampType, long maxTimestamp) {
        long currentMaxTimestamp = maxTimestamp();
        // 如果时间戳没有变化,不需要重新计算CRC
        if (timestampType() == timestampType && currentMaxTimestamp == maxTimestamp)
            return;

        // 计算新的属性值并更新
        byte attributes = computeAttributes(compressionType(), timestampType, isTransactional(), isControlBatch(), hasDeleteHorizonMs());
        buffer.putShort(ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(MAX_TIMESTAMP_OFFSET, maxTimestamp);
        // 重新计算并更新CRC
        long crc = computeChecksum();
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);
    }

    /**
     * 设置分区leader的epoch值
     * 
     * 应用场景:
     * 1. leader变更时更新epoch
     * 2. 副本同步时设置正确的epoch
     * 
     * @param epoch 新的epoch值
     */
    @Override
    public void setPartitionLeaderEpoch(int epoch) {
        // 将新的epoch值写入缓冲区
        buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    /**
     * 获取记录批次的CRC校验和
     * 
     * 应用场景:
     * 1. 验证记录批次的完整性
     * 2. 检测数据损坏
     * 
     * @return CRC校验和
     */
    @Override
    public long checksum() {
        // 从缓冲区读取CRC值
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * 检查记录批次是否有效
     * 通过验证大小和CRC校验和来判断
     * 
     * 应用场景:
     * 1. 接收消息时的有效性检查
     * 2. 日志验证
     * 
     * @return 如果记录批次有效则返回true
     */
    public boolean isValid() {
        // 检查大小是否合法且CRC校验和是否匹配
        return sizeInBytes() >= RECORD_BATCH_OVERHEAD && checksum() == computeChecksum();
    }

    /**
     * 计算记录批次的CRC校验和
     * 使用CRC32C算法计算从属性字段到缓冲区末尾的所有数据的校验和
     * 
     * @return 计算得到的CRC校验和
     */
    private long computeChecksum() {
        // 计算从属性字段开始到缓冲区末尾的CRC值
        return Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET);
    }

    /**
     * 获取记录批次的属性值
     * 属性字段包含了压缩类型、时间戳类型等标志位
     * 
     * @return 属性字段的值
     */
    private byte attributes() {
        // 注意:属性字段的第二个字节未使用
        return (byte) buffer.getShort(ATTRIBUTES_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecordBatch that = (DefaultRecordBatch) o;
        return Objects.equals(buffer, that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    /**
     * 计算记录批次的属性字段值
     * 属性字段是一个16位的标志位集合,用于存储批次的各种特性
     * 
     * 应用场景:
     * 1. 创建新的记录批次时设置其属性
     * 2. 在写入记录批次头部时使用
     * 
     * @param type 压缩类型,使用低3位(0-2)存储
     * @param timestampType 时间戳类型(CREATE_TIME或LOG_APPEND_TIME),使用第3位存储
     * @param isTransactional 是否是事务消息,使用第4位存储
     * @param isControl 是否是控制消息,使用第5位存储
     * @param isDeleteHorizonSet 是否设置了删除时间标记,使用第6位存储
     * @return 计算得到的属性字节值
     * @throws IllegalArgumentException 如果时间戳类型为NO_TIMESTAMP_TYPE
     */
    private static byte computeAttributes(CompressionType type, TimestampType timestampType,
                                          boolean isTransactional, boolean isControl, boolean isDeleteHorizonSet) {
        // 验证时间戳类型必须指定
        if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for message " +
                    "format v2 and above");

        // 初始化属性值,如果是事务消息则设置事务标志位
        byte attributes = isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
        
        // 如果是控制消息,设置控制消息标志位
        if (isControl)
            attributes |= CONTROL_FLAG_MASK;
            
        // 如果使用了压缩,设置压缩类型(低3位)
        if (type.id > 0)
            attributes |= (byte) (COMPRESSION_CODEC_MASK & type.id);
            
        // 如果是LOG_APPEND_TIME类型,设置时间戳类型标志位
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            attributes |= TIMESTAMP_TYPE_MASK;
            
        // 如果设置了删除时间,设置删除时间标志位
        if (isDeleteHorizonSet)
            attributes |= DELETE_HORIZON_FLAG_MASK;
            
        return attributes;
    }

    /**
     * 写入空记录批次的头部信息
     * 用于创建不包含实际记录的批次头部,主要用于保留生产者状态信息
     * 
     * 应用场景:
     * 1. 在日志压缩后创建空批次以保留生产者状态
     * 2. 在事务操作中标记事务状态
     * 
     * @param buffer 目标字节缓冲区
     * @param magic 消息格式版本号
     * @param producerId 生产者ID
     * @param producerEpoch 生产者epoch值
     * @param baseSequence 基准序列号
     * @param baseOffset 基准偏移量
     * @param lastOffset 最后一条消息的偏移量
     * @param partitionLeaderEpoch 分区leader的epoch值
     * @param timestampType 时间戳类型
     * @param timestamp 时间戳值
     * @param isTransactional 是否是事务消息
     * @param isControlRecord 是否是控制消息
     */
    public static void writeEmptyHeader(ByteBuffer buffer,
                                        byte magic,
                                        long producerId,
                                        short producerEpoch,
                                        int baseSequence,
                                        long baseOffset,
                                        long lastOffset,
                                        int partitionLeaderEpoch,
                                        TimestampType timestampType,
                                        long timestamp,
                                        boolean isTransactional,
                                        boolean isControlRecord) {
        // 计算最后一条消息相对于基准偏移量的差值
        int offsetDelta = (int) (lastOffset - baseOffset);
        
        // 调用writeHeader方法写入完整的头部信息
        // 由于是空批次,使用NONE压缩类型,NO_TIMESTAMP基准时间戳,记录数量为0
        writeHeader(buffer, baseOffset, offsetDelta, DefaultRecordBatch.RECORD_BATCH_OVERHEAD, magic,
                    CompressionType.NONE, timestampType, RecordBatch.NO_TIMESTAMP, timestamp, producerId,
                    producerEpoch, baseSequence, isTransactional, isControlRecord, false, partitionLeaderEpoch, 0);
    }

    /**
     * 写入记录批次的完整头部信息
     * 按照预定义的格式将所有头部字段写入字节缓冲区
     * 
     * 应用场景:
     * 1. 创建新的记录批次时写入头部
     * 2. 在日志段写入时序列化记录批次
     * 
     * 设计考虑:
     * 1. 字段写入顺序按照批次格式定义的布局
     * 2. CRC校验和计算范围从属性字段到批次末尾
     * 3. 分区leader epoch不包含在CRC计算中
     * 
     * @param buffer 目标字节缓冲区
     * @param baseOffset 基准偏移量
     * @param lastOffsetDelta 最后一条消息的偏移量增量
     * @param sizeInBytes 批次总大小(字节)
     * @param magic 消息格式版本号
     * @param compressionType 压缩类型
     * @param timestampType 时间戳类型
     * @param baseTimestamp 基准时间戳
     * @param maxTimestamp 最大时间戳
     * @param producerId 生产者ID
     * @param epoch 生产者epoch值
     * @param sequence 基准序列号
     * @param isTransactional 是否是事务消息
     * @param isControlBatch 是否是控制批次
     * @param isDeleteHorizonSet 是否设置了删除时间标记
     * @param partitionLeaderEpoch 分区leader的epoch值
     * @param numRecords 记录数量
     * @throws IllegalArgumentException 如果magic值小于当前版本或基准时间戳无效
     */
    public static void writeHeader(ByteBuffer buffer,
                                   long baseOffset,
                                   int lastOffsetDelta,
                                   int sizeInBytes,
                                   byte magic,
                                   CompressionType compressionType,
                                   TimestampType timestampType,
                                   long baseTimestamp,
                                   long maxTimestamp,
                                   long producerId,
                                   short epoch,
                                   int sequence,
                                   boolean isTransactional,
                                   boolean isControlBatch,
                                   boolean isDeleteHorizonSet,
                                   int partitionLeaderEpoch,
                                   int numRecords) {
        // 验证magic值必须大于等于当前版本
        if (magic < RecordBatch.CURRENT_MAGIC_VALUE)
            throw new IllegalArgumentException("Invalid magic value " + magic);
            
        // 验证基准时间戳必须是有效值或NO_TIMESTAMP
        if (baseTimestamp < 0 && baseTimestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + baseTimestamp);

        // 计算属性字段值
        short attributes = computeAttributes(compressionType, timestampType, isTransactional, isControlBatch, isDeleteHorizonSet);

        // 获取缓冲区当前位置
        int position = buffer.position();
        
        // 按照预定义的格式顺序写入各个字段
        buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset);           // 基准偏移量
        buffer.putInt(position + LENGTH_OFFSET, sizeInBytes - LOG_OVERHEAD); // 批次长度(不包括日志开销)
        buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch); // 分区leader epoch
        buffer.put(position + MAGIC_OFFSET, magic);                          // 消息格式版本号
        buffer.putShort(position + ATTRIBUTES_OFFSET, attributes);           // 属性字段
        buffer.putLong(position + BASE_TIMESTAMP_OFFSET, baseTimestamp);     // 基准时间戳
        buffer.putLong(position + MAX_TIMESTAMP_OFFSET, maxTimestamp);       // 最大时间戳
        buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta); // 最后消息的偏移量增量
        buffer.putLong(position + PRODUCER_ID_OFFSET, producerId);           // 生产者ID
        buffer.putShort(position + PRODUCER_EPOCH_OFFSET, epoch);            // 生产者epoch
        buffer.putInt(position + BASE_SEQUENCE_OFFSET, sequence);            // 基准序列号
        buffer.putInt(position + RECORDS_COUNT_OFFSET, numRecords);          // 记录数量
        
        // 计算CRC校验和(从属性字段到批次末尾)
        long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
        buffer.putInt(position + CRC_OFFSET, (int) crc);                    // 写入CRC校验和
        
        // 将缓冲区位置移动到记录数据的起始位置
        buffer.position(position + RECORD_BATCH_OVERHEAD);
    }

    @Override
    public String toString() {
        return "RecordBatch(magic=" + magic() + ", offsets=[" + baseOffset() + ", " + lastOffset() + "], " +
                "sequence=[" + baseSequence() + ", " + lastSequence() + "], " +
                "isTransactional=" + isTransactional() + ", isControlBatch=" + isControlBatch() + ", " +
                "compression=" + compressionType() + ", timestampType=" + timestampType() + ", crc=" + checksum() + ")";
    }

    /**
     * 计算记录批次的总大小(字节)
     * 包括批次头部开销和所有记录的大小
     * 
     * 应用场景:
     * 1. 在创建新的记录批次前预分配缓冲区
     * 2. 在写入日志段前计算所需空间
     * 
     * 设计考虑:
     * 1. 记录使用增量编码(偏移量增量和时间戳增量)以节省空间
     * 2. 批次头部大小固定为RECORD_BATCH_OVERHEAD
     * 
     * @param baseOffset 基准偏移量,用于计算每条记录的偏移量增量
     * @param records 要写入的记录集合
     * @return 记录批次的总字节数,如果没有记录则返回0
     */
    public static int sizeInBytes(long baseOffset, Iterable<Record> records) {
        Iterator<Record> iterator = records.iterator();
        // 如果没有记录,返回0
        if (!iterator.hasNext())
            return 0;

        // 初始大小为批次头部开销
        int size = RECORD_BATCH_OVERHEAD;
        // 用于计算时间戳增量的基准时间戳
        Long baseTimestamp = null;
        
        // 遍历所有记录计算总大小
        while (iterator.hasNext()) {
            Record record = iterator.next();
            // 计算相对于基准偏移量的增量
            int offsetDelta = (int) (record.offset() - baseOffset);
            
            // 第一条记录的时间戳作为基准时间戳
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            // 计算相对于基准时间戳的增量    
            long timestampDelta = record.timestamp() - baseTimestamp;
            
            // 累加每条记录的大小(包括键、值和头部)
            size += DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    /**
     * 计算给定记录集合的总字节大小
     * 
     * 应用场景:
     * 1. 在创建新的记录批次时预分配缓冲区
     * 2. 在进行批次大小限制检查时使用
     * 
     * @param records 需要计算大小的记录集合
     * @return 记录批次的总字节大小
     */
    public static int sizeInBytes(Iterable<SimpleRecord> records) {
        Iterator<SimpleRecord> iterator = records.iterator();
        // 如果没有记录,返回0
        if (!iterator.hasNext())
            return 0;

        // 初始大小为批次头部开销
        int size = RECORD_BATCH_OVERHEAD;
        // 记录的偏移量增量,从0开始递增
        int offsetDelta = 0;
        // 基准时间戳,用第一条记录的时间戳
        Long baseTimestamp = null;
        
        // 遍历所有记录计算总大小
        while (iterator.hasNext()) {
            SimpleRecord record = iterator.next();
            // 设置基准时间戳(第一条记录的时间戳)
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            // 计算时间戳相对于基准时间戳的增量    
            long timestampDelta = record.timestamp() - baseTimestamp;
            // 累加每条记录的大小
            size += DefaultRecord.sizeInBytes(offsetDelta++, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    /**
     * 估算单条记录批次的最大字节大小上限
     * 
     * 应用场景:
     * 1. 在创建新的记录批次时预分配缓冲区
     * 2. 在进行批次大小限制检查时使用
     * 
     * 注意:这只是一个估计值,因为没有考虑压缩算法带来的额外开销
     * 
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部数组
     * @return 批次的最大字节大小上限
     */
    static int estimateBatchSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 批次头部开销 + 记录的最大大小上限
        return RECORD_BATCH_OVERHEAD + DefaultRecord.recordSizeUpperBound(key, value, headers);
    }

    /**
     * 增加序列号
     * 
     * 应用场景:
     * 1. 生产者发送消息时递增序列号
     * 2. 计算批次中最后一条消息的序列号
     * 
     * 实现细节:
     * 处理序列号溢出的情况,当序列号超过Integer.MAX_VALUE时进行回环
     * 
     * @param sequence 当前序列号
     * @param increment 增量值
     * @return 增加后的序列号
     */
    public static int incrementSequence(int sequence, int increment) {
        // 如果增加increment会导致溢出
        if (sequence > Integer.MAX_VALUE - increment)
            // 回环到序列号的起始位置继续计数
            return increment - (Integer.MAX_VALUE - sequence) - 1;
        return sequence + increment;
    }

    /**
     * 减少序列号
     * 
     * 应用场景:
     * 1. 处理消息重试时回退序列号
     * 2. 进行序列号验证时的计算
     * 
     * 实现细节:
     * 处理序列号下溢的情况,当序列号小于要减少的值时进行回环
     * 
     * @param sequence 当前序列号
     * @param decrement 减少值
     * @return 减少后的序列号
     */
    public static int decrementSequence(int sequence, int decrement) {
        // 如果sequence小于要减少的值
        if (sequence < decrement)
            // 回环到序列号的最大值继续计数
            return Integer.MAX_VALUE - (decrement - sequence) + 1;
        return sequence - decrement;
    }

    /**
     * 记录迭代器的抽象基类
     * 提供遍历记录批次中所有记录的功能
     * 
     * 应用场景:
     * 1. 顺序读取记录批次中的消息
     * 2. 支持压缩和非压缩记录的统一访问
     */
    abstract class RecordIterator implements CloseableIterator<Record> {
        // 如果是LOG_APPEND_TIME,则使用批次的最大时间戳
        private final Long logAppendTime;
        // 批次的基准偏移量
        private final long baseOffset;
        // 批次的基准时间戳
        private final long baseTimestamp;
        // 批次的基准序列号
        private final int baseSequence;
        // 批次中的记录总数
        private final int numRecords;
        // 已读取的记录数
        private int readRecords = 0;

        /**
         * 构造函数,初始化迭代器的基本信息
         */
        RecordIterator() {
            // 如果是LOG_APPEND_TIME类型,使用批次的最大时间戳
            this.logAppendTime = timestampType() == TimestampType.LOG_APPEND_TIME ? maxTimestamp() : null;
            this.baseOffset = baseOffset();
            this.baseTimestamp = baseTimestamp();
            this.baseSequence = baseSequence();
            int numRecords = count();
            // 验证记录数量的有效性
            if (numRecords < 0)
                throw new InvalidRecordException("Found invalid record count " + numRecords + " in magic v" +
                        magic() + " batch");
            this.numRecords = numRecords;
        }

        /**
         * 检查是否还有下一条记录
         * 
         * @return 如果还有未读取的记录返回true
         */
        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        /**
         * 获取下一条记录
         * 
         * @return 下一条记录
         * @throws NoSuchElementException 如果没有更多记录
         * @throws InvalidRecordException 如果批次大小声明不正确
         */
        @Override
        public Record next() {
            // 检查是否还有未读记录
            if (readRecords >= numRecords)
                throw new NoSuchElementException();

            // 读取记录并递增计数器
            readRecords++;
            Record rec = readNext(baseOffset, baseTimestamp, baseSequence, logAppendTime);
            
            // 如果是最后一条记录,验证是否还有剩余数据
            if (readRecords == numRecords) {
                // 验证实际批次大小是否等于声明的大小
                // 通过检查读取声明的记录数后是否还有剩余项
                // (溢出情况,即读取超过缓冲区末尾的情况在其他地方检查)
                if (!ensureNoneRemaining())
                    throw new InvalidRecordException("Incorrect declared batch size, records still remaining in file");
            }
            return rec;
        }

        /**
         * 读取下一条记录的抽象方法
         * 由具体的迭代器实现类提供实现
         */
        protected abstract Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime);

        /**
         * 确保没有剩余数据的抽象方法
         * 由具体的迭代器实现类提供实现
         */
        protected abstract boolean ensureNoneRemaining();

        /**
         * 不支持删除操作
         * 
         * @throws UnsupportedOperationException 总是抛出此异常
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * 流式记录迭代器
     * 用于处理压缩记录,通过InputStream读取解压缩后的记录数据
     * 
     * 应用场景:
     * 1. 读取压缩的记录批次
     * 2. 支持大批次数据的高效处理
     */
    abstract class StreamRecordIterator extends RecordIterator {
        // 用于读取记录数据的输入流
        private final InputStream inputStream;

        /**
         * 构造函数
         * 
         * @param inputStream 用于读取记录数据的输入流
         */
        StreamRecordIterator(InputStream inputStream) {
            super();
            this.inputStream = inputStream;
        }

        /**
         * 从输入流读取记录的抽象方法
         * 由具体的实现类提供实现
         */
        abstract Record doReadRecord(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) throws IOException;

        /**
         * 实现父类的readNext方法
         * 处理读取记录时可能发生的异常
         */
        @Override
        protected Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) {
            try {
                // 尝试读取记录
                return doReadRecord(baseOffset, baseTimestamp, baseSequence, logAppendTime);
            } catch (IllegalArgumentException e) {
                // 如果遇到意外的EOF,说明批次大小声明不正确
                throw new InvalidRecordException("Incorrect declared batch size, premature EOF reached", e);
            } catch (IOException e) {
                // 解压缩失败
                throw new KafkaException("Failed to decompress record stream", e);
            }
        }

        /**
         * 检查是否还有剩余数据
         * 
         * @return 如果没有剩余数据返回true
         */
        @Override
        protected boolean ensureNoneRemaining() {
            try {
                // 尝试读取下一个字节,如果返回-1表示已到达流末尾
                return inputStream.read() == -1;
            } catch (IOException e) {
                throw new KafkaException("Error checking for remaining bytes after reading batch", e);
            }
        }

        /**
         * 关闭输入流
         * 
         * @throws KafkaException 如果关闭流时发生IO异常
         */
        @Override
        public void close() {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new KafkaException("Failed to close record stream", e);
            }
        }
    }

    /**
     * DefaultFileChannelRecordBatch是基于文件通道的记录批次实现类
     * 继承自FileLogInputStream.FileChannelRecordBatch
     * 
     * 应用场景:
     * 1. 从磁盘文件中读取记录批次数据
     * 2. 支持延迟加载批次头部信息,减少不必要的I/O操作
     * 3. 提供对记录批次各个属性的访问方法
     * 
     * 设计考虑:
     * 1. 使用延迟加载机制,只有在需要访问批次头部信息时才从磁盘读取
     * 2. 通过loadBatchHeader()方法缓存批次头部,避免重复读取
     * 3. 继承FileChannelRecordBatch以复用文件通道操作的基础功能
     */
    static class DefaultFileChannelRecordBatch extends FileLogInputStream.FileChannelRecordBatch {

        /**
         * 构造函数,创建一个新的DefaultFileChannelRecordBatch实例
         * 
         * @param offset 记录批次的基准偏移量
         * @param magic 消息格式版本号
         * @param fileRecords 文件记录对象,用于访问底层文件
         * @param position 批次在文件中的起始位置
         * @param batchSize 批次的大小(字节)
         */
        DefaultFileChannelRecordBatch(long offset,
                                      byte magic,
                                      FileRecords fileRecords,
                                      int position,
                                      int batchSize) {
            // 调用父类构造函数初始化基本属性
            super(offset, magic, fileRecords, position, batchSize);
        }

        /**
         * 将文件中的记录批次转换为内存中的记录批次
         * 
         * @param buffer 包含记录批次数据的字节缓冲区
         * @return 内存中的DefaultRecordBatch实例
         */
        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            // 创建新的DefaultRecordBatch实例
            return new DefaultRecordBatch(buffer);
        }

        /**
         * 获取批次的基准偏移量
         * 直接返回构造时传入的offset值
         * 
         * @return 批次中第一条消息的偏移量
         */
        @Override
        public long baseOffset() {
            return offset; // 返回构造函数中设置的偏移量
        }

        /**
         * 获取批次中最后一条消息的偏移量
         * 通过加载批次头部信息获取
         * 
         * @return 最后一条消息的偏移量
         */
        @Override
        public long lastOffset() {
            // 加载批次头部并获取最后偏移量
            return loadBatchHeader().lastOffset();
        }

        /**
         * 获取生产者ID
         * 用于幂等性和事务特性
         * 
         * @return 生产者ID
         */
        @Override
        public long producerId() {
            // 加载批次头部并获取生产者ID
            return loadBatchHeader().producerId();
        }

        /**
         * 获取生产者epoch值
         * 用于处理生产者故障恢复
         * 
         * @return 生产者epoch值
         */
        @Override
        public short producerEpoch() {
            // 加载批次头部并获取生产者epoch值
            return loadBatchHeader().producerEpoch();
        }

        /**
         * 获取基准序列号
         * 用于消息排序和重复检测
         * 
         * @return 批次的基准序列号
         */
        @Override
        public int baseSequence() {
            // 加载批次头部并获取基准序列号
            return loadBatchHeader().baseSequence();
        }

        /**
         * 获取批次中最后一条消息的序列号
         * 
         * @return 最后一条消息的序列号
         */
        @Override
        public int lastSequence() {
            // 加载批次头部并获取最后序列号
            return loadBatchHeader().lastSequence();
        }

        /**
         * 获取批次中的记录数量
         * 
         * @return 记录数量,如果批次为空则返回null
         */
        @Override
        public Integer countOrNull() {
            // 加载批次头部并获取记录数量
            return loadBatchHeader().countOrNull();
        }

        /**
         * 检查记录批次是否是事务消息
         * 
         * @return 如果是事务消息则返回true
         */
        @Override
        public boolean isTransactional() {
            // 加载批次头部并检查事务标志
            return loadBatchHeader().isTransactional();
        }

        /**
         * 获取删除时间戳
         * 用于日志压缩时确定记录是否可以被删除
         * 
         * @return 包含删除时间的OptionalLong,如果未设置则返回空
         */
        @Override
        public OptionalLong deleteHorizonMs() {
            // 加载批次头部并获取删除时间戳
            return loadBatchHeader().deleteHorizonMs();
        }

        /**
         * 检查记录批次是否是控制消息
         * 
         * @return 如果是控制消息则返回true
         */
        @Override
        public boolean isControlBatch() {
            // 加载批次头部并检查控制消息标志
            return loadBatchHeader().isControlBatch();
        }

        /**
         * 获取分区leader的epoch值
         * 用于确保消息的一致性和处理leader切换
         * 
         * @return 分区leader的epoch值
         */
        @Override
        public int partitionLeaderEpoch() {
            // 加载批次头部并获取分区leader epoch值
            return loadBatchHeader().partitionLeaderEpoch();
        }

        /**
         * 获取记录批次头部的大小
         * 
         * @return 头部大小(字节)
         */
        @Override
        protected int headerSize() {
            // 返回固定的记录批次头部大小
            return RECORD_BATCH_OVERHEAD;
        }
    }

}
