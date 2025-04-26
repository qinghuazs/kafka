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
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

/**
 * 这个类实现了旧版本(magic versions 0和1)的{@link RecordBatch}。除了实现{@link RecordBatch}接口外,
 * 它还实现了{@link Record}接口,这体现了旧消息格式在处理压缩消息时的二元性。在这个接口中,
 * 外层记录被视为记录批次,而内部记录被视为日志记录(尽管它们共享相同的模式)。
 * 
 * 通常不应直接使用这个类。{@link Records}的实例通过{@link RecordBatch}接口间接提供对这个类的访问。
 * 
 * 应用场景:
 * 1. 处理Kafka旧版本(0和1)的消息格式
 * 2. 支持压缩消息的读写,其中外层记录包含压缩的内部记录
 * 3. 提供向后兼容性,使新版本客户端可以读取旧版本的消息
 */
public abstract class AbstractLegacyRecordBatch extends AbstractRecordBatch implements Record {

    /**
     * 获取外层记录对象
     * 外层记录在处理压缩消息时作为包装器使用
     */
    public abstract LegacyRecord outerRecord();

    /**
     * 获取批次中最后一条记录的偏移量
     * 在旧版本格式中,这与当前记录的偏移量相同
     */
    @Override
    public long lastOffset() {
        return offset(); // 返回当前记录的偏移量,因为在旧格式中每条记录都是独立的批次
    }

    /**
     * 检查记录是否有效
     * 通过校验外层记录的有效性来判断
     */
    @Override
    public boolean isValid() {
        return outerRecord().isValid(); // 委托给外层记录的有效性检查
    }

    /**
     * 确保记录有效,如果无效则抛出异常
     */
    @Override
    public void ensureValid() {
        outerRecord().ensureValid(); // 验证外层记录的有效性
    }

    /**
     * 获取记录键的大小(字节)
     */
    @Override
    public int keySize() {
        return outerRecord().keySize(); // 返回外层记录键的大小
    }

    /**
     * 检查记录是否包含键
     */
    @Override
    public boolean hasKey() {
        return outerRecord().hasKey(); // 检查外层记录是否有键
    }

    /**
     * 获取记录的键
     */
    @Override
    public ByteBuffer key() {
        return outerRecord().key(); // 返回外层记录的键
    }

    /**
     * 获取记录值的大小(字节)
     */
    @Override
    public int valueSize() {
        return outerRecord().valueSize(); // 返回外层记录值的大小
    }

    /**
     * 检查记录是否包含值
     * 通过检查外层记录的值是否为null来判断
     */
    @Override
    public boolean hasValue() {
        return !outerRecord().hasNullValue(); // 如果外层记录的值不为null,则返回true
    }

    /**
     * 获取记录的值
     */
    @Override
    public ByteBuffer value() {
        return outerRecord().value(); // 返回外层记录的值
    }

    /**
     * 获取记录的头部数组
     * 旧版本格式不支持头部,因此返回空数组
     */
    @Override
    public Header[] headers() {
        return Record.EMPTY_HEADERS; // 旧版本不支持消息头
    }

    /**
     * 检查记录的magic值是否匹配
     * magic值用于标识消息格式版本
     */
    @Override
    public boolean hasMagic(byte magic) {
        return magic == outerRecord().magic(); // 检查magic值是否匹配
    }

    /**
     * 检查记录的时间戳类型是否匹配
     */
    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return outerRecord().timestampType() == timestampType; // 检查时间戳类型是否匹配
    }

    /**
     * 获取记录的校验和
     */
    @Override
    public long checksum() {
        return outerRecord().checksum(); // 返回外层记录的校验和
    }

    /**
     * 获取批次中最大的时间戳
     * 在旧版本中,这与当前记录的时间戳相同
     */
    @Override
    public long maxTimestamp() {
        return timestamp(); // 返回当前记录的时间戳
    }

    /**
     * 获取记录的时间戳
     */
    @Override
    public long timestamp() {
        return outerRecord().timestamp(); // 返回外层记录的时间戳
    }

    /**
     * 获取时间戳类型
     */
    @Override
    public TimestampType timestampType() {
        return outerRecord().timestampType(); // 返回外层记录的时间戳类型
    }

    /**
     * 获取批次的基准偏移量
     * 通过获取第一条记录的偏移量实现
     */
    @Override
    public long baseOffset() {
        return iterator().next().offset(); // 返回第一条记录的偏移量作为基准偏移量
    }

    /**
     * 获取消息格式版本号(magic value)
     */
    @Override
    public byte magic() {
        return outerRecord().magic(); // 返回外层记录的magic值
    }

    /**
     * 获取压缩类型
     */
    @Override
    public CompressionType compressionType() {
        return outerRecord().compressionType(); // 返回外层记录的压缩类型
    }

    /**
     * 获取记录批次的总大小(字节)
     * 包括记录数据和日志开销
     */
    @Override
    public int sizeInBytes() {
        return outerRecord().sizeInBytes() + LOG_OVERHEAD; // 记录大小加上日志开销
    }

    /**
     * 获取批次中的记录数量
     * 旧版本格式不支持此功能,返回null
     */
    @Override
    public Integer countOrNull() {
        return null; // 旧版本不支持记录数量统计
    }

    @Override
    public String toString() {
        return "LegacyRecordBatch(offset=" + offset() + ", " + outerRecord() + ")";
    }

    /**
     * 将记录批次写入ByteBuffer
     * 包括写入头部信息和记录数据
     */
    @Override
    public void writeTo(ByteBuffer buffer) {
        writeHeader(buffer, offset(), outerRecord().sizeInBytes()); // 写入头部信息
        buffer.put(outerRecord().buffer().duplicate()); // 写入记录数据
    }

    /**
     * 获取生产者ID
     * 旧版本格式不支持生产者ID
     */
    @Override
    public long producerId() {
        return RecordBatch.NO_PRODUCER_ID; // 旧版本不支持生产者ID
    }

    /**
     * 获取生产者epoch
     * 旧版本格式不支持生产者epoch
     */
    @Override
    public short producerEpoch() {
        return RecordBatch.NO_PRODUCER_EPOCH; // 旧版本不支持生产者epoch
    }

    /**
     * 检查是否包含生产者ID
     * 旧版本格式不支持生产者ID
     */
    @Override
    public boolean hasProducerId() {
        return false; // 旧版本不支持生产者ID
    }

    /**
     * 获取序列号
     * 旧版本格式不支持序列号
     */
    @Override
    public int sequence() {
        return RecordBatch.NO_SEQUENCE; // 旧版本不支持序列号
    }

    /**
     * 获取基准序列号
     * 旧版本格式不支持序列号
     */
    @Override
    public int baseSequence() {
        return RecordBatch.NO_SEQUENCE; // 旧版本不支持序列号
    }

    /**
     * 获取最后的序列号
     * 旧版本格式不支持序列号
     */
    @Override
    public int lastSequence() {
        return RecordBatch.NO_SEQUENCE; // 旧版本不支持序列号
    }

    /**
     * 检查是否为事务性批次
     * 旧版本格式不支持事务
     */
    @Override
    public boolean isTransactional() {
        return false; // 旧版本不支持事务
    }

    /**
     * 获取分区leader的epoch
     * 旧版本格式不支持分区leader epoch
     */
    @Override
    public int partitionLeaderEpoch() {
        return RecordBatch.NO_PARTITION_LEADER_EPOCH; // 旧版本不支持分区leader epoch
    }

    /**
     * 检查是否为控制批次
     * 旧版本格式不支持控制批次
     */
    @Override
    public boolean isControlBatch() {
        return false; // 旧版本不支持控制批次
    }

    /**
     * 获取删除时间界限
     * 旧版本格式不支持删除时间界限
     */
    @Override
    public OptionalLong deleteHorizonMs() {
        return OptionalLong.empty(); // 旧版本不支持删除时间界限
    }

    /**
     * 获取批次中包含的记录的迭代器
     * 如果批次未压缩,则返回仅包含当前记录的迭代器
     * 如果批次已压缩,则返回解压缩后内部记录的迭代器
     * 
     * @return 批次中记录的迭代器
     */
    @Override
    public Iterator<Record> iterator() {
        return iterator(BufferSupplier.NO_CACHING); // 使用无缓存的缓冲区供应器
    }

    /**
     * 获取带有缓冲区供应器的记录迭代器
     * 用于处理压缩和非压缩的记录批次
     * 
     * @param bufferSupplier 缓冲区供应器,用于解压缩时的内存管理
     * @return 可关闭的记录迭代器
     */
    CloseableIterator<Record> iterator(BufferSupplier bufferSupplier) {
        if (isCompressed()) {
            // 如果是压缩批次,创建深度迭代器来处理内部记录
            return new DeepRecordsIterator(this, false, Integer.MAX_VALUE, bufferSupplier);
        }

        // 如果是非压缩批次,返回只包含当前记录的简单迭代器
        return new CloseableIterator<>() {
            private boolean hasNext = true; // 标记是否还有下一个记录

            @Override
            public void close() {} // 空实现,因为没有资源需要释放

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Record next() {
                if (!hasNext)
                    throw new NoSuchElementException();
                hasNext = false; // 因为只有一条记录,所以获取后标记为false
                return AbstractLegacyRecordBatch.this; // 返回当前记录批次
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException(); // 不支持删除操作
            }
        };
    }

    /**
     * 获取流式迭代器
     * 旧版本格式不支持流式处理,返回普通迭代器
     */
    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        // 旧消息格式版本不支持流式处理,返回普通迭代器
        return iterator(bufferSupplier);
    }

    /**
     * 写入记录头部信息到ByteBuffer
     * 包括偏移量和大小信息
     * 
     * @param buffer 目标缓冲区
     * @param offset 记录偏移量
     * @param size 记录大小
     */
    static void writeHeader(ByteBuffer buffer, long offset, int size) {
        buffer.putLong(offset); // 写入8字节的偏移量
        buffer.putInt(size);    // 写入4字节的大小
    }

    /**
     * 写入记录头部信息到DataOutputStream
     * 包括偏移量和大小信息
     * 
     * @param out 输出流
     * @param offset 记录偏移量
     * @param size 记录大小
     */
    static void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
        out.writeLong(offset); // 写入8字节的偏移量
        out.writeInt(size);    // 写入4字节的大小
    }

    /**
     * 用于从输入流中读取旧版本记录批次的实现类
     * 主要用于处理压缩消息时读取内部记录
     */
    private static final class DataLogInputStream implements LogInputStream<AbstractLegacyRecordBatch> {
        private final InputStream stream;        // 用于读取记录数据的输入流
        private final int maxMessageSize;       // 允许的最大消息大小
        private final ByteBuffer offsetAndSizeBuffer;  // 用于读取记录头部(偏移量和大小)的缓冲区

        /**
         * 创建DataLogInputStream实例
         * @param stream 输入流,用于读取记录数据
         * @param maxMessageSize 允许的最大消息大小,用于验证记录大小是否合法
         */
        DataLogInputStream(InputStream stream, int maxMessageSize) {
            this.stream = stream;
            this.maxMessageSize = maxMessageSize;
            // 分配一个固定大小的缓冲区用于读取记录头部信息(偏移量和大小)
            this.offsetAndSizeBuffer = ByteBuffer.allocate(Records.LOG_OVERHEAD);
        }

        /**
         * 从输入流中读取下一个记录批次
         * 
         * @return 如果成功读取返回记录批次,如果到达流末尾返回null
         * @throws IOException 如果读取过程中发生I/O错误
         * @throws CorruptRecordException 如果记录数据损坏或大小非法
         */
        public AbstractLegacyRecordBatch nextBatch() throws IOException {
            // 清空并准备复用头部缓冲区
            offsetAndSizeBuffer.clear();
            // 读取记录头部信息(偏移量和大小)
            Utils.readFully(stream, offsetAndSizeBuffer);
            // 如果没有读满,说明到达流末尾
            if (offsetAndSizeBuffer.hasRemaining())
                return null;

            // 从头部缓冲区解析偏移量和大小信息
            long offset = offsetAndSizeBuffer.getLong(Records.OFFSET_OFFSET);
            int size = offsetAndSizeBuffer.getInt(Records.SIZE_OFFSET);
            
            // 验证记录大小是否合法
            if (size < LegacyRecord.RECORD_OVERHEAD_V0)
                throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", LegacyRecord.RECORD_OVERHEAD_V0));
            if (size > maxMessageSize)
                throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

            // 分配缓冲区并读取完整的记录数据
            ByteBuffer batchBuffer = ByteBuffer.allocate(size);
            Utils.readFully(stream, batchBuffer);
            // 如果没有读满,说明到达流末尾
            if (batchBuffer.hasRemaining())
                return null;
            // 准备缓冲区供读取使用
            batchBuffer.flip();

            // 创建并返回记录批次实例
            return new BasicLegacyRecordBatch(offset, new LegacyRecord(batchBuffer));
        }
    }

    /**
     * 用于遍历压缩消息中内部记录的迭代器实现
     * 负责解压缩消息并提供对内部记录的访问
     * 
     * 主要功能:
     * 1. 解压缩消息内容
     * 2. 处理内部记录的偏移量计算
     * 3. 确保内部记录的格式版本一致性
     */
    private static class DeepRecordsIterator extends AbstractIterator<Record> implements CloseableIterator<Record> {
        private final ArrayDeque<AbstractLegacyRecordBatch> innerEntries;  // 存储解压后的内部记录队列
        private final long absoluteBaseOffset;  // 用于计算内部记录的绝对偏移量
        private final byte wrapperMagic;       // 外层记录的magic值(格式版本)

        /**
         * 创建深度记录迭代器
         * 
         * @param wrapperEntry 外层记录批次,包含压缩的内部记录
         * @param ensureMatchingMagic 是否确保内部记录的magic值与外层一致
         * @param maxMessageSize 允许的最大消息大小
         * @param bufferSupplier 用于解压缩的缓冲区供应器
         */
        private DeepRecordsIterator(AbstractLegacyRecordBatch wrapperEntry,
                                    boolean ensureMatchingMagic,
                                    int maxMessageSize,
                                    BufferSupplier bufferSupplier) {
            // 获取外层记录并进行基本验证
            LegacyRecord wrapperRecord = wrapperEntry.outerRecord();
            this.wrapperMagic = wrapperRecord.magic();
            // 验证magic值是否为支持的版本(V0或V1)
            if (wrapperMagic != RecordBatch.MAGIC_VALUE_V0 && wrapperMagic != RecordBatch.MAGIC_VALUE_V1)
                throw new InvalidRecordException("Invalid wrapper magic found in legacy deep record iterator " + wrapperMagic);

            // 获取并验证压缩类型
            CompressionType compressionType = wrapperRecord.compressionType();
            // 旧版本格式不支持ZSTD压缩
            if (compressionType == CompressionType.ZSTD)
                throw new InvalidRecordException("Invalid wrapper compressionType found in legacy deep record iterator " + wrapperMagic);
            
            // 获取并验证压缩的消息内容
            ByteBuffer wrapperValue = wrapperRecord.value();
            if (wrapperValue == null)
                throw new InvalidRecordException("Found invalid compressed record set with null value (magic = " +
                        wrapperMagic + ")");

            // 创建解压缩流
            InputStream stream = Compression.of(compressionType).build().wrapForInput(wrapperValue, wrapperRecord.magic(), bufferSupplier);
            // 创建记录读取流
            LogInputStream<AbstractLegacyRecordBatch> logStream = new DataLogInputStream(stream, maxMessageSize);

            // 保存外层记录的偏移量和时间戳,用于处理内部记录
            long lastOffsetFromWrapper = wrapperEntry.lastOffset();
            long timestampFromWrapper = wrapperRecord.timestamp();
            // 初始化内部记录队列
            this.innerEntries = new ArrayDeque<>();

            // 如果使用相对偏移量,需要先解压整个消息来计算绝对偏移量
            // 为了简单起见,对于消息格式版本0也采用相同处理方式
            try {
                // 循环读取所有内部记录
                while (true) {
                    // 读取下一个内部记录批次
                    AbstractLegacyRecordBatch innerEntry = logStream.nextBatch();
                    if (innerEntry == null)  // 如果没有更多记录,退出循环
                        break;

                    // 获取内部记录并验证其格式版本
                    LegacyRecord record = innerEntry.outerRecord();
                    byte magic = record.magic();

                    // 如果要求magic值匹配,则验证内部记录的magic值是否与外层记录一致
                    if (ensureMatchingMagic && magic != wrapperMagic)
                        throw new InvalidRecordException("Compressed message magic " + magic +
                                " does not match wrapper magic " + wrapperMagic);

                    // 对于V1格式的记录,使用外层记录的时间戳信息创建新的记录
                    if (magic == RecordBatch.MAGIC_VALUE_V1) {
                        LegacyRecord recordWithTimestamp = new LegacyRecord(
                                record.buffer(),
                                timestampFromWrapper,  // 使用外层记录的时间戳
                                wrapperRecord.timestampType());  // 使用外层记录的时间戳类型
                        innerEntry = new BasicLegacyRecordBatch(innerEntry.lastOffset(), recordWithTimestamp);
                    }

                    // 将处理后的内部记录添加到队列
                    innerEntries.addLast(innerEntry);
                }

                // 验证是否存在内部记录
                if (innerEntries.isEmpty())
                    throw new InvalidRecordException("Found invalid compressed record set with no inner records");

                // 计算绝对基准偏移量(仅针对V1格式)
                if (wrapperMagic == RecordBatch.MAGIC_VALUE_V1) {
                    if (lastOffsetFromWrapper == 0) {
                        // 外层偏移量为0的特殊情况(某些版本的librdkafka可能产生这种情况)
                        this.absoluteBaseOffset = 0;
                    } else {
                        // 获取最后一条内部记录的偏移量
                        long lastInnerOffset = innerEntries.getLast().offset();
                        // 验证外层偏移量是否大于等于最后一条内部记录的偏移量
                        if (lastOffsetFromWrapper < lastInnerOffset)
                            throw new InvalidRecordException("Found invalid wrapper offset in compressed v1 message set, " +
                                    "wrapper offset '" + lastOffsetFromWrapper + "' is less than the last inner message " +
                                    "offset '" + lastInnerOffset + "' and it is not zero.");
                        // 计算绝对基准偏移量 = 外层偏移量 - 最后一条内部记录的偏移量
                        this.absoluteBaseOffset = lastOffsetFromWrapper - lastInnerOffset;
                    }
                } else {
                    // V0格式不需要计算绝对偏移量
                    this.absoluteBaseOffset = -1;
                }
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                // 确保解压缩流被正确关闭
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        /**
         * 获取下一条记录
         * 处理内部记录的偏移量转换,并进行必要的验证
         * 
         * @return 下一条记录,如果没有更多记录则返回null
         * @throws InvalidRecordException 如果发现压缩的内部记录
         */
        @Override
        protected Record makeNext() {
            // 如果没有更多内部记录,返回结束标记
            if (innerEntries.isEmpty())
                return allDone();

            // 从队列中取出下一条内部记录
            AbstractLegacyRecordBatch entry = innerEntries.remove();

            // 对于V1格式,需要将相对偏移量转换为绝对偏移量
            if (wrapperMagic == RecordBatch.MAGIC_VALUE_V1) {
                long absoluteOffset = absoluteBaseOffset + entry.offset();
                entry = new BasicLegacyRecordBatch(absoluteOffset, entry.outerRecord());
            }

            // 验证内部记录不能是压缩的
            if (entry.isCompressed())
                throw new InvalidRecordException("Inner messages must not be compressed");

            return entry;
        }

        @Override
        public void close() {}
    }

    /**
     * 旧版本记录批次的基本实现类
     * 用于表示单个非压缩记录或解压缩后的内部记录
     * 
     * 主要功能:
     * 1. 存储记录的偏移量和实际记录内容
     * 2. 提供对记录内容的访问
     * 3. 作为旧版本记录格式的简单封装
     */
    private static class BasicLegacyRecordBatch extends AbstractLegacyRecordBatch {
        private final LegacyRecord record;  // 实际的记录内容
        private final long offset;          // 记录的偏移量

        /**
         * 创建基本记录批次实例
         * 
         * @param offset 记录的偏移量
         * @param record 实际的记录内容
         */
        private BasicLegacyRecordBatch(long offset, LegacyRecord record) {
            this.offset = offset;
            this.record = record;
        }

        /**
         * 获取记录的偏移量
         * @return 记录的偏移量
         */
        @Override
        public long offset() {
            return offset;
        }

        /**
         * 获取外层记录对象
         * 在这个简单实现中,直接返回存储的记录对象
         * 
         * @return 记录对象
         */
        @Override
        public LegacyRecord outerRecord() {
            return record;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BasicLegacyRecordBatch that = (BasicLegacyRecordBatch) o;

            return offset == that.offset &&
                Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            int result = record != null ? record.hashCode() : 0;
            result = 31 * result + Long.hashCode(offset);
            return result;
        }
    }

    /**
     * 基于ByteBuffer实现的旧版本记录批次
     * 主要用于在内存中处理旧版本(magic 0和1)的消息格式
     * 
     * 应用场景:
     * 1. 读写内存中的旧版本消息
     * 2. 支持消息的压缩和解压缩
     * 3. 维护消息的时间戳和校验和
     */
    static class ByteBufferLegacyRecordBatch extends AbstractLegacyRecordBatch implements MutableRecordBatch {
        private final ByteBuffer buffer;  // 存储记录数据的缓冲区
        private final LegacyRecord record;  // 外层记录对象,用于访问记录的具体内容

        /**
         * 创建ByteBufferLegacyRecordBatch实例
         * @param buffer 包含记录数据的ByteBuffer
         */
        ByteBufferLegacyRecordBatch(ByteBuffer buffer) {
            this.buffer = buffer;  // 保存缓冲区引用
            buffer.position(LOG_OVERHEAD);  // 将位置设置到日志开销之后
            this.record = new LegacyRecord(buffer.slice());  // 创建记录对象
            buffer.position(OFFSET_OFFSET);  // 将位置重置到偏移量字段
        }

        /**
         * 获取记录的偏移量
         * @return 记录在日志中的偏移量
         */
        @Override
        public long offset() {
            return buffer.getLong(OFFSET_OFFSET);  // 从缓冲区读取8字节的偏移量
        }

        /**
         * 获取外层记录对象
         * @return 外层LegacyRecord对象
         */
        @Override
        public LegacyRecord outerRecord() {
            return record;  // 返回记录对象
        }

        /**
         * 设置记录的最后偏移量
         * @param offset 要设置的偏移量值
         */
        @Override
        public void setLastOffset(long offset) {
            buffer.putLong(OFFSET_OFFSET, offset);  // 将新的偏移量写入缓冲区
        }

        /**
         * 设置记录的最大时间戳
         * 仅支持magic值为1的记录,因为版本0不支持时间戳
         * 
         * @param timestampType 时间戳类型(CREATE_TIME或LOG_APPEND_TIME)
         * @param timestamp 要设置的时间戳值
         * @throws UnsupportedOperationException 如果记录的magic值为0
         */
        @Override
        public void setMaxTimestamp(TimestampType timestampType, long timestamp) {
            if (record.magic() == RecordBatch.MAGIC_VALUE_V0)
                throw new UnsupportedOperationException("Cannot set timestamp for a record with magic = 0");

            long currentTimestamp = record.timestamp();
            // 如果时间戳类型和值都没有变化,则不需要重新计算校验和
            if (record.timestampType() == timestampType && currentTimestamp == timestamp)
                return;

            setTimestampAndUpdateCrc(timestampType, timestamp);  // 更新时间戳并重新计算校验和
        }

        /**
         * 设置分区leader的epoch
         * 旧版本格式不支持此功能
         * 
         * @param epoch 要设置的epoch值
         * @throws UnsupportedOperationException 总是抛出此异常,因为旧版本不支持
         */
        @Override
        public void setPartitionLeaderEpoch(int epoch) {
            throw new UnsupportedOperationException("Magic versions prior to 2 do not support partition leader epoch");
        }

        /**
         * 更新时间戳并重新计算记录的校验和
         * 
         * @param timestampType 时间戳类型
         * @param timestamp 新的时间戳值
         */
        private void setTimestampAndUpdateCrc(TimestampType timestampType, long timestamp) {
            // 计算新的属性值(包含压缩类型和时间戳类型)
            byte attributes = LegacyRecord.computeAttributes(magic(), compressionType(), timestampType);
            // 更新属性字段
            buffer.put(LOG_OVERHEAD + LegacyRecord.ATTRIBUTES_OFFSET, attributes);
            // 更新时间戳字段
            buffer.putLong(LOG_OVERHEAD + LegacyRecord.TIMESTAMP_OFFSET, timestamp);
            // 重新计算并更新校验和
            long crc = record.computeChecksum();
            ByteUtils.writeUnsignedInt(buffer, LOG_OVERHEAD + LegacyRecord.CRC_OFFSET, crc);
        }

        /**
         * 获取跳过键值的记录迭代器
         * 旧版本格式不支持此功能,返回普通迭代器
         * 
         * @param bufferSupplier 缓冲区供应器
         * @return 记录迭代器
         */
        @Override
        public CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier) {
            return CloseableIterator.wrap(iterator(bufferSupplier));
        }

        /**
         * 将记录批次写入输出流
         * 
         * @param outputStream 目标输出流
         */
        @Override
        public void writeTo(ByteBufferOutputStream outputStream) {
            outputStream.write(buffer.duplicate());  // 写入缓冲区的副本
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ByteBufferLegacyRecordBatch that = (ByteBufferLegacyRecordBatch) o;

            return Objects.equals(buffer, that.buffer);
        }

        @Override
        public int hashCode() {
            return buffer != null ? buffer.hashCode() : 0;
        }
    }

    /**
     * 基于文件通道的旧版本记录批次实现
     * 用于从磁盘文件中读取旧版本(magic 0和1)的消息格式
     * 
     * 应用场景:
     * 1. 从日志文件中读取旧版本消息
     * 2. 支持延迟加载,只有需要时才读取完整记录
     * 3. 提供与新版本格式兼容的接口实现
     */
    static class LegacyFileChannelRecordBatch extends FileLogInputStream.FileChannelRecordBatch {

        /**
         * 创建LegacyFileChannelRecordBatch实例
         * 
         * @param offset 记录的起始偏移量
         * @param magic 消息格式版本号
         * @param fileRecords 文件记录对象
         * @param position 在文件中的位置
         * @param batchSize 批次大小(字节)
         */
        LegacyFileChannelRecordBatch(long offset,
                                     byte magic,
                                     FileRecords fileRecords,
                                     int position,
                                     int batchSize) {
            super(offset, magic, fileRecords, position, batchSize);  // 调用父类构造器初始化
        }

        /**
         * 将文件中的记录转换为内存中的记录批次
         * 
         * @param buffer 包含记录数据的缓冲区
         * @return 内存中的记录批次对象
         */
        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            return new ByteBufferLegacyRecordBatch(buffer);  // 创建基于ByteBuffer的记录批次
        }

        /**
         * 获取批次的基准偏移量
         * 需要加载完整批次才能获取
         * 
         * @return 批次的基准偏移量
         */
        @Override
        public long baseOffset() {
            return loadFullBatch().baseOffset();  // 加载完整批次并获取基准偏移量
        }

        /**
         * 获取删除时间界限
         * 旧版本格式不支持此功能
         * 
         * @return 空的OptionalLong
         */
        @Override
        public OptionalLong deleteHorizonMs() {
            return OptionalLong.empty();  // 旧版本不支持删除时间界限
        }

        /**
         * 获取批次中最后一条记录的偏移量
         * 在旧版本中与起始偏移量相同
         * 
         * @return 最后一条记录的偏移量
         */
        @Override
        public long lastOffset() {
            return offset;  // 返回起始偏移量
        }

        /**
         * 获取生产者ID
         * 旧版本格式不支持生产者ID
         * 
         * @return NO_PRODUCER_ID常量
         */
        @Override
        public long producerId() {
            return RecordBatch.NO_PRODUCER_ID;  // 旧版本不支持生产者ID
        }

        /**
         * 获取生产者epoch
         * 旧版本格式不支持生产者epoch
         * 
         * @return NO_PRODUCER_EPOCH常量
         */
        @Override
        public short producerEpoch() {
            return RecordBatch.NO_PRODUCER_EPOCH;  // 旧版本不支持生产者epoch
        }

        /**
         * 获取基准序列号
         * 旧版本格式不支持序列号
         * 
         * @return NO_SEQUENCE常量
         */
        @Override
        public int baseSequence() {
            return RecordBatch.NO_SEQUENCE;  // 旧版本不支持序列号
        }

        /**
         * 获取最后的序列号
         * 旧版本格式不支持序列号
         * 
         * @return NO_SEQUENCE常量
         */
        @Override
        public int lastSequence() {
            return RecordBatch.NO_SEQUENCE;  // 旧版本不支持序列号
        }

        /**
         * 获取批次中的记录数量
         * 旧版本格式不支持记录计数
         * 
         * @return null表示不支持计数
         */
        @Override
        public Integer countOrNull() {
            return null;  // 旧版本不支持记录计数
        }

        /**
         * 检查是否为事务性批次
         * 旧版本格式不支持事务
         * 
         * @return false
         */
        @Override
        public boolean isTransactional() {
            return false;  // 旧版本不支持事务
        }

        /**
         * 检查是否为控制批次
         * 旧版本格式不支持控制批次
         * 
         * @return false
         */
        @Override
        public boolean isControlBatch() {
            return false;  // 旧版本不支持控制批次
        }

        /**
         * 获取分区leader的epoch
         * 旧版本格式不支持分区leader epoch
         * 
         * @return NO_PARTITION_LEADER_EPOCH常量
         */
        @Override
        public int partitionLeaderEpoch() {
            return RecordBatch.NO_PARTITION_LEADER_EPOCH;  // 旧版本不支持分区leader epoch
        }

        /**
         * 获取记录头部的大小
         * 包括日志开销和版本特定的头部大小
         * 
         * @return 头部总大小(字节)
         */
        @Override
        protected int headerSize() {
            return LOG_OVERHEAD + LegacyRecord.headerSize(magic);  // 计算总的头部大小
        }

    }

}
