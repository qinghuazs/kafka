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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;

/**
 * This class implements the inner record format for magic 2 and above. The schema is as follows:
 *
 *
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   KeyLength => Varint
 *   Key => Bytes
 *   ValueLength => Varint
 *   Value => Bytes
 *   HeadersCount => Varint
 *   Headers => [HeaderKey HeaderValue]
 *     HeaderKeyLength => Varint
 *     HeaderKey => String
 *     HeaderValueLength => Varint
 *     HeaderValue => Bytes
 *
 * Note that in this schema, the Bytes and String types use a variable length integer to represent
 * the length of the field. The array type used for the headers also uses a Varint for the number of
 * headers.
 *
 * The current record attributes are depicted below:
 *
 *  ----------------
 *  | Unused (0-7) |
 *  ----------------
 *
 * The offset and timestamp deltas compute the difference relative to the base offset and
 * base timestamp of the batch that this record is contained in.
 */
/**
 * DefaultRecord类实现了Kafka 2.0及以上版本的内部记录格式。
 * 记录格式的结构如下：
 * 
 * Record =>
 *   Length => Varint（记录体的长度）
 *   Attributes => Int8（记录的属性标志位）
 *   TimestampDelta => Varlong（相对于批次基准时间的时间戳增量）
 *   OffsetDelta => Varint（相对于批次基准偏移量的偏移量增量）
 *   KeyLength => Varint（键的长度）
 *   Key => Bytes（键的内容）
 *   ValueLength => Varint（值的长度）
 *   Value => Bytes（值的内容）
 *   HeadersCount => Varint（头部数量）
 *   Headers => [HeaderKey HeaderValue]（头部信息数组）
 *     HeaderKeyLength => Varint（头部键长度）
 *     HeaderKey => String（头部键内容）
 *     HeaderValueLength => Varint（头部值长度）
 *     HeaderValue => Bytes（头部值内容）
 */
public class DefaultRecord implements Record {

    // 记录开销的最大字节数（不包括键、值和头部）：
    // 5字节长度 + 10字节时间戳 + 5字节偏移量 + 1字节属性
    public static final int MAX_RECORD_OVERHEAD = 21;

    // 表示空值的变长整数大小（-1的变长整数编码大小）
    private static final int NULL_VARINT_SIZE_BYTES = ByteUtils.sizeOfVarint(-1);

    // 记录的总字节大小
    private final int sizeInBytes;
    // 记录的属性标志位（当前版本未使用）
    private final byte attributes;
    // 记录在分区中的绝对偏移量
    private final long offset;
    // 记录的时间戳（毫秒）
    private final long timestamp;
    // 记录的序列号（用于幂等性和事务）
    private final int sequence;
    // 记录的键（可选）
    private final ByteBuffer key;
    // 记录的值（实际消息内容）
    private final ByteBuffer value;
    // 记录的头部信息数组
    private final Header[] headers;

    /**
     * 创建一个DefaultRecord实例。
     * 
     * @param sizeInBytes 记录的总字节大小
     * @param attributes 记录的属性标志位
     * @param offset 记录在分区中的绝对偏移量
     * @param timestamp 记录的时间戳（毫秒）
     * @param sequence 记录的序列号
     * @param key 记录的键（可选）
     * @param value 记录的值（实际消息内容）
     * @param headers 记录的头部信息数组
     */
    DefaultRecord(int sizeInBytes,
                  byte attributes,
                  long offset,
                  long timestamp,
                  int sequence,
                  ByteBuffer key,
                  ByteBuffer value,
                  Header[] headers) {
        this.sizeInBytes = sizeInBytes;
        this.attributes = attributes;
        this.offset = offset;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    /**
     * 获取记录在分区中的绝对偏移量。
     * 这个偏移量是通过批次的基准偏移量加上记录的偏移量增量计算得到的。
     * @return 记录的绝对偏移量
     */
    @Override
    public long offset() {
        return offset;
    }

    /**
     * 获取记录的序列号。
     * 序列号用于实现生产者的幂等性和事务特性。
     * @return 记录的序列号
     */
    @Override
    public int sequence() {
        return sequence;
    }

    /**
     * 获取记录的总字节大小。
     * 包括记录的所有组成部分：长度、属性、时间戳、偏移量、键、值和头部。
     * @return 记录的总字节数
     */
    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    /**
     * 获取记录的时间戳。
     * 这个时间戳是通过批次的基准时间戳加上记录的时间戳增量计算得到的。
     * @return 记录的时间戳（毫秒）
     */
    @Override
    public long timestamp() {
        return timestamp;
    }

    /**
     * 获取记录的属性标志位。
     * 在当前版本中，这个字段未被使用。
     * @return 记录的属性字节
     */
    public byte attributes() {
        return attributes;
    }

    /**
     * 验证记录的有效性。
     * 在当前版本中，这个方法不执行任何验证。
     */
    @Override
    public void ensureValid() {}

    /**
     * 获取记录键的字节大小。
     * @return 键的字节数，如果没有键则返回-1
     */
    @Override
    public int keySize() {
        return key == null ? -1 : key.remaining();
    }

    /**
     * 获取记录值的字节大小。
     * @return 值的字节数，如果没有值则返回-1
     */
    @Override
    public int valueSize() {
        return value == null ? -1 : value.remaining();
    }

    /**
     * 检查记录是否包含键。
     * @return 如果记录有键则返回true，否则返回false
     */
    @Override
    public boolean hasKey() {
        return key != null;
    }

    /**
     * 获取记录的键。
     * 返回键的副本以防止外部修改。
     * @return 记录的键，如果没有则返回null
     */
    @Override
    public ByteBuffer key() {
        return key == null ? null : key.duplicate();
    }

    /**
     * 检查记录是否包含值。
     * @return 如果记录有值则返回true，否则返回false
     */
    @Override
    public boolean hasValue() {
        return value != null;
    }

    /**
     * 获取记录的值。
     * 返回值的副本以防止外部修改。
     * @return 记录的值，如果没有则返回null
     */
    @Override
    public ByteBuffer value() {
        return value == null ? null : value.duplicate();
    }

    /**
     * 获取记录的所有头部信息。
     * @return 头部信息数组
     */
    @Override
    public Header[] headers() {
        return headers;
    }

    /**
     * 将记录写入输出流，并返回写入的字节数。
     * 记录的序列化格式如下：
     * 1. 记录体长度（变长整数）
     * 2. 属性标志位（1字节）
     * 3. 时间戳增量（变长长整数）
     * 4. 偏移量增量（变长整数）
     * 5. 键长度和键内容
     * 6. 值长度和值内容
     * 7. 头部数量和头部内容
     *
     * @param out 目标输出流
     * @param offsetDelta 相对于批次基准偏移量的偏移量增量
     * @param timestampDelta 相对于批次基准时间戳的时间戳增量
     * @param key 记录的键（可选）
     * @param value 记录的值
     * @param headers 记录的头部信息数组
     * @return 写入的总字节数
     * @throws IOException 如果写入过程中发生I/O错误
     * @throws IllegalArgumentException 如果headers为null或者包含null的键
     */
    public static int writeTo(DataOutputStream out,
                              int offsetDelta,
                              long timestampDelta,
                              ByteBuffer key,
                              ByteBuffer value,
                              Header[] headers) throws IOException {
        // 计算记录体的大小
        int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        // 写入记录体长度
        ByteUtils.writeVarint(sizeInBytes, out);

        // 写入属性标志位（当前版本未使用）
        byte attributes = 0;
        out.write(attributes);

        // 写入时间戳增量和偏移量增量
        ByteUtils.writeVarlong(timestampDelta, out);
        ByteUtils.writeVarint(offsetDelta, out);

        // 写入键（如果存在）
        if (key == null) {
            ByteUtils.writeVarint(-1, out); // 表示空键
        } else {
            int keySize = key.remaining();
            ByteUtils.writeVarint(keySize, out); // 写入键长度
            Utils.writeTo(out, key, keySize); // 写入键内容
        }

        // 写入值（如果存在）
        if (value == null) {
            ByteUtils.writeVarint(-1, out); // 表示空值
        } else {
            int valueSize = value.remaining();
            ByteUtils.writeVarint(valueSize, out); // 写入值长度
            Utils.writeTo(out, value, valueSize); // 写入值内容
        }

        // 验证头部数组不为null
        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        // 写入头部数量
        ByteUtils.writeVarint(headers.length, out);

        // 写入每个头部的键和值
        for (Header header : headers) {
            String headerKey = header.key();
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            // 写入头部键
            byte[] utf8Bytes = Utils.utf8(headerKey);
            ByteUtils.writeVarint(utf8Bytes.length, out); // 写入键长度
            out.write(utf8Bytes); // 写入键内容

            // 写入头部值
            byte[] headerValue = header.value();
            if (headerValue == null) {
                ByteUtils.writeVarint(-1, out); // 表示空值
            } else {
                ByteUtils.writeVarint(headerValue.length, out); // 写入值长度
                out.write(headerValue); // 写入值内容
            }
        }

        // 返回写入的总字节数（记录体长度的变长整数大小 + 记录体大小）
        return ByteUtils.sizeOfVarint(sizeInBytes) + sizeInBytes;
    }

    @Override
    public boolean hasMagic(byte magic) {
        return magic >= MAGIC_VALUE_V2;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return false;
    }

    @Override
    public String toString() {
        return String.format("DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
                offset,
                timestamp,
                key == null ? 0 : key.limit(),
                value == null ? 0 : value.limit());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecord that = (DefaultRecord) o;
        return sizeInBytes == that.sizeInBytes &&
                attributes == that.attributes &&
                offset == that.offset &&
                timestamp == that.timestamp &&
                sequence == that.sequence &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = sizeInBytes;
        result = 31 * result + (int) attributes;
        result = 31 * result + Long.hashCode(offset);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + sequence;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    public static DefaultRecord readFrom(InputStream input,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        ByteBuffer recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes);
        int bytesRead = Utils.readFully(input, recordBuffer);
        if (bytesRead != sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                " bytes in record payload, but the record payload reached EOF.");
        recordBuffer.flip(); // prepare for reading
        return readFrom(recordBuffer, sizeOfBodyInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }

    public static DefaultRecord readFrom(ByteBuffer buffer,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) {
        int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        return readFrom(buffer, sizeOfBodyInBytes, baseOffset, baseTimestamp,
            baseSequence, logAppendTime);
    }

    /**
     * 从ByteBuffer中读取一条记录。
     * 记录的反序列化过程如下：
     * 1. 验证缓冲区中剩余的字节数是否足够
     * 2. 读取属性标志位
     * 3. 读取并计算时间戳（基准时间戳 + 增量）
     * 4. 读取并计算偏移量（基准偏移量 + 增量）
     * 5. 读取并计算序列号
     * 6. 读取键（如果存在）
     * 7. 读取值（如果存在）
     * 8. 读取头部信息
     * 9. 验证读取的字节数是否正确
     *
     * @param buffer 包含记录数据的ByteBuffer
     * @param sizeOfBodyInBytes 记录体的字节大小
     * @param baseOffset 批次的基准偏移量
     * @param baseTimestamp 批次的基准时间戳
     * @param baseSequence 批次的基准序列号
     * @param logAppendTime 日志追加时间（如果使用）
     * @return 解析出的DefaultRecord实例
     * @throws InvalidRecordException 如果记录格式无效或数据不完整
     */
    private static DefaultRecord readFrom(ByteBuffer buffer,
                                          int sizeOfBodyInBytes,
                                          long baseOffset,
                                          long baseTimestamp,
                                          int baseSequence,
                                          Long logAppendTime) {
        // 验证缓冲区中剩余的字节数是否足够
        if (buffer.remaining() < sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                " bytes in record payload, but instead the buffer has only " + buffer.remaining() +
                " remaining bytes.");
        try {
            // 记录开始位置，用于后续验证读取的字节数
            int recordStart = buffer.position();
            
            // 读取属性标志位（1字节）
            byte attributes = buffer.get();
            
            // 读取时间戳增量并计算绝对时间戳
            long timestampDelta = ByteUtils.readVarlong(buffer);
            long timestamp = baseTimestamp + timestampDelta;
            // 如果指定了日志追加时间，则使用该时间
            if (logAppendTime != null)
                timestamp = logAppendTime;

            // 读取偏移量增量并计算绝对偏移量
            int offsetDelta = ByteUtils.readVarint(buffer);
            long offset = baseOffset + offsetDelta;
            
            // 计算序列号（用于幂等性生产者）
            int sequence = baseSequence >= 0 ?
                    DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
                    RecordBatch.NO_SEQUENCE;

            // 读取键（如果存在）
            int keySize = ByteUtils.readVarint(buffer);
            ByteBuffer key = Utils.readBytes(buffer, keySize);

            // 读取值（如果存在）
            int valueSize = ByteUtils.readVarint(buffer);
            ByteBuffer value = Utils.readBytes(buffer, valueSize);

            // 读取头部数量
            int numHeaders = ByteUtils.readVarint(buffer);
            if (numHeaders < 0)
                throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
            if (numHeaders > buffer.remaining())
                throw new InvalidRecordException("Found invalid number of record headers. " + numHeaders + " is larger than the remaining size of the buffer");

            // 读取头部信息
            final Header[] headers;
            if (numHeaders == 0)
                headers = Record.EMPTY_HEADERS;
            else
                headers = readHeaders(buffer, numHeaders);

            // 验证是否正确读取了所有字节
            if (buffer.position() - recordStart != sizeOfBodyInBytes)
                throw new InvalidRecordException("Invalid record size: expected to read " + sizeOfBodyInBytes +
                        " bytes in record payload, but instead read " + (buffer.position() - recordStart));

            // 计算记录的总大小（包括记录体长度的变长整数）
            int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
            
            // 创建并返回新的DefaultRecord实例
            return new DefaultRecord(totalSizeInBytes, attributes, offset, timestamp, sequence, key, value, headers);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }

    /**
     * 从输入流中部分读取一条记录，只读取记录的基本信息而不读取实际内容。
     * 这个方法主要用于快速扫描记录的元数据，而不需要加载完整的记录内容到内存中。
     *
     * @param input 包含记录数据的输入流
     * @param baseOffset 批次的基准偏移量，用于计算记录的实际偏移量
     * @param baseTimestamp 批次的基准时间戳，用于计算记录的实际时间戳
     * @param baseSequence 批次的基准序列号，用于计算记录的实际序列号
     * @param logAppendTime 如果不为null，则使用这个时间戳替代记录的原始时间戳
     * @return 包含记录基本信息的PartialDefaultRecord实例
     * @throws IOException 如果读取过程中发生I/O错误
     */
    public static PartialDefaultRecord readPartiallyFrom(InputStream input,
                                                         long baseOffset,
                                                         long baseTimestamp,
                                                         int baseSequence,
                                                         Long logAppendTime) throws IOException {
        // 读取记录体的大小（变长整数编码）
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        // 计算记录的总大小（记录体大小 + 记录体大小的变长整数编码的大小）
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;

        return readPartiallyFrom(input, totalSizeInBytes, baseOffset, baseTimestamp,
            baseSequence, logAppendTime);
    }

    /**
     * 从输入流中部分读取一条记录的内部实现方法。
     * 这个方法会读取记录的所有元数据信息，但会跳过实际的键值对和头部内容，
     * 从而减少内存使用并提高处理速度。
     *
     * @param input 包含记录数据的输入流
     * @param sizeInBytes 记录的总字节大小
     * @param baseOffset 批次的基准偏移量
     * @param baseTimestamp 批次的基准时间戳
     * @param baseSequence 批次的基准序列号
     * @param logAppendTime 如果不为null，则使用这个时间戳替代记录的原始时间戳
     * @return 包含记录基本信息的PartialDefaultRecord实例
     * @throws IOException 如果读取过程中发生I/O错误
     * @throws InvalidRecordException 如果记录格式无效或数据不完整
     */
    private static PartialDefaultRecord readPartiallyFrom(InputStream input,
                                                          int sizeInBytes,
                                                          long baseOffset,
                                                          long baseTimestamp,
                                                          int baseSequence,
                                                          Long logAppendTime) throws IOException {
        try {
            // 读取属性标志位（1字节）
            byte attributes = (byte) input.read();
            
            // 读取时间戳增量并计算实际时间戳
            long timestampDelta = ByteUtils.readVarlong(input);
            long timestamp = baseTimestamp + timestampDelta;
            // 如果指定了日志追加时间，则使用该时间
            if (logAppendTime != null)
                timestamp = logAppendTime;

            // 读取偏移量增量并计算实际偏移量
            int offsetDelta = ByteUtils.readVarint(input);
            long offset = baseOffset + offsetDelta;
            // 计算序列号（用于幂等性生产者）
            int sequence = baseSequence >= 0 ?
                DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
                RecordBatch.NO_SEQUENCE;

            // 读取键的长度并跳过键的内容
            int keySize = ByteUtils.readVarint(input);
            skipBytes(input, keySize);

            // 读取值的长度并跳过值的内容
            int valueSize = ByteUtils.readVarint(input);
            skipBytes(input, valueSize);

            // 读取头部数量
            int numHeaders = ByteUtils.readVarint(input);
            if (numHeaders < 0)
                throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
            
            // 跳过所有头部的内容
            for (int i = 0; i < numHeaders; i++) {
                // 读取头部键的长度并跳过键的内容
                int headerKeySize = ByteUtils.readVarint(input);
                if (headerKeySize < 0)
                    throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);
                skipBytes(input, headerKeySize);

                // 读取头部值的长度并跳过值的内容
                int headerValueSize = ByteUtils.readVarint(input);
                skipBytes(input, headerValueSize);
            }

            // 创建并返回包含基本信息的PartialDefaultRecord实例
            return new PartialDefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, keySize, valueSize);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }


    /**
     * 从输入流中跳过指定数量的字节。
     * 这个方法确保精确地跳过指定的字节数，即使底层的InputStream.skip()方法
     * 可能一次跳过的字节数少于请求的字节数。
     *
     * 实现说明：
     * 1. 由于InputStream.skip()方法可能跳过比请求更少的字节数，
     *    所以需要在循环中多次调用skip直到跳过所有请求的字节。
     * 2. 当skip()返回0时，说明没有跳过任何字节，这时需要尝试读取一个字节
     *    来检查是否已到达流的末尾。
     * 3. 从JDK 12开始，这个实现可以被InputStream.skipNBytes()方法替代。
     *
     * @param in 要跳过字节的输入流
     * @param bytesToSkip 要跳过的字节数
     * @throws InvalidRecordException 如果在跳过所有请求的字节之前到达了流的末尾
     * @throws IOException 如果在尝试跳过字节时发生I/O错误
     * 
     * @see java.io.InputStream#skip(long)
     */
    private static void skipBytes(InputStream in, int bytesToSkip) throws IOException {
        // 如果要跳过的字节数小于等于0，直接返回
        // 这种情况可能发生在字段值为null的情况下
        if (bytesToSkip <= 0) return;

        // 循环直到跳过所有请求的字节
        while (bytesToSkip > 0) {
            // 尝试跳过指定数量的字节
            int ns = (int) in.skip(bytesToSkip);
            if (ns > 0 && ns <= bytesToSkip) {
                // 成功跳过了一些字节，更新剩余需要跳过的字节数
                bytesToSkip -= ns;
            } else if (ns == 0) { // 没有跳过任何字节
                // 尝试读取一个字节来检查是否到达了流的末尾
                if (in.read() == -1) {
                    throw new InvalidRecordException("Reached end of input stream before skipping all bytes. " +
                        "Remaining bytes:" + bytesToSkip);
                }
                // 成功读取了一个字节，减少需要跳过的字节数
                bytesToSkip--;
            } else { // skip()返回了负数或超过请求的字节数
                throw new IOException("Unable to skip exactly");
            }
        }
    }

    /**
     * 从ByteBuffer中读取记录的头部信息。
     * 头部信息的格式为：[HeaderKey HeaderValue]数组，每个元素包含：
     * - HeaderKeyLength => Varint（头部键长度）
     * - HeaderKey => String（头部键内容）
     * - HeaderValueLength => Varint（头部值长度）
     * - HeaderValue => Bytes（头部值内容）
     *
     * @param buffer 包含头部信息的ByteBuffer
     * @param numHeaders 头部信息的数量
     * @return 解析出的Header数组
     * @throws InvalidRecordException 如果头部键长度为负数
     */
    private static Header[] readHeaders(ByteBuffer buffer, int numHeaders) {
        // 创建指定大小的头部数组
        Header[] headers = new Header[numHeaders];
        
        // 遍历读取每个头部信息
        for (int i = 0; i < numHeaders; i++) {
            // 读取头部键的长度（变长整数）
            int headerKeySize = ByteUtils.readVarint(buffer);
            // 验证头部键长度的有效性
            if (headerKeySize < 0)
                throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);

            // 读取头部键的内容
            ByteBuffer headerKeyBuffer = Utils.readBytes(buffer, headerKeySize);

            // 读取头部值的长度（变长整数）
            int headerValueSize = ByteUtils.readVarint(buffer);
            // 读取头部值的内容
            ByteBuffer headerValue = Utils.readBytes(buffer, headerValueSize);

            // 创建新的RecordHeader并存储到数组中
            headers[i] = new RecordHeader(headerKeyBuffer, headerValue);
        }

        return headers;
    }

    /**
     * 计算记录的总字节大小。
     * 总大小包括：记录体大小的变长整数 + 记录体大小
     *
     * @param offsetDelta 相对于批次基准偏移量的偏移量增量
     * @param timestampDelta 相对于批次基准时间戳的时间戳增量
     * @param key 记录的键（可选）
     * @param value 记录的值
     * @param headers 记录的头部信息数组
     * @return 记录的总字节大小
     */
    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  ByteBuffer key,
                                  ByteBuffer value,
                                  Header[] headers) {
        // 计算记录体的大小
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        // 返回：记录体大小的变长整数所占字节数 + 记录体大小
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    /**
     * 计算记录的总字节大小（重载方法）。
     * 与上一个方法类似，但接受键和值的大小作为参数，而不是实际的ByteBuffer对象。
     *
     * @param offsetDelta 相对于批次基准偏移量的偏移量增量
     * @param timestampDelta 相对于批次基准时间戳的时间戳增量
     * @param keySize 键的字节大小（-1表示没有键）
     * @param valueSize 值的字节大小（-1表示没有值）
     * @param headers 记录的头部信息数组
     * @return 记录的总字节大小
     */
    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  int keySize,
                                  int valueSize,
                                  Header[] headers) {
        // 计算记录体的大小
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers);
        // 返回：记录体大小的变长整数所占字节数 + 记录体大小
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    /**
     * 计算记录体的字节大小。
     * 将ByteBuffer类型的键和值转换为对应的大小，然后调用重载方法计算记录体大小。
     *
     * @param offsetDelta 相对于批次基准偏移量的偏移量增量
     * @param timestampDelta 相对于批次基准时间戳的时间戳增量
     * @param key 记录的键（可选）
     * @param value 记录的值
     * @param headers 记录的头部信息数组
     * @return 记录体的字节大小
     */
    private static int sizeOfBodyInBytes(int offsetDelta,
                                         long timestampDelta,
                                         ByteBuffer key,
                                         ByteBuffer value,
                                         Header[] headers) {
        // 计算键的大小（如果键为null则返回-1）
        int keySize = key == null ? -1 : key.remaining();
        // 计算值的大小（如果值为null则返回-1）
        int valueSize = value == null ? -1 : value.remaining();
        // 调用重载方法计算记录体大小
        return sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers);
    }

    /**
     * 计算记录体的字节大小（重载方法）。
     * 记录体包括：属性、时间戳增量、偏移量增量、键、值和头部信息。
     *
     * @param offsetDelta 相对于批次基准偏移量的偏移量增量
     * @param timestampDelta 相对于批次基准时间戳的时间戳增量
     * @param keySize 键的字节大小（-1表示没有键）
     * @param valueSize 值的字节大小（-1表示没有值）
     * @param headers 记录的头部信息数组
     * @return 记录体的字节大小
     */
    public static int sizeOfBodyInBytes(int offsetDelta,
                                        long timestampDelta,
                                        int keySize,
                                        int valueSize,
                                        Header[] headers) {
        int size = 1; // 属性字段固定占用1字节
        // 计算偏移量增量的变长整数大小
        size += ByteUtils.sizeOfVarint(offsetDelta);
        // 计算时间戳增量的变长长整数大小
        size += ByteUtils.sizeOfVarlong(timestampDelta);
        // 计算键、值和头部信息的总大小
        size += sizeOf(keySize, valueSize, headers);
        return size;
    }

    /**
     * 计算记录的键、值和头部信息的总字节大小。
     *
     * @param keySize 键的字节大小（-1表示没有键）
     * @param valueSize 值的字节大小（-1表示没有值）
     * @param headers 记录的头部信息数组
     * @return 键、值和头部信息的总字节大小
     * @throws IllegalArgumentException 如果headers为null或者包含null的键
     */
    private static int sizeOf(int keySize, int valueSize, Header[] headers) {
        int size = 0;
        
        // 计算键的大小
        if (keySize < 0)
            // 如果没有键，添加表示null的变长整数大小
            size += NULL_VARINT_SIZE_BYTES;
        else
            // 键的大小 = 键长度的变长整数大小 + 键的实际大小
            size += ByteUtils.sizeOfVarint(keySize) + keySize;

        // 计算值的大小
        if (valueSize < 0)
            // 如果没有值，添加表示null的变长整数大小
            size += NULL_VARINT_SIZE_BYTES;
        else
            // 值的大小 = 值长度的变长整数大小 + 值的实际大小
            size += ByteUtils.sizeOfVarint(valueSize) + valueSize;

        // 验证头部数组不为null
        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        // 添加头部数量的变长整数大小
        size += ByteUtils.sizeOfVarint(headers.length);
        // 计算每个头部的大小
        for (Header header : headers) {
            // 获取头部键
            String headerKey = header.key();
            // 验证头部键不为null
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            // 计算头部键的UTF-8编码大小
            int headerKeySize = Utils.utf8Length(headerKey);
            // 头部键的总大小 = 键长度的变长整数大小 + 键的实际大小
            size += ByteUtils.sizeOfVarint(headerKeySize) + headerKeySize;

            // 获取头部值
            byte[] headerValue = header.value();
            if (headerValue == null) {
                // 如果头部值为null，添加表示null的变长整数大小
                size += NULL_VARINT_SIZE_BYTES;
            } else {
                // 头部值的总大小 = 值长度的变长整数大小 + 值的实际大小
                size += ByteUtils.sizeOfVarint(headerValue.length) + headerValue.length;
            }
        }
        return size;
    }

    /**
     * 计算记录大小的上限。
     * 上限等于记录的最大开销（固定部分）加上键、值和头部信息的大小。
     *
     * @param key 记录的键（可选）
     * @param value 记录的值
     * @param headers 记录的头部信息数组
     * @return 记录大小的上限字节数
     */
    static int recordSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 计算键的大小（如果键为null则返回-1）
        int keySize = key == null ? -1 : key.remaining();
        // 计算值的大小（如果值为null则返回-1）
        int valueSize = value == null ? -1 : value.remaining();
        // 返回：记录的最大开销 + 键、值和头部信息的大小
        return MAX_RECORD_OVERHEAD + sizeOf(keySize, valueSize, headers);
    }
}
