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
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Checksums;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * 该类表示消息格式版本0和1中序列化的键值对以及相关的CRC和其他字段。
 * 注意：通常不需要直接访问此类，而是通过{@link Records}对象暴露的{@link Record}接口间接访问。
 * 
 * 应用场景：
 * 1. 处理Kafka早期版本(V0/V1)的消息格式
 * 2. 提供向后兼容性支持
 * 3. 在消息格式升级过程中进行版本转换
 */
public final class LegacyRecord {

    /**
     * 所有固定长度字段的当前偏移量和大小
     * 消息格式布局(按字节偏移):
     * V0: CRC(4) + Magic(1) + Attributes(1) + Key长度(4) + Key + Value长度(4) + Value
     * V1: CRC(4) + Magic(1) + Attributes(1) + 时间戳(8) + Key长度(4) + Key + Value长度(4) + Value
     */
    public static final int CRC_OFFSET = 0;          // CRC校验码的起始位置
    public static final int CRC_LENGTH = 4;          // CRC校验码占4字节
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;  // 消息格式版本号的位置
    public static final int MAGIC_LENGTH = 1;        // 版本号占1字节
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;  // 属性字段的位置
    public static final int ATTRIBUTES_LENGTH = 1;    // 属性字段占1字节
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;  // 时间戳字段的位置(仅V1)
    public static final int TIMESTAMP_LENGTH = 8;     // 时间戳占8字节
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;  // V0版本Key长度的位置
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;    // V1版本Key长度的位置
    public static final int KEY_SIZE_LENGTH = 4;      // Key长度字段占4字节
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;  // V0版本Key数据的位置
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;  // V1版本Key数据的位置
    public static final int VALUE_SIZE_LENGTH = 4;    // Value长度字段占4字节

    /**
     * 记录头部的大小
     * V0版本: CRC(4) + Magic(1) + Attributes(1) = 6字节
     * V1版本: CRC(4) + Magic(1) + Attributes(1) + Timestamp(8) = 14字节
     */
    public static final int HEADER_SIZE_V0 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;
    public static final int HEADER_SIZE_V1 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH;

    /**
     * 记录的开销字节数
     * V0版本: 头部(6) + Key长度(4) + Value长度(4) = 14字节
     */
    public static final int RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * 记录的开销字节数
     * V1版本: 头部(14) + Key长度(4) + Value长度(4) = 22字节
     */
    public static final int RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * 压缩编码掩码
     * 使用3位来存储压缩编解码器类型，0表示不压缩
     * 支持最多8种压缩类型(0~7)
     */
    private static final byte COMPRESSION_CODEC_MASK = 0x07;

    /**
     * 时间戳类型掩码
     * 0: CreateTime - 消息创建时间
     * 1: LogAppendTime - 消息追加到日志时间
     */
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    /**
     * 表示没有时间戳的记录的时间戳值
     */
    public static final long NO_TIMESTAMP = -1L;

    // 存储序列化消息数据的缓冲区
    private final ByteBuffer buffer;
    // 包装记录的时间戳(用于压缩消息)
    private final Long wrapperRecordTimestamp;
    // 包装记录的时间戳类型(用于压缩消息)
    private final TimestampType wrapperRecordTimestampType;

    /**
     * 创建一个新的LegacyRecord实例
     * @param buffer 包含序列化消息数据的缓冲区
     */
    public LegacyRecord(ByteBuffer buffer) {
        this(buffer, null, null);
    }

    /**
     * 创建一个新的LegacyRecord实例，支持压缩消息场景
     * @param buffer 包含序列化消息数据的缓冲区
     * @param wrapperRecordTimestamp 外层记录的时间戳
     * @param wrapperRecordTimestampType 外层记录的时间戳类型
     */
    public LegacyRecord(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    /**
     * 计算记录内容的校验和
     * 实现细节：从magic字段开始计算CRC32校验码，不包含CRC字段本身
     * @return 计算得到的CRC32校验码
     */
    public long computeChecksum() {
        return crc32(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * 获取记录中存储的CRC校验码
     * 实现细节：从缓冲区的CRC_OFFSET位置读取4字节无符号整数
     * @return 存储的CRC32校验码
     */
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * 检查记录是否有效
     * 实现细节：
     * 1. 验证记录大小是否至少包含V0版本的开销字节数
     * 2. 比较存储的CRC和计算的CRC是否相等
     * @return 如果记录有效返回true，否则返回false
     */
    public boolean isValid() {
        return sizeInBytes() >= RECORD_OVERHEAD_V0 && checksum() == computeChecksum();
    }

    /**
     * 确保记录有效，如果无效则抛出异常
     * 实现细节：
     * 1. 检查记录大小是否小于最小开销
     * 2. 验证CRC校验码是否匹配
     * @throws CorruptRecordException 当记录损坏时抛出此异常
     */
    public void ensureValid() {
        if (sizeInBytes() < RECORD_OVERHEAD_V0)
            throw new CorruptRecordException("Record is corrupt (crc could not be retrieved as the record is too "
                    + "small, size = " + sizeInBytes() + ")");

        if (!isValid())
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * 获取记录的完整序列化大小(字节)
     * 包含CRC、头部属性等，但不包括日志开销(偏移量和记录大小)
     * 实现细节：直接返回缓冲区的限制值，表示实际使用的字节数
     * @return 记录大小(字节)
     */
    public int sizeInBytes() {
        return buffer.limit();
    }

    /**
     * 获取Key的字节长度
     * 实现细节：
     * 1. 根据消息格式版本选择正确的偏移量位置
     * 2. 从对应位置读取4字节整数表示的Key长度
     * @return Key的字节大小(如果Key为null则返回0)
     */
    public int keySize() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        else
            return buffer.getInt(KEY_SIZE_OFFSET_V1);
    }

    /**
     * 检查记录是否包含Key
     * 实现细节：通过检查Key大小是否大于等于0来判断
     * @return 如果有Key返回true，否则返回false
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * 获取Value大小字段的存储位置
     * 实现细节：
     * 1. 根据消息格式版本选择基础偏移量
     * 2. 加上Key的实际大小(如果Key存在)
     * @return Value大小字段的字节偏移量
     */
    private int valueSizeOffset() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        else
            return KEY_OFFSET_V1 + Math.max(0, keySize());
    }

    /**
     * 获取Value的字节长度
     * 实现细节：从计算得到的Value大小字段位置读取4字节整数
     * @return Value的字节大小(如果Value为null则返回0)
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * 检查记录的Value字段是否为null
     * 实现细节：通过检查Value大小是否小于0来判断
     * @return 如果Value为null返回true，否则返回false
     */
    public boolean hasNullValue() {
        return valueSize() < 0;
    }

    /**
     * 获取消息格式版本号(magic value)
     * 实现细节：从MAGIC_OFFSET位置读取1字节作为版本号
     * @return 消息格式版本号
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * 获取记录的属性字段
     * 实现细节：从ATTRIBUTES_OFFSET位置读取1字节的属性值
     * 属性字段包含：
     * - 低3位：压缩类型(0-7)
     * - 第4位：时间戳类型(0=创建时间，1=追加时间)
     * @return 属性字节值
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * 获取记录的时间戳
     * 实现细节：
     * 1. V0版本：始终返回NO_TIMESTAMP
     * 2. V1版本：根据以下规则确定
     *   - 未压缩消息：直接从消息中读取时间戳
     *   - 压缩消息(LOG_APPEND_TIME)：使用外层记录的时间戳
     *   - 压缩消息(CREATE_TIME)：使用消息自身的时间戳
     * @return 记录的时间戳值
     */
    public long timestamp() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return RecordBatch.NO_TIMESTAMP;
        else {
            // case 2: 使用日志追加时间
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null)
                return wrapperRecordTimestamp;
            // Case 1,3: 使用消息中的时间戳
            else
                return buffer.getLong(TIMESTAMP_OFFSET);
        }
    }

    /**
     * 获取记录的时间戳类型
     * 实现细节：
     * 1. V0版本：返回NO_TIMESTAMP_TYPE
     * 2. V1版本：根据属性字段和外层记录的时间戳类型确定
     * @return 时间戳类型，如果是V0版本则返回NO_TIMESTAMP_TYPE
     */
    public TimestampType timestampType() {
        return timestampType(magic(), wrapperRecordTimestampType, attributes());
    }

    /**
     * 获取记录使用的压缩类型
     * 实现细节：
     * 1. 从ATTRIBUTES_OFFSET位置读取属性字节
     * 2. 使用COMPRESSION_CODEC_MASK(0x07)提取压缩类型
     * 3. 通过CompressionType.forId转换为对应的压缩类型枚举值
     * @return 记录的压缩类型，如果未压缩则返回NONE
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * 获取记录的值内容
     * 实现细节：
     * 1. 通过valueSizeOffset()获取值大小字段的位置
     * 2. 使用Utils.sizeDelimited从缓冲区中提取值内容
     * @return 包含记录值的ByteBuffer，如果值为null则返回null
     */
    public ByteBuffer value() {
        return Utils.sizeDelimited(buffer, valueSizeOffset());
    }

    /**
     * 获取记录的键内容
     * 实现细节：
     * 1. 根据magic值判断记录版本
     * 2. 从对应版本的键偏移位置读取键内容
     * @return 包含记录键的ByteBuffer，如果键为null则返回null
     */
    public ByteBuffer key() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V0);
        else
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V1);
    }

    /**
     * 获取记录的底层缓冲区
     * 实现细节：直接返回存储记录数据的ByteBuffer对象
     * @return 记录的底层缓冲区
     */
    public ByteBuffer buffer() {
        return this.buffer;
    }

    public String toString() {
        if (magic() > 0)
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, %s=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 timestampType(),
                                 timestamp(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
        else
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(LegacyRecord.class))
            return false;
        LegacyRecord record = (LegacyRecord) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

    /**
     * 创建一个新的记录实例
     * 实现细节：
     * 1. 计算键值的大小
     * 2. 分配足够大小的缓冲区
     * 3. 写入记录数据
     * 4. 重置缓冲区位置
     * 
     * 注意：如果记录的压缩类型不是NONE，则其值负载应该已经被压缩；
     * 构造函数会原样写入值负载，不会执行压缩操作。
     *
     * @param magic 使用的魔数值(版本号)
     * @param timestamp 记录的时间戳
     * @param key 记录的键(如果没有则为null)
     * @param value 记录的值
     * @param compressionType 记录内容使用的压缩类型
     * @param timestampType 记录使用的时间戳类型
     */
    public static LegacyRecord create(byte magic,
                                      long timestamp,
                                      byte[] key,
                                      byte[] value,
                                      CompressionType compressionType,
                                      TimestampType timestampType) {
        int keySize = key == null ? 0 : key.length;
        int valueSize = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize));
        write(buffer, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
        buffer.rewind();
        return new LegacyRecord(buffer);
    }

    /**
     * 使用默认配置创建一个新的记录实例
     * 实现细节：调用完整版本的create方法，使用无压缩和创建时间类型
     * @param magic 使用的魔数值(版本号)
     * @param timestamp 记录的时间戳
     * @param key 记录的键(如果没有则为null)
     * @param value 记录的值
     * @return 新创建的LegacyRecord实例
     */
    public static LegacyRecord create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    /**
     * 原地写入压缩记录集的头部信息
     * 假设压缩的记录数据已经写入到包装记录的值偏移位置。这允许动态创建压缩消息集，
     * 然后稍后回填其大小和CRC，避免了复制到另一个缓冲区的需要。
     * 
     * 实现细节：
     * 1. 记录当前缓冲区位置
     * 2. 计算实际值大小(总大小减去记录开销)
     * 3. 写入空值的记录头部(包装记录的键始终为null)
     * 4. 回填值大小
     * 5. 计算并填充CRC校验码
     * 
     * 应用场景：
     * 1. 批量压缩多条消息时的头部写入
     * 2. 动态构建压缩消息集
     * 3. 支持延迟填充大小和CRC的场景
     *
     * @param buffer 包含压缩记录数据的缓冲区，位于第一个偏移量处
     * @param magic 记录集的魔数值(版本号)
     * @param recordSize 记录的总大小(包含记录开销)
     * @param timestamp 包装记录的时间戳
     * @param compressionType 使用的压缩类型
     * @param timestampType 包装记录的时间戳类型
     */
    public static void writeCompressedRecordHeader(ByteBuffer buffer,
                                                   byte magic,
                                                   int recordSize,
                                                   long timestamp,
                                                   CompressionType compressionType,
                                                   TimestampType timestampType) {
        // 保存当前缓冲区位置，用于后续回填
        int recordPosition = buffer.position();
        // 计算实际值大小(总大小减去记录开销)
        int valueSize = recordSize - recordOverhead(magic);

        // 写入带空值的记录头部(包装记录的键始终为null)
        write(buffer, magic, timestamp, null, null, compressionType, timestampType);
        // 恢复到原始位置，准备回填
        buffer.position(recordPosition);

        // 回填值大小字段
        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        // 计算并填充消息开始处的CRC校验码
        long crc = crc32(buffer, MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    /**
     * 将记录写入ByteBuffer
     * 
     * 实现细节：
     * 1. 创建DataOutputStream包装ByteBuffer
     * 2. 调用完整的write方法写入记录
     * 3. 自动关闭流并处理异常
     * 
     * @param buffer 目标缓冲区
     * @param magic 魔数值(版本号)
     * @param timestamp 时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param compressionType 压缩类型
     * @param timestampType 时间戳类型
     */
    private static void write(ByteBuffer buffer,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) {
        try (DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer))) {
            write(out, magic, timestamp, key, value, compressionType, timestampType);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * 使用指定的压缩类型写入记录数据并返回计算的CRC校验码
     * 
     * 实现细节：
     * 1. 将字节数组包装为ByteBuffer
     * 2. 调用ByteBuffer版本的write方法
     * 
     * 应用场景：
     * 1. 写入原始字节数组形式的记录数据
     * 2. 支持压缩和时间戳类型设置
     *
     * @param out 输出流
     * @param magic 魔数值(版本号)
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param compressionType 压缩类型
     * @param timestampType 时间戳类型
     * @return 计算的CRC校验码
     * @throws IOException 写入输出流时发生IO错误
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             byte[] key,
                             byte[] value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        return write(out, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
    }

    /**
     * 使用ByteBuffer写入记录数据
     * 
     * 实现细节：
     * 1. 计算属性字节(包含压缩类型和时间戳类型)
     * 2. 计算记录的CRC校验码
     * 3. 写入完整的记录数据
     * 
     * 应用场景：
     * 1. 写入ByteBuffer形式的记录数据
     * 2. 支持压缩和时间戳类型设置
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             ByteBuffer key,
                             ByteBuffer value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        // 计算属性字节，包含压缩类型和时间戳类型信息
        byte attributes = computeAttributes(magic, compressionType, timestampType);
        // 计算记录的CRC校验码
        long crc = computeChecksum(magic, attributes, timestamp, key, value);
        // 写入完整的记录数据
        write(out, magic, crc, attributes, timestamp, key, value);
        return crc;
    }

    /**
     * 使用原始字段写入记录(不进行验证)
     * 此方法仅用于测试目的
     * 
     * 实现细节：
     * 1. 将字节数组包装为ByteBuffer
     * 2. 调用ByteBuffer版本的write方法
     */
    public static void write(DataOutputStream out,
                             byte magic,
                             long crc,
                             byte attributes,
                             long timestamp,
                             byte[] key,
                             byte[] value) throws IOException {
        write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    /**
     * 将记录写入缓冲区
     * 如果记录的压缩类型为NONE，则其值负载应该已经使用指定类型压缩
     * 
     * 实现细节：
     * 1. 验证魔数值和时间戳的有效性
     * 2. 按顺序写入：CRC、魔数值、属性、时间戳(V1)、键长度和内容、值长度和内容
     * 3. 对于null的键或值，写入-1作为长度
     * 
     * 应用场景：
     * 1. 写入完整的记录数据
     * 2. 支持V0和V1两个版本的消息格式
     * 3. 处理压缩和非压缩的消息
     */
    private static void write(DataOutputStream out,
                              byte magic,
                              long crc,
                              byte attributes,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value) throws IOException {
        // 验证魔数值的有效性
        if (magic != RecordBatch.MAGIC_VALUE_V0 && magic != RecordBatch.MAGIC_VALUE_V1)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        // 验证时间戳的有效性
        if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);

        // 写入CRC校验码(4字节)
        out.writeInt((int) (crc & 0xffffffffL));
        // 写入魔数值(1字节)
        out.writeByte(magic);
        // 写入属性(1字节)
        out.writeByte(attributes);

        // V1版本需要写入时间戳(8字节)
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            out.writeLong(timestamp);

        // 写入键数据
        if (key == null) {
            // 键为null时写入-1表示
            out.writeInt(-1);
        } else {
            // 写入键的长度和内容
            int size = key.remaining();
            out.writeInt(size);
            Utils.writeTo(out, key, size);
        }
        // 写入值数据
        if (value == null) {
            // 值为null时写入-1表示
            out.writeInt(-1);
        } else {
            // 写入值的长度和内容
            int size = value.remaining();
            out.writeInt(size);
            Utils.writeTo(out, value, size);
        }
    }

    /**
     * 计算记录的总大小(字节)
     * 
     * 实现细节：
     * 1. 检查key和value是否为null
     * 2. 如果不为null，获取其limit()值作为大小
     * 3. 调用另一个重载方法计算总大小
     * 
     * @param magic 消息格式版本号
     * @param key 记录的键缓冲区
     * @param value 记录的值缓冲区
     * @return 记录的总字节数
     */
    static int recordSize(byte magic, ByteBuffer key, ByteBuffer value) {
        return recordSize(magic, key == null ? 0 : key.limit(), value == null ? 0 : value.limit());
    }

    /**
     * 计算记录的总大小(字节)
     * 
     * 实现细节：
     * 1. 根据版本号获取记录开销大小
     * 2. 加上键和值的大小得到总大小
     * 
     * 应用场景：
     * 1. 分配缓冲区空间
     * 2. 验证记录大小
     * 3. 计算批次容量
     * 
     * @param magic 消息格式版本号
     * @param keySize 键的字节大小
     * @param valueSize 值的字节大小
     * @return 记录的总字节数
     */
    public static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    /**
     * 计算记录的属性字节
     * 
     * 实现细节：
     * 1. 初始化属性字节为0
     * 2. 如果使用压缩，设置压缩类型位
     * 3. 对于V1版本，设置时间戳类型位
     * 
     * 应用场景：
     * 1. 创建新记录时设置属性
     * 2. 支持消息压缩
     * 3. 控制时间戳类型
     * 
     * @param magic 消息格式版本号
     * @param type 压缩类型
     * @param timestampType 时间戳类型
     * @return 计算得到的属性字节
     * @throws IllegalArgumentException 当V1版本未指定时间戳类型时抛出
     */
    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        byte attributes = 0;
        if (type.id > 0)
            attributes |= (byte) (COMPRESSION_CODEC_MASK & type.id);
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
                throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for " +
                        "message format v1");
            if (timestampType == TimestampType.LOG_APPEND_TIME)
                attributes |= TIMESTAMP_TYPE_MASK;
        }
        return attributes;
    }

    /**
     * 计算记录的CRC32校验和(字节数组版本)
     * 
     * 实现细节：
     * 1. 将字节数组包装为ByteBuffer
     * 2. 调用ByteBuffer版本的方法
     * 
     * @param magic 消息格式版本号
     * @param attributes 属性字节
     * @param timestamp 时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @return CRC32校验和
     */
    public static long computeChecksum(byte magic, byte attributes, long timestamp, byte[] key, byte[] value) {
        return computeChecksum(magic, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    /**
     * 计算缓冲区指定范围的CRC32校验和
     * 
     * 实现细节：
     * 1. 创建CRC32对象
     * 2. 使用Checksums工具类更新校验和
     * 
     * @param buffer 源数据缓冲区
     * @param offset 起始偏移量
     * @param size 计算校验和的字节数
     * @return CRC32校验和
     */
    private static long crc32(ByteBuffer buffer, int offset, int size) {
        CRC32 crc = new CRC32();
        Checksums.update(crc, buffer, offset, size);
        return crc.getValue();
    }

    /**
     * 计算记录的CRC32校验和
     * 
     * 实现细节：
     * 1. 创建CRC32对象
     * 2. 按顺序更新各字段：
     *   - 版本号(magic)
     *   - 属性字节
     *   - 时间戳(仅V1版本)
     *   - 键的长度和内容
     *   - 值的长度和内容
     * 
     * 应用场景：
     * 1. 创建新记录时生成校验和
     * 2. 验证记录完整性
     * 3. 检测记录损坏
     * 
     * @param magic 消息格式版本号
     * @param attributes 属性字节
     * @param timestamp 时间戳
     * @param key 记录的键缓冲区
     * @param value 记录的值缓冲区
     * @return CRC32校验和
     */
    private static long computeChecksum(byte magic, byte attributes, long timestamp, ByteBuffer key, ByteBuffer value) {
        CRC32 crc = new CRC32();
        crc.update(magic);
        crc.update(attributes);
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            Checksums.updateLong(crc, timestamp);
        // 更新键的校验和
        if (key == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = key.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, key, size);
        }
        // 更新值的校验和
        if (value == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = value.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, value, size);
        }
        return crc.getValue();
    }

    /**
     * 获取指定版本记录的开销字节数
     * 
     * 实现细节：
     * 1. V0版本返回RECORD_OVERHEAD_V0(14字节)
     * 2. V1版本返回RECORD_OVERHEAD_V1(22字节)
     * 
     * @param magic 消息格式版本号
     * @return 记录开销字节数
     * @throws IllegalArgumentException 当版本号无效时抛出
     */
    static int recordOverhead(byte magic) {
        if (magic == 0)
            return RECORD_OVERHEAD_V0;
        else if (magic == 1)
            return RECORD_OVERHEAD_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    /**
     * 获取指定版本记录头部的大小
     * 
     * 实现细节：
     * 1. V0版本返回HEADER_SIZE_V0(6字节)
     * 2. V1版本返回HEADER_SIZE_V1(14字节)
     * 
     * @param magic 消息格式版本号
     * @return 记录头部大小
     * @throws IllegalArgumentException 当版本号无效时抛出
     */
    static int headerSize(byte magic) {
        if (magic == 0)
            return HEADER_SIZE_V0;
        else if (magic == 1)
            return HEADER_SIZE_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    /**
     * 获取指定版本记录键数据的偏移量
     * 
     * 实现细节：
     * 1. V0版本返回KEY_OFFSET_V0
     * 2. V1版本返回KEY_OFFSET_V1
     * 
     * @param magic 消息格式版本号
     * @return 键数据的偏移量
     * @throws IllegalArgumentException 当版本号无效时抛出
     */
    private static int keyOffset(byte magic) {
        if (magic == 0)
            return KEY_OFFSET_V0;
        else if (magic == 1)
            return KEY_OFFSET_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    /**
     * 确定记录的时间戳类型
     * 
     * 实现细节：
     * 1. V0版本总是返回NO_TIMESTAMP_TYPE
     * 2. V1版本：
     *   - 如果有外层记录时间戳类型，使用它
     *   - 否则根据属性字节判断(0=CREATE_TIME, 1=LOG_APPEND_TIME)
     * 
     * 应用场景：
     * 1. 处理压缩消息时确定内部记录的时间戳类型
     * 2. 在消息格式版本间转换时处理时间戳
     * 
     * @param magic 消息格式版本号
     * @param wrapperRecordTimestampType 外层记录的时间戳类型
     * @param attributes 记录的属性字节
     * @return 确定的时间戳类型
     */
    public static TimestampType timestampType(byte magic, TimestampType wrapperRecordTimestampType, byte attributes) {
        if (magic == 0)
            return TimestampType.NO_TIMESTAMP_TYPE;
        else if (wrapperRecordTimestampType != null)
            return wrapperRecordTimestampType;
        else
            return (attributes & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

}
