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

import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 记录批次（RecordBatch）是记录的容器。在旧版本的记录格式中（版本0和1），
 * 如果没有启用压缩，一个批次总是只包含一条记录，但启用压缩后可以包含多条记录。
 * 在新版本（魔数版本2及以上）中，无论是否启用压缩，一个批次通常都会包含多条记录。
 */
public interface RecordBatch extends Iterable<Record> {

    /**
     * 魔数值定义
     * - 魔数用于标识记录格式的版本
     * - 不同版本支持不同的特性
     */
    byte MAGIC_VALUE_V0 = 0;  // 最初的版本，功能最基础
    byte MAGIC_VALUE_V1 = 1;  // 添加了时间戳支持
    byte MAGIC_VALUE_V2 = 2;  // 引入了记录批次、幂等性和事务支持

    /**
     * 当前使用的魔数值
     * - 默认使用最新的V2版本
     */
    byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V2;

    /**
     * 表示没有时间戳的记录的时间戳值
     * - 用-1表示未设置时间戳
     */
    long NO_TIMESTAMP = -1L;

    /**
     * V2版本记录格式中使用的特殊值
     * - 用于非幂等性/非事务性生产者
     * - 或从旧格式升级时使用
     */
    long NO_PRODUCER_ID = -1L;    // 表示未设置生产者ID
    short NO_PRODUCER_EPOCH = -1;  // 表示未设置生产者世代
    int NO_SEQUENCE = -1;          // 表示未设置序列号

    /**
     * 用于表示未知的领导者世代
     * - 当记录集首次由生产者创建时使用
     * - 领导者世代用于确保副本一致性
     */
    int NO_PARTITION_LEADER_EPOCH = -1;

    /**
     * 检查此批次的校验和是否正确
     * 
     * 校验和用途：
     * 1. 确保数据完整性
     * 2. 检测数据传输或存储过程中的错误
     * 
     * @return 如果校验和正确返回true，否则返回false
     */
    boolean isValid();

    /**
     * 如果校验和无效则抛出异常
     * 
     * 使用场景：
     * 1. 在处理关键数据时强制进行完整性检查
     * 2. 在不允许处理损坏数据的场景中使用
     */
    void ensureValid();

    /**
     * 获取此记录批次的校验和，校验和覆盖批次头部和所有记录
     * 
     * 实现细节：
     * 1. 使用4字节无符号整数作为校验和
     * 2. 以long类型返回以支持大值
     * 
     * @return 4字节无符号校验和（以long表示）
     */
    long checksum();

    /**
     * 获取此记录批次的最大时间戳或日志追加时间
     * 
     * 工作原理：
     * 1. 如果时间戳类型是创建时间，返回批次中所有记录的最大时间戳
     * 2. 该值在日志压缩过程中会更新
     * 3. 用于跟踪批次中最新的记录时间
     * 
     * @return 最大时间戳
     */
    long maxTimestamp();

    /**
     * 获取此记录批次的时间戳类型
     * 
     * 说明：
     * 1. 对于魔数值为0的批次，将返回{@link TimestampType#NO_TIMESTAMP_TYPE}
     * 2. 时间戳类型用于区分创建时间和追加时间
     * 
     * @return 时间戳类型
     */
    TimestampType timestampType();

    /**
     * 获取此记录批次的基准偏移量
     * 
     * 版本差异：
     * 1. 魔数版本2之前：
     *    - 总是返回批次中第一条消息的偏移量
     *    - 需要深度迭代才能获取
     * 2. 魔数版本2及以上：
     *    - 返回原始记录批次的第一个偏移量（压缩之前的）
     *    - 对于未压缩的主题，行为与旧版本相同
     * 
     * 使用注意：
     * - 由于旧版本需要深度迭代，使用此方法时需谨慎
     * - 建议使用{@link #lastOffset()}，因为它对所有版本都更高效
     * 
     * @return 记录批次的基准偏移量（可能是也可能不是第一条记录的偏移量）
     */
    long baseOffset();

    /**
     * 获取此记录批次中的最后一个偏移量（包含）
     * 
     * 特点：
     * 1. 与{@link #baseOffset()}类似，保留原始批次信息
     * 2. 即使在日志压缩过程中记录被删除，仍返回原始批次最后一条记录的偏移量
     * 3. 用于保持批次边界的完整性
     * 
     * @return 批次中最后一条记录的偏移量
     */
    long lastOffset();

    /**
     * Get the offset following this record batch (i.e. the last offset contained in this batch plus one).
     *
     * @return the next consecutive offset following this batch
     */
    long nextOffset();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    byte magic();

    /**
     * 获取此日志记录批次的生产者ID
     * 
     * 说明：
     * 1. 用于幂等性和事务特性
     * 2. 在旧的魔数版本中返回-1
     * 3. 每个生产者都有唯一的ID
     * 
     * @return 生产者ID，如果没有则返回-1
     */
    long producerId();

    /**
     * 获取此日志记录批次的生产者世代
     * 
     * 用途：
     * 1. 用于处理生产者重启和故障转移
     * 2. 每次生产者重启时世代会增加
     * 3. 帮助识别过期的事务和请求
     * 
     * @return 生产者世代，如果没有则返回-1
     */
    short producerEpoch();

    /**
     * 检查批次是否设置了有效的生产者ID
     * 
     * 使用场景：
     * 1. 验证幂等性生产者的消息
     * 2. 事务消息的处理
     * 3. 消息去重处理
     */
    boolean hasProducerId();

    /**
     * 获取此记录批次的基准序列号
     * 
     * 特点：
     * 1. 与{@link #baseOffset()}类似，不受压缩影响
     * 2. 始终保持原始批次的基准序列号
     * 3. 用于幂等性生产者的消息排序
     * 
     * @return 第一个序列号，如果没有则返回-1
     */
    int baseSequence();

    /**
     * 获取此记录批次的最后一个序列号
     * 
     * 特点：
     * 1. 与{@link #lastOffset()}类似，保留原始信息
     * 2. 即使在日志压缩中记录被删除，仍返回原始批次最后一条记录的序列号
     * 3. 用于维护消息顺序和检测丢失的消息
     * 
     * @return 最后一个序列号，如果没有则返回-1
     */
    int lastSequence();

    /**
     * 获取此记录批次的压缩类型
     * 
     * 压缩的作用：
     * 1. 减少存储空间
     * 2. 降低网络传输开销
     * 3. 提高整体性能
     * 
     * @return 压缩类型
     */
    CompressionType compressionType();

    /**
     * 获取此批次的字节大小，包括记录大小和批次开销
     * 
     * 计算内容：
     * 1. 批次头部大小
     * 2. 所有记录的大小
     * 3. 元数据开销
     * 
     * @return 批次的总字节数
     */
    int sizeInBytes();

    /**
     * 获取记录数量（仅在魔数版本2及以上支持高效获取）
     * 
     * 版本说明：
     * 1. 魔数版本2及以上：直接返回批次中的记录数
     * 2. 魔数版本0和1：返回null，因为需要遍历才能获取准确数量
     * 
     * @return 批次中的记录数，对于魔数版本0和1返回null
     */
    Integer countOrNull();

    /**
     * 检查此记录批次是否被压缩
     * 
     * 压缩状态：
     * 1. true - 批次数据已压缩
     * 2. false - 批次数据未压缩
     * 
     * @return 如果已压缩返回true，否则返回false
     */
    boolean isCompressed();

    /**
     * 将此记录批次写入缓冲区
     * 
     * 序列化过程：
     * 1. 写入批次头部信息
     * 2. 写入所有记录数据
     * 3. 如果启用压缩，在写入前进行压缩
     * 
     * @param buffer 目标缓冲区
     */
    void writeTo(ByteBuffer buffer);

    /**
     * 检查此记录批次是否是事务的一部分
     * 
     * 事务特性：
     * 1. 用于保证多条消息的原子性写入
     * 2. 支持跨分区的事务操作
     * 3. 仅在魔数版本2及以上支持
     * 
     * @return 如果是事务的一部分返回true，否则返回false
     */
    boolean isTransactional();

    /**
     * 获取删除范围的时间戳
     * 
     * 说明：
     * 1. 用于日志压缩和清理
     * 2. 如果第一个时间戳不是删除范围，返回OptionalLong.EMPTY
     * 
     * @return 删除范围的时间戳
     */
    OptionalLong deleteHorizonMs();

    /**
     * 获取此记录批次的分区领导者世代
     * 
     * 用途：
     * 1. 用于检测领导者变更
     * 2. 确保副本一致性
     * 3. 防止脑裂问题
     * 
     * @return 领导者世代，如果未知则返回-1
     */
    int partitionLeaderEpoch();

    /**
     * 返回一个流式迭代器，它会延迟记录流的解压缩操作，直到实际调用{@link Iterator#next()}时才进行
     * 
     * 工作原理：
     * 1. 延迟解压缩：只在实际需要访问记录时才解压缩
     * 2. 内存优化：避免一次性解压整个批次
     * 3. 性能优化：通过重用缓冲区减少内存分配
     * 
     * 注意事项：
     * 1. 如果消息格式不支持流式迭代，将返回普通迭代器
     * 2. 调用者必须确保迭代器被正确关闭
     * 3. 对于小批次，分配大缓冲区（如LZ4的64KB）可能成为主要开
     * 
     **/
    CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier);

    /**
     * Check whether this is a control batch (i.e. whether the control bit is set in the batch attributes).
     * For magic versions prior to 2, this is always false.
     *
     * @return Whether this is a batch containing control records
     */
    boolean isControlBatch();

    /**
     * iterate all records to find the offset of max timestamp.
     * noted:
     * 1) that the earliest offset will return if there are multi records having same (max) timestamp
     * 2) it always returns None if the {@link RecordBatch#magic()} is equal to {@link RecordBatch#MAGIC_VALUE_V0}
     * @return offset of max timestamp
     */
    default Optional<Long> offsetOfMaxTimestamp() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0) return Optional.empty();
        long maxTimestamp = maxTimestamp();
        try (CloseableIterator<Record> iter = streamingIterator(BufferSupplier.create())) {
            while (iter.hasNext()) {
                Record record = iter.next();
                if (maxTimestamp == record.timestamp()) return Optional.of(record.offset());
            }
        }
        return Optional.empty();
    }
}
