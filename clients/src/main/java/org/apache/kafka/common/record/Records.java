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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;

import java.util.Iterator;
import java.util.Optional;


/**
 * 用于访问日志中记录的接口。日志本身表示为一系列记录批次(参见 {@link RecordBatch})。
 * 
 * 对于魔数版本1及以下版本：
 * - 每个批次包含：8字节偏移量、4字节记录大小和一个"浅层"的{@link Record record}
 * - 未压缩情况：每个批次只包含一个浅层记录
 * - 压缩情况：批次包含"深层"记录，这些记录被打包到浅层记录的值字段中
 * - 访问方式：
 *   - 使用{@link Records#batches()}遍历浅层批次
 *   - 使用{@link Records#records()}访问深层记录
 * - 深层迭代器处理机制：
 *   - 未压缩：直接返回浅层记录
 *   - 已压缩：解压缩浅层批次并返回深层记录
 * 
 * 对于魔数版本2：
 * - 每个批次包含1个或多个日志记录，与压缩无关
 * - 访问方式：
 *   - 使用{@link Records#batches()}直接遍历批次
 *   - 可以直接从单个批次或通过{@link Records#records()}遍历记录
 * - 注意：遍历记录通常需要解压缩，因此应谨慎使用
 * 
 * 实现类：
 * - {@link MemoryRecords}：内存中的表示形式
 * - {@link FileRecords}：磁盘上的表示形式
 */
public interface Records extends TransferableRecords {
    // 记录格式中各字段的偏移量和长度定义
    /** 偏移量字段的起始位置 */
    int OFFSET_OFFSET = 0;
    /** 偏移量字段的长度(8字节) */
    int OFFSET_LENGTH = 8;
    /** 大小字段的起始位置(等于偏移量字段的结束位置) */
    int SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    /** 大小字段的长度(4字节) */
    int SIZE_LENGTH = 4;
    /** 日志记录的基本开销(偏移量和大小字段的总长度) */
    int LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;

    // 魔数字段的位置和长度定义
    // 魔数在所有当前消息格式中的偏移量相同，但大小字段和魔数之间的4字节取决于版本
    /** 魔数字段的起始位置(基本开销后4字节) */
    int MAGIC_OFFSET = LOG_OVERHEAD + 4;
    /** 魔数字段的长度(1字节) */
    int MAGIC_LENGTH = 1;
    /** 直到魔数字段为止的头部总大小 */
    int HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH;

    /**
     * 获取记录批次。
     * 方法签名允许子类返回更具体的批次类型，这使得以下优化成为可能：
     * 1. 原地偏移量分配(参见{@link DefaultRecordBatch})
     * 2. 记录数据的部分读取(参见{@link FileLogInputStream.FileChannelRecordBatch#magic()})
     *
     * @return 日志记录批次的迭代器
     */
    Iterable<? extends RecordBatch> batches();

    /**
     * 获取记录批次的迭代器。
     * 与{@link #batches()}类似，但返回{@link AbstractIterator}而不是{@link Iterator}，
     * 这样客户端可以使用{@link AbstractIterator#peek() peek}等方法。
     *
     * @return 日志记录批次的迭代器
     */
    AbstractIterator<? extends RecordBatch> batchIterator();

    /**
     * 返回最后一个记录批次。
     * 如果记录集合非空，返回最后一个批次；否则返回空Optional。
     * 
     * 注意：此操作需要遍历所有记录批次，因此开销较大。
     *
     * @return 包含最后一个记录批次的Optional，如果没有批次则为空
     */
    Optional<RecordBatch> lastBatch();

    /**
     * 检查此缓冲区中的所有批次是否具有特定的魔数值。
     * 魔数用于标识记录格式的版本。
     *
     * @param magic 要检查的魔数值
     * @return 如果所有记录批次都具有匹配的魔数值则返回true，否则返回false
     */
    boolean hasMatchingMagic(byte magic);

    /**
     * 将此缓冲区中的所有批次转换为指定的格式。
     * 由于需要转换所有深层记录，此操作需要深度迭代。
     *
     * @param toMagic 要转换到的目标魔数值
     * @param firstOffset 返回记录的起始偏移量。这只影响某些情况，
     *                   详见{@link RecordsUtil#downConvert(Iterable, byte, long, Time)}
     * @param time 用于报告统计信息的时间实例
     * @return ConvertedRecords实例，其records字段可能包含相同或不同的实例
     */
    ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time);

    /**
     * 获取此日志中记录的迭代器。
     * 注意：此操作通常需要解压缩，因此应谨慎使用。
     *
     * @return 记录迭代器
     */
    Iterable<Record> records();
}
