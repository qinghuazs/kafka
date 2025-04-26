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
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个由文件支持的{@link Records}实现。可以对该实例应用可选的起始和结束位置,
 * 以实现对日志记录范围的切片访问。
 * 
 * 应用场景:
 * 1. 用于Kafka日志段文件的读写操作
 * 2. 支持对大文件进行切片访问,提高效率
 * 3. 提供记录批次的迭代访问功能
 * 4. 实现日志文件的追加和截断操作
 */
public class FileRecords extends AbstractRecords implements Closeable {
    // 标识当前实例是否为切片视图
    private final boolean isSlice;
    // 记录读取的起始位置
    private final int start;
    // 记录读取的结束位置
    private final int end;

    // 提供对文件中记录批次的迭代访问
    private final Iterable<FileLogInputStream.FileChannelRecordBatch> batches;

    // 可变状态
    // 当前记录集合的大小(字节数)
    private final AtomicInteger size;
    // 底层文件通道,用于文件读写操作
    private final FileChannel channel;
    // 对应的物理文件,使用volatile保证可见性
    private volatile File file;

    /**
     * 创建FileRecords实例。推荐使用{@code FileRecords.open}方法代替此构造函数。
     * 此构造函数仅用于测试目的。
     *
     * @param file 底层物理文件
     * @param channel 文件通道
     * @param start 起始位置
     * @param end 结束位置
     * @param isSlice 是否为切片视图
     */
    FileRecords(File file,
                FileChannel channel,
                int start,
                int end,
                boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        this.size = new AtomicInteger();

        if (isSlice) {
            // 如果是切片视图,直接计算大小,不检查文件大小
            size.set(end - start);
        } else {
            // 检查文件大小是否超过限制
            if (channel.size() > Integer.MAX_VALUE)
                throw new KafkaException("The size of segment " + file + " (" + channel.size() +
                        ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);

            // 计算实际可用大小
            int limit = Math.min((int) channel.size(), end);
            size.set(limit - start);

            // 如果不是切片,将文件指针移动到文件末尾
            channel.position(limit);
        }

        // 初始化记录批次迭代器
        batches = batchesFrom(start);
    }

    /**
     * 获取记录集合的总字节数
     * 
     * @return 记录集合的大小(字节)
     */
    @Override
    public int sizeInBytes() {
        return size.get();
    }

    /**
     * 获取底层物理文件
     * 
     * @return 对应的File对象
     */
    public File file() {
        return file;
    }

    /**
     * 获取底层文件通道
     * 
     * @return 文件通道对象
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * 将日志批次读入给定的缓冲区,直到缓冲区没有剩余空间或到达文件末尾。
     * 
     * 实现细节:
     * 1. 从指定位置开始读取数据
     * 2. 考虑切片的起始偏移量
     * 3. 读取完成后翻转缓冲区准备读取
     *
     * @param buffer 用于写入批次数据的缓冲区
     * @param position 开始读取的位置
     * @throws IOException 如果发生I/O错误
     */
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        Utils.readFully(channel, buffer, position + this.start);
        buffer.flip();
    }

    /**
     * 返回此实例的记录切片,提供从给定位置开始的视图,并限制大小。
     * 
     * 实现细节:
     * 1. 计算实际可用的字节数
     * 2. 考虑已有切片的起始位置
     * 3. 创建新的切片视图
     * 
     * 特殊处理:
     * 1. 如果size超过文件末尾,则以文件大小为准
     * 2. 如果当前实例已经是切片,则position相对于当前切片计算
     *
     * @param position 开始读取的起始位置
     * @param size 要包含的字节数
     * @return 基于给定位置和大小限制的切片包装器
     */
    public FileRecords slice(int position, int size) throws IOException {
        int availableBytes = availableBytes(position, size);
        int startPosition = this.start + position;
        return new FileRecords(file, channel, startPosition, startPosition + availableBytes, true);
    }

    /**
     * 返回此实例的非对齐记录切片。与{@link FileRecords#slice(int, int)}的区别在于,
     * 起始位置不需要在偏移量边界上对齐。
     * 
     * 此方法专门用于不需要偏移量对齐的场景,例如在复制Raft快照时。
     * 
     * 实现细节:
     * 1. 计算可用字节数
     * 2. 创建非对齐的切片视图
     * 3. 不检查偏移量边界对齐
     *
     * @param position 开始读取的起始位置
     * @param size 要包含的字节数
     * @return 基于给定位置和大小限制的非对齐切片视图
     */
    public UnalignedFileRecords sliceUnaligned(int position, int size) {
        // 计算实际可用的字节数
        int availableBytes = availableBytes(position, size);
        // 创建非对齐的切片视图,直接使用文件通道
        return new UnalignedFileRecords(channel, this.start + position, availableBytes);
    }

    /**
     * 计算给定位置和大小下实际可用的字节数
     * 
     * 实现细节:
     * 1. 缓存当前大小以避免并发写入影响
     * 2. 验证位置和大小参数的合法性
     * 3. 处理整数溢出和文件边界情况
     * 
     * 特殊处理:
     * 1. 如果结束位置超出文件大小,则截断到文件末尾
     * 2. 如果发生整数溢出,同样截断到文件末尾
     * 
     * @param position 起始位置
     * @param size 请求的字节数
     * @return 实际可用的字节数
     * @throws IllegalArgumentException 如果位置或大小参数无效
     */
    private int availableBytes(int position, int size) {
        // 缓存当前大小,避免并发写入的影响
        int currentSizeInBytes = sizeInBytes();

        // 验证位置参数的合法性
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position + " in read from " + this);
        // 位置必须相对于文件起始位置,与文件大小比较以验证是否在文件范围内
        if (position > currentSizeInBytes)
            throw new IllegalArgumentException("Slice from position " + position + " exceeds end position of " + this);
        // 验证大小参数的合法性
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);

        // 计算结束位置
        int end = this.start + position + size;
        // 处理整数溢出或超出文件末尾的情况
        if (end < 0 || end > start + currentSizeInBytes)
            end = this.start + currentSizeInBytes;
        // 返回实际可用的字节数
        return end - (this.start + position);
    }

    /**
     * 将一组记录追加到文件末尾。此方法非线程安全,必须通过锁进行保护。
     * 
     * 实现细节:
     * 1. 检查追加大小是否会导致整数溢出
     * 2. 将记录写入文件通道
     * 3. 更新记录集合的大小
     * 
     * 线程安全:
     * 1. 使用AtomicInteger保证size的原子性更新
     * 2. 外部调用需要通过锁保护
     *
     * @param records 要追加的记录集合
     * @return 写入底层文件的字节数
     * @throws IOException 如果写入过程中发生I/O错误
     * @throws IllegalArgumentException 如果追加会导致大小溢出
     */
    public int append(MemoryRecords records) throws IOException {
        // 检查追加是否会导致整数溢出
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());

        // 将记录完整写入文件通道
        int written = records.writeFullyTo(channel);
        // 原子更新记录集合的大小
        size.getAndAdd(written);
        return written;
    }

    /**
     * 将所有已写入的数据提交到物理磁盘
     * 
     * 实现细节:
     * 1. 调用force方法确保数据和元数据都写入磁盘
     * 2. 参数true表示同时刷新文件元数据
     * 
     * @throws IOException 如果刷新过程中发生I/O错误
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * 关闭记录集合
     * 
     * 实现细节:
     * 1. 先刷新所有数据到磁盘
     * 2. 对文件进行修剪
     * 3. 关闭文件通道
     * 
     * @throws IOException 如果关闭过程中发生I/O错误
     */
    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }

    /**
     * 关闭FileChannel使用的文件句柄,但不写入磁盘。
     * 这用于磁盘可能已经失败的情况。
     * 
     * 实现细节:
     * 1. 直接关闭文件通道
     * 2. 不执行flush操作
     * 
     * @throws IOException 如果关闭过程中发生I/O错误
     */
    public void closeHandlers() throws IOException {
        channel.close();
    }

    /**
     * 从文件系统中删除此消息集合
     * 
     * 实现细节:
     * 1. 安全关闭文件通道
     * 2. 删除物理文件
     * 
     * @throws IOException 如果删除过程中发生I/O错误
     * @return 如果文件被成功删除返回true,如果文件不存在返回false
     */
    public boolean deleteIfExists() throws IOException {
        Utils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    /**
     * 在关闭或滚动到下一个文件时对文件进行修剪
     * 
     * 实现细节:
     * 1. 将文件截断到当前使用的大小
     * 2. 删除任何未使用的空间
     * 
     * @throws IOException 如果修剪过程中发生I/O错误
     */
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * 更新父目录路径(需谨慎使用,因为不会重新打开文件通道)
     * 
     * 实现细节:
     * 1. 保持文件名不变,仅更新父目录路径
     * 2. 不重新打开文件通道,避免额外开销
     * 
     * 应用场景:
     * 1. 日志文件目录迁移
     * 2. 日志文件路径重构
     * 
     * 注意事项:
     * 1. 由于不重新打开文件通道,原有的文件句柄仍指向旧路径
     * 2. 调用此方法后需要谨慎处理文件操作
     * 
     * @param parentDir 新的父目录
     */
    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    /**
     * 重命名当前消息集合对应的文件
     * 
     * 实现细节:
     * 1. 使用原子移动操作确保重命名的一致性
     * 2. 即使重命名失败也会更新文件引用
     * 3. 支持跨文件系统的移动操作
     * 
     * 应用场景:
     * 1. 日志段文件的滚动和清理
     * 2. 临时文件转换为正式文件
     * 3. 文件备份和恢复操作
     * 
     * 线程安全:
     * 1. 原子移动操作保证文件重命名的原子性
     * 2. finally块确保文件引用始终更新
     * 
     * @param f 新的文件对象
     * @throws IOException 如果重命名操作失败
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    /**
     * 将文件消息集合截断到指定大小。注意此API不会检查给定大小是否落在有效的消息边界上。
     * 
     * 实现细节:
     * 1. 首先获取当前文件大小作为基准
     * 2. 验证目标大小的合法性(必须在0到当前大小之间)
     * 3. 仅当目标大小小于当前文件大小时才执行截断
     * 4. 更新内部维护的大小计数器
     * 
     * 应用场景:
     * 1. 日志文件损坏恢复
     * 2. 日志文件清理和压缩
     * 3. 回滚到之前的检查点
     * 
     * 特殊处理:
     * 1. 由于JDK的特性,仅在目标大小小于当前大小时执行截断
     * 2. 这样可以避免不必要的文件修改时间(mtime)更新
     * 
     * 线程安全:
     * 1. 调用此方法时不应有其他线程写入日志
     * 2. 使用原子整数确保size更新的原子性
     *
     * @param targetSize 目标截断大小,必须在0到当前大小之间
     * @return 被截断的字节数
     * @throws IOException 如果截断操作失败
     * @throws KafkaException 如果目标大小无效
     */
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("Attempt to truncate log segment " + file + " to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }

    /**
     * 将记录批次转换为较低版本的消息格式
     * 
     * 实现细节:
     * 1. 调用工具类进行格式转换
     * 2. 检查转换结果是否有效
     * 3. 处理特殊情况(如消息过大)
     * 
     * 应用场景:
     * 1. 支持旧版本客户端访问
     * 2. 数据迁移和兼容性处理
     * 3. 跨版本消息格式转换
     * 
     * 特殊处理:
     * 1. 当消息过大无法转换时返回原始字节
     * 2. 在KIP-74之前,过大的消息可能导致客户端错误
     * 3. KIP-74后broker总是返回至少一个完整批次
     *
     * @param toMagic 目标消息格式版本
     * @param firstOffset 第一条消息的偏移量
     * @param time 时间工具类
     * @return 转换后的记录集合
     */
    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        ConvertedRecords<MemoryRecords> convertedRecords = RecordsUtil.downConvert(batches, toMagic, firstOffset, time);
        if (convertedRecords.recordConversionStats().numRecordsConverted() == 0) {
            // 消息过大,缓冲区无法容纳完整的记录批次
            // 返回原始字节,由旧客户端处理错误
            return new ConvertedRecords<>(this, RecordValidationStats.EMPTY);
        } else {
            return convertedRecords;
        }
    }

    /**
     * 将记录数据写入目标通道
     * 
     * 实现细节:
     * 1. 计算当前可用的文件大小
     * 2. 检查文件是否被截断
     * 3. 计算实际传输的字节数
     * 4. 使用零拷贝方式传输数据
     * 
     * 应用场景:
     * 1. 网络传输消息数据
     * 2. 文件复制和备份
     * 3. 日志段合并
     * 
     * 特殊处理:
     * 1. 处理文件大小变化
     * 2. 确保不超出文件边界
     * 3. 优化传输性能
     *
     * @param destChannel 目标传输通道
     * @param offset 起始偏移量
     * @param length 要传输的长度
     * @return 实际传输的字节数
     * @throws IOException 如果传输过程中发生I/O错误
     * @throws KafkaException 如果文件在传输过程中被截断
     */
    @Override
    public int writeTo(TransferableChannel destChannel, int offset, int length) throws IOException {
        // 计算当前可用的文件大小
        long newSize = Math.min(channel.size(), end) - start;
        int oldSize = sizeInBytes();
        // 检查文件是否被截断
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));

        // 计算传输起始位置和长度
        long position = start + offset;
        int count = Math.min(length, oldSize - offset);
        // 使用零拷贝方式传输数据
        return (int) destChannel.transferFrom(channel, position, count);
    }

    /**
     * 从指定位置开始向前搜索,查找最后一个偏移量大于等于目标偏移量的消息批次位置
     * 
     * 实现细节:
     * 1. 从起始位置迭代消息批次
     * 2. 比较每个批次的最后偏移量
     * 3. 找到匹配批次后返回其信息
     * 
     * 应用场景:
     * 1. 日志段查找和定位
     * 2. 消息消费位置恢复
     * 3. 日志清理和压缩
     * 
     * 优化考虑:
     * 1. 使用迭代器避免一次性加载所有批次
     * 2. 从指定位置开始减少搜索范围
     * 3. 找到匹配后立即返回提高效率
     *
     * @param targetOffset 要搜索的目标偏移量
     * @param startingPosition 开始搜索的文件位置
     * @return 匹配批次的基准偏移量、物理位置和大小(包含日志开销),如果未找到返回null
     */
    public LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        // 从起始位置迭代消息批次
        for (FileChannelRecordBatch batch : batchesFrom(startingPosition)) {
            // 获取当前批次的最后偏移量
            long offset = batch.lastOffset();
            // 如果找到匹配的批次,返回其信息
            if (offset >= targetOffset)
                return new LogOffsetPosition(batch.baseOffset(), batch.position(), batch.sizeInBytes());
        }
        // 未找到匹配的批次
        return null;
    }

    /**
     * 向前搜索第一条满足以下要求的消息:
     * - 消息的时间戳大于或等于目标时间戳
     * - 消息在日志文件中的位置大于或等于起始位置
     * - 消息的偏移量大于或等于起始偏移量
     * 
     * 实现细节:
     * 1. 从指定位置开始遍历记录批次
     * 2. 首先比较批次的最大时间戳进行快速过滤
     * 3. 对于时间戳匹配的批次,遍历其中的每条记录
     * 4. 同时检查时间戳和偏移量条件
     * 
     * 应用场景:
     * 1. 基于时间戳的消息查找
     * 2. 消费者按时间点消费
     * 3. 日志清理时的时间戳处理
     *
     * @param targetTimestamp 要搜索的目标时间戳
     * @param startingPosition 开始搜索的文件位置
     * @param startingOffset 开始搜索的偏移量
     * @return 找到的消息的时间戳和偏移量,如果未找到则返回null
     */
    public TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset) {
        // 从指定位置开始遍历记录批次
        for (RecordBatch batch : batchesFrom(startingPosition)) {
            // 首先检查批次的最大时间戳,进行快速过滤
            if (batch.maxTimestamp() >= targetTimestamp) {
                // 找到可能包含目标消息的批次,遍历其中的记录
                for (Record record : batch) {
                    long timestamp = record.timestamp();
                    // 检查记录的时间戳和偏移量是否满足条件
                    if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                        return new TimestampAndOffset(timestamp, record.offset(),
                                maybeLeaderEpoch(batch.partitionLeaderEpoch()));
                }
            }
        }
        return null;
    }

    /**
     * 返回给定位置之后的消息中最大的时间戳
     * 
     * 实现细节:
     * 1. 从指定位置开始遍历所有记录批次
     * 2. 比较每个批次的最大时间戳
     * 3. 记录最大时间戳及其对应的偏移量和leader epoch
     * 
     * 应用场景:
     * 1. 日志压缩时确定时间范围
     * 2. 消费者查找最新消息
     * 3. 监控和统计时间戳分布
     *
     * @param startingPosition 开始搜索的位置
     * @return 最大时间戳及其对应的偏移量信息
     */
    public TimestampAndOffset largestTimestampAfter(int startingPosition) {
        // 初始化最大时间戳为无效值
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        // 初始化最大时间戳对应的偏移量为-1
        long shallowOffsetOfMaxTimestamp = -1L;
        // 初始化最大时间戳对应的leader epoch为无效值
        int leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH;

        // 遍历指定位置之后的所有记录批次
        for (RecordBatch batch : batchesFrom(startingPosition)) {
            long timestamp = batch.maxTimestamp();
            // 更新最大时间戳及其相关信息
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                shallowOffsetOfMaxTimestamp = batch.lastOffset();
                leaderEpochOfMaxTimestamp = batch.partitionLeaderEpoch();
            }
        }
        return new TimestampAndOffset(maxTimestamp, shallowOffsetOfMaxTimestamp,
                maybeLeaderEpoch(leaderEpochOfMaxTimestamp));
    }

    /**
     * 将leader epoch值转换为Optional对象
     * 
     * 实现细节:
     * 1. 检查leader epoch是否为无效值
     * 2. 如果是无效值返回空Optional
     * 3. 否则将值包装为Optional
     * 
     * 应用场景:
     * 1. 处理可能不存在的leader epoch
     * 2. 与旧版本格式兼容
     * 3. 在API返回中表示可选值
     *
     * @param leaderEpoch leader epoch值
     * @return 包装后的Optional对象
     */
    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        // 如果leader epoch为无效值则返回空Optional,否则包装为Optional
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * 获取文件中记录批次的迭代器
     * 
     * 实现细节:
     * 1. 返回在构造时创建的批次迭代器
     * 2. 迭代器由打开的文件通道支持
     * 3. 当实例关闭时批次将不可读
     * 
     * 应用场景:
     * 1. 顺序遍历日志文件
     * 2. 批量处理消息记录
     * 3. 实现延迟加载机制
     * 
     * 注意事项:
     * 1. 迭代器依赖于文件通道
     * 2. 文件关闭后不可用
     * 3. 适合大文件处理
     *
     * @return 记录批次的迭代器
     */
    @Override
    public Iterable<FileChannelRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "FileRecords(size=" + sizeInBytes() +
                ", file=" + file +
                ", start=" + start +
                ", end=" + end +
                ")";
    }

    /**
     * 获取从指定位置开始的记录批次迭代器
     * 
     * 实现细节:
     * 1. 创建一个从指定位置开始的迭代器
     * 2. 使用lambda表达式返回迭代器实例
     * 3. 调用内部batchIterator方法实现
     * 
     * 应用场景:
     * 1. 断点续传场景
     * 2. 指定位置的消息查找
     * 3. 日志段的部分读取
     * 
     * 注意事项:
     * 1. 起始位置必须是批次的有效起始点
     * 2. 不正确的起始位置可能导致解析错误
     * 3. 主要用于内部或受控场景
     *
     * @param start 开始迭代的位置,必须是批次的有效起始位置
     * @return 从指定位置开始的批次迭代器
     */
    public Iterable<FileChannelRecordBatch> batchesFrom(final int start) {
        return () -> batchIterator(start);
    }

    /**
     * 获取默认的记录批次迭代器,从当前实例的起始位置开始
     * 
     * @return 记录批次迭代器
     */
    @Override
    public AbstractIterator<FileChannelRecordBatch> batchIterator() {
        return batchIterator(start);
    }

    /**
     * 创建从指定位置开始的记录批次迭代器
     * 
     * 实现细节:
     * 1. 确定迭代的结束位置
     * 2. 创建文件日志输入流
     * 3. 构造批次迭代器
     * 
     * 应用场景:
     * 1. 支持分片读取
     * 2. 实现高效的批次遍历
     * 3. 处理大型日志文件
     *
     * @param start 开始迭代的位置
     * @return 记录批次迭代器
     */
    private AbstractIterator<FileChannelRecordBatch> batchIterator(int start) {
        // 确定迭代的结束位置
        final int end;
        if (isSlice)
            end = this.end;  // 如果是切片,使用切片的结束位置
        else
            end = this.sizeInBytes();  // 否则使用整个文件的大小
        
        // 创建文件日志输入流
        FileLogInputStream inputStream = new FileLogInputStream(this, start, end);
        // 返回新的记录批次迭代器
        return new RecordBatchIterator<>(inputStream);
    }

    /**
     * 打开或创建一个FileRecords实例
     * 
     * 实现细节:
     * 1. 打开或创建文件通道
     * 2. 根据预分配选项设置结束位置
     * 3. 创建新的FileRecords实例
     * 
     * 应用场景:
     * 1. 创建新的日志段文件
     * 2. 打开已存在的日志文件
     * 3. 支持文件预分配优化
     * 
     * 设计考虑:
     * 1. 支持可变/只读模式
     * 2. 处理文件预分配
     * 3. 优化文件系统性能
     *
     * @param file 要打开的文件
     * @param mutable 是否可修改
     * @param fileAlreadyExists 文件是否已存在
     * @param initFileSize 初始文件大小
     * @param preallocate 是否预分配空间
     * @return 新的FileRecords实例
     * @throws IOException 如果发生I/O错误
     */
    public static FileRecords open(File file,
                                   boolean mutable,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        // 打开或创建文件通道
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        // 如果是新文件且需要预分配,结束位置设为0,否则设为最大值
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        // 创建并返回新的FileRecords实例
        return new FileRecords(file, channel, 0, end, false);
    }

    /**
     * 打开一个文件记录实例,支持文件预分配功能
     * 
     * 实现细节:
     * 1. 调用完整的open方法,设置为可变模式
     * 2. 支持文件预分配以提升性能
     * 
     * 应用场景:
     * 1. 创建新的日志段文件
     * 2. 需要预分配空间以提升写入性能
     * 
     * @param file 要打开的文件
     * @param fileAlreadyExists 文件是否已存在
     * @param initFileSize 预分配的文件大小
     * @param preallocate 是否启用预分配
     * @return FileRecords实例
     * @throws IOException 如果文件操作失败
     */
    public static FileRecords open(File file,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    /**
     * 打开一个文件记录实例,可指定是否为可变模式
     * 
     * 实现细节:
     * 1. 调用完整的open方法
     * 2. 默认不预分配空间
     * 
     * 应用场景:
     * 1. 打开已存在的日志文件
     * 2. 需要控制文件的可变性
     * 
     * @param file 要打开的文件
     * @param mutable 是否为可变模式
     * @return FileRecords实例
     * @throws IOException 如果文件操作失败
     */
    public static FileRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    /**
     * 以可变模式打开一个文件记录实例
     * 
     * 实现细节:
     * 1. 调用双参数的open方法
     * 2. 默认设置为可变模式
     * 
     * 应用场景:
     * 1. 快速打开日志文件
     * 2. 不需要特殊配置时使用
     * 
     * @param file 要打开的文件
     * @return FileRecords实例
     * @throws IOException 如果文件操作失败
     */
    public static FileRecords open(File file) throws IOException {
        return open(file, true);
    }

    /**
     * 为给定的文件打开一个文件通道
     * 
     * 实现细节:
     * 1. 根据可变性标志选择打开模式
     * 2. 支持文件预分配功能
     * 3. 使用RandomAccessFile实现预分配
     * 
     * 性能优化:
     * 对于Windows NTFS和某些旧的Linux文件系统,设置preallocate为true并指定合适的initFileSize
     * (例如512 * 1025 * 1024)可以显著提升Kafka的生产性能。
     * 
     * 应用场景:
     * 1. 创建新的日志段文件
     * 2. 打开已存在的日志文件
     * 3. 需要预分配空间提升性能
     * 
     * @param file 文件路径
     * @param mutable 是否可变
     * @param fileAlreadyExists 文件是否已存在
     * @param initFileSize 预分配的文件大小
     * @param preallocate 是否预分配空间
     * @return 打开的文件通道
     * @throws IOException 如果文件操作失败
     */
    private static FileChannel openChannel(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists || !preallocate) {
                // 文件已存在或不需要预分配时,使用标准打开选项
                return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                // 需要预分配时,使用RandomAccessFile并设置文件长度
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(initFileSize);
                return randomAccessFile.getChannel();
            }
        } else {
            // 只读模式下简单打开文件
            return FileChannel.open(file.toPath());
        }
    }

    /**
     * 记录在日志文件中的位置信息
     * 
     * 实现细节:
     * 1. 使用不可变字段存储位置信息
     * 2. 提供完整的equals和hashCode实现
     * 3. 支持字符串表示
     * 
     * 应用场景:
     * 1. 记录消息批次在日志文件中的精确位置
     * 2. 用于日志段的索引和查找
     * 3. 支持日志段的切片和复制操作
     */
    public static class LogOffsetPosition {
        // 消息的逻辑偏移量
        public final long offset;
        // 消息在文件中的物理位置
        public final int position;
        // 消息批次的大小(字节)
        public final int size;

        /**
         * 创建一个新的日志偏移量位置实例
         * 
         * @param offset 消息的逻辑偏移量
         * @param position 消息在文件中的物理位置
         * @param size 消息批次的大小
         */
        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset &&
                    position == that.position &&
                    size == that.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(offset);
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition(" +
                    "offset=" + offset +
                    ", position=" + position +
                    ", size=" + size +
                    ')';
        }
    }

    /**
     * 记录消息的时间戳和偏移量信息
     * 
     * 实现细节:
     * 1. 使用不可变字段存储时间戳和偏移量
     * 2. 支持可选的leader epoch信息
     * 3. 提供完整的equals和hashCode实现
     * 
     * 应用场景:
     * 1. 用于基于时间戳的消息查找
     * 2. 支持日志压缩和清理
     * 3. 用于消息的复制和恢复
     */
    public static class TimestampAndOffset {
        // 消息的时间戳
        public final long timestamp;
        // 消息的逻辑偏移量
        public final long offset;
        // 可选的leader epoch信息
        public final Optional<Integer> leaderEpoch;

        /**
         * 创建一个新的时间戳和偏移量实例
         * 
         * @param timestamp 消息的时间戳
         * @param offset 消息的逻辑偏移量
         * @param leaderEpoch 可选的leader epoch信息
         */
        public TimestampAndOffset(long timestamp, long offset, Optional<Integer> leaderEpoch) {
            this.timestamp = timestamp;
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampAndOffset that = (TimestampAndOffset) o;
            return timestamp == that.timestamp &&
                    offset == that.offset &&
                    Objects.equals(leaderEpoch, that.leaderEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, offset, leaderEpoch);
        }

        @Override
        public String toString() {
            return "TimestampAndOffset(" +
                    "timestamp=" + timestamp +
                    ", offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ')';
        }
    }
}
