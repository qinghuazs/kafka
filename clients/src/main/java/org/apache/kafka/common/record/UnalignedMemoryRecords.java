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

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * 表示一个不需要按偏移量对齐的内存记录集
 * 这个类主要用于处理Kafka中的内存记录，特别是在需要处理不按消息边界对齐的数据时，
 * 例如在Raft快照传输等场景下，允许从内存缓冲区的任意位置读取指定大小的数据块。
 */
public class UnalignedMemoryRecords implements UnalignedRecords {
    /**
     * 空记录集的单例实例，用于表示一个空的内存记录集
     * 通过分配大小为0的ByteBuffer来初始化
     */
    private static final UnalignedMemoryRecords EMPTY = new UnalignedMemoryRecords(ByteBuffer.allocate(0));

    /**
     * 存储实际数据的ByteBuffer，用于保存记录集的内容
     */
    private final ByteBuffer buffer;

    /**
     * 创建一个非对齐的内存记录集实例
     * 
     * @param buffer 用于存储数据的ByteBuffer，不能为null
     */
    public UnalignedMemoryRecords(ByteBuffer buffer) {
        // 确保传入的buffer不为null，否则抛出NullPointerException
        this.buffer = Objects.requireNonNull(buffer);
    }

    /**
     * 获取当前记录集的ByteBuffer的副本
     * 通过duplicate()创建副本以确保线程安全，避免多线程访问时的并发问题
     * 
     * @return ByteBuffer的副本，可以独立操作而不影响原始buffer
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    /**
     * 获取记录集中剩余的字节数
     * 
     * @return 剩余可读取的字节数
     */
    @Override
    public int sizeInBytes() {
        return buffer.remaining();
    }

    /**
     * 将数据写入目标通道
     * 支持从指定位置开始写入指定长度的数据
     * 
     * @param channel 目标传输通道
     * @param position 要写入的起始位置
     * @param length 要写入的字节数
     * @return 实际写入的字节数
     * @throws IOException 如果发生I/O错误
     * @throws IllegalArgumentException 如果position和length的和超过buffer的限制
     */
    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        // 检查写入范围是否超出buffer的限制
        if (((long) position) + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());
        // 使用工具类方法尝试写入数据
        return Utils.tryWriteTo(channel, position, length, buffer);
    }

    /**
     * 获取一个空的记录集实例
     * 用于表示不包含任何数据的记录集，避免创建多个空实例
     * 
     * @return 空的记录集实例
     */
    public static UnalignedMemoryRecords empty() {
        return EMPTY;
    }
}
