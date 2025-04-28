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

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * 表示一个不需要按偏移量对齐的文件记录集
 * 这个类主要用于处理Kafka中的文件记录，特别是在Raft快照传输等场景下，
 * 允许从文件的任意位置读取指定大小的数据块，而不需要考虑消息边界的对齐。
 */
public class UnalignedFileRecords implements UnalignedRecords {

    /**
     * 用于文件操作的通道，提供了对文件的底层访问能力
     */
    private final FileChannel channel;

    /**
     * 在文件中的起始位置，表示从哪里开始读取数据
     */
    private final long position;

    /**
     * 要读取的数据块大小（字节数）
     */
    private final int size;

    /**
     * 创建一个非对齐的文件记录集实例
     * 
     * @param channel 用于读取数据的文件通道
     * @param position 文件中的起始位置
     * @param size 要读取的数据块大小
     */
    public UnalignedFileRecords(FileChannel channel, long position, int size) {
        this.channel = channel;
        this.position = position;
        this.size = size;
    }

    /**
     * 获取记录集的总字节大小
     * 
     * @return 记录集的字节大小
     */
    @Override
    public int sizeInBytes() {
        return size;
    }

    /**
     * 将数据写入目标通道
     * 这个方法支持分片传输，可以从指定位置开始写入指定大小的数据
     * 
     * @param destChannel 目标传输通道
     * @param previouslyWritten 已经写入的字节数
     * @param remaining 剩余要写入的字节数
     * @return 实际写入的字节数
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public int writeTo(TransferableChannel destChannel, int previouslyWritten, int remaining) throws IOException {
        // 计算实际的文件位置，考虑已写入的字节数
        long position = this.position + previouslyWritten;
        // 计算本次要传输的字节数，取剩余要写入的字节数和未传输的字节数的较小值
        int count = Math.min(remaining, sizeInBytes() - previouslyWritten);
        // 由于count是int类型，所以这里的类型转换是安全的
        // 使用零拷贝方式将数据从文件通道传输到目标通道
        return (int) destChannel.transferFrom(channel, position, count);
    }
}
