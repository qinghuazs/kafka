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
package org.apache.kafka.common.network;

import org.apache.kafka.common.memory.MemoryPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * 一个带有大小前缀的网络数据接收器，由4字节的网络字节序大小N和后续N字节的内容组成。
 * 该类主要用于Kafka的网络通信层，处理来自网络的数据包接收。
 * 实现了零拷贝和内存池管理，支持SASL认证等场景下的数据传输。
 */
public class NetworkReceive implements Receive {

    // 表示未知数据源的常量
    public static final String UNKNOWN_SOURCE = "";
    // 表示不限制接收数据大小的常量
    public static final int UNLIMITED = -1;
    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    // 用于表示空缓冲区的常量
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // 数据源标识符
    private final String source;
    // 用于存储数据包大小信息的缓冲区（固定4字节）
    private final ByteBuffer size;
    // 允许接收的最大数据大小
    private final int maxSize;
    // 用于管理内存分配的内存池
    private final MemoryPool memoryPool;
    // 请求分配的缓冲区大小，-1表示尚未知道需要的大小
    private int requestedBufferSize = -1;
    // 用于存储实际接收数据的缓冲区
    private ByteBuffer buffer;


    /**
     * 使用指定的数据源和预分配的缓冲区构造接收器
     * @param source 数据源标识符
     * @param buffer 预分配的数据缓冲区
     */
    public NetworkReceive(String source, ByteBuffer buffer) {
        this(UNLIMITED, source);
        this.buffer = buffer;
    }

    /**
     * 使用指定的数据源构造接收器，不限制数据大小
     * @param source 数据源标识符
     */
    public NetworkReceive(String source) {
        this(UNLIMITED, source);
    }

    /**
     * 使用指定的最大数据大小和数据源构造接收器
     * @param maxSize 最大允许的数据大小
     * @param source 数据源标识符
     */
    public NetworkReceive(int maxSize, String source) {
        this(maxSize, source, MemoryPool.NONE);
    }

    /**
     * 使用完整参数构造接收器
     * @param maxSize 最大允许的数据大小
     * @param source 数据源标识符
     * @param memoryPool 用于内存管理的内存池
     */
    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = ByteBuffer.allocate(4); // 分配4字节用于存储数据大小
        this.buffer = null; // 数据缓冲区初始为空
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    /**
     * 使用未知数据源构造接收器的默认构造函数
     */
    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    /**
     * 获取数据源标识符
     * @return 数据源标识符
     */
    @Override
    public String source() {
        return source;
    }

    /**
     * 检查数据接收是否完成
     * @return 当大小信息已完全读取且数据缓冲区已分配并完全填充时返回true
     */
    @Override
    public boolean complete() {
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    /**
     * 从通道读取数据
     * 实现了两阶段读取：首先读取4字节的大小信息，然后根据大小信息读取实际数据
     * @param channel 数据源通道
     * @return 实际读取的字节数
     * @throws IOException 如果读取过程中发生IO错误
     */
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        // 第一阶段：读取大小信息
        if (size.hasRemaining()) {
            // 从通道读取大小信息到size缓冲区
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException(); // 通道已关闭
            read += bytesRead;
            
            // 如果大小信息读取完成，进行处理
            if (!size.hasRemaining()) {
                size.rewind(); // 重置position以读取大小值
                int receiveSize = size.getInt(); // 获取数据包大小
                
                // 验证数据包大小的合法性
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                
                requestedBufferSize = receiveSize; // 记录所需的缓冲区大小（SASL认证时可能为0）
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER; // 对于空数据包使用空缓冲区
                }
            }
        }
        
        // 第二阶段：分配并读取数据
        if (buffer == null && requestedBufferSize != -1) { // 已知所需大小但尚未分配缓冲区
            // 尝试从内存池分配缓冲区
            buffer = memoryPool.tryAllocate(requestedBufferSize);
            if (buffer == null)
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
        }
        
        // 如果缓冲区已分配，读取数据
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException(); // 通道已关闭
            read += bytesRead;
        }

        return read; // 返回总共读取的字节数
    }

    /**
     * 检查是否已知所需的内存大小
     * @return 如果已经读取到数据大小信息则返回true
     */
    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    /**
     * 检查是否已经分配了内存
     * @return 如果数据缓冲区已经分配则返回true
     */
    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }


    /**
     * 关闭接收器并释放资源
     * @throws IOException 如果释放资源时发生错误
     */
    @Override
    public void close() throws IOException {
        if (buffer != null && buffer != EMPTY_BUFFER) {
            memoryPool.release(buffer); // 释放缓冲区回内存池
            buffer = null;
        }
    }

    /**
     * 获取数据负载缓冲区
     * @return 存储实际数据的缓冲区
     */
    public ByteBuffer payload() {
        return this.buffer;
    }

    /**
     * 获取已读取的字节数
     * @return 当前已读取的总字节数（包括大小信息和数据）
     */
    public int bytesRead() {
        if (buffer == null)
            return size.position(); // 如果数据缓冲区未分配，只返回大小信息的读取进度
        return buffer.position() + size.position(); // 返回大小信息和数据的总读取进度
    }

    /**
     * 获取接收数据的总大小，包括大小信息缓冲区和数据负载缓冲区
     * 用于指标统计，与{@link NetworkSend#size()}保持一致的计算方式
     * @return 总大小（字节数）
     */
    public int size() {
        return payload().limit() + size.limit(); // 数据缓冲区限制 + 大小信息缓冲区限制
    }

}
