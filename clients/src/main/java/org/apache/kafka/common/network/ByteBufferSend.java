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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 一个由字节缓冲区数组支持的发送实现
 * 该类用于在Kafka网络层中高效地发送数据，支持多个ByteBuffer的批量传输
 * 实现了Send接口，提供了数据传输的核心功能
 */
public class ByteBufferSend implements Send {

    // 要发送的总字节数
    private final long size;
    // 存储待发送数据的字节缓冲区数组，protected访问级别允许子类访问
    protected final ByteBuffer[] buffers;
    // 剩余待发送的字节数
    private long remaining;
    // 标识当前是否有待处理的写操作
    private boolean pending = false;

    /**
     * 构造函数，接收可变数量的ByteBuffer参数
     * @param buffers 待发送的字节缓冲区数组
     * 实现细节：
     * 1. 遍历所有缓冲区，计算总的待发送字节数
     * 2. 初始化size和remaining字段
     */
    public ByteBufferSend(ByteBuffer... buffers) {
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        this.size = remaining;
    }

    /**
     * 构造函数，接收固定大小的ByteBuffer数组和预定义的大小
     * @param buffers 待发送的字节缓冲区数组
     * @param size 预定义的总字节数
     * 应用场景：当预先知道要发送的确切大小时使用，可以避免遍历计算大小
     */
    public ByteBufferSend(ByteBuffer[] buffers, long size) {
        this.buffers = buffers;
        this.size = size;
        this.remaining = size;
    }

    /**
     * 检查数据发送是否完成
     * @return 当所有数据都已发送且没有待处理的写操作时返回true
     * 实现细节：通过检查remaining和pending两个状态判断发送完成状态
     */
    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    /**
     * 获取要发送的总字节数
     * @return 总字节数
     */
    @Override
    public long size() {
        return this.size;
    }

    /**
     * 将数据写入到指定的传输通道
     * @param channel 目标传输通道
     * @return 本次写入的字节数
     * @throws IOException 如果写入过程中发生IO错误
     * 实现细节：
     * 1. 调用channel.write写入数据
     * 2. 更新剩余字节数
     * 3. 检查是否有待处理的写操作
     */
    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        pending = channel.hasPendingWrites();
        return written;
    }

    /**
     * 获取剩余待发送的字节数
     * @return 剩余字节数
     */
    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return "ByteBufferSend(" +
            ", size=" + size +
            ", remaining=" + remaining +
            ", pending=" + pending +
            ')';
    }

    /**
     * 创建一个带有大小前缀的ByteBufferSend实例
     * @param buffer 要发送的数据缓冲区
     * @return 新的ByteBufferSend实例
     * 实现细节：
     * 1. 创建一个4字节的缓冲区用于存储数据大小
     * 2. 将数据大小写入缓冲区
     * 3. 返回包含大小信息和数据的ByteBufferSend实例
     * 应用场景：在需要在数据前面附加长度信息的场景，如消息帧的传输
     */
    public static ByteBufferSend sizePrefixed(ByteBuffer buffer) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(0, buffer.remaining());
        return new ByteBufferSend(sizeBuffer, buffer);
    }
}
