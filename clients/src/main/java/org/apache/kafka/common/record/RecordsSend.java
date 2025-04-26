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

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransferableChannel;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 用于发送记录的抽象基类，负责将记录批次写入到可传输通道中。
 * 该类实现了Send接口，提供了记录批次的写入功能，支持分批写入和写入状态跟踪。
 * 泛型参数T必须是BaseRecords的子类，用于表示具体的记录类型。
 * 
 * 应用场景：
 * 1. 在Kafka生产者发送消息时，用于将消息记录批次写入网络通道
 * 2. 在Kafka服务器之间复制数据时，用于传输消息记录
 * 3. 支持大数据量的分批传输，避免一次性加载过多数据到内存
 */
public abstract class RecordsSend<T extends BaseRecords> implements Send {
    // 空字节缓冲区，用于在写入完成但仍有待处理操作时发送
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    // 待发送的记录批次
    private final T records;
    // 最大可写入字节数，用于控制写入大小
    private final int maxBytesToWrite;
    // 剩余待写入的字节数
    private int remaining;
    // 是否有待处理的写入操作
    private boolean pending = false;

    /**
     * 构造函数，初始化记录发送器
     * @param records 要发送的记录批次
     * @param maxBytesToWrite 最大可写入字节数，用于控制单次发送的数据量
     */
    protected RecordsSend(T records, int maxBytesToWrite) {
        this.records = records;
        this.maxBytesToWrite = maxBytesToWrite;
        this.remaining = maxBytesToWrite;
    }

    /**
     * 检查记录发送是否已完成
     * @return 当剩余字节数小于等于0且没有待处理的写入操作时返回true
     */
    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    /**
     * 将记录写入到指定的传输通道中
     * 实现了分批写入逻辑，每次写入一部分数据，直到所有数据都写入完成
     * 
     * @param channel 目标传输通道
     * @return 本次实际写入的字节数
     * @throws IOException 发生IO错误时抛出
     * @throws EOFException 写入返回负值时抛出（这种情况不应该发生）
     */
    @Override
    public final long writeTo(TransferableChannel channel) throws IOException {
        int written = 0;

        if (remaining > 0) {
            written = writeTo(channel, maxBytesToWrite - remaining, remaining);
            if (written < 0)
                throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
            remaining -= written;
        }

        pending = channel.hasPendingWrites();
        if (remaining <= 0 && pending)
            channel.write(EMPTY_BYTE_BUFFER);

        return written;
    }

    /**
     * 获取要发送的记录总大小
     * @return 最大可写入字节数
     */
    @Override
    public long size() {
        return maxBytesToWrite;
    }

    /**
     * 获取待发送的记录批次
     * @return 记录批次对象
     */
    protected T records() {
        return records;
    }

    /**
     * 将记录批次写入到传输通道中，支持分批写入和状态跟踪
     * 
     * 该方法是实现记录传输的核心方法，允许子类维护写入状态，实现自定义的写入逻辑。
     * 写入过程遵循以下约定：
     * 1. 首次调用时，previouslyWritten为0，remaining等于要写入的最大字节数
     * 2. 后续调用时，previouslyWritten表示已写入字节数，remaining表示剩余可写字节数
     * 3. 方法应尽可能多地写入数据，但不超过remaining指定的字节数
     * 4. 返回实际写入的字节数，用于更新写入进度
     * 
     * 应用场景：
     * - 生产者发送大量消息时的批量写入
     * - 服务器间复制数据时的增量传输
     * - 处理网络延迟时的断点续传
     * 
     * 实现建议：
     * - 根据previouslyWritten跟踪写入位置
     * - 使用remaining控制写入大小
     * - 处理网络缓冲区满等异常情况
     * 
     * @param channel 目标传输通道，用于实际的数据写入操作
     * @param previouslyWritten 之前调用已写入的字节数；首次调用时为0
     * @param remaining 当前剩余可写入的字节数
     * @return 本次实际写入的字节数
     * @throws IOException 发生IO错误时抛出异常
     * 
     * @see #writeTo(TransferableChannel) 外层方法负责调用该方法并管理写入状态
     */
    protected abstract int writeTo(TransferableChannel channel, int previouslyWritten, int remaining) throws IOException;
}
