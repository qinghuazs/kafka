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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MultiRecordsSend;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * 该类用于构建网络传输所需的{@link Send}对象，主要用于处理从{@link org.apache.kafka.common.protocol.ApiMessage}类型
 * 生成的消息。它的特点是对于"零拷贝"字段（参见{@link #writeByteBuffer(ByteBuffer)}和{@link #writeRecords(BaseRecords)}）
 * 不会分配新的内存空间，从而提高性能。
 *
 * 使用示例可以参考{@link org.apache.kafka.common.requests.EnvelopeRequest#toSend(RequestHeader)}
 */
public class SendBuilder implements Writable {
    // 主缓冲区，用于存储需要写入的基本数据类型
    private final ByteBuffer buffer;

    // 存储Send对象的队列，初始容量为1
    private final Queue<Send> sends = new ArrayDeque<>(1);
    // 记录所有Send对象的总大小
    private long sizeOfSends = 0;

    // 存储ByteBuffer对象的列表，用于零拷贝操作
    private final List<ByteBuffer> buffers = new ArrayList<>();
    // 记录所有ByteBuffer对象的总大小
    private long sizeOfBuffers = 0;

    /**
     * 构造函数，创建指定大小的主缓冲区
     * @param size 缓冲区大小（字节）
     */
    SendBuilder(int size) {
        // 分配指定大小的字节缓冲区
        this.buffer = ByteBuffer.allocate(size);
        // 标记当前位置，用于后续重置
        this.buffer.mark();
    }

    /**
     * 写入单个字节
     * @param val 要写入的字节值
     */
    @Override
    public void writeByte(byte val) {
        // 将字节值写入缓冲区
        buffer.put(val);
    }

    /**
     * 写入short类型值（2字节）
     * @param val 要写入的short值
     */
    @Override
    public void writeShort(short val) {
        // 将short值写入缓冲区
        buffer.putShort(val);
    }

    /**
     * 写入int类型值（4字节）
     * @param val 要写入的int值
     */
    @Override
    public void writeInt(int val) {
        // 将int值写入缓冲区
        buffer.putInt(val);
    }

    /**
     * 写入long类型值（8字节）
     * @param val 要写入的long值
     */
    @Override
    public void writeLong(long val) {
        // 将long值写入缓冲区
        buffer.putLong(val);
    }

    /**
     * 写入double类型值（8字节）
     * @param val 要写入的double值
     */
    @Override
    public void writeDouble(double val) {
        // 将double值写入缓冲区
        buffer.putDouble(val);
    }

    /**
     * 写入字节数组
     * @param arr 要写入的字节数组
     */
    @Override
    public void writeByteArray(byte[] arr) {
        // 将字节数组写入缓冲区
        buffer.put(arr);
    }

    /**
     * 写入无符号可变长度整数
     * @param i 要写入的整数值
     */
    @Override
    public void writeUnsignedVarint(int i) {
        // 使用ByteUtils工具类写入无符号可变长度整数
        ByteUtils.writeUnsignedVarint(i, buffer);
    }

    /**
     * 写入ByteBuffer对象，这是一个零拷贝操作。
     * 该方法不会复制缓冲区的内容，而是保留对原始缓冲区的引用，
     * 这个引用会在{@link #build()}方法的结果中保留。
     *
     * @param buf 要写入的ByteBuffer对象
     */
    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        // 先刷新当前缓冲区中的数据
        flushPendingBuffer();
        // 添加缓冲区的副本（只复制引用，不复制数据）
        addBuffer(buf.duplicate());
    }

    /**
     * 写入可变长度整数
     * @param i 要写入的整数值
     */
    @Override
    public void writeVarint(int i) {
        // 使用ByteUtils工具类写入可变长度整数
        ByteUtils.writeVarint(i, buffer);
    }

    /**
     * 写入可变长度长整数
     * @param i 要写入的长整数值
     */
    @Override
    public void writeVarlong(long i) {
        // 使用ByteUtils工具类写入可变长度长整数
        ByteUtils.writeVarlong(i, buffer);
    }

    /**
     * 添加ByteBuffer到缓冲区列表
     * @param buffer 要添加的ByteBuffer
     */
    private void addBuffer(ByteBuffer buffer) {
        // 将ByteBuffer添加到列表
        buffers.add(buffer);
        // 更新总大小
        sizeOfBuffers += buffer.remaining();
    }

    /**
     * 添加Send对象到发送队列
     * @param send 要添加的Send对象
     */
    private void addSend(Send send) {
        // 将Send对象添加到队列
        sends.add(send);
        // 更新总大小
        sizeOfSends += send.size();
    }

    /**
     * 清空缓冲区列表
     */
    private void clearBuffers() {
        // 清空ByteBuffer列表
        buffers.clear();
        // 重置总大小
        sizeOfBuffers = 0;
    }

    /**
     * 写入记录集。这是一个零拷贝操作，底层的记录数据会被保留在{@link #build()}的结果中。
     * 详见{@link BaseRecords#toSend()}。
     *
     * @param records 要写入的记录集
     */
    @Override
    public void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            // 对于内存记录，直接使用其底层缓冲区
            flushPendingBuffer();
            addBuffer(((MemoryRecords) records).buffer());
        } else if (records instanceof UnalignedMemoryRecords) {
            // 对于未对齐的内存记录，也直接使用其底层缓冲区
            flushPendingBuffer();
            addBuffer(((UnalignedMemoryRecords) records).buffer());
        } else {
            // 对于其他类型的记录，转换为Send对象
            flushPendingSend();
            addSend(records.toSend());
        }
    }

    /**
     * 刷新待发送的数据
     * 将所有缓冲区中的数据转换为Send对象
     */
    private void flushPendingSend() {
        // 先刷新当前缓冲区
        flushPendingBuffer();
        if (!buffers.isEmpty()) {
            // 将所有ByteBuffer转换为数组
            ByteBuffer[] byteBufferArray = buffers.toArray(new ByteBuffer[0]);
            // 创建新的ByteBufferSend对象并添加到发送队列
            addSend(new ByteBufferSend(byteBufferArray, sizeOfBuffers));
            // 清空缓冲区列表
            clearBuffers();
        }
    }

    /**
     * 刷新当前缓冲区
     * 如果当前缓冲区有新写入的数据，将其转换为一个新的ByteBuffer
     */
    private void flushPendingBuffer() {
        // 获取当前位置
        int latestPosition = buffer.position();
        // 重置到上次标记的位置
        buffer.reset();

        if (latestPosition > buffer.position()) {
            // 设置限制为当前位置
            buffer.limit(latestPosition);
            // 创建一个新的ByteBuffer，包含从标记位置到当前位置的数据
            addBuffer(buffer.slice());

            // 重置缓冲区状态，准备接收新数据
            buffer.position(latestPosition);
            buffer.limit(buffer.capacity());
            buffer.mark();
        }
    }

    /**
     * 构建最终的Send对象
     * @return 如果只有一个Send对象则直接返回，否则返回MultiRecordsSend
     */
    public Send build() {
        // 确保所有数据都已经刷新到Send队列
        flushPendingSend();

        if (sends.size() == 1) {
            // 如果只有一个Send对象，直接返回它
            return sends.poll();
        } else {
            // 如果有多个Send对象，创建MultiRecordsSend
            return new MultiRecordsSend(sends, sizeOfSends);
        }
    }

    public static Send buildRequestSend(
        RequestHeader header,
        Message apiRequest
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiRequest,
            header.apiVersion()
        );
    }

    public static Send buildResponseSend(
        ResponseHeader header,
        Message apiResponse,
        short apiVersion
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiResponse,
            apiVersion
        );
    }

    private static Send buildSend(
        Message header,
        short headerVersion,
        Message apiMessage,
        short apiVersion
    ) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        MessageSizeAccumulator messageSize = new MessageSizeAccumulator();
        header.addSize(messageSize, serializationCache, headerVersion);
        apiMessage.addSize(messageSize, serializationCache, apiVersion);

        SendBuilder builder = new SendBuilder(messageSize.sizeExcludingZeroCopy() + 4);
        builder.writeInt(messageSize.totalSize());
        header.write(builder, serializationCache, headerVersion);
        apiMessage.write(builder, serializationCache, apiVersion);

        return builder.build();
    }

}
