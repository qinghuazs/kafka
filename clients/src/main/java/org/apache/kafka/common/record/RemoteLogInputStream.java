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

import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/**
 * RemoteLogInputStream类实现了从远程存储读取Kafka记录批次的功能。
 * 
 * 应用场景：
 * 1. 从远程存储系统（如S3、HDFS等）读取Kafka日志数据
 * 2. 支持不同版本的消息格式（V0/V1/V2）
 * 3. 提供记录批次的顺序读取功能
 * 
 * 设计考虑：
 * 1. 使用InputStream作为底层读取机制，支持多种远程存储系统
 * 2. 采用缓冲区机制减少I/O操作
 * 3. 实现版本兼容性，支持新旧消息格式
 */
public class RemoteLogInputStream implements LogInputStream<RecordBatch> {
    /**
     * 底层输入流，用于从远程存储读取数据
     */
    private final InputStream inputStream;
    
    /**
     * 日志头部缓冲区，用于存储直到magic字段的头部数据
     * 大小固定为HEADER_SIZE_UP_TO_MAGIC，包含了记录大小、magic值等关键信息
     */
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /**
     * 创建RemoteLogInputStream实例
     * 
     * @param inputStream 用于读取远程日志数据的输入流
     */
    public RemoteLogInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * 读取下一个记录批次
     * 
     * 实现步骤：
     * 1. 读取并解析日志头部信息
     * 2. 验证记录大小的合法性
     * 3. 读取完整的记录数据
     * 4. 根据magic值创建对应版本的记录批次
     * 
     * @return 下一个RecordBatch对象，如果到达流末尾则返回null
     * @throws IOException 如果读取过程中发生I/O错误
     * @throws CorruptRecordException 如果发现损坏的记录
     */
    @Override
    public RecordBatch nextBatch() throws IOException {
        // 清空头部缓冲区并读取新的头部数据
        logHeaderBuffer.clear();
        Utils.readFully(inputStream, logHeaderBuffer);

        // 检查是否读取到完整的头部数据
        if (logHeaderBuffer.position() < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        // 重置缓冲区位置并读取记录大小
        logHeaderBuffer.rewind();
        int size = logHeaderBuffer.getInt(SIZE_OFFSET);

        // 验证记录大小是否大于最小开销（使用V0版本的开销作为基准）
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Found record size %d smaller than minimum record " +
                                                                   "overhead (%d).", size, LegacyRecord.RECORD_OVERHEAD_V0));

        // 计算总缓冲区大小：日志开销 + 记录内容大小
        int bufferSize = LOG_OVERHEAD + size;
        // 创建包含完整负载（头部和记录）的缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // 将日志头部写入缓冲区
        buffer.put(logHeaderBuffer);

        // 读取记录负载并写入缓冲区
        Utils.readFully(inputStream, buffer);
        // 验证是否读取了完整的数据
        if (buffer.position() != bufferSize)
            return null;
        buffer.rewind();

        // 获取magic值（消息格式版本）并创建对应的记录批次
        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        MutableRecordBatch batch;
        if (magic > RecordBatch.MAGIC_VALUE_V1)
            batch = new DefaultRecordBatch(buffer);  // 创建新版本（V2及以上）记录批次
        else
            batch = new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(buffer);  // 创建旧版本记录批次

        return batch;
    }
}
