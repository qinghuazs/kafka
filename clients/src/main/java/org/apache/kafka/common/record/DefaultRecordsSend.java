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

/**
 * DefaultRecordsSend是RecordsSend的默认实现类，用于处理Kafka记录的发送操作。
 * 该类支持泛型T（必须是TransferableRecords的子类），提供了记录批次的传输功能。
 * 主要用于在Kafka的网络层中，将记录批次写入到网络通道中进行传输。
 */
public class DefaultRecordsSend<T extends TransferableRecords> extends RecordsSend<T> {
    /**
     * 构造一个新的DefaultRecordsSend实例。
     * 使用记录批次的完整大小作为最大可写入字节数。
     *
     * @param records 要发送的记录批次
     */
    public DefaultRecordsSend(T records) {
        // 调用另一个构造函数，使用records的实际大小作为最大可写入字节数
        this(records, records.sizeInBytes());
    }

    /**
     * 构造一个新的DefaultRecordsSend实例，允许指定最大可写入字节数。
     *
     * @param records 要发送的记录批次
     * @param maxBytesToWrite 最大可写入字节数，用于控制写入大小
     */
    public DefaultRecordsSend(T records, int maxBytesToWrite) {
        // 调用父类构造函数，初始化记录批次和最大写入字节数
        super(records, maxBytesToWrite);
    }

    /**
     * 将记录批次写入到指定的传输通道中。
     * 这是实际执行网络传输的核心方法，支持部分写入和续传。
     *
     * @param channel 目标传输通道
     * @param previouslyWritten 之前已经写入的字节数
     * @param remaining 剩余需要写入的字节数
     * @return 本次实际写入的字节数
     * @throws IOException 如果写入过程中发生I/O错误
     */
    @Override
    protected int writeTo(TransferableChannel channel, int previouslyWritten, int remaining) throws IOException {
        // 委托给记录批次的writeTo方法执行实际的写入操作
        return records().writeTo(channel, previouslyWritten, remaining);
    }
}
