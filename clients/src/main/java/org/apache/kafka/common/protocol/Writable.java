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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;

import java.nio.ByteBuffer;

/**
 * Writable接口定义了Kafka序列化机制中的数据写入操作
 * 该接口提供了一系列方法用于将不同类型的数据写入到底层存储中
 * 实现类负责处理具体的字节写入逻辑
 */
public interface Writable {
    /**
     * 写入一个字节的数据
     * @param val 要写入的字节值
     */
    void writeByte(byte val);

    /**
     * 写入一个short类型的数据(2字节)
     * @param val 要写入的short值
     */
    void writeShort(short val);

    /**
     * 写入一个int类型的数据(4字节)
     * @param val 要写入的int值
     */
    void writeInt(int val);

    /**
     * 写入一个long类型的数据(8字节)
     * @param val 要写入的long值
     */
    void writeLong(long val);

    /**
     * 写入一个double类型的数据(8字节)
     * @param val 要写入的double值
     */
    void writeDouble(double val);

    /**
     * 写入一个字节数组
     * @param arr 要写入的字节数组
     */
    void writeByteArray(byte[] arr);

    /**
     * 写入一个无符号的可变长度整数
     * 使用可变长度编码可以节省空间
     * @param i 要写入的整数值
     */
    void writeUnsignedVarint(int i);

    /**
     * 写入一个ByteBuffer中的数据
     * @param buf 包含要写入数据的ByteBuffer
     */
    void writeByteBuffer(ByteBuffer buf);

    /**
     * 写入一个有符号的可变长度整数
     * @param i 要写入的整数值
     */
    void writeVarint(int i);

    /**
     * 写入一个可变长度的long值
     * @param i 要写入的long值
     */
    void writeVarlong(long i);

    /**
     * 写入记录集合，支持MemoryRecords和UnalignedMemoryRecords两种类型
     * @param records 要写入的记录集合
     * @throws UnsupportedOperationException 当记录类型不支持时抛出异常
     */
    default void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            MemoryRecords memRecords = (MemoryRecords) records;
            writeByteBuffer(memRecords.buffer());
        } else if (records instanceof UnalignedMemoryRecords) {
            UnalignedMemoryRecords memRecords = (UnalignedMemoryRecords) records;
            writeByteBuffer(memRecords.buffer());
        } else {
            throw new UnsupportedOperationException("Unsupported record type " + records.getClass());
        }
    }

    /**
     * 写入UUID，按照最高有效位优先的顺序写入
     * @param uuid 要写入的UUID对象
     */
    default void writeUuid(Uuid uuid) {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    /**
     * 写入无符号short值
     * 生成的代码中的setter函数会确保输入值在short的有效范围内
     * @param i 要写入的无符号short值
     */
    default void writeUnsignedShort(int i) {
        // The setter functions in the generated code prevent us from setting
        // ints outside the valid range of a short.
        writeShort((short) i);
    }

    /**
     * 写入无符号int值
     * @param i 要写入的无符号int值
     */
    default void writeUnsignedInt(long i) {
        writeInt((int) i);
    }
}
