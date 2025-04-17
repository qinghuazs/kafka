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
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Readable接口定义了Kafka序列化机制中的数据读取操作
 * 该接口提供了读取各种基本数据类型、字节数组、可变长整数等方法
 * 实现类负责从底层字节流中反序列化数据
 */
public interface Readable {
    /**
     * 读取一个字节的数据
     * @return 读取到的字节值
     */
    byte readByte();

    /**
     * 读取一个short类型的数据(2字节)
     * @return 读取到的short值
     */
    short readShort();

    /**
     * 读取一个int类型的数据(4字节)
     * @return 读取到的int值
     */
    int readInt();

    /**
     * 读取一个long类型的数据(8字节)
     * @return 读取到的long值
     */
    long readLong();

    /**
     * 读取一个double类型的数据(8字节)
     * @return 读取到的double值
     */
    double readDouble();

    /**
     * 读取指定长度的字节数组
     * @param length 要读取的字节数
     * @return 读取到的字节数组
     */
    byte[] readArray(int length);

    /**
     * 读取一个无符号的可变长度整数
     * 使用可变长度编码可以节省空间
     * @return 读取到的整数值
     */
    int readUnsignedVarint();

    /**
     * 读取指定长度的ByteBuffer
     * @param length 要读取的字节数
     * @return 包含读取数据的ByteBuffer
     */
    ByteBuffer readByteBuffer(int length);

    /**
     * 读取一个有符号的可变长度整数
     * @return 读取到的整数值
     */
    int readVarint();

    /**
     * 读取一个可变长度的long值
     * @return 读取到的long值
     */
    long readVarlong();

    /**
     * 获取剩余可读取的字节数
     * @return 剩余的字节数
     */
    int remaining();

    /**
     * 读取指定长度的字符串，使用UTF-8编码
     * @param length 要读取的字节数
     * @return 解码后的字符串
     */
    default String readString(int length) {
        byte[] arr = readArray(length);
        return new String(arr, StandardCharsets.UTF_8);
    }

    /**
     * 读取未知的标签字段
     * 用于处理协议版本兼容性，保存不认识的字段数据
     * @param unknowns 存储未知字段的列表
     * @param tag 字段标签
     * @param size 字段数据大小
     * @return 更新后的未知字段列表
     */
    default List<RawTaggedField> readUnknownTaggedField(List<RawTaggedField> unknowns, int tag, int size) {
        if (unknowns == null) {
            unknowns = new ArrayList<>();
        }
        byte[] data = readArray(size);
        unknowns.add(new RawTaggedField(tag, data));
        return unknowns;
    }

    /**
     * 读取消息记录集合
     * @param length 记录集合的字节长度
     * @return 读取到的MemoryRecords对象，如果length小于0则返回null
     */
    default MemoryRecords readRecords(int length) {
        if (length < 0) {
            // no records
            return null;
        } else {
            ByteBuffer recordsBuffer = readByteBuffer(length);
            return MemoryRecords.readableRecords(recordsBuffer);
        }
    }

    /**
     * Read a UUID with the most significant digits first.
     */
    default Uuid readUuid() {
        return new Uuid(readLong(), readLong());
    }

    /**
     * 读取无符号short值
     * @return 转换为int的无符号short值
     */
    default int readUnsignedShort() {
        return Short.toUnsignedInt(readShort());
    }

    /**
     * 读取无符号int值
     * @return 转换为long的无符号int值
     */
    default long readUnsignedInt() {
        return Integer.toUnsignedLong(readInt());
    }
}
