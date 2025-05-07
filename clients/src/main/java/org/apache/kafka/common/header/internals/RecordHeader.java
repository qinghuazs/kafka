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
package org.apache.kafka.common.header.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * 记录头部类
 * 实现Header接口，用于管理Kafka记录的头部信息。
 * 支持延迟解析的设计，可以从ByteBuffer中懒加载键值对。
 * 
 * 应用场景：
 * 1. 消息元数据：存储消息的附加信息
 * 2. 消息路由：基于头部信息进行消息路由
 * 3. 应用集成：在不同系统间传递元数据
 * 4. 消息追踪：记录消息处理的跟踪信息
 *
 * 设计考虑：
 * 1. 延迟解析：支持从ByteBuffer懒加载以提高性能
 * 2. 内存优化：释放ByteBuffer以节省内存
 * 3. 不可变性：键值对一旦设置不可修改
 * 4. 空值处理：不允许空键，但允许空值
 */
public class RecordHeader implements Header {
    /**
     * 键的ByteBuffer
     * 用于延迟解析键的原始数据
     */
    private ByteBuffer keyBuffer;

    /**
     * 解析后的键字符串
     * 从keyBuffer解析得到的UTF-8字符串
     */
    private String key;

    /**
     * 值的ByteBuffer
     * 用于延迟解析值的原始数据
     */
    private ByteBuffer valueBuffer;

    /**
     * 解析后的值字节数组
     * 从valueBuffer解析得到的字节数组
     */
    private byte[] value;

    /**
     * 使用字符串键和字节数组值创建记录头部
     * 
     * 实现说明：
     * - 验证键不能为null
     * - 直接存储键值对，不需要延迟解析
     *
     * @param key 头部的键，不能为null
     * @param value 头部的值，可以为null
     * @throws NullPointerException 如果key为null
     */
    public RecordHeader(String key, byte[] value) {
        // 验证key不能为null
        Objects.requireNonNull(key, "Null header keys are not permitted");
        // 直接存储键值对
        this.key = key;
        this.value = value;
    }

    /**
     * 使用ByteBuffer创建记录头部
     * 支持延迟解析的构造方法
     * 
     * 实现说明：
     * - 验证keyBuffer不能为null
     * - 存储原始ByteBuffer以支持延迟解析
     *
     * @param keyBuffer 键的ByteBuffer，不能为null
     * @param valueBuffer 值的ByteBuffer，可以为null
     * @throws NullPointerException 如果keyBuffer为null
     */
    public RecordHeader(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        // 验证keyBuffer不能为null
        this.keyBuffer = Objects.requireNonNull(keyBuffer, "Null header keys are not permitted");
        // 存储valueBuffer
        this.valueBuffer = valueBuffer;
    }
    
    /**
     * 获取头部的键
     * 如果需要，从ByteBuffer中解析键
     * 
     * 实现说明：
     * - 懒加载方式解析键
     * - 解析后释放ByteBuffer
     *
     * @return 头部的键字符串
     */
    public String key() {
        // 如果key为null，从keyBuffer中解析
        if (key == null) {
            // 将ByteBuffer解析为UTF-8字符串
            key = Utils.utf8(keyBuffer, keyBuffer.remaining());
            // 释放ByteBuffer
            keyBuffer = null;
        }
        // 返回键字符串
        return key;
    }

    /**
     * 获取头部的值
     * 如果需要，从ByteBuffer中解析值
     * 
     * 实现说明：
     * - 懒加载方式解析值
     * - 解析后释放ByteBuffer
     *
     * @return 头部的值字节数组，可能为null
     */
    public byte[] value() {
        // 如果value为null且valueBuffer不为null，从valueBuffer中解析
        if (value == null && valueBuffer != null) {
            // 将ByteBuffer转换为字节数组
            value = Utils.toArray(valueBuffer);
            // 释放ByteBuffer
            valueBuffer = null;
        }
        // 返回值字节数组
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RecordHeader header = (RecordHeader) o;
        return Objects.equals(key(), header.key()) &&
               Arrays.equals(value(), header.value());
    }

    @Override
    public int hashCode() {
        int result = key().hashCode();
        result = 31 * result + Arrays.hashCode(value());
        return result;
    }

    @Override
    public String toString() {
        return "RecordHeader(key = " + key() + ", value = " + Arrays.toString(value()) + ")";
    }

}
