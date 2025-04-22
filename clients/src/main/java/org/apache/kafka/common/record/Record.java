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

import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;

/**
 * Kafka日志记录接口，表示一条完整的消息记录。
 * 每条记录由以下核心组件构成：
 * 1. 唯一的日志偏移量(offset) - 标识记录在日志中的位置
 * 2. 生产者分配的序列号(sequence) - 用于消息排序和重复数据删除
 * 3. 时间戳(timestamp) - 记录创建或接收的时间
 * 4. 键值对(key-value) - 实际消息内容
 * 5. 头部信息(headers) - 可选的元数据信息
 */
public interface Record {

    /** 空的消息头数组，用于在不需要头部信息时返回 */
    Header[] EMPTY_HEADERS = new Header[0];

    /**
     * 获取记录在日志中的偏移量。
     * 偏移量是一个单调递增的值，用于唯一标识分区内的消息位置。
     * @return 记录的偏移量
     */
    long offset();

    /**
     * 获取生产者分配的序列号。
     * 序列号用于确保消息的顺序性，也用于检测重复消息。
     * @return 序列号
     */
    int sequence();

    /**
     * 获取记录的总字节大小。
     * 包括记录的所有组成部分：头部、键、值等。
     * @return 记录的总字节数
     */
    int sizeInBytes();

    /**
     * 获取记录的时间戳。
     * 时间戳可以是消息创建时间或broker接收时间，具体取决于配置。
     * @return 记录的时间戳
     */
    long timestamp();

    /**
     * 验证记录的完整性。
     * 通过校验和检查确保记录未被损坏，如果校验和无效则抛出CorruptRecordException异常。
     */
    void ensureValid();

    /**
     * 获取键的字节大小。
     * @return 键的字节数，如果没有键则返回-1
     */
    int keySize();

    /**
     * 检查记录是否包含键。
     * @return 如果有键返回true，否则返回false
     */
    boolean hasKey();

    /**
     * 获取记录的键。
     * 键通常用于消息的分区路由和数据分组。
     * @return 记录的键，如果没有则返回null
     */
    ByteBuffer key();

    /**
     * 获取值的字节大小。
     * @return 值的字节数，如果值为null则返回-1
     */
    int valueSize();

    /**
     * 检查记录是否包含值。
     * @return 如果有值返回true，否则返回false
     */
    boolean hasValue();

    /**
     * 获取记录的值。
     * 值包含了实际的消息内容。
     * @return 记录的值，可能为null
     */
    ByteBuffer value();

    /**
     * 检查记录是否具有特定的magic值。
     * magic值用于标识记录格式的版本：
     * - 版本2之前：每条记录包含自己的magic值
     * - 版本2及以上：如果传入的magic值大于等于2则返回true
     *
     * @param magic 要检查的magic值
     * @return 如果记录的magic值匹配则返回true
     */
    boolean hasMagic(byte magic);

    /**
     * 检查记录是否被压缩。
     * 仅适用于版本2之前的记录：
     * - 版本2之前：可能包含嵌套的记录内容
     * - 版本2及以上：始终返回false
     * 
     * @return 如果magic值小于2且记录被压缩则返回true
     */
    boolean isCompressed();

    /**
     * 检查记录的时间戳类型。
     * 仅适用于版本2之前的记录：
     * - 版本2之前：记录包含时间戳类型属性
     * - 版本2及以上：始终返回false
     *
     * @param timestampType 要比较的时间戳类型
     * @return 如果版本小于2且时间戳类型匹配则返回true
     */
    boolean hasTimestampType(TimestampType timestampType);

    /**
     * 获取记录的所有头部信息。
     * 头部信息用于存储消息的元数据：
     * - magic版本1及以下：始终返回空数组
     * - magic版本2及以上：返回实际的头部信息
     *
     * @return 头部信息数组
     */
    Header[] headers();
}
