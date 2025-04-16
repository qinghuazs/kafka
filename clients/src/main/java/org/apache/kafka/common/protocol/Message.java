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

import org.apache.kafka.common.protocol.types.RawTaggedField;

import java.util.List;

/**
 * 一个可以自我序列化的对象接口。该接口定义了Kafka协议中消息的序列化规范。
 * 序列化协议是有版本控制的，以支持不同版本的Kafka集群间的兼容性。
 * 所有实现该接口的消息类都需要实现toString、equals和hashCode方法。
 * 
 * 该接口是Kafka网络通信协议的核心组件之一，用于定义消息的序列化、反序列化、大小计算等基本操作，
 * 确保消息在网络传输过程中的正确性和兼容性。
 */
public interface Message {
    /**
     * 返回此消息支持的最低API版本号（包含此版本）。
     * 
     * 该方法用于版本兼容性检查，确保消息可以被较旧版本的Kafka正确处理。
     * 当尝试使用低于此版本的API处理消息时，将会抛出UnsupportedVersionException异常。
     * 
     * @return 支持的最低API版本号
     */
    short lowestSupportedVersion();

    /**
     * 返回此消息支持的最高API版本号（包含此版本）。
     * 
     * 该方法用于版本兼容性检查，确保消息不会使用过高版本的API特性。
     * 当尝试使用高于此版本的API处理消息时，将会抛出UnsupportedVersionException异常。
     * 
     * @return 支持的最高API版本号
     */
    short highestSupportedVersion();

    /**
     * 计算序列化此消息所需的字节数。
     * 
     * 该方法用于在消息序列化之前计算所需的字节数，这对于内存分配和缓冲区管理至关重要。
     * 方法会考虑消息的所有字段，包括头部信息、元数据和实际数据内容，确保分配足够的内存空间。
     * 计算过程会使用缓存来优化性能，避免重复计算相同对象的大小。
     *
     * @param cache         序列化大小缓存对象，用于存储和复用已计算过的对象大小，
     *                      通过缓存机制可以显著提高性能，特别是在处理包含重复对象的消息时
     * @param version       使用的API版本号，不同版本可能有不同的序列化格式和字段，
     *                      版本号决定了如何计算消息大小
     *
     * @return             返回序列化此消息所需的总字节数
     *
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *                      当指定的版本号超出当前软件支持的范围时抛出此异常
     */
    default int size(ObjectSerializationCache cache, short version) {
        // 创建一个消息大小累加器对象，用于计算消息的总字节数
        // MessageSizeAccumulator提供了一个线程安全的方式来累加消息各部分的大小
        MessageSizeAccumulator size = new MessageSizeAccumulator();
        
        // 调用addSize方法将消息的各个部分大小添加到累加器中
        // 这个方法会遍历消息的所有字段，根据版本号和序列化格式计算每个字段的大小
        addSize(size, cache, version);
        
        // 返回消息的总字节数
        // totalSize方法会返回所有已添加部分的大小之和
        return size.totalSize();
    }

    /**
     * 将此消息的大小添加到累加器中。
     * 
     * 该方法用于计算消息序列化后的字节大小，这对于内存分配和缓冲区管理很重要。
     * 实现类需要将消息中所有字段的大小都添加到累加器中。
     *
     * @param size          消息大小累加器，用于累加消息各部分的大小
     * @param cache         序列化大小缓存，用于优化重复对象的大小计算
     * @param version       使用的API版本号，不同版本可能有不同的字段和序列化格式
     */
    void addSize(MessageSizeAccumulator size, ObjectSerializationCache cache, short version);

    /**
     * 将此消息写入给定的Writable对象中。
     * 
     * 该方法执行实际的序列化操作，将消息的所有字段按照指定版本的格式写入输出流。
     * 在调用此方法之前，必须先调用size()方法来计算和缓存消息大小。
     *
     * @param writable      目标输出对象，用于写入序列化后的字节数据
     * @param cache         对象序列化缓存，用于优化序列化过程，必须通过之前调用size()方法来填充
     * @param version       使用的API版本号，决定了消息的序列化格式
     *
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *                      如果指定的版本号过高，当前软件版本不支持时抛出此异常
     */
    void write(Writable writable, ObjectSerializationCache cache, short version);

    /**
     * 从给定的Readable对象中读取此消息的内容。
     * 
     * 该方法执行反序列化操作，将字节流数据解析为消息对象。
     * 会覆盖当前对象中的所有相关字段。
     *
     * @param readable      源输入对象，提供要读取的序列化数据
     * @param version       使用的API版本号，决定了如何解析消息格式
     *
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *                      如果指定的版本号过高，当前软件版本不支持时抛出此异常
     */
    void read(Readable readable, short version);

    /**
     * 返回此软件无法理解的标记字段列表。
     * 
     * 该方法用于处理向前兼容性，当消息中包含新版本增加的未知字段时，
     * 这些字段会被保存下来，以便在之后转发消息时保持完整性。
     *
     * @return              原始的标记字段列表
     */
    List<RawTaggedField> unknownTaggedFields();

    /**
     * 创建消息的深拷贝。
     * 
     * 该方法确保返回的新消息对象与原消息完全独立，
     * 所有可变字段都会被复制，而不是共享引用。
     *
     * @return              消息的深拷贝，与原消息不共享任何可变字段
     */
    Message duplicate();
}
