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
 * 表示一个可以传输到通道的记录集合
 * 该接口继承自BaseRecords，用于处理Kafka中需要进行网络传输的记录集合
 * 实现该接口的类需要提供将记录内容写入到传输通道的能力
 * 
 * 应用场景：
 * 1. 在Kafka的生产者和消费者之间传输消息记录
 * 2. 在Kafka的副本同步过程中传输数据
 * 3. 在Kafka的跨数据中心复制过程中传输消息
 * 
 * 设计考虑：
 * 1. 通过position和length参数支持部分传输，提高传输灵活性
 * 2. 返回实际写入的字节数，便于调用方进行写入确认和重试
 * 3. 继承BaseRecords接口，保持与Kafka记录处理体系的一致性
 * 
 * @see Records 基础记录接口，定义了记录集合的基本操作
 * @see UnalignedRecords 非对齐记录实现，用于特殊场景的记录处理
 */
public interface TransferableRecords extends BaseRecords {

    /**
     * 尝试将缓冲区的内容写入到指定的通道中
     * 
     * 实现细节：
     * 1. 从指定的position位置开始写入数据
     * 2. 最多写入length指定的字节数
     * 3. 如果通道的写入缓冲区已满，可能写入的字节数少于请求的长度
     * 4. 如果发生IO错误，将抛出IOException异常
     * 
     * @param channel 要写入的目标通道，通常是网络连接或文件通道
     * @param position 要开始写入的缓冲区位置，用于支持断点续传和部分传输
     * @param length 要写入的字节数，用于控制每次传输的数据量
     * @return 实际写入的字节数，可能小于请求的length
     * @throws IOException 当发生IO错误时抛出，如网络断开或磁盘写入失败
     */
    int writeTo(TransferableChannel channel, int position, int length) throws IOException;
}
