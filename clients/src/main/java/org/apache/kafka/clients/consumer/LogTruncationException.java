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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

/**
 * 日志截断异常类
 * 
 * 在发生非正常的Leader选举(Unclean Leader Election)时，日志会被截断，
 * 之前已提交的数据将会丢失，新的数据会被写入到这些偏移量位置。
 * 当这种情况发生时，如果没有配置自动重置策略(auto reset policy)，
 * 消费者会检测到截断并抛出此异常，同时提供第一个已知与消费者之前读取内容不一致的偏移量。
 * 
 * 这个异常通常发生在以下场景：
 * 1. Broker发生故障，触发了非正常的Leader选举
 * 2. 新的Leader的日志比老的Leader要短，导致日志被截断
 * 3. 消费者尝试读取的偏移量在被截断的范围内
 */
public class LogTruncationException extends OffsetOutOfRangeException {

    // 存储每个分区的发散偏移量信息，即与消费者之前读取内容不一致的第一个偏移量
    private final Map<TopicPartition, OffsetAndMetadata> divergentOffsets;

    /**
     * 构造函数
     * @param fetchOffsets 消费者尝试获取消息的偏移量映射
     * @param divergentOffsets 每个分区发生截断的发散偏移量映射
     */
    public LogTruncationException(Map<TopicPartition, Long> fetchOffsets,
                                  Map<TopicPartition, OffsetAndMetadata> divergentOffsets) {
        // 使用默认的错误消息构造异常
        this("Truncated partitions detected with divergent offsets " + divergentOffsets, fetchOffsets, divergentOffsets);
    }

    /**
     * 构造函数
     * @param message 自定义错误消息
     * @param fetchOffsets 消费者尝试获取消息的偏移量映射
     * @param divergentOffsets 每个分区发生截断的发散偏移量映射
     */
    public LogTruncationException(String message,
                                  Map<TopicPartition, Long> fetchOffsets,
                                  Map<TopicPartition, OffsetAndMetadata> divergentOffsets) {
        // 调用父类构造函数，传入错误消息和获取偏移量映射
        super(message, fetchOffsets);
        // 将发散偏移量映射转换为不可修改的Map
        this.divergentOffsets = Collections.unmodifiableMap(divergentOffsets);
    }

    /**
     * 获取被截断分区的发散偏移量
     * 
     * 对于每个分区，返回的偏移量是第一个已知与消费者之前读取内容不一致的位置。
     * 这些偏移量表示了日志截断发生的位置，可以帮助诊断和处理数据丢失问题。
     * 
     * 注意：并不保证所有被截断的分区都能获知其发散偏移量。因此在使用时需要：
     * 1. 首先通过{@link #partitions()}方法获取所有被截断的分区集合
     * 2. 然后检查每个分区在本方法返回的Map中是否存在对应的发散偏移量
     * 
     * @return 包含被截断分区及其发散偏移量的不可修改Map
     */
    public Map<TopicPartition, OffsetAndMetadata> divergentOffsets() {
        return divergentOffsets;
    }
}
