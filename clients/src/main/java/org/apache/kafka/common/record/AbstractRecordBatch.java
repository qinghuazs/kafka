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

/**
 * Kafka记录批次的抽象基类，提供了记录批次的基本功能实现。
 * 
 * 记录批次(RecordBatch)是Kafka中消息存储的基本单位，它可以包含一个或多个消息记录。
 * 这个抽象类实现了RecordBatch接口的部分方法，为不同版本的记录批次格式提供了统一的行为。
 * 
 * 主要功能：
 * 1. 生产者ID的管理：用于事务和幂等性支持
 * 2. 偏移量计算：提供批次中消息的位置信息
 * 3. 压缩状态判断：支持消息压缩功能
 */
abstract class AbstractRecordBatch implements RecordBatch {
    
    /**
     * 检查记录批次是否包含有效的生产者ID
     * 
     * 生产者ID用于：
     * 1. 支持事务功能：标识特定生产者的事务
     * 2. 实现幂等性：防止消息重复写入
     * 
     * @return 如果生产者ID大于NO_PRODUCER_ID（通常是-1），则返回true，表示批次包含有效的生产者ID
     */
    @Override
    public boolean hasProducerId() {
        return RecordBatch.NO_PRODUCER_ID < producerId();
    }

    /**
     * 计算批次中下一条消息的偏移量
     * 
     * 在Kafka中，偏移量用于：
     * 1. 唯一标识分区内的消息位置
     * 2. 跟踪消费进度
     * 3. 支持消息查找
     * 
     * @return 返回最后一条消息的偏移量加1，即下一条消息的预期偏移量
     */
    @Override
    public long nextOffset() {
        return lastOffset() + 1;
    }

    /**
     * 判断记录批次是否使用了压缩
     * 
     * Kafka支持多种压缩类型：
     * 1. NONE：不压缩
     * 2. GZIP：通用压缩算法
     * 3. SNAPPY：针对速度优化的压缩算法
     * 4. LZ4：高性能压缩算法
     * 5. ZSTD：高压缩比算法
     * 
     * @return 如果使用了任何压缩类型（非NONE），则返回true
     */
    @Override
    public boolean isCompressed() {
        return compressionType() != CompressionType.NONE;
    }

}
