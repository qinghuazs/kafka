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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;

import java.util.Optional;

/**
 * OffsetAndTimestamp的内部表示类
 * 允许使用负数时间戳和偏移量，这在内部处理中可能需要
 * 
 * 应用场景：
 * 1. 处理特殊的时间戳值（如-1表示最早可用偏移量）
 * 2. 处理特殊的偏移量值（如-1表示无效偏移量）
 * 3. 在内部操作中需要使用负值的场景
 * 4. 在构建公共API响应前的中间表示
 */
public class OffsetAndTimestampInternal {
    /**
     * 消息的时间戳
     * 可以是负值，用于特殊场景（如-1表示无效时间戳）
     */
    private final long timestamp;

    /**
     * 消息的偏移量
     * 可以是负值，用于特殊场景（如-1表示无效偏移量）
     */
    private final long offset;

    /**
     * 领导者纪元
     * 用于确保消息的一致性和顺序性
     * Optional包装允许表示无领导者纪元的情况
     */
    private final Optional<Integer> leaderEpoch;

    /**
     * 构造函数
     * 创建一个新的OffsetAndTimestampInternal实例
     *
     * @param offset 消息偏移量，可以是负值
     * @param timestamp 消息时间戳，可以是负值
     * @param leaderEpoch 可选的领导者纪元
     */
    public OffsetAndTimestampInternal(long offset, long timestamp, Optional<Integer> leaderEpoch) {
        // 初始化偏移量
        this.offset = offset;
        // 初始化时间戳
        this.timestamp = timestamp;
        // 初始化领导者纪元
        this.leaderEpoch = leaderEpoch;
    }

    /**
     * 获取偏移量
     * 
     * @return 消息的偏移量，可能为负值
     */
    long offset() {
        // 返回存储的偏移量值
        return offset;
    }

    /**
     * 获取时间戳
     * 
     * @return 消息的时间戳，可能为负值
     */
    long timestamp() {
        // 返回存储的时间戳值
        return timestamp;
    }

    /**
     * 获取领导者纪元
     * 
     * @return 可选的领导者纪元
     */
    Optional<Integer> leaderEpoch() {
        // 返回存储的领导者纪元
        return leaderEpoch;
    }

    /**
     * 构建公共API使用的OffsetAndTimestamp实例
     * 将内部表示转换为外部API使用的格式
     * 
     * @return 新创建的OffsetAndTimestamp实例
     */
    public OffsetAndTimestamp buildOffsetAndTimestamp() {
        // 使用当前实例的值创建新的OffsetAndTimestamp对象
        return new OffsetAndTimestamp(offset, timestamp, leaderEpoch);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + leaderEpoch.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OffsetAndTimestampInternal)) return false;

        OffsetAndTimestampInternal that = (OffsetAndTimestampInternal) o;

        if (timestamp != that.timestamp) return false;
        if (offset != that.offset) return false;
        return leaderEpoch.equals(that.leaderEpoch);
    }

    @Override
    public String toString() {
        return "OffsetAndTimestampInternal{" +
                "timestamp=" + timestamp +
                ", offset=" + offset +
                ", leaderEpoch=" + leaderEpoch +
                '}';
    }
}
