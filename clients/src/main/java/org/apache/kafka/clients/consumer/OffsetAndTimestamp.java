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

import java.util.Objects;
import java.util.Optional;

/**
 * 一个用于存储偏移量和时间戳的容器类。
 * 该类在Kafka消费者API中用于表示特定消息的时间戳和对应的偏移量位置。
 * 同时还包含了可选的领导者纪元(leader epoch)信息，用于确保日志完整性。
 */
public final class OffsetAndTimestamp {
    // 消息的时间戳，表示消息的创建时间或追加时间
    private final long timestamp;
    // 消息在分区中的偏移量位置
    private final long offset;
    // 可选的领导者纪元，用于检测日志是否被截断
    private final Optional<Integer> leaderEpoch;

    /**
     * 创建一个不包含领导者纪元信息的OffsetAndTimestamp实例
     * 
     * @param offset    消息的偏移量，必须是非负数
     * @param timestamp 消息的时间戳，必须是非负数
     */
    public OffsetAndTimestamp(long offset, long timestamp) {
        this(offset, timestamp, Optional.empty());
    }

    /**
     * 创建一个包含完整信息的OffsetAndTimestamp实例
     * 
     * @param offset      消息的偏移量，必须是非负数
     * @param timestamp   消息的时间戳，必须是非负数
     * @param leaderEpoch 可选的领导者纪元信息
     * @throws IllegalArgumentException 当偏移量或时间戳为负数时抛出异常
     */
    public OffsetAndTimestamp(long offset, long timestamp, Optional<Integer> leaderEpoch) {
        // 验证偏移量是否为非负数
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");

        // 验证时间戳是否为非负数
        if (timestamp < 0)
            throw new IllegalArgumentException("Invalid negative timestamp");

        // 初始化字段
        this.offset = offset;
        this.timestamp = timestamp;
        this.leaderEpoch = leaderEpoch;
    }

    /**
     * 获取消息的时间戳
     * 
     * @return 返回消息的时间戳，可能是消息的创建时间或追加到日志的时间
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * 获取消息在分区中的偏移量
     * 
     * @return 返回消息的偏移量位置
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取与找到的偏移量对应的领导者纪元（如果存在）。
     * 领导者纪元是Kafka用于跟踪分区领导者变更的单调递增的数字。
     * 在执行seek()操作时可以提供此值，以确保在获取消息之前日志没有被截断。
     * 当发生领导者切换时，新的领导者可能会截断旧的日志条目，使用领导者纪元可以检测这种情况。
     *
     * @return 返回领导者纪元，如果未知则返回空
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * 将对象转换为字符串表示形式
     * 
     * @return 返回包含时间戳、领导者纪元和偏移量的字符串
     */
    @Override
    public String toString() {
        return "(timestamp=" + timestamp +
                ", leaderEpoch=" + leaderEpoch.orElse(null) +
                ", offset=" + offset + ")";
    }

    /**
     * 比较两个OffsetAndTimestamp对象是否相等
     * 当两个对象的时间戳、偏移量和领导者纪元都相等时，认为这两个对象相等
     * 
     * @param o 要比较的对象
     * @return 如果对象相等返回true，否则返回false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetAndTimestamp that = (OffsetAndTimestamp) o;
        return timestamp == that.timestamp &&
                offset == that.offset &&
                Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    /**
     * 计算对象的哈希码
     * 使用时间戳、偏移量和领导者纪元计算哈希值
     * 
     * @return 返回对象的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(timestamp, offset, leaderEpoch);
    }
}
