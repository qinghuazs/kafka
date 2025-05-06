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
package org.apache.kafka.clients.admin;

import java.util.Map;

/** 
 * 此类用于在使用 {@link KafkaAdminClient#listOffsets(Map, ListOffsetsOptions)} 时指定所需的偏移量。
 * 它提供了多种偏移量查询规格，包括：
 * - 最早偏移量（earliest）：获取分区中最早的消息偏移量
 * - 最新偏移量（latest）：获取分区中最新的消息偏移量
 * - 时间戳偏移量（timestamp）：获取指定时间戳之后的第一个消息偏移量
 * - 最大时间戳偏移量（maxTimestamp）：获取具有最大时间戳的消息偏移量
 * - 本地最早偏移量（earliestLocal）：获取本地磁盘上可用的最早偏移量
 * - 远程存储最新偏移量（latestTiered）：获取远程存储中的最新偏移量
 */
public class OffsetSpec {

    /**
     * 用于获取分区中最早的消息偏移量的规格类
     */
    public static class EarliestSpec extends OffsetSpec { }

    /**
     * 用于获取分区中最新的消息偏移量的规格类
     */
    public static class LatestSpec extends OffsetSpec { }

    /**
     * 用于获取具有最大时间戳的消息偏移量的规格类
     */
    public static class MaxTimestampSpec extends OffsetSpec { }

    /**
     * 用于获取本地磁盘上可用的最早偏移量的规格类
     */
    public static class EarliestLocalSpec extends OffsetSpec { }

    /**
     * 用于获取远程存储中最新偏移量的规格类
     */
    public static class LatestTieredSpec extends OffsetSpec { }

    /**
     * 用于根据时间戳获取偏移量的规格类
     */
    public static class TimestampSpec extends OffsetSpec {
        /** 用于查询的目标时间戳（毫秒） */
        private final long timestamp;

        /**
         * 构造一个时间戳偏移量规格
         * @param timestamp 目标时间戳（毫秒）
         */
        TimestampSpec(long timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * 获取设置的时间戳值
         * @return 时间戳（毫秒）
         */
        long timestamp() {
            return timestamp;
        }
    }

    /**
     * 获取分区中最新消息的偏移量
     * 应用场景：当需要从最新的消息开始消费时使用
     * 实现细节：返回一个LatestSpec实例，用于查询分区的最新偏移量
     * @return OffsetSpec实例，用于获取最新偏移量
     */
    public static OffsetSpec latest() {
        return new LatestSpec();
    }

    /**
     * 获取分区中最早消息的偏移量
     * 应用场景：当需要从头开始消费分区中的所有消息时使用
     * 实现细节：返回一个EarliestSpec实例，用于查询分区的最早偏移量
     * @return OffsetSpec实例，用于获取最早偏移量
     */
    public static OffsetSpec earliest() {
        return new EarliestSpec();
    }

    /**
     * 获取大于或等于指定时间戳的第一个消息的偏移量
     * 应用场景：当需要从特定时间点开始消费消息时使用
     * 实现细节：返回一个TimestampSpec实例，包含用户指定的时间戳
     * @param timestamp 目标时间戳（毫秒）
     * @return OffsetSpec实例，用于获取指定时间戳的偏移量
     */
    public static OffsetSpec forTimestamp(long timestamp) {
        return new TimestampSpec(timestamp);
    }

    /**
     * 获取具有最大时间戳的消息的偏移量
     * 应用场景：当需要找到时间戳最大的消息时使用，注意这可能与latest()返回的偏移量不同，
     * 因为消息的时间戳是由客户端指定的
     * 实现细节：返回一个MaxTimestampSpec实例，用于查询具有最大时间戳的消息偏移量
     * @return OffsetSpec实例，用于获取最大时间戳的偏移量
     */
    public static OffsetSpec maxTimestamp() {
        return new MaxTimestampSpec();
    }

    /**
     * 获取本地日志的起始偏移量
     * 应用场景：当需要确保从leader broker的本地磁盘读取数据时使用
     * 实现细节：返回一个EarliestLocalSpec实例，用于查询本地磁盘上可用的最早偏移量
     * <br/>
     * 注意：当分层存储未启用时，其行为与获取最早偏移量相同
     * @return OffsetSpec实例，用于获取本地最早偏移量
     */
    public static OffsetSpec earliestLocal() {
        return new EarliestLocalSpec();
    }

    /**
     * 获取存储在远程存储中的最高偏移量
     * 应用场景：当使用分层存储功能，需要查询远程存储中的最新消息时使用
     * 实现细节：返回一个LatestTieredSpec实例，用于查询远程存储中的最新偏移量
     * <br/>
     * 注意：当分层存储未启用时，将返回未知偏移量
     * @return OffsetSpec实例，用于获取远程存储的最新偏移量
     */
    public static OffsetSpec latestTiered() {
        return new LatestTieredSpec();
    }
}
