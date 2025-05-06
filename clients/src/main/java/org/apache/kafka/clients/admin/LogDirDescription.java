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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;

import java.util.Map;
import java.util.OptionalLong;

import static java.util.Collections.unmodifiableMap;
import static org.apache.kafka.common.requests.DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES;

/**
 * 描述Kafka broker上特定日志目录的信息。
 * 
 * 应用场景：
 * 1. 用于监控和管理Kafka broker的日志存储状态
 * 2. 帮助诊断日志目录的问题，如存储空间不足或目录离线
 * 3. 提供日志目录中分区副本的分布信息
 */
public class LogDirDescription {
    /**
     * 存储日志目录中的分区副本信息映射表
     * key: 主题分区标识（TopicPartition）
     * value: 对应的副本信息（ReplicaInfo）
     */
    private final Map<TopicPartition, ReplicaInfo> replicaInfos;

    /**
     * 日志目录的错误状态
     * - 如果目录正常，则为null
     * - 如果目录出现问题（如离线），则包含具体错误信息
     */
    private final ApiException error;

    /**
     * 日志目录所在磁盘卷的总容量（字节）
     * - 如果broker未返回该值，则为空
     * - 如果容量超过Long.MAX_VALUE，则返回Long.MAX_VALUE
     */
    private final OptionalLong totalBytes;

    /**
     * 日志目录所在磁盘卷的可用容量（字节）
     * - 如果broker未返回该值，则为空
     * - 如果可用容量超过Long.MAX_VALUE，则返回Long.MAX_VALUE
     */
    private final OptionalLong usableBytes;

    /**
     * 构造函数 - 创建日志目录描述对象（不包含存储容量信息）
     * 
     * @param error 目录的错误状态
     * @param replicaInfos 目录中的副本信息映射
     */
    public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
        // 调用完整构造函数，使用未知值表示存储容量
        this(error, replicaInfos, UNKNOWN_VOLUME_BYTES, UNKNOWN_VOLUME_BYTES);
    }

    /**
     * 构造函数 - 创建完整的日志目录描述对象
     * 
     * @param error 目录的错误状态
     * @param replicaInfos 目录中的副本信息映射
     * @param totalBytes 目录所在磁盘卷的总容量
     * @param usableBytes 目录所在磁盘卷的可用容量
     */
    public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos, long totalBytes, long usableBytes) {
        this.error = error;
        this.replicaInfos = replicaInfos;
        // 如果容量值未知，返回空Optional，否则包装实际值
        this.totalBytes = (totalBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(totalBytes);
        this.usableBytes = (usableBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(usableBytes);
    }

    /**
     * 获取日志目录的错误状态
     * 
     * @return 如果日志目录离线或发生错误，返回对应的ApiException；如果目录正常，返回null
     * 
     * 可能的错误类型：
     * <ul>
     * <li> KafkaStorageException - 日志目录处于离线状态
     * <li> UnknownServerException - 服务器处理请求时发生未知错误
     * </ul>
     */
    public ApiException error() {
        return error;
    }

    /**
     * 获取日志目录中的分区副本信息映射
     * 
     * @return 返回一个不可修改的Map，包含目录中所有分区副本的信息
     *         key: 主题分区（TopicPartition）
     *         value: 对应的副本信息（ReplicaInfo）
     */
    public Map<TopicPartition, ReplicaInfo> replicaInfos() {
        return unmodifiableMap(replicaInfos);
    }

    /**
     * 获取日志目录所在磁盘卷的总容量
     * 
     * @return 返回磁盘卷总容量的Optional包装
     *         - 如果broker未返回该值，则返回空Optional
     *         - 如果总容量超过Long.MAX_VALUE，则返回Long.MAX_VALUE
     */
    public OptionalLong totalBytes() {
        return totalBytes;
    }

    /**
     * 获取日志目录所在磁盘卷的可用容量
     * 
     * @return 返回磁盘卷可用容量的Optional包装
     *         - 如果broker未返回该值，则返回空Optional
     *         - 如果可用容量超过Long.MAX_VALUE，则返回Long.MAX_VALUE
     */
    public OptionalLong usableBytes() {
        return usableBytes;
    }

    @Override
    public String toString() {
        return "LogDirDescription(" +
                "replicaInfos=" + replicaInfos +
                ", error=" + error +
                ", totalBytes=" + totalBytes +
                ", usableBytes=" + usableBytes +
                ')';
    }
}
