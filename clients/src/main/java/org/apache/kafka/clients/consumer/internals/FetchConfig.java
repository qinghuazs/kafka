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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredIsolationLevel;

/**
 * FetchConfig表示从Kafka获取记录的静态配置
 * 这是一种将在创建{@link Consumer}时提供的不可变设置打包在一起的方式
 * 供后续的类如{@link Fetcher}, {@link CompletedFetch}等使用
 */
public class FetchConfig {

    /**
     * 获取操作的最小字节数
     * 服务器在响应获取请求之前应该累积的最小数据量
     */
    public final int minBytes;

    /**
     * 获取操作的最大字节数
     * 服务器在响应获取请求时应返回的最大数据量
     */
    public final int maxBytes;

    /**
     * 获取操作的最大等待时间（毫秒）
     * 如果没有足够的数据满足minBytes，服务器在响应获取请求前将等待的最长时间
     */
    public final int maxWaitMs;

    /**
     * 每个分区的获取大小
     * 服务器应该为每个分区返回的最大数据量
     */
    public final int fetchSize;

    /**
     * 单次poll调用返回的最大记录数
     * 限制每次poll()操作返回的记录数量
     */
    public final int maxPollRecords;

    /**
     * 是否检查CRC校验和
     * 用于验证消息的完整性
     */
    public final boolean checkCrcs;

    /**
     * 客户端机架ID
     * 用于机架感知的分区分配
     */
    public final String clientRackId;

    /**
     * 隔离级别
     * 定义读取消息的隔离级别
     */
    public final IsolationLevel isolationLevel;

    /**
     * 构造函数
     * 使用显式提供的值创建新的FetchConfig实例
     * 这主要用于测试场景，可以构造特定的配置值而不需要构建完整的ConsumerConfig
     *
     * @param minBytes 最小字节数
     * @param maxBytes 最大字节数
     * @param maxWaitMs 最大等待时间
     * @param fetchSize 获取大小
     * @param maxPollRecords 最大poll记录数
     * @param checkCrcs 是否检查CRC
     * @param clientRackId 客户端机架ID
     * @param isolationLevel 隔离级别
     */
    public FetchConfig(int minBytes,
                       int maxBytes,
                       int maxWaitMs,
                       int fetchSize,
                       int maxPollRecords,
                       boolean checkCrcs,
                       String clientRackId,
                       IsolationLevel isolationLevel) {
        // 初始化最小字节数
        this.minBytes = minBytes;
        // 初始化最大字节数
        this.maxBytes = maxBytes;
        // 初始化最大等待时间
        this.maxWaitMs = maxWaitMs;
        // 初始化获取大小
        this.fetchSize = fetchSize;
        // 初始化最大poll记录数
        this.maxPollRecords = maxPollRecords;
        // 初始化CRC检查标志
        this.checkCrcs = checkCrcs;
        // 初始化客户端机架ID
        this.clientRackId = clientRackId;
        // 初始化隔离级别
        this.isolationLevel = isolationLevel;
    }

    /**
     * 使用ConsumerConfig构造FetchConfig实例
     * 从消费者配置中提取所有必要的配置值
     *
     * @param config 消费者配置对象
     */
    public FetchConfig(ConsumerConfig config) {
        // 从配置中获取最小字节数
        this.minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        // 从配置中获取最大字节数
        this.maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        // 从配置中获取最大等待时间
        this.maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        // 从配置中获取每个分区的获取大小
        this.fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        // 从配置中获取最大poll记录数
        this.maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        // 从配置中获取是否检查CRC
        this.checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG);
        // 从配置中获取客户端机架ID
        this.clientRackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG);
        // 从配置中获取并配置隔离级别
        this.isolationLevel = configuredIsolationLevel(config);
    }

    @Override
    public String toString() {
        return "FetchConfig{" +
                "minBytes=" + minBytes +
                ", maxBytes=" + maxBytes +
                ", maxWaitMs=" + maxWaitMs +
                ", fetchSize=" + fetchSize +
                ", maxPollRecords=" + maxPollRecords +
                ", checkCrcs=" + checkCrcs +
                ", clientRackId='" + clientRackId + '\'' +
                ", isolationLevel=" + isolationLevel +
                '}';
    }
}
