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
package kafka.log.remote;

import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManager;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * 远程日志读取器，用于从远程存储中异步读取日志数据
 * 该类实现了Callable接口，支持异步执行，通过回调函数返回读取结果
 * 主要功能：
 * 1. 从远程存储读取指定主题分区的日志数据
 * 2. 收集读取操作的性能指标
 * 3. 处理读取过程中的异常情况
 * 4. 管理远程读取的配额
 */
public class RemoteLogReader implements Callable<Void> {
    // 日志记录器实例
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogReader.class);
    // 远程存储获取信息，包含要读取的主题分区和偏移量范围
    private final RemoteStorageFetchInfo fetchInfo;
    // 远程日志管理器，负责实际的远程存储读取操作
    private final RemoteLogManager rlm;
    // broker主题统计信息，用于收集性能指标
    private final BrokerTopicStats brokerTopicStats;
    // 处理读取结果的回调函数
    private final Consumer<RemoteLogReadResult> callback;
    // 远程日志管理配额管理器，用于控制读取速率
    private final RLMQuotaManager quotaManager;
    // 远程读取操作的计时器，用于监控读取性能
    private final Timer remoteReadTimer;

    /**
     * 构造远程日志读取器
     * @param fetchInfo 远程存储获取信息，指定要读取的数据范围
     * @param rlm 远程日志管理器实例
     * @param callback 处理读取结果的回调函数
     * @param brokerTopicStats broker主题统计信息收集器
     * @param quotaManager 远程日志管理配额管理器
     * @param remoteReadTimer 远程读取操作计时器
     */
    public RemoteLogReader(RemoteStorageFetchInfo fetchInfo,
                           RemoteLogManager rlm,
                           Consumer<RemoteLogReadResult> callback,
                           BrokerTopicStats brokerTopicStats,
                           RLMQuotaManager quotaManager,
                           Timer remoteReadTimer) {
        // 初始化成员变量
        this.fetchInfo = fetchInfo;
        this.rlm = rlm;
        this.brokerTopicStats = brokerTopicStats;
        this.callback = callback;
        // 记录远程获取请求的速率指标
        this.brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).remoteFetchRequestRate().mark();
        this.brokerTopicStats.allTopicsStats().remoteFetchRequestRate().mark();
        this.quotaManager = quotaManager;
        this.remoteReadTimer = remoteReadTimer;
    }

    /**
     * 执行远程日志读取操作
     * 实现Callable接口的call方法，支持异步执行
     * @return 返回null，实际结果通过回调函数传递
     */
    @Override
    public Void call() {
        RemoteLogReadResult result;
        try {
            // 记录开始读取的调试日志
            LOGGER.debug("Reading records from remote storage for topic partition {}", fetchInfo.topicPartition);
            // 使用计时器记录读取操作的执行时间，并从远程存储读取数据
            FetchDataInfo fetchDataInfo = remoteReadTimer.time(() -> rlm.read(fetchInfo));
            // 记录远程获取的字节数指标
            brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).remoteFetchBytesRate().mark(fetchDataInfo.records.sizeInBytes());
            brokerTopicStats.allTopicsStats().remoteFetchBytesRate().mark(fetchDataInfo.records.sizeInBytes());
            // 创建成功的读取结果
            result = new RemoteLogReadResult(Optional.of(fetchDataInfo), Optional.empty());
        } catch (OffsetOutOfRangeException e) {
            // 处理偏移量超出范围的异常
            result = new RemoteLogReadResult(Optional.empty(), Optional.of(e));
        } catch (Exception e) {
            // 处理其他异常，记录失败指标
            brokerTopicStats.topicStats(fetchInfo.topicPartition.topic()).failedRemoteFetchRequestRate().mark();
            brokerTopicStats.allTopicsStats().failedRemoteFetchRequestRate().mark();
            LOGGER.error("Error occurred while reading the remote data for {}", fetchInfo.topicPartition, e);
            result = new RemoteLogReadResult(Optional.empty(), Optional.of(e));
        }
        // 记录完成读取的调试日志
        LOGGER.debug("Finished reading records from remote storage for topic partition {}", fetchInfo.topicPartition);
        // 记录读取的数据量到配额管理器
        quotaManager.record(result.fetchDataInfo.map(fetchDataInfo -> fetchDataInfo.records.sizeInBytes()).orElse(0));
        // 通过回调函数返回结果
        callback.accept(result);
        return null;
    }
}
