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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import scala.Option;
import scala.jdk.javaapi.OptionConverters;

/**
 * 远程日志偏移量读取器，用于根据时间戳查找消息偏移量
 * 该类实现了Callable接口，可以异步执行偏移量查找操作
 * 查找策略：优先从远程存储查找，如果未找到则回退到本地存储查找
 */
public class RemoteLogOffsetReader implements Callable<Void> {
    // 日志记录器实例
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogOffsetReader.class);
    // 远程日志管理器，用于管理远程存储的日志操作
    private final RemoteLogManager rlm;
    // 主题分区信息
    private final TopicPartition tp;
    // 要查找的目标时间戳
    private final long timestamp;
    // 开始查找的起始偏移量
    private final long startingOffset;
    // leader epoch文件缓存，用于跟踪分区leader的变更历史
    private final LeaderEpochFileCache leaderEpochCache;
    // 在本地日志中搜索偏移量的函数式接口
    private final Supplier<Optional<FileRecords.TimestampAndOffset>> searchInLocalLog;
    // 处理查找结果的回调函数
    private final Consumer<OffsetResultHolder.FileRecordsOrError> callback;

    /**
     * 构造远程日志偏移量读取器
     * @param rlm 远程日志管理器实例
     * @param tp 目标主题分区
     * @param timestamp 要查找的时间戳
     * @param startingOffset 开始查找的起始偏移量
     * @param leaderEpochCache leader epoch缓存，用于跟踪分区leader变更
     * @param searchInLocalLog 在本地日志中搜索的函数
     * @param callback 处理查找结果的回调函数
     */
    public RemoteLogOffsetReader(RemoteLogManager rlm,
                                 TopicPartition tp,
                                 long timestamp,
                                 long startingOffset,
                                 LeaderEpochFileCache leaderEpochCache,
                                 Supplier<Option<FileRecords.TimestampAndOffset>> searchInLocalLog,
                                 Consumer<OffsetResultHolder.FileRecordsOrError> callback) {
        // 初始化所有成员变量
        this.rlm = rlm;
        this.tp = tp;
        this.timestamp = timestamp;
        this.startingOffset = startingOffset;
        this.leaderEpochCache = leaderEpochCache;
        // 将Scala的Option转换为Java的Optional
        this.searchInLocalLog = () -> OptionConverters.toJava(searchInLocalLog.get());
        this.callback = callback;
    }

    /**
     * 执行偏移量查找操作
     * 实现Callable接口的call方法，支持异步执行
     * @return 返回null，实际结果通过回调函数传递
     * @throws Exception 执行过程中可能抛出的异常
     */
    @Override
    public Void call() throws Exception {
        OffsetResultHolder.FileRecordsOrError result;
        try {
            // 首先尝试在远程存储中查找，如果未找到则回退到本地存储查找
            // 使用startingOffset作为查找的起始位置
            Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt = 
                    rlm.findOffsetByTimestamp(tp, timestamp, startingOffset, leaderEpochCache).or(searchInLocalLog);
            // 创建查找结果，如果成功找到则不包含异常信息
            result = new OffsetResultHolder.FileRecordsOrError(Optional.empty(), timestampAndOffsetOpt);
        } catch (Exception e) {
            // 捕获所有类型的异常，不仅限于KafkaException
            // 这样可以处理各种可能的存储系统异常
            LOGGER.error("Error occurred while reading the remote log offset for {}", tp, e);
            // 创建包含异常信息的结果对象
            result = new OffsetResultHolder.FileRecordsOrError(Optional.of(e), Optional.empty());
        }
        // 通过回调函数返回查找结果
        callback.accept(result);
        return null;
    }
}
