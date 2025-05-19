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
package kafka.server.share;

import kafka.cluster.Partition;
import kafka.server.LogReadResult;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.purgatory.DelayedOperation;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.PartitionMaxBytesStrategy;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

/**
 * 延迟共享获取操作类，用于处理无法立即完成的共享获取请求。
 * 该类实现了Kafka中的延迟操作机制，主要用于处理需要等待一定时间才能完成的共享分区数据获取请求。
 */
public class DelayedShareFetch extends DelayedOperation {

    private static final Logger log = LoggerFactory.getLogger(DelayedShareFetch.class);

    // 共享获取请求的参数和状态信息
    private final ShareFetch shareFetch;
    // 副本管理器，用于读取日志和完成请求
    private final ReplicaManager replicaManager;
    // 异常处理器，用于处理共享获取请求过程中的异常
    private final BiConsumer<SharePartitionKey, Throwable> exceptionHandler;
    // 分区最大字节策略，用于控制每个分区的最大获取字节数
    private final PartitionMaxBytesStrategy partitionMaxBytesStrategy;
    // 需要完成的主题分区集合，是shareFetchData的子集
    // 注意：分区的插入/删除顺序很重要，因此使用LinkedHashMap保持顺序
    private final LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions;
    // 已获取到锁的分区集合，键为分区ID，值为获取偏移量
    private LinkedHashMap<TopicIdPartition, Long> partitionsAcquired;
    // 已经从日志中读取的分区数据，键为分区ID，值为读取结果
    private LinkedHashMap<TopicIdPartition, LogReadResult> partitionsAlreadyFetched;

    /**
     * 构造延迟共享获取操作实例，用于立即或延迟完成共享获取请求。
     * 
     * @param shareFetch - 共享获取请求的参数信息
     * @param replicaManager - 用于读取日志和完成请求的副本管理器实例
     * @param exceptionHandler - 处理共享获取请求异常的处理器
     * @param sharePartitions - 请求中引用的共享分区集合
     */
    public DelayedShareFetch(
            ShareFetch shareFetch,
            ReplicaManager replicaManager,
            BiConsumer<SharePartitionKey, Throwable> exceptionHandler,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions) {
        // 使用统一分区字节策略初始化
        this(shareFetch, replicaManager, exceptionHandler, sharePartitions, PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM));
    }

    /**
     * 构造延迟共享获取操作实例的内部方法，支持自定义分区字节策略。
     * 
     * @param shareFetch - 共享获取请求的参数信息
     * @param replicaManager - 副本管理器实例
     * @param exceptionHandler - 异常处理器
     * @param sharePartitions - 共享分区集合
     * @param partitionMaxBytesStrategy - 自定义的分区最大字节策略
     */
    DelayedShareFetch(
        ShareFetch shareFetch,
        ReplicaManager replicaManager,
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler,
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions,
        PartitionMaxBytesStrategy partitionMaxBytesStrategy) {
        // 调用父类构造函数，设置最大等待时间
        super(shareFetch.fetchParams().maxWaitMs, Optional.empty());
        // 初始化成员变量
        this.shareFetch = shareFetch;
        this.replicaManager = replicaManager;
        this.partitionsAcquired = new LinkedHashMap<>();  // 存储已获取锁的分区
        this.partitionsAlreadyFetched = new LinkedHashMap<>();  // 存储已读取的分区数据
        this.exceptionHandler = exceptionHandler;
        this.sharePartitions = sharePartitions;
        this.partitionMaxBytesStrategy = partitionMaxBytesStrategy;
    }

    @Override
    public void onExpiration() {
        // 过期处理，当前实现为空
    }

    /**
     * 完成共享获取操作，获取所有分区的记录，无论是否已经获取到记录。
     * 该方法在以下两种情况下被调用：
     * 1. 某些分区可以获取到记录
     * 2. 达到最大等待时间(MaxWaitMs)超时
     */
    @Override
    public void onComplete() {
        // 使用锁来防止对实例变量(partitionsAcquired和partitionsAlreadyFetched)的脏读
        // 因为这些变量可能在其他tryComplete线程中被更新
        lock.lock();
        log.trace("正在完成延迟共享获取请求，组：{}，成员：{}，主题分区：{}", 
            shareFetch.groupId(), shareFetch.memberId(),
            partitionsAcquired.keySet());

        try {
            LinkedHashMap<TopicIdPartition, Long> topicPartitionData;
            // 如果tryComplete没有调用forceComplete，需要检查是否有可获取的分区
            if (partitionsAcquired.isEmpty())
                topicPartitionData = acquirablePartitions();
            // 如果tryComplete调用了forceComplete，直接使用已获取的数据
            else
                topicPartitionData = partitionsAcquired;

            if (topicPartitionData.isEmpty()) {
                // 如果没有获取到任何分区的锁，使用空响应完成请求
                shareFetch.maybeComplete(Collections.emptyMap());
                return;
            }
            log.trace("可获取的共享分区数据：{}，组ID：{}，获取参数：{}",
                topicPartitionData, shareFetch.groupId(), shareFetch.fetchParams());

            // 完成共享获取请求的处理
            completeShareFetchRequest(topicPartitionData);
        } finally {
            // 释放锁，允许其他线程处理
            lock.unlock();
        }
    }

    /**
     * 完成共享获取请求的具体处理逻辑。
     * 该方法负责从日志中读取数据，处理响应，并确保正确释放资源。
     *
     * @param topicPartitionData 待处理的主题分区数据，键为分区ID，值为获取偏移量
     */
    private void completeShareFetchRequest(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        try {
            // 存储从日志读取的响应数据
            LinkedHashMap<TopicIdPartition, LogReadResult> responseData;
            if (partitionsAlreadyFetched.isEmpty()) {
                // 如果没有预先获取的数据，从日志中读取
                responseData = readFromLog(
                    topicPartitionData,
                    // 根据策略计算每个分区的最大字节数
                    partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, topicPartitionData.keySet(), topicPartitionData.size()));
            } else {
                // 使用已经获取的数据
                // 注意：此时partitionsAlreadyFetched不应该被其他tryComplete线程更新
                responseData = combineLogReadResponse(topicPartitionData, partitionsAlreadyFetched);
            }

            // 将日志读取结果转换为获取分区数据格式
            LinkedHashMap<TopicIdPartition, FetchPartitionData> fetchPartitionsData = new LinkedHashMap<>();
            for (Map.Entry<TopicIdPartition, LogReadResult> entry : responseData.entrySet())
                fetchPartitionsData.put(entry.getKey(), entry.getValue().toFetchPartitionData(false));

            // 处理获取响应并尝试完成请求
            shareFetch.maybeComplete(ShareFetchUtils.processFetchResponse(shareFetch, fetchPartitionsData,
                sharePartitions, replicaManager, exceptionHandler));
        } catch (Exception e) {
            // 记录错误并处理异常
            log.error("处理延迟共享获取请求时发生错误", e);
            handleFetchException(shareFetch, topicPartitionData.keySet(), e);
        } finally {
            // 释放分区锁，允许处理队列中的下一个请求
            releasePartitionLocks(topicPartitionData.keySet());
            
            // 将检查和完成待处理的共享获取请求的操作添加到延迟动作队列
            // 这样可以避免直接调用delayedShareFetchPurgatory.checkAndComplete可能导致的无限调用栈
            replicaManager.addToActionQueue(() -> topicPartitionData.keySet().forEach(topicIdPartition ->
                replicaManager.completeDelayedShareFetchRequest(
                    new DelayedShareFetchGroupKey(shareFetch.groupId(), topicIdPartition.topicId(), topicIdPartition.partition()))));
        }
    }

    /**
     * 尝试完成共享获取操作，如果能够为共享获取请求中的任何分区获取记录则完成操作。
     * 该方法实现了DelayedOperation的tryComplete接口，是延迟操作完成逻辑的核心。
     * 
     * 实现策略：
     * 1. 首先尝试获取可用分区的数据
     * 2. 检查并更新分区的偏移量元数据
     * 3. 根据读取结果和最小字节数要求决定是否完成操作
     * 4. 确保在异常情况下正确释放资源
     * 
     * @return 如果操作被成功完成返回true，否则返回false
     */
    @Override
    public boolean tryComplete() {
        // 获取当前可以获取记录的分区列表
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = acquirablePartitions();

        try {
            if (!topicPartitionData.isEmpty()) {
                // 对于一个或多个主题分区，如果获取偏移量元数据不存在，
                // 我们需要通过replicaManager.readFromLog来填充偏移量元数据
                // 并更新这些主题分区的获取偏移量元数据
                LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse = maybeReadFromLog(topicPartitionData);
                maybeUpdateFetchOffsetMetadata(topicPartitionData, replicaManagerReadResponse);
                
                // 检查是否有分区存在日志读取错误，或者是否满足最小字节数要求
                if (anyPartitionHasLogReadError(replicaManagerReadResponse) || 
                    isMinBytesSatisfied(topicPartitionData, partitionMaxBytesStrategy.maxBytes(
                        shareFetch.fetchParams().maxBytes, 
                        topicPartitionData.keySet(), 
                        topicPartitionData.size()))) {
                    // 更新已获取的分区和已读取的数据
                    partitionsAcquired = topicPartitionData;
                    partitionsAlreadyFetched = replicaManagerReadResponse;
                    // 强制完成操作
                    boolean completedByMe = forceComplete();
                    // 如果forceComplete调用不成功，说明请求已经被其他线程完成
                    // 因此需要释放已获取的锁
                    if (!completedByMe) {
                        releasePartitionLocks(partitionsAcquired.keySet());
                    }
                    return completedByMe;
                } else {
                    // 如果不满足最小字节数要求，记录日志并释放锁
                    log.debug("共享获取请求未满足最小字节数要求，组：{}，成员：{}，主题分区：{}", 
                        shareFetch.groupId(), shareFetch.memberId(),
                        sharePartitions.keySet());
                    releasePartitionLocks(topicPartitionData.keySet());
                }
            } else {
                // 如果没有可获取记录的分区，记录跟踪日志
                log.trace("无法为共享获取请求获取任何分区的记录，组：{}，成员：{}，主题分区：{}", 
                    shareFetch.groupId(), shareFetch.memberId(),
                    sharePartitions.keySet());
            }
            return false;
        } catch (Exception e) {
            // 发生异常时，记录错误日志，清理状态并释放资源
            log.error("处理延迟共享获取请求时发生错误", e);
            partitionsAcquired.clear();
            partitionsAlreadyFetched.clear();
            releasePartitionLocks(topicPartitionData.keySet());
            return forceComplete();
        }
    }

    /**
     * 准备可以获取记录的共享获取请求分区的获取请求结构。
     * 该方法遍历所有共享分区，尝试获取分区锁并检查是否可以获取更多记录。
     * 
     * 实现策略：
     * 1. 遍历所有共享分区
     * 2. 尝试获取分区的获取锁
     * 3. 检查分区是否可以获取更多记录
     * 4. 确保在异常情况下释放锁资源
     * 
     * @return 返回可以获取记录的分区映射，键为主题分区ID，值为下一个获取偏移量
     */
    // 用于测试可见
    LinkedHashMap<TopicIdPartition, Long> acquirablePartitions() {
        // 初始化将要尝试获取的主题分区集合
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = new LinkedHashMap<>();

        // 遍历所有共享分区
        sharePartitions.forEach((topicIdPartition, sharePartition) -> {
            // 只有在能够获取到分区的获取锁时，才将共享分区添加到待获取列表中
            if (sharePartition.maybeAcquireFetchLock()) {
                try {
                    // 如果共享分区已经达到容量限制，不应该尝试获取
                    if (sharePartition.canAcquireRecords()) {
                        // 添加可以获取的分区及其下一个获取偏移量
                        topicPartitionData.put(topicIdPartition, sharePartition.nextFetchOffset());
                    } else {
                        // 如果达到容量限制，释放锁并记录日志
                        sharePartition.releaseFetchLock();
                        log.trace("共享分区{}已达到记录锁定限制，无法获取更多记录", sharePartition);
                    }
                } catch (Exception e) {
                    // 发生异常时记录错误并释放锁
                    log.error("检查共享分区{}条件时发生错误", sharePartition, e);
                    sharePartition.releaseFetchLock();
                }
            }
        });
        return topicPartitionData;
    }

    /**
     * 尝试从日志中读取数据，用于处理缺失获取偏移量元数据的分区。
     * 该方法检查每个分区的偏移量元数据，如果缺失则从日志中读取。
     * 
     * 实现策略：
     * 1. 识别缺失偏移量元数据的分区
     * 2. 使用副本管理器读取这些分区的日志数据
     * 3. 使用合适的分区最大字节数策略
     * 
     * @param topicPartitionData 主题分区数据映射
     * @return 返回从日志中读取的结果映射
     */
    private LinkedHashMap<TopicIdPartition, LogReadResult> maybeReadFromLog(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        // 创建不匹配获取偏移量元数据的分区集合
        LinkedHashMap<TopicIdPartition, Long> partitionsNotMatchingFetchOffsetMetadata = new LinkedHashMap<>();
        
        // 遍历所有分区，检查偏移量元数据
        topicPartitionData.forEach((topicIdPartition, fetchOffset) -> {
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            // 如果分区的获取偏移量元数据为空，添加到待读取列表
            if (sharePartition.fetchOffsetMetadata(fetchOffset).isEmpty()) {
                partitionsNotMatchingFetchOffsetMetadata.put(topicIdPartition, fetchOffset);
            }
        });
        
        // 如果没有需要读取的分区，返回空映射
        if (partitionsNotMatchingFetchOffsetMetadata.isEmpty()) {
            return new LinkedHashMap<>();
        }
        
        // 从副本管理器读取缺失获取偏移量元数据的主题分区对应的数据
        // 虽然我们为partitionsNotMatchingFetchOffsetMetadata获取分区最大字节数，
        // 但我们使用topicPartitionData.size()作为已获取分区的大小，
        // 这是为了避免剩余将在稍后获取的分区出现饥饿现象
        return readFromLog(
            partitionsNotMatchingFetchOffsetMetadata,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, 
                partitionsNotMatchingFetchOffsetMetadata.keySet(), 
                topicPartitionData.size()));
    }

    /**
     * 更新分区的获取偏移量元数据。
     * 该方法处理从副本管理器读取的响应，更新相应分区的偏移量元数据。
     * 
     * 实现策略：
     * 1. 遍历副本管理器的读取响应
     * 2. 检查每个分区的读取结果是否有错误
     * 3. 更新成功读取的分区的偏移量元数据
     * 
     * @param topicPartitionData 主题分区数据映射
     * @param replicaManagerReadResponseData 副本管理器的读取响应数据
     */
    private void maybeUpdateFetchOffsetMetadata(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                                LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponseData) {
        // 遍历副本管理器的读取响应
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : replicaManagerReadResponseData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            LogReadResult replicaManagerLogReadResult = entry.getValue();
            
            // 如果读取结果有错误，记录日志并继续处理下一个分区
            if (replicaManagerLogReadResult.error().code() != Errors.NONE.code()) {
                log.debug("副本管理器读取日志结果{}对于主题分区{}发生错误",
                    replicaManagerLogReadResult, topicIdPartition);
                continue;
            }
            
            // 使用读取结果更新分区的获取偏移量元数据
            sharePartition.updateFetchOffsetMetadata(
                topicPartitionData.get(topicIdPartition),
                replicaManagerLogReadResult.info().fetchOffsetMetadata);
        }
    }

    /**
     * 检查是否满足最小字节数要求的方法。
     * 该方法遍历所有主题分区，计算可获取的数据字节数，并与请求的最小字节数进行比较。
     * 在以下情况下会立即返回true：
     * 1. 当获取偏移量大于结束偏移量时（表示正在获取更新的数据段）
     * 2. 当获取偏移量在较旧的数据段时（表示获取操作落后或分区刚刚滚动了新的数据段）
     * 
     * @param topicPartitionData 主题分区数据映射，键为分区ID，值为获取偏移量
     * @param partitionMaxBytes 分区最大字节数映射，键为分区ID，值为最大字节数
     * @return 如果满足最小字节数要求或满足立即返回条件则返回true，否则返回false
     */
    private boolean isMinBytesSatisfied(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {
        // 累计可获取的数据字节数
        long accumulatedSize = 0;
        // 遍历所有主题分区
        for (Map.Entry<TopicIdPartition, Long> entry : topicPartitionData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            long fetchOffset = entry.getValue();

            // 获取分区的结束偏移量元数据
            LogOffsetMetadata endOffsetMetadata;
            try {
                endOffsetMetadata = endOffsetMetadataForTopicPartition(topicIdPartition);
            } catch (Exception e) {
                // 处理异常：将分区标记为错误并通知异常处理器
                shareFetch.addErroneous(topicIdPartition, e);
                exceptionHandler.accept(
                    new SharePartitionKey(shareFetch.groupId(), topicIdPartition), e);
                continue;
            }

            // 如果结束偏移量未知，跳过该分区
            if (endOffsetMetadata == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;

            // 获取共享分区实例
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);

            // 获取获取偏移量的元数据
            Optional<LogOffsetMetadata> optionalFetchOffsetMetadata = sharePartition.fetchOffsetMetadata(fetchOffset);
            if (optionalFetchOffsetMetadata.isEmpty() || optionalFetchOffsetMetadata.get() == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;
            LogOffsetMetadata fetchOffsetMetadata = optionalFetchOffsetMetadata.get();

            // 比较获取偏移量和结束偏移量
            if (fetchOffsetMetadata.messageOffset > endOffsetMetadata.messageOffset) {
                // 如果获取偏移量大于结束偏移量，表示正在获取更新的数据段，立即返回true
                log.debug("正在满足延迟共享获取请求，组：{}，成员：{}，因为正在获取主题分区{}的更新数据段",
                    shareFetch.groupId(), shareFetch.memberId(), topicIdPartition);
                return true;
            } else if (fetchOffsetMetadata.messageOffset < endOffsetMetadata.messageOffset) {
                if (fetchOffsetMetadata.onOlderSegment(endOffsetMetadata)) {
                    // 如果获取偏移量在较旧的数据段，可能是因为获取操作落后或分区刚刚滚动了新的数据段
                    log.debug("立即满足延迟共享获取请求，组：{}，成员：{}，因为正在获取主题分区{}的较旧数据段",
                        shareFetch.groupId(), shareFetch.memberId(), topicIdPartition);
                    return true;
                } else if (fetchOffsetMetadata.onSameSegment(endOffsetMetadata)) {
                    // 如果在同一数据段，计算可用字节数（使用分区获取大小作为上限）
                    long bytesAvailable = Math.min(endOffsetMetadata.positionDiff(fetchOffsetMetadata), partitionMaxBytes.get(topicIdPartition));
                    accumulatedSize += bytesAvailable;
                }
            }
        }
        // 返回累计字节数是否满足最小字节数要求
        return accumulatedSize >= shareFetch.fetchParams().minBytes;
    }

    /**
     * 获取指定主题分区的结束偏移量元数据。
     * 该方法根据获取隔离级别返回不同类型的偏移量：
     * - LOG_END：返回日志结束偏移量
     * - HIGH_WATERMARK：返回高水位标记（默认隔离级别）
     * - 其他：返回最后稳定偏移量
     * 
     * @param topicIdPartition 主题分区ID
     * @return 返回对应隔离级别的偏移量元数据
     */
    private LogOffsetMetadata endOffsetMetadataForTopicPartition(TopicIdPartition topicIdPartition) {
        // 获取分区实例
        Partition partition = ShareFetchUtils.partition(replicaManager, topicIdPartition.topicPartition());
        // 获取分区的偏移量快照
        LogOffsetSnapshot offsetSnapshot = partition.fetchOffsetSnapshot(Optional.empty(), true);
        
        // 获取隔离级别（默认使用HIGH_WATERMARK，未来可能支持其他隔离级别）
        FetchIsolation isolationType = shareFetch.fetchParams().isolation;
        
        // 根据隔离级别返回相应的偏移量
        if (isolationType == FetchIsolation.LOG_END)
            return offsetSnapshot.logEndOffset;  // 返回日志结束偏移量
        else if (isolationType == FetchIsolation.HIGH_WATERMARK)
            return offsetSnapshot.highWatermark;  // 返回高水位标记
        else
            return offsetSnapshot.lastStableOffset;  // 返回最后稳定偏移量
    }

    /**
     * 从日志中读取指定主题分区的数据。
     * 该方法首先过滤掉错误的主题分区，然后构建获取请求数据，最后通过副本管理器读取日志数据。
     * 
     * @param topicPartitionFetchOffsets 主题分区获取偏移量映射，键为分区ID，值为获取偏移量
     * @param partitionMaxBytes 分区最大字节数映射，键为分区ID，值为最大字节数
     * @return 返回日志读取结果映射，键为分区ID，值为读取结果
     */
    private LinkedHashMap<TopicIdPartition, LogReadResult> readFromLog(LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
                                                                       LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {
        // 过滤掉已经存在错误的主题分区
        Set<TopicIdPartition> partitionsToFetch = shareFetch.filterErroneousTopicPartitions(topicPartitionFetchOffsets.keySet());
        if (partitionsToFetch.isEmpty()) {
            return new LinkedHashMap<>();
        }

        // 构建主题分区数据映射，用于获取请求
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();

        // 为每个主题分区创建获取请求数据
        topicPartitionFetchOffsets.forEach((topicIdPartition, fetchOffset) -> topicPartitionData.put(topicIdPartition,
            new FetchRequest.PartitionData(
                topicIdPartition.topicId(),  // 主题ID
                fetchOffset,  // 获取偏移量
                0,  // 日志起始偏移量（0表示使用当前值）
                partitionMaxBytes.get(topicIdPartition),  // 最大字节数
                Optional.empty())  // 额外参数（当前未使用）
        ));

        // 通过副本管理器读取日志数据
        Seq<Tuple2<TopicIdPartition, LogReadResult>> responseLogResult = replicaManager.readFromLog(
            shareFetch.fetchParams(),  // 获取参数
            CollectionConverters.asScala(  // 转换为Scala集合
                partitionsToFetch.stream().map(topicIdPartition ->
                    new Tuple2<>(topicIdPartition, topicPartitionData.get(topicIdPartition))).collect(Collectors.toList())
            ),
            QuotaFactory.UNBOUNDED_QUOTA,  // 使用无限配额
            true);  // 允许从本地副本读取

        // 将读取结果转换为Java映射
        LinkedHashMap<TopicIdPartition, LogReadResult> responseData = new LinkedHashMap<>();
        responseLogResult.foreach(tpLogResult -> {
            responseData.put(tpLogResult._1(), tpLogResult._2());
            return BoxedUnit.UNIT;
        });

        log.trace("副本管理器成功获取数据：{}", responseData);
        return responseData;
    }

    /**
     * 检查是否有任何分区存在日志读取错误。
     * 该方法遍历所有分区的读取结果，检查是否存在非NONE的错误代码。
     * 
     * @param replicaManagerReadResponse 副本管理器的读取响应映射
     * @return 如果存在任何错误则返回true，否则返回false
     */
    private boolean anyPartitionHasLogReadError(LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse) {
        // 使用流式处理检查是否有任何读取结果包含错误
        return replicaManagerReadResponse.values().stream()
            .anyMatch(logReadResult -> logReadResult.error().code() != Errors.NONE.code());
    }

    /**
     * 处理从日志读取过程中发生的异常。
     * 该方法会为请求中的每个主题分区处理异常。共享分区可能会从缓存中移除。
     * 
     * 副本读取请求可能在某个共享分区出错，但由于我们无法确定具体是哪个共享分区出错，
     * 因此可能需要移除请求中的所有共享分区。
     *
     * @param shareFetch 共享获取请求
     * @param topicIdPartitions 副本读取请求中的主题分区集合
     * @param throwable 获取消息时发生的异常
     */
    private void handleFetchException(
        ShareFetch shareFetch,
        Set<TopicIdPartition> topicIdPartitions,
        Throwable throwable
    ) {
        // 遍历所有主题分区，对每个分区调用异常处理器
        topicIdPartitions.forEach(topicIdPartition -> exceptionHandler.accept(
            new SharePartitionKey(shareFetch.groupId(), topicIdPartition), throwable));
        // 使用异常完成共享获取请求
        shareFetch.maybeCompleteWithException(topicIdPartitions, throwable);
    }

    /**
     * 合并日志读取响应，用于测试。
     * 该方法将已存在的获取数据与新读取的日志数据合并。
     *
     * @param topicPartitionData 主题分区数据映射，键为分区ID，值为获取偏移量
     * @param existingFetchedData 已获取的数据映射，键为分区ID，值为日志读取结果
     * @return 合并后的日志读取结果映射
     */
    LinkedHashMap<TopicIdPartition, LogReadResult> combineLogReadResponse(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                                                          LinkedHashMap<TopicIdPartition, LogReadResult> existingFetchedData) {
        // 创建缺失日志读取的主题分区集合
        LinkedHashMap<TopicIdPartition, Long> missingLogReadTopicPartitions = new LinkedHashMap<>();
        // 遍历主题分区数据，找出尚未获取数据的分区
        topicPartitionData.forEach((topicIdPartition, fetchOffset) -> {
            if (!existingFetchedData.containsKey(topicIdPartition)) {
                missingLogReadTopicPartitions.put(topicIdPartition, fetchOffset);
            }
        });
        // 如果没有缺失的分区，直接返回已有数据
        if (missingLogReadTopicPartitions.isEmpty()) {
            return existingFetchedData;
        }

        // 读取缺失分区的日志数据
        LinkedHashMap<TopicIdPartition, LogReadResult> missingTopicPartitionsLogReadResponse = readFromLog(
            missingLogReadTopicPartitions,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, missingLogReadTopicPartitions.keySet(), topicPartitionData.size()));
        // 合并新读取的数据和已有数据
        missingTopicPartitionsLogReadResponse.putAll(existingFetchedData);
        return missingTopicPartitionsLogReadResponse;
    }

    /**
     * 释放分区锁，用于测试。
     * 该方法遍历所有主题分区，释放它们的获取锁。
     *
     * @param topicIdPartitions 需要释放锁的主题分区集合
     */
    void releasePartitionLocks(Set<TopicIdPartition> topicIdPartitions) {
        // 遍历所有主题分区，释放每个分区的获取锁
        topicIdPartitions.forEach(tp -> {
            SharePartition sharePartition = sharePartitions.get(tp);
            sharePartition.releaseFetchLock();
        });
    }

    /**
     * 获取当前实例的锁对象，用于测试。
     *
     * @return 锁对象
     */
    Lock lock() {
        return lock;
    }
}
