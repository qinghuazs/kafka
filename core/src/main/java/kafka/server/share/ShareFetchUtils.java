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
import kafka.server.ReplicaManager;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.OffsetNotAvailableException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import scala.Option;
import scala.Some;

/**
 * 共享获取操作的后处理工具类。
 * 该类主要用于处理副本管理器的获取响应，并创建共享获取响应。
 * 应用场景：在Kafka的共享消费模式中，多个消费者可以共享同一分区的数据，
 * 这个类负责处理获取请求的响应，并确保数据被正确地分配给各个消费者。
 */
public class ShareFetchUtils {
    private static final Logger log = LoggerFactory.getLogger(ShareFetchUtils.class);

    /**
     * 处理副本管理器的获取响应，创建共享获取响应。
     * 该响应通过从共享分区获取记录来创建。
     *
     * @param shareFetch 共享获取请求对象，包含获取记录的相关参数
     * @param responseData 副本管理器的响应数据，包含每个主题分区的获取数据
     * @param sharePartitions 共享分区的映射，用于管理分区的共享状态
     * @param replicaManager 副本管理器，用于管理分区的副本
     * @param exceptionHandler 异常处理器，用于处理获取过程中的错误
     * @return 返回每个主题分区的获取响应数据
     */
    static Map<TopicIdPartition, ShareFetchResponseData.PartitionData> processFetchResponse(
            ShareFetch shareFetch,
            Map<TopicIdPartition, FetchPartitionData> responseData,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions,
            ReplicaManager replicaManager,
            BiConsumer<SharePartitionKey, Throwable> exceptionHandler) {
        // 创建响应Map，用于存储每个主题分区的获取响应数据
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> response = new HashMap<>();

        // 记录已获取的记录数量，用于控制获取的总记录数不超过最大限制
        int acquiredRecordsCount = 0;
        // 遍历每个主题分区的获取数据
        for (Map.Entry<TopicIdPartition, FetchPartitionData> entry : responseData.entrySet()) {
            // 获取主题分区标识符和对应的获取数据
            TopicIdPartition topicIdPartition = entry.getKey();
            FetchPartitionData fetchPartitionData = entry.getValue();

            // 获取对应的共享分区对象，用于管理分区的共享状态
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            // 创建分区响应数据对象，设置分区索引
            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(topicIdPartition.partition());

            if (fetchPartitionData.error.code() != Errors.NONE.code()) {
                partitionData
                    .setRecords(null)
                    .setErrorCode(fetchPartitionData.error.code())
                    .setErrorMessage(fetchPartitionData.error.message())
                    .setAcquiredRecords(Collections.emptyList());

                // 处理偏移量超出范围的错误
                // 这种情况发生在日志起始偏移量晚于获取偏移量时
                // 我们需要更新共享分区的起始和结束偏移量，返回空响应
                // 让客户端重试获取，这样不会丢失其他共享分区的数据
                if (fetchPartitionData.error.code() == Errors.OFFSET_OUT_OF_RANGE.code()) {
                    try {
                        sharePartition.updateCacheAndOffsets(offsetForEarliestTimestamp(topicIdPartition,
                            replicaManager, sharePartition.leaderEpoch()));
                    } catch (Exception e) {
                        log.error("Error while fetching offset for earliest timestamp for topicIdPartition: {}", topicIdPartition, e);
                        shareFetch.addErroneous(topicIdPartition, e);
                        exceptionHandler.accept(new SharePartitionKey(shareFetch.groupId(), topicIdPartition), e);
                        // Do not fill the response for this partition and continue.
                        continue;
                    }
                    // We set the error code to NONE, as we have updated the start offset of the share partition
                    // and the client can retry the fetch.
                    partitionData.setErrorCode(Errors.NONE.code());
                    partitionData.setErrorMessage(Errors.NONE.message());
                }
            } else {
                // 从共享分区获取记录
                // 参数包括：成员ID、批次大小、剩余可获取记录数（最大记录数减去已获取数）
                ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(shareFetch.memberId(), shareFetch.batchSize(), shareFetch.maxFetchRecords() - acquiredRecordsCount, fetchPartitionData);
                log.trace("获取的记录: {} 对应的主题分区: {}", shareAcquiredRecords, topicIdPartition);
                // 未来可能需要检查是否没有获取到记录，并决定是否重试副本管理器获取
                // 这取决于共享分区管理器的实现，是否允许对同一共享分区进行并行请求
                if (shareAcquiredRecords.acquiredRecords().isEmpty()) {
                    partitionData
                        .setRecords(null)
                        .setAcquiredRecords(Collections.emptyList());
                } else {
                    partitionData
                        // We set the records to the fetchPartitionData records. We do not alter the records
                        // fetched from the replica manager as they follow zero copy buffer. The acquired records
                        // might be a subset of the records fetched from the replica manager, depending
                        // on the max fetch records or available records in the share partition. The client
                        // sends the max bytes in request which should limit the bytes sent to the client
                        // in the response.
                        .setRecords(fetchPartitionData.records)
                        .setAcquiredRecords(shareAcquiredRecords.acquiredRecords());
                    acquiredRecordsCount += shareAcquiredRecords.count();
                }
            }
            response.put(topicIdPartition, partitionData);
        }
        return response;
    }

    /**
     * 获取主题分区最早时间戳对应的偏移量。
     * 该方法用于处理消费者需要从最早的可用数据开始消费的场景。
     *
     * @param topicIdPartition 主题分区标识符
     * @param replicaManager 副本管理器
     * @param leaderEpoch 领导者纪元号，用于确保一致性
     * @return 返回最早时间戳对应的偏移量
     * @throws OffsetNotAvailableException 当无法获取偏移量时抛出异常
     */
    static long offsetForEarliestTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager, int leaderEpoch) {
        // Isolation level is only required when reading from the latest offset hence use Option.empty() for now.
        Optional<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
                topicIdPartition.topicPartition(), ListOffsetsRequest.EARLIEST_TIMESTAMP, Option.empty(),
                Optional.of(leaderEpoch), true).timestampAndOffsetOpt();
        if (timestampAndOffset.isEmpty()) {
            throw new OffsetNotAvailableException("Offset for earliest timestamp not found for topic partition: " + topicIdPartition);
        }
        return timestampAndOffset.get().offset;
    }

    /**
     * 获取主题分区最新时间戳对应的偏移量。
     * 该方法用于处理消费者需要从最新的数据开始消费的场景。
     * 使用READ_UNCOMMITTED隔离级别，与共享获取请求保持一致。
     *
     * @param topicIdPartition 主题分区标识符
     * @param replicaManager 副本管理器
     * @param leaderEpoch 领导者纪元号，用于确保一致性
     * @return 返回最新时间戳对应的偏移量
     * @throws OffsetNotAvailableException 当无法获取偏移量时抛出异常
     */
    static long offsetForLatestTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager, int leaderEpoch) {
        // Isolation level is set to READ_UNCOMMITTED, matching with that used in share fetch requests
        Optional<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
            topicIdPartition.topicPartition(), ListOffsetsRequest.LATEST_TIMESTAMP, new Some<>(IsolationLevel.READ_UNCOMMITTED),
            Optional.of(leaderEpoch), true).timestampAndOffsetOpt();
        if (timestampAndOffset.isEmpty()) {
            throw new OffsetNotAvailableException("Offset for latest timestamp not found for topic partition: " + topicIdPartition);
        }
        return timestampAndOffset.get().offset;
    }

    /**
     * 根据给定的时间戳获取主题分区的偏移量。
     * 该方法使用READ_UNCOMMITTED隔离级别，以确保能读取到最新的数据。
     * 
     * @param topicIdPartition 主题分区标识符，包含主题ID和分区号
     * @param replicaManager 副本管理器，用于管理分区的副本状态
     * @param timestampToSearch 要搜索的目标时间戳
     * @param leaderEpoch 领导者纪元号，用于确保一致性
     * @return 返回指定时间戳对应的偏移量
     * @throws OffsetNotAvailableException 当无法找到指定时间戳对应的偏移量时抛出异常
     */
    static long offsetForTimestamp(TopicIdPartition topicIdPartition, ReplicaManager replicaManager, long timestampToSearch, int leaderEpoch) {
        // 使用副本管理器获取指定时间戳的偏移量，设置READ_UNCOMMITTED隔离级别
        Optional<FileRecords.TimestampAndOffset> timestampAndOffset = replicaManager.fetchOffsetForTimestamp(
            topicIdPartition.topicPartition(), timestampToSearch, new Some<>(IsolationLevel.READ_UNCOMMITTED), Optional.of(leaderEpoch), true).timestampAndOffsetOpt();
        // 如果找不到对应的偏移量，抛出异常
        if (timestampAndOffset.isEmpty()) {
            throw new OffsetNotAvailableException("Offset for timestamp " + timestampToSearch + " not found for topic partition: " + topicIdPartition);
        }
        // 返回找到的偏移量
        return timestampAndOffset.get().offset;
    }

    /**
     * 获取指定主题分区的领导者纪元号。
     * 领导者纪元用于跟踪分区领导者的变更历史，每次领导者变更时都会递增。
     * 
     * @param replicaManager 副本管理器，用于管理分区的副本状态
     * @param tp 主题分区对象
     * @return 返回当前的领导者纪元号
     * @throws NotLeaderOrFollowerException 当broker不是该分区的领导者时抛出异常
     */
    static int leaderEpoch(ReplicaManager replicaManager, TopicPartition tp) {
        // 获取分区对象并返回其领导者纪元号
        return partition(replicaManager, tp).getLeaderEpoch();
    }

    /**
     * 获取指定主题分区的分区对象，并验证当前broker是否为该分区的领导者。
     * 该方法在进行分区操作前进行领导者检查，确保操作的正确性。
     * 
     * @param replicaManager 副本管理器，用于管理分区的副本状态
     * @param tp 主题分区对象
     * @return 返回分区对象
     * @throws NotLeaderOrFollowerException 当broker不是该分区的领导者时抛出异常
     */
    static Partition partition(ReplicaManager replicaManager, TopicPartition tp) {
        // 从副本管理器获取分区对象，如果分区不存在会抛出异常
        Partition partition = replicaManager.getPartitionOrException(tp);
        // 验证当前broker是否为该分区的领导者
        if (!partition.isLeader()) {
            log.debug("The broker is not the leader for topic partition: {}-{}", tp.topic(), tp.partition());
            throw new NotLeaderOrFollowerException();
        }
        // 返回验证通过的分区对象
        return partition;
    }
}
