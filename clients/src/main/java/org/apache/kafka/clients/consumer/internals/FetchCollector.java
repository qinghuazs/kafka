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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code FetchCollector} 在 {@link RecordBatch} 级别上操作，因为这是存储在
 * {@link FetchBuffer} 中的内容。{@link RecordBatch} 中的每个 {@link org.apache.kafka.common.record.Record} 
 * 都会被转换成 {@link ConsumerRecord} 并添加到返回的 {@link Fetch} 中。
 * 
 * FetchCollector是Kafka消费者的核心组件之一，负责从FetchBuffer中收集消息记录并进行处理。
 * 它主要完成以下工作：
 * 1. 从FetchBuffer中获取已完成的获取请求(CompletedFetch)
 * 2. 处理消息批次(RecordBatch)，将其转换为消费者记录(ConsumerRecord)
 * 3. 管理消费位移，更新分区状态
 * 4. 处理各种异常情况，如分区重分配、授权错误等
 *
 * @param <K> 记录键的类型
 * @param <V> 记录值的类型
 */
public class FetchCollector<K, V> {

    // 用于记录日志的Logger实例
    private final Logger log;
    // 消费者元数据，包含broker、topic等信息
    private final ConsumerMetadata metadata;
    // 订阅状态管理器，维护分区订阅和位移信息
    private final SubscriptionState subscriptions;
    // 获取配置，包含最大拉取字节数等参数
    private final FetchConfig fetchConfig;
    // 反序列化器，用于将字节数组转换为键值对
    private final Deserializers<K, V> deserializers;
    // 度量指标管理器，用于监控和统计
    private final FetchMetricsManager metricsManager;
    // 时间工具类，用于处理时间相关操作
    private final Time time;

    /**
     * FetchCollector的构造函数
     * 
     * @param logContext 日志上下文，用于创建日志记录器
     * @param metadata 消费者元数据，包含集群、主题等信息
     * @param subscriptions 订阅状态管理器，维护分区订阅和消费位移
     * @param fetchConfig 获取配置，如最大拉取字节数等
     * @param deserializers 反序列化器，用于将字节数组转换为键值对
     * @param metricsManager 度量指标管理器，用于监控和统计
     * @param time 时间工具类，用于处理时间相关操作
     */
    public FetchCollector(final LogContext logContext,
                          final ConsumerMetadata metadata,
                          final SubscriptionState subscriptions,
                          final FetchConfig fetchConfig,
                          final Deserializers<K, V> deserializers,
                          final FetchMetricsManager metricsManager,
                          final Time time) {
        // 初始化日志记录器
        this.log = logContext.logger(FetchCollector.class);
        // 设置消费者元数据
        this.metadata = metadata;
        // 设置订阅状态管理器
        this.subscriptions = subscriptions;
        // 设置获取配置
        this.fetchConfig = fetchConfig;
        // 设置反序列化器
        this.deserializers = deserializers;
        // 设置度量指标管理器
        this.metricsManager = metricsManager;
        // 设置时间工具类
        this.time = time;
    }

    /**
     * 从FetchBuffer中获取已完成的消费记录，清空记录缓冲区，并更新消费位移。
     * 
     * 该方法是消费者获取消息的核心实现，主要完成以下工作：
     * 1. 从FetchBuffer中获取已完成的获取请求
     * 2. 处理暂停的分区，将其记录保存到临时队列
     * 3. 对于活跃的分区，将记录转换为ConsumerRecord并返回
     * 4. 更新分区的消费位移
     * 5. 处理各种异常情况
     *
     * 注意：返回空的Fetch对象可以保证消费位移不会被更新
     *
     * @param fetchBuffer 用于获取ConsumerRecord记录的FetchBuffer
     * @return 包含请求分区记录的Fetch对象
     * @throws OffsetOutOfRangeException 当fetchResponse中存在OffsetOutOfRange错误且defaultResetPolicy为NONE时抛出
     * @throws TopicAuthorizationException 当fetchResponse中存在TopicAuthorization错误时抛出
     */
    public Fetch<K, V> collectFetch(final FetchBuffer fetchBuffer) {
        // 创建一个空的Fetch对象用于存储获取的记录
        final Fetch<K, V> fetch = Fetch.empty();
        // 创建一个队列用于存储暂停分区的已完成获取请求
        final Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
        // 获取配置中设置的最大拉取记录数
        int recordsRemaining = fetchConfig.maxPollRecords;

        try {
            // 当还有剩余记录数时，继续获取记录
            while (recordsRemaining > 0) {
                // 获取下一个待处理的已完成获取请求
                final CompletedFetch nextInLineFetch = fetchBuffer.nextInLineFetch();

                // 如果没有待处理的请求或者当前请求已经被消费完
                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    // 查看缓冲区队列中的下一个请求
                    final CompletedFetch completedFetch = fetchBuffer.peek();

                    // 如果没有更多的请求了，退出循环
                    if (completedFetch == null)
                        break;

                    // 如果请求还未初始化
                    if (!completedFetch.isInitialized()) {
                        try {
                            // 初始化请求并设置为下一个待处理的请求
                            fetchBuffer.setNextInLineFetch(initialize(completedFetch));
                        } catch (Exception e) {
                            // 当解析出现异常时移除completedFetch，需满足两个条件：
                            // 1. 它不包含任何completedFetch
                            // 2. 在这个异常之前没有已获取的带有实际内容的completedFetch
                            // 第一个条件确保completedFetches不会在TopicAuthorizationException等情况下卡在同一个completedFetch上
                            // 第二个条件确保不会因为后续记录中的异常而导致潜在的数据丢失
                            if (fetch.isEmpty() && FetchResponse.recordsOrFail(completedFetch.partitionData).sizeInBytes() == 0)
                                fetchBuffer.poll();

                            throw e;
                        }
                    } else {
                        // 如果请求已初始化，直接设置为下一个待处理的请求
                        fetchBuffer.setNextInLineFetch(completedFetch);
                    }

                    // 从缓冲区队列中移除已处理的请求
                    fetchBuffer.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                    // 如果分区已暂停，将记录添加回completedFetches队列而不是消费它们
                    // 这样当分区恢复时，这些记录可以在后续的poll中返回
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition);
                    pausedCompletedFetches.add(nextInLineFetch);
                    fetchBuffer.setNextInLineFetch(null);
                } else {
                    // 获取记录并添加到结果中
                    final Fetch<K, V> nextFetch = fetchRecords(nextInLineFetch, recordsRemaining);
                    // 更新剩余可获取的记录数
                    recordsRemaining -= nextFetch.numRecords();
                    // 将获取的记录添加到结果中
                    fetch.add(nextFetch);
                }
            }
        } catch (KafkaException e) {
            // 只有在没有成功获取任何记录时才抛出异常
            if (fetch.isEmpty())
                throw e;
        } finally {
            // 将所有已轮询的暂停分区的已完成获取请求添加回队列
            // 这些请求将在下次poll时重新评估
            fetchBuffer.addAll(pausedCompletedFetches);
        }

        return fetch;
    }

    /**
     * 从指定的已完成获取请求中获取记录
     * 
     * 该方法处理单个分区的记录获取，主要完成以下工作：
     * 1. 检查分区的分配状态和可获取性
     * 2. 验证和更新消费位移
     * 3. 获取并处理消费记录
     * 4. 更新消费者监控指标
     *
     * @param nextInLineFetch 待处理的已完成获取请求
     * @param maxRecords 最大可获取的记录数
     * @return 包含获取记录的Fetch对象
     */
    private Fetch<K, V> fetchRecords(final CompletedFetch nextInLineFetch, int maxRecords) {
        // 获取分区信息
        final TopicPartition tp = nextInLineFetch.partition;

        if (!subscriptions.isAssigned(tp)) {
            // 当消费者重平衡发生在获取记录返回给消费者poll调用之前时可能发生这种情况
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", tp);
        } else if (!subscriptions.isFetchable(tp)) {
            // 当分区在获取的记录返回给消费者的poll调用之前被暂停，
            // 或者位移正在被重置时可能发生这种情况
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", tp);
        } else {
            // 获取分区的当前消费位移
            SubscriptionState.FetchPosition position = subscriptions.position(tp);

            // 如果没有位移信息，说明状态异常
            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + tp);

            // 验证获取请求的位移是否匹配当前消费位移
            if (nextInLineFetch.nextFetchOffset() == position.offset) {
                // 获取消费记录，同时进行反序列化
                List<ConsumerRecord<K, V>> partRecords = nextInLineFetch.fetchRecords(fetchConfig,
                        deserializers,
                        maxRecords);

                log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                        partRecords.size(), position, tp);

                boolean positionAdvanced = false;

                // 如果获取到了新的记录，更新消费位移
                if (nextInLineFetch.nextFetchOffset() > position.offset) {
                    // 创建新的位移信息
                    SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                            nextInLineFetch.nextFetchOffset(),
                            nextInLineFetch.lastEpoch(),
                            position.currentLeader);
                    log.trace("Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                            position, nextPosition, tp, partRecords.size());
                    // 更新分区的消费位移
                    subscriptions.position(tp, nextPosition);
                    positionAdvanced = true;
                }

                // 计算并记录分区的消费延迟
                Long partitionLag = subscriptions.partitionLag(tp, fetchConfig.isolationLevel);
                if (partitionLag != null)
                    metricsManager.recordPartitionLag(tp, partitionLag);

                // 计算并记录分区的消费领先量
                Long lead = subscriptions.partitionLead(tp);
                if (lead != null) {
                    metricsManager.recordPartitionLead(tp, lead);
                }

                // 返回包含获取记录的Fetch对象
                return Fetch.forPartition(tp, partRecords, positionAdvanced, new OffsetAndMetadata(nextInLineFetch.nextFetchOffset(), nextInLineFetch.lastEpoch(), ""));
            } else {
                // 如果获取请求的位移与当前消费位移不匹配，说明这些记录不是下一个要消费的
                // 这些记录可能来自过期的请求
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        tp, nextInLineFetch.nextFetchOffset(), position);
            }
        }

        // 清空获取的记录并返回空的Fetch对象
        log.trace("Draining fetched records for partition {}", tp);
        nextInLineFetch.drain();

        return Fetch.empty();
    }

    /**
     * 初始化一个已完成的获取请求对象
     * 
     * 该方法负责处理CompletedFetch对象的初始化工作，主要包括：
     * 1. 检查分区的有效性
     * 2. 处理可能的错误情况
     * 3. 记录相关的度量指标
     * 4. 在发生错误时调整分区顺序
     *
     * @param completedFetch 需要初始化的已完成获取请求
     * @return 初始化后的CompletedFetch对象，如果初始化失败则返回null
     */
    protected CompletedFetch initialize(final CompletedFetch completedFetch) {
        // 获取分区信息和错误码
        final TopicPartition tp = completedFetch.partition;
        final Errors error = Errors.forCode(completedFetch.partitionData.errorCode());
        boolean recordMetrics = true;

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // 当获取请求还在处理中发生重平衡时可能出现这种情况
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
                return null;
            } else if (error == Errors.NONE) {
                // 如果没有错误，处理初始化成功的情况
                final CompletedFetch ret = handleInitializeSuccess(completedFetch);
                recordMetrics = ret == null;
                return ret;
            } else {
                // 处理初始化过程中的错误
                handleInitializeErrors(completedFetch, error);
                return null;
            }
        } finally {
            // 记录聚合指标
            if (recordMetrics) {
                completedFetch.recordAggregatedMetrics(0, 0);
            }

            // 如果发生错误，将分区移到末尾
            // 这样做可以让同一个主题的分区保持在一起（有利于更高效的序列化）
            if (error != Errors.NONE)
                subscriptions.movePartitionToEnd(tp);
        }
    }

    /**
     * 处理获取请求初始化成功的情况
     * 
     * 该方法主要完成以下工作：
     * 1. 验证获取位移是否匹配当前消费位置
     * 2. 处理记录批次的获取
     * 3. 更新分区状态信息
     * 4. 处理大消息的特殊情况
     *
     * @param completedFetch 已完成的获取请求
     * @return 处理成功返回CompletedFetch对象，失败返回null
     */
    private CompletedFetch handleInitializeSuccess(final CompletedFetch completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset();

        // 只有当起始位移匹配当前消费位置时，我们才对这个获取请求感兴趣
        SubscriptionState.FetchPosition position = subscriptions.positionOrNull(tp);
        if (position == null || position.offset != fetchOffset) {
            log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                "the expected offset {} or the partition has been unassigned", tp, fetchOffset, position);
            return null;
        }

        // 获取分区数据并准备读取
        final FetchResponseData.PartitionData partition = completedFetch.partitionData;
        log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
                FetchResponse.recordsSize(partition), tp, position);
        Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();

        // 处理大消息的特殊情况
        if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
            if (completedFetch.requestVersion < 3) {
                // 实现KIP-74之前的行为，抛出RecordTooLargeException
                Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
                        recordTooLargePartitions + " whose size is larger than the fetch size " + fetchConfig.fetchSize +
                        " and hence cannot be returned. Please considering upgrading your broker to 0.10.1.0 or " +
                        "newer to avoid this issue. Alternately, increase the fetch size on the client (using " +
                        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG + ")",
                        recordTooLargePartitions);
            } else {
                // 对于支持FetchRequest/Response V3或更高版本的broker不应该发生这种情况（即KIP-74）
                throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
                        fetchOffset + ". Received a non-empty fetch response from the server, but no " +
                        "complete records were found.");
            }
        }

        // 更新分区状态信息
        if (!updatePartitionState(partition, tp)) {
            return null;
        }

        // 标记初始化完成并返回
        completedFetch.setInitialized();
        return completedFetch;
    }

    /**
     * 更新分区的状态信息，包括高水位、日志起始偏移量、最后稳定偏移量和首选副本等
     * 
     * 该方法在每次获取数据时被调用，用于维护分区的各种状态信息，这些信息对于消费者的正常运行至关重要：
     * 1. 高水位(High Watermark)：表示所有副本都已复制的最大偏移量，用于确保数据一致性
     * 2. 日志起始偏移量(Log Start Offset)：表示日志中最早的可用消息偏移量
     * 3. 最后稳定偏移量(Last Stable Offset)：事务消息中用于标识已提交消息的偏移量
     * 4. 首选副本(Preferred Replica)：用于实现读取负载均衡的副本选择
     *
     * @param partitionData 包含分区数据和元数据的对象
     * @param tp 要更新状态的主题分区
     * @return 如果所有状态更新成功返回true，否则返回false
     */
    private boolean updatePartitionState(final FetchResponseData.PartitionData partitionData,
                                         final TopicPartition tp) {
        // 更新高水位标记，这是已经被所有副本复制的最大偏移量
        // 高水位用于保证数据一致性，消费者只能看到高水位之前的消息
        if (partitionData.highWatermark() >= 0) {
            log.trace("Updating high watermark for partition {} to {}", tp, partitionData.highWatermark());
            if (!subscriptions.tryUpdatingHighWatermark(tp, partitionData.highWatermark())) {
                return false;
            }
        }

        // 更新日志起始偏移量，这是日志中最早的可用消息偏移量
        // 当发生日志清理或压缩时，这个值可能会增加
        if (partitionData.logStartOffset() >= 0) {
            log.trace("Updating log start offset for partition {} to {}", tp, partitionData.logStartOffset());
            if (!subscriptions.tryUpdatingLogStartOffset(tp, partitionData.logStartOffset())) {
                return false;
            }
        }

        // 更新最后稳定偏移量，这在事务消息中特别重要
        // 它标识了已经完成事务提交的消息的最大偏移量
        if (partitionData.lastStableOffset() >= 0) {
            log.trace("Updating last stable offset for partition {} to {}", tp, partitionData.lastStableOffset());
            if (!subscriptions.tryUpdatingLastStableOffset(tp, partitionData.lastStableOffset())) {
                return false;
            }
        }

        // 更新首选副本信息，这用于实现读取负载均衡
        // 如果启用了首选副本功能，消费者可能会从非leader副本读取数据
        if (FetchResponse.isPreferredReplica(partitionData)) {
            return subscriptions.tryUpdatingPreferredReadReplica(
                tp, partitionData.preferredReadReplica(), () -> {
                    // 计算首选副本的过期时间，超过这个时间需要重新评估
                    long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                    log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
                        tp, partitionData.preferredReadReplica(), expireTimeMs);
                    return expireTimeMs;
                });
        }

        return true;
    }

    /**
     * 处理获取请求初始化过程中遇到的各种错误
     * 
     * 该方法负责处理在获取数据过程中可能遇到的所有错误情况，主要包括：
     * 1. 元数据相关错误：如分区leader变更、副本不可用等
     * 2. 偏移量错误：如偏移量超出范围
     * 3. 授权错误：如没有读取权限
     * 4. 其他系统错误：如消息损坏、未知服务器错误等
     *
     * @param completedFetch 已完成的获取请求，包含分区和偏移量信息
     * @param error 错误类型
     * @throws TopicAuthorizationException 当没有主题的读取权限时抛出
     * @throws OffsetOutOfRangeException 当消费位移超出有效范围且没有配置重置策略时抛出
     * @throws KafkaException 当遇到消息损坏等严重错误时抛出
     * @throws IllegalStateException 当遇到未预期的错误类型时抛出
     */
    private void handleInitializeErrors(final CompletedFetch completedFetch, final Errors error) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset();

        // 处理需要更新元数据的错误情况
        // 这些错误通常是由于集群状态变化导致的临时性错误
        if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                error == Errors.REPLICA_NOT_AVAILABLE ||
                error == Errors.KAFKA_STORAGE_ERROR ||
                error == Errors.FENCED_LEADER_EPOCH ||
                error == Errors.OFFSET_NOT_AVAILABLE) {
            log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
            requestMetadataUpdate(metadata, subscriptions, tp);
        } 
        // 处理未知主题或分区错误
        // 这可能是由于主题被删除或者元数据过期导致
        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } 
        // 处理未知主题ID错误
        // 这通常发生在使用主题ID而不是主题名称进行操作时
        else if (error == Errors.UNKNOWN_TOPIC_ID) {
            log.warn("Received unknown topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } 
        // 处理主题ID不一致错误
        // 这可能发生在主题被删除后重新创建的情况
        else if (error == Errors.INCONSISTENT_TOPIC_ID) {
            log.warn("Received inconsistent topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } 
        // 处理偏移量超出范围错误
        // 这可能发生在日志被清理或消费者长时间不活动的情况
        else if (error == Errors.OFFSET_OUT_OF_RANGE) {
            Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);

            if (clearedReplicaId.isEmpty()) {
                // 如果没有首选副本需要清除，说明我们正在从leader获取数据
                SubscriptionState.FetchPosition position = subscriptions.positionOrNull(tp);

                if (position == null || fetchOffset != position.offset) {
                    // 如果位置为空或偏移量不匹配，说明这是一个过期的响应
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                            "does not match the current offset {} or the partition has been unassigned", tp, fetchOffset, position);
                } else {
                    String errorMessage = "Fetch position " + position + " is out of range for partition " + tp;

                    if (subscriptions.hasDefaultOffsetResetPolicy()) {
                        // 如果配置了默认的偏移量重置策略，则重置偏移量
                        log.info("{}, resetting offset", errorMessage);
                        subscriptions.requestOffsetResetIfPartitionAssigned(tp);
                    } else {
                        // 如果没有配置重置策略，则向应用程序抛出异常
                        log.info("{}, raising error to the application since no reset policy is configured", errorMessage);
                        throw new OffsetOutOfRangeException(errorMessage,
                                Collections.singletonMap(tp, position.offset));
                    }
                }
            } else {
                // 如果有首选副本被清除，记录日志并继续
                log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                        clearedReplicaId.get(), tp, error, fetchOffset);
            }
        } 
        // 处理主题授权失败错误
        // 这表示消费者没有读取该主题的权限
        else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
            //记录具体的分区而不是主题，以帮助在大型集群中定位ACL传播问题
            log.warn("Not authorized to read from partition {}.", tp);
            throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
        } 
        // 处理未知leader epoch错误
        // 这通常是由于leader发生变更导致的临时性错误
        else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
            log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
        } 
        // 处理未知服务器错误
        // 这是一个通用的错误，表示服务器端发生了未预期的问题
        else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            log.warn("Unknown server error while fetching offset {} for topic-partition {}",
                    fetchOffset, tp);
        } 
        // 处理消息损坏错误
        // 这表示获取到的消息数据已经损坏
        else if (error == Errors.CORRUPT_MESSAGE) {
            throw new KafkaException("Encountered corrupt message when fetching offset "
                    + fetchOffset
                    + " for topic-partition "
                    + tp);
        } 
        // 处理未预期的错误类型
        // 这表示遇到了一个未知的错误码
        else {
            throw new IllegalStateException("Unexpected error code "
                    + error.code()
                    + " while fetching at offset "
                    + fetchOffset
                    + " from topic-partition " + tp);
        }
    }
}
