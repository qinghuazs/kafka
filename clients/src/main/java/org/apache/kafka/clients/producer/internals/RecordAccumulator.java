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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这个类作为一个队列,将记录累积到{@link MemoryRecords}实例中,以便发送到服务器。
 * <p>
 * 累加器使用有限的内存空间,当内存耗尽时append调用会阻塞,除非显式禁用此行为。
 * <p>
 * 主要功能:
 * 1. 将消息记录批量累积到内存中,提高发送效率
 * 2. 管理消息批次的内存分配和释放
 * 3. 支持消息压缩
 * 4. 实现消息分区的自适应选择
 * 5. 处理事务性消息的发送
 */
public class RecordAccumulator {

    // 日志上下文和日志记录器
    private final LogContext logContext;
    private final Logger log;
    
    // 累加器状态标志
    private volatile boolean closed;  // 是否已关闭
    private final AtomicInteger flushesInProgress;  // 正在进行的刷新操作计数
    private final AtomicInteger appendsInProgress;  // 正在进行的追加操作计数
    
    // 批次配置参数
    private final int batchSize;  // 每个批次的目标大小(字节)
    private final Compression compression;  // 消息压缩类型
    private final int lingerMs;  // 延迟发送时间,用于等待批次填满
    private final ExponentialBackoff retryBackoff;  // 重试退避策略
    private final int deliveryTimeoutMs;  // 消息投递超时时间
    
    // 分区管理相关
    private final long partitionAvailabilityTimeoutMs;  // 标记分区临时不可用的延迟阈值
    private final boolean enableAdaptivePartitioning;  // 是否启用自适应分区
    
    // 资源管理
    private final BufferPool free;  // 内存缓冲池
    private final Time time;  // 时间服务
    private final ApiVersions apiVersions;  // API版本信息
    
    // 主题和节点状态追踪
    private final ConcurrentMap<String /*topic*/, TopicInfo> topicInfoMap = new CopyOnWriteMap<>();  // 主题信息映射
    private final ConcurrentMap<Integer /*nodeId*/, NodeLatencyStats> nodeStats = new CopyOnWriteMap<>();  // 节点延迟统计
    private final IncompleteBatches incomplete;  // 未完成批次管理
    
    // 以下变量仅由sender线程访问,无需同步保护
    private final Set<TopicPartition> muted;  // 已静音的主题分区集合
    private final Map<String, Integer> nodesDrainIndex;  // 节点排空索引
    private final TransactionManager transactionManager;  // 事务管理器
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE;  // 下一个批次过期的最早时间(绝对时间)

    /**
     * 创建一个新的记录累加器
     *
     * @param logContext 用于日志记录的上下文对象
     * @param batchSize 分配{@link MemoryRecords}实例时使用的批次大小(字节)
     * @param compression 记录的压缩编解码器
     * @param lingerMs 在声明一个未满的记录实例准备好发送之前添加的人工延迟时间。
     *                这允许更多记录到达。设置非零的lingerMs将通过更多的批处理(从而减少更大的请求)来权衡一些延迟以获得更好的吞吐量。
     * @param retryBackoffMs 收到错误时重试生产请求的人工延迟时间。
     *                      这避免了在短时间内耗尽所有重试次数。
     * @param retryBackoffMaxMs 重试退避时间的上限
     * @param deliveryTimeoutMs 报告记录投递成功或失败的时间上限
     * @param partitionerConfig 分区器配置,包含分区选择和可用性相关的配置
     * @param metrics 指标收集器,用于监控性能指标
     * @param metricGrpName 指标组名称,用于对指标进行分组
     * @param time 时间实例,用于获取时间戳和计算超时
     * @param apiVersions 当前连接的broker支持的API版本信息
     * @param transactionManager 共享的事务状态对象,用于跟踪每个分区的生产者ID、epoch和序列号
     * @param bufferPool 缓冲池,用于管理消息批次的内存分配
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             PartitionerConfig partitionerConfig,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        // 初始化日志相关组件
        this.logContext = logContext;
        this.log = logContext.logger(RecordAccumulator.class);
        
        // 初始化状态标志和计数器
        this.closed = false;  // 初始状态为未关闭
        this.flushesInProgress = new AtomicInteger(0);  // 刷新操作计数器初始化为0
        this.appendsInProgress = new AtomicInteger(0);  // 追加操作计数器初始化为0
        
        // 设置批次和压缩相关参数
        this.batchSize = batchSize;  // 设置批次大小
        this.compression = compression;  // 设置压缩类型
        this.lingerMs = lingerMs;  // 设置延迟发送时间
        
        // 初始化重试退避策略
        this.retryBackoff = new ExponentialBackoff(retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,  // 退避基数
                retryBackoffMaxMs,  // 最大退避时间
                CommonClientConfigs.RETRY_BACKOFF_JITTER);  // 退避抖动
        
        // 设置超时和分区相关配置
        this.deliveryTimeoutMs = deliveryTimeoutMs;  // 设置投递超时时间
        this.enableAdaptivePartitioning = partitionerConfig.enableAdaptivePartitioning;  // 是否启用自适应分区
        this.partitionAvailabilityTimeoutMs = partitionerConfig.partitionAvailabilityTimeoutMs;  // 分区可用性超时时间
        
        // 初始化资源管理组件
        this.free = bufferPool;  // 设置缓冲池
        this.incomplete = new IncompleteBatches();  // 创建未完成批次管理器
        this.muted = new HashSet<>();  // 初始化已静音分区集合
        this.time = time;  // 设置时间服务
        this.apiVersions = apiVersions;  // 设置API版本信息
        
        // 初始化节点和事务相关组件
        nodesDrainIndex = new HashMap<>();  // 创建节点排空索引映射
        this.transactionManager = transactionManager;  // 设置事务管理器
        
        // 注册监控指标
        registerMetrics(metrics, metricGrpName);
    }

    /**
     * Create a new record accumulator with default partitioner config
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this(logContext,
            batchSize,
            compression,
            lingerMs,
            retryBackoffMs,
            retryBackoffMaxMs,
            deliveryTimeoutMs,
            new PartitionerConfig(),
            metrics,
            metricGrpName,
            time,
            apiVersions,
            transactionManager,
            bufferPool);
    }

    /**
     * 注册累加器的监控指标
     * 
     * @param metrics 指标收集器
     * @param metricGrpName 指标组名称
     */
    private void registerMetrics(Metrics metrics, String metricGrpName) {
        // 注册等待线程数指标
        // 统计因缓冲区内存不足而被阻塞的用户线程数量
        metrics.addMetric(
            metrics.metricName("waiting-threads", metricGrpName,
                "等待缓冲区内存以入队记录的被阻塞用户线程数"),
            (config, now) -> free.queued());

        // 注册总缓冲区大小指标
        // 统计客户端可以使用的最大缓冲区内存量(不论当前是否使用)
        metrics.addMetric(
            metrics.metricName("buffer-total-bytes", metricGrpName,
                "客户端可使用的最大缓冲区内存量(不论当前是否使用)"),
            (config, now) -> free.totalMemory());

        // 注册可用缓冲区大小指标
        // 统计未被使用的缓冲区内存总量(包括未分配或在空闲列表中的内存)
        metrics.addMetric(
            metrics.metricName("buffer-available-bytes", metricGrpName,
                "未被使用的缓冲区内存总量(未分配或在空闲列表中)"),
            (config, now) -> free.availableMemory());
    }

    /**
     * 设置回调对象中的分区信息
     * 
     * @param callbacks 回调对象
     * @param partition 分区号
     */
    private void setPartition(AppendCallbacks callbacks, int partition) {
        if (callbacks != null)
            callbacks.setPartition(partition);  // 如果回调对象存在,设置其分区号
    }

    /**
     * 检查分区是否发生并发变更,或者是否需要完成之前被禁用的分区变更
     *
     * @param topic 主题名称
     * @param topicInfo 主题信息
     * @param partitionInfo 内置分区器的分区信息
     * @param deque 分区队列
     * @param nowMs 当前时间戳(毫秒)
     * @param cluster 集群元数据
     * @return 如果分区发生变更且需要获取新的分区信息并重试则返回true,否则返回false
     */
    private boolean partitionChanged(String topic,
                                     TopicInfo topicInfo,
                                     BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                     Deque<ProducerBatch> deque, long nowMs,
                                     Cluster cluster) {
        // 检查分区是否发生并发变更
        if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
            log.trace("主题 {} 的分区 {} 被并发追加切换,正在重试",
                    partitionInfo.partition(), topic);
            return true;
        }

        // 如果队列中有未完成的批次,我们可能禁用了分区切换
        // 检查所有批次是否都已填满,如果是则可以进行切换
        if (allBatchesFull(deque)) {
            topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, 0, cluster, true);
            if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
                log.trace("完成了主题 {} 分区 {} 之前被禁用的切换,正在重试", topic, partitionInfo.partition());
                return true;
            }
        }

        return false;
    }

    /**
     * 将一条记录添加到累加器中,返回追加结果
     * <p>
     * 追加结果将包含未来的元数据,以及表示追加的批次是否已满或是否创建了新批次的标志
     * <p>
     *
     * @param topic 要发送记录的主题
     * @param partition 要发送记录的分区,如果可以使用任何分区则为RecordMetadata.UNKNOWN_PARTITION
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部信息
     * @param callbacks 要执行的回调
     * @param maxTimeToBlock 等待缓冲区内存可用的最大时间(毫秒)
     * @param nowMs 当前时间戳(毫秒)
     * @param cluster 集群元数据
     */
    public RecordAppendResult append(String topic,
                                     int partition,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     AppendCallbacks callbacks,
                                     long maxTimeToBlock,
                                     long nowMs,
                                     Cluster cluster) throws InterruptedException {
        // 获取或创建主题信息,包含内置分区器
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(topic, k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize)));

        // 追踪正在进行的追加操作数量,确保在abortIncompleteBatches()中不会遗漏批次
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;  // 如果没有头部信息则使用空头部
        try {
            // 循环重试,以处理分区器的竞争条件
            while (true) {
                // 如果消息没有指定分区,则根据broker的可用性和性能选择一个分区
                // 注意:在获取deque锁之前先获取当前分区,需要确保在等待锁期间分区没有变化
                final BuiltInPartitioner.StickyPartitionInfo partitionInfo;
                final int effectivePartition;
                if (partition == RecordMetadata.UNKNOWN_PARTITION) {
                    // 使用内置分区器选择分区
                    partitionInfo = topicInfo.builtInPartitioner.peekCurrentPartitionInfo(cluster);
                    effectivePartition = partitionInfo.partition();
                } else {
                    partitionInfo = null;
                    effectivePartition = partition;  // 使用指定的分区
                }

                // 设置回调中的实际分区信息
                setPartition(callbacks, effectivePartition);

                // 获取或创建分区的批次队列
                Deque<ProducerBatch> dq = topicInfo.batches.computeIfAbsent(effectivePartition, k -> new ArrayDeque<>());
                synchronized (dq) {
                    // 获取锁后,验证分区是否发生变化,如果变化则重试
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    // 尝试将记录追加到现有批次
                    RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                    if (appendResult != null) {
                        // 如果队列有未完成的批次,禁用分区切换
                        boolean enableSwitch = allBatchesFull(dq);
                        topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                        return appendResult;
                    }
                }

                // 如果没有可用批次或批次已满,分配新的缓冲区
                if (buffer == null) {
                    // 计算所需的缓冲区大小,取批次大小和记录估计大小的较大值
                    int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers));
                    log.trace("为主题 {} 分区 {} 分配新的 {} 字节消息缓冲区,剩余超时时间 {}ms", size, topic, effectivePartition, maxTimeToBlock);
                    // 如果内存耗尽,这个调用可能会阻塞
                    buffer = free.allocate(size, maxTimeToBlock);
                    // 如果缓冲区分配发生阻塞,更新当前时间
                    // 注意:获取时间可能很昂贵,应避免在锁内调用
                    nowMs = time.milliseconds();
                }

                synchronized (dq) {
                    // 再次验证分区是否发生变化
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    // 创建新的批次并追加记录
                    RecordAppendResult appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks, buffer, nowMs);
                    // 如果创建了新批次,将buffer设为null以防止其被返回到空闲池
                    if (appendResult.newBatchCreated)
                        buffer = null;
                    // 如果队列有未完成的批次,禁用分区切换
                    boolean enableSwitch = allBatchesFull(dq);
                    topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                    return appendResult;
                }
            }
        } finally {
            free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * 向队列中追加一个新的批次
     * 
     * 该方法负责创建新的消息批次并将其添加到指定主题分区的队列中。主要步骤包括:
     * 1. 首先尝试追加到现有批次
     * 2. 如果无法追加到现有批次,则创建新批次
     * 3. 将新批次添加到队列并标记为未完成
     * 4. 返回追加结果
     *
     * @param topic 消息要发送到的主题
     * @param partition 目标分区(不能是UNKNOWN_PARTITION)
     * @param dq 该分区的批次队列
     * @param timestamp 消息的时间戳
     * @param key 消息的键
     * @param value 消息的值
     * @param headers 消息的头部信息
     * @param callbacks 消息发送完成后要执行的回调
     * @param buffer 用于新批次的内存缓冲区
     * @param nowMs 当前时间戳(毫秒)
     */
    private RecordAppendResult appendNewBatch(String topic,
                                              int partition,
                                              Deque<ProducerBatch> dq,
                                              long timestamp,
                                              byte[] key,
                                              byte[] value,
                                              Header[] headers,
                                              AppendCallbacks callbacks,
                                              ByteBuffer buffer,
                                              long nowMs) {
        // 确保分区号是有效的(不是未知分区)
        assert partition != RecordMetadata.UNKNOWN_PARTITION;

        // 首先尝试追加到队列中最后一个批次
        RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
        if (appendResult != null) {
            // 如果其他线程已经创建了新批次并成功追加,则返回追加结果
            return appendResult;
        }

        // 创建新的内存记录构建器
        MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer);
        // 使用内存记录构建器创建新的生产者批次
        ProducerBatch batch = new ProducerBatch(new TopicPartition(topic, partition), recordsBuilder, nowMs);
        // 将记录追加到新批次中,这里必须成功因为是新批次
        FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                callbacks, nowMs));

        // 将新批次添加到队列尾部
        dq.addLast(batch);
        // 将新批次添加到未完成批次集合中
        incomplete.add(batch);

        // 返回追加结果,包含元数据Future、是否需要创建新批次、是否创建了新批次、追加的字节数
        return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, batch.estimatedSizeInBytes());
    }

    /**
     * 创建内存记录构建器
     * 
     * 该方法用于创建一个新的MemoryRecordsBuilder实例,用于构建消息批次。
     * 构建器配置包括:
     * - 使用提供的内存缓冲区
     * - 使用当前的消息格式版本
     * - 应用配置的压缩类型
     * - 使用CREATE_TIME作为时间戳类型
     * - 基准偏移量设为0
     *
     * @param buffer 用于存储消息的内存缓冲区
     * @return 配置好的内存记录构建器
     */
    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer) {
        // 创建并返回内存记录构建器,设置消息格式版本、压缩类型、时间戳类型和基准偏移量
        return MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     * 检查队列中的所有批次是否都已满
     * 
     * 由于只有队列中的最后一个批次可能未满(正在追加中),
     * 所以只需要检查最后一个批次的状态即可。
     * 如果队列为空或最后一个批次已满,则返回true。
     *
     * @param deque 要检查的批次队列
     * @return 如果所有批次都已满则返回true,否则返回false
     */
    private boolean allBatchesFull(Deque<ProducerBatch> deque) {
        // 获取队列中的最后一个批次
        ProducerBatch last = deque.peekLast();
        // 如果队列为空(last==null)或最后一个批次已满,则返回true
        return last == null || last.isFull();
    }

    /**
     * 尝试将记录追加到现有的生产者批次中
     * 
     * 该方法尝试将新记录追加到队列中最后一个批次。如果批次已满,则返回null以触发创建新批次。
     * 当批次已满时,会关闭批次的追加操作以释放资源(如压缩缓冲区)。
     * 批次在以下情况之一时会被完全关闭(写入批次头部并构建内存记录):
     * 1. 发送之前
     * 2. 批次过期
     * 3. 生产者关闭
     *
     * @param timestamp 记录的时间戳
     * @param key 记录的键
     * @param value 记录的值
     * @param headers 记录的头部信息
     * @param callback 发送完成后的回调函数
     * @param deque 批次队列
     * @param nowMs 当前时间戳(毫秒)
     * @return 追加结果,如果追加失败则返回null
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        // 如果生产者已关闭,则抛出异常
        if (closed)
            throw new KafkaException("Producer closed while send in progress");
        
        // 获取队列中的最后一个批次
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            // 记录追加前的批次大小
            int initialBytes = last.estimatedSizeInBytes();
            // 尝试将记录追加到批次中
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null) {
                // 如果追加失败(批次已满),关闭批次的追加操作以释放资源
                last.closeForRecordAppends();
            } else {
                // 计算本次追加的字节数
                int appendedBytes = last.estimatedSizeInBytes() - initialBytes;
                // 返回追加结果,包含元数据Future、是否需要创建新批次、是否创建了新批次、追加的字节数
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, appendedBytes);
            }
        }
        // 如果队列为空或追加失败,返回null以触发创建新批次
        return null;
    }

    /**
     * 检查指定的主题分区是否被静音(muted)
     * 静音的分区表示暂时不接受新的消息追加
     * 
     * @param tp 要检查的主题分区
     * @return 如果分区被静音则返回true,否则返回false
     */
    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);  // 检查静音集合中是否包含该分区
    }

    /**
     * 重置下一个批次的过期时间为最大值
     * 这通常在所有批次都被处理完成后调用
     */
    public void resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE;  // 将过期时间设置为最大值
    }

    /**
     * 更新下一个批次的过期时间
     * 过期时间为批次创建时间加上投递超时时间
     * 
     * @param batch 要检查的生产者批次
     */
    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        if (batch.createdMs + deliveryTimeoutMs  > 0) {  // 检查是否会发生溢出
            // 防止因设置过大的deliveryTimeoutMs导致溢出
            // 取当前最早过期时间和新批次过期时间的较小值
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            // 如果发生溢出,记录警告日志
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * 获取在累加器中停留时间过长需要过期的批次列表
     * 
     * @param now 当前时间戳
     * @return 已过期的批次列表
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();  // 存储过期的批次
        // 遍历所有主题的所有分区
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                // 按发送顺序检查批次是否过期
                synchronized (deque) {  // 同步访问队列
                    while (!deque.isEmpty()) {  // 持续检查队列中的批次
                        ProducerBatch batch = deque.getFirst();  // 获取队首批次
                        if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {  // 检查是否超时
                            deque.poll();  // 移除过期批次
                            batch.abortRecordAppends();  // 中止该批次的追加操作
                            expiredBatches.add(batch);  // 添加到过期列表
                        } else {
                            maybeUpdateNextBatchExpiryTime(batch);  // 更新下一个过期时间
                            break;  // 如果当前批次未过期,后续批次也不会过期
                        }
                    }
                }
            }
        }
        return expiredBatches;  // 返回所有过期的批次
    }

    /**
     * 获取消息投递超时时间
     * 
     * @return 投递超时时间(毫秒)
     */
    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * 将给定的记录批次重新入队到累加器中
     * 在Sender.completeBatch方法中已经检查了批次是否达到投递超时,因此这里不再检查
     * 
     * @param batch 要重新入队的批次
     * @param now 当前时间戳
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);  // 更新批次的重新入队时间
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);  // 获取或创建分区队列
        synchronized (deque) {  // 同步访问队列
            if (transactionManager != null)  // 如果启用了事务
                insertInSequenceOrder(deque, batch);  // 按序列号顺序插入
            else
                deque.addFirst(batch);  // 直接插入队首
        }
    }

    /**
     * 分裂被拒绝的大批次并将分裂后的批次重新入队到累加器中
     * 
     * @param bigBatch 要分裂的大批次
     * @return 分裂后的批次数量
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // 重置压缩率估计值为初始值或大批次压缩率中的较大值
        // 选择最保守的方式以确保分裂不会过于频繁
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression.type(),
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        
        // 执行批次分裂
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);  // 按配置的批次大小进行分裂
        int numSplitBatches = dq.size();  // 记录分裂后的批次数量
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);  // 获取分区队列
        
        // 处理分裂后的每个批次
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();  // 从后向前处理分裂的批次
            incomplete.add(batch);  // 添加到未完成批次集合
            
            // 将新分裂的批次视为未尝试发送过
            synchronized (partitionDequeue) {  // 同步访问队列
                if (transactionManager != null) {  // 如果启用了事务
                    // 跟踪新创建的批次,因为它们已经分配了序列号
                    transactionManager.addInFlightBatch(batch);  // 添加到飞行中批次
                    insertInSequenceOrder(partitionDequeue, batch);  // 按序列号顺序插入
                } else {
                    partitionDequeue.addFirst(batch);  // 直接插入队首
                }
            }
        }
        return numSplitBatches;  // 返回分裂后的批次数量
    }

    /**
     * 按序列号顺序将批次插入队列
     * 当请求重试且有多个请求同时发送到分区时,需要额外工作来确保队列顺序
     * 如果第一个飞行中请求追加失败,则所有后续飞行中请求也会失败,因为序列号不会被接受
     * 
     * 一旦批次开始重试,对该分区的请求就会减少到单个飞行中请求
     * 当后续批次按序列顺序返回时,它们必须放在队列更靠后的位置
     * 
     * 注意:这假设队列中所有具有已分配序列的批次都具有当前的生产者ID
     * 如果生产者ID已更改,我们不会尝试重新排序消息,而是抛出IllegalStateException
     * 
     * @param deque 目标队列
     * @param batch 要插入的批次
     */
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // 重新入队时启用了幂等性,重新入队的批次必须有序列号
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        // 验证批次是否被跟踪为飞行中请求的一部分
        if (!transactionManager.hasInflightBatches(batch.topicPartition))
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        // 获取队列中的第一个批次
        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // 如果直接插入队首会违反序列顺序,说明传入的批次应该放在更靠后的位置
            // 需要找到正确的位置插入传入的批次
            // 只有当我们有多个发送到不同代理的飞行中请求并且需要重试飞行中批次时,才会进入这个分支
            
            // 由于我们每次只重新入队一个批次,并始终确保队列按序列排序
            // 因此每次只需要对飞行中批次的一个子集进行简单的线性扫描,就能找到队列中的正确位置
            List<ProducerBatch> orderedBatches = new ArrayList<>();  // 存储需要重新排序的批次
            // 收集序列号小于当前批次的所有批次
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            // 记录重排序操作的日志
            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            
            // 此时要么到达了没有序列号的批次(即从未被排空,因此默认是有序的)
            // 要么队列前面的批次序列号大于传入批次
            // 这就是添加传入批次的正确位置
            deque.addFirst(batch);  // 插入传入的批次

            // 现在需要按正确的顺序重新插入之前排队的批次
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));  // 从后向前重新插入
            }

            // 此时,传入的批次已经按照其序列号被放置在队列的正确位置
        } else {
            deque.addFirst(batch);  // 如果没有序列顺序冲突,直接插入队首
        }
    }

    /**
     * 如果批次准备就绪,将leader节点添加到就绪节点集合中
     * 
     * @param exhausted 缓冲池是否已耗尽,true表示缓冲池中有等待分配的线程
     * @param part 分区信息
     * @param leader 该分区的leader节点
     * @param waitedTimeMs 批次已等待的时间(毫秒)
     * @param backingOff 是否正在退避(发生错误后的重试等待)
     * @param backoffAttempts 用于计算退避延迟的重试次数
     * @param full 批次是否已满
     * @param nextReadyCheckDelayMs 下次检查的延迟时间
     * @param readyNodes 就绪节点集合(待填充)
     * @return 更新后的下次检查延迟时间
     */
    private long batchReady(boolean exhausted, TopicPartition part, Node leader,
                            long waitedTimeMs, boolean backingOff, int backoffAttempts,
                            boolean full, long nextReadyCheckDelayMs, Set<Node> readyNodes) {
        // 如果leader节点不在就绪集合中且分区未被静音
        if (!readyNodes.contains(leader) && !isMuted(part)) {
            // 计算需要等待的时间:如果正在退避则使用退避时间,否则使用lingerMs
            long timeToWaitMs = backingOff ? retryBackoff.backoff(backoffAttempts > 0 ? backoffAttempts - 1 : 0) : lingerMs;
            // 检查批次是否已过期(等待时间超过了允许的最大等待时间)
            boolean expired = waitedTimeMs >= timeToWaitMs;
            // 检查事务是否正在完成
            boolean transactionCompleting = transactionManager != null && transactionManager.isCompleting();
            // 判断批次是否可发送,满足以下任一条件:
            // 1. 批次已满
            // 2. 等待时间已过期
            // 3. 缓冲池已耗尽
            // 4. 累加器已关闭
            // 5. 正在执行刷新操作
            // 6. 事务正在完成
            boolean sendable = full
                    || expired
                    || exhausted
                    || closed
                    || flushInProgress()
                    || transactionCompleting;
            
            if (sendable && !backingOff) {
                // 如果批次可发送且不在退避状态,将leader添加到就绪节点集合
                readyNodes.add(leader);
            } else {
                // 否则计算剩余等待时间
                long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                // 更新下次检查延迟时间(取较小值)
                // 注意:这是一个保守估计,因为未就绪的分区可能稍后会有可发送的数据
                // 但这已足够好,因为我们会在剩余时间后重新唤醒并检查
                nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
            }
        }
        return nextReadyCheckDelayMs;
    }

    /**
     * 遍历分区以检查哪些分区有准备就绪的批次,并将这些分区的leader节点收集到就绪节点集合中。
     * 如果分区没有leader,则将主题添加到无leader主题集合中。
     * 该方法同时计算自适应分区的统计信息。
     *
     * @param metadataSnapshot      集群元数据快照
     * @param nowMs                 当前时间戳(毫秒)
     * @param topic                 主题名称
     * @param topicInfo             主题信息
     * @param nextReadyCheckDelayMs 下次检查的延迟时间
     * @param readyNodes            就绪节点集合(待填充)
     * @param unknownLeaderTopics   无leader主题集合(待填充)
     * @return 更新后的下次检查延迟时间
     */
    private long partitionReady(MetadataSnapshot metadataSnapshot, long nowMs, String topic,
                                TopicInfo topicInfo,
                                long nextReadyCheckDelayMs, Set<Node> readyNodes, Set<String> unknownLeaderTopics) {
        // 获取主题的批次队列映射
        ConcurrentMap<Integer, Deque<ProducerBatch>> batches = topicInfo.batches;
        
        // 为自适应分区收集可用分区的队列大小
        int[] queueSizes = null;
        int[] partitionIds = null;
        // 只有在启用自适应分区且所有分区都至少有一个批次时才进行自适应分区
        if (enableAdaptivePartitioning && batches.size() >= metadataSnapshot.cluster().partitionsForTopic(topic).size()) {
            // 我们要等到所有分区都至少调度了一个批次(即在batches映射中有对应的条目)才进行自适应分区
            // 否则只进行均匀分区。这是因为我们从batches映射中构建队列大小,
            // 如果batches映射中缺少某个条目,自适应分区逻辑就不会感知到它,也就不会切换到它
            queueSizes = new int[batches.size()];
            partitionIds = new int[queueSizes.length];
        }

        // 队列大小数组的索引
        int queueSizesIndex = -1;
        // 检查缓冲池是否有等待线程
        boolean exhausted = this.free.queued() > 0;
        
        // 遍历每个分区的批次队列
        for (Map.Entry<Integer, Deque<ProducerBatch>> entry : batches.entrySet()) {
            // 创建主题分区对象
            TopicPartition part = new TopicPartition(topic, entry.getKey());

            // 获取分区的leader节点
            Node leader = metadataSnapshot.cluster().leaderFor(part);
            // 如果启用了自适应分区且有leader,记录分区信息
            if (leader != null && queueSizes != null) {
                ++queueSizesIndex;
                assert queueSizesIndex < queueSizes.length;
                partitionIds[queueSizesIndex] = part.partition();
            }

            // 获取分区的批次队列
            Deque<ProducerBatch> deque = entry.getValue();

            // 声明批次状态变量
            final long waitedTimeMs;     // 批次已等待时间
            final boolean backingOff;    // 是否正在退避
            final int backoffAttempts;   // 重试次数
            final int dequeSize;         // 队列大小
            final boolean full;          // 是否已满

            // 获取leader的epoch
            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(part);

            // 这个循环在分区数量大时特别热门,所以:
            // 1. 我们应该避免增加应用线程调用send()和后台线程运行runOnce()之间的同步
            // 2. 我们只在同步块内执行最少的必要操作,因为这个锁也用于同步生产者线程尝试append()到分区/批次

            synchronized (deque) {
                // 获取队列中第一个批次,队列经常为空,尤其是在分区数量大时,所以尽早退出
                ProducerBatch batch = deque.peekFirst();
                if (batch == null) {
                    continue;
                }

                // 获取批次状态信息
                waitedTimeMs = batch.waitedTimeMs(nowMs);                // 计算等待时间
                batch.maybeUpdateLeaderEpoch(leaderEpoch);              // 更新leader epoch
                backingOff = shouldBackoff(batch.hasLeaderChangedForTheOngoingRetry(), batch, waitedTimeMs);  // 检查是否需要退避
                backoffAttempts = batch.attempts();                      // 获取重试次数
                dequeSize = deque.size();                               // 获取队列大小
                full = dequeSize > 1 || batch.isFull();                 // 检查是否已满
            }

            if (leader == null) {
                // 这是一个没有已知leader但有消息要发送的分区
                // 注意:当队列为空时,条目当前不会从batches中移除
                unknownLeaderTopics.add(part.topic());
            } else {
                // 如果启用了自适应分区,记录队列大小
                if (queueSizes != null)
                    queueSizes[queueSizesIndex] = dequeSize;
                    
                // 检查分区可用性超时
                if (partitionAvailabilityTimeoutMs > 0) {
                    // 如果broker长时间未响应,检查是否要将分区从可用分区列表中排除
                    NodeLatencyStats nodeLatencyStats = nodeStats.get(leader.id());
                    if (nodeLatencyStats != null) {
                        // 注意:读取指标时没有同步,所以我们先读取ready时间
                        // 以避免在指标更新时意外将分区标记为不可用
                        long readyTimeMs = nodeLatencyStats.readyTimeMs;
                        if (readyTimeMs - nodeLatencyStats.drainTimeMs > partitionAvailabilityTimeoutMs)
                            --queueSizesIndex;
                    }
                }

                // 检查批次是否准备就绪
                nextReadyCheckDelayMs = batchReady(exhausted, part, leader, waitedTimeMs, backingOff,
                    backoffAttempts, full, nextReadyCheckDelayMs, readyNodes);
            }
        }

        // 我们已收集了该主题所有分区的队列大小,现在可以计算负载统计信息
        // 注意:统计信息是就地计算的,会修改queueSizes数组
        topicInfo.builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, queueSizesIndex + 1);
        return nextReadyCheckDelayMs;
    }

    /**
     * 获取已准备好发送的节点列表,以及任何未就绪分区下次可发送的最早时间;
     * 同时返回是否存在未知leader的主题标志。
     * <p>
     * 一个目标节点准备好发送数据需要满足以下条件:
     * <ol>
     * <li>至少有一个分区不在退避发送状态
     * <li><b>并且</b>这些分区没有被静音(当{@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   设置为1时,用于防止消息重排序)</li>
     * <li><b>并且满足<i>以下任一</i></b>条件</li>
     * <ul>
     *     <li>记录集已满</li>
     *     <li>记录集在累加器中停留时间已达到lingerMs毫秒</li>
     *     <li>累加器内存耗尽且有线程因等待数据而阻塞(此时所有分区立即被视为就绪)</li>
     *     <li>累加器已关闭</li>
     * </ul>
     * </ol>
     * 
     * @param metadataSnapshot 集群元数据快照
     * @param nowMs 当前时间戳(毫秒)
     * @return 包含就绪节点集合、下次检查延迟和未知leader主题的结果对象
     */
    public ReadyCheckResult ready(MetadataSnapshot metadataSnapshot, long nowMs) {
        // 创建就绪节点集合
        Set<Node> readyNodes = new HashSet<>();
        // 初始化下次检查延迟为最大值
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        // 创建未知leader主题集合
        Set<String> unknownLeaderTopics = new HashSet<>();
        // 逐个主题检查,以便获取主题内分区的队列大小并计算累积频率表(用于分区器)
        for (Map.Entry<String, TopicInfo> topicInfoEntry : this.topicInfoMap.entrySet()) {
            final String topic = topicInfoEntry.getKey();
            nextReadyCheckDelayMs = partitionReady(metadataSnapshot, nowMs, topic, topicInfoEntry.getValue(), nextReadyCheckDelayMs, readyNodes, unknownLeaderTopics);
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * 检查是否存在尚未排空的批次
     * 
     * @return 如果存在未排空的批次则返回true,否则返回false
     */
    public boolean hasUndrained() {
        // 遍历所有主题信息
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            // 遍历主题下所有分区的批次队列
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                synchronized (deque) {
                    // 如果队列非空,说明存在未排空的批次
                    if (!deque.isEmpty())
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断是否应该对批次进行退避
     * 
     * @param hasLeaderChanged 分区leader是否发生变更
     * @param batch 要检查的生产者批次
     * @param waitedTimeMs 已等待时间(毫秒)
     * @return 如果需要退避则返回true,否则返回false
     */
    private boolean shouldBackoff(boolean hasLeaderChanged, final ProducerBatch batch, final long waitedTimeMs) {
        // 如果批次已重试且等待时间小于退避时间,则需要继续等待
        boolean shouldWaitMore = batch.attempts() > 0 && waitedTimeMs < retryBackoff.backoff(batch.attempts() - 1);
        // 如果leader未变更且需要继续等待,则进行退避
        boolean shouldBackoff = !hasLeaderChanged && shouldWaitMore;
        
        // 记录退避相关的日志
        if (log.isTraceEnabled()) {
            if (shouldBackoff) {
                log.trace(
                    "批次 {} 将进行退避", batch);
            } else {
                log.trace(
                    "批次 {} 不需要退避, 是否需要继续等待 {}, leader是否变更 {}", batch,
                    shouldWaitMore, hasLeaderChanged);
            }
        } else if (log.isDebugEnabled() && hasLeaderChanged) {
            // 在DEBUG级别添加简略日志
            log.debug("批次 {} 的leader已变更,跳过退避", batch);
        }
        return shouldBackoff;
    }

    /**
     * 判断是否应该停止为分区排空批次
     * 
     * @param first 分区队列中的第一个批次
     * @param tp 主题分区
     * @return 如果应该停止排空则返回true,否则返回false
     */
    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch;
        // 只在启用事务时进行检查
        if (transactionManager != null) {
            // 检查是否允许向该分区发送数据
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            // 获取生产者ID和epoch
            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // 如果生产者ID无效,需要等待刷新后才能发送批次
                return true;

            // 处理没有序列号的批次
            if (!first.hasSequence()) {
                // 如果分区有使用不同epoch的在途批次,停止排空以避免序列号错乱
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    return true;
                }

                // 如果存在未解决的序列号状态,停止排空
                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    return true;
            }

            // 获取分区第一个在途批次的序列号
            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            // 如果队列中的批次已有序列号(表示正在重试),则等待下一个批次就绪再排空
            // 这样可以确保按序列号顺序处理批次,有效将在途请求数限制为1
            return firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                    && first.baseSequence() != firstInFlightSequence;
        }
        return false;
    }

    /**
     * 为单个节点排空批次
     * 
     * @param metadataSnapshot 集群元数据快照
     * @param node 目标节点
     * @param maxSize 最大请求大小
     * @param now 当前时间戳
     * @return 已就绪可发送的批次列表
     */
    private List<ProducerBatch> drainBatchesForOneNode(MetadataSnapshot metadataSnapshot, Node node, int maxSize, long now) {
        int size = 0;  // 累计已排空批次的总大小
        // 获取节点上的所有分区信息
        List<PartitionInfo> parts = metadataSnapshot.cluster().partitionsForNode(node.id());
        List<ProducerBatch> ready = new ArrayList<>();  // 存储已就绪的批次
        if (parts.isEmpty())
            return ready;
            
        // 为避免饥饿,每个节点维护自己的排空索引
        int drainIndex = getDrainIndex(node.idString());
        int start = drainIndex = drainIndex % parts.size();
        
        do {
            // 获取当前要处理的分区信息
            PartitionInfo part = parts.get(drainIndex);
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            
            // 更新排空索引并移动到下一个分区
            updateDrainIndex(node.idString(), drainIndex);
            drainIndex = (drainIndex + 1) % parts.size();
            
            // 如果分区已被静音(有在途请求),则跳过
            if (isMuted(tp))
                continue;
                
            // 获取分区的批次队列
            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                continue;

            // 获取分区的leader epoch
            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(tp);

            final ProducerBatch batch;
            synchronized (deque) {
                // 获取队列中第一个批次
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    continue;

                // 更新批次的leader epoch并检查是否需要退避
                first.maybeUpdateLeaderEpoch(leaderEpoch);
                if (shouldBackoff(first.hasLeaderChangedForTheOngoingRetry(), first, first.waitedTimeMs(now)))
                    continue;

                // 检查添加此批次是否会超过最大请求大小
                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // 注意:在极少数情况下,单个批次可能因压缩而大于请求大小
                    // 这种情况下我们最终会在单个请求中发送该批次
                    break;
                } else {
                    // 检查是否应该停止为该分区排空批次
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        break;
                }

                // 从队列中移除并获取批次
                batch = deque.pollFirst();

                // 处理事务相关的状态
                boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                ProducerIdAndEpoch producerIdAndEpoch =
                    transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                    
                // 如果批次没有序列号且启用了事务,则设置生产者状态
                if (producerIdAndEpoch != null && !batch.hasSequence()) {
                    // 更新分区的生产者ID和epoch
                    transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                    // 设置批次的生产者状态(ID、序列号等)
                    batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                    // 增加序列号
                    transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                    log.debug("为发往分区 {} 的批次分配生产者ID {} 和epoch {}, 基础序列号为 {}", 
                        tp, producerIdAndEpoch.producerId,
                        producerIdAndEpoch.epoch, batch.baseSequence());

                    // 将批次添加到事务管理器的在途批次跟踪中
                    transactionManager.addInFlightBatch(batch);
                }
            }

            // 在锁外处理剩余工作
            // close()操作特别耗时
            batch.close();
            size += batch.records().sizeInBytes();
            ready.add(batch);

            // 标记批次已被排空
            batch.drained(now);
        } while (start != drainIndex);
        return ready;
    }

    /**
     * 获取指定节点的排空索引
     * 如果节点不存在排空索引,则初始化为0
     * 
     * @param idString 节点ID字符串
     * @return 节点的排空索引值
     */
    private int getDrainIndex(String idString) {
        // 如果节点不存在排空索引,则初始化为0并返回
        return nodesDrainIndex.computeIfAbsent(idString, s -> 0);
    }

    /**
     * 更新指定节点的排空索引
     * 
     * @param idString 节点ID字符串
     * @param drainIndex 新的排空索引值
     */
    private void updateDrainIndex(String idString, int drainIndex) {
        // 更新节点的排空索引值
        nodesDrainIndex.put(idString, drainIndex);
    }

    /**
     * 排空指定节点的所有数据,并将其整理成一个批次列表
     * 每个节点的批次总大小不超过指定的最大值
     * 该方法会尝试避免重复选择相同的主题-节点组合
     *
     * @param metadataSnapshot 当前集群元数据快照
     * @param nodes 需要排空的节点列表
     * @param maxSize 每个节点最大允许排空的字节数
     * @param now 当前UNIX时间戳(毫秒)
     * @return 每个节点的ProducerBatch列表,总大小不超过请求的maxSize
     */
    public Map<Integer, List<ProducerBatch>> drain(MetadataSnapshot metadataSnapshot, Set<Node> nodes, int maxSize, long now) {
        // 如果节点列表为空,返回空Map
        if (nodes.isEmpty())
            return Collections.emptyMap();

        // 创建结果Map,用于存储每个节点的批次列表
        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        // 遍历每个节点,排空其数据
        for (Node node : nodes) {
            // 获取节点准备好发送的批次列表
            List<ProducerBatch> ready = drainBatchesForOneNode(metadataSnapshot, node, maxSize, now);
            // 将批次列表添加到结果Map中
            batches.put(node.id(), ready);
        }
        return batches;
    }

    /**
     * 更新节点的延迟统计信息
     * 
     * @param nodeId 节点ID
     * @param nowMs 当前时间戳(毫秒)
     * @param canDrain 是否可以排空数据
     */
    public void updateNodeLatencyStats(Integer nodeId, long nowMs, boolean canDrain) {
        // 如果分区可用性超时功能已关闭,则不更新统计信息
        if (partitionAvailabilityTimeoutMs <= 0)
            return;

        // 当sender获取到一个有数据要发送但节点未就绪的节点时(通过ready()函数返回)
        // 我们只更新readyTime,这样时间差就能反映节点未就绪的持续时间
        // 然后我们可以暂时从可用分区列表中移除该节点处理的分区
        // 这样分区器就不会选择这些分区
        // 注意:指标更新没有同步机制,所以先更新drainTimeMs
        // 以避免在更新之间读取值时意外将分区标记为不可用
        NodeLatencyStats nodeLatencyStats = nodeStats.computeIfAbsent(nodeId, id -> new NodeLatencyStats(nowMs));
        if (canDrain)
            nodeLatencyStats.drainTimeMs = nowMs;  // 更新最后一次成功排空的时间
        nodeLatencyStats.readyTimeMs = nowMs;  // 更新节点就绪时间
    }

    /**
     * 获取节点的延迟统计信息(用于测试)
     */
    public NodeLatencyStats getNodeLatencyStats(Integer nodeId) {
        return nodeStats.get(nodeId);
    }

    /**
     * 获取主题的内置分区器(用于测试)
     */
    public BuiltInPartitioner getBuiltInPartitioner(String topic) {
        return topicInfoMap.get(topic).builtInPartitioner;
    }

    /**
     * 获取下一个批次将要过期的最早绝对时间(毫秒)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

    /**
     * 获取指定主题分区的消息队列(用于测试)
     */
    public Deque<ProducerBatch> getDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.get(tp.topic());
        if (topicInfo == null)
            return null;
        return topicInfo.batches.get(tp.partition());
    }

    /**
     * 获取指定主题分区的消息队列,如果不存在则创建
     * 
     * @param tp 主题分区
     * @return 该主题分区的消息队列
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        // 获取或创建主题信息
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(tp.topic(),
                k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize)));
        // 获取或创建分区的消息队列
        return topicInfo.batches.computeIfAbsent(tp.partition(), k -> new ArrayDeque<>());
    }

    /**
     * 创建内置分区器
     */
    BuiltInPartitioner createBuiltInPartitioner(LogContext logContext, String topic, int stickyBatchSize) {
        return new BuiltInPartitioner(logContext, topic, stickyBatchSize);
    }

    /**
     * 释放记录批次占用的资源
     * 
     * @param batch 要释放的批次
     */
    public void deallocate(ProducerBatch batch) {
        // 从未完成批次集合中移除
        incomplete.remove(batch);
        // 只有非拆分批次才需要释放缓冲池资源
        // 因为拆分批次是在缓冲池外分配的
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * 获取缓冲池的剩余可用内存大小(字节)
     * 包访问权限,用于单元测试
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * 检查是否有线程正在等待刷新操作完成
     * 包访问权限,用于测试
     * 
     * @return 如果有刷新操作正在进行则返回true
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /**
     * 启动累加器中数据的刷新操作,使所有请求立即准备就绪
     * <p>
     * 该方法通过增加正在进行的刷新操作计数来标记刷新操作的开始。
     * 这允许多个线程同时发起刷新操作,并通过计数器追踪所有正在进行的刷新。
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();  // 原子递增刷新操作计数
    }

    /**
     * 检查当前是否有线程正在追加消息
     * <p>
     * 该方法用于确认是否存在正在进行的追加操作,这对于安全地关闭生产者和中止批次很重要。
     * 
     * @return 如果有正在进行的追加操作则返回true,否则返回false
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;  // 检查追加操作计数是否大于0
    }

    /**
     * 将所有分区标记为可发送状态,并阻塞等待发送完成
     * <p>
     * 该方法会等待所有未完成的生产请求完成。它通过以下步骤实现:
     * 1. 获取刷新时刻所有未完成的ProduceRequestResult副本
     * 2. 等待每个请求完成
     * 3. 确保在完成后减少刷新计数
     * 
     * @throws InterruptedException 如果等待过程中线程被中断
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // 获取所有未完成的ProduceRequestResult副本
            // 注意不要持有ProducerBatch的引用以允许垃圾回收
            // sender线程会从原始incomplete集合中移除ProducerBatch
            for (ProduceRequestResult result : this.incomplete.requestResults())
                result.await();  // 等待每个请求完成
        } finally {
            this.flushesInProgress.decrementAndGet();  // 减少刷新操作计数
        }
    }

    /**
     * 检查是否存在任何待处理的批次(无论是否已发送)
     * <p>
     * 该方法用于判断累加器中是否还有未完成的批次,这对于确保所有消息都已处理完成很重要。
     * 
     * @return 如果存在未完成的批次则返回true,否则返回false
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();  // 检查未完成批次集合是否为空
    }

    /**
     * 该函数仅在sender被强制关闭时调用,它会使所有未完成的批次失败并返回
     * <p>
     * 实现说明:
     * 1. 持续中止未完成的批次,直到没有线程在尝试追加
     * 2. 这样做是为了:
     *    - 避免丢失批次
     *    - 在追加线程因缓冲区已满而阻塞时释放内存
     * 3. 最后再执行一次中止操作,以处理最后一个追加线程可能添加的新批次
     */
    public void abortIncompleteBatches() {
        // 持续中止批次直到没有正在进行的追加操作
        do {
            abortBatches();  // 中止所有未完成的批次
        } while (appendsInProgress());  // 检查是否还有追加操作在进行
        
        // 此时没有线程会追加任何消息,因为它们会看到关闭标志
        // 执行最后一次中止操作,处理最后一个追加线程可能添加的新批次
        abortBatches();
        this.topicInfoMap.clear();  // 清空主题信息映射
    }

    /**
     * 遍历未完成的批次并中止它们
     * <p>
     * 使用默认的强制关闭异常来中止所有未完成的批次
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));  // 使用强制关闭异常
    }

    /**
     * 中止所有未完成的批次(无论它们是否已经发送)
     * <p>
     * 实现步骤:
     * 1. 复制所有未完成批次以避免并发修改
     * 2. 对每个批次:
     *    - 获取其分区队列并同步访问
     *    - 中止记录追加
     *    - 从队列中移除批次
     *    - 中止批次并释放资源
     *
     * @param reason 中止批次的原因,将传递给回调函数
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {  // 复制所有未完成批次
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);  // 获取分区队列
            synchronized (dq) {  // 同步访问队列
                batch.abortRecordAppends();  // 中止记录追加
                dq.remove(batch);  // 从队列中移除批次
            }
            batch.abort(reason);  // 中止批次
            deallocate(batch);  // 释放批次占用的资源
        }
    }

    /**
     * 中止所有尚未被排空(发送)的批次
     * 
     * @param reason 中止的原因,将传递给每个被中止的批次
     */
    void abortUndrainedBatches(RuntimeException reason) {
        // 遍历所有未完成的批次
        for (ProducerBatch batch : incomplete.copyAll()) {
            // 获取该批次所属主题分区的队列
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            boolean aborted = false;
            synchronized (dq) {  // 同步访问队列
                // 判断批次是否需要中止:
                // 1. 如果存在事务管理器且批次没有序列号,或
                // 2. 如果不存在事务管理器且批次未关闭
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    batch.abortRecordAppends();  // 中止批次中的记录追加
                    dq.remove(batch);  // 从队列中移除该批次
                }
            }
            if (aborted) {
                batch.abort(reason);  // 中止批次,传入中止原因
                deallocate(batch);  // 释放批次占用的资源
            }
        }
    }

    /**
     * 将指定的主题分区设置为静音状态
     * 处于静音状态的分区将暂时不会发送数据
     * 
     * @param tp 要静音的主题分区
     */
    public void mutePartition(TopicPartition tp) {
        muted.add(tp);  // 将分区添加到静音集合中
    }

    /**
     * 解除指定主题分区的静音状态
     * 解除静音后,分区可以恢复发送数据
     * 
     * @param tp 要解除静音的主题分区
     */
    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);  // 从静音集合中移除分区
    }

    /**
     * 关闭累加器并强制排空所有记录缓冲区
     * 关闭操作会:
     * 1. 将累加器标记为已关闭状态
     * 2. 关闭并释放内存缓冲池
     */
    public void close() {
        this.closed = true;  // 标记累加器为已关闭状态
        this.free.close();  // 关闭内存缓冲池
    }

    /**
     * 内置分区器的配置类
     * 用于控制分区选择的行为和可用性检测
     */
    public static final class PartitionerConfig {
        private final boolean enableAdaptivePartitioning;  // 是否启用自适应分区
        private final long partitionAvailabilityTimeoutMs;  // 分区可用性超时时间

        /**
         * 创建分区器配置
         *
         * @param enableAdaptivePartitioning 如果为true,分区切换会根据broker负载自适应调整;
         *                                   如果为false,分区切换采用随机方式
         * @param partitionAvailabilityTimeoutMs 如果broker在指定时间内无法处理某个分区的生产请求,
         *                                       该分区将被分区器标记为不可用。
         *                                       如果设置为0,则禁用此逻辑
         */
        public PartitionerConfig(boolean enableAdaptivePartitioning, long partitionAvailabilityTimeoutMs) {
            this.enableAdaptivePartitioning = enableAdaptivePartitioning;  // 设置是否启用自适应分区
            this.partitionAvailabilityTimeoutMs = partitionAvailabilityTimeoutMs;  // 设置分区可用性超时时间
        }

        /**
         * 创建默认的分区器配置
         * 默认不启用自适应分区,且不启用分区可用性检测
         */
        public PartitionerConfig() {
            this(false, 0);  // 使用默认值初始化配置
        }
    }

    /**
     * 记录追加到累加器后的元数据信息
     * 包含追加操作的结果状态和相关指标
     */
    public static final class RecordAppendResult {
        public final FutureRecordMetadata future;      // 用于获取追加记录的元数据的Future对象
        public final boolean batchIsFull;              // 批次是否已满
        public final boolean newBatchCreated;          // 是否创建了新的批次
        public final int appendedBytes;                // 追加的字节数

        /**
         * 创建记录追加结果
         *
         * @param future 包含追加记录元数据的Future对象
         * @param batchIsFull 批次是否已满标志
         * @param newBatchCreated 是否创建新批次标志
         * @param appendedBytes 本次追加的字节数
         */
        public RecordAppendResult(FutureRecordMetadata future,
                                  boolean batchIsFull,
                                  boolean newBatchCreated,
                                  int appendedBytes) {
            this.future = future;              // 设置元数据Future
            this.batchIsFull = batchIsFull;    // 设置批次是否已满
            this.newBatchCreated = newBatchCreated;  // 设置是否创建新批次
            this.appendedBytes = appendedBytes;  // 设置追加的字节数
        }
    }

    /**
     * 传递给append方法的回调接口
     * 扩展了基础Callback接口,增加了设置分区的功能
     */
    public interface AppendCallbacks extends Callback {
        /**
         * 设置分区号
         * 在调用append时,分区可能尚未计算出来,
         * 待分区确定后会通过此方法设置实际的分区号
         * 
         * @param partition 确定的分区号
         */
        void setPartition(int partition);
    }

    /**
     * 记录累加器中至少有一个完整记录批次的节点集合
     * 用于跟踪哪些节点有数据可以发送
     */
    public static final class ReadyCheckResult {
        public final Set<Node> readyNodes;              // 有完整批次待发送的节点集合
        public final long nextReadyCheckDelayMs;        // 下次检查准备状态的延迟时间(毫秒)
        public final Set<String> unknownLeaderTopics;   // 未知leader的主题集合

        /**
         * 创建就绪检查结果
         *
         * @param readyNodes 有数据待发送的节点集合
         * @param nextReadyCheckDelayMs 下次检查的延迟时间
         * @param unknownLeaderTopics 未知leader的主题集合
         */
        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;  // 设置就绪节点集合
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;  // 设置下次检查延迟
            this.unknownLeaderTopics = unknownLeaderTopics;  // 设置未知leader主题
        }
    }

    /**
     * 每个主题的相关信息
     * 包含该主题的分区批次队列和分区器
     */
    private static class TopicInfo {
        // 主题的分区批次映射表
        // 键为分区号,值为该分区的批次队列
        public final ConcurrentMap<Integer /*partition*/, Deque<ProducerBatch>> batches = new CopyOnWriteMap<>();
        
        // 该主题使用的内置分区器
        public final BuiltInPartitioner builtInPartitioner;

        /**
         * 创建主题信息对象
         *
         * @param builtInPartitioner 用于该主题的内置分区器
         */
        public TopicInfo(BuiltInPartitioner builtInPartitioner) {
            this.builtInPartitioner = builtInPartitioner;  // 设置分区器
        }
    }

    /**
     * 节点延迟统计信息
     * 用于自适应分区分配,记录每个节点的性能指标
     * 该类可见性为public是为了便于测试
     */
    public static final class NodeLatencyStats {
        // 节点上次有批次准备好发送的时间(毫秒)
        // volatile保证多线程可见性
        public volatile long readyTimeMs;
        
        // 节点上次成功排空(发送完)批次的时间(毫秒)
        // volatile保证多线程可见性
        public volatile long drainTimeMs;

        /**
         * 创建节点延迟统计对象
         *
         * @param nowMs 当前时间戳(毫秒)
         */
        NodeLatencyStats(long nowMs) {
            readyTimeMs = nowMs;   // 初始化准备时间
            drainTimeMs = nowMs;   // 初始化排空时间
        }
    }
}
