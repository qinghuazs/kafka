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

import kafka.server.ReplicaManager;
import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.ShareGroupAutoOffsetResetStrategy;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.PartitionAllData;
import org.apache.kafka.server.share.persister.PartitionErrorData;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdLeaderEpochData;
import org.apache.kafka.server.share.persister.PartitionStateBatchData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.server.share.persister.ReadShareGroupStateParameters;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.share.persister.WriteShareGroupStateParameters;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static kafka.server.share.ShareFetchUtils.offsetForEarliestTimestamp;
import static kafka.server.share.ShareFetchUtils.offsetForLatestTimestamp;
import static kafka.server.share.ShareFetchUtils.offsetForTimestamp;

/**
 * SharePartition用于跟踪在多个消费者之间共享的分区状态。
 * 该类维护从leader节点获取的正在处理中(in-flight)的记录状态。
 * 
 * 应用场景：
 * 1. 在Kafka的共享消费模式中，多个消费者可以共同消费同一个分区的数据
 * 2. 通过状态管理确保消息被正确投递和处理，避免重复消费或消息丢失
 * 3. 支持消息的确认和重试机制，保证消息处理的可靠性
 */
public class SharePartition {

    private static final Logger log = LoggerFactory.getLogger(SharePartition.class);

    /**
     * 空成员ID，用于表示某条记录未被任何成员获取
     * 使用UUID.ZERO作为默认值
     */
    static final String EMPTY_MEMBER_ID = Uuid.ZERO_UUID.toString();

    /**
     * SharePartitionState用于跟踪共享分区的状态
     * 分区状态决定了是否可以接收请求、是否需要初始化持久化状态、或是否初始化失败
     * 
     * 状态流转：
     * 1. EMPTY -> INITIALIZING: 开始初始化
     * 2. INITIALIZING -> ACTIVE: 初始化成功
     * 3. INITIALIZING -> FAILED: 初始化失败
     * 4. ACTIVE -> FENCED: 分区被封禁
     */
    // 用于测试可见
    enum SharePartitionState {
        /**
         * 分区为空，尚未初始化持久化状态
         * 这是分区创建后的初始状态
         */
        EMPTY,
        
        /**
         * 分区正在使用持久化状态进行初始化
         * 此状态表示正在从磁盘加载之前保存的状态
         */
        INITIALIZING,
        
        /**
         * 分区已激活且可以处理请求
         * 此状态表示分区可以正常工作
         */
        ACTIVE,
        
        /**
         * 分区初始化失败
         * 可能由于持久化数据损坏或其他系统错误导致
         */
        FAILED,
        
        /**
         * 分区被封禁，无法使用
         * 通常发生在分区需要维护或检测到异常时
         */
        FENCED
    }

    /**
     * RecordState用于跟踪从leader获取的记录状态
     * 记录状态决定了是否需要重新投递、是否可以移动获取偏移量、以及是否需要持久化到磁盘
     * 
     * 状态流转：
     * 1. AVAILABLE -> ACQUIRED: 记录被消费者获取
     * 2. ACQUIRED -> ACKNOWLEDGED: 记录被成功处理并确认
     * 3. ACQUIRED -> ARCHIVED: 记录因重试次数过多被归档
     * 4. ACQUIRED -> AVAILABLE: 记录获取超时，重新变为可用
     */
    // 用于测试可见
    enum RecordState {
        // 记录可用，等待被消费者获取
        AVAILABLE((byte) 0),
        // 记录已被消费者获取，正在处理中
        ACQUIRED((byte) 1),
        // 记录已被消费者确认处理完成
        ACKNOWLEDGED((byte) 2),
        // 记录已归档，不再投递
        ARCHIVED((byte) 4);

        // 状态对应的唯一标识符
        public final byte id;

        RecordState(byte id) {
            this.id = id;
        }

        /**
         * 验证当前状态是否可以转换到目标状态
         * 
         * 状态转换规则：
         * 1. AVAILABLE只能转换为ACQUIRED
         * 2. ACQUIRED可以转换为AVAILABLE/ACKNOWLEDGED/ARCHIVED
         * 3. ACKNOWLEDGED和ARCHIVED是终态，不能再转换
         * 4. 不允许转换到相同状态
         *
         * @param newState 目标状态，不能为null
         * @return 如果验证通过，返回目标状态
         * @throws IllegalStateException 如果状态转换验证失败
         */
        public RecordState validateTransition(RecordState newState) throws IllegalStateException {
            // 确保目标状态不为空
            Objects.requireNonNull(newState, "newState cannot be null");
            
            // 不允许转换到相同状态
            if (this == newState) {
                throw new IllegalStateException("The state transition is invalid as the new state is"
                    + "the same as the current state");
            }

            // ACKNOWLEDGED和ARCHIVED是终态，不能再转换
            if (this == ACKNOWLEDGED || this == ARCHIVED) {
                throw new IllegalStateException("The state transition is invalid from the current state: " + this);
            }

            // AVAILABLE只能转换为ACQUIRED
            if (this == AVAILABLE && newState != ACQUIRED) {
                throw new IllegalStateException("The state can only be transitioned to ACQUIRED from AVAILABLE");
            }

            // 此时状态转换只可能是：
            // 1. AVAILABLE -> ACQUIRED
            // 2. ACQUIRED -> AVAILABLE/ACKNOWLEDGED/ARCHIVED
            return newState;
        }

        /**
         * 根据状态ID获取对应的RecordState枚举值
         * 
         * @param id 状态ID
         * @return 对应的RecordState枚举值
         * @throws IllegalArgumentException 如果状态ID未知
         */
        public static RecordState forId(byte id) {
            switch (id) {
                case 0:
                    return AVAILABLE;
                case 1:
                    return ACQUIRED;
                case 2:
                    return ACKNOWLEDGED;
                case 4:
                    return ARCHIVED;
                default:
                    throw new IllegalArgumentException("Unknown record state id: " + id);
            }
        }
    }

    /**
     * 共享分区所属的消费者组ID
     * 用于标识和管理共享分区的消费者组
     */
    private final String groupId;

    /**
     * 共享分区的主题ID和分区信息
     * 用于唯一标识一个主题分区，包含主题ID和分区号
     */
    private final TopicIdPartition topicIdPartition;

    /**
     * 领导者纪元号，用于跟踪分区的版本
     * 在分区领导者变更时会递增，确保数据一致性
     */
    private final int leaderEpoch;

    /**
     * 正在处理中的记录状态缓存
     * 用途：
     * 1. 跟踪从leader获取的记录状态
     * 2. 决定记录是否需要重新获取或可以被确认/归档
     * 3. 当分区起始偏移量移动时，移除缓存中较早的记录
     * 
     * 设计：使用NavigableMap存储，key为批次的第一个偏移量
     */
    private final NavigableMap<Long, InFlightBatch> cachedState;

    /**
     * 用于同步访问正在处理中的记录的读写锁
     * 设计考虑：
     * 1. 使用ReadWriteLock支持并发读操作
     * 2. 写操作时确保线程安全
     * 3. 避免在高并发场景下的数据竞争
     */
    private final ReadWriteLock lock;

    /**
     * 标记是否需要重新计算下一个获取偏移量的标志
     * 应用场景：
     * 1. 当消费进度发生变化时
     * 2. 当需要重新平衡消费位置时
     */
    private final AtomicBoolean findNextFetchOffset;

    /**
     * 获取队列锁，确保同一分区不会在已有获取请求时重复进入获取队列
     * 设计考虑：
     * 1. 避免重复获取导致的资源浪费
     * 2. 确保获取请求的顺序性
     */
    private final AtomicBoolean fetchLock;

    /**
     * 最大正在处理中的消息数量限制
     * 设计目的：
     * 1. 防止消费者获取过多记录导致内存溢出
     * 2. 控制单个消费者的处理负载
     * 3. 实现背压机制
     */
    private final int maxInFlightMessages;

    /**
     * 消息最大投递次数限制
     * 应用场景：
     * 1. 防止消息无限重试，占用系统资源
     * 2. 识别处理异常的消息
     * 3. 实现死信队列机制
     */
    private final int maxDeliveryCount;

    /**
     * 消费者组配置管理器
     * 用于获取和管理动态的消费者组配置
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * 记录锁定时长的默认值（毫秒）
     * 应用场景：
     * 1. 限制消费者获取记录的锁定时间
     * 2. 超时后根据投递次数决定记录是重新可用还是归档
     * 3. 可被消费者组特定配置覆盖
     */
    private final int defaultRecordLockDurationMs;

    /**
     * 定时器，用于实现记录的获取锁定
     * 功能：
     * 1. 保证记录从获取状态转换为可用/归档状态的超时机制
     * 2. 处理超时的记录状态转换
     */
    private final Timer timer;

    /**
     * 时间服务，用于获取当前时间
     * 主要用于记录处理时间、计算超时等
     */
    private final Time time;

    /**
     * 状态持久化器
     * 功能：
     * 1. 将共享分区状态持久化到磁盘
     * 2. 在broker重启后恢复分区状态
     * 3. 确保消息处理的可靠性
     */
    private final Persister persister;

    /**
     * 共享分区状态变更监听器
     * 用于在分区状态发生变化时通知分区管理器
     */
    private final SharePartitionListener listener;

    /**
     * 共享分区的起始偏移量
     * 表示：
     * 1. 分区中缓存记录的起始位置
     * 2. 用于维护cachedState中的记录范围
     */
    private long startOffset;

    /**
     * 共享分区的结束偏移量
     * 表示：
     * 1. 已经从分区获取的记录的最新位置
     * 2. 用于追踪获取进度
     */
    private long endOffset;

    /**
     * 初始读取间隙偏移量
     * 用途：
     * 1. 跟踪从持久化器初始读取状态时的记录间隙
     * 2. 确保数据完整性和连续性
     */
    private InitialReadGapOffset initialReadGapOffset;

    /**
     * 最新获取偏移量的元数据
     * 用途：
     * 1. 更高效地估算最小字节需求
     * 2. 优化获取请求的性能
     */
    private final OffsetMetadata fetchOffsetMetadata;

    /**
     * 状态纪元号
     * 用途：
     * 1. 跟踪共享分区状态的版本
     * 2. 确保状态更新的顺序性和一致性
     */
    private int stateEpoch;

    /**
     * 分区状态
     * 用途：
     * 1. 跟踪共享分区的当前状态
     * 2. 控制分区的生命周期
     */
    private SharePartitionState partitionState;

    /**
     * 副本管理器
     * 功能：
     * 1. 检查延迟的共享获取请求是否可以完成
     * 2. 处理因获取锁超时导致的数据可用性
     */
    private final ReplicaManager replicaManager;

    /**
     * SharePartition的构造函数，用于创建一个新的共享分区实例
     * 该构造函数会将分区状态初始化为EMPTY
     *
     * @param groupId 消费者组ID，用于标识分区所属的消费者组
     * @param topicIdPartition 主题分区标识，包含主题ID和分区号
     * @param leaderEpoch 领导者纪元号，用于跟踪分区版本
     * @param maxInFlightMessages 最大处理中消息数量限制
     * @param maxDeliveryCount 消息最大投递次数
     * @param defaultRecordLockDurationMs 记录锁定时长默认值(毫秒)
     * @param timer 定时器，用于处理记录锁定超时
     * @param time 时间服务，用于获取当前时间
     * @param persister 状态持久化器
     * @param replicaManager 副本管理器
     * @param groupConfigManager 消费者组配置管理器
     * @param listener 分区状态变更监听器
     */
    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int leaderEpoch,
        int maxInFlightMessages,
        int maxDeliveryCount,
        int defaultRecordLockDurationMs,
        Timer timer,
        Time time,
        Persister persister,
        ReplicaManager replicaManager,
        GroupConfigManager groupConfigManager,
        SharePartitionListener listener
    ) {
        // 调用完整构造函数，并将初始状态设置为EMPTY
        this(groupId, topicIdPartition, leaderEpoch, maxInFlightMessages, maxDeliveryCount, defaultRecordLockDurationMs,
            timer, time, persister, replicaManager, groupConfigManager, SharePartitionState.EMPTY, listener);
    }

    /**
     * SharePartition的完整构造函数，允许指定初始分区状态
     * 初始化所有必要的字段和状态
     *
     * @param groupId 消费者组ID，用于标识分区所属的消费者组
     * @param topicIdPartition 主题分区标识，包含主题ID和分区号
     * @param leaderEpoch 领导者纪元号，用于跟踪分区版本
     * @param maxInFlightMessages 最大处理中消息数量限制
     * @param maxDeliveryCount 消息最大投递次数
     * @param defaultRecordLockDurationMs 记录锁定时长默认值(毫秒)
     * @param timer 定时器，用于处理记录锁定超时
     * @param time 时间服务，用于获取当前时间
     * @param persister 状态持久化器
     * @param replicaManager 副本管理器
     * @param groupConfigManager 消费者组配置管理器
     * @param sharePartitionState 初始分区状态
     * @param listener 分区状态变更监听器
     */
    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int leaderEpoch,
        int maxInFlightMessages,
        int maxDeliveryCount,
        int defaultRecordLockDurationMs,
        Timer timer,
        Time time,
        Persister persister,
        ReplicaManager replicaManager,
        GroupConfigManager groupConfigManager,
        SharePartitionState sharePartitionState,
        SharePartitionListener listener
    ) {
        // 初始化基本属性
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.leaderEpoch = leaderEpoch;
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        
        // 初始化状态管理相关的字段
        this.cachedState = new ConcurrentSkipListMap<>(); // 使用线程安全的跳表存储记录状态
        this.lock = new ReentrantReadWriteLock(); // 初始化读写锁
        this.findNextFetchOffset = new AtomicBoolean(false); // 初始化获取偏移量标志
        this.fetchLock = new AtomicBoolean(false); // 初始化获取锁
        
        // 初始化配置和服务相关的字段
        this.defaultRecordLockDurationMs = defaultRecordLockDurationMs;
        this.timer = timer;
        this.time = time;
        this.persister = persister;
        this.partitionState = sharePartitionState;
        this.replicaManager = replicaManager;
        this.groupConfigManager = groupConfigManager;
        
        // 初始化元数据相关的字段
        this.fetchOffsetMetadata = new OffsetMetadata();
        this.listener = listener;
    }

    /**
     * 尝试初始化共享分区，通过从持久化存储中读取状态来完成初始化
     * 
     * 初始化流程：
     * 1. 检查分区是否已经初始化
     * 2. 如果未初始化，将状态从EMPTY转换为INITIALIZING
     * 3. 从持久化存储读取状态并恢复
     * 4. 初始化成功后转换为ACTIVE状态
     * 
     * 状态处理：
     * - 如果分区已处于ACTIVE状态，直接返回成功
     * - 如果分区处于其他状态，返回异常（可重试）
     * - 初始化失败时，分区状态会设置为FAILED
     * 
     * 线程安全：
     * - 使用读写锁确保并发安全
     * - 同一时刻只允许一个请求执行初始化
     * 
     * @return 返回一个CompletableFuture
     *         - 初始化成功时完成
     *         - 初始化失败时返回异常
     */
    public CompletableFuture<Void> maybeInitialize() {
        log.debug("Maybe initialize share partition: {}-{}", groupId, topicIdPartition);
        
        // 检查分区是否已经初始化
        try {
            if (initializedOrThrowException()) return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        // 分区未初始化，尝试将状态从EMPTY转换为INITIALIZING
        // 获取锁以避免并发请求的处理
        try {
            if (!emptyToInitialState()) return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        // 创建Future用于异步处理初始化结果
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        // 从持久化存储读取分区状态
        persister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(this.groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicIdPartition.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionIdLeaderEpochData(topicIdPartition.partition(), leaderEpoch)))))
                .build())
            .build()
        ).whenComplete((result, exception) -> {
            Throwable throwable = null;
            lock.writeLock().lock();
            try {
                // 处理读取状态时的异常
                if (exception != null) {
                    log.error("Failed to initialize the share partition: {}-{}", groupId, topicIdPartition, exception);
                    throwable = exception;
                    return;
                }

                // 验证读取结果的有效性
                if (result == null || result.topicsData() == null || result.topicsData().size() != 1) {
                    log.error("Failed to initialize the share partition: {}-{}. Invalid state found: {}.",
                        groupId, topicIdPartition, result);
                    throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                    return;
                }

                // 验证主题分区信息的正确性
                TopicData<PartitionAllData> state = result.topicsData().get(0);
                if (state.topicId() != topicIdPartition.topicId() || state.partitions().size() != 1) {
                    log.error("Failed to initialize the share partition: {}-{}. Invalid topic partition response: {}.",
                        groupId, topicIdPartition, result);
                    throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                    return;
                }

                // 验证分区数据的正确性
                PartitionAllData partitionData = state.partitions().get(0);
                if (partitionData.partition() != topicIdPartition.partition()) {
                    log.error("Failed to initialize the share partition: {}-{}. Invalid partition response: {}.",
                        groupId, topicIdPartition, partitionData);
                    throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                    return;
                }

                // 检查是否有错误码
                if (partitionData.errorCode() != Errors.NONE.code()) {
                    KafkaException ex = fetchPersisterError(partitionData.errorCode(), partitionData.errorMessage());
                    log.error("Failed to initialize the share partition: {}-{}. Exception occurred: {}.",
                        groupId, topicIdPartition, partitionData);
                    throwable = ex;
                    return;
                }

                // 初始化分区的起始偏移量和状态纪元
                startOffset = startOffsetDuringInitialization(partitionData.startOffset());
                stateEpoch = partitionData.stateEpoch();

                // 处理持久化的状态批次
                List<PersisterStateBatch> stateBatches = partitionData.stateBatches();
                long gapStartOffset = -1; // 用于记录批次之间的间隙起始位置
                // previousBatchLastOffset用于追踪前一个批次的最后偏移量
                // 对于第一个批次，如果没有间隙，应该从startOffset开始
                // 因此初始值设为startOffset - 1
                long previousBatchLastOffset = startOffset - 1;
                
                // 遍历所有状态批次并重建内存中的状态
                for (PersisterStateBatch stateBatch : stateBatches) {
                    // 验证批次的起始偏移量不小于分区的起始偏移量
                    if (stateBatch.firstOffset() < startOffset) {
                        log.error("Invalid state batch found for the share partition: {}-{}. The base offset: {}"
                                + " is less than the start offset: {}.", groupId, topicIdPartition,
                            stateBatch.firstOffset(), startOffset);
                        throwable = new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                        return;
                    }
                    
                    // 检测批次之间是否存在间隙
                    if (gapStartOffset == -1 && stateBatch.firstOffset() > previousBatchLastOffset + 1) {
                        gapStartOffset = previousBatchLastOffset + 1;
                    }
                    previousBatchLastOffset = stateBatch.lastOffset();
                    
                    // 创建处理中的批次并加入缓存
                    InFlightBatch inFlightBatch = new InFlightBatch(EMPTY_MEMBER_ID, stateBatch.firstOffset(),
                        stateBatch.lastOffset(), RecordState.forId(stateBatch.deliveryState()), stateBatch.deliveryCount(), null);
                    cachedState.put(stateBatch.firstOffset(), inFlightBatch);
                }
                
                // 更新分区的结束偏移量
                if (!cachedState.isEmpty()) {
                    // 如果缓存不为空，设置findNextFetchOffset标志
                    // 确保不会遗漏任何可用的记录
                    findNextFetchOffset.set(true);
                    endOffset = cachedState.lastEntry().getValue().lastOffset();
                    
                    // 如果存在间隙，记录初始读取间隙信息
                    if (gapStartOffset != -1) {
                        initialReadGapOffset = new InitialReadGapOffset(endOffset, gapStartOffset);
                    }
                    
                    // 如果持久化状态中没有可用记录，更新缓存状态和偏移量
                    maybeUpdateCachedStateAndOffsets();
                } else {
                    // 如果缓存为空，结束偏移量等于起始偏移量
                    endOffset = startOffset;
                }
                
                // 设置分区状态为激活状态
                partitionState = SharePartitionState.ACTIVE;
            } catch (Exception e) {
                throwable = e;
            } finally {
                // 处理初始化结果
                boolean isFailed = throwable != null;
                if (isFailed) {
                    // 初始化失败时设置状态为FAILED
                    partitionState = SharePartitionState.FAILED;
                }
                // 释放写锁
                lock.writeLock().unlock();
                // 完成Future
                if (isFailed) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(null);
                }
            }
        });

        return future;
    }

    /**
     * 获取下一个应该从leader节点获取的偏移量
     * 
     * 设计目标：
     * 1. 确定下一个需要从leader获取的消息偏移量
     * 2. 处理消息确认和锁定超时导致的偏移量变化
     * 3. 优化获取性能，避免不必要的重新计算
     * 
     * 实现策略：
     * 1. 使用findNextFetchOffset标志控制是否需要重新计算
     * 2. 当标志为false时，直接返回缓存的偏移量
     * 3. 当标志为true时，遍历缓存状态找到第一个可用记录
     * 
     * 并发控制：
     * 1. 使用写锁保护整个计算过程
     * 2. 确保偏移量计算的原子性和一致性
     * 
     * @return 下一个应该从leader获取的偏移量
     */
    public long nextFetchOffset() {
        /*
        下一个获取偏移量的计算逻辑依赖于findNextFetchOffset标志：
        
        1. 标志为true时重新计算的场景：
           - 之前获取的记录被确认释放
           - 记录锁定时间到期
           - 共享会话关闭时记录被释放
        
        2. 重新计算过程：
           - 遍历cachedState找到第一条可用记录
           - 如果没有可用记录，设置为endOffset + 1
           - 计算完成后将标志设为false
        */
        lock.writeLock().lock();
        try {
            // 当cachedState中没有AVAILABLE状态的记录时，findNextFetchOffset为false
            if (!findNextFetchOffset.get()) {
                if (cachedState.isEmpty() || startOffset > cachedState.lastEntry().getValue().lastOffset()) {
                    // 两种情况返回endOffset：
                    // 1. cachedState为空：表示最后一个批次已被移除，endOffset指向下一个要获取的位置
                    // 2. startOffset超过了处理中记录：表示startOffset和endOffset都指向LSO(最后稳定偏移量)
                    return endOffset;
                } else {
                    return endOffset + 1;
                }
            }

            // 执行到这里说明findNextFetchOffset为true，需要重新计算下一个获取偏移量
            if (cachedState.isEmpty() || startOffset > cachedState.lastEntry().getValue().lastOffset()) {
                // 两种情况不需要继续重新计算：
                // 1. cachedState为空：没有需要处理的记录
                // 2. startOffset超过了处理中记录：缓存状态已是最新
                findNextFetchOffset.set(false);
                return endOffset;
            }

            // 初始化下一个获取偏移量为-1（无效值）
            long nextFetchOffset = -1;
            // 如果初始读取间隙窗口激活，获取间隙起始偏移量
            long gapStartOffset = isInitialReadGapOffsetWindowActive() ? initialReadGapOffset.gapStartOffset() : -1;
            
            // 遍历缓存状态查找下一个可用记录
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                // 检查处理中批次是否存在需要获取的间隙
                // 如果initialReadGapOffset的endOffset等于分区的endOffset，则只考虑初始间隙
                // 一旦分区的endOffset超过初始读取结束偏移量，所有间隙都会被获取
                if (isInitialReadGapOffsetWindowActive()) {
                    if (entry.getKey() > gapStartOffset) {
                        nextFetchOffset = gapStartOffset;
                        break;
                    }
                    gapStartOffset = entry.getValue().lastOffset() + 1;
                }

                // 检查状态是按偏移量还是按批次维护
                // 如果没有偏移量状态，则使用批次状态判断
                if (entry.getValue().offsetState() == null) {
                    if (entry.getValue().batchState() == RecordState.AVAILABLE) {
                        nextFetchOffset = entry.getValue().firstOffset();
                        break;
                    }
                } else {
                    // 存在偏移量状态，查找下一个可用的偏移量
                    for (Map.Entry<Long, InFlightState> offsetState : entry.getValue().offsetState().entrySet()) {
                        if (offsetState.getValue().state == RecordState.AVAILABLE) {
                            nextFetchOffset = offsetState.getKey();
                            break;
                        }
                    }
                    // 如果找到了可用偏移量，跳出外层循环
                    if (nextFetchOffset != -1) {
                        break;
                    }
                }
            }

            // 如果nextFetchOffset为-1，说明没有找到可用记录
            // 将标志设为false，避免后续请求重复计算
            if (nextFetchOffset == -1) {
                findNextFetchOffset.set(false);
                nextFetchOffset = endOffset + 1;
            }
            return nextFetchOffset;
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取共享分区的记录，并将获取的记录添加到处理中记录集合
     * 
     * 设计目标：
     * 1. 支持多个消费者共享消费同一分区的数据
     * 2. 确保记录的有序性和一致性
     * 3. 处理记录获取的并发和重复问题
     * 
     * 实现策略：
     * 1. 使用写锁保护整个获取过程
     * 2. 处理记录批次的完整匹配和部分匹配
     * 3. 维护记录的状态和投递计数
     * 
     * 并发控制：
     * 1. 使用写锁确保获取操作的原子性
     * 2. 避免多个消费者同时获取相同记录
     * 
     * 异常处理：
     * 1. 检查分区状态和参数有效性
     * 2. 处理记录获取失败的情况
     * 3. 确保锁的正确释放
     *
     * @param memberId 获取记录的客户端成员ID
     * @param batchSize 每个获取记录批次的大小
     * @param maxFetchRecords 最大获取记录数（软限制）
     * @param fetchPartitionData 从分区获取的记录数据
     * @return 获取到的记录集合
     */
    @SuppressWarnings("cyclomaticcomplexity") // 考虑重构以避免抑制
    public ShareAcquiredRecords acquire(
        String memberId,
        int batchSize,
        int maxFetchRecords,
        FetchPartitionData fetchPartitionData
    ) {
        log.trace("收到共享分区的获取请求: {}-{} 成员ID: {}", groupId, topicIdPartition, memberId);
        
        // 检查分区状态和参数有效性
        if (stateNotActive() || maxFetchRecords <= 0) {
            // 分区不活跃或参数无效，无法获取记录
            return ShareAcquiredRecords.empty();
        }

        // 获取最后一个批次，如果为空则返回空结果
        RecordBatch lastBatch = fetchPartitionData.records.lastBatch().orElse(null);
        if (lastBatch == null) {
            return ShareAcquiredRecords.empty();
        }

        // 获取第一个批次以确定基准偏移量
        RecordBatch firstBatch = fetchPartitionData.records.batches().iterator().next();
        
        // 获取写锁以保护获取过程
        lock.writeLock().lock();
        try {
            // 获取基准偏移量
            long baseOffset = firstBatch.baseOffset();
            
            // 查找请求批次的floor记录
            // 例如：缓存批次偏移量为10-14，请求批次为12-13
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(baseOffset);
            
            // 检查floor记录是否与请求批次有重叠
            // 例如：缓存批次为10-14，请求批次为15-18，虽然找到floor记录但无重叠
            if (floorOffset != null && floorOffset.getValue().lastOffset() >= baseOffset) {
                baseOffset = floorOffset.getKey();
            }
            
            // 获取可能重叠的记录子集
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(baseOffset, true, lastBatch.lastOffset(), true);
            
            // 如果没有重叠，获取完整的新批次
            if (subMap.isEmpty()) {
                log.trace("共享分区没有缓存数据，请求获取批次: {}-{}", groupId, topicIdPartition);
                return acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(),
                    firstBatch.baseOffset(), lastBatch.lastOffset(), batchSize, maxFetchRecords);
            }

            log.trace("存在处理中记录重叠，尝试获取可用记录，共享分区: {}-{}", groupId, topicIdPartition);
            
            // 初始化结果集合和计数器
            List<AcquiredRecords> result = new ArrayList<>();
            int acquiredCount = 0;
            
            // 跟踪子集中的间隙起始偏移量
            long maybeGapStartOffset = baseOffset;
            
            // 处理已在处理中的记录
            // 这些记录可能可以重新投递，批次可能完全匹配、部分匹配或跨越多个已获取的批次
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                // 达到最大获取数量时退出循环
                if (acquiredCount >= maxFetchRecords) {
                    break;
                }

                InFlightBatch inFlightBatch = entry.getValue();
                
                // 处理初始读取间隙
                // 当窗口激活时，将间隙视为可获取
                // 窗口关闭后，剩余间隙为自然间隙（这些偏移量不存在数据）
                if (isInitialReadGapOffsetWindowActive()) {
                    // 如果存在间隙，获取间隙中的记录
                    if (maybeGapStartOffset < entry.getKey()) {
                        ShareAcquiredRecords shareAcquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(),
                            maybeGapStartOffset, entry.getKey() - 1, batchSize, maxFetchRecords);
                        result.addAll(shareAcquiredRecords.acquiredRecords());
                        acquiredCount += shareAcquiredRecords.count();
                    }
                    // 更新下一个批次的起始偏移量
                    maybeGapStartOffset = inFlightBatch.lastOffset() + 1;
                    if (acquiredCount >= maxFetchRecords) {
                        break;
                    }
                }

                // 检查是否完全匹配
                boolean fullMatch = checkForFullMatch(inFlightBatch, firstBatch.baseOffset(), lastBatch.lastOffset());

                // 处理部分匹配或按偏移量跟踪的批次
                if (!fullMatch || inFlightBatch.offsetState() != null) {
                    log.trace("发现子集或按偏移量跟踪的批次记录，批次: {} 请求偏移量 - 起始: {}, 结束: {} 共享分区: {}-{}",
                        inFlightBatch, firstBatch.baseOffset(), lastBatch.lastOffset(), groupId, topicIdPartition);
                    
                    // 初始化偏移量状态（如果需要）
                    if (inFlightBatch.offsetState() == null) {
                        // 检查批次是否可用
                        if (inFlightBatch.batchState() != RecordState.AVAILABLE || inFlightBatch.batchHasOngoingStateTransition()) {
                            log.trace("批次不可获取，跳过处理，共享分区: {}-{}, 批次: {}", groupId, topicIdPartition, inFlightBatch);
                            continue;
                        }
                        // 初始化偏移量状态
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    
                    // 获取子集批次记录
                    int acquiredSubsetCount = acquireSubsetBatchRecords(memberId, firstBatch.baseOffset(),
                        lastBatch.lastOffset(), inFlightBatch, result);
                    acquiredCount += acquiredSubsetCount;
                    continue;
                }

                // 处理完全匹配的批次
                if (inFlightBatch.batchState() != RecordState.AVAILABLE || inFlightBatch.batchHasOngoingStateTransition()) {
                    log.trace("批次不可获取，跳过处理，共享分区: {}-{}, 批次: {}", groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                // 尝试更新批次状态为已获取
                InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.ACQUIRED, true, maxDeliveryCount, memberId);
                if (updateResult == null) {
                    log.info("无法获取批次记录，共享分区: {}-{}, 批次: {}", groupId, topicIdPartition, inFlightBatch);
                    continue;
                }
                
                // 设置获取锁定超时
                AcquisitionLockTimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId,
                    inFlightBatch.firstOffset(), inFlightBatch.lastOffset());
                inFlightBatch.updateAcquisitionLockTimeout(acquisitionLockTimeoutTask);

                // 添加获取的记录到结果集
                result.add(new AcquiredRecords()
                    .setFirstOffset(inFlightBatch.firstOffset())
                    .setLastOffset(inFlightBatch.lastOffset())
                    .setDeliveryCount((short) inFlightBatch.batchDeliveryCount()));
                acquiredCount += (int) (inFlightBatch.lastOffset() - inFlightBatch.firstOffset() + 1);
            }

            // 获取未在已获取批次中的记录
            if (acquiredCount < maxFetchRecords && subMap.lastEntry().getValue().lastOffset() < lastBatch.lastOffset()) {
                log.trace("存在需要获取的额外批次");
                ShareAcquiredRecords shareAcquiredRecords = acquireNewBatchRecords(memberId, fetchPartitionData.records.batches(),
                    subMap.lastEntry().getValue().lastOffset() + 1,
                    lastBatch.lastOffset(), batchSize, maxFetchRecords - acquiredCount);
                result.addAll(shareAcquiredRecords.acquiredRecords());
                acquiredCount += shareAcquiredRecords.count();
            }
            
            // 更新读取间隙获取偏移量
            if (!result.isEmpty()) {
                maybeUpdateReadGapFetchOffset(result.get(result.size() - 1).lastOffset() + 1);
            }
            return new ShareAcquiredRecords(result, acquiredCount);
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 确认共享分区中已获取的记录
     * 
     * 功能：
     * 1. 将已确认的批次从正在处理的记录中移除并持久化
     * 2. 根据需要更新下一个从leader获取的偏移量
     * 3. 支持批量确认和单条记录确认两种模式
     * 
     * 并发控制：
     * 1. 使用写锁确保状态更新的原子性
     * 2. 在批处理过程中维护状态一致性
     * 
     * 状态转换：
     * 1. ACQUIRED -> ACKNOWLEDGED: 记录确认成功
     * 2. ACQUIRED -> ARCHIVED: 记录因重试次数过多被归档
     * 
     * 异常处理：
     * 1. 无效的确认类型：返回InvalidRequestException
     * 2. 记录状态错误：返回InvalidRecordStateException
     * 3. 其他异常：回滚状态更新
     *
     * @param memberId 获取记录的客户端成员ID
     * @param acknowledgementBatches 需要确认的批次列表
     * @return 当记录确认完成时完成的Future
     */
    public CompletableFuture<Void> acknowledge(
        String memberId,
        List<ShareAcknowledgementBatch> acknowledgementBatches
    ) {
        // 记录确认请求的跟踪日志
        log.trace("Acknowledgement batch request for share partition: {}-{}", groupId, topicIdPartition);

        // 初始化Future和状态更新列表
        CompletableFuture<Void> future = new CompletableFuture<>();
        Throwable throwable = null;
        List<InFlightState> updatedStates = new ArrayList<>();
        List<PersisterStateBatch> stateBatches = new ArrayList<>();
        
        // 获取写锁以确保状态更新的原子性
        lock.writeLock().lock();
        try {
            // 遍历需要确认的批次
            // 不使用增强for循环是因为需要检查最后一个批次是否在偏移量范围内
            for (ShareAcknowledgementBatch batch : acknowledgementBatches) {
                // 客户端可以发送单个确认类型表示整个批次的状态
                // 或者发送每个偏移量的单独状态
                Map<Long, RecordState> recordStateMap;
                try {
                    // 获取批次的记录状态映射
                    recordStateMap = fetchRecordStateMapForAcknowledgementBatch(batch);
                } catch (IllegalArgumentException e) {
                    // 记录无效确认类型的调试日志
                    log.debug("Invalid acknowledge type: {} for share partition: {}-{}",
                        batch.acknowledgeTypes(), groupId, topicIdPartition);
                    throwable = new InvalidRequestException("Invalid acknowledge type: " + batch.acknowledgeTypes());
                    break;
                }

                // 如果批次的最后偏移量小于起始偏移量，说明该批次已被归档
                if (batch.lastOffset() < startOffset) {
                    log.trace("All offsets in the acknowledgement batch {} are already archived: {}-{}",
                        batch, groupId, topicIdPartition);
                    continue;
                }

                // 从缓存中获取需要确认的批次子集
                // 子集可能是完全匹配、部分匹配或跨越多个获取的批次
                NavigableMap<Long, InFlightBatch> subMap;
                try {
                    subMap = fetchSubMapForAcknowledgementBatch(batch);
                } catch (InvalidRecordStateException | InvalidRequestException e) {
                    throwable = e;
                    break;
                }

                // 确认批次中的记录
                Optional<Throwable> ackThrowable = acknowledgeBatchRecords(
                    memberId,
                    batch,
                    recordStateMap,
                    subMap,
                    updatedStates,
                    stateBatches
                );

                // 如果确认过程中出现异常，中断处理
                if (ackThrowable.isPresent()) {
                    throwable = ackThrowable.get();
                    break;
                }
            }

            // 如果确认成功则持久化状态、完成状态转换并更新缓存的起始偏移量
            // 如果失败则回滚状态转换
            rollbackOrProcessStateUpdates(future, throwable, updatedStates, stateBatches);
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }

        return future;
    }

    /**
     * 释放指定成员已获取的记录
     * 
     * 功能：
     * 1. 将成员获取的记录状态重置为可用
     * 2. 更新下一个需要从leader获取的偏移量
     * 3. 处理跨批次的记录释放
     * 
     * 并发控制：
     * 1. 使用写锁保护状态更新
     * 2. 确保批处理操作的原子性
     * 
     * 状态转换：
     * 1. ACQUIRED -> AVAILABLE: 记录重新变为可用
     * 2. ACQUIRED -> ARCHIVED: 对于起始偏移量内的记录归档
     * 
     * 处理策略：
     * 1. 支持按偏移量和整批次两种释放模式
     * 2. 考虑起始偏移量对记录状态的影响
     * 3. 保证状态转换的一致性
     *
     * @param memberId 需要释放记录的客户端成员ID
     * @return 当记录释放完成时完成的Future
     */
    public CompletableFuture<Void> releaseAcquiredRecords(String memberId) {
        // 记录释放请求的跟踪日志
        log.trace("Release acquired records request for share partition: {}-{} memberId: {}", groupId, topicIdPartition, memberId);

        // 初始化Future和状态更新列表
        CompletableFuture<Void> future = new CompletableFuture<>();
        Throwable throwable = null;
        List<InFlightState> updatedStates = new ArrayList<>();
        List<PersisterStateBatch> stateBatches = new ArrayList<>();

        // 获取写锁以保护状态更新
        lock.writeLock().lock();
        try {
            // 设置记录状态为可用
            RecordState recordState = RecordState.AVAILABLE;
            
            // 遍历多个获取的批次，每个偏移量的状态可能不同
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                // 检查是否需要初始化偏移量状态更新
                // 当批次的第一个偏移量小于起始偏移量，且最后偏移量大于等于起始偏移量时
                // 需要将已获取的记录移动到归档状态
                if (inFlightBatch.offsetState() == null
                        && inFlightBatch.batchState() == RecordState.ACQUIRED
                        && inFlightBatch.batchMemberId().equals(memberId)
                        && checkForStartOffsetWithinBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset())) {
                    inFlightBatch.maybeInitializeOffsetStateUpdate();
                }

                // 处理按偏移量更新的批次
                if (inFlightBatch.offsetState() != null) {
                    Optional<Throwable> releaseAcquiredRecordsThrowable = releaseAcquiredRecordsForPerOffsetBatch(memberId, inFlightBatch, recordState, updatedStates, stateBatches);
                    if (releaseAcquiredRecordsThrowable.isPresent()) {
                        throwable = releaseAcquiredRecordsThrowable.get();
                        break;
                    }
                    continue;
                }
                
                // 处理整个批次的更新
                Optional<Throwable> releaseAcquiredRecordsThrowable = releaseAcquiredRecordsForCompleteBatch(memberId, inFlightBatch, recordState, updatedStates, stateBatches);
                if (releaseAcquiredRecordsThrowable.isPresent()) {
                    throwable = releaseAcquiredRecordsThrowable.get();
                    break;
                }
            }

            // 如果释放成功则持久化状态、完成状态转换并更新缓存的起始偏移量
            // 如果失败则回滚状态转换
            rollbackOrProcessStateUpdates(future, throwable, updatedStates, stateBatches);
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
        return future;
    }

    /**
     * 释放按偏移量跟踪的批次中已获取的记录
     * 
     * 应用场景：
     * 1. 当消费者需要释放已获取但未处理完的记录时
     * 2. 处理消费者超时或失败的场景
     * 3. 支持记录重试和归档机制
     * 
     * 设计考虑：
     * 1. 使用偏移量级别的状态跟踪，支持更细粒度的控制
     * 2. 实现记录的重试和归档策略
     * 3. 确保消息处理的可靠性和一致性
     * 
     * @param memberId 消费者成员ID
     * @param inFlightBatch 正在处理中的批次
     * @param recordState 目标记录状态
     * @param updatedStates 更新后的状态列表
     * @param stateBatches 需要持久化的状态批次列表
     * @return 如果发生错误返回异常，否则返回空
     */
    private Optional<Throwable> releaseAcquiredRecordsForPerOffsetBatch(String memberId,
                                                                        InFlightBatch inFlightBatch,
                                                                        RecordState recordState,
                                                                        List<InFlightState> updatedStates,
                                                                        List<PersisterStateBatch> stateBatches) {

        // 记录处理日志，包含批次信息和分区标识
        log.trace("Offset tracked batch record found, batch: {} for the share partition: {}-{}", inFlightBatch,
                groupId, topicIdPartition);

        // 遍历批次中的每个偏移量状态
        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {

            // 检查成员ID是否为偏移量的所有者
            // 如果既不是指定成员也不是空成员，则跳过处理
            if (!offsetState.getValue().memberId().equals(memberId) && !offsetState.getValue().memberId().equals(EMPTY_MEMBER_ID)) {
                log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                        + " partition: {}-{}. Skipping offset.", memberId, offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                return Optional.empty();
            }

            // 只处理处于ACQUIRED状态的记录
            if (offsetState.getValue().state == RecordState.ACQUIRED) {
                // 尝试进行状态转换
                // 如果偏移量小于起始偏移量，转换为ARCHIVED状态
                // 否则转换为指定的recordState状态
                InFlightState updateResult = offsetState.getValue().startStateTransition(
                        offsetState.getKey() < startOffset ? RecordState.ARCHIVED : recordState,
                        false,
                        this.maxDeliveryCount,
                        EMPTY_MEMBER_ID
                );

                // 如果状态转换失败，记录错误并返回异常
                if (updateResult == null) {
                    log.debug("Unable to release records from acquired state for the offset: {} in batch: {}"
                                    + " for the share partition: {}-{}", offsetState.getKey(),
                            inFlightBatch, groupId, topicIdPartition);
                    return Optional.of(new InvalidRecordStateException("Unable to release acquired records for the offset"));
                }

                // 状态转换成功，更新状态列表和持久化批次
                updatedStates.add(updateResult);
                stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                        updateResult.state.id, (short) updateResult.deliveryCount));

                // 如果记录未被归档，需要重新计算下一个获取偏移量
                // 已归档的记录不再参与获取，因此不需要更新获取偏移量
                if (updateResult.state != RecordState.ARCHIVED) {
                    findNextFetchOffset.set(true);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * 释放完整批次中已获取的记录
     * 
     * 应用场景：
     * 1. 当消费者需要释放整个批次的记录时
     * 2. 批次级别的状态管理和重试控制
     * 3. 处理批量消息的超时和失败场景
     * 
     * 设计考虑：
     * 1. 使用批次级别的状态跟踪，提高处理效率
     * 2. 统一管理批次内所有记录的状态
     * 3. 支持批次级别的重试和归档策略
     * 
     * @param memberId 消费者成员ID
     * @param inFlightBatch 正在处理中的批次
     * @param recordState 目标记录状态
     * @param updatedStates 更新后的状态列表
     * @param stateBatches 需要持久化的状态批次列表
     * @return 如果发生错误返回异常，否则返回空
     */
    private Optional<Throwable> releaseAcquiredRecordsForCompleteBatch(String memberId,
                                                                       InFlightBatch inFlightBatch,
                                                                       RecordState recordState,
                                                                       List<InFlightState> updatedStates,
                                                                       List<PersisterStateBatch> stateBatches) {

        // 检查成员ID是否为批次的所有者
        // 如果既不是指定成员也不是空成员，则跳过处理
        if (!inFlightBatch.batchMemberId().equals(memberId) && !inFlightBatch.batchMemberId().equals(EMPTY_MEMBER_ID)) {
            log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}. Skipping batch.",
                    memberId, inFlightBatch, groupId, topicIdPartition);
            return Optional.empty();
        }

        // 记录处理日志，准备更新整个批次的状态
        log.trace("Releasing acquired records for complete batch {} for the share partition: {}-{}",
                inFlightBatch, groupId, topicIdPartition);

        // 只处理处于ACQUIRED状态的批次
        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
            // 尝试进行批次状态转换
            // 如果批次的最后偏移量小于起始偏移量，转换为ARCHIVED状态
            // 否则转换为指定的recordState状态
            InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                    inFlightBatch.lastOffset() < startOffset ? RecordState.ARCHIVED : recordState,
                    false,
                    this.maxDeliveryCount,
                    EMPTY_MEMBER_ID
            );

            // 如果状态转换失败，记录错误并返回异常
            if (updateResult == null) {
                log.debug("Unable to release records from acquired state for the batch: {}"
                        + " for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
                return Optional.of(new InvalidRecordStateException("Unable to release acquired records for the batch"));
            }

            // 状态转换成功，更新状态列表和持久化批次
            // 注意：这里使用批次的首尾偏移量来标识整个批次范围
            updatedStates.add(updateResult);
            stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset(),
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // 如果记录未被归档，需要重新计算下一个获取偏移量
            // 已归档的记录不再参与获取，因此不需要更新获取偏移量
            if (updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
        }
        return Optional.empty();
    }

    /**
     * 根据新的日志起始偏移量更新共享分区的缓存状态和起始/结束偏移量
     * 
     * 应用场景：
     * 1. 日志压缩或清理导致起始偏移量变化时
     * 2. 分区初始化时发现日志起始偏移量已移动
     * 3. 处理日志保留策略导致的偏移量变化
     * 
     * 设计考虑：
     * 1. 使用写锁确保并发安全
     * 2. 处理缓存状态和偏移量的一致性
     * 3. 支持增量更新和延迟写入
     * 
     * @param logStartOffset 新的日志起始偏移量
     */
    void updateCacheAndOffsets(long logStartOffset) {
        // 获取写锁，确保并发安全
        lock.writeLock().lock();
        try {
            // 验证新的起始偏移量是否有效
            // 新偏移量必须大于当前起始偏移量
            if (logStartOffset <= startOffset) {
                log.error("The log start offset: {} is not greater than the start offset: {} for the share partition: {}-{}",
                        logStartOffset, startOffset, groupId, topicIdPartition);
                return;
            }

            // 记录偏移量更新信息
            log.debug("Updating start offset for share partition: {}-{} from: {} to: {} since LSO has moved to: {}",
                    groupId, topicIdPartition, startOffset, logStartOffset, logStartOffset);

            // 处理缓存为空的特殊情况
            if (cachedState.isEmpty()) {
                // 如果缓存状态为空，直接将起始和结束偏移量设置为新的日志起始偏移量
                // 这种情况可能发生在分区初始化时日志起始偏移量已经移动
                startOffset = logStartOffset;
                endOffset = logStartOffset;
                return;
            }

            // 归档新起始偏移量之前的可用记录
            boolean anyRecordArchived = archiveAvailableRecordsOnLsoMovement(logStartOffset);
            // 如果有记录从AVAILABLE状态转换为ARCHIVED状态
            // 则可能需要更新下一个获取偏移量
            if (anyRecordArchived) {
                findNextFetchOffset.set(true);
            }

            // 更新起始偏移量为新的日志起始偏移量
            startOffset = logStartOffset;
            // 处理结束偏移量小于起始偏移量的情况
            if (endOffset < startOffset) {
                // 这种情况表示缓存状态需要完全刷新
                // 示例：缓存中有偏移量0-10的已获取记录，然后日志起始偏移量移动到15
                // 此时结束偏移量也应该更新为15
                endOffset = startOffset;
            }

            // 注意事项：
            // 1. 新的起始偏移量将在确认/释放已获取记录的API调用时延迟写入
            // 2. 不会将归档状态批次写入持久化器
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 当日志起始偏移量(LSO)发生变化时，归档可用的记录
     * 
     * 应用场景：
     * 1. 日志压缩或清理导致LSO前移
     * 2. 需要归档LSO之前的记录以释放资源
     * 
     * 并发控制：
     * - 使用写锁确保在归档过程中状态的一致性
     * - 避免与其他读写操作的并发冲突
     * 
     * @param logStartOffset 新的日志起始偏移量
     * @return 如果有任何记录被归档则返回true
     */
    private boolean archiveAvailableRecordsOnLsoMovement(long logStartOffset) {
        // 获取写锁以确保线程安全
        lock.writeLock().lock();
        try {
            // 跟踪是否有记录被归档的标志
            boolean isAnyOffsetArchived = false, isAnyBatchArchived = false;
            
            // 遍历所有正在处理中的批次
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                long batchStartOffset = entry.getKey();
                // 如果批次的起始偏移量大于等于新的LSO，则不需要处理
                if (batchStartOffset >= logStartOffset) {
                    break;
                }
                
                InFlightBatch inFlightBatch = entry.getValue();
                // 检查批次是否完全匹配需要归档的范围
                boolean fullMatch = checkForFullMatch(inFlightBatch, startOffset, logStartOffset - 1);

                // 如果批次不完全匹配或已经在跟踪偏移量状态，则需要按偏移量维护状态
                if (!fullMatch || inFlightBatch.offsetState() != null) {
                    log.debug("Subset or offset tracked batch record found while trying to update offsets and cached" +
                                    " state map due to LSO movement, batch: {}, offsets to update - " +
                                    "first: {}, last: {} for the share partition: {}-{}", inFlightBatch, startOffset,
                            logStartOffset - 1, groupId, topicIdPartition);

                    // 如果还没有初始化偏移量状态，且批次状态为可用，则初始化
                    if (inFlightBatch.offsetState() == null) {
                        if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                            continue;
                        }
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    // 按偏移量归档记录
                    isAnyOffsetArchived = isAnyOffsetArchived || archivePerOffsetBatchRecords(inFlightBatch, startOffset, logStartOffset - 1);
                    continue;
                }
                // 如果是完全匹配，则归档整个批次
                isAnyBatchArchived = isAnyBatchArchived || archiveCompleteBatch(inFlightBatch);
            }
            return isAnyOffsetArchived || isAnyBatchArchived;
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 按偏移量归档批次中的记录
     * 
     * 应用场景：
     * 1. 部分记录需要归档时
     * 2. 批次中的记录状态不一致时
     * 
     * 并发控制：
     * - 使用写锁保护偏移量状态的更新
     * - 确保归档操作的原子性
     * 
     * @param inFlightBatch 需要处理的批次
     * @param startOffsetToArchive 开始归档的偏移量
     * @param endOffsetToArchive 结束归档的偏移量
     * @return 如果有任何记录被归档则返回true
     */
    private boolean archivePerOffsetBatchRecords(InFlightBatch inFlightBatch,
                                                 long startOffsetToArchive,
                                                 long endOffsetToArchive) {
        // 获取写锁以保护状态更新
        lock.writeLock().lock();
        try {
            boolean isAnyOffsetArchived = false;
            log.trace("Archiving offset tracked batch: {} for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
            
            // 遍历批次中的所有偏移量状态
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {
                // 跳过范围之前的偏移量
                if (offsetState.getKey() < startOffsetToArchive) {
                    continue;
                }
                // 超出范围则结束处理
                if (offsetState.getKey() > endOffsetToArchive) {
                    break;
                }
                // 只归档状态为AVAILABLE的记录
                if (offsetState.getValue().state != RecordState.AVAILABLE) {
                    continue;
                }

                // 将记录状态设置为已归档
                offsetState.getValue().archive(EMPTY_MEMBER_ID);
                isAnyOffsetArchived = true;
            }
            return isAnyOffsetArchived;
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 归档完整的批次
     * 
     * 应用场景：
     * 1. 整个批次都需要归档时
     * 2. 批次中所有记录状态一致时
     * 
     * 并发控制：
     * - 使用写锁保护批次状态的更新
     * - 确保批次归档的原子性
     * 
     * @param inFlightBatch 需要归档的批次
     * @return 如果批次被归档则返回true
     */
    private boolean archiveCompleteBatch(InFlightBatch inFlightBatch) {
        // 获取写锁以保护状态更新
        lock.writeLock().lock();
        try {
            log.trace("Archiving complete batch: {} for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
            // 只归档状态为AVAILABLE的批次
            if (inFlightBatch.batchState() == RecordState.AVAILABLE) {
                // 将整个批次的状态设置为已归档
                inFlightBatch.archiveBatch(EMPTY_MEMBER_ID);
                return true;
            }
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
        return false;
    }

    /**
     * 检查是否可以获取更多记录
     * 
     * 应用场景：
     * 1. 消费者请求获取新记录时的预检查
     * 2. 控制处理中记录数量，实现背压机制
     * 
     * 判断条件：
     * 1. 如果下一个获取偏移量不连续，说明是重新获取之前的记录，允许获取
     * 2. 如果处理中的记录数量小于最大限制，允许获取新记录
     * 
     * 并发控制：
     * - 使用读锁保护状态读取
     * - 允许多个线程同时检查状态
     * 
     * @return 如果可以获取更多记录则返回true
     */
    boolean canAcquireRecords() {
        // 检查是否是重新获取之前的记录
        if (nextFetchOffset() != endOffset() + 1) {
            return true;
        }

        // 获取读锁以安全访问状态
        lock.readLock().lock();
        long numRecords;
        try {
            // 计算当前处理中的记录数量
            if (cachedState.isEmpty()) {
                numRecords = 0;
            } else {
                numRecords = this.endOffset - this.startOffset + 1;
            }
        } finally {
            // 释放读锁
            lock.readLock().unlock();
        }
        // 检查是否超过最大处理中记录数限制
        return numRecords < maxInFlightMessages;
    }

    /**
     * 尝试获取获取锁，以确保同一个共享分区不会被多个客户端并发获取。
     * 只有在获取记录并完成处理后，获取锁才会被释放。
     * 
     * 应用场景：
     * 1. 在从leader获取记录之前调用，防止并发获取
     * 2. 确保分区数据的顺序性和一致性
     * 3. 避免重复获取导致的资源浪费
     *
     * @return 如果成功获取锁返回true，否则返回false
     */
    public boolean maybeAcquireFetchLock() {
        // 如果分区状态不是ACTIVE，则不允许获取锁
        if (stateNotActive()) {
            return false;
        }
        // 使用CAS操作尝试将fetchLock从false设置为true
        // 只有之前值为false时才能获取成功
        return fetchLock.compareAndSet(false, true);
    }

    /**
     * 释放获取锁，允许其他客户端获取记录
     * 
     * 应用场景：
     * 1. 在完成记录获取后调用
     * 2. 在获取操作发生异常时调用
     * 3. 确保锁的正确释放，避免死锁
     */
    void releaseFetchLock() {
        // 直接设置fetchLock为false，释放锁
        fetchLock.set(false);
    }

    /**
     * 将共享分区标记为已封禁状态
     * 
     * 应用场景：
     * 1. 分区需要维护时
     * 2. 检测到异常状态时
     * 3. 需要暂停服务时
     * 
     * 实现细节：
     * 1. 使用写锁确保线程安全
     * 2. 状态变更为FENCED
     * 3. 确保锁的正确释放
     */
    void markFenced() {
        // 获取写锁，确保状态变更的原子性
        lock.writeLock().lock();
        try {
            // 将分区状态设置为FENCED
            partitionState = SharePartitionState.FENCED;
        } finally {
            // 在finally块中释放锁，确保锁一定会被释放
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取共享分区的状态变更监听器
     * 
     * 用途：
     * 1. 监听分区状态变更
     * 2. 触发相应的处理逻辑
     *
     * @return 返回分区状态监听器实例
     */
    SharePartitionListener listener() {
        return this.listener;
    }

    /**
     * 获取领导者纪元号
     * 
     * 用途：
     * 1. 跟踪分区版本
     * 2. 确保数据一致性
     * 
     * @return 返回当前的领导者纪元号
     */
    int leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * 检查分区状态是否不是ACTIVE
     * 
     * 用途：
     * 1. 快速判断分区是否可用
     * 2. 作为获取锁的前置检查
     * 
     * @return 如果状态不是ACTIVE返回true，否则返回false
     */
    private boolean stateNotActive() {
        return partitionState() != SharePartitionState.ACTIVE;
    }

    /**
     * 尝试将分区状态从EMPTY转换为INITIALIZING
     * 
     * 状态转换流程：
     * 1. 获取写锁确保线程安全
     * 2. 检查是否已初始化
     * 3. 如果未初始化，将状态设置为INITIALIZING
     * 
     * @return 如果状态转换成功返回true，否则返回false
     */
    private boolean emptyToInitialState() {
        // 获取写锁，确保状态转换的原子性
        lock.writeLock().lock();
        try {
            // 检查是否已初始化，如果已初始化则返回false
            if (initializedOrThrowException()) return false;
            // 将状态设置为INITIALIZING
            partitionState = SharePartitionState.INITIALIZING;
            return true;
        } finally {
            // 确保锁的释放
            lock.writeLock().unlock();
        }
    }

    /**
     * 检查分区是否已初始化，如果状态异常则抛出相应异常
     * 
     * 状态处理规则：
     * 1. ACTIVE: 已初始化，返回true
     * 2. FAILED: 抛出IllegalStateException
     * 3. INITIALIZING: 抛出LeaderNotAvailableException
     * 4. FENCED: 抛出FencedStateEpochException
     * 5. EMPTY: 未初始化，返回false
     * 
     * @return 如果分区已初始化返回true，未初始化返回false
     * @throws IllegalStateException 如果分区加载失败
     * @throws LeaderNotAvailableException 如果分区正在初始化
     * @throws FencedStateEpochException 如果分区已被封禁
     */
    private boolean initializedOrThrowException() {
        // 获取当前分区状态
        SharePartitionState currentState = partitionState();
        // 根据不同状态返回结果或抛出异常
        return switch (currentState) {
            case ACTIVE -> true;
            case FAILED -> throw new IllegalStateException(
                String.format("Share partition failed to load %s-%s", groupId, topicIdPartition));
            case INITIALIZING -> throw new LeaderNotAvailableException(
                String.format("Share partition is already initializing %s-%s", groupId, topicIdPartition));
            case FENCED -> throw new FencedStateEpochException(
                String.format("Share partition is fenced %s-%s", groupId, topicIdPartition));
            case EMPTY ->
                // 分区尚未初始化
                false;
        };
    }

    /**
     * 更新读取间隙的获取偏移量，用于减少跟踪cachedState中的间隙的窗口大小
     * 
     * 应用场景：
     * 1. 在获取新记录后调用
     * 2. 管理读取间隙的范围
     * 3. 优化内存使用
     * 
     * @param offset 新的偏移量
     */
    private void maybeUpdateReadGapFetchOffset(long offset) {
        // 获取写锁，确保线程安全
        lock.writeLock().lock();
        try {
            // 如果存在初始读取间隙偏移量
            if (initialReadGapOffset != null) {
                // 如果结束偏移量等于当前结束偏移量，更新间隙起始偏移量
                if (initialReadGapOffset.endOffset() == endOffset) {
                    initialReadGapOffset.gapStartOffset(offset);
                } else {
                    // 如果结束偏移量已经移动，说明初始读取间隙不再有效
                    // 重置初始读取间隙偏移量
                    initialReadGapOffset = null;
                }
            }
        } finally {
            // 确保锁的释放
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取新的批次记录，并将其添加到处理中的记录缓存中
     * 
     * 功能：
     * 1. 根据偏移量范围和限制获取新的记录批次
     * 2. 更新分区的起始和结束偏移量
     * 3. 处理记录间隙和缓存状态
     * 
     * @param memberId 消费者成员ID
     * @param batches 要获取的记录批次
     * @param firstOffset 第一个记录的偏移量
     * @param lastOffset 最后一个记录的偏移量
     * @param batchSize 每个批次的大小限制
     * @param maxFetchRecords 最大获取记录数
     * @return 返回获取的记录信息
     */
    private ShareAcquiredRecords acquireNewBatchRecords(
        String memberId,
        Iterable<? extends RecordBatch> batches,
        long firstOffset,
        long lastOffset,
        int batchSize,
        int maxFetchRecords
    ) {
        // 获取写锁，确保线程安全
        lock.writeLock().lock();
        try {
            // 如果获取了相同的批次且之前的批次已从缓存中移除
            // 则需要更新批次的第一个偏移量为endOffset
            // 但仅在endOffset大于firstOffset时更新
            // 对于从主题分区初始启动的共享获取，endOffset将初始化为0
            // 但firstOffset可能大于0
            long firstAcquiredOffset = firstOffset;
            if (cachedState.isEmpty() && endOffset > firstAcquiredOffset) {
                firstAcquiredOffset = endOffset;
            }

            // 检查可以从批次中获取多少消息
            long lastAcquiredOffset = lastOffset;
            // 如果最大获取记录数小于可用批次的记录数
            if (maxFetchRecords < lastAcquiredOffset - firstAcquiredOffset + 1) {
                // 限制获取的记录数
                // 最后的偏移量应该是在最大消息限制内的批次的最后偏移量
                // 由于maxFetchRecords是软限制，最后的偏移量可能会超过最大消息数
                lastAcquiredOffset = lastOffsetFromBatchWithRequestOffset(batches, firstAcquiredOffset + maxFetchRecords - 1);
            }

            // 创建已获取记录的批次
            List<AcquiredRecords> acquiredRecords = createBatches(memberId, batches, firstAcquiredOffset, lastAcquiredOffset, batchSize);
            // 如果在获取新批次之前cachedState为空，则需要更新startOffset
            if (cachedState.firstKey() == firstAcquiredOffset)  {
                startOffset = firstAcquiredOffset;
            }

            // 如果新获取的批次是cachedState中的间隙的一部分，则不应更新endOffset
            // 例如：如果startOffset是10，endOffset是30，有一个从10到20的间隙
            // 和一个从21到30的处理中批次。在这种情况下，nextFetchOffset返回10
            // 并获取记录。新批次从10到20被获取，但endOffset保持在30
            if (lastAcquiredOffset > endOffset) {
                endOffset = lastAcquiredOffset;
            }
            // 更新读取间隙的获取偏移量
            maybeUpdateReadGapFetchOffset(lastAcquiredOffset + 1);
            // 返回获取的记录信息
            return new ShareAcquiredRecords(acquiredRecords, (int) (lastAcquiredOffset - firstAcquiredOffset + 1));
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 创建记录批次，根据需要将大批次分割成多个小批次
     * 
     * 功能：
     * 1. 根据批次大小限制分割记录
     * 2. 为每个批次创建获取锁超时任务
     * 3. 将批次添加到处理中的记录缓存
     * 
     * @param memberId 消费者成员ID
     * @param batches 原始记录批次
     * @param firstAcquiredOffset 第一个获取的偏移量
     * @param lastAcquiredOffset 最后一个获取的偏移量
     * @param batchSize 批次大小限制
     * @return 返回创建的批次列表
     */
    private List<AcquiredRecords> createBatches(
        String memberId,
        Iterable<? extends RecordBatch> batches,
        long firstAcquiredOffset,
        long lastAcquiredOffset,
        int batchSize
    ) {
        // 获取写锁，确保线程安全
        lock.writeLock().lock();
        try {
            List<AcquiredRecords> result = new ArrayList<>();
            long currentFirstOffset = firstAcquiredOffset;
            
            // 如果批次大小大于可以获取的记录数，则不需要分割批次
            // 否则将批次分割成多个批次
            if (lastAcquiredOffset - firstAcquiredOffset + 1 > batchSize) {
                // 根据批次大小分割批次
                // 注意：尽量只读取批次的baseOffset，避免读取lastOffset
                // 因为RecordBatch的lastOffset调用开销很大（需要加载头信息）
                for (RecordBatch batch : batches) {
                    long batchBaseOffset = batch.baseOffset();
                    // 检查批次是否已经超过最后获取的偏移量，如果是则跳出循环
                    if (batchBaseOffset > lastAcquiredOffset) {
                        // 跳出循环，最后一个批次将在循环外处理
                        break;
                    }

                    // 当达到批次大小时创建新批次
                    if (batchBaseOffset - currentFirstOffset >= batchSize) {
                        result.add(new AcquiredRecords()
                            .setFirstOffset(currentFirstOffset)
                            .setLastOffset(batchBaseOffset - 1)
                            .setDeliveryCount((short) 1));
                        currentFirstOffset = batchBaseOffset;
                    }
                }
            }
            // 添加最后一个批次，或者如果批次大小大于可以获取的记录数
            // 则添加唯一的批次
            result.add(new AcquiredRecords()
                .setFirstOffset(currentFirstOffset)
                .setLastOffset(lastAcquiredOffset)
                .setDeliveryCount((short) 1));

            // 为每个批次设置获取锁超时和添加到缓存
            result.forEach(acquiredRecords -> {
                // 为批次调度获取锁超时任务
                AcquisitionLockTimerTask timerTask = scheduleAcquisitionLockTimeout(memberId, acquiredRecords.firstOffset(), acquiredRecords.lastOffset());
                // 将新批次添加到处理中的记录缓存，包括获取锁超时任务
                cachedState.put(acquiredRecords.firstOffset(), new InFlightBatch(
                    memberId,
                    acquiredRecords.firstOffset(),
                    acquiredRecords.lastOffset(),
                    RecordState.ACQUIRED,
                    1,
                    timerTask));
            });
            return result;
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取子集批次记录，将可用的记录分配给指定的消费者成员
     * 
     * 功能：
     * 1. 从批次中获取指定偏移量范围内的记录
     * 2. 将记录状态从AVAILABLE转换为ACQUIRED
     * 3. 设置记录的获取锁定超时任务
     * 
     * 并发控制：
     * - 使用写锁确保状态转换的原子性
     * - 避免多个消费者同时获取同一条记录
     * 
     * @param memberId 消费者成员ID
     * @param requestFirstOffset 请求的起始偏移量
     * @param requestLastOffset 请求的结束偏移量
     * @param inFlightBatch 正在处理中的批次
     * @param result 获取到的记录列表
     * @return 成功获取的记录数量
     */
    private int acquireSubsetBatchRecords(
        String memberId,
        long requestFirstOffset,
        long requestLastOffset,
        InFlightBatch inFlightBatch,
        List<AcquiredRecords> result
    ) {
        // 获取写锁，确保状态转换的原子性
        lock.writeLock().lock();
        int acquiredCount = 0;
        try {
            // 遍历批次中的所有记录状态
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                // 跳过请求范围之前的记录
                // 例如：缓存批次为10-14，请求批次为12-13
                if (offsetState.getKey() < requestFirstOffset) {
                    continue;
                }

                // 超出请求范围，停止处理
                if (offsetState.getKey() > requestLastOffset) {
                    break;
                }

                // 检查记录是否可用且没有正在进行的状态转换
                if (offsetState.getValue().state != RecordState.AVAILABLE || offsetState.getValue().hasOngoingStateTransition()) {
                    log.trace("The offset {} is not available in share partition: {}-{}, skipping: {}",
                        offsetState.getKey(), groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                // 尝试将记录状态更新为ACQUIRED
                InFlightState updateResult =  offsetState.getValue().tryUpdateState(RecordState.ACQUIRED, true, maxDeliveryCount,
                    memberId);
                if (updateResult == null) {
                    log.trace("Unable to acquire records for the offset: {} in batch: {}"
                            + " for the share partition: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }
                // 为该偏移量创建并调度获取锁定超时任务
                AcquisitionLockTimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, offsetState.getKey(), offsetState.getKey());
                // 更新记录的获取锁定超时任务
                offsetState.getValue().updateAcquisitionLockTimeoutTask(acquisitionLockTimeoutTask);

                // TODO: 可以考虑将连续的偏移量合并处理
                // 将获取到的记录添加到结果列表
                result.add(new AcquiredRecords()
                    .setFirstOffset(offsetState.getKey())
                    .setLastOffset(offsetState.getKey())
                    .setDeliveryCount((short) offsetState.getValue().deliveryCount));
                acquiredCount++;
            }
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
        return acquiredCount;
    }

    /**
     * 检查正在处理中的批次是否与请求的偏移量范围完全匹配
     * 
     * 完全匹配的定义：
     * - 批次的起始偏移量大于等于请求的起始偏移量
     * - 批次的结束偏移量小于等于请求的结束偏移量
     * 
     * 应用场景：
     * 1. 优化批次处理逻辑，避免不必要的部分批次处理
     * 2. 确保批次边界的正确性
     *
     * @param inFlightBatch 要检查的正在处理中的批次
     * @param firstOffsetToCompare 请求批次的起始偏移量
     * @param lastOffsetToCompare 请求批次的结束偏移量
     * @return 如果批次完全匹配则返回true，否则返回false
     */
    private boolean checkForFullMatch(InFlightBatch inFlightBatch, long firstOffsetToCompare, long lastOffsetToCompare) {
        return inFlightBatch.firstOffset() >= firstOffsetToCompare && inFlightBatch.lastOffset() <= lastOffsetToCompare;
    }

    /**
     * 检查起始偏移量是否已移动且在请求的批次范围内
     * 
     * 应用场景：
     * 1. 处理消费进度变化导致的起始偏移量移动
     * 2. 确保不会错过已移动的起始偏移量之后的记录
     * 
     * 判断逻辑：
     * 1. 批次的起始偏移量小于当前起始偏移量
     * 2. 批次的结束偏移量大于等于当前起始偏移量
     *
     * @param batchFirstOffset 批次的起始偏移量
     * @param batchLastOffset 批次的结束偏移量
     * @return 如果起始偏移量在批次范围内则返回true，否则返回false
     */
    private boolean checkForStartOffsetWithinBatch(long batchFirstOffset, long batchLastOffset) {
        // 获取当前的起始偏移量
        long localStartOffset = startOffset();
        // 检查起始偏移量是否在批次范围内
        return batchFirstOffset < localStartOffset && batchLastOffset >= localStartOffset;
    }

    /**
     * 为确认批次构建记录状态映射
     * 
     * 功能：
     * 1. 将客户端的确认类型转换为对应的记录状态
     * 2. 支持批量确认和单条记录确认两种模式
     * 
     * 处理逻辑：
     * - 如果acknowledgeTypes只有一个元素，表示对整个批次使用相同的状态
     * - 如果acknowledgeTypes有多个元素，表示每个偏移量都有独立的状态
     *
     * @param batch 要处理的确认批次
     * @return 偏移量到记录状态的映射
     */
    private Map<Long, RecordState> fetchRecordStateMapForAcknowledgementBatch(
        ShareAcknowledgementBatch batch) {
        // 创建记录状态映射
        Map<Long, RecordState> recordStateMap = new HashMap<>();
        // 遍历所有确认类型，为每个偏移量构建状态映射
        for (int index = 0; index < batch.acknowledgeTypes().size(); index++) {
            recordStateMap.put(batch.firstOffset() + index,
                fetchRecordState(batch.acknowledgeTypes().get(index)));
        }
        return recordStateMap;
    }

    /**
     * 根据确认类型获取对应的记录状态
     * 
     * 状态映射规则：
     * 1. ACCEPT(1) -> ACKNOWLEDGED: 记录已被成功处理
     * 2. RELEASE(2) -> AVAILABLE: 记录需要重新投递
     * 3. REJECT(3)/GAP(0) -> ARCHIVED: 记录不再投递
     * 
     * 应用场景：
     * 1. 处理消费者的确认请求
     * 2. 更新记录的处理状态
     * 3. 支持不同的确认语义
     *
     * @param acknowledgeType 确认类型的字节值
     * @return 对应的记录状态
     * @throws IllegalArgumentException 如果确认类型无效
     */
    private static RecordState fetchRecordState(byte acknowledgeType) {
        switch (acknowledgeType) {
            case 1 /* ACCEPT */:
                return RecordState.ACKNOWLEDGED;
            case 2 /* RELEASE */:
                return RecordState.AVAILABLE;
            case 3 /* REJECT */:
            case 0 /* GAP */:
                return RecordState.ARCHIVED;
            default:
                throw new IllegalArgumentException("Invalid acknowledge type: " + acknowledgeType);
        }
    }

    /**
     * 获取确认批次对应的子映射
     * 
     * 应用场景：
     * 1. 处理消费者确认消息时，需要找到对应的批次记录
     * 2. 支持部分确认场景，如一个批次中只确认部分消息
     * 3. 处理起始偏移量变更导致的批次分割场景
     * 
     * 设计考虑：
     * 1. 使用写锁保证并发安全
     * 2. 处理批次边界情况，如批次重叠、部分确认等
     * 3. 验证批次的有效性，避免无效确认
     *
     * @param batch 需要确认的批次信息
     * @return 返回包含相关记录的子映射
     * @throws InvalidRecordStateException 当批次记录未找到时抛出
     * @throws InvalidRequestException 当批次偏移量无效时抛出
     */
    private NavigableMap<Long, InFlightBatch> fetchSubMapForAcknowledgementBatch(
        ShareAcknowledgementBatch batch
    ) {
        // 获取写锁以确保线程安全
        lock.writeLock().lock();
        try {
            // 查找请求批次的floor批次记录
            // 因为请求批次可能是已缓存批次的子集
            // 例如：缓存批次偏移量为10-14，而请求批次为12-13
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(batch.firstOffset());
            if (floorOffset == null) {
                // 检查起始偏移量是否在批次范围内移动
                boolean hasStartOffsetMoved = checkForStartOffsetWithinBatch(batch.firstOffset(), batch.lastOffset());
                if (hasStartOffsetMoved) {
                    // 如果起始偏移量已移动且在请求批次范围内
                    // 则从新的起始偏移量获取floor记录并确认缓存的偏移量
                    // 例如：起始偏移量从0移动到10，缓存批次为0-5,5-10,10-12,12-15
                    // 确认请求批次为5-15，则获取锁超时后缓存只包含10-15的数据
                    floorOffset = cachedState.floorEntry(startOffset);
                } else {
                    // 记录未找到，记录日志并抛出异常
                    log.debug("Batch record {} not found for share partition: {}-{}", batch, groupId,
                        topicIdPartition);
                    throw new InvalidRecordStateException(
                        "Batch record not found. The request batch offsets are not found in the cache.");
                }
            }

            // 获取包含请求批次范围的子映射
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, batch.lastOffset(), true);
            
            // 验证请求批次的首个偏移量是否大于最后一个已缓存批次的最后偏移量
            // 如果是，则表示请求中没有可以确认的偏移量
            if (subMap.lastEntry().getValue().lastOffset < batch.firstOffset()) {
                log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                throw new InvalidRequestException("Batch record not found. The first offset in request is past acquired records.");
            }

            // 验证请求批次的最后偏移量是否大于最后一个已缓存批次的最后偏移量
            // 如果是，则表示请求中包含未获取的偏移量
            if (batch.lastOffset() > subMap.lastEntry().getValue().lastOffset) {
                log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                throw new InvalidRequestException("Batch record not found. The last offset in request is past acquired records.");
            }

            return subMap;
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 确认批次记录的状态
     * 
     * 应用场景：
     * 1. 处理消费者对消息的确认请求
     * 2. 支持完整批次和部分批次的确认
     * 3. 处理跨多个获取批次的确认请求
     * 4. 管理每个偏移量的独立状态
     * 
     * 设计考虑：
     * 1. 使用写锁保证并发安全
     * 2. 支持不同的确认模式（完整批次/部分批次）
     * 3. 处理起始偏移量变更场景
     * 4. 维护批次状态的一致性
     *
     * @param memberId 消费者成员ID
     * @param batch 需要确认的批次
     * @param recordStateMap 记录状态映射
     * @param subMap 包含相关记录的子映射
     * @param updatedStates 更新后的状态列表
     * @param stateBatches 需要持久化的状态批次列表
     * @return 如果发生错误则返回异常，否则返回空
     */
    private Optional<Throwable> acknowledgeBatchRecords(
        String memberId,
        ShareAcknowledgementBatch batch,
        Map<Long, RecordState> recordStateMap,
        NavigableMap<Long, InFlightBatch> subMap,
        final List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        Optional<Throwable> throwable;
        // 获取写锁以确保线程安全
        lock.writeLock().lock();
        try {
            // 确认批次可能是：
            // 1. 完全匹配获取的批次（最常见）
            // 2. 获取批次的子集
            // 3. 跨越多个获取的批次
            // 当批次是子集或客户端发送单独的偏移量状态时，每个偏移量的状态可能不同
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                // 如果起始偏移量已经超过了处理中批次的范围，跳过该批次
                if (inFlightBatch.lastOffset() < startOffset) {
                    log.trace("All offsets in the inflight batch {} are already archived: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);
                    continue;
                }

                // 验证请求的成员ID是否是批次的所有者
                if (inFlightBatch.offsetState() == null) {
                    throwable = validateAcknowledgementBatchMemberId(memberId, inFlightBatch);
                    if (throwable.isPresent()) {
                        return throwable;
                    }
                }

                // 确定处理中的批次是否与请求批次完全匹配
                boolean fullMatch = checkForFullMatch(inFlightBatch, batch.firstOffset(), batch.lastOffset());
                // 检查客户端是否发送了多个确认类型（表示需要单独处理每个偏移量）
                boolean isPerOffsetClientAck = batch.acknowledgeTypes().size() > 1;
                // 检查起始偏移量是否在批次范围内移动
                boolean hasStartOffsetMoved = checkForStartOffsetWithinBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset());

                // 在以下情况下需要维护每个偏移量的状态：
                // 1. 不是完全匹配
                // 2. 已经在管理偏移量状态
                // 3. 客户端发送了单独的偏移量状态
                // 4. 起始偏移量在这个处理中的批次范围内
                if (!fullMatch || inFlightBatch.offsetState() != null || isPerOffsetClientAck || hasStartOffsetMoved) {
                    log.debug("Subset or offset tracked batch record found for acknowledgement,"
                            + " batch: {}, request offsets - first: {}, last: {}, client per offset"
                            + "state {} for the share partition: {}-{}", inFlightBatch, batch.firstOffset(),
                        batch.lastOffset(), isPerOffsetClientAck, groupId, topicIdPartition);
                    if (inFlightBatch.offsetState() == null) {
                        // 虽然请求是处理中批次的子集，但偏移量状态跟踪尚未初始化
                        // 这意味着我们只能在整个批次已被获取的情况下确认部分偏移量
                        // 因此，先进行预检查以避免不必要的偏移量状态跟踪初始化
                        if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                            log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                                inFlightBatch, groupId, topicIdPartition);
                            return Optional.of(new InvalidRecordStateException("The batch cannot be acknowledged. The subset batch is not in the acquired state."));
                        }
                        // 请求批次是子集且需要每个偏移量的状态
                        // 初始化处理中批次的偏移量状态
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }

                    // 确认每个偏移量的批次记录
                    throwable = acknowledgePerOffsetBatchRecords(memberId, batch, inFlightBatch,
                        recordStateMap, updatedStates, stateBatches);
                } else {
                    // 处理中的批次是完全匹配，因此改变整个批次的状态
                    throwable = acknowledgeCompleteBatch(batch, inFlightBatch,
                        recordStateMap.get(batch.firstOffset()), updatedStates, stateBatches);
                }

                if (throwable.isPresent()) {
                    return throwable;
                }
            }
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
        return Optional.empty();
    }

    /**
     * 验证确认批次的成员ID是否有效
     * 
     * 应用场景：
     * 1. 确保只有批次的所有者才能确认消息
     * 2. 防止未获取的批次被确认
     * 3. 避免重复确认和越权确认
     * 
     * 设计考虑：
     * 1. 使用EMPTY_MEMBER_ID标识批次未被获取
     * 2. 严格校验成员ID匹配
     * 3. 提供详细的错误信息便于问题诊断
     *
     * @param memberId 请求确认的成员ID
     * @param inFlightBatch 正在处理的批次
     * @return 如果验证失败返回异常，验证通过返回空
     */
    private Optional<Throwable> validateAcknowledgementBatchMemberId(
        String memberId,
        InFlightBatch inFlightBatch
    ) {
        // EMPTY_MEMBER_ID表示批次尚未被获取
        // 如果批次的成员ID是EMPTY_MEMBER_ID，说明批次不在已获取状态
        if (inFlightBatch.batchMemberId().equals(EMPTY_MEMBER_ID)) {
            log.debug("The batch is not in the acquired state: {} for share partition: {}-{}. Empty member id for batch.",
                inFlightBatch, groupId, topicIdPartition);
            return Optional.of(new InvalidRecordStateException("The batch cannot be acknowledged. The batch is not in the acquired state."));
        }

        // 验证请求确认的成员ID是否与批次的所有者ID匹配
        // 防止非所有者确认批次，确保消息处理的安全性
        if (!inFlightBatch.batchMemberId().equals(memberId)) {
            log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}",
                memberId, inFlightBatch, groupId, topicIdPartition);
            return Optional.of(new InvalidRecordStateException("Member is not the owner of batch record"));
        }
        return Optional.empty();
    }

    /**
     * 按偏移量确认批次记录的状态
     * 
     * 应用场景：
     * 1. 消费者需要对获取的消息进行逐条确认
     * 2. 支持对同一批次中的不同消息采用不同的确认策略
     * 
     * 设计考虑：
     * 1. 使用写锁保证并发安全
     * 2. 支持部分确认机制
     * 3. 维护消息状态和所有权
     * 
     * @param memberId 消费者成员ID
     * @param batch 待确认的批次
     * @param inFlightBatch 正在处理中的批次
     * @param recordStateMap 记录状态映射
     * @param updatedStates 更新后的状态列表
     * @param stateBatches 需要持久化的状态批次列表
     * @return 如果确认成功返回空，否则返回异常
     */
    private Optional<Throwable> acknowledgePerOffsetBatchRecords(
        String memberId,
        ShareAcknowledgementBatch batch,
        InFlightBatch inFlightBatch,
        Map<Long, RecordState> recordStateMap,
        List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        lock.writeLock().lock();
        try {
            // 获取第一条记录的状态作为默认状态
            // 当客户端没有提供某条记录的状态时使用该默认值
            RecordState recordStateDefault = recordStateMap.get(batch.firstOffset());
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {

                // 跳过以下记录：
                // 1. 批次中早于请求基准偏移量的记录
                // 2. 早于分区起始偏移量的记录
                if (offsetState.getKey() < batch.firstOffset() || offsetState.getKey() < startOffset) {
                    continue;
                }

                // 如果当前偏移量超过批次末尾，停止处理
                if (offsetState.getKey() > batch.lastOffset()) {
                    break;
                }

                // 验证记录是否处于已获取状态
                if (offsetState.getValue().state != RecordState.ACQUIRED) {
                    log.debug("The offset is not acquired, offset: {} batch: {} for the share"
                            + " partition: {}-{}", offsetState.getKey(), inFlightBatch, groupId,
                        topicIdPartition);
                    return Optional.of(new InvalidRecordStateException(
                        "The batch cannot be acknowledged. The offset is not acquired."));
                }

                // 验证消费者是否是该记录的所有者
                if (!offsetState.getValue().memberId.equals(memberId)) {
                    log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                            + " partition: {}-{}", memberId, offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    return Optional.of(
                        new InvalidRecordStateException("Member is not the owner of offset"));
                }

                // 确定记录的目标状态
                // 如果客户端提供了每个偏移量的状态，使用对应状态
                // 否则使用批次默认状态
                RecordState recordState =
                    recordStateMap.size() > 1 ? recordStateMap.get(offsetState.getKey()) :
                        recordStateDefault;
                
                // 执行状态转换
                InFlightState updateResult = offsetState.getValue().startStateTransition(
                    recordState,
                    false,
                    this.maxDeliveryCount,
                    EMPTY_MEMBER_ID
                );
                
                // 验证状态转换结果
                if (updateResult == null) {
                    log.debug("Unable to acknowledge records for the offset: {} in batch: {}"
                            + " for the share partition: {}-{}", offsetState.getKey(),
                        inFlightBatch, groupId, topicIdPartition);
                    return Optional.of(new InvalidRecordStateException(
                        "Unable to acknowledge records for the batch"));
                }
                
                // 记录更新后的状态
                updatedStates.add(updateResult);
                stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                    updateResult.state.id, (short) updateResult.deliveryCount));
                    
                // 如果记录状态变为可用且未被归档
                // 需要重新计算下一个获取偏移量
                if (recordState == RecordState.AVAILABLE
                    && updateResult.state != RecordState.ARCHIVED) {
                    findNextFetchOffset.set(true);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return Optional.empty();
    }

    /**
     * 确认完整批次的状态
     * 
     * 应用场景：
     * 1. 消费者需要对整个批次进行统一确认
     * 2. 批量处理场景下的性能优化
     * 
     * 设计考虑：
     * 1. 使用写锁保证并发安全
     * 2. 批次级别的原子操作
     * 3. 状态一致性保证
     * 
     * @param batch 待确认的批次
     * @param inFlightBatch 正在处理中的批次
     * @param recordState 目标记录状态
     * @param updatedStates 更新后的状态列表
     * @param stateBatches 需要持久化的状态批次列表
     * @return 如果确认成功返回空，否则返回异常
     */
    private Optional<Throwable> acknowledgeCompleteBatch(
        ShareAcknowledgementBatch batch,
        InFlightBatch inFlightBatch,
        RecordState recordState,
        List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        lock.writeLock().lock();
        try {
            // 记录完整批次确认的操作
            log.trace("Acknowledging complete batch record {} for the share partition: {}-{}",
                batch, groupId, topicIdPartition);
                
            // 验证批次是否处于已获取状态
            if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                    inFlightBatch, groupId, topicIdPartition);
                return Optional.of(new InvalidRecordStateException(
                    "The batch cannot be acknowledged. The batch is not in the acquired state."));
            }

            // 执行批次状态转换
            // 无论确认类型如何，都将成员ID重置为EMPTY_MEMBER_ID
            // 因为批次要么被释放，要么转移到不需要成员ID的状态
            // 成员ID只在批次被获取时才重要
            InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                recordState,
                false,
                this.maxDeliveryCount,
                EMPTY_MEMBER_ID
            );
            
            // 验证状态转换结果
            if (updateResult == null) {
                log.debug("Unable to acknowledge records for the batch: {} with state: {}"
                        + " for the share partition: {}-{}", inFlightBatch, recordState, groupId,
                    topicIdPartition);
                return Optional.of(
                    new InvalidRecordStateException("Unable to acknowledge records for the batch"));
            }

            // 记录更新后的状态
            updatedStates.add(updateResult);
            stateBatches.add(
                new PersisterStateBatch(inFlightBatch.firstOffset, inFlightBatch.lastOffset,
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // 如果批次状态变为可用且未被归档
            // 需要重新计算下一个获取偏移量
            if (recordState == RecordState.AVAILABLE
                && updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return Optional.empty();
    }

    /**
     * 更新获取偏移量的元数据信息
     * 
     * 应用场景：
     * 1. 更新分区的获取进度
     * 2. 优化后续获取请求的性能
     * 
     * 设计考虑：
     * 1. 使用写锁保证并发安全
     * 2. 元数据更新的原子性
     * 
     * @param nextFetchOffset 下一个要获取的偏移量
     * @param logOffsetMetadata 日志偏移量的元数据信息
     */
    protected void updateFetchOffsetMetadata(long nextFetchOffset, LogOffsetMetadata logOffsetMetadata) {
        lock.writeLock().lock();
        try {
            // 更新获取偏移量的元数据信息
            fetchOffsetMetadata.updateOffsetMetadata(nextFetchOffset, logOffsetMetadata);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取指定偏移量的元数据信息
     * 
     * 应用场景：
     * 1. 获取分区特定偏移量的元数据
     * 2. 用于优化获取请求的性能
     * 
     * 设计考虑：
     * 1. 使用读锁提高并发性能
     * 2. 空值处理机制
     * 
     * @param nextFetchOffset 要查询的偏移量
     * @return 返回偏移量对应的元数据信息，如果不存在则返回空
     */
    protected Optional<LogOffsetMetadata> fetchOffsetMetadata(long nextFetchOffset) {
        lock.readLock().lock();
        try {
            // 如果元数据为空或偏移量不匹配，返回空
            if (fetchOffsetMetadata.offsetMetadata() == null || fetchOffsetMetadata.offset() != nextFetchOffset)
                return Optional.empty();
            // 返回找到的元数据信息
            return Optional.of(fetchOffsetMetadata.offsetMetadata());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取分区当前状态
     * 该方法主要用于测试目的
     * 
     * 应用场景：
     * 1. 单元测试中验证分区状态
     * 2. 调试时检查分区状态
     * 
     * 设计考虑：
     * 1. 使用读锁提高并发性能
     * 2. 保证状态访问的线程安全
     * 
     * @return 返回当前的分区状态
     */
    // 用于测试可见
    SharePartitionState partitionState() {
        lock.readLock().lock();
        try {
            // 返回当前分区状态
            return partitionState;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 回滚或处理状态更新
     * 该方法用于处理状态更新的结果，包括成功和失败的情况
     * 
     * 应用场景：
     * 1. 当消费者确认或释放记录时，需要更新记录状态
     * 2. 当状态更新失败时，需要回滚已更改的状态
     * 3. 当状态更新成功时，需要更新缓存和偏移量
     * 
     * 设计考虑：
     * 1. 使用写锁确保状态更新的原子性
     * 2. 异步处理持久化操作，避免阻塞
     * 3. 完整的错误处理和日志记录
     * 
     * @param future 用于完成异步操作的Future
     * @param throwable 可能的异常信息
     * @param updatedStates 需要更新的状态列表
     * @param stateBatches 需要持久化的状态批次列表
     */
    // 用于测试可见
    void rollbackOrProcessStateUpdates(
        CompletableFuture<Void> future,
        Throwable throwable,
        List<InFlightState> updatedStates,
        List<PersisterStateBatch> stateBatches
    ) {
        // 获取写锁以确保状态更新的原子性
        lock.writeLock().lock();
        try {
            // 如果存在异常，回滚所有状态更改
            if (throwable != null) {
                // 使用DEBUG级别日志避免因客户端错误导致日志泛滥
                log.debug("Request failed for updating state, rollback any changed state"
                    + " for the share partition: {}-{}", groupId, topicIdPartition);
                // 将所有状态转换标记为失败
                updatedStates.forEach(state -> state.completeStateTransition(false));
                // 使用异常完成Future
                future.completeExceptionally(throwable);
                return;
            }

            // 如果没有需要更新的状态，直接完成Future
            if (stateBatches.isEmpty() && updatedStates.isEmpty()) {
                future.complete(null);
                return;
            }
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }

        // 异步写入状态到持久化存储
        writeShareGroupState(stateBatches).whenComplete((result, exception) -> {
            // 获取写锁以更新内存中的状态
            lock.writeLock().lock();
            try {
                // 如果持久化过程中发生异常
                if (exception != null) {
                    // 记录错误日志
                    log.error("Failed to write state to persister for the share partition: {}-{}",
                        groupId, topicIdPartition, exception);
                    // 回滚所有状态更改
                    updatedStates.forEach(state -> state.completeStateTransition(false));
                    // 使用异常完成Future
                    future.completeExceptionally(exception);
                    return;
                }

                // 持久化成功，记录跟踪日志
                log.trace("State change request successful for share partition: {}-{}",
                    groupId, topicIdPartition);
                // 更新所有状态
                updatedStates.forEach(state -> {
                    // 标记状态转换成功
                    state.completeStateTransition(true);
                    // 取消获取锁定超时任务，因为记录已被成功确认/释放
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                });
                // 在确认/释放获取的记录后，更新缓存状态和起始/结束偏移量
                maybeUpdateCachedStateAndOffsets();
                // 完成Future
                future.complete(null);
            } finally {
                // 释放写锁
                lock.writeLock().unlock();
            }
        });
    }

    /**
     * 尝试更新缓存状态和偏移量
     * 该方法在记录被确认或释放后，更新共享分区的缓存状态和偏移量
     * 
     * 应用场景：
     * 1. 当消费者确认或拒绝记录后，需要更新分区的消费进度
     * 2. 当需要清理已确认的记录缓存，释放内存资源
     * 3. 当需要处理记录间隙（gap）时，确保不丢失数据
     * 
     * 设计考虑：
     * 1. 使用写锁确保状态更新的原子性
     * 2. 批量处理记录状态，提高性能
     * 3. 处理记录间隙，保证数据连续性
     */
    private void maybeUpdateCachedStateAndOffsets() {
        // 获取写锁以确保状态更新的原子性
        lock.writeLock().lock();
        try {
            // 检查是否可以移动起始偏移量
            if (!canMoveStartOffset()) {
                return;
            }

            // 查找最后一个已确认的偏移量
            // 新的起始偏移量将是lastOffsetAcknowledged + 1
            long lastOffsetAcknowledged = findLastOffsetAcknowledged();
            // 如果lastOffsetAcknowledged为-1，表示无法向前移动起始偏移量
            if (lastOffsetAcknowledged == -1) {
                return;
            }

            // 获取缓存中最后一个偏移量
            // 如果所有记录都已确认（接受或拒绝），则清空整个缓存
            long lastCachedOffset = cachedState.lastEntry().getValue().lastOffset();
            if (lastOffsetAcknowledged == lastCachedOffset) {
                // 更新起始偏移量为下一个将被获取的偏移量
                startOffset = lastCachedOffset + 1;
                endOffset = lastCachedOffset + 1;
                // 清空缓存状态
                cachedState.clear();
                return;
            }

            /*
             缓存状态中包含一些尚未确认的记录，这些记录不应被移除
             只能移除部分缓存（subMap）。移除批次的逻辑如下：
             a) 只能移除完全确认的批次。例如，如果有批次(0-99)，
                且只有0-49的记录被确认（接受或拒绝），则前50条记录
                不会从缓存中移除。相反，起始偏移量会移动到50，但批次
                只有在所有消息(0-99)都被确认后才会被移除。
            */

            // 由于只移除部分缓存，需要找到要移除的子映射的第一个和最后一个键
            long firstKeyToRemove = cachedState.firstKey();
            long lastKeyToRemove;
            // 获取小于等于lastOffsetAcknowledged的最大条目
            NavigableMap.Entry<Long, InFlightBatch> entry = cachedState.floorEntry(lastOffsetAcknowledged);
            
            // 如果lastOffsetAcknowledged等于条目的最后偏移量，则可能移除整个批次
            if (lastOffsetAcknowledged == entry.getValue().lastOffset()) {
                // 获取下一个批次的起始偏移量
                startOffset = cachedState.higherKey(lastOffsetAcknowledged);
                // 检查是否存在初始读取间隙偏移量窗口
                if (isInitialReadGapOffsetWindowActive()) {
                    // 处理lastOffsetAcknowledged之后存在可获取间隙的情况
                    // 例如，缓存状态包含以下批次：{(0, 10), (11, 20), (31,40)}，且所有批次都已确认
                    // 存在21到30的间隙。假设initialReadGapOffset.gapStartOffset为21
                    // 此时lastOffsetAcknowledged为20，但不能简单地将起始偏移量移动到
                    // 下一个缓存批次的第一个偏移量（31）。中间存在可获取的间隙(21-30)
                    // 起始偏移量应该在21。因此，将startOffset设置为gapStartOffset
                    // 和lastOffsetAcknowledged的下一个键中的较小值
                    startOffset = Math.min(initialReadGapOffset.gapStartOffset(), startOffset);
                }
                lastKeyToRemove = entry.getKey();
            } else {
                // 只有当lastOffsetAcknowledged在某个状态批次中间时才会到达这里
                // 此时可以简单地将起始偏移量移动到lastOffsetAcknowledged的下一个偏移量
                startOffset = lastOffsetAcknowledged + 1;
                // 检查是否是缓存中的第一个批次
                if (entry.getKey().equals(cachedState.firstKey())) {
                    // 如果缓存中的第一个批次还有未确认的记录，则不移除任何内容
                    lastKeyToRemove = -1;
                } else {
                    // 获取entry键的前一个键
                    lastKeyToRemove = cachedState.lowerKey(entry.getKey());
                }
            }

            // 如果有需要移除的键，清理对应的子映射
            if (lastKeyToRemove != -1) {
                cachedState.subMap(firstKeyToRemove, true, lastKeyToRemove, true).clear();
            }
        } finally {
            // 释放写锁
            lock.writeLock().unlock();
        }
    }

    /**
     * 检查是否可以移动分区的起始偏移量
     * 
     * 应用场景：
     * 1. 在确认请求完成后，需要更新分区的消费进度
     * 2. 处理记录间隙时，确保不会跳过未确认的记录
     * 3. 优化内存使用，清理已确认的记录缓存
     * 
     * 移动起始偏移量的条件：
     * 1. 缓存状态不为空
     * 2. 记录的确认类型为ACCEPT或REJECT
     * 3. 之前的所有记录都已被确认（ACCEPT或REJECT）
     * 
     * @return 如果可以移动起始偏移量则返回true，否则返回false
     */
    private boolean canMoveStartOffset() {
        // 检查缓存状态是否为空
        if (cachedState.isEmpty()) {
            return false;
        }

        // 获取小于等于起始偏移量的最大条目
        NavigableMap.Entry<Long, InFlightBatch> entry = cachedState.floorEntry(startOffset);
        if (entry == null) {
            // 当起始偏移量处存在间隙时，在缓存状态中找不到起始偏移量
            // 例如，如果起始偏移量是10，而缓存状态包含批次 -> { (21, 30), (31, 40) }
            // 这种情况只会在共享分区初始化时出现，且读取状态响应中包含间隙
            // 这种情况可能发生在前一个实例中：
            // 1. 间隙偏移量(10-20)被获取但未确认
            // 2. 下一批偏移量(21-30)被获取并确认
            // 在上面的例子中，可能在共享分区的前一个实例中，
            // 批次10-20被获取但未确认，而批次21-30被获取并确认
            // 因此，持久化器不知道批次10-20发生了什么
            // 在重新初始化共享分区时，起始偏移量设置为10，
            // 而缓存状态有批次21-30，导致出现间隙
            log.debug("The start offset: {} is not found in the cached state for share partition: {}-{} " +
                "as there is an acquirable gap at the beginning. Cannot move the start offset.", startOffset, groupId, topicIdPartition);
            return false;
        }

        // 获取起始偏移量的记录状态
        // 如果offsetState为null，使用批次状态
        // 否则使用偏移量对应的状态
        RecordState startOffsetState = entry.getValue().offsetState == null ?
            entry.getValue().batchState() :
            entry.getValue().offsetState().get(startOffset).state();
        // 检查记录是否已被确认
        return isRecordStateAcknowledged(startOffsetState);
    }

    /**
     * 检查初始读取间隙偏移量窗口是否处于活动状态
     * 
     * 应用场景：
     * 1. 处理分区初始化时的记录间隙
     * 2. 确保不会丢失间隙中的记录
     * 3. 维护数据的连续性
     * 
     * 设计考虑：
     * 1. 通过比较endOffset确保窗口范围的准确性
     * 2. 支持处理不连续的记录序列
     * 
     * @return 如果初始读取间隙偏移量窗口活动则返回true，否则返回false
     */
    private boolean isInitialReadGapOffsetWindowActive() {
        // 检查initialReadGapOffset是否存在且其结束偏移量等于当前结束偏移量
        return initialReadGapOffset != null && initialReadGapOffset.endOffset() == endOffset;
    }

    /**
     * The record state is considered acknowledged if it is either acknowledged or archived.
     * These are terminal states for the record.
     *
     * @param recordState The record state to check.
     *
     * @return True if the record state is acknowledged or archived, false otherwise.
     */
    private boolean isRecordStateAcknowledged(RecordState recordState) {
        return recordState == RecordState.ACKNOWLEDGED || recordState == RecordState.ARCHIVED;
    }

    // Visible for testing
    long findLastOffsetAcknowledged() {
        long lastOffsetAcknowledged = -1;
        lock.readLock().lock();
        try {
            for (NavigableMap.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (isInitialReadGapOffsetWindowActive() && inFlightBatch.lastOffset() >= initialReadGapOffset.gapStartOffset()) {
                    return lastOffsetAcknowledged;
                }

                if (inFlightBatch.offsetState() == null) {
                    if (!isRecordStateAcknowledged(inFlightBatch.batchState())) {
                        return lastOffsetAcknowledged;
                    }
                    lastOffsetAcknowledged = inFlightBatch.lastOffset();
                } else {
                    for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                        if (!isRecordStateAcknowledged(offsetState.getValue().state())) {
                            return lastOffsetAcknowledged;
                        }
                        lastOffsetAcknowledged = offsetState.getKey();
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return lastOffsetAcknowledged;
    }

    /**
     * Find the last offset from the batch which contains the request offset. If found, return the last offset
     * of the batch, otherwise return the request offset.
     *
     * @param batches The batches to search for the request offset.
     * @param offset The request offset to find.
     * @return The last offset of the batch which contains the request offset, otherwise the request offset.
     */
    private long lastOffsetFromBatchWithRequestOffset(
        Iterable<? extends RecordBatch> batches,
        long offset
    ) {
        // Fetch the last batch which might contains the request offset. Avoid calling lastOffset()
        // on the batches as it loads the header in memory.
        RecordBatch previousBatch = null;
        for (RecordBatch batch : batches) {
            if (offset >= batch.baseOffset()) {
                previousBatch =  batch;
                continue;
            }
            break;
        }
        if (previousBatch != null && offset <= previousBatch.lastOffset())
            return previousBatch.lastOffset();
        return offset;
    }

    // Visible for testing
    CompletableFuture<Void> writeShareGroupState(List<PersisterStateBatch> stateBatches) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        persister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(this.groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicIdPartition.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionStateBatchData(
                        topicIdPartition.partition(), stateEpoch, startOffset, leaderEpoch, stateBatches))))
                ).build()).build())
            .whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Failed to write the share group state for share partition: {}-{}", groupId, topicIdPartition, exception);
                    future.completeExceptionally(new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition), exception));
                    return;
                }

                if (result == null || result.topicsData() == null || result.topicsData().size() != 1) {
                    log.error("Failed to write the share group state for share partition: {}-{}. Invalid state found: {}",
                        groupId, topicIdPartition, result);
                    future.completeExceptionally(new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition)));
                    return;
                }

                TopicData<PartitionErrorData> state = result.topicsData().get(0);
                if (state.topicId() != topicIdPartition.topicId() || state.partitions().size() != 1
                    || state.partitions().get(0).partition() != topicIdPartition.partition()) {
                    log.error("Failed to write the share group state for share partition: {}-{}. Invalid topic partition response: {}",
                        groupId, topicIdPartition, result);
                    future.completeExceptionally(new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition)));
                    return;
                }

                PartitionErrorData partitionData = state.partitions().get(0);
                if (partitionData.errorCode() != Errors.NONE.code()) {
                    KafkaException ex = fetchPersisterError(partitionData.errorCode(), partitionData.errorMessage());
                    log.error("Failed to write the share group state for share partition: {}-{} due to exception",
                        groupId, topicIdPartition, ex);
                    future.completeExceptionally(ex);
                    return;
                }
                future.complete(null);
            });
        return future;
    }

    private KafkaException fetchPersisterError(short errorCode, String errorMessage) {
        Errors error = Errors.forCode(errorCode);
        switch (error) {
            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
            case COORDINATOR_LOAD_IN_PROGRESS:
                return new CoordinatorNotAvailableException(errorMessage);
            case GROUP_ID_NOT_FOUND:
                return new GroupIdNotFoundException(errorMessage);
            case UNKNOWN_TOPIC_OR_PARTITION:
                return new UnknownTopicOrPartitionException(errorMessage);
            case FENCED_STATE_EPOCH:
                return new FencedStateEpochException(errorMessage);
            case FENCED_LEADER_EPOCH:
                return new NotLeaderOrFollowerException(errorMessage);
            default:
                return new UnknownServerException(errorMessage);
        }
    }

    // Visible for testing
    AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(String memberId, long firstOffset, long lastOffset) {
        // The recordLockDuration value would depend on whether the dynamic config SHARE_RECORD_LOCK_DURATION_MS in
        // GroupConfig.java is set or not. If dynamic config is set, then that is used, otherwise the value of
        // SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG defined in ShareGroupConfig is used
        int recordLockDurationMs;
        if (groupConfigManager.groupConfig(groupId).isPresent()) {
            recordLockDurationMs = groupConfigManager.groupConfig(groupId).get().shareRecordLockDurationMs();
        } else {
            recordLockDurationMs = defaultRecordLockDurationMs;
        }
        return scheduleAcquisitionLockTimeout(memberId, firstOffset, lastOffset, recordLockDurationMs);
    }

    /**
     * Apply acquisition lock to acquired records.
     *
     * @param memberId The member id of the client that is putting the acquisition lock.
     * @param firstOffset The first offset of the acquired records.
     * @param lastOffset The last offset of the acquired records.
     * @param delayMs The delay in milliseconds after which the acquisition lock will be released.
     */
    private AcquisitionLockTimerTask scheduleAcquisitionLockTimeout(
        String memberId,
        long firstOffset,
        long lastOffset,
        long delayMs
    ) {
        AcquisitionLockTimerTask acquisitionLockTimerTask = acquisitionLockTimerTask(memberId, firstOffset, lastOffset, delayMs);
        timer.add(acquisitionLockTimerTask);
        return acquisitionLockTimerTask;
    }

    private AcquisitionLockTimerTask acquisitionLockTimerTask(
        String memberId,
        long firstOffset,
        long lastOffset,
        long delayMs
    ) {
        return new AcquisitionLockTimerTask(delayMs, memberId, firstOffset, lastOffset);
    }

    private void releaseAcquisitionLockOnTimeout(String memberId, long firstOffset, long lastOffset) {
        List<PersisterStateBatch> stateBatches;
        lock.writeLock().lock();
        try {
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(firstOffset);
            if (floorOffset == null) {
                log.error("Base offset {} not found for share partition: {}-{}", firstOffset, groupId, topicIdPartition);
                return;
            }
            stateBatches = new ArrayList<>();
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, lastOffset, true);
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (inFlightBatch.offsetState() == null
                        && inFlightBatch.batchState() == RecordState.ACQUIRED
                        && checkForStartOffsetWithinBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset())) {

                    // For the case when batch.firstOffset < start offset <= batch.lastOffset, we will be having some
                    // acquired records that need to move to archived state despite their delivery count.
                    inFlightBatch.maybeInitializeOffsetStateUpdate();
                }

                // Case when the state of complete batch is valid
                if (inFlightBatch.offsetState() == null) {
                    releaseAcquisitionLockOnTimeoutForCompleteBatch(inFlightBatch, stateBatches, memberId);
                } else { // Case when batch has a valid offset state map.
                    releaseAcquisitionLockOnTimeoutForPerOffsetBatch(inFlightBatch, stateBatches, memberId, firstOffset, lastOffset);
                }
            }

            if (!stateBatches.isEmpty()) {
                writeShareGroupState(stateBatches).whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to write the share group state on acquisition lock timeout for share partition: {}-{} memberId: {}",
                            groupId, topicIdPartition, memberId, exception);
                    }
                    // Even if write share group state RPC call fails, we will still go ahead with the state transition.
                    // Update the cached state and start and end offsets after releasing the acquisition lock on timeout.
                    maybeUpdateCachedStateAndOffsets();
                });
            }
        } finally {
            lock.writeLock().unlock();
        }

        // Skip null check for stateBatches, it should always be initialized if reached here.
        if (!stateBatches.isEmpty()) {
            // If we have an acquisition lock timeout for a share-partition, then we should check if
            // there is a pending share fetch request for the share-partition and complete it.
            DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
            replicaManager.completeDelayedShareFetchRequest(delayedShareFetchKey);
        }
    }

    private void releaseAcquisitionLockOnTimeoutForCompleteBatch(InFlightBatch inFlightBatch,
                                                                 List<PersisterStateBatch> stateBatches,
                                                                 String memberId) {
        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
            InFlightState updateResult = inFlightBatch.tryUpdateBatchState(
                    inFlightBatch.lastOffset() < startOffset ? RecordState.ARCHIVED : RecordState.AVAILABLE,
                    false,
                    maxDeliveryCount,
                    EMPTY_MEMBER_ID);
            if (updateResult == null) {
                log.error("Unable to release acquisition lock on timeout for the batch: {}"
                        + " for the share partition: {}-{} memberId: {}", inFlightBatch, groupId, topicIdPartition, memberId);
                return;
            }
            stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset(),
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // Cancel the acquisition lock timeout task for the batch since it is completed now.
            updateResult.cancelAndClearAcquisitionLockTimeoutTask();
            if (updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
            return;
        }
        log.debug("The batch is not in acquired state while release of acquisition lock on timeout, skipping, batch: {}"
                + " for the share partition: {}-{} memberId: {}", inFlightBatch, groupId, topicIdPartition, memberId);
    }

    private void releaseAcquisitionLockOnTimeoutForPerOffsetBatch(InFlightBatch inFlightBatch,
                                                                  List<PersisterStateBatch> stateBatches,
                                                                  String memberId,
                                                                  long firstOffset,
                                                                  long lastOffset) {
        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState().entrySet()) {

            // For the first batch which might have offsets prior to the request base
            // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
            if (offsetState.getKey() < firstOffset) {
                continue;
            }
            if (offsetState.getKey() > lastOffset) {
                // No further offsets to process.
                break;
            }
            if (offsetState.getValue().state != RecordState.ACQUIRED) {
                log.debug("The offset is not in acquired state while release of acquisition lock on timeout, skipping, offset: {} batch: {}"
                                + " for the share partition: {}-{} memberId: {}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition, memberId);
                continue;
            }
            InFlightState updateResult = offsetState.getValue().tryUpdateState(
                    offsetState.getKey() < startOffset ? RecordState.ARCHIVED : RecordState.AVAILABLE,
                    false,
                    maxDeliveryCount,
                    EMPTY_MEMBER_ID);
            if (updateResult == null) {
                log.error("Unable to release acquisition lock on timeout for the offset: {} in batch: {}"
                                + " for the share partition: {}-{} memberId: {}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition, memberId);
                continue;
            }
            stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                    updateResult.state.id, (short) updateResult.deliveryCount));

            // Cancel the acquisition lock timeout task for the offset since it is completed now.
            updateResult.cancelAndClearAcquisitionLockTimeoutTask();
            if (updateResult.state != RecordState.ARCHIVED) {
                findNextFetchOffset.set(true);
            }
        }
    }

    private long startOffsetDuringInitialization(long partitionDataStartOffset) throws Exception {
        // Set the state epoch and end offset from the persisted state.
        if (partitionDataStartOffset != PartitionFactory.UNINITIALIZED_START_OFFSET) {
            return partitionDataStartOffset;
        }
        ShareGroupAutoOffsetResetStrategy offsetResetStrategy;
        if (groupConfigManager.groupConfig(groupId).isPresent()) {
            offsetResetStrategy = groupConfigManager.groupConfig(groupId).get().shareAutoOffsetReset();
        } else {
            offsetResetStrategy = GroupConfig.defaultShareAutoOffsetReset();
        }

        if (offsetResetStrategy.type() == ShareGroupAutoOffsetResetStrategy.StrategyType.LATEST) {
            return offsetForLatestTimestamp(topicIdPartition, replicaManager, leaderEpoch);
        } else if (offsetResetStrategy.type() == ShareGroupAutoOffsetResetStrategy.StrategyType.EARLIEST) {
            return offsetForEarliestTimestamp(topicIdPartition, replicaManager, leaderEpoch);
        } else {
            // offsetResetStrategy type is BY_DURATION
            return offsetForTimestamp(topicIdPartition, replicaManager, offsetResetStrategy.timestamp(), leaderEpoch);
        }
    }

    // Visible for testing. Should only be used for testing purposes.
    NavigableMap<Long, InFlightBatch> cachedState() {
        return new ConcurrentSkipListMap<>(cachedState);
    }

    // Visible for testing.
    boolean findNextFetchOffset() {
        return findNextFetchOffset.get();
    }

    // Visible for testing. Should only be used for testing purposes.
    void findNextFetchOffset(boolean findNextOffset) {
        findNextFetchOffset.getAndSet(findNextOffset);
    }

    // Visible for testing
    long startOffset() {
        lock.readLock().lock();
        try {
            return this.startOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing
    long endOffset() {
        lock.readLock().lock();
        try {
            return this.endOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Visible for testing.
    int stateEpoch() {
        return stateEpoch;
    }

    // Visible for testing.
    Timer timer() {
        return timer;
    }

    // Visible for testing
    InitialReadGapOffset initialReadGapOffset() {
        return initialReadGapOffset;
    }

    /**
     * The InitialReadGapOffset class is used to record the gap start and end offset of the probable gaps
     * of available records which are neither known to Persister nor to SharePartition. Share Partition
     * will use this information to determine the next fetch offset and should try to fetch the records
     * in the gap.
     */
    // Visible for Testing
    static class InitialReadGapOffset {
        private final long endOffset;
        private long gapStartOffset;

        InitialReadGapOffset(long endOffset, long gapStartOffset) {
            this.endOffset = endOffset;
            this.gapStartOffset = gapStartOffset;
        }

        long endOffset() {
            return endOffset;
        }

        long gapStartOffset() {
            return gapStartOffset;
        }

        void gapStartOffset(long gapStartOffset) {
            this.gapStartOffset = gapStartOffset;
        }
    }

    // Visible for testing
    final class AcquisitionLockTimerTask extends TimerTask {
        private final long expirationMs;
        private final String memberId;
        private final long firstOffset;
        private final long lastOffset;

        AcquisitionLockTimerTask(long delayMs, String memberId, long firstOffset, long lastOffset) {
            super(delayMs);
            this.expirationMs = time.hiResClockMs() + delayMs;
            this.memberId = memberId;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }

        long expirationMs() {
            return expirationMs;
        }

        /**
         * The task is executed when the acquisition lock timeout is reached. The task releases the acquired records.
         */
        @Override
        public void run() {
            releaseAcquisitionLockOnTimeout(memberId, firstOffset, lastOffset);
        }
    }

    /**
     * The InFlightBatch maintains the in-memory state of the fetched records i.e. in-flight records.
     */
    final class InFlightBatch {
        // The offset of the first record in the batch that is fetched from the log.
        private final long firstOffset;
        // The last offset of the batch that is fetched from the log.
        private final long lastOffset;

        // The batch state of the fetched records. If the offset state map is empty then batchState
        // determines the state of the complete batch else individual offset determines the state of
        // the respective records.
        private InFlightState batchState;

        // The offset state map is used to track the state of the records per offset. However, the
        // offset state map is only required when the state of the offsets within same batch are
        // different. The states can be different when explicit offset acknowledgment is done which
        // is different from the batch state.
        private NavigableMap<Long, InFlightState> offsetState;

        InFlightBatch(String memberId, long firstOffset, long lastOffset, RecordState state,
            int deliveryCount, AcquisitionLockTimerTask acquisitionLockTimeoutTask
        ) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.batchState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
        }

        // Visible for testing.
        long firstOffset() {
            return firstOffset;
        }

        // Visible for testing.
        long lastOffset() {
            return lastOffset;
        }

        // Visible for testing.
        RecordState batchState() {
            return inFlightState().state;
        }

        // Visible for testing.
        String batchMemberId() {
            if (batchState == null) {
                throw new IllegalStateException("The batch member id is not available as the offset state is maintained");
            }
            return batchState.memberId;
        }

        // Visible for testing.
        int batchDeliveryCount() {
            if (batchState == null) {
                throw new IllegalStateException("The batch delivery count is not available as the offset state is maintained");
            }
            return batchState.deliveryCount;
        }

        // Visible for testing.
        AcquisitionLockTimerTask batchAcquisitionLockTimeoutTask() {
            return inFlightState().acquisitionLockTimeoutTask;
        }

        // Visible for testing.
        NavigableMap<Long, InFlightState> offsetState() {
            return offsetState;
        }

        private InFlightState inFlightState() {
            if (batchState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return batchState;
        }

        private boolean batchHasOngoingStateTransition() {
            return inFlightState().hasOngoingStateTransition();
        }

        private void archiveBatch(String newMemberId) {
            inFlightState().archive(newMemberId);
        }

        private InFlightState tryUpdateBatchState(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            if (batchState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return batchState.tryUpdateState(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
        }

        private InFlightState startBatchStateTransition(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount,
                                                        String newMemberId) {
            if (batchState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return batchState.startStateTransition(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
        }

        private void maybeInitializeOffsetStateUpdate() {
            if (offsetState == null) {
                offsetState = new ConcurrentSkipListMap<>();
                // The offset state map is not initialized hence initialize the state of the offsets
                // from the first offset to the last offset. Mark the batch inflightState to null as
                // the state of the records is maintained in the offset state map now.
                for (long offset = this.firstOffset; offset <= this.lastOffset; offset++) {
                    if (batchState.acquisitionLockTimeoutTask != null) {
                        // The acquisition lock timeout task is already scheduled for the batch, hence we need to schedule
                        // the acquisition lock timeout task for the offset as well.
                        long delayMs = batchState.acquisitionLockTimeoutTask.expirationMs() - time.hiResClockMs();
                        AcquisitionLockTimerTask timerTask = acquisitionLockTimerTask(batchState.memberId, offset, offset, delayMs);
                        offsetState.put(offset, new InFlightState(batchState.state, batchState.deliveryCount, batchState.memberId, timerTask));
                        timer.add(timerTask);
                    } else {
                        offsetState.put(offset, new InFlightState(batchState.state, batchState.deliveryCount, batchState.memberId));
                    }
                }
                // Cancel the acquisition lock timeout task for the batch as the offset state is maintained.
                if (batchState.acquisitionLockTimeoutTask != null) {
                    batchState.cancelAndClearAcquisitionLockTimeoutTask();
                }
                batchState = null;
            }
        }

        private void updateAcquisitionLockTimeout(AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
            inFlightState().acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        @Override
        public String toString() {
            return "InFlightBatch(" +
                "firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", inFlightState=" + batchState +
                ", offsetState=" + ((offsetState == null) ? "null" : offsetState) +
                ")";
        }
    }

    /**
     * The InFlightState is used to track the state and delivery count of a record that has been
     * fetched from the leader. The state of the record is used to determine if the record should
     * be re-deliver or if it can be acknowledged or archived.
     */
    static final class InFlightState {

        // The state of the fetch batch records.
        private RecordState state;
        // The number of times the records has been delivered to the client.
        private int deliveryCount;
        // The member id of the client that is fetching/acknowledging the record.
        private String memberId;
        // The state of the records before the transition. In case we need to revert an in-flight state, we revert the above
        // attributes of InFlightState to this state, namely - state, deliveryCount and memberId.
        private InFlightState rollbackState;
        // The timer task for the acquisition lock timeout.
        private AcquisitionLockTimerTask acquisitionLockTimeoutTask;


        InFlightState(RecordState state, int deliveryCount, String memberId) {
            this(state, deliveryCount, memberId, null);
        }

        InFlightState(RecordState state, int deliveryCount, String memberId, AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
            this.state = state;
            this.deliveryCount = deliveryCount;
            this.memberId = memberId;
            this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        // Visible for testing.
        RecordState state() {
            return state;
        }

        String memberId() {
            return memberId;
        }

        // Visible for testing.
        TimerTask acquisitionLockTimeoutTask() {
            return acquisitionLockTimeoutTask;
        }

        void updateAcquisitionLockTimeoutTask(AcquisitionLockTimerTask acquisitionLockTimeoutTask) throws IllegalArgumentException {
            if (this.acquisitionLockTimeoutTask != null) {
                throw new IllegalArgumentException("Existing acquisition lock timeout exists, cannot override.");
            }
            this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        void cancelAndClearAcquisitionLockTimeoutTask() {
            acquisitionLockTimeoutTask.cancel();
            acquisitionLockTimeoutTask = null;
        }

        private boolean hasOngoingStateTransition() {
            if (rollbackState == null) {
                // This case could occur when the batch/offset hasn't transitioned even once or the state transitions have
                // been committed.
                return false;
            }
            return rollbackState.state != null;
        }

        /**
         * Try to update the state of the records. The state of the records can only be updated if the
         * new state is allowed to be transitioned from old state. The delivery count is not incremented
         * if the state update is unsuccessful.
         *
         * @param newState The new state of the records.
         * @param incrementDeliveryCount Whether to increment the delivery count.
         *
         * @return {@code InFlightState} if update succeeds, null otherwise. Returning state
         *         helps update chaining.
         */
        private InFlightState tryUpdateState(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            try {
                if (newState == RecordState.AVAILABLE && deliveryCount >= maxDeliveryCount) {
                    newState = RecordState.ARCHIVED;
                }
                state = state.validateTransition(newState);
                if (incrementDeliveryCount && newState != RecordState.ARCHIVED) {
                    deliveryCount++;
                }
                memberId = newMemberId;
                return this;
            } catch (IllegalStateException e) {
                log.error("Failed to update state of the records", e);
                return null;
            }
        }

        private void archive(String newMemberId) {
            state = RecordState.ARCHIVED;
            memberId = newMemberId;
        }

        private InFlightState startStateTransition(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount, String newMemberId) {
            rollbackState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
            return tryUpdateState(newState, incrementDeliveryCount, maxDeliveryCount, newMemberId);
        }

        private void completeStateTransition(boolean commit) {
            if (commit) {
                rollbackState = null;
                return;
            }
            state = rollbackState.state;
            deliveryCount = rollbackState.deliveryCount;
            memberId = rollbackState.memberId;
            rollbackState = null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, deliveryCount, memberId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InFlightState that = (InFlightState) o;
            return state == that.state && deliveryCount == that.deliveryCount && memberId.equals(that.memberId);
        }

        @Override
        public String toString() {
            return "InFlightState(" +
                "state=" + state.toString() +
                ", deliveryCount=" + deliveryCount +
                ", memberId=" + memberId +
                ")";
        }
    }

    /**
     * FetchOffsetMetadata class is used to cache offset and its log metadata.
     */
    static final class OffsetMetadata {
        // This offset could be different from offsetMetadata.messageOffset if it's in the middle of a batch.
        private long offset;
        private LogOffsetMetadata offsetMetadata;

        OffsetMetadata() {
            offset = -1;
        }

        long offset() {
            return offset;
        }

        LogOffsetMetadata offsetMetadata() {
            return offsetMetadata;
        }

        void updateOffsetMetadata(long offset, LogOffsetMetadata offsetMetadata) {
            this.offset = offset;
            this.offsetMetadata = offsetMetadata;
        }
    }
}
