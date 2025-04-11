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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 事务管理器类,负责维护Kafka生产者的事务状态。同时也维护确保幂等性生产所需的状态。
 * 
 * 主要功能:
 * 1. 事务状态管理 - 通过状态机维护事务的生命周期(初始化、开始、提交、中止等)
 * 2. 事务协调 - 与事务协调器(TransactionCoordinator)交互,处理事务相关请求
 * 3. 幂等性保证 - 维护Producer ID、Epoch和序列号,确保消息的精确一次语义
 * 4. 错误恢复 - 处理各类异常情况,包括网络错误、超时等
 * 
 * 关键组件:
 * - 状态机: 通过State枚举定义事务的各个状态及其转换规则
 * - 请求队列: 使用优先级队列管理事务相关请求的发送顺序
 * - 分区管理: 跟踪事务涉及的主题分区
 * 
 * 线程安全性:
 * - 大部分方法都是同步的(synchronized),确保线程安全
 * - 使用volatile变量保证状态的可见性
 */
public class TransactionManager {
    // 表示当前没有正在处理的请求的标识符
    private static final int NO_INFLIGHT_REQUEST_CORRELATION_ID = -1;

    // 日志记录器
    private final Logger log;
    // 事务ID,用于唯一标识一个事务生产者
    private final String transactionalId;
    // 事务超时时间(毫秒)
    private final int transactionTimeoutMs;
    // API版本信息
    private final ApiVersions apiVersions;

    // 事务分区映射,维护事务涉及的分区信息
    private final TxnPartitionMap txnPartitionMap;

    // 待提交的事务偏移量,用于消费者组提交
    private final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits;

    // 存储具有未解决序列号的分区
    // 当一个批次在发送至少一次后在本地过期,该分区就被认为处于未解决状态
    // 在这种状态下,我们不能为该分区分配新的序列号,直到未解决状态被清除
    // 这种情况可能发生在:
    // 1. 其他在途批次成功返回(表明过期批次实际上已到达broker)
    // 2. 如果在途请求数降为零时仍未收到成功响应,我们会重置producer id并清除此数据结构
    // 
    // map的值是过期批次之后的批次的序列号(通过将其记录数加到其序列号上计算得出)
    // 用于判断后续批次是否紧跟在过期批次之后
    private final Map<TopicPartition, Integer> partitionsWithUnresolvedSequences;

    // 存储需要重写序列号的分区
    // 这些分区收到了触发epoch递增的错误
    // 当epoch递增时,这些分区中在途批次的序列号将被重写
    private final Set<TopicPartition> partitionsToRewriteSequences;

    // 事务请求处理队列,按优先级排序
    // 用于管理和发送事务相关的请求(如FindCoordinator、InitProducerId等)
    private final PriorityQueue<TxnRequestHandler> pendingRequests;
    
    // 新加入事务的分区集合
    // 当生产者首次向某个分区发送数据时,该分区会被添加到此集合
    private final Set<TopicPartition> newPartitionsInTransaction;
    
    // 等待加入事务的分区集合
    // 这些分区已发起AddPartitionsToTxn请求但尚未收到响应
    private final Set<TopicPartition> pendingPartitionsInTransaction;
    
    // 当前事务中的所有分区集合
    // 包含已确认加入事务的所有分区
    private final Set<TopicPartition> partitionsInTransaction;

    /**
     * 事务管理器在其正常运行过程中,会在{@link State}中定义的不同内部状态之间转换
     * (通过更新{@link #currentState})。这些状态转换由以下两类线程的操作触发:
     *
     * <ul>
     *     <li><em>应用程序</em>线程 - 调用{@link Producer} API</li>
     *     <li><em>{@link Sender}</em>线程 - 执行后台操作</li>
     * </ul>
     *
     * 状态转换的处理策略:
     * 1. 应用程序线程:
     *    - 当检测到无效状态转换时,不会更新{@link #currentState}
     *    - 抛出{@link IllegalStateException}异常
     *    - 这使应用程序有机会修复问题,而不会永久破坏事务管理器的状态
     *
     * 2. Sender线程:
     *    - 当检测到无效状态转换时,除了抛出异常
     *    - 还会通过将{@link #currentState}设置为{@link State#FATAL_ERROR}来使事务管理器进入不可恢复的状态
     *
     * 会触发状态转换的Producer API调用包括:
     * <ul>
     *     <li>{@link Producer#initTransactions()} -> {@link #initializeTransactions()}</li>
     *     <li>{@link Producer#beginTransaction()} -> {@link #beginTransaction()}</li>
     *     <li>{@link Producer#commitTransaction()} -> {@link #beginCommit()}</li>
     *     <li>{@link Producer#abortTransaction()} -> {@link #beginAbort()}</li>
     *     <li>{@link Producer#sendOffsetsToTransaction()} -> 
     *         {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}</li>
     *     <li>{@link Producer#send()} -> 
     *         {@link #maybeAddPartition(TopicPartition)},
     *         {@link #maybeTransitionToErrorState(RuntimeException)}</li>
     * </ul>
     *
     * 重要说明:
     * 1. Producer的大部分工作都委托给Sender线程异步执行,包括:
     *    - 记录批处理
     *    - 网络I/O
     *    - Broker响应处理
     *
     * 2. 错误状态处理:
     *    - 当事务管理器处于致命状态时,必须防止可能的数据损坏
     *    - 任何后续的事务操作(无论是应用线程还是Sender线程发起)都应该失败
     *    - 通过{@link #maybeFailWithError()}方法实现,抛出{@link KafkaException}
     *    - 这确保了事务的保证不会被违反
     *
     * 更多细节请参考KAFKA-14831
     */
    private final ThreadLocal<Boolean> shouldPoisonStateOnInvalidTransition;
    private PendingStateTransition pendingTransition;

    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    private final long retryBackoffMs;

    // The retryBackoff is overridden to the following value if the first AddPartitions receives a
    // CONCURRENT_TRANSACTIONS error.
    private static final long ADD_PARTITIONS_RETRY_BACKOFF_MS = 20L;

    private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;
    private boolean coordinatorSupportsBumpingEpoch;

    private volatile State currentState = State.UNINITIALIZED;
    private volatile RuntimeException lastError = null;
    private volatile ProducerIdAndEpoch producerIdAndEpoch;
    private volatile boolean transactionStarted = false;
    private volatile boolean clientSideEpochBumpRequired = false;
    private volatile long latestFinalizedFeaturesEpoch = -1;
    private volatile boolean isTransactionV2Enabled = false;

    /**
     * 事务状态枚举,定义了事务的所有可能状态
     */
    private enum State {
        // 初始状态,事务管理器刚创建时的状态
        UNINITIALIZED,
        // 正在初始化,正在获取Producer ID和Epoch
        INITIALIZING,
        // 已就绪,可以开始新的事务
        READY,
        // 事务进行中,可以发送消息和提交偏移量
        IN_TRANSACTION,
        // 正在提交事务
        COMMITTING_TRANSACTION,
        // 正在中止事务
        ABORTING_TRANSACTION,
        // 可中止错误状态,事务可以被中止并恢复
        ABORTABLE_ERROR,
        // 致命错误状态,事务管理器无法恢复
        FATAL_ERROR;

        /**
         * 检查状态转换是否有效
         * @param source 源状态
         * @param target 目标状态
         * @return 如果转换有效则返回true
         */
        private boolean isTransitionValid(State source, State target) {
            switch (target) {
                case UNINITIALIZED:
                    // 只能从READY或ABORTABLE_ERROR状态回到UNINITIALIZED
                    return source == READY || source == ABORTABLE_ERROR;
                case INITIALIZING:
                    // 可以从UNINITIALIZED开始初始化,或从事务完成状态重新初始化
                    return source == UNINITIALIZED || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case READY:
                    // 初始化完成或事务完成后进入READY状态
                    return source == INITIALIZING || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case IN_TRANSACTION:
                    // 只能从READY状态开始事务
                    return source == READY;
                case COMMITTING_TRANSACTION:
                    // 只能从IN_TRANSACTION状态开始提交
                    return source == IN_TRANSACTION;
                case ABORTING_TRANSACTION:
                    // 可以从IN_TRANSACTION或ABORTABLE_ERROR状态开始中止
                    return source == IN_TRANSACTION || source == ABORTABLE_ERROR;
                case ABORTABLE_ERROR:
                    // 事务执行过程中的可恢复错误
                    return source == IN_TRANSACTION || source == COMMITTING_TRANSACTION || source == ABORTABLE_ERROR
                            || source == INITIALIZING;
                case FATAL_ERROR:
                default:
                    // 可以无条件转换到FATAL_ERROR状态
                    // FATAL_ERROR是不可恢复的,一旦进入该状态:
                    // 1. 必须关闭生产者
                    // 2. 或者只能执行非事务性请求
                    return true;
            }
        }
    }

    /**
     * 请求优先级枚举,用于确定事务请求的发送顺序
     * 
     * 优先级从高到低:
     * 1. FIND_COORDINATOR(0) - 查找协调器请求必须最先发送
     * 2. INIT_PRODUCER_ID(1) - 获取Producer ID请求次之
     * 3. ADD_PARTITIONS_OR_OFFSETS(2) - 添加分区或偏移量到事务
     * 4. END_TXN(3) - 结束事务请求最后发送
     * 5. EPOCH_BUMP(4) - 特殊情况:作为事务结束的一部分递增epoch
     */
    private enum Priority {
        // 查找协调器请求(最高优先级)
        FIND_COORDINATOR(0),
        // 初始化Producer ID请求
        INIT_PRODUCER_ID(1),
        // 添加分区或偏移量到事务
        ADD_PARTITIONS_OR_OFFSETS(2),
        // 结束事务请求
        END_TXN(3),
        // 递增Producer Epoch(最低优先级)
        EPOCH_BUMP(4);

        // 优先级数值,数值越小优先级越高
        final int priority;

        Priority(int priority) {
            this.priority = priority;
        }
    }

    /**
     * 事务管理器构造函数
     * 
     * @param logContext 日志上下文
     * @param transactionalId 事务ID
     * @param transactionTimeoutMs 事务超时时间(毫秒)
     * @param retryBackoffMs 重试等待时间(毫秒)
     * @param apiVersions API版本信息
     */
    public TransactionManager(final LogContext logContext,
                              final String transactionalId,
                              final int transactionTimeoutMs,
                              final long retryBackoffMs,
                              final ApiVersions apiVersions) {
        // 初始化生产者ID和Epoch为NONE
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
        // 设置事务ID
        this.transactionalId = transactionalId;
        // 初始化日志记录器
        this.log = logContext.logger(TransactionManager.class);
        // 设置事务超时时间
        this.transactionTimeoutMs = transactionTimeoutMs;
        // 初始化协调器为null
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        // 初始化分区相关集合
        this.newPartitionsInTransaction = new HashSet<>();
        this.pendingPartitionsInTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        // 初始化状态转换毒化标志
        this.shouldPoisonStateOnInvalidTransition = ThreadLocal.withInitial(() -> false);
        // 初始化请求优先级队列
        this.pendingRequests = new PriorityQueue<>(10, Comparator.comparingInt(o -> o.priority().priority));
        // 初始化偏移量提交和序列号相关Map
        this.pendingTxnOffsetCommits = new HashMap<>();
        this.partitionsWithUnresolvedSequences = new HashMap<>();
        this.partitionsToRewriteSequences = new HashSet<>();
        // 设置重试等待时间
        this.retryBackoffMs = retryBackoffMs;
        // 初始化事务分区映射
        this.txnPartitionMap = new TxnPartitionMap(logContext);
        // 设置API版本信息
        this.apiVersions = apiVersions;
    }

    /**
     * 设置是否在无效状态转换时将事务管理器置于毒化状态
     * 
     * @param shouldPoisonState true表示启用毒化状态,false表示禁用
     */
    void setPoisonStateOnInvalidTransition(boolean shouldPoisonState) {
        shouldPoisonStateOnInvalidTransition.set(shouldPoisonState);
    }

    /**
     * 初始化事务,使用默认的空ProducerIdAndEpoch
     * 
     * @return 事务请求的结果
     */
    public synchronized TransactionalRequestResult initializeTransactions() {
        return initializeTransactions(ProducerIdAndEpoch.NONE);
    }

    /**
     * 使用指定的ProducerIdAndEpoch初始化事务
     * 
     * @param producerIdAndEpoch 生产者ID和Epoch信息
     * @return 事务请求的结果
     */
    synchronized TransactionalRequestResult initializeTransactions(ProducerIdAndEpoch producerIdAndEpoch) {
        // 检查是否有错误需要处理
        maybeFailWithError();

        // 判断是否是Epoch递增操作
        boolean isEpochBump = producerIdAndEpoch != ProducerIdAndEpoch.NONE;
        return handleCachedTransactionRequestResult(() -> {
            // 如果是Epoch递增,我们会在处理EndTxnRequest时进行状态转换
            if (!isEpochBump) {
                // 首次初始化,转换到INITIALIZING状态
                transitionTo(State.INITIALIZING);
                log.info("Invoking InitProducerId for the first time in order to acquire a producer ID");
            } else {
                // Epoch递增操作
                log.info("Invoking InitProducerId with current producer ID and epoch {} in order to bump the epoch", producerIdAndEpoch);
            }
            // 构建InitProducerId请求数据
            InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                    .setTransactionalId(transactionalId)
                    .setTransactionTimeoutMs(transactionTimeoutMs)
                    .setProducerId(producerIdAndEpoch.producerId)
                    .setProducerEpoch(producerIdAndEpoch.epoch);
            // 创建请求处理器并入队
            InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData),
                    isEpochBump);
            enqueueRequest(handler);
            return handler.result;
        }, State.INITIALIZING, "initTransactions");
    }

    /**
     * 开始一个新的事务
     * 该方法会检查事务状态并将状态转换为IN_TRANSACTION
     */
    public synchronized void beginTransaction() {
        // 确保是事务性生产者
        ensureTransactional();
        // 检查是否有待处理的状态转换
        throwIfPendingState("beginTransaction");
        // 检查是否有错误需要处理
        maybeFailWithError();
        // 转换到IN_TRANSACTION状态
        transitionTo(State.IN_TRANSACTION);
    }

    /**
     * 开始提交事务
     * 该方法会将状态转换为COMMITTING_TRANSACTION并开始提交流程
     * 
     * @return 事务提交请求的结果
     */
    public synchronized TransactionalRequestResult beginCommit() {
        return handleCachedTransactionRequestResult(() -> {
            // 检查是否有错误需要处理
            maybeFailWithError();
            // 转换到COMMITTING_TRANSACTION状态
            transitionTo(State.COMMITTING_TRANSACTION);
            // 开始完成事务(提交)
            return beginCompletingTransaction(TransactionResult.COMMIT);
        }, State.COMMITTING_TRANSACTION, "commitTransaction");
    }

    /**
     * 开始中止事务
     * 该方法会将状态转换为ABORTING_TRANSACTION并开始中止流程
     * 
     * @return 事务中止请求的结果
     */
    public synchronized TransactionalRequestResult beginAbort() {
        return handleCachedTransactionRequestResult(() -> {
            // 如果不是ABORTABLE_ERROR状态,检查是否有错误需要处理
            if (currentState != State.ABORTABLE_ERROR)
                maybeFailWithError();
            // 转换到ABORTING_TRANSACTION状态
            transitionTo(State.ABORTING_TRANSACTION);

            // 清空新加入的分区,因为我们要中止事务
            newPartitionsInTransaction.clear();
            // 开始完成事务(中止)
            return beginCompletingTransaction(TransactionResult.ABORT);
        }, State.ABORTING_TRANSACTION, "abortTransaction");
    }

    /**
     * 开始完成事务(提交或中止)
     * 
     * @param transactionResult 事务结果(COMMIT或ABORT)
     * @return 事务完成请求的结果
     */
    private TransactionalRequestResult beginCompletingTransaction(TransactionResult transactionResult) {
        // 如果有新加入的分区,先将它们添加到事务中
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        // 构建EndTxn请求
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(
            new EndTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(producerIdAndEpoch.producerId)
                .setProducerEpoch(producerIdAndEpoch.epoch)
                .setCommitted(transactionResult.id),
            isTransactionV2Enabled
        );

        // 在入队EndTxn请求之前更新事务版本,避免与请求完成时发生竞争
        // 由于此方法可能更新clientSideEpochBumpRequired,我们希望在下面的检查之前更新
        // 但也要在EndTxnRequest.Builder之后调用,这样我们就能用相同的版本完成事务
        maybeUpdateTransactionV2Enabled(false);

        // 创建EndTxn处理器并入队
        EndTxnHandler handler = new EndTxnHandler(builder);
        enqueueRequest(handler);

        // 如果需要为恢复递增epoch,在完成EndTxn请求后初始化事务
        // 如果我们要在下一个事务中升级到TV2事务,也要递增epoch
        if (clientSideEpochBumpRequired) {
            return initializeTransactions(this.producerIdAndEpoch);
        }

        return handler.result;
    }

    /**
     * 将消费者偏移量添加到当前事务中
     * 该方法用于实现消费者-生产者事务,允许在一个事务中同时提交消费偏移量和生产的消息
     *
     * @param offsets 要提交的主题分区偏移量映射
     * @param groupMetadata 消费者组元数据
     * @return 事务请求的结果
     */
    public synchronized TransactionalRequestResult sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                            final ConsumerGroupMetadata groupMetadata) {
        // 确保是事务性生产者
        ensureTransactional();
        // 检查是否有待处理的状态转换
        throwIfPendingState("sendOffsetsToTransaction");
        // 检查是否有错误需要处理
        maybeFailWithError();

        // 确保当前处于事务中
        if (currentState != State.IN_TRANSACTION) {
            throw new IllegalStateException("Cannot send offsets if a transaction is not in progress " +
                "(currentState= " + currentState + ")");
        }

        // 根据事务协议版本选择不同的处理方式
        TxnRequestHandler handler;
        if (isTransactionV2Enabled()) {
            // 事务V2协议下,直接发送事务偏移量提交请求,跳过AddOffsetsToTxn请求
            log.debug("Begin adding offsets {} for consumer group {} to transaction with transaction protocol V2", offsets, groupMetadata);
            handler = txnOffsetCommitHandler(null, offsets, groupMetadata);
            transactionStarted = true;
        } else {
            // 事务V1协议下,需要先发送AddOffsetsToTxn请求
            log.debug("Begin adding offsets {} for consumer group {} to transaction", offsets, groupMetadata);
            AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(
                    new AddOffsetsToTxnRequestData()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerIdAndEpoch.producerId)
                            .setProducerEpoch(producerIdAndEpoch.epoch)
                            .setGroupId(groupMetadata.groupId())
            );
            handler = new AddOffsetsToTxnHandler(builder, offsets, groupMetadata);
        }

        // 将请求加入队列并返回结果
        enqueueRequest(handler);
        return handler.result;
    }

    /**
     * 尝试将分区添加到当前事务中
     * 该方法在向新分区发送消息时被调用,用于跟踪事务涉及的所有分区
     *
     * @param topicPartition 要添加的主题分区
     */
    public synchronized void maybeAddPartition(TopicPartition topicPartition) {
        // 检查是否有错误需要处理
        maybeFailWithError();
        // 检查是否有待处理的状态转换
        throwIfPendingState("send");

        // 只有事务性生产者需要管理分区
        if (isTransactional()) {
            // 确保已初始化生产者ID
            if (!hasProducerId()) {
                throw new IllegalStateException("Cannot add partition " + topicPartition +
                    " to transaction before completing a call to initTransactions");
            // 确保当前处于事务中
            } else if (currentState != State.IN_TRANSACTION) {
                throw new IllegalStateException("Cannot add partition " + topicPartition +
                    " to transaction while in state  " + currentState);
            // 事务V2协议下的处理
            } else if (isTransactionV2Enabled()) {
                txnPartitionMap.getOrCreate(topicPartition);
                partitionsInTransaction.add(topicPartition);
                transactionStarted = true;
            // 如果分区已在事务中或正在添加中,则直接返回
            } else if (transactionContainsPartition(topicPartition) || isPartitionPendingAdd(topicPartition)) {
                return;
            // 事务V1协议下添加新分区
            } else {
                log.debug("Begin adding new partition {} to transaction", topicPartition);
                txnPartitionMap.getOrCreate(topicPartition);
                newPartitionsInTransaction.add(topicPartition);
            }
        }
    }

    /**
     * 获取最后一个错误
     * @return 最后发生的运行时异常
     */
    RuntimeException lastError() {
        return lastError;
    }

    /**
     * 检查是否允许向指定分区发送消息
     * 非事务性生产者可以向任何分区发送消息
     * 事务性生产者只能向已添加到事务中的分区发送消息
     *
     * @param tp 要检查的主题分区
     * @return 如果允许发送则返回true
     */
    synchronized boolean isSendToPartitionAllowed(TopicPartition tp) {
        if (hasFatalError())
            return false;
        return !isTransactional() || partitionsInTransaction.contains(tp);
    }

    /**
     * 获取事务ID
     * @return 事务ID
     */
    public String transactionalId() {
        return transactionalId;
    }

    /**
     * 检查是否已获取生产者ID
     * @return 如果生产者ID有效则返回true
     */
    public boolean hasProducerId() {
        return producerIdAndEpoch.isValid();
    }

    /**
     * 检查是否是事务性生产者
     * @return 如果是事务性生产者则返回true
     */
    public boolean isTransactional() {
        return transactionalId != null;
    }

    /**
     * 检查并更新事务V2功能的启用状态
     * 
     * 该方法主要完成以下工作:
     * 1. 从apiVersions获取已完成特性信息,检查事务V2是否启用
     * 2. 如果升级到V2版本,设置clientSideEpochBumpRequired标志
     * 
     * 为什么需要在升级到V2时递增epoch:
     * - V2版本不再显式添加分区
     * - 通过递增epoch可以避免升级过程中的一些边界情况
     * - 例如:当epoch被fence时,不会将前一个事务的分区视为已添加到新的V2事务中
     * 
     * @param onInitiatialization 是否在初始化时调用此方法
     */
    public synchronized void maybeUpdateTransactionV2Enabled(boolean onInitiatialization) {
        // 如果最新的特性epoch不大于当前已完成的特性epoch,则无需更新
        if (latestFinalizedFeaturesEpoch >= apiVersions.getMaxFinalizedFeaturesEpoch()) {
            return;
        }
        // 获取已完成的特性信息
        ApiVersions.FinalizedFeaturesInfo info = apiVersions.getFinalizedFeaturesInfo();
        latestFinalizedFeaturesEpoch = info.finalizedFeaturesEpoch;
        // 获取事务版本号
        Short transactionVersion = info.finalizedFeatures.get("transaction.version");
        // 记录更新前的V2状态
        boolean wasTransactionV2Enabled = isTransactionV2Enabled;
        // 如果版本号不为空且大于等于2,则启用V2
        isTransactionV2Enabled = transactionVersion != null && transactionVersion >= 2;
        log.debug("Updating isTV2 enabled to {} with FinalizedFeaturesEpoch {}", isTransactionV2Enabled, latestFinalizedFeaturesEpoch);
        // 如果不是初始化调用,且是首次启用V2,则需要递增epoch
        if (!onInitiatialization && !wasTransactionV2Enabled && isTransactionV2Enabled)
            clientSideEpochBumpRequired = true;
    }

    /**
     * 检查事务V2功能是否已启用
     * 
     * @return 如果事务V2功能已启用则返回true,否则返回false
     */
    public boolean isTransactionV2Enabled() {
        return isTransactionV2Enabled;
    }

    /**
     * 检查是否有待添加到事务中的分区
     * 包括新分区和正在添加但尚未确认的分区
     * 
     * @return 如果有待添加的分区则返回true,否则返回false
     */
    synchronized boolean hasPartitionsToAdd() {
        return !newPartitionsInTransaction.isEmpty() || !pendingPartitionsInTransaction.isEmpty();
    }

    /**
     * 检查事务是否正在完成中(提交或中止)
     * 
     * @return 如果事务正在提交或中止则返回true,否则返回false
     */
    synchronized boolean isCompleting() {
        return currentState == State.COMMITTING_TRANSACTION || currentState == State.ABORTING_TRANSACTION;
    }

    /**
     * 检查事务是否处于错误状态
     * 包括可中止错误和致命错误两种状态
     * 
     * @return 如果事务处于错误状态则返回true,否则返回false
     */
    synchronized boolean hasError() {
        return currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR;
    }

    /**
     * 检查事务是否正在中止
     * 
     * @return 如果事务正在中止则返回true,否则返回false
     */
    synchronized boolean isAborting() {
        return currentState == State.ABORTING_TRANSACTION;
    }

    /**
     * 将事务状态转换为可中止错误状态
     * 
     * 如果事务已经在中止中,则跳过状态转换
     * 否则,记录错误信息并执行状态转换
     * 
     * @param exception 导致错误状态的异常
     */
    synchronized void transitionToAbortableError(RuntimeException exception) {
        // 如果事务已经在中止中,则跳过状态转换
        if (currentState == State.ABORTING_TRANSACTION) {
            log.debug("Skipping transition to abortable error state since the transaction is already being " +
                    "aborted. Underlying exception: ", exception);
            return;
        }

        // 记录状态转换信息并执行转换
        log.info("Transiting to abortable error state due to {}", exception.toString());
        transitionTo(State.ABORTABLE_ERROR, exception);
    }

    /**
     * 将事务管理器转换到致命错误状态
     * 这是一个不可恢复的状态,一旦进入该状态:
     * 1. 生产者必须关闭
     * 2. 或者只能执行非事务性请求
     * 
     * @param exception 导致转换到致命错误状态的异常
     */
    synchronized void transitionToFatalError(RuntimeException exception) {
        // 记录转换到致命错误状态的日志,包含异常信息
        log.info("Transiting to fatal error state due to {}", exception.toString());
        // 执行状态转换,将当前状态设置为FATAL_ERROR
        transitionTo(State.FATAL_ERROR, exception);

        // 如果存在待处理的状态转换,使其失败
        if (pendingTransition != null) {
            pendingTransition.result.fail(exception);
        }
    }

    /**
     * 根据协调器的状态,将事务管理器转换到可中止错误状态或致命错误状态
     * 
     * 转换逻辑:
     * 1. 如果协调器可以处理可中止错误,则转换到ABORTABLE_ERROR状态
     * 2. 如果协调器无法处理可中止错误,则转换到FATAL_ERROR状态
     * 3. 在转换到可中止错误状态时,可能需要触发客户端epoch递增
     *
     * @param abortableException 可中止错误对应的异常
     * @param fatalException 致命错误对应的异常
     */
    private void transitionToAbortableErrorOrFatalError(
        RuntimeException abortableException,
        RuntimeException fatalException
    ) {
        // 检查协调器是否可以处理可中止错误
        if (canHandleAbortableError()) {
            // 如果需要从客户端触发epoch递增,设置标志
            if (needToTriggerEpochBumpFromClient())
                clientSideEpochBumpRequired = true;
            // 转换到可中止错误状态
            transitionToAbortableError(abortableException);
        } else {
            // 如果协调器无法处理可中止错误,转换到致命错误状态
            transitionToFatalError(fatalException);
        }
    }

    /**
     * 检查指定分区是否处于待添加状态
     * 分区在以下两种情况下被认为是待添加的:
     * 1. 分区在newPartitionsInTransaction集合中(新加入事务但尚未发送请求)
     * 2. 分区在pendingPartitionsInTransaction集合中(已发送AddPartitionsToTxn请求但尚未收到响应)
     * 
     * @param partition 要检查的主题分区
     * @return 如果分区处于待添加状态则返回true
     */
    // visible for testing
    synchronized boolean isPartitionPendingAdd(TopicPartition partition) {
        // 检查分区是否在新分区集合或待处理分区集合中
        return newPartitionsInTransaction.contains(partition) || pendingPartitionsInTransaction.contains(partition);
    }

    /**
     * 获取当前生产者ID和epoch信息,这是一个非阻塞操作
     * 调用者必须使用{@link ProducerIdAndEpoch#isValid()}方法验证返回结果是否有效
     * 
     * 生产者ID和epoch用于:
     * 1. 唯一标识生产者
     * 2. 实现幂等性发送
     * 3. 防止僵尸生产者
     *
     * @return 当前的ProducerIdAndEpoch对象
     */
    ProducerIdAndEpoch producerIdAndEpoch() {
        return producerIdAndEpoch;
    }

    /**
     * 在必要时更新指定分区的生产者ID和epoch信息
     * 主要在以下情况下进行更新:
     * 1. 当前不处于致命错误状态
     * 2. 分区的生产者ID/epoch已过期
     * 3. 分区没有在途的批次
     * 
     * @param topicPartition 要更新的主题分区
     */
    public synchronized void maybeUpdateProducerIdAndEpoch(TopicPartition topicPartition) {
        // 如果处于致命错误状态,忽略更新请求
        if (hasFatalError()) {
            log.debug("Ignoring producer ID and epoch update request since the producer is in fatal error state");
            return;
        }

        // 检查分区是否需要更新生产者ID/epoch
        // 条件: 1.当前ID/epoch已过期 2.没有在途批次
        if (hasStaleProducerIdAndEpoch(topicPartition) && !hasInflightBatches(topicPartition)) {
            // 重置分区序列号,使用新的epoch从0开始
            txnPartitionMap.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);
            // 记录更新日志
            log.debug("ProducerId of partition {} set to {} with epoch {}. Reinitialize sequence at beginning.",
                      topicPartition, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        }
    }

    /**
     * 原子地设置生产者ID和epoch
     * 这个操作是线程安全的,因为方法是在synchronized块中调用的
     * 
     * @param producerIdAndEpoch 新的生产者ID和epoch信息
     */
    private void setProducerIdAndEpoch(ProducerIdAndEpoch producerIdAndEpoch) {
        // 记录生产者ID和epoch更新的日志
        log.info("ProducerId set to {} with epoch {}", producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        // 原子地更新生产者ID和epoch
        this.producerIdAndEpoch = producerIdAndEpoch;
    }

    /**
     * 重置幂等生产者的ID和epoch
     * 该方法仅在生产者epoch耗尽时调用,此时我们需要重新获取生产者ID
     * 注意: 这个方法不能用于事务性生产者,事务性生产者必须通过中止事务或重新初始化来处理
     * 
     * 重置操作包括:
     * 1. 将生产者ID和epoch设置为NONE
     * 2. 将状态转换为UNINITIALIZED
     * 3. 这将触发一个新的InitProducerId请求
     */
    private void resetIdempotentProducerId() {
        // 如果是事务性生产者,抛出异常
        if (isTransactional())
            throw new IllegalStateException("Cannot reset producer state for a transactional producer. " +
                    "You must either abort the ongoing transaction or reinitialize the transactional producer instead");
        // 记录重置操作的日志
        log.debug("Resetting idempotent producer ID. ID and epoch before reset are {}", this.producerIdAndEpoch);
        // 重置生产者ID和epoch为NONE
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
        // 将状态转换为UNINITIALIZED
        transitionTo(State.UNINITIALIZED);
    }

    /**
     * 重置指定分区的序列号信息
     * 这包括:
     * 1. 从事务分区映射中移除该分区
     * 2. 从未解决序列号的分区集合中移除该分区
     * 
     * @param topicPartition 要重置的主题分区
     */
    private void resetSequenceForPartition(TopicPartition topicPartition) {
        // 从事务分区映射中移除分区
        txnPartitionMap.remove(topicPartition);
        // 从未解决序列号的分区集合中移除分区
        this.partitionsWithUnresolvedSequences.remove(topicPartition);
    }

    /**
     * 重置所有分区的序列号信息
     * 这个操作会:
     * 1. 重置事务分区映射中的所有序列号
     * 2. 清空所有未解决序列号的分区记录
     */
    private void resetSequenceNumbers() {
        // 重置事务分区映射
        txnPartitionMap.reset();
        // 清空未解决序列号的分区集合
        this.partitionsWithUnresolvedSequences.clear();
    }

    /**
     * 为非事务性的幂等生产者触发epoch递增
     * 这个方法用于处理序列号冲突等情况,通过递增epoch来解决
     * 
     * 操作包括:
     * 1. 标记需要客户端执行epoch递增
     * 2. 将分区添加到需要重写序列号的集合中
     * 
     * @param tp 需要触发epoch递增的主题分区
     */
    synchronized void requestIdempotentEpochBumpForPartition(TopicPartition tp) {
        // 设置标志,表示需要客户端执行epoch递增
        clientSideEpochBumpRequired = true;
        // 将分区添加到需要重写序列号的集合
        this.partitionsToRewriteSequences.add(tp);
    }

    /**
     * 递增幂等生产者的epoch值
     * 这个操作用于解决序列号冲突,确保消息的精确一次语义
     * 
     * 处理逻辑:
     * 1. 如果当前epoch已达到最大值,重置生产者ID
     * 2. 否则将epoch加1
     * 3. 重写所有需要更新序列号的分区的序列号
     * 4. 清理相关状态
     */
    private void bumpIdempotentProducerEpoch() {
        // 检查是否达到epoch最大值
        if (this.producerIdAndEpoch.epoch == Short.MAX_VALUE) {
            // 如果达到最大值,重置生产者ID
            resetIdempotentProducerId();
        } else {
            // 递增epoch值
            setProducerIdAndEpoch(new ProducerIdAndEpoch(this.producerIdAndEpoch.producerId, (short) (this.producerIdAndEpoch.epoch + 1)));
            // 记录epoch递增的日志
            log.debug("Incremented producer epoch, current producer ID and epoch are now {}", this.producerIdAndEpoch);
        }

        // 使用新的epoch重写需要更新的分区的序列号
        for (TopicPartition topicPartition : this.partitionsToRewriteSequences) {
            // 重置分区序列号从0开始
            this.txnPartitionMap.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);
            // 从未解决序列号的分区集合中移除该分区
            this.partitionsWithUnresolvedSequences.remove(topicPartition);
        }
        // 清空需要重写序列号的分区集合
        this.partitionsToRewriteSequences.clear();

        // 重置客户端epoch递增标志
        clientSideEpochBumpRequired = false;
    }

    /**
     * 根据需要递增幂等生产者的epoch并重置生产者ID
     * 此方法仅适用于非事务性生产者,用于处理以下场景:
     * 1. 当需要客户端侧epoch递增时
     * 2. 当生产者没有生产者ID时,需要初始化一个新的ID
     */
    synchronized void bumpIdempotentEpochAndResetIdIfNeeded() {
        // 只处理非事务性生产者
        if (!isTransactional()) {
            // 如果需要客户端侧epoch递增,执行递增操作
            if (clientSideEpochBumpRequired) {
                bumpIdempotentProducerEpoch();
            }
            // 如果当前不在初始化状态且没有生产者ID,则初始化一个新的ID
            if (currentState != State.INITIALIZING && !hasProducerId()) {
                // 转换到初始化状态
                transitionTo(State.INITIALIZING);
                // 创建初始化生产者ID的请求数据
                // 对于非事务性生产者,transactionalId为null
                InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                        .setTransactionalId(null)
                        .setTransactionTimeoutMs(Integer.MAX_VALUE);
                // 创建请求处理器并入队
                InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData), false);
                enqueueRequest(handler);
            }
        }
    }

    /**
     * 获取指定主题分区的下一个序列号
     * 用于确保消息的有序性和幂等性
     * 
     * @param topicPartition 主题分区
     * @return 下一个可用的序列号
     */
    synchronized int sequenceNumber(TopicPartition topicPartition) {
        return txnPartitionMap.getOrCreate(topicPartition).nextSequence();
    }

    /**
     * 获取指定主题分区当前的生产者ID和epoch信息
     * 
     * @param topicPartition 主题分区
     * @return 生产者ID和epoch信息
     */
    synchronized ProducerIdAndEpoch producerIdAndEpoch(TopicPartition topicPartition) {
        return txnPartitionMap.getOrCreate(topicPartition).producerIdAndEpoch();
    }

    /**
     * 递增指定主题分区的序列号
     * 
     * @param topicPartition 主题分区
     * @param increment 增量值
     */
    synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        txnPartitionMap.get(topicPartition).incrementSequence(increment);
    }

    /**
     * 添加一个正在传输中的消息批次
     * 用于跟踪消息批次的发送状态
     * 
     * @param batch 要添加的消息批次
     * @throws IllegalStateException 如果批次没有设置序列号
     */
    synchronized void addInFlightBatch(ProducerBatch batch) {
        if (!batch.hasSequence())
            throw new IllegalStateException("Can't track batch for partition " + batch.topicPartition + " when sequence is not set.");
        txnPartitionMap.get(batch.topicPartition).addInflightBatch(batch);
    }

    /**
     * 获取指定分区中第一个正在传输的序列号
     * 这是具有最小序列号的正在传输批次的基础序列号
     * 
     * @param topicPartition 主题分区
     * @return 如果事务管理器正在跟踪该分区的正在传输请求,则返回最小的正在传输序列号
     *         如果没有正在跟踪的请求,则返回RecordBatch.NO_SEQUENCE
     */
    synchronized int firstInFlightSequence(TopicPartition topicPartition) {
        // 如果没有正在传输的批次,返回NO_SEQUENCE
        if (!hasInflightBatches(topicPartition))
            return RecordBatch.NO_SEQUENCE;
        // 获取序列号最小的批次
        ProducerBatch batch = nextBatchBySequence(topicPartition);
        return batch == null ? RecordBatch.NO_SEQUENCE : batch.baseSequence();
    }

    /**
     * 获取指定分区中按序列号排序的下一个批次
     * 
     * @param topicPartition 主题分区
     * @return 下一个批次,如果没有则返回null
     */
    synchronized ProducerBatch nextBatchBySequence(TopicPartition topicPartition) {
        return txnPartitionMap.nextBatchBySequence(topicPartition);
    }

    /**
     * 移除一个正在传输的批次
     * 
     * @param batch 要移除的批次
     */
    synchronized void removeInFlightBatch(ProducerBatch batch) {
        if (hasInflightBatches(batch.topicPartition))
            txnPartitionMap.removeInFlightBatch(batch);
    }

    /**
     * 更新最后确认的序列号
     * 
     * @param topicPartition 主题分区
     * @param sequence 序列号
     * @return 更新后的序列号
     */
    private int maybeUpdateLastAckedSequence(TopicPartition topicPartition, int sequence) {
        return txnPartitionMap.maybeUpdateLastAckedSequence(topicPartition, sequence);
    }

    /**
     * 获取最后确认的序列号
     * 
     * @param topicPartition 主题分区
     * @return 最后确认的序列号,如果没有则返回空
     */
    synchronized OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        return txnPartitionMap.lastAckedSequence(topicPartition);
    }

    /**
     * 获取最后确认的偏移量
     * 
     * @param topicPartition 主题分区
     * @return 最后确认的偏移量,如果没有则返回空
     */
    synchronized OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        return txnPartitionMap.lastAckedOffset(topicPartition);
    }

    /**
     * 更新最后确认的偏移量
     * 
     * @param response 生产请求的分区响应
     * @param batch 生产者批次
     */
    private void updateLastAckedOffset(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        // 如果基础偏移量无效,直接返回
        if (response.baseOffset == ProduceResponse.INVALID_OFFSET)
            return;
        // 计算批次中最后一条消息的偏移量
        long lastOffset = response.baseOffset + batch.recordCount - 1;
        // 更新分区映射中的最后确认偏移量
        txnPartitionMap.updateLastAckedOffset(batch.topicPartition, isTransactional(), lastOffset);
    }

    /**
     * 处理已完成的批次
     * 当一个批次成功发送到broker并收到响应时调用此方法
     * 
     * @param batch 已完成的生产者批次
     * @param response broker的响应
     */
    public synchronized void handleCompletedBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        // 更新最后确认的序列号并获取更新后的值
        int lastAckedSequence = maybeUpdateLastAckedSequence(batch.topicPartition, batch.lastSequence());
        // 记录调试日志
        log.debug("ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
                batch.producerId(),
                batch.topicPartition,
                lastAckedSequence);

        // 更新最后确认的偏移量
        updateLastAckedOffset(response, batch);
        // 从在途批次中移除该批次
        removeInFlightBatch(batch);
    }

    /**
     * 将事务管理器转换到未初始化状态
     * 通常在需要重新初始化事务管理器时调用此方法
     * 
     * @param exception 导致状态转换的异常
     */
    public synchronized void transitionToUninitialized(RuntimeException exception) {
        // 转换到未初始化状态
        transitionTo(State.UNINITIALIZED);
        // 如果有待处理的状态转换,使其失败
        if (pendingTransition != null) {
            pendingTransition.result.fail(exception);
        }
        // 清除最后的错误
        lastError = null;
    }

    /**
     * 根据异常类型可能将事务管理器转换到错误状态
     * 分为两种错误状态:
     * 1. 致命错误 - 不可恢复,需要关闭生产者
     * 2. 可中止错误 - 可以通过中止事务恢复
     * 
     * @param exception 触发错误状态转换的异常
     */
    public synchronized void maybeTransitionToErrorState(RuntimeException exception) {
        // 检查是否是致命错误
        if (exception instanceof ClusterAuthorizationException
                || exception instanceof TransactionalIdAuthorizationException
                || exception instanceof ProducerFencedException
                || exception instanceof UnsupportedVersionException
                || exception instanceof InvalidPidMappingException) {
            // 转换到致命错误状态
            transitionToFatalError(exception);
        } else if (isTransactional()) { // 如果是事务性生产者
            // 检查是否需要在客户端触发Epoch递增
            if (needToTriggerEpochBumpFromClient() && !isCompleting()) {
                clientSideEpochBumpRequired = true;
            }
            // 转换到可中止错误状态
            transitionToAbortableError(exception);
        }
    }

    /**
     * 处理发送失败的批次
     * 根据失败原因采取不同的恢复策略:
     * 1. 序列号乱序 - 递增epoch重置序列号
     * 2. 未知生产者ID - 重置序列号为0
     * 3. 其他错误 - 根据是否事务性决定处理方式
     * 
     * @param batch 失败的生产者批次
     * @param exception 失败原因
     * @param adjustSequenceNumbers 是否需要调整序列号
     */
    synchronized void handleFailedBatch(ProducerBatch batch, RuntimeException exception, boolean adjustSequenceNumbers) {
        // 根据异常可能转换到错误状态
        maybeTransitionToErrorState(exception);
        // 从在途批次中移除失败的批次
        removeInFlightBatch(batch);

        // 如果已经处于致命错误状态,忽略该批次
        if (hasFatalError()) {
            log.debug("Ignoring batch {} with producer id {}, epoch {}, and sequence number {} " +
                            "since the producer is already in fatal error state", batch, batch.producerId(),
                    batch.producerEpoch(), batch.baseSequence(), exception);
            return;
        }

        // 处理序列号乱序异常
        if (exception instanceof OutOfOrderSequenceException && !isTransactional()) {
            log.error("The broker returned {} for topic-partition {} with producerId {}, epoch {}, and sequence number {}",
                    exception, batch.topicPartition, batch.producerId(), batch.producerEpoch(), batch.baseSequence());

            // 序列号乱序表示日志中有空隙,递增该分区的epoch来重置序列号为0
            requestIdempotentEpochBumpForPartition(batch.topicPartition);
        } 
        // 处理未知生产者ID异常
        else if (exception instanceof UnknownProducerIdException) {
            // broker没有该生产者的状态,将接受序列号为0的写入
            // 重置该分区的序列号,使生产者可以在中止事务后继续
            // 所有到该分区的在途请求也会因UnknownProducerId错误而失败
            // 如果broker支持递增epoch,稍后会在调用InitProducerId后重置所有序列号
            resetSequenceForPartition(batch.topicPartition);
        } 
        // 处理其他异常
        else {
            if (adjustSequenceNumbers) {
                if (!isTransactional()) {
                    // 非事务性生产者递增epoch
                    requestIdempotentEpochBumpForPartition(batch.topicPartition);
                } else {
                    // 事务性生产者调整序列号
                    txnPartitionMap.adjustSequencesDueToFailedBatch(batch);
                }
            }
        }
    }

    /**
     * 检查指定分区是否有在途批次
     * 
     * @param topicPartition 主题分区
     * @return 如果有在途批次返回true,否则返回false
     */
    synchronized boolean hasInflightBatches(TopicPartition topicPartition) {
        return txnPartitionMap.getOrCreate(topicPartition).hasInflightBatches();
    }

    /**
     * 检查指定分区是否有过期的生产者ID和Epoch
     * 
     * @param topicPartition 主题分区
     * @return 如果分区的生产者ID和Epoch与当前不匹配返回true,否则返回false
     */
    synchronized boolean hasStaleProducerIdAndEpoch(TopicPartition topicPartition) {
        return !producerIdAndEpoch.equals(txnPartitionMap.getOrCreate(topicPartition).producerIdAndEpoch());
    }

    /**
     * 检查是否有未解决的序列号
     * 
     * @return 如果有分区的序列号未解决返回true,否则返回false
     */
    synchronized boolean hasUnresolvedSequences() {
        return !partitionsWithUnresolvedSequences.isEmpty();
    }

    /**
     * 检查指定分区是否有未解决的序列号
     * 
     * @param topicPartition 主题分区
     * @return 如果该分区有未解决的序列号返回true,否则返回false
     */
    synchronized boolean hasUnresolvedSequence(TopicPartition topicPartition) {
        return partitionsWithUnresolvedSequences.containsKey(topicPartition);
    }

    /**
     * 标记批次的序列号为未解决状态
     * 当批次在本地过期但可能已发送到broker时调用此方法
     * 
     * @param batch 需要标记的生产者批次
     */
    synchronized void markSequenceUnresolved(ProducerBatch batch) {
        // 计算下一个序列号
        int nextSequence = batch.lastSequence() + 1;
        // 更新分区的未解决序列号,取当前值和新值的较大者
        partitionsWithUnresolvedSequences.compute(batch.topicPartition,
            (k, v) -> v == null ? nextSequence : Math.max(v, nextSequence));
        // 记录调试日志
        log.debug("Marking partition {} unresolved with next sequence number {}", batch.topicPartition,
                partitionsWithUnresolvedSequences.get(batch.topicPartition));
    }

    /**
     * 尝试解决未解决的序列号问题。如果所有在途请求都已完成但仍有分区未解决，
     * 要么在可能的情况下递增epoch，要么转换到致命错误状态。
     * 
     * 主要处理逻辑：
     * 1. 遍历所有具有未解决序列号的分区
     * 2. 检查分区是否还有在途批次
     * 3. 如果分区已完全排空，检查序列号状态并进行相应处理
     * 4. 根据生产者类型(事务性/幂等性)采取不同的恢复策略
     */
    synchronized void maybeResolveSequences() {
        // 遍历所有具有未解决序列号的分区
        for (Iterator<TopicPartition> iter = partitionsWithUnresolvedSequences.keySet().iterator(); iter.hasNext(); ) {
            TopicPartition topicPartition = iter.next();
            // 检查该分区是否还有在途批次
            if (!hasInflightBatches(topicPartition)) {
                // 分区已完全排空。此时，最后确认的序列号应该比下一个要发送到该分区的序列号小1
                // 如果是这种情况，则分区完全解决。否则，我们需要在必要时重置序列号
                if (isNextSequence(topicPartition, sequenceNumber(topicPartition))) {
                    // 当一个批次过期，但后续批次成功的情况下会发生这种情况
                    iter.remove();
                } else {
                    // 如果生产者中的所有在途批次最终都过期了，我们会进入这个分支
                    if (isTransactional()) {
                        // 对于事务性生产者，如果可能的话递增epoch，否则转换到致命错误状态
                        String unackedMessagesErr = "The client hasn't received acknowledgment for some previously " +
                                "sent messages and can no longer retry them. ";
                        // 创建可中止异常 - 表示可以安全地中止事务并继续
                        KafkaException abortableException = new KafkaException(unackedMessagesErr + "It is safe to abort " +
                                "the transaction and continue.");
                        // 创建致命异常 - 表示继续执行是不安全的
                        KafkaException fatalException = new KafkaException(unackedMessagesErr + "It isn't safe to continue.");

                        // 转换到可中止错误状态或致命错误状态
                        transitionToAbortableErrorOrFatalError(abortableException, fatalException);
                    } else {
                        // 对于幂等性生产者，递增epoch
                        log.info("No inflight batches remaining for {}, last ack'd sequence for partition is {}, next sequence is {}. " +
                                        "Going to bump epoch and reset sequence numbers.", topicPartition,
                                lastAckedSequence(topicPartition).orElse(TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER), sequenceNumber(topicPartition));
                        // 请求为该分区递增幂等性epoch
                        requestIdempotentEpochBumpForPartition(topicPartition);
                    }

                    // 从未解决序列号集合中移除该分区
                    iter.remove();
                }
            }
        }
    }

    /**
     * 检查给定的序列号是否是分区的下一个预期序列号
     * 
     * @param topicPartition 主题分区
     * @param sequence 要检查的序列号
     * @return 如果序列号正好比最后确认的序列号大1，则返回true
     */
    private boolean isNextSequence(TopicPartition topicPartition, int sequence) {
        return sequence - lastAckedSequence(topicPartition).orElse(TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER) == 1;
    }

    /**
     * 检查给定的序列号是否是未解决分区的下一个预期序列号
     * 
     * @param topicPartition 主题分区
     * @param sequence 要检查的序列号
     * @return 如果分区有未解决的序列号且给定序列号匹配，则返回true
     */
    private boolean isNextSequenceForUnresolvedPartition(TopicPartition topicPartition, int sequence) {
        return this.hasUnresolvedSequence(topicPartition) &&
                sequence == this.partitionsWithUnresolvedSequences.get(topicPartition);
    }

    /**
     * 获取下一个要发送的事务请求
     * 
     * @param hasIncompleteBatches 是否存在未完成的消息批次
     * @return 下一个要发送的事务请求处理器,如果没有则返回null
     */
    synchronized TxnRequestHandler nextRequest(boolean hasIncompleteBatches) {
        // 如果有新的分区需要加入事务,创建并入队AddPartitionsToTxn请求
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        // 获取但不移除队列头部的请求处理器
        TxnRequestHandler nextRequestHandler = pendingRequests.peek();
        if (nextRequestHandler == null)
            return null;

        // 如果是EndTxn请求且还有未完成的批次,则不发送该请求
        // 这确保了所有消息都已发送完成后才结束事务
        if (nextRequestHandler.isEndTxn() && hasIncompleteBatches)
            return null;

        // 从队列中移除该请求
        pendingRequests.poll();
        // 检查是否因错误状态需要终止该请求
        if (maybeTerminateRequestWithError(nextRequestHandler)) {
            log.trace("Not sending transactional request {} because we are in an error state",
                    nextRequestHandler.requestBuilder());
            return null;
        }

        // 处理EndTxn请求的特殊情况
        // 如果事务尚未开始(没有任何操作),则不需要发送EndTxn请求
        if (nextRequestHandler.isEndTxn() && !transactionStarted) {
            nextRequestHandler.result.done();
            if (currentState != State.FATAL_ERROR) {
                if (isTransactionV2Enabled) {
                    log.debug("Not sending EndTxn for completed transaction since no send " +
                            "or sendOffsetsToTransaction were triggered");
                } else {
                    log.debug("Not sending EndTxn for completed transaction since no partitions " +
                            "or offsets were successfully added");
                }
                // 完成事务,清理相关状态
                completeTransaction();
            }
            // 获取下一个请求
            nextRequestHandler = pendingRequests.poll();
        }

        // 如果有请求要发送,记录日志
        if (nextRequestHandler != null)
            log.trace("Request {} dequeued for sending", nextRequestHandler.requestBuilder());

        return nextRequestHandler;
    }

    /**
     * 重试发送事务请求
     * 
     * @param request 需要重试的事务请求处理器
     */
    synchronized void retry(TxnRequestHandler request) {
        // 设置请求为重试状态
        request.setRetry();
        // 将请求重新加入队列
        enqueueRequest(request);
    }

    /**
     * 处理认证失败的情况
     * 当认证失败时,所有待处理的请求都会失败
     * 
     * @param e 认证异常
     */
    synchronized void authenticationFailed(AuthenticationException e) {
        // 遍历所有待处理请求,将其标记为致命错误
        for (TxnRequestHandler request : pendingRequests)
            request.fatalError(e);
    }

    /**
     * 使所有待处理的请求失败
     * 用于处理可中止的错误情况
     * 
     * @param exception 导致失败的运行时异常
     */
    synchronized void failPendingRequests(RuntimeException exception) {
        // 将所有待处理请求标记为可中止错误
        pendingRequests.forEach(handler ->
                handler.abortableError(exception));
    }

    /**
     * 关闭事务管理器
     * 会强制关闭所有待处理的请求
     */
    synchronized void close() {
        // 创建强制关闭异常
        KafkaException shutdownException = new KafkaException("The producer closed forcefully");
        // 将所有待处理请求标记为致命错误
        pendingRequests.forEach(handler ->
                handler.fatalError(shutdownException));
        // 如果有待处理的状态转换,也将其标记为失败
        if (pendingTransition != null) {
            pendingTransition.result.fail(shutdownException);
        }
    }

    /**
     * 根据协调器类型获取对应的协调器节点
     * 
     * @param type 协调器类型(GROUP或TRANSACTION)
     * @return 对应的协调器节点
     */
    Node coordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                return consumerGroupCoordinator;
            case TRANSACTION:
                return transactionCoordinator;
            default:
                throw new IllegalStateException("Received an invalid coordinator type: " + type);
        }
    }

    /**
     * 查找请求对应的协调器
     * 
     * @param request 事务请求处理器
     */
    void lookupCoordinator(TxnRequestHandler request) {
        lookupCoordinator(request.coordinatorType(), request.coordinatorKey());
    }

    /**
     * 设置当前正在处理的请求的关联ID
     * 
     * @param correlationId 请求关联ID
     */
    void setInFlightCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    /**
     * 清除当前正在处理的请求的关联ID
     * 将其重置为NO_INFLIGHT_REQUEST_CORRELATION_ID
     */
    private void clearInFlightCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    /**
     * 检查是否有正在处理的请求
     * 
     * @return 如果有正在处理的请求返回true,否则返回false
     */
    boolean hasInFlightRequest() {
        return inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    /**
     * 检查事务管理器是否处于致命错误状态
     * 该状态表示事务管理器已经不可恢复,需要关闭生产者或只能执行非事务性请求
     * 
     * @return 如果处于致命错误状态返回true,否则返回false
     */
    // visible for testing.
    boolean hasFatalError() {
        return currentState == State.FATAL_ERROR;
    }

    /**
     * 检查事务管理器是否处于可中止错误状态
     * 该状态表示当前事务出现了错误,但可以通过中止事务来恢复
     * 
     * @return 如果处于可中止错误状态返回true,否则返回false
     */
    // visible for testing.
    boolean hasAbortableError() {
        return currentState == State.ABORTABLE_ERROR;
    }

    /**
     * 检查指定的主题分区是否在当前事务中
     * 
     * @param topicPartition 要检查的主题分区
     * @return 如果分区在当前事务中返回true,否则返回false
     */
    // visible for testing
    public synchronized boolean transactionContainsPartition(TopicPartition topicPartition) {
        return partitionsInTransaction.contains(topicPartition);
    }

    /**
     * 检查是否有待提交的事务偏移量
     * 
     * @return 如果有待提交的偏移量返回true,否则返回false
     */
    // visible for testing
    synchronized boolean hasPendingOffsetCommits() {
        return !pendingTxnOffsetCommits.isEmpty();
    }

    /**
     * 检查是否有待处理的事务请求
     * 
     * @return 如果有待处理的请求返回true,否则返回false
     */
    synchronized boolean hasPendingRequests() {
        return !pendingRequests.isEmpty();
    }

    /**
     * 检查是否有正在进行的事务
     * 当事务处于以下状态之一时被认为是正在进行中:
     * 1. IN_TRANSACTION - 事务正在执行
     * 2. COMMITTING_TRANSACTION/ABORTING_TRANSACTION - 事务正在完成
     * 3. ABORTABLE_ERROR - 事务出现了可恢复的错误
     * 
     * @return 如果有正在进行的事务返回true,否则返回false
     */
    // visible for testing
    synchronized boolean hasOngoingTransaction() {
        // transactions are considered ongoing once started until completion or a fatal error
        return currentState == State.IN_TRANSACTION || isCompleting() || hasAbortableError();
    }

    /**
     * 判断生产请求是否可以重试
     * 主要处理两类错误:
     * 1. UNKNOWN_PRODUCER_ID - 表示broker上丢失了生产者状态
     * 2. OUT_OF_ORDER_SEQUENCE_NUMBER - 表示消息序列号不连续
     * 
     * @param response Broker的响应
     * @param batch 发送的消息批次
     * @return 如果可以重试返回true,否则返回false
     */
    synchronized boolean canRetry(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        Errors error = response.error;

        // UNKNOWN_PRODUCER_ID错误表示broker上丢失了生产者状态
        // 根据日志起始偏移量的不同情况,我们可能需要重试,具体如下:
        // 1. 如果没有这些情况,对于幂等生产者,我们会在本地递增epoch并重置在途批次的序列号从0开始
        // 2. 对于事务性生产者,允许批次失败,在处理失败的批次时,我们会转换到可中止错误状态
        // 并设置一个标志表明我们需要递增epoch(如果broker支持)
        if (error == Errors.UNKNOWN_PRODUCER_ID) {
            if (response.logStartOffset == -1) {
                // 我们不知道日志的起始偏移量,应该继续重试请求直到获取到它
                // UNKNOWN_PRODUCER_ID错误码是与包含logStartOffset的新ProduceResponse一起添加的
                // 所以'-1'标记不是为了向后兼容。相反,broker在返回响应时可能不知道logStartOffset
                // 因为从最初引发错误到构造响应期间,分区可能已经从broker移走
                // 在这些情况下,我们应该重试请求:一旦情况稳定下来,我们一定能获得logStartOffset
                return true;
            }

            if (batch.sequenceHasBeenReset()) {
                // 当第一个在途批次因截断而失败时,所有其他在途批次的序列号都会从头开始重置
                // 但是,当这些响应从broker返回时,它们也会带有UNKNOWN_PRODUCER_ID错误
                // 在这种情况下,我们不应该将序列号重置到开始
                return true;
            } else if (lastAckedOffset(batch.topicPartition).orElse(TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER) < response.logStartOffset) {
                // 日志的头部已被删除,可能是因为保留时间已过
                // 在这种情况下,我们预计会丢失生产者状态
                // 对于事务性生产者,重置所有在途批次的序列号并重试它们,这样事务就不需要中止
                // 对于幂等性生产者,递增epoch以避免重用(序列号,epoch)对
                if (isTransactional()) {
                    txnPartitionMap.startSequencesAtBeginning(batch.topicPartition, this.producerIdAndEpoch);
                } else {
                    requestIdempotentEpochBumpForPartition(batch.topicPartition);
                }
                return true;
            }

            if (!isTransactional()) {
                // 对于幂等性生产者,始终重试UNKNOWN_PRODUCER_ID错误
                // 如果批次具有当前的producer ID和epoch,请求递增epoch
                // 否则只需重试生产请求
                requestIdempotentEpochBumpForPartition(batch.topicPartition);
                return true;
            }
        } else if (error == Errors.OUT_OF_ORDER_SEQUENCE_NUMBER) {
            if (!hasUnresolvedSequence(batch.topicPartition) &&
                    (batch.sequenceHasBeenReset() || !isNextSequence(batch.topicPartition, batch.baseSequence()))) {
                // 如果批次不是下一个批次(即其基础序列号不是lastAckedSequence + 1)
                // 我们应该重试OutOfOrderSequenceException
                return true;
            } else if (!isTransactional()) {
                // 对于幂等性生产者,重试所有OUT_OF_ORDER_SEQUENCE_NUMBER错误
                // 如果没有未解决的序列,或者这个批次紧跟在未解决的序列之后
                // 我们知道序列中确实存在间隙,因此递增epoch
                // 否则,不递增epoch重试,等待看序列是否能解决
                if (!hasUnresolvedSequence(batch.topicPartition) ||
                        isNextSequenceForUnresolvedPartition(batch.topicPartition, batch.baseSequence())) {
                    requestIdempotentEpochBumpForPartition(batch.topicPartition);
                }
                return true;
            }
        }

        // 如果以上情况都不符合,则检查异常是否可重试
        return error.exception() instanceof RetriableException;
    }

    /**
     * 检查事务管理器是否处于就绪状态
     * 只有当生产者是事务性的且当前状态为READY时才返回true
     * 
     * @return 如果事务管理器处于就绪状态则返回true
     */
    // visible for testing
    synchronized boolean isReady() {
        // 检查是否是事务性生产者且状态为READY
        return isTransactional() && currentState == State.READY;
    }

    /**
     * 检查事务管理器是否处于初始化状态
     * 只有当生产者是事务性的且当前状态为INITIALIZING时才返回true
     * 
     * @return 如果事务管理器处于初始化状态则返回true
     */
    // visible for testing
    synchronized boolean isInitializing() {
        // 检查是否是事务性生产者且状态为INITIALIZING
        return isTransactional() && currentState == State.INITIALIZING;
    }

    /**
     * 处理事务协调器就绪事件
     * 当事务协调器准备就绪时调用此方法,用于检查协调器是否支持递增Producer Epoch
     */
    void handleCoordinatorReady() {
        // 获取事务协调器的API版本信息
        NodeApiVersions nodeApiVersions = transactionCoordinator != null ?
                apiVersions.get(transactionCoordinator.idString()) :
                null;
        // 获取InitProducerId请求的API版本
        ApiVersion initProducerIdVersion = nodeApiVersions != null ?
                nodeApiVersions.apiVersion(ApiKeys.INIT_PRODUCER_ID) :
                null;
        // 检查是否支持递增Producer Epoch(需要API版本>=3)
        this.coordinatorSupportsBumpingEpoch = initProducerIdVersion != null &&
                initProducerIdVersion.maxVersion() >= 3;
    }

    /**
     * 将事务管理器转换到目标状态
     * 这是无错误版本的状态转换方法
     * 
     * @param target 目标状态
     */
    private void transitionTo(State target) {
        // 调用带错误参数的状态转换方法,错误参数为null
        transitionTo(target, null);
    }

    /**
     * 将事务管理器转换到目标状态
     * 如果状态转换无效,根据配置可能会使事务管理器进入致命错误状态
     * 
     * @param target 目标状态
     * @param error 导致状态转换的错误(如果有)
     * @throws IllegalStateException 如果状态转换无效
     * @throws IllegalArgumentException 如果转换到错误状态时没有提供错误信息
     */
    private void transitionTo(State target, RuntimeException error) {
        // 检查状态转换是否有效
        if (!currentState.isTransitionValid(currentState, target)) {
            // 构建错误消息
            String idString = transactionalId == null ?  "" : "TransactionalId " + transactionalId + ": ";
            String message = idString + "Invalid transition attempted from state "
                    + currentState.name() + " to state " + target.name();

            // 如果配置了在无效转换时毒化状态,则进入FATAL_ERROR状态
            if (shouldPoisonStateOnInvalidTransition.get()) {
                currentState = State.FATAL_ERROR;
                lastError = new IllegalStateException(message);
                throw lastError;
            } else {
                // 否则仅抛出异常
                throw new IllegalStateException(message);
            }
        } else if (target == State.FATAL_ERROR || target == State.ABORTABLE_ERROR) {
            // 转换到错误状态时必须提供错误信息
            if (error == null)
                throw new IllegalArgumentException("Cannot transition to " + target + " with a null exception");
            lastError = error;
        } else {
            // 正常状态转换时清除上一个错误
            lastError = null;
        }

        // 记录状态转换日志
        if (lastError != null)
            log.debug("Transition from state {} to error state {}", currentState, target, lastError);
        else
            log.debug("Transition from state {} to {}", currentState, target);

        // 更新当前状态
        currentState = target;
    }

    /**
     * 确保当前生产者是事务性的
     * 如果不是事务性生产者则抛出异常
     * 
     * @throws IllegalStateException 如果在非事务性生产者上调用事务方法
     */
    private void ensureTransactional() {
        if (!isTransactional())
            throw new IllegalStateException("Transactional method invoked on a non-transactional producer.");
    }

    /**
     * 检查是否有错误需要处理,如果有则抛出相应的异常
     * 不同类型的错误会抛出不同的异常类型
     * 
     * @throws ProducerFencedException 如果生产者被围栏
     * @throws InvalidProducerEpochException 如果生产者Epoch无效
     * @throws IllegalStateException 如果发生无效的状态转换
     * @throws KafkaException 其他类型的错误
     */
    private void maybeFailWithError() {
        // 如果没有错误则直接返回
        if (!hasError()) {
            return;
        }
        // 对于ProducerFencedException,不包装为KafkaException
        // 而是创建一个新的实例,因为这个异常不是由当前调用引起的
        if (lastError instanceof ProducerFencedException) {
            throw new ProducerFencedException("Producer with transactionalId '" + transactionalId
                    + "' and " + producerIdAndEpoch + " has been fenced by another producer " +
                    "with the same transactionalId");
        }
        // 处理无效的Producer Epoch异常
        if (lastError instanceof InvalidProducerEpochException) {
            throw new InvalidProducerEpochException("Producer with transactionalId '" + transactionalId
                    + "' and " + producerIdAndEpoch + " attempted to produce with an old epoch");
        }
        // 处理无效状态转换异常
        if (lastError instanceof IllegalStateException) {
            throw new IllegalStateException("Producer with transactionalId '" + transactionalId
                    + "' and " + producerIdAndEpoch + " cannot execute transactional method because of previous invalid state transition attempt", lastError);
        }
        // 其他类型的错误包装为KafkaException
        throw new KafkaException("Cannot execute transactional method because we are in an error state", lastError);
    }

    /**
     * 检查是否需要因错误终止请求
     * 
     * @param requestHandler 要检查的事务请求处理器
     * @return 如果请求应该被终止则返回true,否则返回false
     */
    private boolean maybeTerminateRequestWithError(TxnRequestHandler requestHandler) {
        // 检查是否存在错误
        if (hasError()) {
            // 如果是可中止错误且请求是查找协调器请求,则允许请求继续
            if (hasAbortableError() && requestHandler instanceof FindCoordinatorHandler)
                // 如果我们期望中止事务,让FindCoordinator请求继续执行不会有害
                return false;

            // 使用最后一个错误使请求失败
            requestHandler.fail(lastError);
            return true;
        }
        return false;
    }

    /**
     * 将事务请求加入待处理队列
     * 
     * @param requestHandler 要入队的事务请求处理器
     */
    private void enqueueRequest(TxnRequestHandler requestHandler) {
        // 记录调试日志
        log.debug("Enqueuing transactional request {}", requestHandler.requestBuilder());
        // 将请求添加到优先级队列
        pendingRequests.add(requestHandler);
    }

    /**
     * 查找指定类型的协调器
     * 
     * @param type 协调器类型(GROUP或TRANSACTION)
     * @param coordinatorKey 协调器键(消费者组ID或事务ID)
     */
    private void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
        // 根据协调器类型重置相应的协调器引用
        switch (type) {
            case GROUP:
                // 重置消费者组协调器
                consumerGroupCoordinator = null;
                break;
            case TRANSACTION:
                // 重置事务协调器
                transactionCoordinator = null;
                break;
            default:
                throw new IllegalStateException("Invalid coordinator type: " + type);
        }

        // 构建查找协调器请求数据
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(type.id())
                .setKey(coordinatorKey);
        // 创建请求构建器
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(data);
        // 将查找协调器请求入队
        enqueueRequest(new FindCoordinatorHandler(builder));
    }

    /**
     * 创建向事务添加分区的请求处理器
     * 
     * @return 添加分区到事务的请求处理器
     */
    private TxnRequestHandler addPartitionsToTransactionHandler() {
        // 将新分区添加到待处理分区集合
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
        // 清空新分区集合
        newPartitionsInTransaction.clear();
        // 构建添加分区到事务的请求
        AddPartitionsToTxnRequest.Builder builder =
            AddPartitionsToTxnRequest.Builder.forClient(transactionalId,
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
                new ArrayList<>(pendingPartitionsInTransaction));
        // 返回请求处理器
        return new AddPartitionsToTxnHandler(builder);
    }

    /**
     * 创建事务偏移量提交请求处理器
     * 
     * @param result 事务请求结果
     * @param offsets 要提交的偏移量映射
     * @param groupMetadata 消费者组元数据
     * @return 事务偏移量提交请求处理器
     */
    private TxnOffsetCommitHandler txnOffsetCommitHandler(TransactionalRequestResult result,
                                                          Map<TopicPartition, OffsetAndMetadata> offsets,
                                                          ConsumerGroupMetadata groupMetadata) {
        // 遍历要提交的偏移量
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            // 获取偏移量元数据
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            // 创建已提交偏移量对象
            CommittedOffset committedOffset = new CommittedOffset(offsetAndMetadata.offset(),
                    offsetAndMetadata.metadata(), offsetAndMetadata.leaderEpoch());
            // 将偏移量添加到待提交集合
            pendingTxnOffsetCommits.put(entry.getKey(), committedOffset);
        }

        // 构建事务偏移量提交请求
        final TxnOffsetCommitRequest.Builder builder =
            new TxnOffsetCommitRequest.Builder(transactionalId,
                groupMetadata.groupId(),
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
                pendingTxnOffsetCommits,
                groupMetadata.memberId(),
                groupMetadata.generationId(),
                groupMetadata.groupInstanceId(),
                isTransactionV2Enabled()
            );
        // 根据是否使用事务V2创建相应的处理器
        if (result == null) {
            // 如果使用事务V2
            return new TxnOffsetCommitHandler(builder);
        }
        return new TxnOffsetCommitHandler(result, builder);
    }

    /**
     * 检查是否有待处理的状态转换,如果有且未完成则抛出异常
     * 
     * @param operation 当前尝试执行的操作名称
     * @throws IllegalStateException 如果存在未完成的状态转换
     */
    private void throwIfPendingState(String operation) {
        // 检查是否有待处理的状态转换
        if (pendingTransition != null) {
            // 如果状态转换已确认,清除它
            if (pendingTransition.result.isAcked()) {
                pendingTransition = null;
            } else {
                // 如果状态转换未完成,抛出异常
                throw new IllegalStateException("Cannot attempt operation `" + operation + "` "
                    + "because the previous call to `" + pendingTransition.operation + "` "
                    + "timed out and must be retried");
            }
        }
    }

    /**
     * 处理缓存的事务请求结果
     * 
     * @param transactionalRequestResultSupplier 事务请求结果提供者
     * @param nextState 期望的下一个状态
     * @param operation 当前操作名称
     * @return 事务请求结果
     */
    private TransactionalRequestResult handleCachedTransactionRequestResult(
        Supplier<TransactionalRequestResult> transactionalRequestResultSupplier,
        State nextState,
        String operation
    ) {
        // 确保是事务性生产者
        ensureTransactional();

        // 检查是否有待处理的状态转换
        if (pendingTransition != null) {
            // 如果状态转换已确认,清除它
            if (pendingTransition.result.isAcked()) {
                pendingTransition = null;
            } else if (nextState != pendingTransition.state) {
                // 如果期望的下一个状态与待处理状态不匹配,抛出异常
                throw new IllegalStateException("Cannot attempt operation `" + operation + "` "
                    + "because the previous call to `" + pendingTransition.operation + "` "
                    + "timed out and must be retried");
            } else {
                // 如果状态匹配,返回待处理的结果
                return pendingTransition.result;
            }
        }

        // 获取新的事务请求结果
        TransactionalRequestResult result = transactionalRequestResultSupplier.get();
        // 创建新的待处理状态转换
        pendingTransition = new PendingStateTransition(result, nextState, operation);
        return result;
    }

    /**
     * 根据API版本判断是否可以手动触发epoch递增
     *
     * <b>注意:</b>
     * 此方法仅适用于事务型生产者。
     * 对于非事务型生产者，始终允许epoch递增。
     *
     * <ol>
     *   <li><b>客户端触发的Epoch递增</b>:
     *          如果协调器支持epoch递增(initProducerIdVersion.maxVersion() >= 3),
     *          则允许客户端触发epoch递增，返回true。
     *          在这种情况下，必须将<code>clientSideEpochBumpTriggerRequired</code>设置为true。</li>
     *
     *   <li><b>不允许Epoch递增</b>:
     *          如果协调器不支持epoch递增，返回false。</li>
     *
     *   <li><b>仅服务器触发</b>:
     *          当启用TransactionV2时，epoch递增由服务器在EndTxn中自动处理，
     *          因此不需要手动epoch递增，返回false。</li>
     * </ol>
     *
     * @return 如果允许客户端触发epoch递增则返回true，否则返回false
     */
    // 包级私有，用于测试
    boolean needToTriggerEpochBumpFromClient() {
        // 只有当协调器支持epoch递增且未启用TransactionV2时，才允许客户端触发epoch递增
        return coordinatorSupportsBumpingEpoch && !isTransactionV2Enabled;
    }

    /**
     * 判断协调器是否能够处理可中止错误
     * 从可中止错误中恢复需要epoch递增，这可以由客户端触发，
     * 或者在每个事务结束时自动处理(Transaction V2)。
     * 使用<code>needToTriggerEpochBumpFromClient</code>检查是否需要手动触发epoch递增。
     *
     * <b>注意:</b>
     * 此方法仅适用于事务型生产者。
     * 对于幂等生产者，不存在可中止错误的概念。
     *
     * @return 如果可以处理可中止错误则返回true，否则返回false
     */
    boolean canHandleAbortableError() {
        // 当协调器支持epoch递增或启用了TransactionV2时，可以处理可中止错误
        return coordinatorSupportsBumpingEpoch || isTransactionV2Enabled;
    }

    /**
     * 完成事务，清理事务状态
     * 根据是否需要客户端触发epoch递增来决定转换到的状态，
     * 并重置所有事务相关的状态变量和集合
     */
    private void completeTransaction() {
        // 如果需要客户端触发epoch递增，转换到INITIALIZING状态
        if (clientSideEpochBumpRequired) {
            transitionTo(State.INITIALIZING);
        } else {
            // 否则转换到READY状态，准备开始新的事务
            transitionTo(State.READY);
        }
        // 清除最后一次错误
        lastError = null;
        // 重置客户端epoch递增标志
        clientSideEpochBumpRequired = false;
        // 重置事务开始标志
        transactionStarted = false;
        // 清空所有事务相关的分区集合
        newPartitionsInTransaction.clear();
        pendingPartitionsInTransaction.clear();
        partitionsInTransaction.clear();
    }

    /**
     * 事务请求处理器抽象基类
     * 负责处理所有事务相关的请求，包括错误处理、重试逻辑等
     */
    abstract class TxnRequestHandler implements RequestCompletionHandler {
        // 事务请求的结果
        protected final TransactionalRequestResult result;
        // 标记是否是重试请求
        private boolean isRetry = false;

        /**
         * 使用指定的事务请求结果构造处理器
         * @param result 事务请求结果
         */
        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        /**
         * 使用操作名称构造处理器
         * @param operation 操作名称
         */
        TxnRequestHandler(String operation) {
            this(new TransactionalRequestResult(operation));
        }

        /**
         * 处理致命错误
         * 将事务管理器转换到FATAL_ERROR状态
         * @param e 运行时异常
         */
        void fatalError(RuntimeException e) {
            result.fail(e);
            transitionToFatalError(e);
        }

        /**
         * 处理可中止错误
         * 将事务管理器转换到ABORTABLE_ERROR状态
         * @param e 运行时异常
         */
        void abortableError(RuntimeException e) {
            result.fail(e);
            transitionToAbortableError(e);
        }

        /**
         * 根据事务状态和配置,判断一个错误是应该被视为可中止错误还是致命错误。
         * <ol><l> 注意：此方法仅用于事务型生产者 </l></ol>
         *
         * - <b>可中止错误(Abortable Error)</b>:
         *     如果支持epoch递增,可中止错误可以被有效处理。
         *     1) 如果启用了transactionV2,每个事务结束时会自动递增epoch
         *     2) 如果客户端可以触发epoch递增,则可以处理可中止错误
         *
         * - <b>致命错误(Fatal Error)</b>:
         *     如果不支持epoch递增,系统无法恢复,错误必须被视为致命错误
         * @param e 需要判断是可中止还是致命的错误
         */
        void abortableErrorIfPossible(RuntimeException e) {
            // 检查是否可以处理可中止错误
            if (canHandleAbortableError()) {
                // 如果需要客户端触发epoch递增,设置标志
                if (needToTriggerEpochBumpFromClient())
                    clientSideEpochBumpRequired = true;
                // 将错误标记为可中止错误
                abortableError(e);
            } else {
                // 如果无法处理可中止错误,将其标记为致命错误
                fatalError(e);
            }
        }

        /**
         * 使请求失败,将异常设置到结果中
         * @param e 导致失败的运行时异常
         */
        void fail(RuntimeException e) {
            result.fail(e);
        }

        /**
         * 重新将请求加入队列,用于请求重试
         * 在同步块中设置重试标志并重新入队
         */
        void reenqueue() {
            synchronized (TransactionManager.this) {
                this.isRetry = true;
                enqueueRequest(this);
            }
        }

        /**
         * 获取重试等待时间
         * @return 重试等待的毫秒数
         */
        long retryBackoffMs() {
            return retryBackoffMs;
        }

        /**
         * 处理客户端响应完成的回调方法
         * 处理各种响应情况:断开连接、版本不匹配、正常响应等
         * @param response 从服务器收到的响应
         */
        @Override
        public void onComplete(ClientResponse response) {
            // 检查是否存在多个在途事务请求
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                fatalError(new RuntimeException("检测到多个在途事务请求"));
            } else {
                // 清除在途请求ID
                clearInFlightCorrelationId();
                if (response.wasDisconnected()) {
                    // 处理连接断开的情况
                    log.debug("与{}的连接断开,将重试", response.destination());
                    if (this.needsCoordinator())
                        lookupCoordinator(this.coordinatorType(), this.coordinatorKey());
                    reenqueue();
                } else if (response.versionMismatch() != null) {
                    // 处理版本不匹配错误
                    fatalError(response.versionMismatch());
                } else if (response.hasResponse()) {
                    // 处理正常响应
                    log.trace("收到事务响应 {} ,对应请求 {}", response.responseBody(),
                            requestBuilder());
                    synchronized (TransactionManager.this) {
                        handleResponse(response.responseBody());
                    }
                } else {
                    // 处理未知错误
                    fatalError(new KafkaException("由于未知原因无法执行事务请求"));
                }
            }
        }

        /**
         * 检查请求是否需要协调器
         * @return 如果需要协调器返回true
         */
        boolean needsCoordinator() {
            return coordinatorType() != null;
        }

        /**
         * 获取协调器类型
         * @return 返回事务协调器类型
         */
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
        }

        /**
         * 获取协调器查找的键
         * @return 返回事务ID作为协调器键
         */
        String coordinatorKey() {
            return transactionalId;
        }

        /**
         * 设置请求为重试状态
         */
        void setRetry() {
            this.isRetry = true;
        }

        /**
         * 检查是否是重试请求
         * @return 如果是重试请求返回true
         */
        boolean isRetry() {
            return isRetry;
        }

        /**
         * 检查是否是结束事务请求
         * @return 默认返回false,子类可以覆盖此方法
         */
        boolean isEndTxn() {
            return false;
        }

        /**
         * 构建具体的请求对象
         * @return 返回请求构建器
         */
        abstract AbstractRequest.Builder<?> requestBuilder();

        /**
         * 处理响应体
         * @param responseBody 服务器返回的响应体
         */
        abstract void handleResponse(AbstractResponse responseBody);

        /**
         * 获取请求的优先级
         * @return 返回请求优先级
         */
        abstract Priority priority();
    }

    /**
     * 初始化生产者ID的请求处理器类
     * 负责处理InitProducerId请求的构建、发送和响应处理
     */
    private class InitProducerIdHandler extends TxnRequestHandler {
        // 用于构建InitProducerId请求的构建器
        private final InitProducerIdRequest.Builder builder;
        // 标识是否为Epoch递增操作
        private final boolean isEpochBump;

        /**
         * 构造函数
         * @param builder InitProducerId请求构建器
         * @param isEpochBump 是否为Epoch递增操作
         */
        private InitProducerIdHandler(InitProducerIdRequest.Builder builder, boolean isEpochBump) {
            super("InitProducerId"); // 调用父类构造函数,设置请求名称
            this.builder = builder;
            this.isEpochBump = isEpochBump;
        }

        /**
         * 获取请求构建器
         * @return InitProducerId请求的构建器
         */
        @Override
        InitProducerIdRequest.Builder requestBuilder() {
            return builder;
        }

        /**
         * 获取请求的优先级
         * 如果是Epoch递增操作则返回EPOCH_BUMP优先级
         * 否则返回INIT_PRODUCER_ID优先级
         */
        @Override
        Priority priority() {
            return this.isEpochBump ? Priority.EPOCH_BUMP : Priority.INIT_PRODUCER_ID;
        }

        /**
         * 获取协调器类型
         * 如果是事务性生产者则返回TRANSACTION类型
         * 否则返回null
         */
        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            if (isTransactional()) {
                return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
            } else {
                return null;
            }
        }

        /**
         * 处理InitProducerId请求的响应
         * @param response 服务器返回的响应
         */
        @Override
        public void handleResponse(AbstractResponse response) {
            // 将响应转换为InitProducerIdResponse类型
            InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response;
            // 获取响应中的错误码
            Errors error = initProducerIdResponse.error();

            if (error == Errors.NONE) { // 请求成功
                // 创建新的ProducerIdAndEpoch对象
                ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(initProducerIdResponse.data().producerId(),
                        initProducerIdResponse.data().producerEpoch());
                // 设置生产者ID和Epoch
                setProducerIdAndEpoch(producerIdAndEpoch);
                // 将状态转换为READY
                transitionTo(State.READY);
                // 清除上一次的错误
                lastError = null;
                // 如果是Epoch递增操作,重置序列号
                if (this.isEpochBump) {
                    resetSequenceNumbers();
                }
                // 标记请求完成
                result.done();
            } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                // 如果协调器不可用,重新查找协调器并重新入队请求
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                // 对于可重试的异常,重新入队请求
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.CLUSTER_AUTHORIZATION_FAILED) {
                // 授权失败错误,转换到可中止错误状态
                log.info("Abortable authorization error: {}.  Transition the producer state to {}", error.message(), State.ABORTABLE_ERROR);
                lastError = error.exception();
                abortableError(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // 生产者被篱笆或Epoch无效,这是致命错误
                // 注意:旧版本的事务协调器可能返回INVALID_PRODUCER_EPOCH,我们将其视为PRODUCER_FENCED处理
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                // 事务可中止错误
                abortableError(error.exception());
            } else {
                // 其他未预期的错误,作为致命错误处理
                fatalError(new KafkaException("Unexpected error in InitProducerIdResponse; " + error.message()));
            }
        }
    }

    /**
     * 添加分区到事务的请求处理器类
     * 负责处理将新的分区添加到当前事务中的请求,包括请求的构建、发送和响应处理
     */
    private class AddPartitionsToTxnHandler extends TxnRequestHandler {
        // 用于构建AddPartitionsToTxn请求的构建器
        private final AddPartitionsToTxnRequest.Builder builder;
        // 重试等待时间(毫秒)
        private long retryBackoffMs;

        /**
         * 构造函数
         * @param builder AddPartitionsToTxn请求构建器
         */
        private AddPartitionsToTxnHandler(AddPartitionsToTxnRequest.Builder builder) {
            super("AddPartitionsToTxn"); // 调用父类构造函数,设置请求名称
            this.builder = builder;
            this.retryBackoffMs = TransactionManager.this.retryBackoffMs; // 初始化重试等待时间
        }

        /**
         * 获取请求构建器
         * @return AddPartitionsToTxn请求构建器
         */
        @Override
        AddPartitionsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        /**
         * 获取请求优先级
         * @return 添加分区到事务的请求优先级
         */
        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        /**
         * 处理AddPartitionsToTxn请求的响应
         * 根据不同的错误类型进行相应处理:
         * 1. 可重试错误: 重新入队请求
         * 2. 致命错误: 将事务管理器置于错误状态
         * 3. 可中止错误: 允许事务中止并重试
         * 4. 成功: 更新事务状态
         * 
         * @param response 服务器的响应
         */
        @Override
        public void handleResponse(AbstractResponse response) {
            // 将响应转换为AddPartitionsToTxnResponse类型
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            // 获取每个分区的错误信息
            Map<TopicPartition, Errors> errors = addPartitionsToTxnResponse.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID);
            boolean hasPartitionErrors = false; // 是否存在分区错误
            Set<String> unauthorizedTopics = new HashSet<>(); // 未授权的主题集合
            retryBackoffMs = TransactionManager.this.retryBackoffMs; // 重置重试等待时间

            // 遍历每个分区的错误信息
            for (Map.Entry<TopicPartition, Errors> topicPartitionErrorEntry : errors.entrySet()) {
                TopicPartition topicPartition = topicPartitionErrorEntry.getKey();
                Errors error = topicPartitionErrorEntry.getValue();

                if (error == Errors.NONE) { // 无错误,继续处理下一个分区
                    continue;
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    // 协调器不可用或不是正确的协调器,重新查找协调器并重试
                    lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                    reenqueue();
                    return;
                } else if (error == Errors.CONCURRENT_TRANSACTIONS) {
                    // 存在并发事务,可能需要调整重试等待时间
                    maybeOverrideRetryBackoffMs();
                    reenqueue();
                    return;
                } else if (error.exception() instanceof RetriableException) {
                    // 可重试的异常,重新入队请求
                    reenqueue();
                    return;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                    // Producer epoch无效或Producer被隔离,这是致命错误
                    // 对于旧版本的事务协调器,可能会收到INVALID_PRODUCER_EPOCH错误
                    // 将其视为PRODUCER_FENCED处理
                    fatalError(Errors.PRODUCER_FENCED.exception());
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                        error == Errors.INVALID_TXN_STATE || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                    // 事务ID未授权、事务状态无效或Producer ID映射无效,这些都是致命错误
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    // 主题未授权,记录未授权的主题
                    unauthorizedTopics.add(topicPartition.topic());
                } else if (error == Errors.OPERATION_NOT_ATTEMPTED) {
                    // 由于批次中其他分区出错,未尝试添加该分区
                    log.debug("Did not attempt to add partition {} to transaction because other partitions in the " +
                            "batch had errors.", topicPartition);
                    hasPartitionErrors = true;
                } else if (error == Errors.UNKNOWN_PRODUCER_ID) {
                    // Producer ID未知,这是可中止错误
                    abortableErrorIfPossible(error.exception());
                    return;
                } else if (error == Errors.TRANSACTION_ABORTABLE) {
                    // 事务可中止
                    abortableError(error.exception());
                    return;
                } else {
                    // 其他未预期的错误
                    log.error("Could not add partition {} due to unexpected error {}", topicPartition, error);
                    hasPartitionErrors = true;
                }
            }

            // 获取所有涉及的分区
            Set<TopicPartition> partitions = errors.keySet();

            // 无论结果如何,都从待处理集合中移除这些分区
            // 我们使用待处理集合来判断何时可以安全地发送批次
            // 如果分区添加失败并进入错误状态,我们预期这些批次会被中止
            // 在这种情况下,我们必须能够继续发送那些已成功添加的分区的重试批次
            pendingPartitionsInTransaction.removeAll(partitions);

            if (!unauthorizedTopics.isEmpty()) {
                // 存在未授权的主题,抛出可中止错误
                abortableError(new TopicAuthorizationException(unauthorizedTopics));
            } else if (hasPartitionErrors) {
                // 存在分区错误,抛出可中止错误
                abortableError(new KafkaException("Could not add partitions to transaction due to errors: " + errors));
            } else {
                // 成功添加所有分区到事务
                log.debug("Successfully added partitions {} to transaction", partitions);
                partitionsInTransaction.addAll(partitions);
                transactionStarted = true;
                result.done();
            }
        }

        /**
         * 获取重试等待时间
         * @return 当前重试等待时间和事务管理器重试等待时间的较小值
         */
        @Override
        public long retryBackoffMs() {
            return Math.min(TransactionManager.this.retryBackoffMs, this.retryBackoffMs);
        }

        /**
         * 可能需要覆盖重试等待时间
         * 仅在第一次AddPartition因CONCURRENT_TRANSACTIONS错误而失败时减少等待时间
         * 这是因为前一个事务仍在完成中,我们不希望等待太长时间再尝试启动新事务
         * 
         * 注意: 这只是临时解决方案,长期解决方案正在KAFKA-5482中跟踪
         */
        private void maybeOverrideRetryBackoffMs() {
            if (partitionsInTransaction.isEmpty())
                this.retryBackoffMs = ADD_PARTITIONS_RETRY_BACKOFF_MS;
        }
    }

    /**
     * FindCoordinatorHandler类负责处理查找协调器的请求
     * 协调器分为两种类型:
     * 1. 事务协调器(TransactionCoordinator) - 负责管理事务状态
     * 2. 消费者组协调器(GroupCoordinator) - 负责管理消费者组的成员关系和位移提交
     */
    private class FindCoordinatorHandler extends TxnRequestHandler {
        // 用于构建FindCoordinator请求的构建器
        private final FindCoordinatorRequest.Builder builder;

        /**
         * 构造函数
         * @param builder FindCoordinator请求构建器
         */
        private FindCoordinatorHandler(FindCoordinatorRequest.Builder builder) {
            // 调用父类构造函数,设置处理器名称
            super("FindCoordinator");
            this.builder = builder;
        }

        @Override
        FindCoordinatorRequest.Builder requestBuilder() {
            // 返回请求构建器
            return builder;
        }

        @Override
        Priority priority() {
            // 查找协调器请求具有最高优先级
            return Priority.FIND_COORDINATOR;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            // 返回null,因为协调器类型在请求数据中已指定
            return null;
        }

        @Override
        String coordinatorKey() {
            // 返回null,因为协调器key在请求数据中已指定
            return null;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            // 从请求数据中获取协调器类型(GROUP或TRANSACTION)
            CoordinatorType coordinatorType = CoordinatorType.forId(builder.data().keyType());

            // 获取响应中的协调器列表
            List<Coordinator> coordinators = ((FindCoordinatorResponse) response).coordinators();
            // 检查响应中是否只包含一个协调器
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                fatalError(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            // 获取协调器数据
            Coordinator coordinatorData = coordinators.get(0);
            // 对于不支持批处理的旧版本,从请求数据中获取key,因为响应中不包含该信息
            String key = coordinatorData.key() == null ? builder.data().key() : coordinatorData.key();
            // 获取错误码
            Errors error = Errors.forCode(coordinatorData.errorCode());
            
            if (error == Errors.NONE) {
                // 如果没有错误,创建新的Node对象表示协调器节点
                Node node = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
                // 根据协调器类型更新相应的协调器引用
                switch (coordinatorType) {
                    case GROUP:
                        // 更新消费者组协调器
                        consumerGroupCoordinator = node;
                        break;
                    case TRANSACTION:
                        // 更新事务协调器
                        transactionCoordinator = node;
                        break;
                    default:
                        // 未知的协调器类型,记录错误并抛出异常
                        log.error("Group coordinator lookup failed: Unexpected coordinator type in response");
                        fatalError(new IllegalStateException("Group coordinator lookup failed: Unexpected coordinator type in response"));
                }
                // 标记请求完成
                result.done();
                // 记录发现协调器的日志
                log.info("Discovered {} coordinator {}", coordinatorType.toString().toLowerCase(Locale.ROOT), node);
            } else if (error.exception() instanceof RetriableException) {
                // 如果是可重试的异常,将请求重新入队
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                // 事务ID认证失败,抛出致命错误
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // 消费者组认证失败,抛出可中止错误
                abortableError(GroupAuthorizationException.forGroupId(key));
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                // 事务可中止,抛出可中止错误
                abortableError(error.exception());
            } else {
                // 其他未预期的错误,抛出致命错误
                fatalError(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to " +
                        "unexpected error: %s", coordinatorType, key,
                        coordinatorData.errorMessage())));
            }
        }
    }

    /**
     * EndTxnHandler类负责处理事务的结束(提交或中止)请求
     * 该处理器会向事务协调器发送EndTxn请求,并处理响应结果
     * 主要功能:
     * 1. 发送结束事务请求
     * 2. 处理响应中的生产者ID和epoch信息
     * 3. 处理各种错误情况
     */
    private class EndTxnHandler extends TxnRequestHandler {
        // 用于构建EndTxn请求的构建器
        private final EndTxnRequest.Builder builder;

        /**
         * 构造函数
         * @param builder EndTxn请求构建器
         */
        private EndTxnHandler(EndTxnRequest.Builder builder) {
            // 调用父类构造函数,设置处理器名称,包含事务提交状态(true表示提交,false表示中止)
            super("EndTxn(" + builder.data.committed() + ")");
            this.builder = builder;
        }

        @Override
        EndTxnRequest.Builder requestBuilder() {
            // 返回请求构建器
            return builder;
        }

        @Override
        Priority priority() {
            // 结束事务请求的优先级
            return Priority.END_TXN;
        }

        @Override
        boolean isEndTxn() {
            // 标识这是一个结束事务的请求
            return true;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            // 将响应转换为EndTxnResponse类型
            EndTxnResponse endTxnResponse = (EndTxnResponse) response;
            // 获取错误码
            Errors error = endTxnResponse.error();

            if (error == Errors.NONE) {
                // 对于EndTxn版本5+,broker会在响应中包含producerId和producerEpoch
                // 对于版本5以下,producerId和epoch默认设置为-1
                // 当启用事务版本2时,使用EndTxn请求5+版本,
                // 它要求在每个事务后递增epoch
                // 如果epoch溢出,会返回一个新的producerId,epoch设置为0
                // 注意:当生产者升级到TV2时,由于升级发生在beginCompletingTransaction结束时,
                // 我们仍可能看到EndTxn TV1(<5)的响应。下一个启动的事务应该是TV2。
                if (endTxnResponse.data().producerId() != -1) {
                    // 创建新的ProducerIdAndEpoch对象
                    ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(
                        endTxnResponse.data().producerId(),
                        endTxnResponse.data().producerEpoch()
                    );
                    // 更新生产者ID和epoch
                    setProducerIdAndEpoch(producerIdAndEpoch);
                    // 重置序列号
                    resetSequenceNumbers();
                }
                // 完成事务
                completeTransaction();
                // 标记请求完成
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                // 如果协调器不可用或不是正确的协调器,重新查找协调器并重试请求
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                // 对于可重试的异常,重新入队请求
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // 我们可能从旧版本的事务协调器收到INVALID_PRODUCER_EPOCH错误,
                // 将其视为PRODUCER_FENCED处理
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.INVALID_TXN_STATE || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                // 事务ID认证失败、无效的事务状态或无效的生产者ID映射,抛出致命错误
                fatalError(error.exception());
            } else if (error == Errors.UNKNOWN_PRODUCER_ID) {
                // 未知的生产者ID,尝试将其作为可中止错误处理
                abortableErrorIfPossible(error.exception());
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                // 事务可中止,抛出可中止错误
                abortableError(error.exception());
            } else {
                // 其他未处理的错误,抛出致命错误
                fatalError(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
            }
        }
    }

    /**
     * AddOffsetsToTxnHandler类负责处理将消费者组偏移量添加到事务的请求
     * 该处理器实现了消费者-生产者事务的关键功能:
     * 1. 将消费者组的偏移量作为事务的一部分
     * 2. 确保偏移量的提交与事务的原子性
     * 3. 处理各种错误情况并维护事务状态
     */
    private class AddOffsetsToTxnHandler extends TxnRequestHandler {
        // 用于构建AddOffsetsToTxn请求的构建器
        private final AddOffsetsToTxnRequest.Builder builder;
        // 需要添加到事务中的分区偏移量映射
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        // 消费者组元数据信息
        private final ConsumerGroupMetadata groupMetadata;

        /**
         * 构造函数
         * @param builder AddOffsetsToTxn请求构建器
         * @param offsets 需要添加到事务中的分区偏移量映射
         * @param groupMetadata 消费者组元数据
         */
        private AddOffsetsToTxnHandler(AddOffsetsToTxnRequest.Builder builder,
                                       Map<TopicPartition, OffsetAndMetadata> offsets,
                                       ConsumerGroupMetadata groupMetadata) {
            // 调用父类构造函数,设置处理器名称
            super("AddOffsetsToTxn");
            this.builder = builder;
            this.offsets = offsets;
            this.groupMetadata = groupMetadata;
        }

        @Override
        AddOffsetsToTxnRequest.Builder requestBuilder() {
            // 返回请求构建器
            return builder;
        }

        @Override
        Priority priority() {
            // 添加分区或偏移量请求的优先级
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            // 将响应转换为AddOffsetsToTxnResponse类型
            AddOffsetsToTxnResponse addOffsetsToTxnResponse = (AddOffsetsToTxnResponse) response;
            // 获取错误码
            Errors error = Errors.forCode(addOffsetsToTxnResponse.data().errorCode());

            if (error == Errors.NONE) {
                // 成功将消费者组的分区添加到事务中
                log.debug("Successfully added partition for consumer group {} to transaction", builder.data.groupId());

                // 注意:在TxnOffsetCommit返回之前,结果不会被标记为完成
                // 创建并添加事务偏移量提交处理器
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, groupMetadata));

                // 标记事务已启动
                transactionStarted = true;
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                // 如果协调器不可用或不是正确的协调器,重新查找协调器并重试请求
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                // 对于可重试的异常,重新入队请求
                reenqueue();
            } else if (error == Errors.UNKNOWN_PRODUCER_ID) {
                // 未知的生产者ID,尝试将其作为可中止错误处理
                abortableErrorIfPossible(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // 我们可能从旧版本的事务协调器收到INVALID_PRODUCER_EPOCH错误,
                // 将其视为PRODUCER_FENCED处理
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.INVALID_TXN_STATE || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                // 事务ID认证失败、无效的事务状态或无效的生产者ID映射,抛出致命错误
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // 消费者组认证失败,抛出可中止错误
                abortableError(GroupAuthorizationException.forGroupId(builder.data.groupId()));
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                // 事务可中止,抛出可中止错误
                abortableError(error.exception());
            } else {
                // 其他未处理的错误,抛出致命错误
                fatalError(new KafkaException("Unexpected error in AddOffsetsToTxnResponse: " + error.message()));
            }
        }
    }

    /**
     * TxnOffsetCommitHandler类负责处理事务性偏移量提交请求
     * 该处理器的主要功能:
     * 1. 提交事务中消费者组的偏移量
     * 2. 处理提交过程中的各种错误情况
     * 3. 维护待提交偏移量的状态
     * 4. 确保偏移量提交的事务性语义
     */
    private class TxnOffsetCommitHandler extends TxnRequestHandler {
        // 用于构建TxnOffsetCommit请求的构建器
        private final TxnOffsetCommitRequest.Builder builder;

        /**
         * 构造函数
         * @param result 事务请求结果
         * @param builder TxnOffsetCommit请求构建器
         */
        private TxnOffsetCommitHandler(TransactionalRequestResult result,
                                       TxnOffsetCommitRequest.Builder builder) {
            super(result);
            this.builder = builder;
        }

        /**
         * 构造函数
         * @param builder TxnOffsetCommit请求构建器
         */
        private TxnOffsetCommitHandler(TxnOffsetCommitRequest.Builder builder) {
            super("TxnOffsetCommitHandler");
            this.builder = builder;
        }

        @Override
        TxnOffsetCommitRequest.Builder requestBuilder() {
            // 返回请求构建器
            return builder;
        }

        @Override
        Priority priority() {
            // 添加分区或偏移量请求的优先级
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            // 返回GROUP类型,因为偏移量提交需要与消费者组协调器交互
            return FindCoordinatorRequest.CoordinatorType.GROUP;
        }

        @Override
        String coordinatorKey() {
            // 返回消费者组ID作为协调器key
            return builder.data.groupId();
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            // 将响应转换为TxnOffsetCommitResponse类型
            TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) response;
            // 标记是否已重新加载协调器
            boolean coordinatorReloaded = false;
            // 获取每个分区的错误信息
            Map<TopicPartition, Errors> errors = txnOffsetCommitResponse.errors();

            // 记录调试日志
            log.debug("Received TxnOffsetCommit response for consumer group {}: {}", builder.data.groupId(),
                    errors);

            // 遍历每个分区的错误信息
            for (Map.Entry<TopicPartition, Errors> entry : errors.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    // 如果没有错误,从待提交列表中移除该分区
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR
                        || error == Errors.REQUEST_TIMED_OUT) {
                    // 如果协调器不可用、不是正确的协调器或请求超时
                    if (!coordinatorReloaded) {
                        // 重新查找消费者组协调器
                        coordinatorReloaded = true;
                        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.data.groupId());
                    }
                } else if (error.exception() instanceof RetriableException) {
                    // 如果是可重试的异常(如主题未知、协调器正在加载等),继续处理下一个分区
                    continue;
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    // 消费者组认证失败
                    abortableError(GroupAuthorizationException.forGroupId(builder.data.groupId()));
                    break;
                } else if (error == Errors.FENCED_INSTANCE_ID ||
                        error == Errors.TRANSACTION_ABORTABLE) {
                    // 消费者实例被隔离或事务可中止
                    abortableError(error.exception());
                    break;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    // 未知的成员ID或非法的消费者组代次
                    abortableError(new CommitFailedException("Transaction offset Commit failed " +
                        "due to consumer group metadata mismatch: " + error.exception().getMessage()));
                    break;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH
                        || error == Errors.PRODUCER_FENCED) {
                    // 从旧版本事务协调器收到INVALID_PRODUCER_EPOCH错误,
                    // 将其视为PRODUCER_FENCED处理
                    fatalError(Errors.PRODUCER_FENCED.exception());
                    break;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                        || error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                    // 事务ID认证失败或消息格式不支持
                    fatalError(error.exception());
                    break;
                } else {
                    // 其他未预期的错误
                    fatalError(new KafkaException("Unexpected error in TxnOffsetCommitResponse: " + error.message()));
                    break;
                }
            }

            // 处理结果
            if (result.isCompleted()) {
                // 如果请求已完成,清空待提交列表
                pendingTxnOffsetCommits.clear();
            } else if (pendingTxnOffsetCommits.isEmpty()) {
                // 如果待提交列表为空,标记请求完成
                result.done();
            } else {
                // 重试那些因可重试错误而失败的提交
                reenqueue();
            }
        }
    }

    /**
     * PendingStateTransition类用于管理事务状态的转换过程
     * 该类维护了状态转换的关键信息:
     * 1. 转换结果 - 用于跟踪状态转换的完成情况
     * 2. 目标状态 - 转换要达到的目标状态
     * 3. 操作名称 - 触发状态转换的操作
     */
    private static final class PendingStateTransition {
        // 状态转换的结果
        private final TransactionalRequestResult result;
        // 转换的目标状态
        private final State state;
        // 触发状态转换的操作名称
        private final String operation;

        /**
         * 构造函数
         * @param result 状态转换的结果
         * @param state 转换的目标状态
         * @param operation 触发状态转换的操作名称
         */
        private PendingStateTransition(
            TransactionalRequestResult result,
            State state,
            String operation
        ) {
            this.result = result;
            this.state = state;
            this.operation = operation;
        }
    }


}
