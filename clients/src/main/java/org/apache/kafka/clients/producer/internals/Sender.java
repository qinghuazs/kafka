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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ProduceResponse.INVALID_OFFSET;

/**
 * Kafka生产者的后台发送线程，负责处理消息发送请求。该线程会定期更新集群元数据视图，
 * 并将消息批次发送到合适的Kafka broker节点。
 * 
 * 主要功能:
 * 1. 维护与Kafka集群的网络连接
 * 2. 管理消息批次的发送和重试
 * 3. 处理事务相关的状态
 * 4. 保证消息的顺序性(如果配置)
 * 5. 处理发送超时和失败重试
 */
public class Sender implements Runnable {

    /* 用于记录日志的Logger实例 */
    private final Logger log;

    /* 维护与Kafka集群中各个节点的网络连接状态 */
    private final KafkaClient client;

    /* 消息累加器，用于将消息分批并进行缓存 */
    private final RecordAccumulator accumulator;

    /* 保存Kafka集群的元数据信息，如主题分区、leader副本等 */
    private final ProducerMetadata metadata;

    /* 是否保证消息在broker上的顺序，true表示需要保证顺序 */
    private final boolean guaranteeMessageOrder;

    /* 发送给服务器的请求大小上限(字节) */
    private final int maxRequestSize;

    /* 消息发送的确认机制(acks):
     * 0: 不等待确认
     * 1: 等待leader确认
     * -1: 等待所有ISR确认 */
    private final short acks;

    /* 发送失败时的最大重试次数 */
    private final int retries;

    /* 用于获取系统时间的实例 */
    private final Time time;

    /* 标记发送线程是否正在运行 */
    private volatile boolean running;

    /* 是否强制关闭发送线程，true时会忽略所有未发送和在途的消息 */
    private volatile boolean forceClose;

    /* 用于监控和统计的度量指标 */
    private final SenderMetrics sensors;

    /* 等待服务器响应的最大超时时间(毫秒) */
    private final int requestTimeoutMs;

    /* 重试发送失败的请求前的等待时间(毫秒) */
    private final long retryBackoffMs;

    /* Kafka集群中各个broker支持的API版本信息 */
    private final ApiVersions apiVersions;

    /* 事务管理器，维护事务相关的状态(producerId、epoch、序列号等) */
    private final TransactionManager transactionManager;

    /* 记录每个主题分区当前在途(已发送未确认)的消息批次，按创建时间排序 */
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    /**
     * 获取指定主题分区当前在途的消息批次列表
     * 
     * @param tp 主题分区对象
     * @return 该分区的在途消息批次列表，如果没有则返回空列表
     */
    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    /**
     * 从在途批次集合中移除指定的消息批次
     * 
     * @param batch 要移除的消息批次
     */
    private void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        // 获取该批次所属主题分区的所有在途批次
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            // 从列表中移除该批次
            batches.remove(batch);
            // 如果该分区没有在途批次了，则从map中移除该分区的记录
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    /**
     * 移除指定的消息批次，并释放其占用的内存资源
     * 
     * @param batch 要移除和释放的消息批次
     */
    private void maybeRemoveAndDeallocateBatch(ProducerBatch batch) {
        // 从在途批次集合中移除
        maybeRemoveFromInflightBatches(batch);
        // 释放批次占用的内存资源
        this.accumulator.deallocate(batch);
    }

    /**
     * 获取所有已超过发送超时时间的在途消息批次
     * 
     * @param now 当前时间戳(毫秒)
     * @return 已超时的消息批次列表
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        // 遍历所有主题分区的在途批次
        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    // 检查批次是否已超时
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove(); // 从在途列表中移除
                        // 该方法在sendProducerData中调用，在client.poll之前
                        // 此时批次必须是未完成状态，否则说明状态异常
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        // 更新下一个可能过期的批次时间
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                // 如果该分区的所有批次都已处理完，从map中移除该分区
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    /**
     * 将一组消息批次添加到在途批次集合中
     * 
     * @param batches 要添加的消息批次列表
     */
    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            // 如果该分区还没有在途批次列表，则创建一个新的列表
            List<ProducerBatch> inflightBatchList = inFlightBatches.computeIfAbsent(batch.topicPartition,
                k -> new ArrayList<>());
            inflightBatchList.add(batch);
        }
    }

    /**
     * 将多个节点的消息批次添加到在途批次集合中
     * 
     * @param batches Map<节点ID, 消息批次列表>
     */
    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    /**
     * 检查是否有未完成的事务请求
     * 
     * @return true表示存在未完成的事务请求，false表示没有
     */
    private boolean hasPendingTransactionalRequests() {
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

    /**
     * 发送线程的主循环方法，负责消息的发送和事务的处理
     * 该方法会一直运行，直到线程被关闭
     */
    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // 如果启用了事务，设置状态转换异常时的处理策略
        if (transactionManager != null)
            transactionManager.setPoisonStateOnInvalidTransition(true);

        // 主循环，直到调用close方法时才会退出
        while (running) {
            try {
                runOnce(); // 执行一次消息发送循环
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // 虽然已停止接收新的请求，但可能还有未处理完的请求：
        // 1. 事务管理器中的请求
        // 2. 累加器中未发送的消息
        // 3. 已发送但等待确认的请求
        // 需要等待这些请求处理完成
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // 如果有未完成的事务(未经过事务管理器的提交或中止操作)，需要中止该事务
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");
                try {
                    // 事务管理器在中止事务时可能会抛出异常
                    // 捕获这些异常以避免影响其他关闭逻辑的执行
                    transactionManager.beginAbort();
                } catch (Exception e) {
                    log.error("Error in kafka producer I/O thread while aborting transaction when during closing: ", e);
                    // 如果事务管理器处于错误状态，强制关闭
                    forceClose = true;
                }
            }
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // 如果是强制关闭
        if (forceClose) {
            // 需要使所有未完成的事务请求和批次失败
            // 并唤醒等待这些请求的线程
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close(); // 关闭网络客户端
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * 执行一次消息发送迭代
     * 该方法是Sender线程的核心方法，负责处理事务状态、准备和发送消息批次
     */
    void runOnce() {
        // 如果启用了事务，需要先处理事务相关的逻辑
        if (transactionManager != null) {
            try {
                // 尝试解析事务序列号，确保消息的顺序性
                transactionManager.maybeResolveSequences();

                // 获取事务管理器最近的错误
                RuntimeException lastError = transactionManager.lastError();

                // 如果事务管理器处于致命错误状态，中止所有批次并返回
                if (transactionManager.hasFatalError()) {
                    if (lastError != null)
                        maybeAbortBatches(lastError); // 中止所有未完成的批次
                    client.poll(retryBackoffMs, time.milliseconds()); // 等待重试时间后再次尝试
                    return;
                }

                // 处理授权错误，如果需要则中止事务
                if (transactionManager.hasAbortableError() && shouldHandleAuthorizationError(lastError)) {
                    return;
                }

                // 检查是否需要新的生产者ID，如果需要则将初始化生产者ID的请求加入队列
                transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

                // 尝试发送事务相关的请求（如InitPid、AddPartitions等）
                if (maybeSendAndPollTransactionalRequest()) {
                    return;
                }
            } catch (AuthenticationException e) {
                // 认证异常已被记录，这里传播异常以执行必要的清理工作
                log.trace("Authentication exception while processing transactional request", e);
                transactionManager.authenticationFailed(e);
            }
        }

        // 获取当前时间戳
        long currentTimeMs = time.milliseconds();
        // 发送准备好的消息数据，并获取下一次轮询的超时时间
        long pollTimeout = sendProducerData(currentTimeMs);
        // 执行网络I/O操作，发送请求并处理响应
        client.poll(pollTimeout, currentTimeMs);
    }

    // We handle {@code TransactionalIdAuthorizationException} and {@code ClusterAuthorizationException} by first
    // failing the inflight requests, then transition the state to UNINITIALIZED so that the user doesn't need to
    // instantiate the producer again.
    private boolean shouldHandleAuthorizationError(RuntimeException exception) {
        if (exception instanceof TransactionalIdAuthorizationException ||
                        exception instanceof ClusterAuthorizationException) {
            transactionManager.failPendingRequests(new AuthenticationException(exception));
            maybeAbortBatches(exception);
            transactionManager.transitionToUninitialized(exception);
            return true;
        }
        return false;
    }

    /**
     * 准备并发送生产者数据
     * 该方法负责检查消息批次是否准备就绪，处理元数据更新，创建发送请求等核心功能
     * 
     * @param now 当前时间戳(毫秒)
     * @return 下一次轮询的超时时间(毫秒)
     */
    private long sendProducerData(long now) {
        // 获取当前集群的元数据快照
        MetadataSnapshot metadataSnapshot = metadata.fetchMetadataSnapshot();
        // 检查哪些分区的数据已经准备好可以发送
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(metadataSnapshot, now);

        // 如果有分区的leader未知，强制更新元数据
        if (!result.unknownLeaderTopics.isEmpty()) {
            // 未知leader的主题可能是因为leader选举正在进行或主题已过期
            // 将这些主题重新添加到元数据中并请求更新
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic, now);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics);
            this.metadata.requestUpdate(false);
        }

        // 移除当前无法发送数据的节点
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            if (!this.client.ready(node, now)) {
                // 仅更新延迟统计中的readyTimeMs，使其前进
                // 这样readyTimeMs和drainTimeMs的差值就表示数据等待节点的时间
                this.accumulator.updateNodeLatencyStats(node.id(), now, false);
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
            } else {
                // 节点已就绪，更新readyTimeMs和drainTimeMs，重置节点延迟
                this.accumulator.updateNodeLatencyStats(node.id(), now, true);
            }
        }

        // 创建生产请求：从累加器中抽取数据，按节点分组形成批次
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(metadataSnapshot, result.readyNodes, this.maxRequestSize, now);
        addToInflightBatches(batches); // 将批次添加到在途批次集合中
        
        // 如果需要保证消息顺序
        if (guaranteeMessageOrder) {
            // 暂停已抽取数据的分区，确保顺序性
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        // 重置下一个批次的过期时间
        accumulator.resetNextBatchExpiryTime();
        // 获取已过期的在途批次和累加器中的批次
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
        expiredBatches.addAll(expiredInflightBatches);

        // 处理过期的批次：
        // 1. 如果批次之前已发送给broker，需要重置生产者ID
        // 2. 更新过期批次的指标统计
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        for (ProducerBatch expiredBatch : expiredBatches) {
            // 构建错误信息并使批次失败
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            failBatch(expiredBatch, new TimeoutException(errorMessage), false);
            // 如果启用了事务且批次正在重试
            if (transactionManager != null && expiredBatch.inRetry()) {
                // 标记序列号未解析，确保在当前在途批次完全解析前不会抽取新的批次
                transactionManager.markSequenceUnresolved(expiredBatch);
            }
        }
        // 更新生产请求的指标
        sensors.updateProduceRequestMetrics(batches);

        // 计算下一次轮询的超时时间：
        // 1. 如果有节点已准备好且有数据可发送，超时时间为0，立即进行下一次循环
        // 2. 否则，超时时间为下一个批次过期时间和检查数据可用性延迟时间的较小值
        // 注意：某些节点可能因为lingering或backing off而暂时无法发送数据
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        pollTimeout = Math.max(pollTimeout, 0);
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            // 如果有分区已准备好发送，超时时间设为0
            // 如果有分区已累积数据但未就绪，超时时间为到linger过期的时间
            // 否则超时时间为到元数据过期的时间
            pollTimeout = 0;
        }
        // 发送生产请求
        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    /**
     * 尝试发送和轮询事务请求
     * 该方法负责处理事务相关的请求发送，包括查找事务协调者、处理错误状态和重试机制
     * 
     * @return 如果发送了事务请求、执行了轮询操作或者入队了查找协调者的请求，则返回true；否则返回false
     */
    private boolean maybeSendAndPollTransactionalRequest() {
        // 如果当前有未完成的事务请求，等待其返回
        if (transactionManager.hasInFlightRequest()) {
            // 轮询网络I/O，等待已发送请求的响应
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        }

        // 处理事务错误状态
        if (transactionManager.hasAbortableError()) {
            // 如果存在可中止的错误，中止所有未发送的批次
            accumulator.abortUndrainedBatches(transactionManager.lastError());
        } else if (transactionManager.isAborting()) {
            // 如果事务正在中止中，使用事务中止异常中止所有未发送的批次
            accumulator.abortUndrainedBatches(new TransactionAbortedException());
        }

        // 获取下一个要处理的事务请求
        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        // 构建请求对象
        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        Node targetNode = null;
        try {
            // 获取协调者类型，并选择目标节点
            FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
            // 如果需要协调者，使用已知的协调者节点；否则选择负载最小的节点
            targetNode = coordinatorType != null ?
                    transactionManager.coordinator(coordinatorType) :
                    client.leastLoadedNode(time.milliseconds()).node();
            if (targetNode != null) {
                // 等待目标节点就绪
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace("目标节点 {} 在请求超时时间内未就绪，将在节点就绪后重试", targetNode);
                    maybeFindCoordinatorAndRetry(nextRequestHandler);
                    return true;
                }
            } else if (coordinatorType != null) {
                // 如果需要协调者但协调者未知，先查找协调者
                log.trace("协调者 {} 未知，将在找到协调者后重试请求 {}", coordinatorType, requestBuilder.apiKey());
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            } else {
                // 如果没有可用节点，等待并重试
                log.trace("没有可用的节点处理请求，将等待并在节点就绪后重试");
                transactionManager.retry(nextRequestHandler);
                client.poll(retryBackoffMs, time.milliseconds());
                return true;
            }

            // 如果是重试请求，先等待重试退避时间
            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());

            // 创建并发送客户端请求
            long currentTimeMs = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(targetNode.idString(), requestBuilder, currentTimeMs,
                true, requestTimeoutMs, nextRequestHandler);
            log.debug("正在向节点 {} 发送事务请求 {}，关联ID为 {}", requestBuilder, targetNode, clientRequest.correlationId());
            client.send(clientRequest, currentTimeMs);
            // 记录请求的关联ID，用于后续响应匹配
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            // 执行一次轮询，处理响应
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            // 处理网络连接异常
            log.debug("尝试向节点 {} 发送请求 {} 时断开连接，将退避后重试", targetNode, requestBuilder, e);
            // 立即触发查找协调者的请求
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    /**
     * 处理协调者查找和请求重试逻辑
     * 该方法根据请求类型决定是否需要查找协调者，并相应地处理重试机制
     * 
     * @param nextRequestHandler 需要重试的事务请求处理器
     */
    private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        // 检查请求是否需要协调者
        if (nextRequestHandler.needsCoordinator()) {
            // 如果需要协调者，触发查找协调者的操作
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // 对于不需要协调者的请求，为避免无节点可用时的紧密循环，先等待一段时间
            time.sleep(retryBackoffMs);
            // 请求更新元数据，以便发现新的可用节点
            metadata.requestUpdate(false);
        }

        // 将请求标记为需要重试
        transactionManager.retry(nextRequestHandler);
    }

    /**
     * 在发生致命错误时中止未完成的消息批次
     * 该方法会检查是否有未完成的批次，如果有则使用给定的异常中止它们
     * 
     * @param exception 导致中止的运行时异常
     */
    private void maybeAbortBatches(RuntimeException exception) {
        // 检查是否存在未完成的消息批次
        if (accumulator.hasIncomplete()) {
            // 记录错误日志
            log.error("由于致命错误正在中止生产者批次", exception);
            // 中止所有未完成的批次，并传播异常
            accumulator.abortBatches(exception);
        }
    }

    /**
     * 开始关闭发送线程(在所有数据发送完成之前不会真正结束)
     * 
     * 该方法会先关闭消息累加器，确保不再接受新的消息追加请求，
     * 然后将running标记设为false，最后唤醒发送线程处理剩余消息。
     */
    public void initiateClose() {
        // 首先关闭累加器，确保在退出发送循环后不再接受新的追加请求
        // 否则在关闭过程中可能会丢失一些回调
        this.accumulator.close();
        // 标记发送线程停止运行
        this.running = false;
        // 唤醒发送线程，使其能够及时处理关闭请求
        this.wakeup();
    }

    /**
     * 强制关闭发送线程，不等待任何未发送的消息
     * 
     * 该方法会立即将forceClose标记设为true，然后调用initiateClose()方法
     * 进行关闭。与普通关闭不同，强制关闭会丢弃所有未发送的消息。
     */
    public void forceClose() {
        // 设置强制关闭标记
        this.forceClose = true;
        // 调用普通关闭方法
        initiateClose();
    }

    /**
     * 检查发送线程是否正在运行
     * 
     * @return true表示发送线程正在运行，false表示已停止
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * 等待指定节点就绪
     * 
     * 该方法会尝试等待节点准备就绪，如果是事务协调器节点，
     * 还会通知事务管理器协调器已就绪。
     * 
     * @param node 要等待的节点
     * @param coordinatorType 协调器类型(事务或消费者组)
     * @return true表示节点已就绪，false表示等待超时
     * @throws IOException 如果等待过程中发生I/O错误
     */
    private boolean awaitNodeReady(Node node, FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        // 等待节点就绪，超时时间为requestTimeoutMs
        if (NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            // 如果是事务协调器节点
            if (coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                // 通知事务管理器协调器已就绪，这样即使协调器暂时不可用
                // 也可以在处理可中止错误时增加事务epoch
                transactionManager.handleCoordinatorReady();
            }
            return true;
        }
        return false;
    }

    /**
     * 处理生产请求的响应
     * 
     * 该方法负责处理从broker返回的生产请求响应，包括：
     * 1. 处理超时、断开连接等错误情况
     * 2. 解析响应数据，更新分区leader信息
     * 3. 完成消息批次的处理
     * 4. 记录延迟等监控指标
     * 
     * @param response broker返回的响应对象
     * @param batches 与该响应关联的消息批次映射表
     * @param now 当前时间戳(毫秒)
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
        // 获取请求头和关联ID
        RequestHeader requestHeader = response.requestHeader();
        int correlationId = requestHeader.correlationId();

        // 处理请求超时的情况
        if (response.wasTimedOut()) {
            log.trace("Cancelled request with header {} due to the last request to node {} timed out",
                requestHeader, response.destination());
            // 将所有批次标记为超时错误
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.REQUEST_TIMED_OUT, String.format("Disconnected from node %s due to timeout", response.destination())),
                        correlationId, now, null);
        }
        // 处理连接断开的情况
        else if (response.wasDisconnected()) {
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            // 将所有批次标记为网络错误
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION, String.format("Disconnected from node %s", response.destination())),
                        correlationId, now, null);
        }
        // 处理API版本不匹配的情况
        else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            // 将所有批次标记为版本不支持错误
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now, null);
        }
        // 处理正常响应
        else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // 如果有响应体，解析响应内容
            if (response.hasResponse()) {
                // 使用PartitionProduceResponse而不是ProduceResponse.PartitionResponse
                // 参见：https://issues.apache.org/jira/browse/KAFKA-10696
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                // 用于存储需要更新leader信息的分区
                Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();

                // 处理每个分区的响应
                produceResponse.data().responses().forEach(r -> r.partitionResponses().forEach(p -> {
                    // 创建主题分区对象
                    TopicPartition tp = new TopicPartition(r.name(), p.index());
                    // 构建分区响应对象
                    ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(
                            Errors.forCode(p.errorCode()),  // 错误码
                            p.baseOffset(),                  // 基准偏移量
                            INVALID_OFFSET,                  // 无效偏移量
                            p.logAppendTimeMs(),            // 日志追加时间
                            p.logStartOffset(),             // 日志起始偏移量
                            p.recordErrors()                 // 记录错误列表
                                .stream()
                                .map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                                .collect(Collectors.toList()),
                            p.errorMessage(),               // 错误消息
                            p.currentLeader());             // 当前leader信息
                    // 获取对应的消息批次并完成处理
                    ProducerBatch batch = batches.get(tp);
                    completeBatch(batch, partResp, correlationId, now, partitionsWithUpdatedLeaderInfo);
                }));

                // 如果有分区的leader信息需要更新
                if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                    // 从响应中提取新的leader节点信息
                    List<Node> leaderNodes = produceResponse.data().nodeEndpoints().stream()
                        .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                        .filter(e -> !e.equals(Node.noNode()))
                        .collect(Collectors.toList());
                    // 更新元数据中的分区leadership信息
                    Set<TopicPartition> updatedPartitions = metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
                    // 记录更新的分区信息
                    if (log.isTraceEnabled()) {
                        updatedPartitions.forEach(
                            part -> log.debug("For {} leader was updated.", part)
                        );
                    }
                }

                // 记录请求延迟指标
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // acks=0的情况，不等待响应，直接完成所有批次
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, null);
                }
            }
        }
    }

    /**
     * 处理消息批次的完成或重试逻辑
     * 该方法根据服务器的响应结果，决定如何处理一个消息批次：
     * 1. 如果批次太大，则进行分割并重新发送
     * 2. 如果发生错误，根据错误类型决定是重试、标记成功还是失败
     * 3. 处理元数据相关的错误，必要时更新集群元数据
     * 4. 如果成功，则完成批次处理
     *
     * @param batch 要处理的消息批次
     * @param response broker的响应结果
     * @param correlationId 请求的关联ID
     * @param now 当前时间戳(毫秒)
     * @param partitionsWithUpdatedLeaderInfo 用于存储需要更新leader信息的分区
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now, Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo) {
        // 获取响应中的错误码
        Errors error = response.error;

        // 处理消息批次过大的情况
        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // 如果批次太大且包含多条消息，将其分割成多个小批次重新发送
            // 这种情况下不减少重试次数
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                formatErrMsg(response));
            // 如果启用了事务，从事务管理器中移除该批次
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            // 分割批次并重新入队
            this.accumulator.splitAndReenqueue(batch);
            // 移除原批次并释放内存
            maybeRemoveAndDeallocateBatch(batch);
            // 记录批次分割事件
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) { // 处理其他错误情况
            if (canRetry(batch, response, now)) { // 如果可以重试
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    formatErrMsg(response));
                // 将批次重新入队等待重试
                reenqueueBatch(batch, now);
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // 如果收到重复序列号错误，说明序列号已经超过当前批次的序列
                // 且broker上没有保留批次元数据来返回正确的偏移量和时间戳
                // 此时只能向用户返回成功，但不返回有效的偏移量和时间戳
                completeBatch(batch, response);
            } else {
                // 通知用户请求的结果
                // 只有在批次未耗尽重试次数时才调整序列号
                // 因为如果重试次数耗尽，我们不知道序列号是否被接受
                // 因此不能安全地重新分配序列号
                failBatch(batch, response, batch.attempts() < this.retries);
            }
            // 处理元数据相关的错误
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    // 主题或分区不存在，或用户没有Describe权限
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    // 收到无效的元数据错误，请求更新元数据
                    log.warn("Received invalid metadata error in produce request on partition {} due to {} Going " +
                            "to request metadata update now", batch.topicPartition, error.exception(response.errorMessage).toString());
                }
                // 处理leader相关的错误
                if (error.exception() instanceof NotLeaderOrFollowerException || error.exception() instanceof FencedLeaderEpochException) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", batch.topicPartition, error, response.currentLeader);
                    // 如果响应中包含新的leader信息，更新元数据
                    if (partitionsWithUpdatedLeaderInfo != null
                        && (response.currentLeader.leaderId() != -1 && response.currentLeader.leaderEpoch() != -1)) {
                        partitionsWithUpdatedLeaderInfo.put(batch.topicPartition, new Metadata.LeaderIdAndEpoch(
                            Optional.of(response.currentLeader.leaderId()), Optional.of(response.currentLeader.leaderEpoch())));
                    }
                }
                // 请求更新元数据
                metadata.requestUpdate(false);
            }
        } else { // 没有错误，正常完成批次
            completeBatch(batch, response);
        }

        // 如果配置了消息顺序保证，解除对该分区的静默
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * 将ProduceResponse.PartitionResponse中的错误格式化为用户友好的字符串
     * 例如："NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     * 
     * @param response broker的响应结果
     * @return 格式化后的错误消息字符串
     */
    private String formatErrMsg(ProduceResponse.PartitionResponse response) {
        // 如果错误消息为空，则不添加错误消息后缀
        String errorMessageSuffix = (response.errorMessage == null || response.errorMessage.isEmpty()) ?
                "" : String.format(". Error Message: %s", response.errorMessage);
        // 返回格式化后的错误信息
        return String.format("%s%s", response.error, errorMessageSuffix);
    }

    /**
     * 重新将批次放入队列等待重试
     * 
     * @param batch 需要重试的消息批次
     * @param currentTimeMs 当前时间戳(毫秒)
     */
    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        // 将批次重新放入累加器的队列中
        this.accumulator.reenqueue(batch, currentTimeMs);
        // 从在途批次集合中移除
        maybeRemoveFromInflightBatches(batch);
        // 记录重试次数统计
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    /**
     * 完成一个消息批次的处理
     * 
     * @param batch 要完成的消息批次
     * @param response broker的响应结果
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        // 如果启用了事务，通知事务管理器批次已完成
        if (transactionManager != null) {
            transactionManager.handleCompletedBatch(batch, response);
        }

        // 使用broker返回的基准偏移量和日志追加时间完成批次
        // 如果批次成功完成，则移除并释放资源
        if (batch.complete(response.baseOffset, response.logAppendTime)) {
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    /**
     * 处理批次失败的情况，包括整个批次失败和部分记录失败的场景
     * 
     * @param batch 失败的消息批次
     * @param response broker的响应结果
     * @param adjustSequenceNumbers 是否需要调整序列号
     */
    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           boolean adjustSequenceNumbers) {
        // 根据错误类型创建顶层异常
        final RuntimeException topLevelException;
        if (response.error == Errors.TOPIC_AUTHORIZATION_FAILED)
            // 主题授权失败
            topLevelException = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
        else if (response.error == Errors.CLUSTER_AUTHORIZATION_FAILED)
            // 集群授权失败，生产者不被允许进行幂等发送
            topLevelException = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
        else
            // 其他错误类型
            topLevelException = response.error.exception(response.errorMessage);

        // 如果没有具体的记录错误，则整个批次都失败
        if (response.recordErrors == null || response.recordErrors.isEmpty()) {
            failBatch(batch, topLevelException, adjustSequenceNumbers);
        } else {
            // 处理部分记录失败的情况
            Map<Integer, RuntimeException> recordErrorMap = new HashMap<>(response.recordErrors.size());
            for (ProduceResponse.RecordError recordError : response.recordErrors) {
                // API的设计使得我们难以区分不同的错误情况（如INVALID_TIMESTAMP）
                // 因为在分区级别只有一个错误码，所以我们对所有失败的记录使用INVALID_RECORD
                // 并依赖错误消息来区分具体原因
                final String errorMessage;
                if (recordError.message != null) {
                    errorMessage = recordError.message;
                } else if (response.errorMessage != null) {
                    errorMessage = response.errorMessage;
                } else {
                    errorMessage = response.error.message();
                }

                // 如果批次只包含一个记录错误，我们可以明确地使用分区级别错误码对应的异常类型
                if (response.recordErrors.size() == 1) {
                    recordErrorMap.put(recordError.batchIndex, response.error.exception(errorMessage));
                } else {
                    // 多个记录错误时使用InvalidRecordException
                    recordErrorMap.put(recordError.batchIndex, new InvalidRecordException(errorMessage));
                }
            }

            // 创建一个函数来获取每个记录的异常
            Function<Integer, RuntimeException> recordExceptions = batchIndex -> {
                RuntimeException exception = recordErrorMap.get(batchIndex);
                if (exception != null) {
                    return exception;
                } else {
                    // 如果响应包含记录错误，那么验证失败的记录会出现在响应中
                    // 为了避免对其余记录造成混淆，返回一个通用异常
                    return new KafkaException("Failed to append record because it was part of a batch " +
                        "which had one more more invalid records");
                }
            };

            // 使用记录级别的异常处理批次失败
            failBatch(batch, topLevelException, recordExceptions, adjustSequenceNumbers);
        }
    }

    /**
     * 处理消息批次的失败情况，将同一个异常应用于批次中的所有记录
     * 
     * @param batch 需要处理失败的消息批次
     * @param topLevelException 导致批次失败的顶层异常
     * @param adjustSequenceNumbers 是否需要调整序列号(用于事务场景)
     */
    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        boolean adjustSequenceNumbers
    ) {
        // 调用重载方法，将同一个异常应用于批次中的所有记录
        failBatch(batch, topLevelException, batchIndex -> topLevelException, adjustSequenceNumbers);
    }

    /**
     * 处理消息批次的失败情况，可以为批次中的每条记录指定不同的异常
     * 
     * @param batch 需要处理失败的消息批次
     * @param topLevelException 导致批次失败的顶层异常
     * @param recordExceptions 一个函数，根据记录索引返回对应的异常
     * @param adjustSequenceNumbers 是否需要调整序列号(用于事务场景)
     */
    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions,
        boolean adjustSequenceNumbers
    ) {
        // 记录错误指标，包括主题和失败的记录数
        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        // 将批次标记为异常完成，如果成功标记则进行后续处理
        if (batch.completeExceptionally(topLevelException, recordExceptions)) {
            // 如果启用了事务，需要通过事务管理器处理失败的批次
            if (transactionManager != null) {
                try {
                    // 调用事务管理器处理失败的批次，可能会抛出状态转换异常
                    // 捕获异常以避免影响其他逻辑的执行
                    transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers);
                } catch (Exception e) {
                    log.debug("Encountered error when transaction manager was handling a failed batch", e);
                }
            }
            // 移除批次并释放其占用的内存资源
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    /**
     * 判断一个失败的消息批次是否可以重试发送
     * 
     * 满足以下条件时可以重试:
     * 1. 批次未超过投递超时时间
     * 2. 重试次数未超过最大限制
     * 3. 批次未完成(未成功发送也未被标记为失败)
     * 4. 错误是可重试的:
     *    - 如果未启用事务，错误必须是RetriableException类型
     *    - 如果启用了事务，由事务管理器判断是否可重试
     * 
     * 注意：对于序列号不连续(OutOfOrderSequence)的错误也可以重试，因为如果第一个批次
     * 失败了，后续批次必定会因为序列号不连续而失败
     * 
     * @param batch 要判断的消息批次
     * @param response broker返回的分区级别响应
     * @param now 当前时间戳(毫秒)
     * @return true表示可以重试，false表示不能重试
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) && // 检查是否超时
            batch.attempts() < this.retries && // 检查重试次数
            !batch.isDone() && // 检查批次状态
            (transactionManager == null ?
                    response.error.exception() instanceof RetriableException : // 非事务模式下检查错误类型
                    transactionManager.canRetry(response, batch)); // 事务模式下由事务管理器判断
    }

    /**
     * 将按节点分组的消息批次转换为生产请求并发送
     * 
     * @param collated 按节点ID分组的消息批次映射，Map<节点ID, 该节点的批次列表>
     * @param now 当前时间戳(毫秒)
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        // 遍历每个节点的批次列表，为每个节点创建并发送一个生产请求
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * 根据给定的消息批次创建并发送生产请求
     * 
     * @param now 当前时间戳(毫秒)
     * @param destination 目标节点ID
     * @param acks 消息确认机制(0:不等待确认, 1:等待leader确认, -1:等待所有ISR确认)
     * @param timeout 请求超时时间(毫秒)
     * @param batches 要发送的消息批次列表
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        // 如果批次列表为空，直接返回
        if (batches.isEmpty())
            return;

        // 创建一个映射，用于在收到响应时快速定位批次
        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());
        // 创建请求数据集合，用于存储每个主题的生产数据
        ProduceRequestData.TopicProduceDataCollection tpd = new ProduceRequestData.TopicProduceDataCollection();
        
        // 遍历所有批次，按主题分区组织数据
        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();
            // 查找或创建主题的生产数据
            ProduceRequestData.TopicProduceData tpData = tpd.find(tp.topic());
            if (tpData == null) {
                tpData = new ProduceRequestData.TopicProduceData().setName(tp.topic());
                tpd.add(tpData);
            }
            // 添加分区的生产数据
            tpData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                    .setIndex(tp.partition())
                    .setRecords(records));
            // 保存批次引用，用于后续处理响应
            recordsByPartition.put(tp, batch);
        }

        // 处理事务相关的参数
        String transactionalId = null;
        boolean useTransactionV1Version = false;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
            // 根据事务管理器的配置决定使用哪个版本的事务协议
            useTransactionV1Version = !transactionManager.isTransactionV2Enabled();
        }

        // 创建生产请求构建器
        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(
                new ProduceRequestData()
                        .setAcks(acks) // 设置确认级别
                        .setTimeoutMs(timeout) // 设置超时时间
                        .setTransactionalId(transactionalId) // 设置事务ID
                        .setTopicData(tpd), // 设置主题数据
                useTransactionV1Version // 指定事务协议版本
        );
        
        // 创建响应处理回调
        RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());

        // 创建并发送客户端请求
        String nodeId = Integer.toString(destination);
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * Sender的度量指标收集器，负责收集和管理生产者的各种性能指标
     */
    private static class SenderMetrics {
        /* 记录消息重试次数的传感器 */
        public final Sensor retrySensor;
        /* 记录错误发生次数的传感器 */
        public final Sensor errorSensor;
        /* 记录消息在队列中等待时间的传感器 */
        public final Sensor queueTimeSensor;
        /* 记录请求响应时间的传感器 */
        public final Sensor requestTimeSensor;
        /* 记录每个请求包含的消息数量的传感器 */
        public final Sensor recordsPerRequestSensor;
        /* 记录消息批次大小的传感器 */
        public final Sensor batchSizeSensor;
        /* 记录消息压缩比率的传感器 */
        public final Sensor compressionRateSensor;
        /* 记录单条消息最大大小的传感器 */
        public final Sensor maxRecordSizeSensor;
        /* 记录批次分裂次数的传感器 */
        public final Sensor batchSplitSensor;
        /* 度量指标注册表，用于管理所有指标 */
        private final SenderMetricsRegistry metrics;
        /* 用于获取系统时间的实例 */
        private final Time time;

        /**
         * 创建一个新的SenderMetrics实例
         * 
         * @param metrics 度量指标注册表
         * @param metadata 集群元数据
         * @param client Kafka网络客户端
         * @param time 时间实例
         */
        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            // 初始化批次大小传感器，记录平均值和最大值
            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            // 初始化压缩率传感器，记录平均压缩比率
            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            // 初始化队列时间传感器，记录消息在累加器中的等待时间
            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            // 初始化请求时间传感器，记录请求的延迟情况
            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            // 初始化每请求记录数传感器，记录发送速率和平均批次大小
            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            // 初始化重试传感器，记录消息重试的速率和总次数
            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            // 初始化错误传感器，记录错误发生的速率和总次数
            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            // 初始化记录大小传感器，记录单条消息的大小统计
            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            // 添加在途请求数量指标
            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            // 添加元数据年龄指标（上次更新到现在的秒数）
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            // 初始化批次分裂传感器，记录因大小超限需要分裂的情况
            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        /**
         * 为指定主题注册度量指标。如果该主题的指标尚未注册，则创建以下指标：
         * 1. 每批次记录数
         * 2. 字节发送速率
         * 3. 压缩比率
         * 4. 重试次数
         * 5. 错误次数
         * 
         * @param topic 需要注册度量指标的主题名称
         */
        private void maybeRegisterTopicMetrics(String topic) {
            // 如果主题已注册了任一指标，则说明所有指标都已注册
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                // 创建主题标签，用于标识指标所属主题
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                // 注册每批次记录数指标
                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                // 注册字节发送速率指标
                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                // 注册压缩比率指标
                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                // 注册重试次数指标
                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                // 注册错误次数指标
                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        /**
         * 更新生产请求相关的度量指标，包括全局指标和每个主题的指标
         * 
         * @param batches Map<节点ID, 该节点要发送的消息批次列表>
         */
        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            // 遍历每个节点的批次列表
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0; // 记录该节点的总消息数
                for (ProducerBatch batch : nodeBatch) {
                    // 获取批次所属的主题，并确保该主题的指标已注册
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // 更新主题级别的每批次记录数指标
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // 更新主题级别的字节发送速率指标
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // 更新主题级别的压缩比率指标
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // 更新全局指标
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);  // 批次大小
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);           // 队列等待时间
                    this.compressionRateSensor.record(batch.compressionRatio());     // 压缩比率
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);       // 最大记录大小
                    records += batch.recordCount;                                     // 累加记录数
                }
                // 更新每个请求的记录数指标
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        /**
         * 记录消息重试次数，包括全局重试计数和特定主题的重试计数
         * 
         * @param topic 发生重试的主题
         * @param count 重试次数
         */
        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            // 更新全局重试计数
            this.retrySensor.record(count, now);
            // 更新主题级别的重试计数
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        /**
         * 记录错误发生次数，包括全局错误计数和特定主题的错误计数
         * 
         * @param topic 发生错误的主题
         * @param count 错误次数
         */
        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            // 更新全局错误计数
            this.errorSensor.record(count, now);
            // 更新主题级别的错误计数
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        /**
         * 记录请求延迟时间，包括全局延迟统计和特定节点的延迟统计
         * 
         * @param node 目标节点的标识符
         * @param latency 延迟时间(毫秒)
         */
        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            // 更新全局请求延迟统计
            this.requestTimeSensor.record(latency, now);
            // 如果指定了节点，更新该节点的延迟统计
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        /**
         * 记录批次分裂事件，当消息批次因大小超限需要分裂时调用
         */
        void recordBatchSplit() {
            // 更新批次分裂计数
            this.batchSplitSensor.record();
        }
    }

}
