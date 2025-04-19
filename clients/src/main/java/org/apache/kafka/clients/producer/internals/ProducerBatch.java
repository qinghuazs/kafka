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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * 一个将要被发送或正在发送的消息批次。
 * 这个批次包含了多条待发送的消息记录，可以进行压缩和事务处理。
 *
 * 该类不是线程安全的，在修改时必须使用外部同步机制
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    // 批次的最终状态枚举：已中止、已失败、已成功
    private enum FinalState { ABORTED, FAILED, SUCCEEDED }

    // 批次创建时的时间戳(毫秒)
    final long createdMs;
    // 该批次要发送到的主题分区
    final TopicPartition topicPartition;
    // 生产请求的结果Future，用于异步获取发送结果
    final ProduceRequestResult produceFuture;

    // 保存每条消息的回调和对应的Future
    private final List<Thunk> thunks = new ArrayList<>();
    // 内存中的消息记录构建器，用于追加和构建消息
    private final MemoryRecordsBuilder recordsBuilder;
    // 发送尝试次数计数器
    private final AtomicInteger attempts = new AtomicInteger(0);
    // 标记该批次是否是通过分割大批次得到的
    private final boolean isSplitBatch;
    // 批次的最终状态引用，初始为null
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);

    // 批次中的消息记录数量
    int recordCount;
    // 批次中最大消息的大小(字节)
    int maxRecordSize;
    // 最后一次发送尝试的时间戳
    private long lastAttemptMs;
    // 最后一次追加消息的时间戳
    private long lastAppendTime;
    // 批次被排空(全部发送完成)的时间戳
    private long drainedMs;
    // 标记该批次是否处于重试状态
    private boolean retry;
    // 标记该批次是否被重新打开
    private boolean reopened;

    // 跟踪当前要发送该批次的leader的epoch值
    private OptionalInt currentLeaderEpoch;
    // 跟踪leader首次更改为currentLeaderEpoch时的尝试次数
    private int attemptsWhenLeaderLastChanged;

    /**
     * 创建一个新的生产者批次
     * 
     * @param tp 目标主题分区
     * @param recordsBuilder 内存记录构建器
     * @param createdMs 创建时间戳
     */
    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    /**
     * 创建一个新的生产者批次
     * 
     * @param tp 目标主题分区
     * @param recordsBuilder 内存记录构建器
     * @param createdMs 创建时间戳
     * @param isSplitBatch 是否是通过分割大批次得到的子批次
     */
    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        // 初始化创建时间
        this.createdMs = createdMs;
        // 设置最后尝试时间为创建时间
        this.lastAttemptMs = createdMs;
        // 设置记录构建器
        this.recordsBuilder = recordsBuilder;
        // 设置目标主题分区
        this.topicPartition = tp;
        // 设置最后追加时间为创建时间
        this.lastAppendTime = createdMs;
        // 创建生产请求结果Future
        this.produceFuture = new ProduceRequestResult(topicPartition);
        // 初始化重试标志为false
        this.retry = false;
        // 设置分割批次标志
        this.isSplitBatch = isSplitBatch;
        // 估算压缩比率
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compression().type());
        // 初始化leader epoch为空
        this.currentLeaderEpoch = OptionalInt.empty();
        // 初始化leader变更时的尝试次数为0
        this.attemptsWhenLeaderLastChanged = 0;
        // 设置预估的压缩比率
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * 如果发现有更新的leader，则更新此批次将要发送到的leader的epoch
     * 
     * @param latestLeaderEpoch 最新的leader epoch值
     */
    void maybeUpdateLeaderEpoch(OptionalInt latestLeaderEpoch) {
        // 如果最新的leader epoch存在，且当前leader epoch为空或小于最新的epoch
        if (latestLeaderEpoch.isPresent()
            && (currentLeaderEpoch.isEmpty() || currentLeaderEpoch.getAsInt() < latestLeaderEpoch.getAsInt())) {
            // 记录更新leader的日志
            log.trace("For {}, leader will be updated, currentLeaderEpoch: {}, attemptsWhenLeaderLastChanged:{}, latestLeaderEpoch: {}, current attempt: {}",
                this, currentLeaderEpoch, attemptsWhenLeaderLastChanged, latestLeaderEpoch, attempts);
            // 记录leader变更时的尝试次数
            attemptsWhenLeaderLastChanged = attempts();
            // 更新当前leader epoch
            currentLeaderEpoch = latestLeaderEpoch;
        } else {
            // 记录未更新leader的日志
            log.trace("For {}, leader wasn't updated, currentLeaderEpoch: {}, attemptsWhenLeaderLastChanged:{}, latestLeaderEpoch: {}, current attempt: {}",
                this, currentLeaderEpoch, attemptsWhenLeaderLastChanged, latestLeaderEpoch, attempts);
        }
    }

    /**
     * 判断当前重试是否发送到了一个新的leader
     * 
     * @return 如果批次正在重试且将发送到新的leader则返回true
     */
    boolean hasLeaderChangedForTheOngoingRetry() {
        // 获取当前尝试次数
        int attempts = attempts();
        // 判断是否处于重试状态(尝试次数>=1)
        boolean isRetry = attempts >= 1;
        if (!isRetry)
            return false;
        // 如果当前尝试次数等于leader最后变更时的尝试次数，说明这次重试会发送到新leader
        return attempts == attemptsWhenLeaderLastChanged;
    }


    /**
     * 尝试将一条消息记录追加到当前批次中，并返回该记录在批次内的相对偏移量
     *
     * @param timestamp 消息时间戳
     * @param key 消息键
     * @param value 消息值
     * @param headers 消息头部
     * @param callback 发送完成回调
     * @param now 当前时间戳
     * @return 消息的Future元数据，如果批次空间不足则返回null
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 检查批次是否有足够空间容纳新消息
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            // 追加消息到记录构建器
            this.recordsBuilder.append(timestamp, key, value, headers);
            // 更新批次中最大消息大小： 最大记录大小和新消息的大小 estimateSizeInBytesUpperBound得出的是一个预估值
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compression().type(), key, value, headers));
            // 更新最后追加时间
            this.lastAppendTime = now;
            // 创建消息的Future元数据
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length,
                                                                   Time.SYSTEM);
            // 保存回调和Future，以便在批次需要分割时能够正确处理
            thunks.add(new Thunk(callback, future));
            // 增加记录计数
            this.recordCount++;
            return future;
        }
    }

    /**
     * 该方法仅在分割大批次为小批次时使用，用于将记录追加到分割后的新批次中
     * 
     * @param timestamp 消息时间戳
     * @param key 消息键
     * @param value 消息值
     * @param headers 消息头部
     * @param thunk 原始批次中的回调和Future
     * @return 如果追加成功返回true，空间不足返回false
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        // 检查新批次是否有足够空间
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // 追加消息到新批次，无需计算CRC
            this.recordsBuilder.append(timestamp, key, value, headers);
            // 更新最大消息大小
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compression().type(), key, value, headers));
            // 创建新的Future元数据
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp,
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining(),
                                                                   Time.SYSTEM);
            // 将新Future链接到原始thunk的Future
            thunk.future.chain(future);
            // 保存thunk
            this.thunks.add(thunk);
            // 增加记录计数
            this.recordCount++;
            return true;
        }
    }

    /**
     * 中止批次并完成Future和回调
     * 
     * @param exception 用于完成Future和回调的异常
     */
    public void abort(RuntimeException exception) {
        // 尝试将状态设置为ABORTED，如果批次已经完成则抛出异常
        if (!finalState.compareAndSet(null, FinalState.ABORTED))
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

        // 记录中止日志
        log.trace("Aborting batch for partition {}", topicPartition, exception);
        // 使用无效偏移量和异常完成Future和回调
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, index -> exception);
    }

    /**
     * 检查批次是否已完成(成功或异常)
     * 
     * @return 如果批次已完成返回true，否则返回false
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     * 成功完成批次
     * 
     * @param baseOffset 服务器分配的消息基准偏移量
     * @param logAppendTime 日志追加时间，如果使用CreateTime则为-1
     * @return 如果此次调用导致批次完成则返回true，如果批次之前已完成则返回false
     */
    public boolean complete(long baseOffset, long logAppendTime) {
        return done(baseOffset, logAppendTime, null, null);
    }

    /**
     * 异常完成批次。提供的顶层异常将用于批次中的每个记录Future
     *
     * @param topLevelException 顶层分区错误
     * @param recordExceptions 记录异常函数，将批次索引映射到对应的记录异常
     * @return 如果此次调用导致批次完成则返回true，如果批次之前已完成则返回false
     */
    public boolean completeExceptionally(
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        Objects.requireNonNull(topLevelException);
        Objects.requireNonNull(recordExceptions);
        return done(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, topLevelException, recordExceptions);
    }

    /**
     * 确定批次的最终状态。最终状态一旦设置就不可变。这个函数可能会在一个批次上被调用一次或两次。会被调用两次的情况：
     * 1. 正在传输的批次在收到broker响应之前超时。批次的最终状态被设置为FAILED。但它可能在broker上成功，
     *    第二次调用batch.done()可能会尝试设置SUCCEEDED最终状态。
     * 2. 如果发生事务中止或生产者被强制关闭，最终状态是ABORTED，但如果broker返回成功，它仍可能成功。
     *
     * 从[FAILED | ABORTED]到SUCCEEDED的状态转换尝试会被记录。
     * 从一个失败状态到相同或不同失败状态的转换尝试会被忽略。
     * 从SUCCEEDED到相同或失败状态的转换尝试会抛出异常。
     *
     * @param baseOffset 服务器分配的消息基准偏移量
     * @param logAppendTime 日志追加时间，如果使用CreateTime则为-1
     * @param topLevelException 发生的异常(如果请求成功则为null)
     * @param recordExceptions 记录异常函数，将批次索引映射到对应的记录异常
     * @return 如果批次成功完成返回true，如果批次之前已被中止返回false
     */
    private boolean done(
        long baseOffset,
        long logAppendTime,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        // 根据是否有异常确定尝试设置的最终状态
        final FinalState tryFinalState = (topLevelException == null) ? FinalState.SUCCEEDED : FinalState.FAILED;
        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, topLevelException);
        }

        // 尝试将状态从null设置为新状态
        if (this.finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, recordExceptions);
            return true;
        }

        // 处理状态转换
        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // 记录之前失败的批次后来成功的情况
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                    tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // 忽略FAILED->FAILED和ABORTED->FAILED的状态转换
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                    this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            // 成功的批次不允许进行任何状态转换
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    /**
     * 完成Future并执行所有回调
     * 
     * @param baseOffset 基准偏移量
     * @param logAppendTime 日志追加时间
     * @param recordExceptions 记录异常函数
     */
    private void completeFutureAndFireCallbacks(
        long baseOffset,
        long logAppendTime,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        // 在执行回调之前设置Future的值，因为回调的onCompletion调用依赖于Future的状态
        produceFuture.set(baseOffset, logAppendTime, recordExceptions);

        // 执行所有回调
        for (int i = 0; i < thunks.size(); i++) {
            try {
                Thunk thunk = thunks.get(i);
                if (thunk.callback != null) {
                    if (recordExceptions == null) {
                        // 成功情况：传递元数据
                        RecordMetadata metadata = thunk.future.value();
                        thunk.callback.onCompletion(metadata, null);
                    } else {
                        // 失败情况：传递异常
                        RuntimeException exception = recordExceptions.apply(i);
                        thunk.callback.onCompletion(null, exception);
                    }
                }
            } catch (Exception e) {
                // 记录回调执行错误但不抛出异常
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        // 标记Future为完成状态
        produceFuture.done();
    }

    /**
     * 将当前批次分割成多个较小的批次
     * 这个方法用于处理过大的批次，将其分割成多个符合大小限制的小批次
     * 
     * @param splitBatchSize 分割后每个批次的目标大小
     * @return 分割后的批次队列
     * @throws IllegalStateException 如果试图分割空批次
     * @throws IllegalArgumentException 如果批次格式不支持分割或存在多个记录批次
     */
    public Deque<ProducerBatch> split(int splitBatchSize) {
        // 创建一个双端队列来存储分割后的批次
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        // 构建内存中的记录集合
        MemoryRecords memoryRecords = recordsBuilder.build();

        // 获取记录批次的迭代器
        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        // 如果批次为空，抛出异常
        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");

        // 获取第一个记录批次
        RecordBatch recordBatch = recordBatchIter.next();
        // 检查批次格式版本和压缩状态
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");

        // 确保只有一个记录批次
        if (recordBatchIter.hasNext())
            throw new IllegalArgumentException("A producer batch should only have one record batch.");

        // 获取回调和Future的迭代器
        Iterator<Thunk> thunkIter = thunks.iterator();
        // 当前正在处理的批次，初始为null
        ProducerBatch batch = null;

        // 遍历原始批次中的所有记录
        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            // 获取对应的回调和Future
            Thunk thunk = thunkIter.next();
            // 如果还没有创建批次，创建一个新的
            if (batch == null)
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

            // 尝试将记录追加到当前批次
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                // 如果追加失败（批次已满），将当前批次添加到队列
                batches.add(batch);
                batch.closeForRecordAppends();
                // 创建新批次并追加当前记录
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // 处理最后一个批次
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        // 设置原始批次的Future为失败状态
        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, index -> new RecordBatchTooLargeException());
        produceFuture.done();

        // 如果原始批次有序列号，为新批次设置序列号
        if (hasSequence()) {
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            // 为每个新批次设置生产者状态和序列号
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                // 更新序列号，为下一个批次准备
                sequence += newBatch.recordCount;
            }
        }
        return batches;
    }

    /**
     * 为指定的记录创建一个新的生产者批次
     * 
     * @param record 需要存储的记录
     * @param batchSize 批次的目标大小
     * @return 新创建的生产者批次
     */
    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        // 计算初始缓冲区大小，取记录估算大小和目标批次大小的较大值
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compression().type(), record.key(), record.value(), record.headers()), batchSize);
        // 分配缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // 注意：我们故意不设置生产者状态（producerId, epoch, sequence和isTransactional）
        // 这些状态将在批次被出队准备发送时设置，这与普通批次的处理方式一致
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compression(),
                TimestampType.CREATE_TIME, 0L);
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    /**
     * 检查当前批次是否使用了压缩
     * 
     * @return 如果批次使用了压缩则返回true
     */
    public boolean isCompressed() {
        return recordsBuilder.compression().type() != CompressionType.NONE;
    }

    /**
     * 内部类，用于存储消息的回调函数和对应的Future
     * 每条消息都会有一个对应的Thunk对象，用于在发送完成时执行回调
     */
    private static final class Thunk {
        // 消息发送完成时要执行的回调函数
        final Callback callback;
        // 用于获取发送结果的Future对象
        final FutureRecordMetadata future;

        /**
         * 创建一个新的Thunk对象
         * 
         * @param callback 回调函数
         * @param future Future对象
         */
        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * 检查批次是否已达到传递超时时间
     * 
     * @param deliveryTimeoutMs 传递超时时间(毫秒)
     * @param now 当前时间戳
     * @return 如果批次已超时返回true，否则返回false
     */
    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        // 计算从创建到现在的时间是否超过了超时时限
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    /**
     * 获取批次的最终状态
     * 
     * @return 批次的最终状态(ABORTED、FAILED或SUCCEEDED)
     */
    public FinalState finalState() {
        // 返回原子引用中存储的最终状态
        return this.finalState.get();
    }

    /**
     * 获取批次的发送尝试次数
     * 
     * @return 当前的尝试次数
     */
    int attempts() {
        // 返回原子计数器中的尝试次数
        return attempts.get();
    }

    /**
     * 当批次被重新入队时调用此方法，更新相关状态
     * 
     * @param now 当前时间戳
     */
    void reenqueued(long now) {
        // 增加尝试次数
        attempts.getAndIncrement();
        // 更新最后尝试时间为最后追加时间和当前时间的较大值
        lastAttemptMs = Math.max(lastAppendTime, now);
        // 更新最后追加时间为最后追加时间和当前时间的较大值
        lastAppendTime = Math.max(lastAppendTime, now);
        // 标记批次为重试状态
        retry = true;
    }

    /**
     * 获取批次在队列中的等待时间
     * 
     * @return 批次在队列中等待的毫秒数
     */
    long queueTimeMs() {
        // 返回从创建到被排空的时间差
        return drainedMs - createdMs;
    }

    /**
     * 获取自上次尝试以来等待的时间
     * 
     * @param nowMs 当前时间戳
     * @return 等待的毫秒数，如果小于0则返回0
     */
    long waitedTimeMs(long nowMs) {
        // 返回从上次尝试到现在的时间差，最小为0
        return Math.max(0, nowMs - lastAttemptMs);
    }

    /**
     * 标记批次已被排空，更新排空时间戳
     * 
     * @param nowMs 当前时间戳
     */
    void drained(long nowMs) {
        // 更新排空时间为当前排空时间和传入时间的较大值
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    /**
     * 检查该批次是否是通过分割大批次得到的
     * 
     * @return 如果是分割得到的批次返回true
     */
    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * 检查批次是否处于重试状态
     * 
     * @return 如果批次正在重试返回true
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * 构建并返回内存中的记录集合
     * 
     * @return 记录集合对象
     */
    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    /**
     * 估算批次的大小(字节)
     * 
     * @return 估算的字节数
     */
    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    /**
     * 获取批次的压缩比率
     * 
     * @return 压缩比率
     */
    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    /**
     * 检查批次是否已满
     * 
     * @return 如果批次已满返回true
     */
    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    /**
     * 设置生产者状态，包括生产者ID、epoch和事务标记
     * 
     * @param producerIdAndEpoch 生产者ID和epoch信息
     * @param baseSequence 基准序列号
     * @param isTransactional 是否是事务性的
     */
    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        // 设置记录构建器的生产者状态
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * 重置生产者状态，用于重新打开批次时
     * 
     * @param producerIdAndEpoch 生产者ID和epoch信息
     * @param baseSequence 新的基准序列号
     */
    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence) {
        // 记录序列号重置的日志
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        // 标记批次已重新打开
        reopened = true;
        // 重新打开记录构建器并重写生产者状态
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional());
    }

    /**
     * 释放记录追加所需的资源(如压缩缓冲区)
     * 调用此方法后只能更新RecordBatch的头部信息
     */
    public void closeForRecordAppends() {
        // 关闭记录构建器的追加功能
        recordsBuilder.closeForRecordAppends();
    }

    /**
     * 完全关闭批次，更新压缩比率估算并重置状态
     */
    public void close() {
        // 关闭记录构建器
        recordsBuilder.close();
        // 如果不是控制批次，更新压缩比率估算
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compression().type(),
                                                       (float) recordsBuilder.compressionRatio());
        }
        // 重置重新打开标志
        reopened = false;
    }

    /**
     * 中止记录构建器并重置底层缓冲区的状态
     * 在调用abort()之前使用，确保之前追加的记录不能被读取
     * 用于需要确保批次最终被中止但不能安全调用完成回调的场景
     * (例如在持有锁时中止批次，如在RecordAccumulator中)
     */
    public void abortRecordAppends() {
        // 中止记录构建器
        recordsBuilder.abort();
    }

    /**
     * 检查批次是否已关闭
     * 
     * @return 如果批次已关闭返回true
     */
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    /**
     * 获取底层的字节缓冲区
     * 
     * @return 字节缓冲区
     */
    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    /**
     * 获取批次的初始容量
     * 
     * @return 初始容量(字节)
     */
    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    /**
     * 检查批次是否可写
     * 
     * @return 如果批次未关闭返回true
     */
    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    /**
     * 获取批次的魔数(版本标识)
     * 
     * @return 魔数值
     */
    public byte magic() {
        return recordsBuilder.magic();
    }

    /**
     * 获取生产者ID
     * 
     * @return 生产者ID
     */
    public long producerId() {
        return recordsBuilder.producerId();
    }

    /**
     * 获取生产者epoch
     * 
     * @return 生产者epoch
     */
    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    /**
     * 获取基准序列号
     * 
     * @return 基准序列号
     */
    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    /**
     * 获取最后一条记录的序列号
     * 
     * @return 最后的序列号
     */
    public int lastSequence() {
        // 基准序列号加上记录数减1得到最后序列号
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    /**
     * 检查批次是否有序列号
     * 
     * @return 如果有序列号返回true
     */
    public boolean hasSequence() {
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    /**
     * 检查批次是否是事务性的
     * 
     * @return 如果是事务性批次返回true
     */
    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    /**
     * 检查序列号是否已被重置
     * 
     * @return 如果序列号已重置返回true
     */
    public boolean sequenceHasBeenReset() {
        return reopened;
    }

    /**
     * 获取当前leader的epoch值(仅用于测试)
     * 
     * @return 当前leader epoch
     */
    OptionalInt currentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    /**
     * 获取leader最后变更时的尝试次数(仅用于测试)
     * 
     * @return leader最后变更时的尝试次数
     */
    int attemptsWhenLeaderLastChanged() {
        return attemptsWhenLeaderLastChanged;
    }
}
