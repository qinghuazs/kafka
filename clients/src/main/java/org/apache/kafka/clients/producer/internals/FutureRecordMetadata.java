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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 消息发送的Future结果对象
 * 该类实现了Future接口，用于异步获取消息发送的结果。当生产者发送消息时，会立即返回这个对象，
 * 应用程序可以通过该对象来检查发送是否完成，或者等待发送完成并获取结果。
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    // 生产请求的结果，包含了消息发送的状态和元数据信息
    private final ProduceRequestResult result;
    // 消息在批次中的索引位置
    private final int batchIndex;
    // 消息创建的时间戳
    private final long createTimestamp;
    // 序列化后的key大小（字节数）
    private final int serializedKeySize;
    // 序列化后的value大小（字节数）
    private final int serializedValueSize;
    // 时间工具类，用于处理超时等时间相关的操作
    private final Time time;
    // 链式元数据的下一个节点，用于处理批次分割场景
    // volatile保证多线程可见性
    private volatile FutureRecordMetadata nextRecordMetadata = null;

    /**
     * 构造函数
     * @param result 生产请求的结果对象
     * @param batchIndex 消息在批次中的索引
     * @param createTimestamp 消息创建时间戳
     * @param serializedKeySize 序列化后的key大小
     * @param serializedValueSize 序列化后的value大小
     * @param time 时间工具类实例
     */
    public FutureRecordMetadata(ProduceRequestResult result, int batchIndex, long createTimestamp, int serializedKeySize,
                                int serializedValueSize, Time time) {
        this.result = result;
        this.batchIndex = batchIndex;
        this.createTimestamp = createTimestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.time = time;
    }

    /**
     * 取消操作实现
     * Kafka不支持取消已发送的消息，因此始终返回false
     */
    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    /**
     * 检查是否被取消
     * 由于不支持取消操作，始终返回false
     */
    @Override
    public boolean isCancelled() {
        return false;
    }

    /**
     * 获取发送结果，会一直阻塞直到发送完成或发生异常
     * 如果存在链式元数据（批次分割的场景），则递归获取最终结果
     */
    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        if (nextRecordMetadata != null)
            return nextRecordMetadata.get();
        return valueOrError();
    }

    /**
     * 带超时的获取发送结果
     * @param timeout 超时时间
     * @param unit 时间单位
     * @throws TimeoutException 如果在指定时间内没有得到结果
     */
    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // Handle overflow.
        long now = time.milliseconds();
        long timeoutMillis = unit.toMillis(timeout);
        long deadline = Long.MAX_VALUE - timeoutMillis < now ? Long.MAX_VALUE : now + timeoutMillis;
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.");
        if (nextRecordMetadata != null)
            return nextRecordMetadata.get(deadline - time.milliseconds(), TimeUnit.MILLISECONDS);
        return valueOrError();
    }

    /**
     * This method is used when we have to split a large batch in smaller ones. A chained metadata will allow the
     * future that has already returned to the users to wait on the newly created split batches even after the
     * old big batch has been deemed as done.
     */
    /**
     * 链接新的元数据对象
     * 当一个大的消息批次被分割成多个小批次时，通过链式结构保持对分割后批次的跟踪
     * 采用递归方式将新的元数据对象添加到链表末尾
     */
    void chain(FutureRecordMetadata futureRecordMetadata) {
        if (nextRecordMetadata == null)
            nextRecordMetadata = futureRecordMetadata;
        else
            nextRecordMetadata.chain(futureRecordMetadata);
    }

    /**
     * 获取结果值或抛出异常
     * 检查当前批次索引位置是否有错误，如有则抛出异常，否则返回结果
     */
    RecordMetadata valueOrError() throws ExecutionException {
        RuntimeException exception = this.result.error(batchIndex);
        if (exception != null)
            throw new ExecutionException(exception);
        else
            return value();
    }

    /**
     * 创建并返回发送结果的元数据
     * 如果存在链式元数据，则递归获取最终的元数据结果
     */
    RecordMetadata value() {
        if (nextRecordMetadata != null)
            return nextRecordMetadata.value();
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.batchIndex,
                                  timestamp(), this.serializedKeySize, this.serializedValueSize);
    }

    /**
     * 获取消息的时间戳
     * 如果broker配置了日志追加时间，则使用日志追加时间，否则使用消息创建时间
     */
    private long timestamp() {
        return result.hasLogAppendTime() ? result.logAppendTime() : createTimestamp;
    }

    /**
     * 检查发送是否完成
     * 如果存在链式元数据，则递归检查整个链上的完成状态
     */
    @Override
    public boolean isDone() {
        if (nextRecordMetadata != null)
            return nextRecordMetadata.isDone();
        return this.result.completed();
    }

}
