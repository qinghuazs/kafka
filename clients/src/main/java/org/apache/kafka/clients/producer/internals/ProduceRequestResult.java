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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 该类用于模拟单个分区的生产请求的未来完成状态。每个分区的生产请求都会对应一个实例，
 * 并且这个实例会被同一分区中所有批量处理在一起的{@link RecordMetadata}实例共享。
 * 
 * 主要功能：
 * 1. 跟踪生产请求的完成状态
 * 2. 管理请求的基准偏移量和日志追加时间
 * 3. 处理请求过程中可能出现的错误
 */
public class ProduceRequestResult {

    // 使用CountDownLatch实现请求完成的等待机制，初始值为1
    private final CountDownLatch latch = new CountDownLatch(1);
    // 存储目标主题分区信息
    private final TopicPartition topicPartition;

    // 记录消息集合的基准偏移量，使用volatile确保多线程可见性
    private volatile Long baseOffset = null;
    // 记录消息的日志追加时间，-1表示使用创建时间
    private volatile long logAppendTime = RecordBatch.NO_TIMESTAMP;
    // 存储批次索引到异常的映射函数，用于错误处理
    private volatile Function<Integer, RuntimeException> errorsByIndex;

    /**
     * 创建ProduceRequestResult实例
     *
     * @param topicPartition 记录集合要发送到的目标主题分区
     */
    public ProduceRequestResult(TopicPartition topicPartition) {
        // 初始化目标主题分区
        this.topicPartition = topicPartition;
    }

    /**
     * 设置生产请求的结果
     *
     * @param baseOffset 分配给记录的基准偏移量
     * @param logAppendTime 日志追加时间，如果使用创建时间则为-1
     * @param errorsByIndex 批次索引到异常的映射函数，如果请求成功则为null
     */
    public void set(long baseOffset, long logAppendTime, Function<Integer, RuntimeException> errorsByIndex) {
        // 设置基准偏移量
        this.baseOffset = baseOffset;
        // 设置日志追加时间
        this.logAppendTime = logAppendTime;
        // 设置错误映射函数
        this.errorsByIndex = errorsByIndex;
    }

    /**
     * 标记请求完成并解除所有等待完成的线程的阻塞状态
     */
    public void done() {
        // 检查是否已设置基准偏移量，确保set方法已被调用
        if (baseOffset == null)
            throw new IllegalStateException("The method `set` must be invoked before this method.");
        // 计数器减1，释放所有等待的线程
        this.latch.countDown();
    }

    /**
     * 等待请求完成，会一直阻塞直到请求完成
     */
    public void await() throws InterruptedException {
        // 等待计数器变为0
        latch.await();
    }

    /**
     * 等待请求完成，但最多等待指定的时间
     * @param timeout 最大等待时间
     * @param unit 时间单位
     * @return 如果请求在超时前完成返回true，如果超时返回false
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        // 带超时的等待
        return latch.await(timeout, unit);
    }

    /**
     * 获取请求的基准偏移量（记录集合中的第一个偏移量）
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * 判断是否使用了日志追加时间
     * @return 如果使用了日志追加时间返回true，如果使用创建时间返回false
     */
    public boolean hasLogAppendTime() {
        return logAppendTime != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * 获取日志追加时间，如果使用创建时间则返回-1
     */
    public long logAppendTime() {
        return logAppendTime;
    }

    /**
     * 获取处理请求时抛出的错误（通常是服务器端的错误）
     * @param batchIndex 批次索引
     * @return 如果指定批次索引有错误则返回对应的异常，否则返回null
     */
    public RuntimeException error(int batchIndex) {
        // 如果没有错误映射函数，表示请求成功
        if (errorsByIndex == null) {
            return null;
        } else {
            // 根据批次索引获取对应的异常
            return errorsByIndex.apply(batchIndex);
        }
    }

    /**
     * 获取记录追加到的目标主题分区
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * 检查请求是否已完成
     * @return 如果请求已完成返回true，否则返回false
     */
    public boolean completed() {
        // 通过检查计数器是否为0来判断请求是否完成
        return this.latch.getCount() == 0L;
    }
}
