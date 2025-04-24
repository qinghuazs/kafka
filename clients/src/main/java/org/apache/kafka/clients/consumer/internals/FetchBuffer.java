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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * {@code FetchBuffer} 用于缓存从broker接收到的 {@link CompletedFetch 获取结果}。
 * 本质上是一个 {@link java.util.Queue} 队列的包装器，用于存储 {@link CompletedFetch} 对象。
 * 队列中每个分区最多只能有一个 {@link CompletedFetch} 对象。
 *
 * <p/>
 *
 * <em>注意</em>: 该类是线程安全的，设计意图是让 {@link CompletedFetch 数据} 由后台线程
 * "生产"，并由应用线程消费。这种生产者-消费者模式确保了数据的安全传递。
 */
public class FetchBuffer implements AutoCloseable {

    // 用于记录日志的Logger实例
    private final Logger log;
    // 存储已完成的获取请求的线程安全队列
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    // 用于同步访问的重入锁
    private final Lock lock;
    // 用于实现线程等待/通知机制的条件变量
    private final Condition notEmptyCondition;
    // 用于确保幂等关闭的工具类实例
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();

    // 用于标记是否被唤醒的原子布尔值
    private final AtomicBoolean wokenup = new AtomicBoolean(false);

    // 下一个要处理的获取结果
    private CompletedFetch nextInLineFetch;

    /**
     * 创建一个新的FetchBuffer实例
     * 
     * @param logContext 日志上下文对象，用于创建日志记录器
     */
    public FetchBuffer(final LogContext logContext) {
        // 初始化日志记录器
        this.log = logContext.logger(FetchBuffer.class);
        // 创建线程安全的队列用于存储已完成的获取请求
        this.completedFetches = new ConcurrentLinkedQueue<>();
        // 创建重入锁用于同步访问
        this.lock = new ReentrantLock();
        // 创建条件变量用于线程等待/通知机制
        this.notEmptyCondition = lock.newCondition();
    }

    /**
     * 检查缓冲区是否为空
     * 
     * @return 如果没有待返回给用户的已完成获取请求，返回{@code true}；否则返回{@code false}
     */
    boolean isEmpty() {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 检查队列是否为空
            return completedFetches.isEmpty();
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 检查是否有符合条件的已完成获取请求待返回给用户
     * 该方法是线程安全的，主要用于测试目的
     *
     * @param predicate 用于匹配CompletedFetch的断言条件
     * @return 如果存在符合断言条件的已完成获取请求，返回{@code true}；否则返回{@code false}
     */
    boolean hasCompletedFetches(Predicate<CompletedFetch> predicate) {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 使用Stream API检查是否有符合条件的元素
            return completedFetches.stream().anyMatch(predicate);
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 添加一个已完成的获取请求到缓冲区
     * 
     * @param completedFetch 要添加的已完成获取请求
     */
    void add(CompletedFetch completedFetch) {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 将获取请求添加到队列
            completedFetches.add(completedFetch);
            // 通知所有等待的线程有新数据到达
            notEmptyCondition.signalAll();
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 批量添加已完成的获取请求到缓冲区
     * 
     * @param completedFetches 要添加的已完成获取请求集合
     */
    void addAll(Collection<CompletedFetch> completedFetches) {
        // 如果集合为空或null，直接返回
        if (completedFetches == null || completedFetches.isEmpty())
            return;

        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 将所有获取请求添加到队列
            this.completedFetches.addAll(completedFetches);
            // 通知所有等待的线程有新数据到达
            notEmptyCondition.signalAll();
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 获取下一个要处理的获取请求
     * 
     * @return 下一个要处理的CompletedFetch对象
     */
    CompletedFetch nextInLineFetch() {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 返回下一个要处理的获取请求
            return nextInLineFetch;
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 设置下一个要处理的获取请求
     * 
     * @param nextInLineFetch 要设置的CompletedFetch对象
     */
    void setNextInLineFetch(CompletedFetch nextInLineFetch) {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 设置下一个要处理的获取请求
            this.nextInLineFetch = nextInLineFetch;
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 查看队列头部的获取请求，但不移除它
     * 
     * @return 队列头部的CompletedFetch对象，如果队列为空则返回null
     */
    CompletedFetch peek() {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 返回队列头部的元素但不移除
            return completedFetches.peek();
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 获取并移除队列头部的获取请求
     * 
     * @return 队列头部的CompletedFetch对象，如果队列为空则返回null
     */
    CompletedFetch poll() {
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 获取并移除队列头部的元素
            return completedFetches.poll();
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 允许调用者等待缓冲区中有数据。该方法会阻塞，直到以下条件之一满足才返回：
     *
     * <ol>
     *     <li>进入方法时缓冲区已经非空</li>
     *     <li>等待期间缓冲区被填充了数据</li>
     *     <li>{@link Timer 计时器}超时</li>
     *     <li>线程被中断</li>
     * </ol>
     *
     * @param timer 提供等待时间的计时器
     */
    void awaitNotEmpty(Timer timer) {
        try {
            // 获取锁以确保线程安全
            lock.lock();

            // 当缓冲区为空且没有被唤醒时，继续等待
            while (isEmpty() && !wokenup.compareAndSet(true, false)) {
                // 在进入循环前更新计时器，因为获取锁可能花费了一些时间
                timer.update();

                // 检查计时器是否已过期
                if (timer.isExpired()) {
                    // 如果在开始等待之前线程就被中断了，从KafkaConsumer.poll(Duration)合约的角度来看
                    // 这也算作中断。我们只需要在不打算等待时检查这一点，因为await操作本身会检查线程是否被中断
                    if (Thread.interrupted())
                        throw new InterruptException("等待获取记录的结果时被中断");

                    break;
                }

                // 等待指定的时间，如果超时返回false
                if (!notEmptyCondition.await(timer.remainingMs(), TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            throw new InterruptException("等待获取记录的结果时被中断", e);
        } finally {
            // 确保锁始终被释放
            lock.unlock();
            // 更新计时器
            timer.update();
        }
    }

    /**
     * 唤醒等待数据的消费者线程
     * 这个方法通常在需要中断长时间等待的消费者线程时使用，比如关闭消费者时
     */
    void wakeup() {
        // 设置唤醒标志
        wokenup.set(true);
        try {
            // 获取锁以确保线程安全
            lock.lock();
            // 唤醒所有等待的线程
            notEmptyCondition.signalAll();
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 更新缓冲区，只保留指定分区的获取数据。如果之前获取的数据
     * {@link CompletedFetch 已完成的获取}的分区不在给定的分区集合中，则会被移除。
     *
     * @param partitions 需要保留数据的{@link TopicPartition 主题分区}集合
     */
    void retainAll(final Set<TopicPartition> partitions) {
        try {
            // 获取锁以确保线程安全
            lock.lock();

            // 移除不在指定分区集合中的已完成获取
            completedFetches.removeIf(cf -> maybeDrain(partitions, cf));

            // 检查并可能清除下一个要处理的获取请求
            if (maybeDrain(partitions, nextInLineFetch))
                nextInLineFetch = null;
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 清空（即<em>移除</em>）给定的{@link CompletedFetch}内容，因为这些数据
     * 不应该返回给用户。
     * 
     * @param partitions 要保留的分区集合
     * @param completedFetch 要检查的已完成获取
     * @return 如果数据被清空返回true，否则返回false
     */
    private boolean maybeDrain(final Set<TopicPartition> partitions, final CompletedFetch completedFetch) {
        // 如果获取结果存在且其分区不在要保留的分区集合中
        if (completedFetch != null && !partitions.contains(completedFetch.partition)) {
            // 记录日志
            log.debug("从缓冲的获取数据中移除{}，因为它不在要保留的分区集合({})中", completedFetch.partition, partitions);
            // 清空数据
            completedFetch.drain();
            return true;
        } else {
            return false;
        }
    }

    /**
     * 返回缓冲区中有数据的{@link TopicPartition 主题分区}集合
     *
     * @return 包含数据的{@link TopicPartition 分区}集合
     */
    Set<TopicPartition> bufferedPartitions() {
        try {
            // 获取锁以确保线程安全
            lock.lock();

            // 创建结果集合
            final Set<TopicPartition> partitions = new HashSet<>();

            // 如果下一个要处理的获取请求存在且未被消费，添加其分区
            if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
                partitions.add(nextInLineFetch.partition);
            }

            // 添加所有已完成获取的分区
            completedFetches.forEach(cf -> partitions.add(cf.partition));
            return partitions;
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }

    /**
     * 关闭FetchBuffer，清理所有资源
     * 实现AutoCloseable接口的方法
     */
    @Override
    public void close() {
        try {
            // 获取锁以确保线程安全
            lock.lock();

            // 使用幂等关闭器确保只关闭一次
            idempotentCloser.close(
                    // 清空所有缓存的数据
                    () -> retainAll(Collections.emptySet()),
                    // 如果已经关闭，记录警告日志
                    () -> log.warn("获取缓冲区已经关闭")
            );
        } finally {
            // 确保锁始终被释放
            lock.unlock();
        }
    }
}
