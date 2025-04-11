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

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * ByteBuffer池，用于在给定内存限制下管理ByteBuffer。这个类专门针对生产者的需求设计，具有以下特点：
 * <ol>
 * <li>设有特定的"可池化大小"(poolable size)，该大小的缓冲区会被保存在空闲列表中并循环使用
 * <li>采用公平分配策略：内存分配给等待时间最长的线程，直到其获得足够的内存。这可以防止当线程请求大块内存时
 *     出现饥饿或死锁（因为可能需要等待多个缓冲区被释放）
 * </ol>
 * 
 * 该类的主要作用：
 * 1. 管理内存分配：控制生产者可使用的总内存量，防止内存溢出
 * 2. 内存复用：通过对特定大小的缓冲区进行池化，减少内存分配和GC压力
 * 3. 公平性保证：确保所有线程都能公平地获得所需内存，避免饥饿问题
 * 4. 性能监控：通过度量指标跟踪内存使用情况和等待时间
 */
public class BufferPool {

    // 用于度量缓冲池等待时间的传感器名称
    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    // 缓冲池可以分配的最大内存总量
    private final long totalMemory;
    // 可以被池化（被重复使用）的缓冲区的固定大小
    private final int poolableSize;
    // 用于同步的重入锁，保护对共享资源的访问
    private final ReentrantLock lock;
    // 存储空闲的、可重用的ByteBuffer队列
    private final Deque<ByteBuffer> free;
    // 等待获取内存的线程条件队列
    private final Deque<Condition> waiters;
    /** 
     * 总可用内存 = 非池化可用内存 + (空闲列表中的缓冲区数量 * poolableSize)
     * 非池化内存是指未被分配或已被释放的内存
     */
    private long nonPooledAvailableMemory;
    // 度量指标管理器，用于监控缓冲池的性能
    private final Metrics metrics;
    // 时间工具类，用于计算等待时间
    private final Time time;
    // 等待时间传感器，用于记录线程等待分配内存的时间
    private final Sensor waitTime;
    // 标记缓冲池是否已关闭
    private boolean closed;

    /**
     * 创建一个新的缓冲池
     *
     * @param memory 缓冲池可以分配的最大内存量（字节）
     * @param poolableSize 可被缓存在空闲列表中的缓冲区大小，而不是直接释放
     * @param metrics 度量指标实例，用于性能监控
     * @param time 时间实例，用于时间相关的计算
     * @param metricGrpName 度量指标的逻辑分组名称
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalNsMetricName = metrics.metricName("bufferpool-wait-time-ns-total",
                                                    metricGrpName,
                                                    "The total time in nanoseconds an appender waits for space allocation.");

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalNsMetricName));
        this.closed = false;
    }

    /**
     * 分配指定大小的缓冲区。如果没有足够的内存且缓冲池配置为阻塞模式，该方法会阻塞等待。
     * 
     * 分配策略：
     * 1. 如果请求大小等于poolableSize且有空闲缓冲区，直接从空闲列表中获取
     * 2. 如果当前可用内存足够，直接分配新的缓冲区
     * 3. 如果内存不足，则阻塞等待其他线程释放内存
     *
     * @param size 要分配的缓冲区大小（字节）
     * @param maxTimeToBlockMs 等待可用缓冲区内存的最大阻塞时间（毫秒）
     * @return 分配的缓冲区
     * @throws InterruptedException 如果线程在阻塞等待时被中断
     * @throws IllegalArgumentException 如果请求的大小超过了缓冲池的总内存限制（这种情况下会永远阻塞）
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        // 检查请求的内存大小是否超过总内存限制
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        ByteBuffer buffer = null;
        // 获取锁，确保内存分配的线程安全
        this.lock.lock();

        // 如果缓冲池已关闭，则抛出异常
        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        try {
            // 检查是否有合适大小的空闲缓冲区可用
            // 如果请求的大小正好等于poolableSize且空闲列表不为空，直接返回一个空闲缓冲区
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // 检查当前可用内存（非池化内存 + 空闲列表中的内存）是否足够满足请求
            int freeListSize = freeSize() * this.poolableSize;
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // 有足够的未分配或已池化的内存可以立即满足请求
                // 需要先释放足够的空间
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else {
                // 内存不足，需要阻塞等待
                int accumulated = 0;  // 累计获得的内存大小
                Condition moreMemory = this.lock.newCondition();  // 创建条件变量用于等待
                try {
                    // 将等待时间从毫秒转换为纳秒
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    // 将当前线程的条件变量加入等待队列
                    this.waiters.addLast(moreMemory);
                    // 循环直到获得足够的内存或超时
                    while (accumulated < size) {
                        // 记录等待开始时间
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            // 等待其他线程释放内存，返回false表示等待超时
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            // 计算实际等待时间并记录
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        // 如果缓冲池已关闭，抛出异常
                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        // 如果等待超时，记录缓冲区耗尽的指标并抛出异常
                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate " + size + " bytes within the configured max blocking time "
                                + maxTimeToBlockMs + " ms. Total memory: " + totalMemory() + " bytes. Available memory: " + availableMemory()
                                + " bytes. Poolable size: " + poolableSize() + " bytes");
                        }

                        // 更新剩余等待时间
                        remainingTimeToBlockNs -= timeNs;

                        // 尝试从空闲列表分配内存或进行部分内存分配
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // 如果请求的大小等于poolableSize且有空闲缓冲区，直接从空闲列表获取
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // 需要分配新的内存，可能一次只能得到部分需要的内存
                            freeUp(size - accumulated);
                            // 计算本次可以分配的内存大小
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // 如果没有抛出异常，将accumulated设为0（因为已经成功分配了内存）
                    accumulated = 0;
                } finally {
                    // 如果循环未能成功完成，确保不会丢失已累积的可用内存
                    this.nonPooledAvailableMemory += accumulated;
                    // 从等待队列中移除当前线程的条件变量
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // 如果还有可用内存，通知其他等待的线程
            try {
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // 释放锁
                lock.unlock();
            }
        }

        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    // 用于测试的受保护方法，记录等待时间
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * 安全地分配缓冲区。如果分配失败（例如发生OOM），则将大小计数返回到可用内存，
     * 并通知下一个等待者（如果存在）。
     * 
     * @param size 要分配的缓冲区大小
     * @return 分配的ByteBuffer
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            // 尝试分配新的ByteBuffer
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            if (error) {
                // 如果分配失败，恢复可用内存并通知等待者
                this.lock.lock();
                try {
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // 用于测试的受保护方法，实际分配ByteBuffer
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * 通过释放已池化的缓冲区（如果需要），确保我们至少有请求的字节数可用于分配。
     * 这个方法会不断从空闲列表中移除缓冲区，直到累积足够的非池化可用内存。
     * 
     * @param size 需要的内存大小
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * 将缓冲区返回到池中。如果缓冲区大小等于poolableSize，则将其添加到空闲列表中；
     * 否则仅将内存标记为可用。
     * 
     * @param buffer 要返回的缓冲区
     * @param size 要标记为已释放的缓冲区大小，注意这可能小于buffer.capacity，
     *            因为缓冲区可能在原地压缩期间重新分配自身
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                // 如果是可池化大小的缓冲区，清空并添加到空闲列表
                buffer.clear();
                this.free.add(buffer);
            } else {
                // 否则只增加非池化可用内存
                this.nonPooledAvailableMemory += size;
            }
            // 通知等待中的线程有新的内存可用
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放一个缓冲区，将其容量大小的内存返回到缓冲池中。
     * 这是deallocate(ByteBuffer, int)方法的简化版本，直接使用缓冲区的容量作为释放的大小。
     * 
     * @param buffer 要释放的缓冲区，如果为null则不执行任何操作
     */
    public void deallocate(ByteBuffer buffer) {
        // 只有在缓冲区不为null时才执行释放操作
        if (buffer != null)
            // 调用重载方法，使用缓冲区的实际容量作为释放大小
            deallocate(buffer, buffer.capacity());
    }

    /**
     * 获取缓冲池中当前可用的总内存大小，包括未分配的内存和空闲列表中的内存。
     * 计算方式：未分配内存 + (空闲列表中的缓冲区数量 * 可池化大小)
     * 
     * 该方法是线程安全的，使用锁来保护对共享资源的访问。
     * 
     * @return 当前可用的总内存字节数
     */
    public long availableMemory() {
        // 获取锁以确保线程安全
        lock.lock();
        try {
            // 计算并返回总可用内存：未分配内存 + 空闲列表中的总内存
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            // 确保在方法返回前释放锁
            lock.unlock();
        }
    }

    /**
     * 获取空闲列表中当前可用的缓冲区数量。
     * 这个方法主要用于测试目的，因此被声明为protected。
     * 
     * @return 空闲列表中的缓冲区数量
     */
    protected int freeSize() {
        // 返回空闲队列中的缓冲区数量
        return this.free.size();
    }

    /**
     * 获取当前未分配的内存大小（不包括空闲列表中的内存或正在使用的内存）。
     * 这个值代表了可以直接分配给新请求的内存量。
     * 
     * 该方法是线程安全的，使用锁来保护对共享资源的访问。
     * 
     * @return 当前未分配的内存字节数
     */
    public long unallocatedMemory() {
        // 获取锁以确保线程安全
        lock.lock();
        try {
            // 返回当前未分配的内存大小
            return this.nonPooledAvailableMemory;
        } finally {
            // 确保在方法返回前释放锁
            lock.unlock();
        }
    }

    /**
     * 获取当前正在等待内存分配的线程数量。
     * 这个方法可以用来监控缓冲池的压力情况，如果等待线程数量较多，
     * 说明当前内存资源紧张，可能需要调整缓冲池的配置。
     * 
     * 该方法是线程安全的，使用锁来保护对共享资源的访问。
     * 
     * @return 当前等待队列中的线程数量
     */
    public int queued() {
        // 获取锁以确保线程安全
        lock.lock();
        try {
            // 返回等待队列中的线程数量
            return this.waiters.size();
        } finally {
            // 确保在方法返回前释放锁
            lock.unlock();
        }
    }

    /**
     * 获取可被池化（在使用后保留在空闲列表中）的缓冲区大小。
     * 这个大小是在构造函数中指定的固定值，用于决定哪些缓冲区可以被重复使用。
     * 只有大小等于这个值的缓冲区才会被保存在空闲列表中，其他大小的缓冲区在释放时
     * 只会增加未分配内存计数。
     * 
     * @return 可池化的缓冲区大小（字节）
     */
    public int poolableSize() {
        // 返回可池化的缓冲区大小
        return this.poolableSize;
    }

    /**
     * 获取此缓冲池管理的总内存大小。
     * 这个值是在构造函数中指定的固定值，代表了缓冲池可以分配的最大内存量。
     * 它是一个硬性限制，任何超过这个大小的内存分配请求都会被拒绝。
     * 
     * @return 缓冲池的总内存容量（字节）
     */
    public long totalMemory() {
        // 返回缓冲池的总内存容量
        return this.totalMemory;
    }

    /**
     * 获取等待内存分配的线程条件队列。
     * 这个方法是包级私有的，仅用于测试目的。
     * 它提供了对内部等待队列的直接访问，便于测试验证等待机制的正确性。
     * 
     * @return 等待内存分配的线程条件队列
     */
    Deque<Condition> waiters() {
        // 返回等待线程的条件队列
        return this.waiters;
    }

    /**
     * 关闭缓冲池。关闭后将阻止新的内存分配，但允许已分配内存的释放。
     * 所有正在等待内存分配的线程都会被通知中止操作。
     * 
     * 关闭过程是线程安全的，使用锁来保护对共享资源的访问。
     * 关闭后的行为：
     * 1. 新的分配请求会抛出KafkaException
     * 2. 已经在等待的线程会被唤醒并收到异常
     * 3. 内存释放操作仍然可以正常进行
     */
    public void close() {
        // 获取锁以确保线程安全
        this.lock.lock();
        // 标记缓冲池为已关闭状态
        this.closed = true;
        try {
            // 唤醒所有等待中的线程，让它们知道缓冲池已关闭
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            // 确保在方法返回前释放锁
            this.lock.unlock();
        }
    }
}
