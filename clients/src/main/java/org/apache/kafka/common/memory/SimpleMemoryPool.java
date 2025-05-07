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
package org.apache.kafka.common.memory;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 一个简单的内存池实现。
 * 该实现主要提供对总体已分配内存的限制，通过跟踪可用内存量来管理内存分配。
 * 重要：所有通过此内存池分配的缓冲区必须显式调用release()方法释放，否则内存将不会被标记为已回收（会发生内存泄漏）。
 */
public class SimpleMemoryPool implements MemoryPool {
    // 日志记录器，protected修饰便于子类使用
    protected final Logger log = LoggerFactory.getLogger(getClass());

    // 内存池的总容量（字节数）
    protected final long sizeBytes;
    // 是否启用严格模式（true：只有当有足够内存时才分配；false：允许过度分配）
    protected final boolean strict;
    // 当前可用内存量，使用AtomicLong保证线程安全
    protected final AtomicLong availableMemory;
    // 单次分配的最大字节数限制
    protected final int maxSingleAllocationSize;
    // 记录开始无可用内存的时间点（纳秒），用于统计内存不足持续时间
    protected final AtomicLong startOfNoMemPeriod = new AtomicLong();
    // 内存不足时间传感器，用于监控和度量内存不足的情况
    protected volatile Sensor oomTimeSensor;

    /**
     * 创建一个SimpleMemoryPool实例
     * 
     * @param sizeInBytes 内存池的总容量（字节数）
     * @param maxSingleAllocationBytes 单次分配的最大字节数
     * @param strict 是否启用严格模式
     * @param oomPeriodSensor 内存不足时间传感器
     */
    public SimpleMemoryPool(long sizeInBytes, int maxSingleAllocationBytes, boolean strict, Sensor oomPeriodSensor) {
        // 参数校验：确保总容量和单次分配上限为正数，且单次分配上限不超过总容量
        if (sizeInBytes <= 0 || maxSingleAllocationBytes <= 0 || maxSingleAllocationBytes > sizeInBytes)
            throw new IllegalArgumentException("must provide a positive size and max single allocation size smaller than size."
                + "provided " + sizeInBytes + " and " + maxSingleAllocationBytes + " respectively");
        this.sizeBytes = sizeInBytes;
        this.strict = strict;
        // 初始化时可用内存等于总容量
        this.availableMemory = new AtomicLong(sizeInBytes);
        this.maxSingleAllocationSize = maxSingleAllocationBytes;
        this.oomTimeSensor = oomPeriodSensor;
    }

    /**
     * 尝试分配指定大小的内存缓冲区
     * 
     * @param sizeBytes 请求分配的字节数
     * @return 如果分配成功，返回一个ByteBuffer；如果内存不足，返回null
     * @throws IllegalArgumentException 如果请求的大小小于1或大于maxSingleAllocationSize
     */
    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        // 参数校验：确保请求的大小合法
        if (sizeBytes < 1)
            throw new IllegalArgumentException("requested size " + sizeBytes + "<=0");
        if (sizeBytes > maxSingleAllocationSize)
            throw new IllegalArgumentException("requested size " + sizeBytes + " is larger than maxSingleAllocationSize " + maxSingleAllocationSize);

        long available;
        boolean success = false;
        // 根据strict模式确定内存分配阈值：
        // - 严格模式：必须有足够的可用内存（至少等于请求的大小）
        // - 非严格模式：只要有任何可用内存就可以分配（允许可用内存为负，最大分配可达sizeBytes + maxSingleAllocationSize）
        long threshold = strict ? sizeBytes : 1;
        // 使用CAS操作尝试更新可用内存量
        while ((available = availableMemory.get()) >= threshold) {
            success = availableMemory.compareAndSet(available, available - sizeBytes);
            if (success)
                break;
        }

        if (success) {
            // 分配成功，可能需要重置内存不足计时
            maybeRecordEndOfDrySpell();
        } else {
            // 分配失败，记录内存不足开始时间
            if (oomTimeSensor != null) {
                startOfNoMemPeriod.compareAndSet(0, System.nanoTime());
            }
            log.trace("refused to allocate buffer of size {}", sizeBytes);
            return null;
        }

        // 创建并返回新的缓冲区
        ByteBuffer allocated = ByteBuffer.allocate(sizeBytes);
        bufferToBeReturned(allocated);
        return allocated;
    }

    /**
     * 释放之前分配的缓冲区
     * 
     * @param previouslyAllocated 要释放的ByteBuffer
     * @throws IllegalArgumentException 如果传入的缓冲区为null
     */
    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null)
            throw new IllegalArgumentException("provided null buffer");

        // 通知子类进行缓冲区释放前的处理
        bufferToBeReleased(previouslyAllocated);
        // 增加可用内存量
        availableMemory.addAndGet(previouslyAllocated.capacity());
        // 可能需要重置内存不足计时
        maybeRecordEndOfDrySpell();
    }

    /**
     * 获取内存池的总容量
     */
    @Override
    public long size() {
        return sizeBytes;
    }

    /**
     * 获取当前可用的内存量
     */
    @Override
    public long availableMemory() {
        return availableMemory.get();
    }

    /**
     * 检查内存池是否已无法分配更多内存
     */
    @Override
    public boolean isOutOfMemory() {
        return availableMemory.get() <= 0;
    }

    /**
     * 在返回缓冲区给客户端代码之前的处理钩子
     * 允许子类进行自己的记录和验证工作
     */
    protected void bufferToBeReturned(ByteBuffer justAllocated) {
        log.trace("allocated buffer of size {} ", justAllocated.capacity());
    }

    /**
     * 在标记内存为已回收之前的处理钩子
     * 允许子类进行自己的记录和验证工作
     */
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        log.trace("released buffer of size {}", justReleased.capacity());
    }

    @Override
    public String toString() {
        long allocated = sizeBytes - availableMemory.get();
        return "SimpleMemoryPool{" + Utils.formatBytes(allocated) + "/" + Utils.formatBytes(sizeBytes) + " used}";
    }

    /**
     * 记录内存不足时期的结束
     * 如果之前处于内存不足状态，计算并记录内存不足持续时间
     */
    protected void maybeRecordEndOfDrySpell() {
        if (oomTimeSensor != null) {
            // 重置内存不足开始时间，并获取之前的值
            long startOfDrySpell = startOfNoMemPeriod.getAndSet(0);
            if (startOfDrySpell != 0) {
                // 计算内存不足持续时间（转换为毫秒）
                oomTimeSensor.record((System.nanoTime() - startOfDrySpell) / 1000000.0);
            }
        }
    }
}
