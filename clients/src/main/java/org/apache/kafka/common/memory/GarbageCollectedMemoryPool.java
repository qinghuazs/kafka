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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * SimpleMemoryPool的扩展实现，用于跟踪已分配的缓冲区并在发生内存泄漏时记录错误
 * （当缓冲区被垃圾回收但未调用release()方法时视为泄漏）。
 * 注意：此实现仅用于开发和调试目的，不适合在生产环境中使用。
 */
public class GarbageCollectedMemoryPool extends SimpleMemoryPool implements AutoCloseable {

    // 用于跟踪被垃圾回收的ByteBuffer的引用队列
    private final ReferenceQueue<ByteBuffer> garbageCollectedBuffers = new ReferenceQueue<>();
    // 跟踪当前正在使用的缓冲区，服务于两个目的：
    // 1. 保持引用对象可达（这是引用对象能被加入队列的必要条件）
    // 2. 为每个已分配的缓冲区保存少量元数据
    private final Map<BufferReference, BufferMetadata> buffersInFlight = new ConcurrentHashMap<>();
    // 垃圾回收监听线程
    private final Thread gcListenerThread;
    // 控制监听线程的运行状态
    private volatile boolean alive = true;

    /**
     * 创建一个带有内存泄漏检测功能的内存池
     * 
     * @param sizeBytes 内存池总容量
     * @param maxSingleAllocationSize 单次分配的最大字节数
     * @param strict 是否启用严格模式
     * @param oomPeriodSensor 内存不足时间传感器
     */
    public GarbageCollectedMemoryPool(long sizeBytes, int maxSingleAllocationSize, boolean strict, Sensor oomPeriodSensor) {
        super(sizeBytes, maxSingleAllocationSize, strict, oomPeriodSensor);
        // 创建并启动垃圾回收监听线程
        GarbageCollectionListener gcListener = new GarbageCollectionListener();
        this.gcListenerThread = new Thread(gcListener, "memory pool GC listener");
        this.gcListenerThread.setDaemon(true); // 设置为守护线程，避免关闭时的问题
        this.gcListenerThread.start();
    }

    /**
     * 在返回新分配的缓冲区之前进行跟踪记录
     */
    @Override
    protected void bufferToBeReturned(ByteBuffer justAllocated) {
        // 创建弱引用并关联到引用队列
        BufferReference ref = new BufferReference(justAllocated, garbageCollectedBuffers);
        BufferMetadata metadata = new BufferMetadata(justAllocated.capacity());
        if (buffersInFlight.put(ref, metadata) != null)
            // 如果已存在相同标识的缓冲区，说明出现了bug
            // 可能是两个不同的缓冲区获得了相同的标识，或者未能正确注销已释放/已回收的缓冲区
            throw new IllegalStateException("allocated buffer identity " + ref.hashCode + " already registered as in use?!");

        log.trace("allocated buffer of size {} and identity {}", sizeBytes, ref.hashCode);
    }

    /**
     * 在释放缓冲区之前进行验证和清理
     */
    @Override
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        // 创建用于查找的引用对象（不需要关联引用队列）
        BufferReference ref = new BufferReference(justReleased);
        BufferMetadata metadata = buffersInFlight.remove(ref);
        if (metadata == null)
            // 由于我们持有缓冲区的强引用（方法参数），不可能已被GC
            // 因此这种情况要么是重复释放，要么是释放了不属于此池的缓冲区
            throw new IllegalArgumentException("returned buffer " + ref.hashCode + " was never allocated by this pool");
        if (metadata.sizeBytes != justReleased.capacity()) {
            // 缓冲区容量与记录的不匹配，这是一个bug
            throw new IllegalStateException("buffer " + ref.hashCode + " has capacity " + justReleased.capacity() + " but recorded as " + metadata.sizeBytes);
        }
        log.trace("released buffer of size {} and identity {}", metadata.sizeBytes, ref.hashCode);
    }

    /**
     * 关闭内存池，停止内存泄漏检测
     */
    @Override
    public void close() {
        alive = false;
        gcListenerThread.interrupt();
    }

    /**
     * 垃圾回收监听器，用于检测未正确释放的缓冲区
     */
    private class GarbageCollectionListener implements Runnable {
        @Override
        public void run() {
            while (alive) {
                try {
                    // 等待下一个被垃圾回收的缓冲区引用（阻塞操作）
                    BufferReference ref = (BufferReference) garbageCollectedBuffers.remove();
                    ref.clear();
                    // 这里不会与release()调用发生竞争，因为：
                    // 1. 对象要么可达要么不可达
                    // 2. release()只能在GC之前发生
                    // 3. 入队列只能在GC之后发生
                    // 因此，如果引用被入队，说明一定没有调用release()
                    BufferMetadata metadata = buffersInFlight.remove(ref);

                    if (metadata == null) {
                        // 极少数情况下，缓冲区可能已正确释放（所以没有元数据），但是
                        // 引用对象在release()后短时间内仍然可达，因此被加入队列
                        // 这是因为我们使用ConcurrentHashMap，它会延迟清理键值对
                        continue;
                    }

                    // 发现泄漏的缓冲区，恢复可用内存并记录错误
                    availableMemory.addAndGet(metadata.sizeBytes);
                    log.error("Reclaimed buffer of size {} and identity {} that was not properly release()ed. This is a bug.", metadata.sizeBytes, ref.hashCode);
                } catch (InterruptedException e) {
                    log.debug("interrupted", e);
                    // 忽略中断异常，因为这是一个守护线程
                }
            }
            log.info("GC listener shutting down");
        }
    }

    /**
     * 缓冲区元数据，记录分配的大小信息
     */
    private static final class BufferMetadata {
        private final int sizeBytes;

        private BufferMetadata(int sizeBytes) {
            this.sizeBytes = sizeBytes;
        }
    }

    /**
     * 用于跟踪ByteBuffer的弱引用实现
     * 通过重写equals和hashCode方法，确保可以正确识别泄漏的缓冲区
     */
    private static final class BufferReference extends WeakReference<ByteBuffer> {
        // 使用对象的标识哈希码，确保不同对象的引用可以被区分
        private final int hashCode;

        // 用于查找的构造函数，不需要关联引用队列
        private BufferReference(ByteBuffer referent) {
            this(referent, null);
        }

        // 完整构造函数，可以关联到引用队列
        private BufferReference(ByteBuffer referent, ReferenceQueue<? super ByteBuffer> q) {
            super(referent, q);
            hashCode = System.identityHashCode(referent);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) { // 通过引用标识查找泄漏的缓冲区
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BufferReference that = (BufferReference) o;
            if (hashCode != that.hashCode) {
                return false;
            }
            ByteBuffer thisBuf = get();
            if (thisBuf == null) {
                // 我们的缓冲区已被GC，而that不是我们，所以不是同一个缓冲区
                return false;
            }
            ByteBuffer thatBuf = that.get();
            return thisBuf == thatBuf;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    @Override
    public String toString() {
        long allocated = sizeBytes - availableMemory.get();
        return "GarbageCollectedMemoryPool{" + Utils.formatBytes(allocated) + "/" + Utils.formatBytes(sizeBytes) + " used in " + buffersInFlight.size() + " buffers}";
    }
}
