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

import java.nio.ByteBuffer;


/**
 * 非阻塞内存池的通用接口。
 * 该接口定义了内存池的基本操作，包括内存分配、释放和状态查询。
 * 重要：从{@link #tryAllocate(int)}获取的每个缓冲区必须通过{@link #release(ByteBuffer)}方法释放。
 */
public interface MemoryPool {
    /**
     * 一个特殊的内存池实现，不进行任何内存限制和管理。
     * 适用于不需要内存限制的场景，每次分配都直接创建新的ByteBuffer。
     */
    MemoryPool NONE = new MemoryPool() {
        @Override
        public ByteBuffer tryAllocate(int sizeBytes) {
            // 直接分配新的ByteBuffer，不进行任何限制
            return ByteBuffer.allocate(sizeBytes);
        }

        @Override
        public void release(ByteBuffer previouslyAllocated) {
            // 不进行任何操作，由GC自动回收
        }

        @Override
        public long size() {
            // 返回最大值表示无限大小
            return Long.MAX_VALUE;
        }

        @Override
        public long availableMemory() {
            // 返回最大值表示总是有可用内存
            return Long.MAX_VALUE;
        }

        @Override
        public boolean isOutOfMemory() {
            // 永远不会内存不足
            return false;
        }

        @Override
        public String toString() {
            return "NONE";
        }
    };

    /**
     * 尝试分配指定大小的ByteBuffer
     * 
     * @param sizeBytes 请求分配的字节数
     * @return 如果分配成功，返回一个ByteBuffer（后续需要调用release()释放）；
     *         如果没有足够的可用内存，返回null。
     *         注意：返回的buffer大小将严格等于请求的大小，即使底层可能使用了更大的内存块。
     */
    ByteBuffer tryAllocate(int sizeBytes);

    /**
     * 将之前分配的缓冲区归还给内存池
     * 
     * @param previouslyAllocated 通过tryAllocate()方法获取的ByteBuffer
     */
    void release(ByteBuffer previouslyAllocated);

    /**
     * 获取内存池的总容量
     * 
     * @return 内存池的总字节数
     */
    long size();

    /**
     * 获取当前可用的内存量
     * 注意：返回值可能为负数（某些实现可能允许过度分配以避免饥饿问题）
     * 
     * @return 可用的字节数
     */
    long availableMemory();

    /**
     * 检查内存池是否已无法分配更多缓冲区
     * - 当已分配的缓冲区总量达到或超过内存池大小时返回true
     * - 此时需要释放一些缓冲区才能进行新的分配
     * 
     * 等价于 availableMemory() <= 0
     * 
     * @return 如果内存耗尽返回true，否则返回false
     */
    boolean isOutOfMemory();
}
