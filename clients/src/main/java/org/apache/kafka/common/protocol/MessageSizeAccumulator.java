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
package org.apache.kafka.common.protocol;

/**
 * 辅助类，用于支持零拷贝网络传输功能。参见 {@link SendBuilder}。
 * 该类主要用于累计计算消息的总大小，包括普通数据大小和零拷贝数据大小。
 * 零拷贝是一种优化技术，可以避免数据在内核空间和用户空间之间的多次拷贝，从而提高性能。
 */
public class MessageSizeAccumulator {
    // 消息的总大小，包括普通数据和零拷贝数据的大小总和
    private int totalSize = 0;
    // 零拷贝数据的大小，是总大小的一部分
    private int zeroCopySize = 0;

    /**
     * 获取消息的总大小。
     * 总大小包含了普通数据大小和零拷贝数据大小的总和。
     *
     * @return 消息总大小（字节数）
     */
    public int totalSize() {
        // 返回累计的总大小
        return totalSize;
    }

    /**
     * 获取不包含零拷贝字段的大小。
     * 这通常是用于序列化消息的字节缓冲区的大小。
     * 计算方式为总大小减去零拷贝数据的大小。
     *
     * @return 不包含零拷贝数据的大小（字节数）
     */
    public int sizeExcludingZeroCopy() {
        // 返回普通数据的大小（总大小减去零拷贝数据大小）
        return totalSize - zeroCopySize;
    }

    /**
     * 添加零拷贝数据的大小。
     * 当添加零拷贝数据时，会同时更新总大小和零拷贝大小。
     *
     * @param size 要添加的零拷贝数据大小（字节数）
     */
    public void addZeroCopyBytes(int size) {
        // 更新零拷贝数据大小
        zeroCopySize += size;
        // 同时更新总大小
        totalSize += size;
    }

    /**
     * 添加普通数据的大小。
     * 只更新总大小，不影响零拷贝数据大小。
     *
     * @param size 要添加的普通数据大小（字节数）
     */
    public void addBytes(int size) {
        // 仅更新总大小
        totalSize += size;
    }

    /**
     * 将另一个MessageSizeAccumulator的大小数据合并到当前实例中。
     * 会分别累加总大小和零拷贝大小。
     *
     * @param size 要合并的MessageSizeAccumulator实例
     */
    public void add(MessageSizeAccumulator size) {
        // 累加总大小
        this.totalSize += size.totalSize;
        // 累加零拷贝数据大小
        this.zeroCopySize += size.zeroCopySize;
    }

}
