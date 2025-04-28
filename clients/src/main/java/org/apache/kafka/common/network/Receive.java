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
package org.apache.kafka.common.network;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

/**
 * 此接口模拟从通道到数据源的进行中的数据读取过程
 * 
 * 设计考虑：
 * 1. 异步IO：使用非阻塞的方式从通道读取数据，支持高并发场景
 * 2. 内存管理：在读取大量数据时，通过预先检查和按需分配内存来避免OOM
 * 3. 状态追踪：跟踪数据读取的完整性和内存分配状态
 */
public interface Receive extends Closeable {

    /**
     * 获取正在接收数据的源的标识符
     * 
     * 实现说明：
     * - 返回一个唯一的字符串标识符，用于区分不同的数据源
     * - 在整个读取过程中保持不变
     * 
     * @return 数据源的标识符
     */
    String source();

    /**
     * 检查数据是否接收完成
     * 
     * 实现说明：
     * - 当所有预期的数据都已被读取时返回true
     * - 用于控制读取循环的终止
     * 
     * @return 如果数据接收完成返回true，否则返回false
     */
    boolean complete();

    /**
     * 从给定的通道读取字节数据
     * 
     * 实现说明：
     * - 使用ScatteringByteChannel支持将数据读取到多个缓冲区
     * - 非阻塞操作，适用于异步IO场景
     * - 返回实际读取的字节数，可能小于请求的字节数
     * 
     * @param channel 要读取的通道
     * @return 实际读取的字节数
     * @throws IOException 如果读取过程中发生IO错误
     */
    long readFrom(ScatteringByteChannel channel) throws IOException;

    /**
     * 检查是否已知道完成读取所需的内存大小
     * 
     * 实现说明：
     * - 用于预先确定内存分配需求
     * - 在处理大量数据时，有助于避免内存溢出
     * 
     * @return 如果已知所需内存大小返回true，否则返回false
     */
    boolean requiredMemoryAmountKnown();

    /**
     * 检查完成读取所需的底层内存是否已分配
     * 
     * 实现说明：
     * - 确保在开始读取之前有足够的内存
     * - 支持延迟内存分配策略
     * 
     * @return 如果所需内存已分配返回true，否则返回false
     */
    boolean memoryAllocated();
}
