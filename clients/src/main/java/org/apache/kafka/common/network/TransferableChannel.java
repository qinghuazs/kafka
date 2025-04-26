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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

/**
 * 扩展GatheringByteChannel接口，提供Send接口所需的最小方法集。
 * 该接口的主要目的是支持TLS安全传输和高效的零拷贝数据传输。
 * 
 * 应用场景：
 * 1. 在Kafka的网络层中用于处理安全传输，特别是在SSL/TLS通信中
 * 2. 用于大文件传输时的性能优化，通过零拷贝技术减少数据复制
 * 3. 在异步网络通信中处理数据写入缓冲和刷新
 * 
 * @see SslTransportLayer SSL传输层的具体实现
 */
public interface TransferableChannel extends GatheringByteChannel {

    /**
     * 检查当前通道是否有待处理的写操作。
     * 
     * 实现细节：
     * 1. 在普通传输层（如PlaintextTransportLayer）中通常返回false，因为数据直接写入
     * 2. 在SSL传输层中，如果加密缓冲区中还有数据未写入，则返回true
     * 3. 用于控制写入流程，确保所有数据都被正确处理
     *
     * @return 如果有待处理的写操作返回true；如果实现直接将所有数据写入输出则返回false
     */
    boolean hasPendingWrites();

    /**
     * 将数据从文件通道传输到当前TransferableChannel通道。
     * 
     * 实现细节：
     * 1. 该方法会调用{@link FileChannel#transferTo}进行实际的数据传输
     * 2. 会尝试解包目标通道以支持零拷贝传输，这是一个重要的性能优化
     * 3. 只有当目标缓冲区继承自JDK内部类时，transferTo的快速路径才会被执行
     * 
     * 应用场景：
     * 1. 用于大文件传输，如日志文件的复制和传输
     * 2. 在Kafka中用于高效地传输消息数据
     * 3. 支持断点续传，可以从指定位置开始传输
     *
     * @param fileChannel 源文件通道
     * @param position 文件中开始传输的位置，必须是非负数
     * @param count 要传输的最大字节数，必须是非负数
     * @return 实际传输的字节数，可能为零
     * @throws IOException 如果传输过程中发生I/O错误
     * @see FileChannel#transferTo 底层的文件传输实现
     */
    long transferFrom(FileChannel fileChannel, long position, long count) throws IOException;
}
