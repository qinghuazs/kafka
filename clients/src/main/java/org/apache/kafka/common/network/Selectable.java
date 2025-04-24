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


import org.apache.kafka.common.memory.MemoryPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * An interface for asynchronous, multi-channel network I/O
 * 用于异步多通道网络I/O的接口
 * 
 * 该接口是Kafka网络层的核心组件，提供了异步非阻塞的网络通信功能。
 * 它基于Java NIO实现，支持多个网络连接的并发处理，包括连接管理、数据收发等操作。
 * 实现类通常使用Selector模式来管理多个Channel，从而实现高效的网络I/O。
 */
public interface Selectable {

    /**
     * See {@link #connect(String, InetSocketAddress, int, int) connect()}
     * 默认缓冲区大小标记值，用于指示使用系统默认的缓冲区大小
     */
    int USE_DEFAULT_BUFFER_SIZE = -1;

    /**
     * Begin establishing a socket connection to the given address identified by the given address
     * 开始建立到指定地址的socket连接
     * 
     * @param id The id for this connection
     *        连接的唯一标识符
     * @param address The address to connect to
     *        要连接的目标地址
     * @param sendBufferSize The send buffer for the socket
     *        socket的发送缓冲区大小
     * @param receiveBufferSize The receive buffer for the socket
     *        socket的接收缓冲区大小
     * @throws IOException If we cannot begin connecting
     *        如果无法开始连接时抛出异常
     * 
     * 该方法用于异步建立TCP连接，它会立即返回而不会等待连接建立完成。
     * 连接的建立状态可以通过后续的poll()调用来检查。
     */
    void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException;

    /**
     * Wakeup this selector if it is blocked on I/O
     * 唤醒在I/O操作上阻塞的selector
     * 
     * 当selector处于阻塞状态时（例如在poll方法中等待），
     * 调用此方法可以立即中断阻塞状态，使selector立即返回。
     * 这在需要紧急处理其他任务时非常有用。
     */
    void wakeup();

    /**
     * Close this selector
     * 关闭当前selector
     * 
     * 关闭selector及其管理的所有网络连接。
     * 这是一个清理操作，会释放所有相关的系统资源。
     */
    void close();

    /**
     * Close the connection identified by the given id
     * 关闭指定ID的连接
     * 
     * 用于关闭特定的网络连接，同时会清理该连接相关的资源。
     * 这个方法通常用于处理连接异常或不再需要的连接。
     */
    void close(String id);

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long) poll()} calls
     * 将给定的请求加入发送队列，在随后的poll()调用中发送
     * 
     * @param send The request to send
     *        要发送的网络请求
     * 
     * 这个方法不会立即发送数据，而是将数据放入发送队列中，
     * 实际的发送操作将在下一次poll()调用时进行。
     * 这种设计允许批量处理网络发送操作，提高效率。
     */
    void send(NetworkSend send);

    /**
     * Do I/O. Reads, writes, connection establishment, etc.
     * 执行I/O操作，包括读取、写入、建立连接等
     * 
     * @param timeout The amount of time to block if there is nothing to do
     *        当没有I/O事件时的阻塞时间
     * @throws IOException
     *        当I/O操作发生错误时抛出异常
     * 
     * 这是整个接口的核心方法，它会：
     * 1. 处理所有待处理的I/O事件
     * 2. 执行已排队的发送操作
     * 3. 接收新的数据
     * 4. 处理连接的建立和断开
     * 如果没有I/O事件，则最多阻塞timeout毫秒
     */
    void poll(long timeout) throws IOException;

    /**
     * The list of sends that completed on the last {@link #poll(long) poll()} call.
     * 获取在最近一次poll()调用中完成的发送操作列表
     * 
     * 返回上一次poll()调用期间成功完成的所有发送操作。
     * 这些信息可用于确认数据是否已成功发送，以及进行相应的清理工作。
     */
    List<NetworkSend> completedSends();

    /**
     * The collection of receives that completed on the last {@link #poll(long) poll()} call.
     * 获取在最近一次poll()调用中完成的接收操作集合
     *
     * Note that the caller of this method assumes responsibility to close the NetworkReceive resources which may be
     * backed by a {@link MemoryPool}. In such scenarios (when NetworkReceive uses a {@link MemoryPool}), it is necessary
     * to close the {@link NetworkReceive} to prevent any memory leaks.
     * 
     * 注意：调用者需要负责关闭NetworkReceive资源，这些资源可能由MemoryPool支持。
     * 在这种情况下（当NetworkReceive使用MemoryPool时），必须关闭NetworkReceive以防止内存泄漏。
     * 
     * 返回上一次poll()调用期间接收到的所有数据。
     * 这些数据需要被及时处理，并正确管理相关的内存资源。
     */
    Collection<NetworkReceive> completedReceives();

    /**
     * The connections that finished disconnecting on the last {@link #poll(long) poll()}
     * call. Channel state indicates the local channel state at the time of disconnection.
     * 获取在最近一次poll()调用中完成断开的连接
     * 
     * 返回在上一次poll()调用期间断开的所有连接及其状态。
     * ChannelState包含了连接断开时的本地通道状态信息，
     * 这对于诊断连接问题和进行清理工作很有帮助。
     */
    Map<String, ChannelState> disconnected();

    /**
     * The list of connections that completed their connection on the last {@link #poll(long) poll()}
     * call.
     * 获取在最近一次poll()调用中完成建立的连接列表
     * 
     * 返回在上一次poll()调用期间成功建立的所有连接。
     * 这些信息用于跟踪新建立的连接，并进行后续的初始化工作。
     */
    List<String> connected();

    /**
     * Disable reads from the given connection
     * 禁用指定连接的读取操作
     * 
     * @param id The id for the connection
     *        要禁用读取的连接ID
     * 
     * 暂停从指定连接读取数据，通常用于流控制或处理背压。
     * 被禁用的连接仍然可以发送数据，但不会接收新数据。
     */
    void mute(String id);

    /**
     * Re-enable reads from the given connection
     * 重新启用指定连接的读取操作
     * 
     * @param id The id for the connection
     *        要重新启用读取的连接ID
     * 
     * 恢复之前被禁用的连接的读取操作。
     * 这通常在流控制条件解除后调用。
     */
    void unmute(String id);

    /**
     * Disable reads from all connections
     * 禁用所有连接的读取操作
     * 
     * 批量暂停所有连接的数据读取。
     * 这在系统需要临时停止处理新请求时很有用，
     * 例如在进行系统维护或处理过载情况时。
     */
    void muteAll();

    /**
     * Re-enable reads from all connections
     * 重新启用所有连接的读取操作
     * 
     * 批量恢复所有连接的数据读取。
     * 通常在系统恢复正常处理能力后调用。
     */
    void unmuteAll();

    /**
     * returns true  if a channel is ready
     * 检查指定的通道是否就绪
     * 
     * @param id The id for the connection
     *        要检查的连接ID
     * 
     * 返回指定通道的就绪状态。
     * 通道就绪表示它可以进行I/O操作，
     * 这个方法通常用于在进行I/O操作前进行检查。
     */
    boolean isChannelReady(String id);
}
