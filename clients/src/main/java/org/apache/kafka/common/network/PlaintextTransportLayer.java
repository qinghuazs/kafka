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

/*
 * PLAINTEXT通信的传输层实现
 * 
 * 该类实现了TransportLayer接口，提供了基于明文的网络通信功能。
 * 主要特点：
 * 1. 不提供加密和身份验证功能
 * 2. 直接使用底层SocketChannel进行数据传输
 * 3. 支持非阻塞I/O操作
 * 4. 适用于内部网络或不需要安全性的场景
 * 
 * 设计考虑：
 * 1. 性能优先：无加密开销，适合高吞吐量场景
 * 2. 简单可靠：直接封装NIO操作，减少复杂性
 * 3. 资源管理：统一的连接和通道生命周期管理
 */

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.Principal;

public class PlaintextTransportLayer implements TransportLayer {
    /**
     * 与该传输层关联的SelectionKey
     * 用于非阻塞I/O操作中的事件监听和操作控制
     */
    private final SelectionKey key;

    /**
     * 底层的Socket通道
     * 负责实际的网络数据传输
     */
    private final SocketChannel socketChannel;

    /**
     * 代表连接对端的身份主体
     * 由于是明文传输，始终使用匿名主体
     */
    private final Principal principal = KafkaPrincipal.ANONYMOUS;

    /**
     * 构造函数
     * @param key 与该传输层关联的SelectionKey
     */
    public PlaintextTransportLayer(SelectionKey key) {
        // 保存SelectionKey引用
        this.key = key;
        // 获取关联的SocketChannel
        this.socketChannel = (SocketChannel) key.channel();
    }

    /**
     * 检查传输层是否准备就绪
     * 由于是明文传输，无需握手过程，始终返回true
     * 
     * @return 始终返回true，表示随时可以进行通信
     */
    @Override
    public boolean ready() {
        return true;
    }

    /**
     * 完成套接字通道的连接过程
     * 
     * @return 如果连接成功建立返回true
     * @throws IOException 如果连接过程中发生I/O错误
     */
    @Override
    public boolean finishConnect() throws IOException {
        // 完成底层套接字的连接
        boolean connected = socketChannel.finishConnect();
        if (connected)
            // 连接成功后，更新感兴趣的操作：
            // 1. 移除CONNECT事件
            // 2. 添加READ事件，准备接收数据
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        return connected;
    }

    /**
     * 断开连接
     * 通过取消SelectionKey来断开连接，使通道不再被Selector监听
     */
    @Override
    public void disconnect() {
        // 取消该键的注册，使通道不再被Selector监听
        key.cancel();
    }

    /**
     * 获取底层的SocketChannel
     * 
     * @return 与该传输层关联的SocketChannel实例
     */
    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    /**
     * 获取SelectionKey
     * 
     * @return 与该传输层关联的SelectionKey实例
     */
    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    /**
     * 检查通道是否打开
     * 
     * @return 如果底层通道处于打开状态返回true
     */
    @Override
    public boolean isOpen() {
        // 直接返回底层通道的打开状态
        return socketChannel.isOpen();
    }

    /**
     * 检查通道是否已连接
     * 
     * @return 如果底层通道已建立连接返回true
     */
    @Override
    public boolean isConnected() {
        // 直接返回底层通道的连接状态
        return socketChannel.isConnected();
    }

    /**
     * 关闭传输层
     * 同时关闭Socket和SocketChannel，释放相关资源
     * 
     * @throws IOException 如果关闭过程中发生I/O错误
     */
    @Override
    public void close() throws IOException {
        // 先关闭Socket
        socketChannel.socket().close();
        // 再关闭SocketChannel
        socketChannel.close();
    }

    /**
     * 执行握手操作
     * 由于是明文传输，无需进行SSL握手，因此这是一个空操作
     * 
     * 设计说明：
     * 1. 该方法在SSL实现中用于执行安全握手
     * 2. 在明文传输中保留该方法是为了保持接口的一致性
     * 3. 空实现意味着可以直接进行数据传输，无需额外的协议交换
     */
    @Override
    public void handshake() {}

    /**
     * 从通道读取字节序列到指定的缓冲区
     * 
     * 实现说明：
     * 1. 直接委托给底层SocketChannel执行读取操作
     * 2. 支持非阻塞读取，可能返回0表示暂时没有数据
     * 3. 返回-1表示对端已关闭连接
     *
     * @param dst 用于存储读取数据的目标缓冲区
     * @return 实际读取的字节数，可能为0；如果返回-1表示通道已到达流末尾
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        // 直接使用底层通道进行读取操作
        return socketChannel.read(dst);
    }

    /**
     * 从通道读取字节序列到多个缓冲区（分散读取）
     * 
     * 实现说明：
     * 1. 支持将数据分散到多个缓冲区，提高读取效率
     * 2. 按照缓冲区数组的顺序依次填充
     * 3. 适用于需要将数据分块处理的场景
     *
     * @param dsts 用于存储读取数据的目标缓冲区数组
     * @return 实际读取的字节总数，可能为0；如果返回-1表示通道已到达流末尾
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        // 使用底层通道的分散读取功能
        return socketChannel.read(dsts);
    }

    /**
     * 从通道读取字节序列到指定范围的缓冲区数组（带偏移量的分散读取）
     * 
     * 实现说明：
     * 1. 支持指定缓冲区数组的起始位置和长度
     * 2. 提供更灵活的数据分散读取控制
     * 3. 适用于只需要填充部分缓冲区的场景
     *
     * @param dsts 用于存储读取数据的目标缓冲区数组
     * @param offset 缓冲区数组中的起始索引，必须非负且不大于dsts.length
     * @param length 要使用的最大缓冲区数量，必须非负且不大于dsts.length - offset
     * @return 实际读取的字节总数，可能为0；如果返回-1表示通道已到达流末尾
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        // 使用底层通道的带偏移量的分散读取功能
        return socketChannel.read(dsts, offset, length);
    }

    /**
     * 将字节序列从给定的缓冲区写入到通道
     * 
     * 实现说明：
     * 1. 直接委托给底层SocketChannel执行写入操作
     * 2. 支持非阻塞写入，可能返回0表示暂时无法写入更多数据
     * 3. 写入操作会尽可能多地传输数据
     *
     * @param src 包含要写入数据的源缓冲区
     * @return 实际写入的字节数，可能为0
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        // 直接使用底层通道进行写入操作
        return socketChannel.write(src);
    }

    /**
     * 将字节序列从多个缓冲区写入到通道（聚集写入）
     * 
     * 实现说明：
     * 1. 支持从多个缓冲区聚集数据进行写入，提高写入效率
     * 2. 按照缓冲区数组的顺序依次写入数据
     * 3. 适用于需要将多个数据块一次性发送的场景
     *
     * @param srcs 包含要写入数据的源缓冲区数组
     * @return 实际写入的字节总数，可能为0
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        // 使用底层通道的聚集写入功能
        return socketChannel.write(srcs);
    }

    /**
     * 将字节序列从指定范围的缓冲区数组写入到通道（带偏移量的聚集写入）
     * 
     * 实现说明：
     * 1. 支持指定缓冲区数组的起始位置和长度
     * 2. 提供更灵活的数据聚集写入控制
     * 3. 适用于只需要发送部分缓冲区数据的场景
     *
     * @param srcs 包含要写入数据的源缓冲区数组
     * @param offset 缓冲区数组中的起始索引，必须非负且不大于srcs.length
     * @param length 要使用的最大缓冲区数量，必须非负且不大于srcs.length - offset
     * @return 实际写入的字节总数，可能为0
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        // 使用底层通道的带偏移量的聚集写入功能
        return socketChannel.write(srcs, offset, length);
    }

    /**
     * 检查是否有待处理的写操作
     * 
     * 实现说明：
     * 1. 由于直接使用SocketChannel写入，不存在数据缓冲
     * 2. 所有写操作都是同步执行的
     * 3. 始终返回false表示没有待处理的写操作
     *
     * @return 始终返回false
     */
    @Override
    public boolean hasPendingWrites() {
        // 由于直接写入，不会有待处理的写操作
        return false;
    }

    /**
     * 获取对等方的身份主体
     * 
     * 实现说明：
     * 1. 明文传输不进行身份认证
     * 2. 始终返回匿名主体（ANONYMOUS）
     * 3. 用于保持与安全传输层接口的一致性
     *
     * @return 返回代表匿名用户的Principal对象
     */
    @Override
    public Principal peerPrincipal() {
        // 返回匿名主体
        return principal;
    }

    /**
     * 添加感兴趣的I/O事件到SelectionKey
     * 
     * 实现说明：
     * 1. 使用位运算|（或）添加新的事件
     * 2. 不影响已有的事件监听
     * 3. 用于动态调整通道的事件监听
     *
     * @param ops 要添加的事件类型，可以是SelectionKey中定义的常量
     */
    @Override
    public void addInterestOps(int ops) {
        // 使用位或运算添加新的事件类型
        key.interestOps(key.interestOps() | ops);
    }

    /**
     * 从SelectionKey中移除感兴趣的I/O事件
     * 
     * 实现说明：
     * 1. 使用位运算&和~（与非）移除指定事件
     * 2. 不影响其他未指定的事件
     * 3. 用于动态调整通道的事件监听
     *
     * @param ops 要移除的事件类型，可以是SelectionKey中定义的常量
     */
    @Override
    public void removeInterestOps(int ops) {
        // 使用位与非运算移除指定的事件类型
        key.interestOps(key.interestOps() & ~ops);
    }

    /**
     * 检查通道是否处于静默状态
     * 
     * 实现说明：
     * 1. 检查SelectionKey是否有效
     * 2. 检查是否未监听READ事件
     * 3. 用于判断通道是否暂停了读取操作
     *
     * @return 如果通道有效且未监听读取事件返回true
     */
    @Override
    public boolean isMute() {
        // 检查key是否有效且未注册READ事件
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    /**
     * 检查是否有已缓冲的字节等待处理
     * 
     * 实现说明：
     * 1. 明文传输层不维护中间缓冲区
     * 2. 所有数据直接通过SocketChannel处理
     * 3. 始终返回false表示没有缓冲的数据
     *
     * @return 始终返回false
     */
    @Override
    public boolean hasBytesBuffered() {
        // 不维护缓冲，直接返回false
        return false;
    }

    /**
     * 从FileChannel传输数据到此通道
     * 
     * 实现说明：
     * 1. 利用FileChannel的零拷贝特性
     * 2. 直接调用transferTo方法提高传输效率
     * 3. 适用于高效的文件传输场景
     *
     * @param fileChannel 源文件通道
     * @param position 文件中的起始位置
     * @param count 要传输的最大字节数
     * @return 实际传输的字节数
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        // 使用transferTo实现零拷贝传输
        return fileChannel.transferTo(position, count, socketChannel);
    }
}
