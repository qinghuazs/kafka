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

import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.Principal;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSession;

/*
 * SSL通信的传输层实现
 *
 * TLS v1.3 说明：
 *   https://tools.ietf.org/html/rfc8446#section-4.6 : 后握手消息
 *   "TLS允许在主握手完成后发送其他消息。
 *   这些消息使用握手内容类型，并使用相应的应用程序流量密钥进行加密。"
 */
public class SslTransportLayer implements TransportLayer {
    /**
     * SSL传输层的状态枚举
     * 用于跟踪SSL连接的生命周期状态
     */
    private enum State {
        // 初始状态，SSLEngine尚未初始化
        NOT_INITIALIZED,
        // SSLEngine处于握手模式，正在进行SSL握手
        HANDSHAKE,
        // SSL握手失败，连接将被终止
        HANDSHAKE_FAILED,
        // SSLEngine已完成握手，但对于TLS v1.3可能还有待处理的后握手消息
        POST_HANDSHAKE,
        // SSLEngine已完成握手，所有TLS v1.3的后握手消息都已处理
        // 对于TLS v1.3，当握手后的传入数据被处理时，我们将通道移至READY状态
        READY,
        // 通道正在关闭
        CLOSING
    }

    // TLS 1.3版本标识符
    private static final String TLS13 = "TLSv1.3";

    // 通道唯一标识符
    private final String channelId;
    // SSL引擎，用于处理SSL/TLS协议
    private final SSLEngine sslEngine;
    // 选择键，用于NIO操作
    private final SelectionKey key;
    // 底层的Socket通道
    private final SocketChannel socketChannel;
    // 通道元数据注册表
    private final ChannelMetadataRegistry metadataRegistry;
    // 日志记录器
    private final Logger log;

    // SSL握手状态
    private HandshakeStatus handshakeStatus;
    // SSL握手操作的结果
    private SSLEngineResult handshakeResult;
    // 当前传输层状态
    private State state;
    // SSL认证异常
    private SslAuthenticationException handshakeException;
    // 网络层读取缓冲区，用于存储加密数据
    private ByteBuffer netReadBuffer;
    // 网络层写入缓冲区，用于存储待发送的加密数据
    private ByteBuffer netWriteBuffer;
    // 应用层读取缓冲区，用于存储解密后的数据
    private ByteBuffer appReadBuffer;
    // 文件通道缓冲区，用于文件传输
    private ByteBuffer fileChannelBuffer;
    // 标记是否有缓冲的字节数据
    private boolean hasBytesBuffered;

    /**
     * 创建SSL传输层实例的工厂方法
     * @param channelId 通道标识符
     * @param key 选择键
     * @param sslEngine SSL引擎实例
     * @param metadataRegistry 元数据注册表
     * @return 新创建的SSL传输层实例
     * @throws IOException 如果创建过程中发生I/O错误
     */
    public static SslTransportLayer create(String channelId, SelectionKey key, SSLEngine sslEngine,
                                           ChannelMetadataRegistry metadataRegistry) throws IOException {
        return new SslTransportLayer(channelId, key, sslEngine, metadataRegistry);
    }

    /**
     * 构造函数，初始化SSL传输层
     * 注意：优先使用create()方法，此构造函数主要用于测试
     * 
     * @param channelId 通道标识符
     * @param key 选择键
     * @param sslEngine SSL引擎实例
     * @param metadataRegistry 元数据注册表
     */
    SslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine,
                      ChannelMetadataRegistry metadataRegistry) {
        // 初始化通道标识符
        this.channelId = channelId;
        // 设置选择键
        this.key = key;
        // 获取底层的Socket通道
        this.socketChannel = (SocketChannel) key.channel();
        // 设置SSL引擎
        this.sslEngine = sslEngine;
        // 初始化状态为未初始化
        this.state = State.NOT_INITIALIZED;
        // 设置元数据注册表
        this.metadataRegistry = metadataRegistry;

        // 创建日志上下文和日志记录器
        final LogContext logContext = new LogContext(String.format("[SslTransportLayer channelId=%s key=%s] ", channelId, key));
        this.log = logContext.logger(getClass());
    }

    /**
     * 启动SSL握手过程
     * 该方法可见性为protected，主要用于测试
     * 
     * @throws IOException 如果握手过程中发生I/O错误
     * @throws IllegalStateException 如果尝试多次调用此方法
     */
    protected void startHandshake() throws IOException {
        // 确保只能在未初始化状态下调用一次
        if (state != State.NOT_INITIALIZED)
            throw new IllegalStateException("startHandshake() can only be called once, state " + state);

        // 分配网络和应用层缓冲区
        this.netReadBuffer = ByteBuffer.allocate(netReadBufferSize());
        this.netWriteBuffer = ByteBuffer.allocate(netWriteBufferSize());
        this.appReadBuffer = ByteBuffer.allocate(applicationBufferSize());
        // 初始化缓冲区限制
        netWriteBuffer.limit(0);
        netReadBuffer.limit(0);

        // 更新状态为握手中
        state = State.HANDSHAKE;
        // 启动SSL握手
        sslEngine.beginHandshake();
        // 获取初始握手状态
        handshakeStatus = sslEngine.getHandshakeStatus();
    }

    /**
     * 检查传输层是否准备就绪
     * 当状态为POST_HANDSHAKE或READY时表示准备就绪
     * 
     * @return 如果传输层准备就绪返回true，否则返回false
     */
    @Override
    public boolean ready() {
        return state == State.POST_HANDSHAKE || state == State.READY;
    }

    /**
     * 完成Socket通道的连接过程
     * 调用底层socketChannel的finishConnect方法，并在连接成功后更新选择键的兴趣操作
     * 
     * @return 如果连接成功完成返回true
     * @throws IOException 如果连接过程中发生I/O错误
     */
    @Override
    public boolean finishConnect() throws IOException {
        // 完成底层Socket通道的连接
        boolean connected = socketChannel.finishConnect();
        if (connected)
            // 连接成功后，移除CONNECT事件的兴趣，添加READ事件的兴趣
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        return connected;
    }

    /**
     * 断开连接
     * 通过取消选择键来断开连接，这将使通道不再被选择器监控
     */
    @Override
    public void disconnect() {
        // 取消选择键，使通道不再被选择器监控
        key.cancel();
    }

    /**
     * 获取底层的Socket通道
     * @return 与此传输层关联的SocketChannel实例
     */
    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    /**
     * 获取选择键
     * @return 与此传输层关联的SelectionKey实例
     */
    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    /**
     * 检查通道是否打开
     * @return 如果底层Socket通道处于打开状态返回true
     */
    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    /**
     * 检查通道是否已连接
     * @return 如果底层Socket通道已建立连接返回true
     */
    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    /**
     * 发送SSL关闭消息并关闭Socket通道
     * 该方法执行完整的SSL关闭流程：
     * 1. 发送SSL关闭通知
     * 2. 关闭SSL引擎
     * 3. 关闭底层Socket通道
     * 4. 清理相关资源
     *
     * @throws IOException 如果关闭过程中发生I/O错误
     */
    @Override
    public void close() throws IOException {
        // 保存当前状态并更新为关闭状态
        State prevState = state;
        if (state == State.CLOSING) return;
        state = State.CLOSING;
        // 关闭SSL引擎的出站方向
        sslEngine.closeOutbound();
        try {
            // 只有在非初始状态且已连接时才发送关闭消息
            if (prevState != State.NOT_INITIALIZED && isConnected()) {
                // 确保所有待发送数据都已发送
                if (!flush(netWriteBuffer)) {
                    throw new IOException("Remaining data in the network buffer, can't send SSL close message.");
                }
                // 准备发送关闭消息的缓冲区
                netWriteBuffer.clear();
                // 执行关闭操作，将关闭消息包装到缓冲区
                SSLEngineResult wrapResult = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer);
                // 验证是否进入关闭状态
                if (wrapResult.getStatus() != SSLEngineResult.Status.CLOSED) {
                    throw new IOException("Unexpected status returned by SSLEngine.wrap, expected CLOSED, received " +
                            wrapResult.getStatus() + ". Will not send close message to peer.");
                }
                // 准备发送并刷新关闭消息
                netWriteBuffer.flip();
                flush(netWriteBuffer);
            }
        } catch (IOException ie) {
            // 记录发送SSL关闭消息失败的日志
            log.debug("Failed to send SSL Close message", ie);
        } finally {
            try {
                // 关闭SSL引擎的入站方向
                sslEngine.closeInbound();
            } catch (SSLException e) {
                // 这个日志用于调试目的，因为在这个点经常会发生异常
                // 这是由于对等方没有遵循TLS规范，未能发送close_notify警报
                // 即使他们发送了，目前在调用close()后我们也不会从socket读取数据
                log.debug("SSLEngine.closeInBound() raised an exception.", e);
            }
            // 关闭Socket和通道
            socketChannel.socket().close();
            socketChannel.close();
            // 清理缓冲区
            netReadBuffer = null;
            netWriteBuffer = null;
            appReadBuffer = null;
            // 如果存在文件通道缓冲区，则解除映射并清理
            if (fileChannelBuffer != null) {
                ByteBufferUnmapper.unmap("fileChannelBuffer", fileChannelBuffer);
                fileChannelBuffer = null;
            }
        }
    }

    /**
     * 检查是否有待发送的数据
     * 通过检查网络写入缓冲区是否还有剩余数据来判断
     * 
     * @return 如果网络写入缓冲区中还有未发送的数据返回true
     */
    @Override
    public boolean hasPendingWrites() {
        return netWriteBuffer.hasRemaining();
    }

    /**
     * 从Socket通道读取可用的字节数据到网络读取缓冲区
     * 该方法可见性为protected，主要用于测试
     * 
     * @return 读取的字节数
     * @throws IOException 如果读取过程中发生I/O错误
     */
    protected int readFromSocketChannel() throws IOException {
        // 从Socket通道读取数据到网络读取缓冲区
        return socketChannel.read(netReadBuffer);
    }

    /**
     * 将缓冲区数据刷新到网络，采用非阻塞方式
     * 该方法可见性为protected，主要用于测试
     * 
     * @param buf 要刷新的ByteBuffer
     * @return 如果缓冲区已完全清空返回true，否则返回false
     * @throws IOException 如果写入过程中发生I/O错误
     */
    protected boolean flush(ByteBuffer buf) throws IOException {
        // 获取缓冲区中剩余的字节数
        int remaining = buf.remaining();
        if (remaining > 0) {
            // 尝试将数据写入到Socket通道
            int written = socketChannel.write(buf);
            // 只有当所有数据都写入成功时才返回true
            return written >= remaining;
        }
        return true;
    }

    /**
     * 执行非阻塞的SSL握手过程
     * 在发送应用数据(Kafka协议)之前，客户端和Kafka代理必须完成SSL握手。
     * 在握手过程中，SSLEngine生成加密数据，这些数据将通过socketChannel传输。
     * 每个SSLEngine操作都会生成SSLEngineResult，其中handshakeStatus字段用于
     * 确定需要执行什么操作来推进握手过程。
     * 
     * 典型的握手过程如下：
     * +-------------+----------------------------------+-------------+
     * |  客户端     |  SSL/TLS消息                     | 握手状态     |
     * +-------------+----------------------------------+-------------+
     * | wrap()      | ClientHello                      | NEED_UNWRAP |
     * | unwrap()    | ServerHello/Cert/ServerHelloDone | NEED_WRAP   |
     * | wrap()      | ClientKeyExchange                | NEED_WRAP   |
     * | wrap()      | ChangeCipherSpec                 | NEED_WRAP   |
     * | wrap()      | Finished                         | NEED_UNWRAP |
     * | unwrap()    | ChangeCipherSpec                 | NEED_UNWRAP |
     * | unwrap()    | Finished                         | FINISHED    |
     * +-------------+----------------------------------+-------------+
     *
     * @throws IOException 如果读/写操作失败
     * @throws SslAuthenticationException 如果握手过程中出现SSL异常
     */
    @Override
    public void handshake() throws IOException {
        // 如果状态为未初始化，则启动握手过程
        if (state == State.NOT_INITIALIZED) {
            try {
                startHandshake();
            } catch (SSLException e) {
                // 处理握手启动失败的情况
                maybeProcessHandshakeFailure(e, false, null);
            }
        }
        // 如果已经准备就绪，则不允许重新协商
        if (ready())
            throw renegotiationException();
        // 如果通道正在关闭，则抛出关闭异常
        if (state == State.CLOSING)
            throw closingException();

        int read = 0;
        // 检查通道是否可读
        boolean readable = key.isReadable();
        try {
            // 在尝试写入之前读取所有可用字节
            // 这确保即使写入失败，也能处理对等方报告的握手失败
            // (因为如果握手失败，对等方会关闭连接)
            if (readable)
                read = readFromSocketChannel();

            // 执行握手过程
            doHandshake();
            // 如果握手完成并准备就绪，更新缓冲字节状态
            if (ready())
                updateBytesBuffered(true);
        } catch (SSLException e) {
            // 处理SSL异常导致的握手失败
            maybeProcessHandshakeFailure(e, true, null);
        } catch (IOException e) {
            // 检查是否有待处理的SSL认证异常
            maybeThrowSslAuthenticationException();

            // 这个异常可能是由写入操作引起的
            // 如果缓冲区中有数据需要解包，或者socket通道中有数据可读并需要解包
            // 则处理这些数据以确保报告任何SSL握手异常
            try {
                do {
                    log.trace("处理来自对等方的可用字节, netReadBuffer {} netWriterBuffer {} handshakeStatus {} readable? {}",
                        netReadBuffer, netWriteBuffer, handshakeStatus, readable);
                    // 尝试在失败后进行握手包装
                    handshakeWrapAfterFailure(false);
                    // 尝试解包握手数据
                    handshakeUnwrap(false, true);
                } while (readable && readFromSocketChannel() > 0);
            } catch (SSLException e1) {
                // 处理解包过程中的SSL异常
                maybeProcessHandshakeFailure(e1, false, e);
            }

            // 如果执行到这里，说明不是握手失败，抛出原始的IO异常
            throw e;
        }

        // 如果从socket读取失败，抛出待处理的握手异常或EOF异常
        if (read == -1) {
            maybeThrowSslAuthenticationException();
            throw new EOFException("握手过程中遇到EOF，握手状态为 " + handshakeStatus);
        }
    }

    /**
     * 执行SSL握手的核心逻辑，处理不同的握手状态
     * 这是一个非阻塞的实现，通过状态机模式处理SSL握手的各个阶段
     * 
     * @throws IOException 如果在握手过程中发生I/O错误
     */
    @SuppressWarnings("fallthrough")
    private void doHandshake() throws IOException {
        // 检查通道的读写状态
        boolean read = key.isReadable();
        boolean write = key.isWritable();
        // 获取当前的握手状态
        handshakeStatus = sslEngine.getHandshakeStatus();
        // 尝试刷新写缓冲区，如果无法完全刷新，则设置写事件感兴趣
        if (!flush(netWriteBuffer)) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            return;
        }
        // 检查是否有待处理的握手异常
        maybeThrowSslAuthenticationException();

        // 根据不同的握手状态执行相应的操作
        switch (handshakeStatus) {
            case NEED_TASK:
                // 需要执行SSLEngine的委托任务
                log.trace("SSL握手需要执行任务 channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                // 运行所有待处理的任务并获取新的握手状态
                handshakeStatus = runDelegatedTasks();
                break;
            case NEED_WRAP:
                // 需要包装数据以发送给对方
                log.trace("SSL握手需要包装数据 channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                // 执行握手包装操作
                handshakeResult = handshakeWrap(write);
                // 处理包装结果的不同状态
                if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    // 写缓冲区空间不足，需要扩容
                    int currentNetWriteBufferSize = netWriteBufferSize();
                    netWriteBuffer.compact();
                    netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, currentNetWriteBufferSize);
                    netWriteBuffer.flip();
                    // 验证扩容后的缓冲区大小是否合理
                    if (netWriteBuffer.limit() >= currentNetWriteBufferSize) {
                        throw new IllegalStateException("缓冲区溢出：可用数据大小 (" + netWriteBuffer.limit() +
                                                        ") >= 网络缓冲区大小 (" + currentNetWriteBufferSize + ")");
                    }
                } else if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    // 在WRAP操作中不应该出现BUFFER_UNDERFLOW状态
                    throw new IllegalStateException("在握手WRAP阶段不应该收到BUFFER_UNDERFLOW状态");
                } else if (handshakeResult.getStatus() == Status.CLOSED) {
                    throw new EOFException();
                }
                log.trace("SSL握手WRAP操作完成 channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                       channelId, handshakeResult, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                // 如果不需要立即解包或无法刷新写缓冲区，则中断处理
                if (handshakeStatus != HandshakeStatus.NEED_UNWRAP || !flush(netWriteBuffer)) {
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    break;
                }
            case NEED_UNWRAP:
                // 需要解包从对方收到的数据
                log.trace("SSL握手需要解包数据 channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                do {
                    // 执行握手解包操作
                    handshakeResult = handshakeUnwrap(read, false);
                    if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                        // 应用缓冲区空间不足，需要扩容
                        int currentAppBufferSize = applicationBufferSize();
                        appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentAppBufferSize);
                        // 验证扩容后的缓冲区大小是否合理
                        if (appReadBuffer.position() > currentAppBufferSize) {
                            throw new IllegalStateException("缓冲区下溢：可用数据大小 (" + appReadBuffer.position() +
                                                           ") > 数据包缓冲区大小 (" + currentAppBufferSize + ")");
                        }
                    }
                } while (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW);
                // 处理解包结果的不同状态
                if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    // 网络读缓冲区空间不足，需要扩容
                    int currentNetReadBufferSize = netReadBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
                    if (netReadBuffer.position() >= currentNetReadBufferSize) {
                        throw new IllegalStateException("存在可用数据时发生缓冲区下溢");
                    }
                } else if (handshakeResult.getStatus() == Status.CLOSED) {
                    throw new EOFException("在握手UNWRAP阶段SSL连接已关闭");
                }
                log.trace("SSL握手UNWRAP操作完成 channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, handshakeResult, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());

                // 如果握手未完成，更新通道的事件兴趣
                // 握手完成后，socket通道中没有需要读写的数据
                // 如果不在这里处理handshakeFinished，选择器将不会触发此通道
                if (handshakeStatus != HandshakeStatus.FINISHED) {
                    if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
                        // 需要发送数据，添加写事件兴趣
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    } else if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
                        // 需要接收数据，移除写事件兴趣
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                    }
                    break;
                }
            case FINISHED:
                // 握手完成，执行完成处理
                handshakeFinished();
                break;
            case NOT_HANDSHAKING:
                // 未在握手状态，执行完成处理
                handshakeFinished();
                break;
            default:
                throw new IllegalStateException(String.format("意外的握手状态 [%s]", handshakeStatus));
        }
    }

    /**
     * 创建SSL重协商异常
     * SSL/TLS重协商是一种在现有连接上重新进行握手的机制，但在本实现中不支持此功能
     * 
     * @return 包含不支持重协商消息的SSLHandshakeException
     */
    private SSLHandshakeException renegotiationException() {
        return new SSLHandshakeException("Renegotiation is not supported");
    }

    /**
     * 创建通道关闭状态异常
     * 当尝试在已关闭的通道上执行操作时抛出此异常
     * 
     * @throws IllegalStateException 通道处于关闭状态时的异常
     */
    private IllegalStateException closingException() {
        throw new IllegalStateException("Channel is in closing state");
    }

    /**
     * 执行SSL引擎委托的任务
     * 在SSL握手过程中，某些操作（如密钥计算）可能需要在单独的线程中执行
     * 这些任务通过SSLEngine的getDelegatedTask()方法获取并执行
     * 
     * @return 执行完所有委托任务后的握手状态
     */
    private HandshakeStatus runDelegatedTasks() {
        // 循环执行所有待处理的任务
        for (;;) {
            // 获取下一个待执行的任务
            Runnable task = delegatedTask();
            // 如果没有更多任务，退出循环
            if (task == null) {
                break;
            }
            // 执行当前任务
            task.run();
        }
        // 返回执行完所有任务后的握手状态
        return sslEngine.getHandshakeStatus();
    }

    /**
     * 处理SSL握手完成的状态
     * 当握手完成时，更新通道状态和选择键的兴趣操作
     * 同时处理TLS 1.3的后握手消息状态
     * 
     * @throws IOException 如果握手状态检查失败
     */
    private void handshakeFinished() throws IOException {
        // SSLEngine的getHandshakeStatus是瞬态的，不会正确记录FINISHED状态
        // 它可能在握手完成后从FINISHED状态转换为NOT_HANDSHAKING
        // 因此我们还需要检查handshakeResult.getHandshakeStatus()来确认握手是否完成
        if (handshakeResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
            // 如果网络写缓冲区还有数据未发送完
            if (netWriteBuffer.hasRemaining())
                // 添加写事件兴趣
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            else {
                // 获取SSL会话信息
                SSLSession session = sslEngine.getSession();
                // 根据协议版本设置状态
                // 对于TLS 1.3，设置为POST_HANDSHAKE以处理可能的后握手消息
                // 对于其他版本，直接设置为READY
                state = session.getProtocol().equals(TLS13) ? State.POST_HANDSHAKE : State.READY;
                // 移除写事件兴趣
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                // 记录握手成功的详细信息
                log.debug("SSL handshake completed successfully with peerHost '{}' peerPort {} peerPrincipal '{}' protocol '{}' cipherSuite '{}'",
                        session.getPeerHost(), session.getPeerPort(), peerPrincipal(), session.getProtocol(), session.getCipherSuite());
                // 注册加密套件信息
                metadataRegistry.registerCipherInformation(
                    new CipherInformation(session.getCipherSuite(),  session.getProtocol()));
            }

            // 记录握手完成时各缓冲区的状态
            log.trace("SSLHandshake FINISHED channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} ",
                      channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
        } else {
            // 如果握手状态不正确，抛出异常
            throw new IOException("NOT_HANDSHAKING during handshake");
        }
    }

    /**
     * 执行SSL握手的包装操作
     * 将应用数据包装（加密）成网络数据
     * 
     * @param doWrite 是否立即将包装后的数据写入通道
     * @return SSL引擎的包装操作结果
     * @throws IOException 如果包装过程中发生I/O错误
     */
    private SSLEngineResult handshakeWrap(boolean doWrite) throws IOException {
        // 记录包装操作的开始
        log.trace("SSLHandshake handshakeWrap {}", channelId);
        // 确保网络写缓冲区为空
        if (netWriteBuffer.hasRemaining())
            throw new IllegalStateException("handshakeWrap called with netWriteBuffer not empty");
        // 清空网络写缓冲区
        netWriteBuffer.clear();
        SSLEngineResult result;
        try {
            // 执行SSL包装操作，将空缓冲区包装到网络写缓冲区
            result = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer);
        } finally {
            // 准备写入数据，将缓冲区从写模式切换到读模式
            netWriteBuffer.flip();
        }
        // 更新握手状态
        handshakeStatus = result.getHandshakeStatus();
        // 如果需要执行委托任务，则执行
        if (result.getStatus() == SSLEngineResult.Status.OK &&
            result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
            handshakeStatus = runDelegatedTasks();
        }

        // 如果需要立即写入，则刷新缓冲区
        if (doWrite) flush(netWriteBuffer);
        return result;
    }

    /**
     * 执行SSL握手的解包操作
     * 将接收到的网络数据解包（解密）成应用数据
     * 此方法可见性为包级别，用于测试
     * 
     * @param doRead 是否从通道读取更多数据
     * @param ignoreHandshakeStatus 是否忽略握手状态继续解包
     * @return SSL引擎的解包操作结果
     * @throws IOException 如果解包过程中发生I/O错误
     */
    SSLEngineResult handshakeUnwrap(boolean doRead, boolean ignoreHandshakeStatus) throws IOException {
        // 记录解包操作的开始
        log.trace("SSLHandshake handshakeUnwrap {}", channelId);
        SSLEngineResult result;
        int read = 0;
        // 如果需要读取数据，则从通道读取
        if (doRead)
            read = readFromSocketChannel();
        boolean cont;
        do {
            // 准备处理接收到的数据
            int position = netReadBuffer.position();
            // 切换到读模式
            netReadBuffer.flip();
            // 执行SSL解包操作
            result = sslEngine.unwrap(netReadBuffer, appReadBuffer);
            // 压缩网络读缓冲区
            netReadBuffer.compact();
            // 更新握手状态
            handshakeStatus = result.getHandshakeStatus();
            // 如果需要执行委托任务，则执行
            if (result.getStatus() == SSLEngineResult.Status.OK &&
                result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
                handshakeStatus = runDelegatedTasks();
            }
            // 确定是否需要继续解包
            cont = (result.getStatus() == SSLEngineResult.Status.OK &&
                    handshakeStatus == HandshakeStatus.NEED_UNWRAP) ||
                    (ignoreHandshakeStatus && netReadBuffer.position() != position);
            // 记录当前解包状态
            log.trace("SSLHandshake handshakeUnwrap: handshakeStatus {} status {}", handshakeStatus, result.getStatus());
        } while (cont);

        // 如果读取到EOF，在处理完已接收的数据后抛出异常
        // 这确保握手失败能够被正确报告
        if (read == -1)
            throw new EOFException("EOF during handshake, handshake status is " + handshakeStatus);

        return result;
    }


    /**
     * 从SSL通道读取字节序列到指定的缓冲区。尽可能多地读取数据，直到目标缓冲区已满或套接字中没有更多数据。
     * 这是SSL传输层的核心读取方法，实现了加密数据的读取、解密和缓冲管理。
     *
     * 实现细节：
     * 1. 首先检查是否有已解密的数据在应用层缓冲区中
     * 2. 然后从网络读取加密数据并进行解密
     * 3. 处理各种SSL引擎状态（如缓冲区溢出、数据不足等）
     * 4. 支持TLS 1.3的后握手消息处理
     *
     * @param dst 用于存储读取数据的目标缓冲区
     * @return 读取的字节数，如果通道已到达流末尾则返回-1，如果当前没有可用数据则返回0
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        // 如果通道正在关闭，返回-1表示已到达流末尾
        if (state == State.CLOSING) return -1;
        // 如果通道未就绪（未完成握手），返回0
        else if (!ready()) return 0;

        // 如果应用层缓冲区中有未读的解密数据，先读取这些数据
        int read = 0;
        if (appReadBuffer.position() > 0) {
            read = readFromAppBuffer(dst);
        }

        // 标记是否从网络读取了新数据和通道是否已关闭
        boolean readFromNetwork = false;
        boolean isClosed = false;
        
        // 主循环：每次最多从套接字读取一次数据
        while (dst.remaining() > 0) {
            // 确保网络读取缓冲区容量足够
            int netread = 0;
            netReadBuffer = Utils.ensureCapacity(netReadBuffer, netReadBufferSize());
            // 如果网络读取缓冲区有剩余空间，尝试读取加密数据
            if (netReadBuffer.remaining() > 0) {
                netread = readFromSocketChannel();
                if (netread > 0)
                    readFromNetwork = true;
            }

            // 处理网络读取缓冲区中的加密数据
            while (netReadBuffer.position() > 0) {
                // 准备读取数据
                netReadBuffer.flip();
                SSLEngineResult unwrapResult;
                try {
                    // 使用SSL引擎解密数据
                    unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer);
                    // TLS 1.3特性：检查是否已完成后握手消息处理
                    if (state == State.POST_HANDSHAKE && appReadBuffer.position() != 0) {
                        state = State.READY;
                    }
                } catch (SSLException e) {
                    // TLS 1.3特性：处理后握手消息过程中的SSL异常
                    if (state == State.POST_HANDSHAKE) {
                        state = State.HANDSHAKE_FAILED;
                        throw new SslAuthenticationException("Failed to process post-handshake messages", e);
                    } else
                        throw e;
                }
                // 压缩网络读取缓冲区，准备下一次读取
                netReadBuffer.compact();
                
                // 处理TLS重协商：TLS 1.3以下版本不支持重协商，但允许TLS 1.3的密钥更新
                if (unwrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING &&
                        unwrapResult.getHandshakeStatus() != HandshakeStatus.FINISHED &&
                        unwrapResult.getStatus() == Status.OK &&
                        !sslEngine.getSession().getProtocol().equals(TLS13)) {
                    log.error("Renegotiation requested, but it is not supported, channelId {}, " +
                        "appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} handshakeStatus {}", channelId,
                        appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position(), unwrapResult.getHandshakeStatus());
                    throw renegotiationException();
                }

                // 根据SSL引擎的解密结果进行相应处理
                if (unwrapResult.getStatus() == Status.OK) {
                    // 解密成功，读取解密后的数据
                    read += readFromAppBuffer(dst);
                } else if (unwrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    // 应用层缓冲区空间不足，需要扩容
                    int currentApplicationBufferSize = applicationBufferSize();
                    appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentApplicationBufferSize);
                    if (appReadBuffer.position() >= currentApplicationBufferSize) {
                        throw new IllegalStateException("Buffer overflow when available data size (" + appReadBuffer.position() +
                                                        ") >= application buffer size (" + currentApplicationBufferSize + ")");
                    }

                    // 如果目标缓冲区还有空间，继续读取现有的解密数据
                    if (dst.hasRemaining())
                        read += readFromAppBuffer(dst);
                    else
                        break;
                } else if (unwrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    // 网络层数据不足以解密一个完整的SSL记录，需要读取更多数据
                    int currentNetReadBufferSize = netReadBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
                    if (netReadBuffer.position() >= currentNetReadBufferSize) {
                        throw new IllegalStateException("Buffer underflow when available data size (" + netReadBuffer.position() +
                                                        ") > packet buffer size (" + currentNetReadBufferSize + ")");
                    }
                    break;
                } else if (unwrapResult.getStatus() == Status.CLOSED) {
                    // SSL连接已关闭
                    if (appReadBuffer.position() == 0 && read == 0)
                        throw new EOFException();
                    else {
                        isClosed = true;
                        break;
                    }
                }
            }
            // 处理读取错误和连接关闭情况
            if (read == 0 && netread < 0)
                throw new EOFException("EOF during read");
            if (netread <= 0 || isClosed)
                break;
        }
        // 更新缓冲状态并返回读取的字节数
        updateBytesBuffered(readFromNetwork || read > 0);
        return read;
    }


    /**
     * 从SSL通道读取字节序列到给定的缓冲区数组中。
     * 这是一个便捷方法，它调用read(ByteBuffer[], int, int)方法来处理整个缓冲区数组。
     *
     * @param dsts 用于存储读取数据的目标缓冲区数组
     * @return 读取的字节总数，可能为0，如果通道已到达流末尾则返回-1
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }


    /**
     * 从SSL通道读取字节序列到给定缓冲区数组的指定子序列中。
     * 这个方法允许在一次调用中向多个缓冲区读取数据，实现了分散读取（Scatter Read）模式。
     * 
     * 实现细节：
     * 1. 首先验证输入参数的有效性
     * 2. 按顺序遍历指定范围内的缓冲区
     * 3. 只要缓冲区还有剩余空间就继续读取数据
     * 4. 累计所有成功读取的字节数
     *
     * @param dsts 用于存储读取数据的目标缓冲区数组
     * @param offset 第一个要使用的缓冲区在数组中的偏移量，必须非负且不大于dsts.length
     * @param length 要使用的最大缓冲区数量，必须非负且不大于dsts.length - offset
     * @return 读取的字节总数，可能为0，如果通道已到达流末尾则返回-1
     * @throws IOException 如果发生I/O错误
     * @throws IndexOutOfBoundsException 如果offset和length参数无效
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        // 验证输入参数的有效性
        if ((offset < 0) || (length < 0) || (offset > dsts.length - length))
            throw new IndexOutOfBoundsException();

        // 记录总共读取的字节数
        int totalRead = 0;
        int i = offset;
        // 遍历指定范围内的缓冲区
        while (i < offset + length) {
            // 如果当前缓冲区还有剩余空间
            if (dsts[i].hasRemaining()) {
                // 读取数据到当前缓冲区
                int read = read(dsts[i]);
                if (read > 0)
                    totalRead += read;
                else
                    break;  // 如果无法读取更多数据，终止循环
            }
            // 如果当前缓冲区已满，移动到下一个缓冲区
            if (!dsts[i].hasRemaining()) {
                i++;
            }
        }
        return totalRead;
    }


    /**
     * 将字节序列从给定的缓冲区写入到此通道
     * 实现了SSL/TLS加密的写入操作，包括数据加密和传输
     * 
     * 应用场景：
     * 1. 用于安全传输应用层数据
     * 2. 支持TLS 1.3的密钥更新
     * 3. 处理SSL缓冲区溢出情况
     *
     * @param src 包含要写入数据的源缓冲区
     * @return 从src读取的字节数，可能为零；如果通道已到达流末尾则返回-1
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        // 如果通道正在关闭，抛出关闭异常
        if (state == State.CLOSING)
            throw closingException();
        // 如果传输层未就绪（握手未完成），返回0
        if (!ready())
            return 0;

        // 记录已写入的字节数
        int written = 0;
        // 当网络写缓冲区可以刷新且源缓冲区还有剩余数据时继续循环
        while (flush(netWriteBuffer) && src.hasRemaining()) {
            // 清空网络写缓冲区，准备接收新的加密数据
            netWriteBuffer.clear();
            // 使用SSL引擎将源数据加密并写入网络缓冲区
            SSLEngineResult wrapResult = sslEngine.wrap(src, netWriteBuffer);
            // 翻转缓冲区，准备读取加密后的数据
            netWriteBuffer.flip();

            // 对TLS 1.3以下版本拒绝重新协商，但允许TLS 1.3的密钥更新
            if (wrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING &&
                    wrapResult.getStatus() == Status.OK &&
                    !sslEngine.getSession().getProtocol().equals(TLS13)) {
                throw renegotiationException();
            }

            // 处理SSL引擎包装结果的不同状态
            if (wrapResult.getStatus() == Status.OK) {
                // 正常情况：更新已处理的字节数
                written += wrapResult.bytesConsumed();
            } else if (wrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                // 缓冲区溢出：扩展网络写缓冲区容量并重试
                netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, netWriteBufferSize());
                netWriteBuffer.position(netWriteBuffer.limit());
            } else if (wrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                // 写入操作不应该出现缓冲区下溢
                throw new IllegalStateException("SSL BUFFER_UNDERFLOW during write");
            } else if (wrapResult.getStatus() == Status.CLOSED) {
                // SSL连接已关闭
                throw new EOFException();
            }
        }
        return written;
    }

    /**
     * 将字节序列从给定的多个缓冲区写入到此通道
     * 支持批量写入操作，可以从多个缓冲区依次写入数据
     * 
     * 应用场景：
     * 1. 批量发送消息
     * 2. 大文件传输
     * 3. 聚合写入操作
     *
     * @param srcs 包含要写入数据的缓冲区数组
     * @param offset 第一个要处理的缓冲区在数组中的偏移量；必须非负且不大于srcs.length
     * @param length 要访问的缓冲区的最大数量；必须非负且不大于srcs.length - offset
     * @return 写入的字节总数，可能为零
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        // 验证参数有效性
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();
        
        // 记录写入的总字节数
        int totalWritten = 0;
        // 当前处理的缓冲区索引
        int i = offset;
        
        // 循环处理指定范围内的所有缓冲区
        while (i < offset + length) {
            // 如果当前缓冲区有剩余数据或有待处理的写入，则继续写入
            if (srcs[i].hasRemaining() || hasPendingWrites()) {
                // 写入当前缓冲区的数据
                int written = write(srcs[i]);
                if (written > 0) {
                    totalWritten += written;
                }
            }
            // 如果当前缓冲区已处理完且没有待处理的写入，移动到下一个缓冲区
            if (!srcs[i].hasRemaining() && !hasPendingWrites()) {
                i++;
            } else {
                // 如果无法写入当前缓冲区，可能是因为达到了socket发送缓冲区大小上限
                break;
            }
        }
        return totalWritten;
    }

    /**
     * 将字节序列从给定的所有缓冲区写入到此通道
     * 是write(ByteBuffer[], int, int)的便捷方法，处理整个缓冲区数组
     * 
     * 应用场景：
     * 1. 需要写入所有提供的缓冲区数据时
     * 2. 简化批量写入操作
     *
     * @param srcs 包含要写入数据的缓冲区数组
     * @return 被SSL引擎wrap方法处理的字节数，可能为零
     * @throws IOException 如果发生I/O错误
     */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        // 调用完整版本的write方法，处理整个缓冲区数组
        return write(srcs, 0, srcs.length);
    }


    /**
     * 获取远程主机的SSL会话对等主体
     * 如果对等方未经过认证，则返回匿名主体
     * 
     * @return 对等方的Principal对象，如果未认证则返回KafkaPrincipal.ANONYMOUS
     */
    public Principal peerPrincipal() {
        try {
            // 尝试获取SSL会话中的对等方主体信息
            return sslEngine.getSession().getPeerPrincipal();
        } catch (SSLPeerUnverifiedException se) {
            // 如果对等方未经过认证，记录日志并返回匿名主体
            log.debug("SSL peer is not authenticated, returning ANONYMOUS instead");
            return KafkaPrincipal.ANONYMOUS;
        }
    }

    /**
     * 获取已建立的SSL会话
     * 在握手完成后可以获取会话信息，包括加密套件、协议版本等
     * 
     * @return SSL会话对象
     * @throws IllegalStateException 如果握手尚未完成
     */
    public SSLSession sslSession() throws IllegalStateException {
        return sslEngine.getSession();
    }

    /**
     * 向传输层的SelectionKey添加感兴趣的操作
     * 用于设置通道关注的I/O事件类型（如读、写、连接等）
     * 
     * @param ops 要添加的SelectionKey操作位掩码
     * @throws CancelledKeyException 如果选择键已失效
     * @throws IllegalStateException 如果SSL握手尚未完成
     */
    @Override
    public void addInterestOps(int ops) {
        // 检查选择键是否有效
        if (!key.isValid())
            throw new CancelledKeyException();
        // 确保SSL握手已完成
        else if (!ready())
            throw new IllegalStateException("handshake is not completed");

        // 使用位或操作添加新的兴趣操作
        key.interestOps(key.interestOps() | ops);
    }

    /**
     * 从传输层的SelectionKey移除感兴趣的操作
     * 用于取消对特定I/O事件的关注
     * 
     * @param ops 要移除的SelectionKey操作位掩码
     * @throws CancelledKeyException 如果选择键已失效
     * @throws IllegalStateException 如果SSL握手尚未完成
     */
    @Override
    public void removeInterestOps(int ops) {
        // 检查选择键是否有效
        if (!key.isValid())
            throw new CancelledKeyException();
        // 确保SSL握手已完成
        else if (!ready())
            throw new IllegalStateException("handshake is not completed");

        // 使用位与非操作移除指定的兴趣操作
        key.interestOps(key.interestOps() & ~ops);
    }

    /**
     * 获取SSL引擎的委托任务
     * SSL引擎可能需要执行一些耗时的操作（如密钥计算），这些操作被封装为委托任务
     * 
     * @return 需要执行的委托任务，如果没有则返回null
     */
    protected Runnable delegatedTask() {
        return sslEngine.getDelegatedTask();
    }

    /**
     * 将应用层读取缓冲区中的解密数据传输到目标缓冲区
     * 用于处理已解密的数据传输
     * 
     * @param dst 目标ByteBuffer
     * @return 传输的字节数
     */
    private int readFromAppBuffer(ByteBuffer dst) {
        // 翻转缓冲区，准备读取
        appReadBuffer.flip();
        // 计算可传输的字节数（源和目标缓冲区剩余空间的较小值）
        int remaining = Math.min(appReadBuffer.remaining(), dst.remaining());
        if (remaining > 0) {
            // 保存原始限制
            int limit = appReadBuffer.limit();
            // 设置新的限制以只传输计算出的字节数
            appReadBuffer.limit(appReadBuffer.position() + remaining);
            // 将数据传输到目标缓冲区
            dst.put(appReadBuffer);
            // 恢复原始限制
            appReadBuffer.limit(limit);
        }
        // 压缩缓冲区，准备下次写入
        appReadBuffer.compact();
        return remaining;
    }

    /**
     * 获取网络读取缓冲区的大小
     * 用于存储加密的网络数据
     * 
     * @return 网络包缓冲区大小（字节）
     */
    protected int netReadBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    /**
     * 获取网络写入缓冲区的大小
     * 用于存储待发送的加密数据
     * 
     * @return 网络包缓冲区大小（字节）
     */
    protected int netWriteBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    /**
     * 获取应用层缓冲区的大小
     * 用于存储解密后的应用数据
     * 
     * @return 应用数据缓冲区大小（字节）
     */
    protected int applicationBufferSize() {
        return sslEngine.getSession().getApplicationBufferSize();
    }

    /**
     * 获取网络读取缓冲区
     * 主要用于测试目的
     * 
     * @return 网络读取缓冲区
     */
    protected ByteBuffer netReadBuffer() {
        return netReadBuffer;
    }

    /**
     * 获取应用层读取缓冲区
     * 主要用于测试目的
     * 
     * @return 应用层读取缓冲区
     */
    protected ByteBuffer appReadBuffer() {
        return appReadBuffer;
    }

    /**
     * 处理SSL握手失败
     * 将SSL异常转换为认证失败异常，以避免客户端重试
     * 如果flush为true，会在传播异常前尝试刷新所有待发送的数据
     * 
     * @param sslException SSL异常
     * @param flush 是否在抛出异常前刷新缓冲区
     */
    private void handshakeFailure(SSLException sslException, boolean flush) {
        // 释放SSLEngine管理的所有资源（如内部缓冲区）
        log.debug("SSL Handshake failed", sslException);
        // 关闭SSL引擎的出站方向
        sslEngine.closeOutbound();
        try {
            // 关闭SSL引擎的入站方向
            sslEngine.closeInbound();
        } catch (SSLException e) {
            // 记录关闭入站方向时的异常
            log.debug("SSLEngine.closeInBound() raised an exception.", e);
        }

        // 设置状态为握手失败
        state = State.HANDSHAKE_FAILED;
        // 创建SSL认证异常
        handshakeException = new SslAuthenticationException("SSL handshake failed", sslException);

        // 尝试刷新所有待发送的数据
        // 如果刷新未完成，延迟异常处理直到数据刷新完成
        // 如果写入失败（因为远端已关闭连接），记录IO异常并继续处理握手失败
        if (!flush || handshakeWrapAfterFailure(flush))
            throw handshakeException;
        else
            log.debug("Delay propagation of handshake exception till {} bytes remaining are flushed", netWriteBuffer.remaining());
    }

    /**
     * 处理SSL握手失败的异常
     * 根据异常类型和消息决定是否将异常转换为不可重试的认证失败
     * 
     * SSL握手失败通常会抛出以下异常：
     * 1. SSLHandshakeException - 握手协议错误
     * 2. SSLProtocolException - 协议违规
     * 3. SSLPeerUnverifiedException - 对等方验证失败
     * 4. SSLKeyException - 密钥相关错误
     * 
     * SSL引擎也可能抛出基类SSLException：
     * a) 当没有匹配的密码套件、TLS版本不兼容或私钥无效时：
     *    "Unrecognized SSL message, plaintext connection?"
     * b) 当服务器在握手期间正常关闭连接时：
     *    "Received close_notify during handshake"
     * 
     * @param sslException SSL异常
     * @param flush 是否刷新缓冲区
     * @param ioException 可能的IO异常
     * @throws IOException 如果发生IO错误
     */
    private void maybeProcessHandshakeFailure(SSLException sslException, boolean flush, IOException ioException) throws IOException {
        // 检查是否是已知的握手失败类型或包含特定的错误消息
        if (sslException instanceof SSLHandshakeException || sslException instanceof SSLProtocolException ||
                sslException instanceof SSLPeerUnverifiedException || sslException instanceof SSLKeyException ||
                sslException.getMessage().contains("Unrecognized SSL message") ||
                sslException.getMessage().contains("Received fatal alert: "))
            // 处理为不可重试的认证失败
            handshakeFailure(sslException, flush);
        else if (ioException == null)
            // 如果没有IO异常，直接抛出SSL异常
            throw sslException;
        else {
            // 如果有IO异常，记录SSL异常并抛出原始的IO异常
            log.debug("SSLException while unwrapping data after IOException, original IOException will be propagated", sslException);
            throw ioException;
        }
    }

    /**
     * 检查并抛出已存在的SSL认证异常
     * 用于确保握手失败被正确处理
     * 
     * @throws SslAuthenticationException 如果之前发生过握手失败
     */
    private void maybeThrowSslAuthenticationException() {
        if (handshakeException != null)
            throw handshakeException;
    }

    /**
     * 在发生SSLException或IOException后执行握手包装操作
     * 
     * 处理两种情况：
     * 1. doWrite=false：对等方已断开连接，无法发送更多数据
     *    执行待处理的包装操作以便解包已有的对等方数据
     * 
     * 2. doWrite=true：处理SSLException，执行包装并刷新数据
     *    通知对等方握手失败
     * 
     * @param doWrite 是否执行写入操作
     * @return 如果不需要更多包装且所有数据已刷新或丢弃则返回true
     */
    private boolean handshakeWrapAfterFailure(boolean doWrite) {
        try {
            // 记录握手状态和写入标志
            log.trace("handshakeWrapAfterFailure status {} doWrite {}", handshakeStatus, doWrite);
            // 当需要包装且（不需要写入或缓冲区已刷新）时继续循环
            while (handshakeStatus == HandshakeStatus.NEED_WRAP && (!doWrite || flush(netWriteBuffer))) {
                if (!doWrite)
                    // 如果不需要写入，清空写缓冲区
                    clearWriteBuffer();
                // 执行握手包装操作
                handshakeWrap(doWrite);
            }
        } catch (Exception e) {
            // 记录包装和刷新失败的日志
            log.debug("Failed to wrap and flush all bytes before closing channel", e);
            clearWriteBuffer();
        }
        if (!doWrite)
            clearWriteBuffer();
        // 返回写缓冲区是否已清空
        return !netWriteBuffer.hasRemaining();
    }

    /**
     * 清空网络写入缓冲区
     * 用于丢弃未发送的数据，通常在连接断开时调用
     */
    private void clearWriteBuffer() {
        // 如果缓冲区中还有数据，记录日志
        if (netWriteBuffer.hasRemaining())
            log.debug("Discarding write buffer {} since peer has disconnected", netWriteBuffer);
        // 重置缓冲区位置和限制
        netWriteBuffer.position(0);
        netWriteBuffer.limit(0);
    }

    /**
     * 检查通道是否处于静默状态
     * 当选择键有效且没有设置读取兴趣时，通道处于静默状态
     * 
     * @return 如果通道处于静默状态返回true
     */
    @Override
    public boolean isMute() {
        // 检查选择键是否有效且未设置读取兴趣位
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    /**
     * 检查是否有缓冲的字节数据
     * 用于判断是否有待处理的数据在缓冲区中
     * 
     * @return 如果有缓冲的字节数据返回true
     */
    @Override
    public boolean hasBytesBuffered() {
        return hasBytesBuffered;
    }

    /**
     * 更新缓冲字节状态
     * 根据读取或处理进度更新hasBytesBuffered标志
     * 应用场景：
     * 1. 网络读取操作后更新缓冲状态
     * 2. 数据处理完成后更新缓冲状态
     * 3. 用于流量控制和背压处理
     * 
     * @param madeProgress 是否有数据处理进度
     */
    private void updateBytesBuffered(boolean madeProgress) {
        if (madeProgress)
            // 如果有进度，检查网络读缓冲区或应用读缓冲区是否还有数据
            hasBytesBuffered = netReadBuffer.position() != 0 || appReadBuffer.position() != 0;
        else
            // 如果没有进度，表示无法继续处理，直到有新的网络数据可读
            hasBytesBuffered = false;
    }

    /**
     * 从文件通道传输数据到SSL通道
     * 实现了高效的零拷贝文件传输机制，通过直接缓冲区优化性能
     * 应用场景：
     * 1. 大文件传输，如日志文件或数据文件的发送
     * 2. 流式数据传输，需要高效的吞吐量
     * 3. 支持断点续传，通过position参数控制传输起始位置
     * 
     * @param fileChannel 源文件通道
     * @param position 文件开始位置
     * @param count 要传输的最大字节数
     * @return 实际传输的字节数
     * @throws IOException 如果传输过程中发生I/O错误
     */
    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        // 检查通道状态，如果正在关闭则抛出异常
        if (state == State.CLOSING)
            throw closingException();
        // 只有在READY状态才能传输数据
        if (state != State.READY)
            return 0;

        // 确保网络写缓冲区已清空，避免数据混淆
        if (!flush(netWriteBuffer))
            return 0;

        // 获取文件大小并验证传输位置的有效性
        long channelSize = fileChannel.size();
        if (position > channelSize)
            return 0;
        // 计算实际可传输的字节数，考虑整数上限
        int totalBytesToWrite = (int) Math.min(Math.min(count, channelSize - position), Integer.MAX_VALUE);

        // 延迟初始化文件通道缓冲区
        if (fileChannelBuffer == null) {
            // 选择32KB作为传输缓冲区大小
            // 原因：1. 支持高效的磁盘读取
            //      2. 每个连接的内存开销可控
            //      3. 通常可以在单次write调用中处理完
            //      4. 与默认的netWriteBuffer(16KB)和socket发送缓冲区(100KB)相匹配
            int transferSize = 32768;
            
            // 使用直接缓冲区避免堆内存拷贝
            // 1. SSLEngine会将源缓冲区(fileChannelBuffer)数据拷贝到目标缓冲区(netWriteBuffer)
            // 2. 然后在原地进行加密
            // 3. 如果使用堆缓冲区，FileChannel.read()会需要额外的直接缓冲区到堆缓冲区的拷贝
            fileChannelBuffer = ByteBuffer.allocateDirect(transferSize);
            
            // 确保新分配的缓冲区没有残留数据
            fileChannelBuffer.position(fileChannelBuffer.limit());
        }

        // 记录已传输的总字节数
        int totalBytesWritten = 0;
        // 当前处理位置
        long pos = position;
        try {
            // 循环传输数据直到达到目标大小
            while (totalBytesWritten < totalBytesToWrite) {
                // 当缓冲区为空时，从文件读取新数据
                if (!fileChannelBuffer.hasRemaining()) {
                    // 清空缓冲区准备新的读取
                    fileChannelBuffer.clear();
                    // 计算剩余需要传输的字节数
                    int bytesRemaining = totalBytesToWrite - totalBytesWritten;
                    // 如果剩余字节数小于缓冲区容量，调整限制避免多读
                    if (bytesRemaining < fileChannelBuffer.limit())
                        fileChannelBuffer.limit(bytesRemaining);
                    // 从文件通道读取数据
                    int bytesRead = fileChannel.read(fileChannelBuffer, pos);
                    // 如果没有读到数据，说明到达文件末尾
                    if (bytesRead <= 0)
                        break;
                    // 切换缓冲区到读模式
                    fileChannelBuffer.flip();
                }
                // 将数据写入网络通道
                int networkBytesWritten = write(fileChannelBuffer);
                // 更新已传输字节计数
                totalBytesWritten += networkBytesWritten;
                
                // 处理部分写入情况
                // 如果缓冲区还有剩余数据，中断传输
                // 这确保了在下次transferFrom调用时，pos值正确反映了实际的文件位置
                if (fileChannelBuffer.hasRemaining())
                    break;
                // 更新文件位置
                pos += networkBytesWritten;
            }
            return totalBytesWritten;
        } catch (IOException e) {
            // 如果已经传输了一些数据，返回已传输的字节数
            // 这允许调用者知道实际传输了多少数据，有助于断点续传
            if (totalBytesWritten > 0)
                return totalBytesWritten;
            throw e;
        }
    }
}
