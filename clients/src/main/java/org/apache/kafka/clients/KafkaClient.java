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
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.Closeable;
import java.util.List;

/**
 * NetworkClient的接口定义，这是Kafka客户端与服务器通信的核心接口。
 * 该接口定义了所有与Kafka服务器进行网络通信所需的基本操作，包括：
 * 1. 连接管理：建立、维护和关闭与Kafka broker的连接
 * 2. 请求处理：发送请求、接收响应
 * 3. 连接状态检查：检查连接是否就绪、是否认证失败等
 * 4. 负载均衡：选择负载最小的节点
 */
public interface KafkaClient extends Closeable {

    /**
     * 检查当前是否可以向指定节点发送新的请求，但如果未连接则不会尝试建立连接。
     * 这是一个非阻塞的检查方法，用于快速判断节点的可用性。
     *
     * @param node 要检查的节点
     * @param now 当前时间戳（毫秒）
     * @return 如果节点已准备好接收新请求则返回true，否则返回false
     */
    boolean isReady(Node node, long now);

    /**
     * 如果需要，启动与指定节点的连接。如果已经连接，则返回true。
     * 节点的就绪状态只会在调用poll方法时发生变化。
     * 这个方法会主动尝试建立连接，与isReady方法的主要区别在于它会触发连接建立过程。
     *
     * @param node 要连接的节点
     * @param now 当前时间（毫秒）
     * @return 如果节点已准备好立即发送请求则返回true，否则返回false
     */
    boolean ready(Node node, long now);

    /**
     * 根据连接状态，返回在尝试发送数据之前需要等待的毫秒数。
     * 此方法用于实现重试机制和流量控制：
     * - 当连接断开时，会遵循重连退避时间
     * - 当正在连接或已连接时，会处理慢连接/停滞连接的情况
     *
     * @param node 要检查的节点
     * @param now 当前时间戳（毫秒）
     * @return 需要等待的毫秒数
     */
    long connectionDelay(Node node, long now);

    /**
     * 根据连接状态和限流时间，返回在尝试发送数据之前需要等待的毫秒数。
     * 这个方法结合了连接状态和限流机制：
     * - 如果连接已建立但被限流，返回限流延迟时间
     * - 否则，返回连接延迟时间
     *
     * @param node 要检查的连接
     * @param now 当前时间（毫秒）
     * @return 需要等待的毫秒数
     */
    long pollDelayMs(Node node, long now);

    /**
     * 根据连接状态检查节点的连接是否已失败。
     * 这种连接失败通常是暂时的，可以在下次调用{@link #ready(org.apache.kafka.common.Node, long)}时恢复，
     * 但在某些情况下需要捕获这些暂时性失败并作出相应处理。
     *
     * @param node 要检查的节点
     * @return 如果连接已失败且节点已断开连接则返回true
     */
    boolean connectionFailed(Node node);

    /**
     * 根据连接状态检查与此节点的认证是否失败。
     * 认证失败是一个严重的错误，不会进行重试，而是直接向上层传播。
     * 这通常表示配置错误或证书/密钥问题。
     *
     * @param node 要检查的节点
     * @return 如果认证失败则返回AuthenticationException，否则返回null
     */
    AuthenticationException authenticationException(Node node);

    /**
     * 将给定的请求加入发送队列。请求只能在连接就绪的情况下发送。
     * 这是一个异步操作，实际的发送操作将在后续的poll调用中执行。
     *
     * @param request 要发送的请求
     * @param now 当前时间戳（毫秒）
     */
    void send(ClientRequest request, long now);

    /**
     * 执行实际的套接字读写操作。
     * 这是客户端的核心网络I/O方法，负责：
     * 1. 发送已排队的请求
     * 2. 接收服务器的响应
     * 3. 处理连接状态变化
     *
     * @param timeout 等待响应的最大时间（毫秒），必须非负。实现可以根据需要使用更小的值
     *               （常见原因是较低的请求超时或元数据更新超时）
     * @param now 当前时间（毫秒）
     * @throws IllegalStateException 如果向未就绪的节点发送请求
     * @return 收到的响应列表
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * 断开与特定节点的连接（如果存在）。
     * 该节点上的所有待处理请求都会收到断开连接的通知。
     * 这个方法通常用于主动关闭不再需要的连接或处理连接异常。
     *
     * @param nodeId 节点的ID
     */
    void disconnect(String nodeId);

    /**
     * 关闭与特定节点的连接（如果存在）。
     * 与disconnect不同，这个方法会：
     * 1. 清除该连接上的所有请求
     * 2. 不会调用被清除请求的回调函数
     * 3. 这些请求也不会从poll()返回
     * 这个方法通常用于彻底清理连接资源。
     *
     * @param nodeId 节点的ID
     */
    void close(String nodeId);

    /**
     * 选择具有最少未完成请求的节点。
     * 这个方法实现了一个简单的负载均衡策略：
     * 1. 优先选择已有连接的节点
     * 2. 如果所有现有连接都在使用中，可能会选择一个尚未建立连接的节点
     * 这有助于在多个broker之间均匀分配请求负载。
     *
     * @param now 当前时间（毫秒）
     * @return 具有最少在途请求的节点
     */
    LeastLoadedNode leastLoadedNode(long now);

    /**
     * 获取当前所有在途请求的数量（尚未收到响应的请求数）。
     * 这个指标可以用来监控客户端的负载状况。
     */
    int inFlightRequestCount();

    /**
     * 检查是否存在至少一个在途请求。
     * 这个方法比inFlightRequestCount()更轻量，适用于只需要知道是否有请求在处理的场景。
     *
     * @return 如果存在至少一个在途请求则返回true，否则返回false
     */
    boolean hasInFlightRequests();

    /**
     * 获取特定节点上的在途请求总数。
     * 这个方法可以用来监控特定节点的负载情况，对负载均衡和故障检测很有帮助。
     *
     * @param nodeId 节点的ID
     * @return 该节点上的在途请求数量
     */
    int inFlightRequestCount(String nodeId);

    /**
     * 检查特定节点是否存在至少一个在途请求。
     * 这是inFlightRequestCount(String nodeId)的轻量级版本。
     *
     * @param nodeId 节点的ID
     * @return 如果该节点存在至少一个在途请求则返回true，否则返回false
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * 检查是否存在至少一个处于READY状态且未被限流的节点连接。
     * 这个方法用于判断客户端是否有可用的服务器节点可以处理新的请求。
     *
     * @param now 当前时间
     * @return 如果存在至少一个就绪且未限流的节点则返回true，否则返回false
     */
    boolean hasReadyNodes(long now);

    /**
     * 唤醒当前正在等待I/O的客户端。
     * 这个方法通常用于：
     * 1. 中断长时间的poll操作
     * 2. 在其他线程添加新请求后通知客户端立即处理
     * 3. 在需要关闭客户端时中断阻塞操作
     */
    void wakeup();

    /**
     * 创建一个新的ClientRequest（客户端请求）。
     * 这是一个简化版的请求创建方法，适用于基本的请求场景。
     *
     * @param nodeId 目标节点的ID
     * @param requestBuilder 用于构建请求的构建器
     * @param createdTimeMs 请求的创建时间（毫秒）
     * @param expectResponse 如果期望收到响应则为true
     * @return 新创建的ClientRequest对象
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs, boolean expectResponse);

    /**
     * 创建一个新的ClientRequest（客户端请求）。
     * 这是完整版的请求创建方法，提供了更多的控制选项：
     * 1. 可以设置请求超时时间
     * 2. 可以指定响应回调处理器
     *
     * @param nodeId 目标节点的ID
     * @param requestBuilder 用于构建请求的构建器
     * @param createdTimeMs 请求的创建时间（毫秒）
     * @param expectResponse 如果期望收到响应则为true
     * @param requestTimeoutMs 等待响应的最大时间（毫秒）。如果超过这个时间还没有收到响应，
     *                        将断开socket连接并取消请求。注意：如果socket因为其他原因断开
     *                        （比如同一节点的另一个请求超时），请求可能会更早被取消
     * @param callback 收到响应时要调用的回调函数
     * @return 新创建的ClientRequest对象
     */
    ClientRequest newClientRequest(String nodeId,
                                   AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs,
                                   boolean expectResponse,
                                   int requestTimeoutMs,
                                   RequestCompletionHandler callback);



    /**
     * 启动客户端的关闭过程。
     * 这个方法实现了优雅关闭机制：
     * 1. 可以从其他线程调用，即使客户端正在执行poll操作
     * 2. 调用后不能再发送新的请求
     * 3. 会通过wakeup()终止当前的poll()操作
     * 4. poll返回后，应该显式调用{@link #close()}完成关闭
     * 注意：不要在执行poll操作时并发调用{@link #close()}
     */
    void initiateClose();

    /**
     * 检查客户端是否仍然处于活动状态。
     * 这个方法用于检查客户端的生命周期状态：
     * - 如果已调用{@link #initiateClose()}或{@link #close()}，返回false
     * - 否则返回true
     *
     * @return 如果客户端仍然活动则返回true，否则返回false
     */
    boolean active();

}
