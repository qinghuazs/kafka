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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Authentication for Channel
 * 通道认证接口
 * 
 * 该接口定义了Kafka网络通道的认证机制，支持多种安全协议：
 * 1. PLAINTEXT - 无需认证
 * 2. SSL - 基于SSL/TLS的认证
 * 3. SASL_PLAINTEXT - 基于SASL的认证，传输层不加密
 * 4. SASL_SSL - 基于SASL的认证，且传输层使用SSL加密
 * 
 * 认证流程：
 * 1. 初始认证：客户端连接时进行首次认证
 * 2. 会话管理：维护认证会话状态和过期时间
 * 3. 重新认证：支持会话过期后的重新认证机制
 * 4. 失败处理：定义认证失败时的处理逻辑
 */
public interface Authenticator extends Closeable {
    /**
     * Implements any authentication mechanism. Use transportLayer to read or write tokens.
     * For security protocols PLAINTEXT and SSL, this is a no-op since no further authentication
     * needs to be done. For SASL_PLAINTEXT and SASL_SSL, this performs the SASL authentication.
     * 实现认证机制。使用transportLayer读写认证令牌。
     * 
     * 不同协议的处理：
     * 1. PLAINTEXT：无需额外认证，此方法为空操作
     * 2. SSL：SSL握手已在传输层完成，此方法为空操作
     * 3. SASL_PLAINTEXT：执行SASL认证流程
     * 4. SASL_SSL：在SSL之上执行SASL认证流程
     *
     * @throws AuthenticationException 当认证失败时抛出，可能原因：
     *      - 无效的凭证
     *      - 安全配置错误
     *      - 不支持的认证机制
     * @throws IOException 当读写操作发生I/O错误时抛出
     */
    void authenticate() throws AuthenticationException, IOException;

    /**
     * Perform any processing related to authentication failure. This is invoked when the channel is about to be closed
     * because of an {@link AuthenticationException} thrown from a prior {@link #authenticate()} call.
     * 处理认证失败相关的操作。当通道因认证异常即将关闭时调用此方法。
     * 
     * 应用场景：
     * 1. 清理认证相关的资源
     * 2. 记录失败原因
     * 3. 通知相关组件认证失败
     * 4. 执行失败后的恢复操作
     * 
     * @throws IOException 当进行清理操作时发生I/O错误
     */
    default void handleAuthenticationFailure() throws IOException {
    }

    /**
     * Returns Principal using PrincipalBuilder
     * 使用PrincipalBuilder返回认证主体
     * 
     * 主要功能：
     * 1. 获取当前认证会话的身份信息
     * 2. 用于权限检查和审计日志
     * 3. 支持自定义主体构建逻辑
     * 
     * @return 返回代表认证主体的KafkaPrincipal对象
     */
    KafkaPrincipal principal();

    /**
     * Returns the serializer/deserializer interface for principal
     * 返回Principal的序列化/反序列化接口
     * 
     * 主要用途：
     * 1. 在网络传输中序列化/反序列化Principal信息
     * 2. 支持自定义序列化格式
     * 3. 确保Principal信息在集群中正确传递
     * 
     * @return 返回Optional包装的KafkaPrincipalSerde对象，如果不需要序列化则返回空
     */
    Optional<KafkaPrincipalSerde> principalSerde();

    /**
     * returns true if authentication is complete otherwise returns false;
     * 返回认证是否完成
     * 
     * 使用场景：
     * 1. 检查认证状态
     * 2. 控制认证流程
     * 3. 决定是否允许数据传输
     * 
     * @return 如果认证已完成返回true，否则返回false
     */
    boolean complete();

    /**
     * Begins re-authentication. Uses transportLayer to read or write tokens as is
     * done for {@link #authenticate()}. For security protocols PLAINTEXT and SSL,
     * this is a no-op since re-authentication does not apply/is not supported,
     * respectively. For SASL_PLAINTEXT and SASL_SSL, this performs a SASL
     * authentication. Any in-flight responses from prior requests can/will be read
     * and collected for later processing as required. There must not be partially
     * written requests; any request queued for writing (for which zero bytes have
     * been written) remains queued until after re-authentication succeeds.
     * 开始重新认证。使用transportLayer读写令牌，过程类似于{@link #authenticate()}。
     * 
     * 不同协议的处理：
     * 1. PLAINTEXT：不适用重认证，此方法为空操作
     * 2. SSL：不支持重认证，此方法为空操作
     * 3. SASL_PLAINTEXT和SASL_SSL：执行SASL重认证
     * 
     * 重认证期间的请求处理：
     * 1. 已发送请求的响应会被收集并在之后处理
     * 2. 未开始发送的请求会保持在队列中直到重认证成功
     * 3. 不允许存在部分写入的请求
     * 
     * @param reauthenticationContext
     *            重认证上下文，负责管理重认证过程。
     *            该实例负责关闭由{@link ReauthenticationContext#previousAuthenticator()}返回的前一个认证器。
     * @throws AuthenticationException
     *             当重认证失败时抛出，可能原因包括：
     *             - 无效的凭证
     *             - 安全配置错误
     * @throws IOException
     *             当读写操作发生I/O错误时抛出
     */
    default void reauthenticate(ReauthenticationContext reauthenticationContext) throws IOException {
        // empty
    }

    /**
     * Return the session expiration time, if any, otherwise null. The value is in
     * nanoseconds as per {@code System.nanoTime()} and is therefore only useful
     * when compared to such a value -- it's absolute value is meaningless. This
     * value may be non-null only on the server-side. It represents the time after
     * which, in the absence of re-authentication, the broker will close the session
     * if it receives a request unrelated to authentication. We store nanoseconds
     * here to avoid having to invoke the more expensive {@code milliseconds()} call
     * on the broker for every request
     * 返回会话过期时间（如果有），否则返回null。时间值以纳秒为单位。
     * 
     * 特点说明：
     * 1. 仅在服务器端可能返回非null值
     * 2. 时间值基于System.nanoTime()，仅用于比较
     * 3. 表示在没有重认证的情况下，会话的过期时间
     * 4. 使用纳秒存储以避免频繁的毫秒转换开销
     * 
     * 应用场景：
     * 1. 服务器用于判断会话是否需要重认证
     * 2. 如果过期后收到非认证请求，broker将关闭会话
     * 
     * @return 会话过期时间（纳秒），如果不适用则返回null
     */
    default Long serverSessionExpirationTimeNanos() {
        return null;
    }

    /**
     * Return the time on or after which a client should re-authenticate this
     * session, if any, otherwise null. The value is in nanoseconds as per
     * {@code System.nanoTime()} and is therefore only useful when compared to such
     * a value -- it's absolute value is meaningless. This value may be non-null
     * only on the client-side. It will be a random time between 85% and 95% of the
     * full session lifetime to account for latency between client and server and to
     * avoid re-authentication storms that could be caused by many sessions
     * re-authenticating simultaneously.
     * 返回客户端应该进行重认证的时间点。时间值以纳秒为单位。
     * 
     * 特点说明：
     * 1. 仅在客户端可能返回非null值
     * 2. 时间值基于System.nanoTime()，仅用于比较
     * 3. 重认证时间点在会话生命周期的85%到95%之间随机选择
     * 
     * 设计考虑：
     * 1. 考虑客户端和服务器之间的延迟
     * 2. 通过随机化避免多个会话同时重认证导致的风暴
     * 3. 确保在会话实际过期前完成重认证
     * 
     * @return 客户端应进行重认证的时间点（纳秒），如果不适用则返回null
     */
    default Long clientSessionReauthenticationTimeNanos() {
        return null;
    }

    /**
     * Return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable, otherwise null.
     * The server-side perspective will yield a lower value than the client-side
     * perspective of the same re-authentication because the client-side observes an
     * additional network round-trip.
     * 返回重认证过程耗时（毫秒）。
     * 
     * 特点说明：
     * 1. 从当前实例的视角测量重认证耗时
     * 2. 服务器端测量值小于客户端
     * 3. 差异源于客户端额外的网络往返时间
     * 
     * 应用场景：
     * 1. 监控重认证性能
     * 2. 诊断重认证延迟问题
     * 3. 系统性能优化参考
     * 
     * @return 重认证耗时（毫秒），如果不适用则返回null
     */
    default Long reauthenticationLatencyMs() {
        return null;
    }

    /**
     * Return the next (always non-null but possibly empty) client-side
     * {@link NetworkReceive} response that arrived during re-authentication that
     * is unrelated to re-authentication, if any. These correspond to requests sent
     * prior to the beginning of re-authentication; the requests were made when the
     * channel was successfully authenticated, and the responses arrived during the
     * re-authentication process. The response returned is removed from the authenticator's
     * queue. Responses of requests sent after completion of re-authentication are
     * processed only when the authenticator response queue is empty.
     * 获取重认证过程中收到的非重认证相关的响应。
     * 
     * 响应特点：
     * 1. 来自重认证开始前发送的请求
     * 2. 这些请求在通道认证有效时发送
     * 3. 响应在重认证过程中到达
     * 
     * 处理机制：
     * 1. 返回的响应会从认证器队列中移除
     * 2. 重认证完成后的新请求响应需等待队列清空
     * 3. 确保请求响应的顺序处理
     * 
     * @return 返回Optional包装的NetworkReceive对象，代表重认证过程中收到的非重认证响应
     */
    default Optional<NetworkReceive> pollResponseReceivedDuringReauthentication() {
        return Optional.empty();
    }
    
    /**
     * Return true if this is a server-side authenticator and the connected client
     * has indicated that it supports re-authentication, otherwise false
     * 检查连接的客户端是否支持重认证。
     * 
     * 使用场景：
     * 1. 服务器端判断是否可以要求客户端重认证
     * 2. 在会话即将过期时决定是否启动重认证
     * 3. 协议兼容性检查
     * 
     * 返回条件：
     * 1. 当前实例必须是服务器端认证器
     * 2. 客户端必须明确表示支持重认证
     * 
     * @return 如果是服务器端认证器且客户端支持重认证返回true，否则返回false
     */
    default boolean connectedClientSupportsReauthentication() {
        return false;
    }
}
