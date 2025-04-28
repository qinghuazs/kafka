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

import java.util.Objects;

/**
 * 定义在重新认证过程中创建{@link Authenticator}的上下文环境。
 * 
 * 应用场景：
 * 1. 会话过期时的重新认证
 * 2. 安全策略要求的定期重新认证
 * 3. 客户端主动发起的重新认证
 * 
 * 设计考虑：
 * 1. 保存重认证所需的状态信息
 * 2. 区分客户端和服务器端的处理逻辑
 * 3. 确保认证过程的连续性和安全性
 */
public class ReauthenticationContext {
    /**
     * 网络数据接收器实例
     * - 客户端：可能包含部分读取的响应数据，或为空
     * - 服务器端：必须包含触发重认证的SaslHandshakeRequest
     */
    private final NetworkReceive networkReceive;

    /**
     * 之前用于认证通道的认证器实例
     * - 用于维护认证状态的连续性
     * - 包含之前的认证配置和会话信息
     */
    private final Authenticator previousAuthenticator;

    /**
     * 重认证开始的时间戳（纳秒）
     * - 用于跟踪重认证过程的时间
     * - 基于System.nanoTime()，仅用于相对时间比较
     */
    private final long reauthenticationBeginNanos;

    /**
     * 构造函数 - 创建重认证上下文
     * 
     * @param previousAuthenticator
     *            必需参数，之前用于认证通道的{@link Authenticator}实例。
     *            用于保持认证状态的连续性，确保重认证过程中的安全性。
     * @param networkReceive
     *            网络接收器实例，其用途因客户端/服务器端而异：
     *            - 客户端：可选参数
     *              1. 可能包含部分读取的响应数据
     *              2. 可能是尚未读取数据的新实例
     *              3. 可能为null
     *              如果非null，将用于在重认证过程中初始读取数据
     *            - 服务器端：必需参数
     *              必须包含已接收的{@code SaslHandshakeRequest}，该请求触发重认证
     * @param nowNanos
     *            重认证开始的时间戳（纳秒）。
     *            基于{@code System.nanoTime()}，仅用于相对时间比较。
     *            用于记录重认证开始的精确时间点。
     */
    public ReauthenticationContext(Authenticator previousAuthenticator, NetworkReceive networkReceive, long nowNanos) {
        // 确保previousAuthenticator不为null，这是重认证必需的组件
        this.previousAuthenticator = Objects.requireNonNull(previousAuthenticator);
        // 设置网络接收器，可能为null（客户端）或包含重认证请求（服务器端）
        this.networkReceive = networkReceive;
        // 记录重认证开始的时间戳
        this.reauthenticationBeginNanos = nowNanos;
    }

    /**
     * 获取网络接收器实例
     * 
     * 使用场景：
     * 1. 客户端：
     *    - 获取可能存在的部分响应数据
     *    - 获取用于读取重认证数据的新实例
     *    - 可能返回null
     * 2. 服务器端：
     *    - 获取包含{@code SaslHandshakeRequest}的接收器
     *    - 该请求是触发重认证的关键信息
     * 
     * @return 适用的{@link NetworkReceive}实例，可能为null（仅客户端）
     */
    public NetworkReceive networkReceive() {
        return networkReceive;
    }

    /**
     * 获取之前的认证器实例
     * 
     * 使用场景：
     * 1. 获取之前的认证配置
     * 2. 访问现有的会话信息
     * 3. 确保认证状态的平滑过渡
     * 
     * @return 之前用于认证通道的{@link Authenticator}实例（永不为null）
     */
    public Authenticator previousAuthenticator() {
        return previousAuthenticator;
    }

    /**
     * 获取重认证开始的时间戳
     * 
     * 使用场景：
     * 1. 跟踪重认证过程的持续时间
     * 2. 实现重认证的超时机制
     * 3. 用于性能监控和问题诊断
     * 
     * @return 重认证开始的时间戳（纳秒），仅用于与其他System.nanoTime()值比较
     */
    public long reauthenticationBeginNanos() {
        return reauthenticationBeginNanos;
    }
}
