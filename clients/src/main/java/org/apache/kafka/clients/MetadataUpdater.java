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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * 该接口由NetworkClient使用，用于请求更新集群元数据信息并从元数据中获取集群节点信息。
 * 这是一个内部类，主要负责管理和维护Kafka集群的元数据状态。
 * <p>
 * 注意：此类不是线程安全的！
 * 
 * 元数据更新器的主要职责：
 * 1. 维护集群节点列表
 * 2. 处理元数据更新请求
 * 3. 处理服务器连接异常
 * 4. 管理元数据重引导机制
 */
public interface MetadataUpdater extends Closeable {

    /**
     * 获取当前集群信息，此方法为非阻塞调用。
     * 
     * @return 返回当前已知的所有集群节点列表
     */
    List<Node> fetchNodes();

    /**
     * 检查是否需要更新集群元数据信息。
     * 
     * @param now 当前时间戳（毫秒）
     * @return 如果需要更新返回true，否则返回false
     */
    boolean isUpdateDue(long now);

    /**
     * 在需要且可能的情况下启动集群元数据更新。
     * 
     * @param now 当前时间戳（毫秒）
     * @return 返回距离下一次元数据更新的时间（如果本次调用已启动更新，则返回0）
     * 
     * 说明：
     * 1. 如果实现依赖NetworkClient发送请求，则在收到元数据响应后会调用handleSuccessfulResponse
     * 2. "需要"和"可能"的语义由具体实现决定，可能考虑多个因素：
     *    - 节点可用性
     *    - 距离上次元数据更新的时间间隔
     *    - 当前网络状况等
     */
    long maybeUpdate(long now);

    /**
     * 处理服务器断开连接的情况。
     * 
     * @param now 当前时间戳（毫秒）
     * @param nodeId 断开连接的节点ID
     * @param maybeAuthException 可能的认证异常，如果发生认证错误则包含具体异常信息
     * 
     * 说明：
     * 此方法为MetadataUpdater实现提供了一种机制，用于处理其通过NetworkClient发送的请求
     * 在遇到连接断开时的特殊处理逻辑。
     */
    void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException);

    /**
     * 处理元数据请求失败的情况。
     * 
     * @param now 当前时间戳（毫秒）
     * @param maybeFatalException 可能的致命错误，例如不支持的版本异常（UnsupportedVersionException）
     * 
     * 说明：
     * 当元数据请求失败时调用此方法，实现类可以在此处理错误恢复、重试策略等逻辑
     */
    void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException);

    /**
     * 处理元数据请求的成功响应。
     * 
     * @param requestHeader 请求头信息
     * @param now 当前时间戳（毫秒）
     * @param metadataResponse 元数据响应内容
     * 
     * 说明：
     * 此方法为MetadataUpdater实现提供了处理成功接收到元数据响应后的机制，
     * 实现类可以在此更新本地缓存的元数据信息。
     */
    void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse);

    /**
     * 检查是否需要进行元数据重引导。
     * 
     * @param now 当前时间戳（毫秒）
     * @param rebootstrapTriggerMs 触发重引导的超时时间配置
     * @return 如果需要重引导返回true，否则返回false
     * 
     * 说明：
     * 在以下情况返回true：
     * 1. 在指定时间内无法获取元数据
     * 2. 服务器要求进行重引导
     */
    default boolean needsRebootstrap(long now, long rebootstrapTriggerMs) {
        return false;
    }

    /**
     * 执行重引导操作，使用引导集群替换现有集群。
     * 
     * @param now 当前时间戳（毫秒）
     * 
     * 说明：
     * 当需要重新建立与集群的连接时，此方法会被调用，用于重置连接状态
     */
    default void rebootstrap(long now) {}

    /**
     * 关闭此更新器。
     * 
     * 说明：
     * 实现类应在此方法中释放所有资源，确保正确关闭所有连接
     */
    @Override
    void close();
}
