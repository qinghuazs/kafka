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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 协调器查找策略类，实现了AdminApiLookupStrategy接口
 * 用于查找消费者组或事务的协调器节点
 * 支持批量查找和单个查找两种模式
 */
public class CoordinatorStrategy implements AdminApiLookupStrategy<CoordinatorKey> {

    /**
     * 批处理请求作用域，用于批量查找协调器时的请求范围标识
     */
    private static final ApiRequestScope BATCH_REQUEST_SCOPE = new ApiRequestScope() { };

    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 协调器类型，可以是GROUP（消费者组）或TRANSACTION（事务）
     */
    private final FindCoordinatorRequest.CoordinatorType type;

    /**
     * 存储无法在请求中表示的键（如null或无效的groupId）
     */
    private Set<CoordinatorKey> unrepresentableKeys = Collections.emptySet();

    /**
     * 是否启用批处理模式，默认为true
     */
    boolean batch = true;

    /**
     * 构造函数
     * @param type 协调器类型（GROUP或TRANSACTION）
     * @param logContext 日志上下文
     */
    public CoordinatorStrategy(
        FindCoordinatorRequest.CoordinatorType type,
        LogContext logContext
    ) {
        this.type = type;
        this.log = logContext.logger(CoordinatorStrategy.class);
    }

    /**
     * 获取查找请求的作用域
     * @param key 协调器键（可以是消费者组ID或事务ID）
     * @return 请求作用域
     * 
     * 实现细节：
     * 1. 如果启用了批处理，返回共享的批处理作用域
     * 2. 如果是单个查找模式，为每个键创建独立的作用域
     */
    @Override
    public ApiRequestScope lookupScope(CoordinatorKey key) {
        if (batch) {
            return BATCH_REQUEST_SCOPE;
        } else {
            // 如果FindCoordinator API不支持批量查找，则为每个协调器键使用单独的查找上下文
            return new LookupRequestScope(key);
        }
    }

    /**
     * 构建查找协调器的请求
     * @param keys 需要查找的协调器键集合
     * @return FindCoordinatorRequest的构建器
     * 
     * 实现细节：
     * 1. 首先过滤出无法表示的键（null或无效的ID）
     * 2. 然后获取可以表示的有效键
     * 3. 根据批处理模式构建不同的请求：
     *    - 批处理模式：确保所有键类型相同，构建包含多个键的请求
     *    - 单个模式：确保只有一个键且类型正确，构建单个键的请求
     */
    @Override
    public FindCoordinatorRequest.Builder buildRequest(Set<CoordinatorKey> keys) {
        // 过滤出无法表示的键（null或无效ID）
        unrepresentableKeys = keys.stream().filter(k -> k == null || !isRepresentableKey(k.idValue)).collect(Collectors.toSet());
        // 获取可以表示的有效键
        Set<CoordinatorKey> representableKeys = keys.stream().filter(k -> k != null && isRepresentableKey(k.idValue)).collect(Collectors.toSet());
        
        if (batch) {
            // 批处理模式：确保所有键类型相同
            ensureSameType(representableKeys);
            FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                    .setKeyType(type.id())
                    .setCoordinatorKeys(representableKeys.stream().map(k -> k.idValue).collect(Collectors.toList()));
            return new FindCoordinatorRequest.Builder(data);
        } else {
            // 单个模式：确保只有一个键且类型正确
            CoordinatorKey key = requireSingletonAndType(representableKeys);
            return new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKey(key.idValue)
                    .setKeyType(key.type.id())
            );
        }
    }

    /**
     * 处理查找协调器的响应
     * @param keys 请求的协调器键集合
     * @param abstractResponse 服务器的响应
     * @return 查找结果，包含成功映射的键和失败的键
     * 
     * 实现细节：
     * 1. 创建两个映射来存储结果：
     *    - mappedKeys：成功找到协调器的键到节点ID的映射
     *    - failedKeys：查找失败的键到异常的映射
     * 2. 首先处理无法表示的键，将它们添加到失败映射
     * 3. 然后处理每个协调器的响应：
     *    - 对于旧版本响应（不支持批处理），使用单个键
     *    - 对于新版本响应，根据类型创建适当的键
     *    - 处理每个响应的错误码
     */
    @Override
    public LookupResult<CoordinatorKey> handleResponse(
        Set<CoordinatorKey> keys,
        AbstractResponse abstractResponse
    ) {
        // 存储成功找到协调器的键到节点ID的映射
        Map<CoordinatorKey, Integer> mappedKeys = new HashMap<>();
        // 存储查找失败的键到异常的映射
        Map<CoordinatorKey, Throwable> failedKeys = new HashMap<>();

        // 处理无法表示的键
        for (CoordinatorKey key : unrepresentableKeys) {
            failedKeys.put(key, new InvalidGroupIdException("The given group id '" +
                key.idValue + "' cannot be represented in a request."));
        }

        // 处理每个协调器的响应
        for (Coordinator coordinator : ((FindCoordinatorResponse) abstractResponse).coordinators()) {
            CoordinatorKey key;
            if (coordinator.key() == null) { // 旧版本不支持批处理
                key = requireSingletonAndType(keys);
            } else {
                // 根据协调器类型创建适当的键
                key = (type == CoordinatorType.GROUP)
                        ? CoordinatorKey.byGroupId(coordinator.key())
                        : CoordinatorKey.byTransactionalId(coordinator.key());
            }
            // 处理响应中的错误码
            handleError(Errors.forCode(coordinator.errorCode()),
                        key,
                        coordinator.nodeId(),
                        mappedKeys,
                        failedKeys);
        }
        return new LookupResult<>(failedKeys, mappedKeys);
    }

    /**
     * 禁用批处理模式
     * 调用此方法后，查找协调器将使用单个查找模式
     */
    public void disableBatch() {
        batch = false;
    }

    /**
     * 获取当前是否启用批处理模式
     * @return 如果启用批处理返回true，否则返回false
     */
    public boolean batch() {
        return batch;
    }

    /**
     * 确保键集合中只有一个键，且类型正确
     * @param keys 协调器键集合
     * @return 集合中的唯一键
     * @throws IllegalArgumentException 如果集合大小不为1或键类型不匹配
     * 
     * 实现细节：
     * 1. 检查集合大小是否为1
     * 2. 获取唯一的键
     * 3. 验证键类型是否与期望的类型匹配
     */
    private CoordinatorKey requireSingletonAndType(Set<CoordinatorKey> keys) {
        // 检查集合大小是否为1
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected size of key set: expected 1, but got " + keys.size());
        }
        // 获取唯一的键
        CoordinatorKey key = keys.iterator().next();
        // 验证键类型是否匹配
        if (key.type != type) {
            throw new IllegalArgumentException("Unexpected key type: expected key to be of type " + type + ", but got " + key.type);
        }
        return key;
    }

    /**
     * 确保键集合不为空且所有键类型相同
     * @param keys 协调器键集合
     * @throws IllegalArgumentException 如果集合为空或存在类型不匹配的键
     * 
     * 实现细节：
     * 1. 检查集合是否为空
     * 2. 验证所有键的类型是否与期望的类型匹配
     */
    private void ensureSameType(Set<CoordinatorKey> keys) {
        // 检查集合是否为空
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Unexpected size of key set: expected >= 1, but got 0");
        }
        // 验证所有键的类型是否匹配
        if (keys.stream().filter(k -> k.type == type).collect(Collectors.toSet()).size() != keys.size()) {
            throw new IllegalArgumentException("Unexpected key set: expected all key to be of type " + type + ", but some key were not");
        }
    }

    /**
     * 检查键ID是否可以在请求中表示
     * @param groupId 组ID或事务ID
     * @return 如果ID不为null返回true，否则返回false
     */
    private static boolean isRepresentableKey(String groupId) {
        return groupId != null;
    }

    /**
     * 处理协调器查找响应中的错误
     * @param error 错误类型
     * @param key 协调器键
     * @param nodeId 节点ID
     * @param mappedKeys 成功映射的键集合
     * @param failedKeys 失败的键集合
     * 
     * 实现细节：
     * 1. 根据错误类型进行不同处理：
     *    - NONE：查找成功，将键和节点ID添加到映射
     *    - COORDINATOR_NOT_AVAILABLE/COORDINATOR_LOAD_IN_PROGRESS：临时错误，稍后重试
     *    - GROUP_AUTHORIZATION_FAILED：组授权失败
     *    - TRANSACTIONAL_ID_AUTHORIZATION_FAILED：事务ID授权失败
     *    - 其他：未预期的错误
     */
    private void handleError(Errors error, CoordinatorKey key, int nodeId, Map<CoordinatorKey, Integer> mappedKeys, Map<CoordinatorKey, Throwable> failedKeys) {
        switch (error) {
            case NONE:
                // 查找成功，记录键到节点的映射
                mappedKeys.put(key, nodeId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case COORDINATOR_LOAD_IN_PROGRESS:
                // 临时错误，记录日志并准备重试
                log.debug("FindCoordinator request for key {} returned topic-level error {}. Will retry",
                    key, error);
                break;
            case GROUP_AUTHORIZATION_FAILED:
                // 消费者组授权失败
                failedKeys.put(key, new GroupAuthorizationException("FindCoordinator request for groupId " +
                    "`" + key + "` failed due to authorization failure", key.idValue));
                break;
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                // 事务ID授权失败
                failedKeys.put(key, new TransactionalIdAuthorizationException("FindCoordinator request for " +
                    "transactionalId `" + key + "` failed due to authorization failure"));
                break;
            default:
                // 未预期的错误
                failedKeys.put(key, error.exception("FindCoordinator request for key " +
                    "`" + key + "` failed due to an unexpected error"));
        }
    }

    /**
     * 查找请求作用域内部类，用于非批处理模式下的请求范围管理
     * 实现了ApiRequestScope接口，为每个协调器键提供独立的作用域
     * 
     * 应用场景：
     * 1. 在非批处理模式下，每个协调器键需要独立的查找上下文
     * 2. 用于区分不同协调器键的查找请求，确保请求的隔离性
     * 3. 支持旧版本的Kafka协调器查找API
     */
    private static class LookupRequestScope implements ApiRequestScope {
        /**
         * 协调器键，用于标识特定的消费者组或事务
         * 在查找请求中用作唯一标识符
         */
        final CoordinatorKey key;

        /**
         * 构造函数
         * @param key 协调器键，可以是消费者组ID或事务ID
         */
        private LookupRequestScope(CoordinatorKey key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LookupRequestScope that = (LookupRequestScope) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }
}
