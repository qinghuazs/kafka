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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Kafka管理API的查找策略接口，负责处理broker节点的查找和请求路由。
 * 该接口支持不同类型的查找机制，包括元数据查找和协调器查找等。
 * 
 * @param <T> 查找键的类型，例如主题名称、事务ID等
 */
public interface AdminApiLookupStrategy<T> {

    /**
     * 定义给定键的查找范围。键的查找过程比较复杂，因为需要适应不同的批处理机制。
     * 
     * 例如，`Metadata`请求支持对主题分区进行任意批处理，以便发现分区的领导者节点。
     * 这种情况下，可以通过为所有键返回单个作用域对象来支持批处理。
     * 
     * 另一方面，`FindCoordinator`请求只支持查找单个键。
     * 这种情况下，需要为每个查找键返回不同的作用域对象。
     * 
     * 注意：如果{@link ApiRequestScope#destinationBrokerId()}映射到特定的brokerId，
     * 则会跳过查找过程。参见{@link StaticBrokerStrategy}在{@link DescribeProducersHandler}
     * 中的使用示例。
     *
     * @param key 要查找的键
     * @return 请求作用域，指示如何将查找请求批量处理在一起
     */
    ApiRequestScope lookupScope(T key);

    /**
     * 为一组键构建查找请求。键的分组是通过{@link #lookupScope(Object)}方法控制的。
     * 换句话说，映射到相同请求作用域对象的每组键都会被发送到此方法。
     * 
     * 实现细节：
     * 1. 根据lookupScope返回的作用域对象对键进行分组
     * 2. 为每组键构建对应的请求，如Metadata请求或FindCoordinator请求
     * 3. 返回请求构建器，供上层调用者使用
     *
     * @param keys 需要查找的键集合
     * @return 查找请求的构建器
     */
    AbstractRequest.Builder<?> buildRequest(Set<T> keys);

    /**
     * 当查找请求成功返回时调用的回调方法。处理器需要：
     * 1. 解析响应内容
     * 2. 检查错误信息
     * 3. 返回结果，指示哪些键成功映射到brokerId，哪些键遇到致命错误（如主题授权失败）
     * 
     * 特殊情况处理：
     * - 对于收到可重试错误的键，应从结果中排除，系统会自动重试这些键
     * - 例如，如果FindCoordinator请求的响应表明协调器不可用，则应排除该键以便重试
     * 
     * 实现细节：
     * 1. 解析broker返回的响应
     * 2. 对每个键检查响应中的错误码
     * 3. 将键分类为：成功映射、致命错误、需要重试
     * 4. 构造并返回LookupResult对象
     *
     * @param keys 关联请求中的键集合
     * @param response broker返回的响应
     * @return 指示键映射成功或遇到致命错误的结果
     */
    LookupResult<T> handleResponse(Set<T> keys, AbstractResponse response);

    /**
     * 当查找请求遇到UnsupportedVersionException时调用的回调方法。
     * 对于无法处理且不应重试的键，必须将其映射到错误并返回。
     * 其余的键将被取消映射，并为它们重试查找请求。
     * 
     * 实现细节：
     * 1. 检查异常的具体原因
     * 2. 识别无法处理的键
     * 3. 将这些键映射到相应的错误
     * 
     * 默认实现：
     * - 将所有键都映射到当前异常，表示完全不支持该操作
     *
     * @param exception 不支持版本的异常
     * @param keys 请求中的键集合
     * @return 无法处理的键到错误的映射。如果异常完全无法处理，将包含所有初始键
     */
    default Map<T, Throwable> handleUnsupportedVersionException(
        UnsupportedVersionException exception,
        Set<T> keys
    ) {
        return keys.stream().collect(Collectors.toMap(k -> k, k -> exception));
    }

    /**
     * 查找结果类，用于封装查找阶段的处理结果。
     * 包含三种状态的键集合：已完成、已映射和失败的键。
     *
     * @param <K> 查找键的类型
     */
    class LookupResult<K> {
        /**
         * 已经在查找阶段完成的键集合。
         * 驱动程序不会对这些键尝试查找或执行请求。
         */
        public final List<K> completedKeys;

        /**
         * 已经映射到特定broker的键集合。
         * 键到broker ID的映射关系，用于后续API请求的执行。
         */
        public final Map<K, Integer> mappedKeys;

        /**
         * 在查找阶段遇到致命错误的键集合。
         * 驱动程序不会对这些键尝试查找或执行请求。
         */
        public final Map<K, Throwable> failedKeys;

        /**
         * 构造函数，创建只包含失败键和映射键的结果对象
         *
         * @param failedKeys 失败的键及其对应的异常
         * @param mappedKeys 成功映射的键及其对应的broker ID
         */
        public LookupResult(
            Map<K, Throwable> failedKeys,
            Map<K, Integer> mappedKeys
        ) {
            this(Collections.emptyList(), failedKeys, mappedKeys);
        }

        /**
         * 构造函数，创建包含所有三种状态键的结果对象
         *
         * @param completedKeys 已完成的键列表
         * @param failedKeys 失败的键及其对应的异常
         * @param mappedKeys 成功映射的键及其对应的broker ID
         */
        public LookupResult(
            List<K> completedKeys,
            Map<K, Throwable> failedKeys,
            Map<K, Integer> mappedKeys
        ) {
            this.completedKeys = Collections.unmodifiableList(completedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
            this.mappedKeys = Collections.unmodifiableMap(mappedKeys);
        }

        /**
         * 创建一个空的查找结果
         */
        static <K> LookupResult<K> empty() {
            return new LookupResult<>(emptyMap(), emptyMap());
        }

        /**
         * 创建一个表示单个键失败的查找结果
         *
         * @param key 失败的键
         * @param exception 导致失败的异常
         */
        static <K> LookupResult<K> failed(K key, Throwable exception) {
            return new LookupResult<>(singletonMap(key, exception), emptyMap());
        }

        /**
         * 创建一个表示单个键成功映射的查找结果
         *
         * @param key 成功映射的键
         * @param brokerId 目标broker的ID
         */
        static <K> LookupResult<K> mapped(K key, Integer brokerId) {
            return new LookupResult<>(emptyMap(), singletonMap(key, brokerId));
        }

    }

}
