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

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.OptionalInt;
import java.util.Set;

/**
 * 这个查找策略用于已知目标broker ID的场景，此时无需进行显式的查找操作。
 * 通过在{@link #lookupScope(Object)}的返回值中设置{@link ApiRequestScope#destinationBrokerId()}，
 * 驱动器将跳过查找过程。
 *
 * 应用场景：
 * 1. 当我们确切知道要与哪个broker通信时使用此策略
 * 2. 用于优化性能，避免不必要的元数据查找
 * 3. 适用于直接针对特定broker的管理操作
 *
 * 设计考虑：
 * 1. 通过内部类SingleBrokerScope封装broker ID，提供清晰的作用域
 * 2. 实现AdminApiLookupStrategy接口，但不支持buildRequest和handleResponse操作
 * 3. 使用泛型参数K支持不同类型的请求key
 */
public class StaticBrokerStrategy<K> implements AdminApiLookupStrategy<K> {
    /**
     * 单一broker作用域实例，用于存储目标broker ID
     */
    private final SingleBrokerScope scope;

    /**
     * 构造函数
     * @param brokerId 目标broker的ID
     */
    public StaticBrokerStrategy(int brokerId) {
        this.scope = new SingleBrokerScope(brokerId);
    }

    /**
     * 返回包含目标broker ID的作用域
     * 实现说明：直接返回构造时创建的scope实例，因为目标broker已确定
     *
     * @param key 请求的key（在此策略中被忽略）
     * @return 包含目标broker ID的ApiRequestScope实例
     */
    @Override
    public ApiRequestScope lookupScope(K key) {
        return scope;
    }

    /**
     * 构建请求（不支持）
     * 实现说明：由于此策略仅用于直接指定目标broker，不需要构建查找请求
     *
     * @param keys 请求的key集合
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public AbstractRequest.Builder<?> buildRequest(Set<K> keys) {
        throw new UnsupportedOperationException();
    }

    /**
     * 处理响应（不支持）
     * 实现说明：由于此策略仅用于直接指定目标broker，不需要处理查找响应
     *
     * @param keys 请求的key集合
     * @param response 服务器的响应
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public LookupResult<K> handleResponse(Set<K> keys, AbstractResponse response) {
        throw new UnsupportedOperationException();
    }

    /**
     * 内部类，实现ApiRequestScope接口
     * 用于封装单个broker ID并提供访问方法
     */
    private static class SingleBrokerScope implements ApiRequestScope {
        /**
         * 目标broker的ID
         */
        private final int brokerId;

        /**
         * 构造函数
         * @param brokerId 目标broker的ID
         */
        private SingleBrokerScope(int brokerId) {
            this.brokerId = brokerId;
        }

        /**
         * 返回目标broker ID
         * 实现说明：将broker ID包装在OptionalInt中返回，表示一定存在目标broker
         *
         * @return 包含broker ID的OptionalInt实例
         */
        @Override
        public OptionalInt destinationBrokerId() {
            return OptionalInt.of(brokerId);
        }
    }
}
