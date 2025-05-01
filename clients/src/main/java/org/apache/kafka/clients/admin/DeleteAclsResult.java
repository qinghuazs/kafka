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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * {@link Admin#deleteAcls(Collection)}调用的结果类。
 * 
 * 此类用于处理批量删除ACL操作的结果。ACL（访问控制列表）是Kafka中用于实现细粒度权限控制的机制，
 * 可以控制用户对特定资源（如Topic、Group等）的访问权限。
 * 
 * 应用场景：
 * 1. 批量删除特定资源的访问控制规则
 * 2. 权限清理和权限重组
 * 3. 撤销之前授予的访问权限
 * 
 * 注意：该类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteAclsResult {

    /**
     * 用于封装单个ACL删除操作的结果。
     * 包含了成功删除的ACL绑定信息或删除失败时的异常信息。
     */
    public static class FilterResult {
        // 成功删除的ACL绑定信息
        private final AclBinding binding;
        // 删除失败时的异常信息
        private final ApiException exception;

        FilterResult(AclBinding binding, ApiException exception) {
            this.binding = binding;
            this.exception = exception;
        }

        /**
         * 返回已删除的ACL绑定信息。
         * 
         * @return 如果删除成功，返回AclBinding对象；如果发生错误，返回null
         */
        public AclBinding binding() {
            return binding;
        }

        /**
         * 返回删除操作的异常信息。
         * 
         * @return 如果删除失败，返回相关的异常；如果删除成功，返回null
         */
        public ApiException exception() {
            return exception;
        }
    }

    /**
     * 用于封装批量删除ACL操作的结果集合。
     * 包含了针对特定过滤条件的所有删除操作结果。
     */
    public static class FilterResults {
        // 存储所有删除操作的结果列表
        private final List<FilterResult> values;

        FilterResults(List<FilterResult> values) {
            this.values = values;
        }

        /**
         * 返回针对特定过滤条件的所有删除操作结果。
         * 
         * @return 包含所有删除操作结果的列表
         */
        public List<FilterResult> values() {
            return values;
        }
    }

    // 存储每个ACL过滤条件对应的异步操作Future
    private final Map<AclBindingFilter, KafkaFuture<FilterResults>> futures;

    DeleteAclsResult(Map<AclBindingFilter, KafkaFuture<FilterResults>> futures) {
        this.futures = futures;
    }

    /**
     * 返回ACL过滤条件到对应删除操作Future的映射。
     * 可用于分别检查每个过滤条件的删除操作状态。
     * 
     * @return 包含所有删除操作Future的映射
     */
    public Map<AclBindingFilter, KafkaFuture<FilterResults>> values() {
        return futures;
    }

    /**
     * 返回一个Future，该Future仅在所有ACL删除操作都成功时才成功完成。
     * 
     * 特点：
     * 1. 返回所有成功删除的ACL信息
     * 2. 如果过滤条件未匹配到任何ACL，不会视为错误
     * 3. 任一删除操作失败都会导致整个操作失败
     * 
     * @return 包含所有已删除ACL的Future
     */
    public KafkaFuture<Collection<AclBinding>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(v -> getAclBindings(futures));
    }

    /**
     * 从所有Future中获取已删除的ACL绑定信息。
     * 
     * 实现细节：
     * 1. 遍历所有Future获取删除结果
     * 2. 检查每个删除操作是否存在异常
     * 3. 收集所有成功删除的ACL绑定
     * 
     * @param futures 包含所有删除操作Future的映射
     * @return 所有成功删除的ACL绑定列表
     * @throws KafkaException 当发生内部错误时抛出
     * @throws ApiException 当任一删除操作失败时抛出
     */
    private List<AclBinding> getAclBindings(Map<AclBindingFilter, KafkaFuture<FilterResults>> futures) {
        List<AclBinding> acls = new ArrayList<>();
        for (KafkaFuture<FilterResults> value: futures.values()) {
            FilterResults results;
            try {
                // 获取异步操作的结果
                results = value.get();
            } catch (Throwable e) {
                // 此处不应该到达，因为KafkaFuture#allOf在有Future失败时就会失败
                throw new KafkaException("DeleteAclsResult#all: internal error", e);
            }
            for (FilterResult result : results.values()) {
                // 如果存在异常，立即抛出
                if (result.exception() != null)
                    throw result.exception();
                // 添加成功删除的ACL绑定
                acls.add(result.binding());
            }
        }
        return acls;
    }
}
