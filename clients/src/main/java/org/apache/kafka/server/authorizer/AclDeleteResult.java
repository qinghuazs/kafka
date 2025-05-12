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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * 表示ACL删除操作的结果。
 * <p>
 * 应用场景:
 * 当通过 {@link org.apache.kafka.clients.admin.Admin#deleteAcls(java.util.Collection)} 方法删除ACL时，此对象用于封装操作的整体结果。
 * 它包含了可能发生的顶层异常以及每个尝试删除的ACL绑定的具体结果。
 * </p>
 * 设计考虑:
 * <ul>
 *     <li>封装了批量删除操作的复杂性，提供了一个统一的结果视图。</li>
 *     <li>通过 {@link #exception()} 方法暴露顶层错误，例如在尝试匹配过滤器时发生的错误。</li>
 *     <li>通过 {@link #aclBindingDeleteResults()} 方法提供每个ACL绑定的详细删除状态，允许调用者了解哪些ACL成功删除，哪些失败以及失败原因。</li>
 * </ul>
 */
@InterfaceStability.Evolving
public class AclDeleteResult {
    // 尝试匹配ACL过滤器以删除ACL时发生的任何异常
    private final ApiException exception;
    // 每个匹配的ACL绑定的删除结果集合
    private final Collection<AclBindingDeleteResult> aclBindingDeleteResults;

    /**
     * 构造函数，用于表示删除操作因顶层异常而失败的情况。
     * @param exception 发生的 {@link ApiException} 异常。
     */
    public AclDeleteResult(ApiException exception) {
        // 调用私有构造函数，传入空的删除结果集合和异常
        this(Collections.emptySet(), exception);
    }

    /**
     * 构造函数，用于表示删除操作已尝试，并包含每个ACL绑定的删除结果。
     * @param deleteResults 每个ACL绑定的删除结果集合。
     */
    public AclDeleteResult(Collection<AclBindingDeleteResult> deleteResults) {
        // 调用私有构造函数，传入删除结果集合，异常为null
        this(deleteResults, null);
    }

    /**
     * 私有构造函数，用于初始化 {@link AclDeleteResult}。
     * @param deleteResults 每个ACL绑定的删除结果集合。
     * @param exception 尝试匹配ACL过滤器时发生的任何异常，如果没有则为null。
     */
    private AclDeleteResult(Collection<AclBindingDeleteResult> deleteResults, ApiException exception) {
        // 初始化ACL绑定删除结果集合
        this.aclBindingDeleteResults = deleteResults;
        // 初始化异常
        this.exception = exception;
    }

    /**
     * 返回在尝试匹配ACL过滤器以删除ACL时发生的任何异常。
     * 如果异常为空，则表示筛选成功。请参阅 {@link #aclBindingDeleteResults()}
     * 查看每个过滤器的删除结果。
     * @return 包含 {@link ApiException} 的 {@link Optional}，如果筛选过程中没有发生异常，则为空。
     */
    public Optional<ApiException> exception() {
        // 如果exception字段为null，则返回一个空的Optional对象
        // 否则，返回一个包含exception字段的Optional对象
        return exception == null ? Optional.empty() : Optional.of(exception);
    }

    /**
     * 返回每个匹配的ACL绑定的删除结果。
     * @return {@link AclBindingDeleteResult} 的集合，表示每个匹配的ACL绑定的删除状态。
     */
    public Collection<AclBindingDeleteResult> aclBindingDeleteResults() {
        // 返回存储ACL绑定删除结果的集合
        return aclBindingDeleteResults;
    }


    /**
     * 针对与删除过滤器匹配的每个ACL绑定的删除结果。
     * <p>
     * 应用场景:
     * 当批量删除ACL时，这个内部类用于表示单个ACL绑定的删除尝试结果。
     * 它包含了被尝试删除的ACL绑定以及操作中可能发生的任何异常。
     * </p>
     * 设计考虑:
     * <ul>
     *     <li>清晰地将单个ACL的删除结果与整体操作结果分开。</li>
     *     <li>允许调用者检查每个ACL绑定的具体状态，包括成功删除或失败原因。</li>
     * </ul>
     */
    public static class AclBindingDeleteResult {
        // 匹配删除过滤器的ACL绑定
        private final AclBinding aclBinding;
        // 删除此ACL绑定时发生的任何异常
        private final ApiException exception;

        /**
         * 构造函数，用于表示ACL绑定成功删除的情况。
         * @param aclBinding 已成功删除的 {@link AclBinding}。
         */
        public AclBindingDeleteResult(AclBinding aclBinding) {
            // 调用另一个构造函数，传入ACL绑定和null作为异常（表示没有异常）
            this(aclBinding, null);
        }

        /**
         * 构造函数，用于表示ACL绑定的删除结果，可能成功也可能失败。
         * @param aclBinding 尝试删除的 {@link AclBinding}。
         * @param exception 删除操作中发生的 {@link ApiException}，如果删除成功则为null。
         */
        public AclBindingDeleteResult(AclBinding aclBinding, ApiException exception) {
            // 初始化ACL绑定
            this.aclBinding = aclBinding;
            // 初始化异常
            this.exception = exception;
        }

        /**
         * 返回与删除过滤器匹配的ACL绑定。如果 {@link #exception()} 为空，
         * 则表示该ACL绑定已成功删除。
         * @return 匹配的 {@link AclBinding}。
         */
        public AclBinding aclBinding() {
            // 返回ACL绑定对象
            return aclBinding;
        }

        /**
         * 返回导致删除ACL绑定失败的任何异常。
         * 如果异常为空，则表示该ACL绑定已成功删除。
         * @return 包含 {@link ApiException} 的 {@link Optional}，如果删除过程中没有发生异常，则为空。
         */
        public Optional<ApiException> exception() {
            // 如果exception字段为null，则返回一个空的Optional对象
            // 否则，返回一个包含exception字段的Optional对象
            return exception == null ? Optional.empty() : Optional.of(exception);
        }
    }
}
