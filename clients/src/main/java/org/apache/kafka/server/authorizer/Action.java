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

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.Objects;

/**
 * @InterfaceStability.Evolving
 * 表示一个授权操作，封装了操作、资源模式、资源引用计数以及是否记录允许/拒绝的日志标志。
 * 这个类在 Kafka 的授权机制中扮演核心角色，用于定义和评估对特定资源的特定操作的权限。
 * 例如，当一个客户端尝试读取一个主题时，会创建一个 Action 对象来表示这个意图，
 * 然后授权器会根据配置的 ACL（访问控制列表）来判断这个 Action 是否被允许。
 */
@InterfaceStability.Evolving
public class Action {

    // 资源模式，定义了此操作针对的资源，例如特定主题或集群。
    private final ResourcePattern resourcePattern;
    // ACL 操作，表示正在执行的操作类型，例如读取、写入或创建。
    private final AclOperation operation;
    // 资源引用计数，表示请求中引用此资源的次数。例如，一个请求可能引用同一主题的 n 个分区，此时 resourceReferenceCount 为 n。
    private final int resourceReferenceCount;
    // 如果结果为 ALLOWED，是否在审计日志中记录此操作。
    private final boolean logIfAllowed;
    // 如果结果为 DENIED，是否在审计日志中记录此操作。
    private final boolean logIfDenied;

    /**
     * 构造一个 Action 实例。
     * 设计考虑：构造函数确保了 operation 和 resourcePattern 不为 null，这是 Action 对象有效性的基本保证。
     * resourceReferenceCount 用于优化授权检查，特别是当一个请求涉及多个相同资源实例时。
     * logIfAllowed 和 logIfDenied 标志提供了对审计日志记录行为的精细控制。
     *
     * @param operation 正在执行的非空操作
     * @param resourcePattern 此操作正在执行的非空资源模式
     * @param resourceReferenceCount 资源被引用的次数
     * @param logIfAllowed 如果授权结果为 ALLOWED，是否记录日志
     * @param logIfDenied 如果授权结果为 DENIED，是否记录日志
     */
    public Action(AclOperation operation,
                  ResourcePattern resourcePattern,
                  int resourceReferenceCount,
                  boolean logIfAllowed,
                  boolean logIfDenied) {
        // 确保 operation 参数不为 null，否则抛出 NullPointerException
        this.operation = Objects.requireNonNull(operation, "operation can't be null");
        // 确保 resourcePattern 参数不为 null，否则抛出 NullPointerException
        this.resourcePattern = Objects.requireNonNull(resourcePattern, "resourcePattern can't be null");
        // 设置 logIfAllowed 标志
        this.logIfAllowed = logIfAllowed;
        // 设置 logIfDenied 标志
        this.logIfDenied = logIfDenied;
        // 设置 resourceReferenceCount
        this.resourceReferenceCount = resourceReferenceCount;
    }

    /**
     * 获取此操作正在执行的资源模式。
     * 应用场景：授权器使用此方法来确定操作针对的具体资源，以便与 ACL 中的资源模式进行匹配。
     * @return 一个非空的资源模式
     */
    public ResourcePattern resourcePattern() {
        // 返回存储的 resourcePattern 字段
        return resourcePattern;
    }

    /**
     * 获取正在执行的操作。
     * 应用场景：授权器使用此方法来确定请求的操作类型（如 READ, WRITE），以便与 ACL 中定义的操作权限进行比较。
     * @return 一个非空的操作
     */
    public AclOperation operation() {
        // 返回存储的 operation 字段
        return operation;
    }

    /**
     * 指示如果结果为 ALLOWED，跟踪 ALLOWED 访问的审计日志是否应包含此操作。
     * 如果由于此授权导致在处理请求时授予对资源的访问权限，则该标志为 true。
     * 仅当请求用于描述访问权限，而实际上没有根据授权结果对资源执行任何操作时，该标志才为 false。
     * 应用场景：控制审计日志的详细程度。例如，对于 DescribeConfigs 请求，即使授权成功，也可能不需要记录，因为它不直接修改或访问数据。
     * @return 如果允许访问时应记录日志，则返回 true
     */
    public boolean logIfAllowed() {
        // 返回存储的 logIfAllowed 标志
        return logIfAllowed;
    }

    /**
     * 指示如果结果为 DENIED，跟踪 DENIED 访问的审计日志是否应包含此操作。
     * 如果显式请求了对资源的访问，并且由于此授权请求而拒绝了该请求，则该标志为 true。
     * 如果请求正在筛选出已授权的资源（例如，订阅正则表达式模式），则该标志为 false。
     * 如果这是一个可选的授权，当此授权失败时会应用替代资源授权（例如，Cluster:Create 被 Topic:Create 覆盖），则该标志也为 false。
     * 应用场景：同样用于控制审计日志的详细程度。对于某些拒绝情况，例如尝试订阅一个不存在的主题（通过正则表达式），可能不需要记录为一次明确的拒绝。
     * 设计考虑：这种区分有助于减少不必要的日志条目，使审计日志更关注于明确的权限冲突。
     * @return 如果拒绝访问时应记录日志，则返回 true
     */
    public boolean logIfDenied() {
        // 返回存储的 logIfDenied 标志
        return logIfDenied;
    }

    /**
     * 请求中被授权资源被引用的次数。例如，单个请求可能引用同一主题的 `n` 个主题分区。
     * Broker 将使用 `resourceReferenceCount=n` 对主题进行一次授权。授权器可以在审计日志中包含此计数。
     * 应用场景：当一个请求涉及对同一逻辑资源的多个实例进行操作时（例如，一个 FetchRequest 可能从一个主题的多个分区读取数据），
     * 这个计数可以帮助授权器了解操作的规模，并可能用于更精细的审计或配额管理。
     * 设计考虑：这避免了对每个分区进行单独的授权检查，提高了效率。
     * @return 资源引用计数
     */
    public int resourceReferenceCount() {
        // 返回存储的 resourceReferenceCount 字段
        return resourceReferenceCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Action)) {
            return false;
        }

        Action that = (Action) o;
        return Objects.equals(this.resourcePattern, that.resourcePattern) &&
            Objects.equals(this.operation, that.operation) &&
            this.resourceReferenceCount == that.resourceReferenceCount &&
            this.logIfAllowed == that.logIfAllowed &&
            this.logIfDenied == that.logIfDenied;

    }

    @Override
    public int hashCode() {
        return Objects.hash(resourcePattern, operation, resourceReferenceCount, logIfAllowed, logIfDenied);
    }

    @Override
    public String toString() {
        return "Action(" +
            "resourcePattern='" + resourcePattern + '\'' +
            ", operation='" + operation + '\'' +
            ", resourceReferenceCount='" + resourceReferenceCount + '\'' +
            ", logIfAllowed='" + logIfAllowed + '\'' +
            ", logIfDenied='" + logIfDenied + '\'' +
            ')';
    }
}
