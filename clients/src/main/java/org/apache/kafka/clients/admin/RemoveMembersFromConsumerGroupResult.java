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
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.Set;

/**
 * {@link Admin#removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)} 调用的结果类。
 * 该类用于处理从消费者组中移除成员的异步操作结果。
 * 
 * 应用场景：
 * 1. 当需要从消费者组中移除特定成员时，比如在重新分配分区或缩减消费者组规模时
 * 2. 在消费者组维护过程中，需要强制移除不活跃或故障的消费者时
 * 3. 在系统重构时，需要批量调整消费者组成员结构
 * 
 * 注意：该类的API仍在演进中，详见 {@link Admin}
 */
public class RemoveMembersFromConsumerGroupResult {

    /**
     * 存储移除操作的Future结果
     * - Key: 成员身份标识（MemberIdentity）
     * - Value: 对应的错误信息（Errors）
     * 用于追踪每个成员的移除操作状态
     */
    private final KafkaFuture<Map<MemberIdentity, Errors>> future;

    /**
     * 存储要移除的成员信息集合
     * 如果集合为空，表示移除所有成员
     * 如果集合不为空，则只移除指定的成员
     */
    private final Set<MemberToRemove> memberInfos;

    /**
     * 构造函数，初始化移除操作的结果对象
     * 
     * @param future 包含成员移除操作结果的Future
     * @param memberInfos 要移除的成员信息集合
     */
    RemoveMembersFromConsumerGroupResult(KafkaFuture<Map<MemberIdentity, Errors>> future,
                                         Set<MemberToRemove> memberInfos) {
        this.future = future;
        this.memberInfos = memberInfos;
    }

    /**
     * 返回一个Future，用于检查所有成员的移除操作是否全部成功
     * 只有当所有成员都成功移除（没有顶层错误和成员级别错误）时，该Future才会成功完成
     * 如果有任何错误，将返回第一个遇到的成员错误
     * 
     * @return 返回一个KafkaFuture<Void>，表示整体操作的完成状态
     */
    public KafkaFuture<Void> all() {
        // 创建新的Future用于返回结果
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        
        // 处理移除操作的完成回调
        this.future.whenComplete((memberErrors, throwable) -> {
            // 如果有异常发生，直接完成Future并返回异常
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                // 检查是否是移除所有成员的模式
                if (removeAll()) {
                    // 遍历所有成员的错误信息
                    for (Map.Entry<MemberIdentity, Errors> entry: memberErrors.entrySet()) {
                        Exception exception = entry.getValue().exception();
                        // 如果有任何成员移除失败，返回第一个错误
                        if (exception != null) {
                            Throwable ex = new KafkaException("Encounter exception when trying to remove: "
                                    + entry.getKey(), exception);
                            result.completeExceptionally(ex);
                            return;
                        }
                    }
                } else {
                    // 遍历指定要移除的成员列表
                    for (MemberToRemove memberToRemove : memberInfos) {
                        // 检查每个成员的移除结果
                        if (maybeCompleteExceptionally(memberErrors, memberToRemove.toMemberIdentity(), result)) {
                            return;
                        }
                    }
                }
                // 所有成员都成功移除，完成Future
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * 返回指定成员的移除操作结果
     * 该方法用于单独查询某个特定成员的移除状态
     * 
     * @param member 要查询的成员信息
     * @return 返回该成员移除操作的Future结果
     * @throws IllegalArgumentException 当处于移除所有成员模式时，或者指定的成员不在原始请求中时抛出
     */
    public KafkaFuture<Void> memberResult(MemberToRemove member) {
        // 检查是否处于移除所有成员模式
        if (removeAll()) {
            throw new IllegalArgumentException("The method: memberResult is not applicable in 'removeAll' mode");
        }
        // 验证成员是否在原始请求中
        if (!memberInfos.contains(member)) {
            throw new IllegalArgumentException("Member " + member + " was not included in the original request");
        }

        // 创建新的Future用于返回结果
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        // 处理移除操作的完成回调
        this.future.whenComplete((memberErrors, throwable) -> {
            if (throwable != null) {
                // 如果有异常发生，直接完成Future并返回异常
                result.completeExceptionally(throwable);
            } else if (!maybeCompleteExceptionally(memberErrors, member.toMemberIdentity(), result)) {
                // 如果没有错误，成功完成Future
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * 检查并处理成员移除操作中的错误
     * 
     * @param memberErrors 所有成员的错误信息映射
     * @param member 要检查的成员身份
     * @param result 用于完成的Future对象
     * @return 如果发现错误并完成Future则返回true，否则返回false
     */
    private boolean maybeCompleteExceptionally(Map<MemberIdentity, Errors> memberErrors,
                                               MemberIdentity member,
                                               KafkaFutureImpl<Void> result) {
        // 获取成员级别的错误信息
        Throwable exception = KafkaAdminClient.getSubLevelError(memberErrors, member,
            "Member \"" + member + "\" was not included in the removal response");
        if (exception != null) {
            // 如果存在错误，完成Future并返回异常
            result.completeExceptionally(exception);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 检查是否是移除所有成员的模式
     * 
     * @return 如果memberInfos为空（表示移除所有成员）返回true，否则返回false
     */
    private boolean removeAll() {
        return memberInfos.isEmpty();
    }
}
