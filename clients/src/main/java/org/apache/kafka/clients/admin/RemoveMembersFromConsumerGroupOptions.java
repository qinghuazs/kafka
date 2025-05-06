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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 用于配置{@link AdminClient#removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)}方法的选项类。
 * 该类用于携带需要从消费者组中移除的成员信息。
 * 
 * 应用场景：
 * 1. 当需要从消费者组中移除特定成员时，例如在重新平衡或维护时
 * 2. 当需要强制移除不活跃或故障的消费者组成员时
 * 3. 支持批量移除多个成员，提高操作效率
 * 
 * 注意：该类的API仍在演进中，详细信息请参见{@link AdminClient}。
 */
@InterfaceStability.Evolving
public class RemoveMembersFromConsumerGroupOptions extends AbstractOptions<RemoveMembersFromConsumerGroupOptions> {

    /**
     * 存储需要移除的消费者组成员集合
     * 使用Set确保成员的唯一性，避免重复操作
     */
    private final Set<MemberToRemove> members;

    /**
     * 记录移除操作的原因，用于审计和日志记录
     * 可选字段，帮助追踪和记录移除操作的目的
     */
    private String reason;

    /**
     * 构造函数，用于创建包含指定成员列表的选项实例
     * 
     * @param members 需要移除的成员集合
     * @throws IllegalArgumentException 当提供的成员集合为空时抛出异常
     */
    public RemoveMembersFromConsumerGroupOptions(Collection<MemberToRemove> members) {
        if (members.isEmpty()) {
            throw new IllegalArgumentException("Invalid empty members has been provided");
        }
        // 创建成员集合的副本，避免外部修改影响内部状态
        this.members = new HashSet<>(members);
    }

    /**
     * 无参构造函数，创建一个空的选项实例
     * 用于移除消费者组中的所有成员的场景
     */
    public RemoveMembersFromConsumerGroupOptions() {
        this.members = Collections.emptySet();
    }

    /**
     * 设置移除操作的原因
     * 
     * @param reason 移除操作的原因说明
     */
    public void reason(final String reason) {
        this.reason = reason;
    }

    /**
     * 获取需要移除的成员集合
     * 
     * @return 返回待移除的成员集合
     */
    public Set<MemberToRemove> members() {
        return members;
    }

    /**
     * 获取移除操作的原因
     * 
     * @return 返回移除原因，如果未设置则返回null
     */
    public String reason() {
        return reason;
    }

    /**
     * 判断是否移除消费者组中的所有成员
     * 
     * @return 如果members为空集合返回true，表示移除所有成员；否则返回false
     */
    public boolean removeAll() {
        return members.isEmpty();
    }
}
