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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Objects;
import java.util.Optional;

/**
 * 消费者组元数据类
 * 包含了消费者组的核心信息，用于标识和管理消费者组成员
 * 注意：对此类的任何修改都被视为公共API变更，需要通过KIP流程
 * 
 * 设计原理：
 * 1. 提供消费者组的唯一标识
 * 2. 跟踪组成员的生命周期
 * 3. 支持静态成员机制
 * 
 * 核心属性：
 * 1. groupId: 消费者组的唯一标识
 * 2. generationId: 标识rebalance的代际
 * 3. memberId: 消费者在组内的唯一标识
 * 4. groupInstanceId: 静态成员的实例ID
 */
public class ConsumerGroupMetadata {
    // 消费者组ID，用于标识一个消费者组
    private final String groupId;
    // 消费者组的代际ID，每次rebalance都会递增
    private final int generationId;
    // 消费者在组内的成员ID，由coordinator分配
    private final String memberId;
    // 静态成员ID，用于支持静态成员机制
    private final Optional<String> groupInstanceId;

    /**
     * 完整构造函数
     * 创建包含所有元数据信息的消费者组元数据对象
     *
     * @param groupId 消费者组ID，不能为null
     * @param generationId 代际ID，标识rebalance的版本
     * @param memberId 成员ID，不能为null
     * @param groupInstanceId 静态成员ID，不能为null但可以为空Optional
     */
    public ConsumerGroupMetadata(String groupId,
                                 int generationId,
                                 String memberId,
                                 Optional<String> groupInstanceId) {
        this.groupId = Objects.requireNonNull(groupId, "group.id can't be null");
        this.generationId = generationId;
        this.memberId = Objects.requireNonNull(memberId, "member.id can't be null");
        this.groupInstanceId = Objects.requireNonNull(groupInstanceId, "group.instance.id can't be null");
    }

    /**
     * 简化构造函数
     * 仅使用groupId创建元数据对象，其他字段使用默认值
     * 适用于首次加入组或不需要完整元数据的场景
     *
     * @param groupId 消费者组ID
     */
    public ConsumerGroupMetadata(String groupId) {
        this(groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            Optional.empty());
    }

    /**
     * 获取消费者组ID
     * @return 消费者组的唯一标识
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 获取代际ID
     * @return 当前的代际ID，标识rebalance的版本
     */
    public int generationId() {
        return generationId;
    }

    /**
     * 获取成员ID
     * @return 消费者在组内的唯一标识
     */
    public String memberId() {
        return memberId;
    }

    /**
     * 获取静态成员ID
     * @return 静态成员的实例ID，如果不是静态成员则为空
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    @Override
    public String toString() {
        return String.format("GroupMetadata(groupId = %s, generationId = %d, memberId = %s, groupInstanceId = %s)",
            groupId,
            generationId,
            memberId,
            groupInstanceId.orElse(""));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConsumerGroupMetadata that = (ConsumerGroupMetadata) o;
        return generationId == that.generationId &&
            Objects.equals(groupId, that.groupId) &&
            Objects.equals(memberId, that.memberId) &&
            Objects.equals(groupInstanceId, that.groupInstanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, generationId, memberId, groupInstanceId);
    }

}
