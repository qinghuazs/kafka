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

import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 集群中单个共享消费者组的详细描述。
 * 
 * 该类用于描述Kafka集群中共享消费者组的详细信息，包括组成员、状态、协调器节点等信息。
 * 共享消费者组是Kafka中的一种特殊消费者组类型，允许多个消费者共享消费分区的数据。
 * 通过该类可以监控和管理共享消费者组的运行状态、成员分配和权限信息。
 */
@InterfaceStability.Evolving
public class ShareGroupDescription {
    // 共享消费者组的唯一标识符
    private final String groupId;
    // 该消费者组中的所有成员列表
    private final Collection<ShareMemberDescription> members;
    // 共享消费者组的当前状态（如：Stable、PreparingRebalance等）
    private final GroupState groupState;
    // 负责管理该共享消费者组的协调器节点
    private final Node coordinator;
    // 共享消费者组的当前纪元号，用于跟踪组的变更历史
    private final int groupEpoch;
    // 目标分配的纪元号，用于跟踪分区分配的版本
    private final int targetAssignmentEpoch;
    // 该共享消费者组被授权的操作集合
    private final Set<AclOperation> authorizedOperations;

    /**
     * 创建共享消费者组描述的构造函数（不包含权限信息）。
     *
     * @param groupId 共享消费者组ID
     * @param members 组成员列表
     * @param groupState 组状态
     * @param coordinator 协调器节点
     * @param groupEpoch 组纪元号
     * @param targetAssignmentEpoch 目标分配纪元号
     */
    public ShareGroupDescription(String groupId,
                                 Collection<ShareMemberDescription> members,
                                 GroupState groupState,
                                 Node coordinator,
                                 int groupEpoch,
                                 int targetAssignmentEpoch) {
        // 调用完整的构造函数，使用空的权限集合作为默认值
        this(groupId, members, groupState, coordinator, groupEpoch, targetAssignmentEpoch, Collections.emptySet());
    }

    /**
     * 创建共享消费者组描述的完整构造函数。
     *
     * @param groupId 共享消费者组ID
     * @param members 组成员列表
     * @param groupState 组状态
     * @param coordinator 协调器节点
     * @param groupEpoch 组纪元号
     * @param targetAssignmentEpoch 目标分配纪元号
     * @param authorizedOperations 授权操作集合
     */
    public ShareGroupDescription(String groupId,
                                 Collection<ShareMemberDescription> members,
                                 GroupState groupState,
                                 Node coordinator,
                                 int groupEpoch,
                                 int targetAssignmentEpoch,
                                 Set<AclOperation> authorizedOperations) {
        // 如果groupId为null则使用空字符串，确保groupId不为null
        this.groupId = groupId == null ? "" : groupId;
        // 如果members为null则使用空列表，并创建不可变副本
        this.members = members == null ? Collections.emptyList() : List.copyOf(members);
        // 设置组状态
        this.groupState = groupState;
        // 设置协调器节点
        this.coordinator = coordinator;
        // 设置组纪元号
        this.groupEpoch = groupEpoch;
        // 设置目标分配纪元号
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        // 设置授权操作集合
        this.authorizedOperations = authorizedOperations;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ShareGroupDescription that = (ShareGroupDescription) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(members, that.members) &&
            groupState == that.groupState &&
            Objects.equals(coordinator, that.coordinator) &&
            groupEpoch == that.groupEpoch &&
            targetAssignmentEpoch == that.targetAssignmentEpoch &&
            Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, members, groupState, coordinator, groupEpoch, targetAssignmentEpoch, authorizedOperations);
    }

    /**
     * 获取共享消费者组的ID。
     * 
     * @return 返回共享消费者组的唯一标识符
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 获取共享消费者组的所有成员列表。
     * 
     * @return 返回包含所有组成员描述信息的集合
     */
    public Collection<ShareMemberDescription> members() {
        return members;
    }

    /**
     * 获取共享消费者组的当前状态。
     * 如果状态是新版本引入的且当前客户端无法识别，则返回UNKNOWN。
     * 
     * @return 返回组的当前状态
     */
    public GroupState groupState() {
        return groupState;
    }

    /**
     * 获取负责管理该共享消费者组的协调器节点。
     * 
     * @return 返回协调器节点信息，如果协调器未知则返回null
     */
    public Node coordinator() {
        return coordinator;
    }

    /**
     * 获取该共享消费者组被授权的操作集合。
     * 
     * @return 返回授权操作的集合，如果权限信息未知则返回null
     */
    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    /**
     * 获取共享消费者组的当前纪元号。
     * 纪元号用于跟踪组成员变更的历史版本，每次成员变更都会增加纪元号。
     * 
     * @return 返回当前的组纪元号
     */
    public int groupEpoch() {
        return groupEpoch;
    }

    /**
     * 获取目标分配的纪元号。
     * 该纪元号用于跟踪分区分配的版本，每次重新分配分区时都会增加该值。
     * 
     * @return 返回目标分配的纪元号
     */
    public int targetAssignmentEpoch() {
        return targetAssignmentEpoch;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", members=" + members.stream().map(ShareMemberDescription::toString).collect(Collectors.joining(",")) +
            ", groupState=" + groupState +
            ", coordinator=" + coordinator +
            ", groupEpoch=" + groupEpoch +
            ", targetAssignmentEpoch=" + targetAssignmentEpoch +
            ", authorizedOperations=" + authorizedOperations +
            ")";
    }
}
