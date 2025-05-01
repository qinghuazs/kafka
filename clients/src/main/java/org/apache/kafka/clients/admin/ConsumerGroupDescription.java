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

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 表示Kafka集群中单个消费者组的详细描述。
 * 这个类包含了消费者组的所有核心属性，用于管理和监控消费者组的状态。
 *
 * 应用场景：
 * 1. 消费者组管理：查看组成员、分区分配策略等
 * 2. 监控和运维：检查组状态、协调器节点等
 * 3. 权限控制：管理消费者组的操作权限
 * 4. 版本兼容：支持经典消费者组和新版消费者组
 */
public class ConsumerGroupDescription {
    /**
     * 消费者组的唯一标识符
     * 在整个Kafka集群中必须是唯一的
     */
    private final String groupId;

    /**
     * 是否是简单消费者组
     * 简单消费者组不使用Kafka的组管理功能
     */
    private final boolean isSimpleConsumerGroup;

    /**
     * 消费者组中的所有成员列表
     * 包含每个成员的详细信息（如成员ID、客户端ID等）
     */
    private final Collection<MemberDescription> members;

    /**
     * 分区分配器的名称
     * 用于在消费者组成员之间分配主题分区
     * 常见的分配器包括RangeAssignor、RoundRobinAssignor等
     */
    private final String partitionAssignor;

    /**
     * 消费者组的类型
     * 可以是经典类型（CLASSIC）或新版本类型（CONSUMER）
     */
    private final GroupType type;

    /**
     * 消费者组的当前状态
     * 表示组的运行状态，如Empty、Stable、PreparingRebalance等
     */
    private final GroupState groupState;

    /**
     * 负责协调该消费者组的节点
     * 处理组成员管理、分区分配等操作
     */
    private final Node coordinator;

    /**
     * 该消费者组被授权的操作集合
     * 定义了可以对该组执行哪些操作
     */
    private final Set<AclOperation> authorizedOperations;

    /**
     * 消费者组的纪元号
     * 用于跟踪组的版本变化，仅在新版本消费者组中使用
     */
    private final Optional<Integer> groupEpoch;

    /**
     * 目标分配的纪元号
     * 用于跟踪分区分配的版本，仅在新版本消费者组中使用
     */
    private final Optional<Integer> targetAssignmentEpoch;

    /**
     * 已废弃的构造函数
     * 创建一个基本的消费者组描述实例，不包含权限信息
     * 
     * @param groupId 消费者组ID
     * @param isSimpleConsumerGroup 是否是简单消费者组
     * @param members 组成员列表
     * @param partitionAssignor 分区分配器名称
     * @param state 组状态
     * @param coordinator 协调器节点
     * 
     * @deprecated 从4.0版本开始废弃。请使用 {@link #ConsumerGroupDescription(String, boolean, Collection, String, GroupType, GroupState, Node, Set, Optional, Optional)} 代替
     */
    @Deprecated
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    ConsumerGroupState state,
                                    Node coordinator) {
        // 使用空的权限集合调用下一个构造函数
        this(groupId, isSimpleConsumerGroup, members, partitionAssignor, state, coordinator, Collections.emptySet());
    }

    /**
     * 已废弃的构造函数
     * 创建一个包含权限信息的消费者组描述实例，使用经典类型
     * 
     * @param groupId 消费者组ID
     * @param isSimpleConsumerGroup 是否是简单消费者组
     * @param members 组成员列表
     * @param partitionAssignor 分区分配器名称
     * @param state 组状态
     * @param coordinator 协调器节点
     * @param authorizedOperations 授权操作集合
     * 
     * @deprecated 从4.0版本开始废弃。请使用 {@link #ConsumerGroupDescription(String, boolean, Collection, String, GroupType, GroupState, Node, Set, Optional, Optional)} 代替
     */
    @Deprecated
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    ConsumerGroupState state,
                                    Node coordinator,
                                    Set<AclOperation> authorizedOperations) {
        // 使用CLASSIC类型调用下一个构造函数
        this(groupId, isSimpleConsumerGroup, members, partitionAssignor, GroupType.CLASSIC, state, coordinator, authorizedOperations);
    }

    /**
     * 已废弃的构造函数
     * 创建一个完整的消费者组描述实例，但不包含纪元信息
     * 
     * @param groupId 消费者组ID
     * @param isSimpleConsumerGroup 是否是简单消费者组
     * @param members 组成员列表
     * @param partitionAssignor 分区分配器名称
     * @param type 组类型
     * @param state 组状态
     * @param coordinator 协调器节点
     * @param authorizedOperations 授权操作集合
     * 
     * @deprecated 从4.0版本开始废弃。请使用 {@link #ConsumerGroupDescription(String, boolean, Collection, String, GroupType, GroupState, Node, Set, Optional, Optional)} 代替
     */
    @Deprecated
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    GroupType type,
                                    ConsumerGroupState state,
                                    Node coordinator,
                                    Set<AclOperation> authorizedOperations) {
        // 处理null值，设置默认值
        this.groupId = groupId == null ? "" : groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        // 创建成员列表的不可变副本
        this.members = members == null ? Collections.emptyList() : List.copyOf(members);
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.type = type;
        // 将ConsumerGroupState转换为GroupState
        this.groupState = GroupState.parse(state.toString());
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
        // 纪元信息设置为空
        this.groupEpoch = Optional.empty();
        this.targetAssignmentEpoch = Optional.empty();
    }

    /**
     * 创建一个完整的消费者组描述实例
     * 这是推荐使用的构造函数，支持所有功能特性
     * 
     * @param groupId 消费者组ID，如果为null则使用空字符串
     * @param isSimpleConsumerGroup 是否是简单消费者组
     * @param members 组成员列表，如果为null则使用空列表
     * @param partitionAssignor 分区分配器名称，如果为null则使用空字符串
     * @param type 组类型（CLASSIC或CONSUMER）
     * @param groupState 组状态
     * @param coordinator 协调器节点
     * @param authorizedOperations 授权操作集合
     * @param groupEpoch 组纪元号（仅用于CONSUMER类型）
     * @param targetAssignmentEpoch 目标分配纪元号（仅用于CONSUMER类型）
     */
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    GroupType type,
                                    GroupState groupState,
                                    Node coordinator,
                                    Set<AclOperation> authorizedOperations,
                                    Optional<Integer> groupEpoch,
                                    Optional<Integer> targetAssignmentEpoch) {
        // 处理null值，设置默认值
        this.groupId = groupId == null ? "" : groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        // 创建成员列表的不可变副本
        this.members = members == null ? Collections.emptyList() : List.copyOf(members);
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.type = type;
        this.groupState = groupState;
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
        this.groupEpoch = groupEpoch;
        this.targetAssignmentEpoch = targetAssignmentEpoch;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConsumerGroupDescription that = (ConsumerGroupDescription) o;
        return isSimpleConsumerGroup == that.isSimpleConsumerGroup &&
            Objects.equals(groupId, that.groupId) &&
            Objects.equals(members, that.members) &&
            Objects.equals(partitionAssignor, that.partitionAssignor) &&
            type == that.type &&
            groupState == that.groupState &&
            Objects.equals(coordinator, that.coordinator) &&
            Objects.equals(authorizedOperations, that.authorizedOperations) &&
            Objects.equals(groupEpoch, that.groupEpoch) &&
            Objects.equals(targetAssignmentEpoch, that.targetAssignmentEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, isSimpleConsumerGroup, members, partitionAssignor, type, groupState, coordinator,
            authorizedOperations, groupEpoch, targetAssignmentEpoch);
    }

    /**
     * 获取消费者组的ID
     * 消费者组ID是该组的唯一标识符，用于在Kafka集群中区分不同的消费者组
     * 
     * 使用场景：
     * 1. 查询特定消费者组的信息
     * 2. 管理消费者组的配置
     * 3. 监控消费者组的状态
     * 
     * @return 消费者组ID，永远不会为null，如果未设置则返回空字符串
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 判断是否为简单消费者组
     * 简单消费者组不使用Kafka的组管理功能，通常用于简单的消费场景
     * 
     * 使用场景：
     * 1. 确定消费者组的管理模式
     * 2. 选择合适的监控策略
     * 
     * @return 如果是简单消费者组返回true，否则返回false
     */
    public boolean isSimpleConsumerGroup() {
        return isSimpleConsumerGroup;
    }

    /**
     * 获取消费者组中的所有成员列表
     * 每个成员都是一个MemberDescription实例，包含成员的详细信息
     * 
     * 使用场景：
     * 1. 查看组内所有消费者实例
     * 2. 监控成员的状态和分配
     * 3. 诊断分区分配问题
     * 
     * @return 消费者组成员的不可变集合，永远不会为null
     */
    public Collection<MemberDescription> members() {
        return members;
    }

    /**
     * 获取消费者组使用的分区分配器名称
     * 分区分配器决定了如何将主题分区分配给组内的消费者
     * 
     * 使用场景：
     * 1. 了解当前的分区分配策略
     * 2. 诊断负载不均衡问题
     * 3. 优化分区分配
     * 
     * @return 分区分配器的名称，如果未设置则返回空字符串
     */
    public String partitionAssignor() {
        return partitionAssignor;
    }

    /**
     * 获取消费者组的类型
     * 可以是经典类型（CLASSIC）或新版本类型（CONSUMER）
     * 
     * 使用场景：
     * 1. 确定组的协议版本
     * 2. 选择合适的管理策略
     * 
     * @return 消费者组类型，如果服务器未提供则默认为CLASSIC
     */
    public GroupType type() {
        return type;
    }

    /**
     * 获取消费者组的状态（已废弃的方法）
     * 
     * @return 消费者组状态，如果状态太新无法解析则返回UNKNOWN
     * @deprecated 从4.0版本开始废弃。请使用 {@link #groupState()} 代替
     */
    @Deprecated
    public ConsumerGroupState state() {
        return ConsumerGroupState.parse(groupState.toString());
    }

    /**
     * 获取消费者组的当前状态
     * 状态表示组的运行情况，如Empty（空）、Stable（稳定）等
     * 
     * 使用场景：
     * 1. 监控组的健康状态
     * 2. 诊断组的问题
     * 3. 确定是否需要干预
     * 
     * @return 组状态，如果状态太新无法解析则返回UNKNOWN
     */
    public GroupState groupState() {
        return groupState;
    }

    /**
     * 获取负责该消费者组的协调器节点
     * 协调器负责管理组成员关系和分区分配
     * 
     * 使用场景：
     * 1. 诊断组协调问题
     * 2. 监控协调器的健康状态
     * 3. 优化网络拓扑
     * 
     * @return 协调器节点信息，如果协调器未知则返回null
     */
    public Node coordinator() {
        return coordinator;
    }

    /**
     * 获取该消费者组被授权的操作集合
     * 定义了可以对该组执行哪些操作
     * 
     * 使用场景：
     * 1. 权限检查
     * 2. 安全审计
     * 3. 访问控制管理
     * 
     * @return 授权操作集合，如果权限信息未知则返回null
     */
    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    /**
     * 获取消费者组的纪元号
     * 纪元号用于跟踪组的版本变化，仅在新版本消费者组中使用
     * 
     * 使用场景：
     * 1. 跟踪组的变更历史
     * 2. 确保操作的顺序性
     * 3. 防止脑裂问题
     * 
     * @return 组纪元号，对于CONSUMER类型组返回整数值，对于CLASSIC类型组返回空
     */
    public Optional<Integer> groupEpoch() {
        return groupEpoch;
    }

    /**
     * 获取目标分配的纪元号
     * 用于跟踪分区分配的版本，仅在新版本消费者组中使用
     * 
     * 使用场景：
     * 1. 跟踪分区分配的变更
     * 2. 确保分配操作的顺序性
     * 3. 处理并发分配问题
     * 
     * @return 目标分配纪元号，对于CONSUMER类型组返回整数值，对于CLASSIC类型组返回空
     */
    public Optional<Integer> targetAssignmentEpoch() {
        return targetAssignmentEpoch;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ", members=" + members.stream().map(MemberDescription::toString).collect(Collectors.joining(",")) +
            ", partitionAssignor=" + partitionAssignor +
            ", type=" + type +
            ", groupState=" + groupState +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations +
            ", groupEpoch=" + groupEpoch.orElse(null) +
            ", targetAssignmentEpoch=" + targetAssignmentEpoch.orElse(null) +
            ")";
    }
}
