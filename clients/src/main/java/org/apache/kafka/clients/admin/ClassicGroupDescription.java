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

import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 集群中单个经典消费者组的详细描述。
 * 该类用于描述Kafka中经典消费者组的所有相关信息，包括组ID、协议类型、成员信息、状态和权限等。
 * 经典消费者组是Kafka早期版本中使用的消费者组实现，与新版本的消费者组相比有一些区别。
 * 应用场景：
 * 1. 管理和监控消费者组的状态和成员信息
 * 2. 进行消费者组的权限控制
 * 3. 协调消费者组的分区分配
 */
public class ClassicGroupDescription {
    // 消费者组的唯一标识符
    private final String groupId;
    // 消费者组使用的协议类型，例如消费者协议或连接器协议
    private final String protocol;
    // 协议相关的数据，对于消费者组来说是分区分配器的名称，对于连接器组来说是启用的连接器协议
    private final String protocolData;
    // 消费者组中的所有成员列表
    private final Collection<MemberDescription> members;
    // 消费者组的当前状态（如Empty、Stable、PreparingRebalance等）
    private final ClassicGroupState state;
    // 负责协调该消费者组的节点信息
    private final Node coordinator;
    // 该消费者组被授权的操作集合
    private final Set<AclOperation> authorizedOperations;

    /**
     * 创建一个经典消费者组描述实例的简化构造函数
     * 
     * @param groupId 消费者组ID
     * @param protocol 使用的协议类型
     * @param protocolData 协议相关的数据
     * @param members 组成员列表
     * @param state 组状态
     * @param coordinator 协调器节点
     */
    public ClassicGroupDescription(String groupId,
                                   String protocol,
                                   String protocolData,
                                   Collection<MemberDescription> members,
                                   ClassicGroupState state,
                                   Node coordinator) {
        // 调用完整构造函数，使用空的权限操作集合作为默认值
        this(groupId, protocol, protocolData, members, state, coordinator, Set.of());
    }

    /**
     * 创建一个经典消费者组描述实例的完整构造函数
     * 
     * @param groupId 消费者组ID
     * @param protocol 使用的协议类型
     * @param protocolData 协议相关的数据
     * @param members 组成员列表
     * @param state 组状态
     * @param coordinator 协调器节点
     * @param authorizedOperations 授权操作集合
     */
    public ClassicGroupDescription(String groupId,
                                   String protocol,
                                   String protocolData,
                                   Collection<MemberDescription> members,
                                   ClassicGroupState state,
                                   Node coordinator,
                                   Set<AclOperation> authorizedOperations) {
        // 如果groupId为null则使用空字符串，确保groupId不为null
        this.groupId = groupId == null ? "" : groupId;
        // protocol可以为null，表示简单消费者组
        this.protocol = protocol;
        // 如果protocolData为null则使用空字符串
        this.protocolData = protocolData == null ? "" : protocolData;
        // 如果members为null则使用空列表，并创建不可变副本
        this.members = members == null ? List.of() : List.copyOf(members);
        // 设置组状态
        this.state = state;
        // 设置协调器节点
        this.coordinator = coordinator;
        // 设置授权操作集合
        this.authorizedOperations = authorizedOperations;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ClassicGroupDescription that = (ClassicGroupDescription) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(protocol, that.protocol) &&
            Objects.equals(protocolData, that.protocolData) &&
            Objects.equals(members, that.members) &&
            state == that.state &&
            Objects.equals(coordinator, that.coordinator) &&
            Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, protocol, protocolData, members, state, coordinator, authorizedOperations);
    }

    /**
     * 获取经典消费者组的ID
     * 组ID用于唯一标识一个消费者组，在整个Kafka集群中必须是唯一的
     * 
     * @return 消费者组ID
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 获取消费者组使用的协议类型
     * 协议类型标识了组成员之间如何协调和通信
     * 对于消费者组来说，通常使用消费者协议
     * 对于连接器组来说，使用连接器协议
     * 
     * @return 协议类型
     */
    public String protocol() {
        return protocol;
    }

    /**
     * 获取协议相关的数据
     * 数据的具体含义取决于协议类型：
     * - 对于经典消费者组，返回分区分配器的名称（如RangeAssignor、RoundRobinAssignor等）
     * - 对于经典连接器组，返回已启用的连接器协议信息
     * 
     * @return 协议相关的数据
     */
    public String protocolData() {
        return protocolData;
    }

    /**
     * 判断是否为简单消费者组
     * 简单消费者组是一种特殊的消费者组类型，它不使用组协调协议
     * 当protocol为空时表示这是一个简单消费者组
     * 
     * @return 如果是简单消费者组返回true，否则返回false
     */
    public boolean isSimpleConsumerGroup() {
        return protocol.isEmpty();
    }

    /**
     * 获取消费者组中的所有成员列表
     * 每个成员都是一个MemberDescription实例，包含了成员的详细信息
     * 如成员ID、客户端ID、主机信息等
     * 
     * @return 消费者组成员集合
     */
    public Collection<MemberDescription> members() {
        return members;
    }

    /**
     * 获取消费者组的当前状态
     * 可能的状态包括：
     * - Empty：组内没有任何成员
     * - Stable：组稳定运行中
     * - PreparingRebalance：正在准备重平衡
     * - CompletingRebalance：正在完成重平衡
     * - Dead：组已死亡
     * 如果状态太新无法解析，则返回UNKNOWN
     * 
     * @return 消费者组状态
     */
    public ClassicGroupState state() {
        return state;
    }

    /**
     * 获取负责协调该消费者组的节点信息
     * 协调器节点负责管理消费者组的成员关系、处理成员加入和离开、
     * 触发分区重平衡等操作
     * 
     * @return 协调器节点信息，如果协调器未知则返回null
     */
    public Node coordinator() {
        return coordinator;
    }

    /**
     * 获取该消费者组被授权的操作集合
     * 这些操作定义了对该消费者组可以执行的权限控制，
     * 如读取消费者组信息、删除消费者组等
     * 
     * @return 授权操作集合，如果权限信息未知则返回null
     */
    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", protocol='" + protocol + '\'' +
            ", protocolData=" + protocolData +
            ", members=" + members.stream().map(MemberDescription::toString).collect(Collectors.joining(",")) +
            ", state=" + state +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations +
            ")";
    }
}
