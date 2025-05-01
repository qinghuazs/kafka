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

import java.util.Objects;
import java.util.Optional;

/**
 * 表示Kafka集群中消费者组的基本信息列表项。
 * 
 * 应用场景：
 * 1. 用于管理和监控Kafka集群中的消费者组
 * 2. 提供消费者组的基本信息，如组ID、状态、类型等
 * 3. 支持区分简单消费者组和常规消费者组
 * 4. 用于消费者组列表查询操作的返回结果
 */
public class ConsumerGroupListing {
    /**
     * 消费者组的唯一标识符
     * 在整个Kafka集群中必须是唯一的
     */
    private final String groupId;

    /**
     * 标识是否为简单消费者组
     * 简单消费者组不使用Kafka的组管理功能，没有组协调器和成员管理
     */
    private final boolean isSimpleConsumerGroup;

    /**
     * 消费者组的当前状态
     * 可选值包括：Empty（空组）、Stable（稳定）、PreparingRebalance（准备再平衡）等
     */
    private final Optional<GroupState> groupState;

    /**
     * 消费者组的类型
     * 可选值包括：CLASSIC（经典类型）、CONSUMER（新版本类型）等
     */
    private final Optional<GroupType> type;

    /**
     * 创建一个基本的消费者组列表项实例
     * 这是最简单的构造函数，只需要组ID和简单消费者组标识
     * 组状态和类型都将被设置为空（Optional.empty()）
     *
     * @param groupId                   消费者组ID
     * @param isSimpleConsumerGroup     是否为简单消费者组
     */
    public ConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup) {
        this(groupId, Optional.empty(), Optional.empty(), isSimpleConsumerGroup);
    }

    /**
     * 创建一个包含状态信息的消费者组列表项实例
     * 该构造函数已废弃，建议使用新的构造函数
     *
     * @param groupId                   消费者组ID
     * @param isSimpleConsumerGroup     是否为简单消费者组
     * @param state                     消费者组状态
     * @deprecated 从4.0版本开始废弃。请使用 {@link #ConsumerGroupListing(String, Optional, boolean)} 代替
     */
    @Deprecated
    public ConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup, Optional<ConsumerGroupState> state) {
        this(groupId, Objects.requireNonNull(state).map(state0 -> GroupState.parse(state0.toString())), Optional.empty(), isSimpleConsumerGroup);
    }

    /**
     * 创建一个包含完整信息的消费者组列表项实例
     * 该构造函数已废弃，建议使用新的构造函数
     *
     * @param groupId                   消费者组ID
     * @param isSimpleConsumerGroup     是否为简单消费者组
     * @param state                     消费者组状态
     * @param type                      消费者组类型
     * @deprecated 从4.0版本开始废弃。请使用 {@link #ConsumerGroupListing(String, Optional, Optional, boolean)} 代替
     */
    @Deprecated
    public ConsumerGroupListing(
        String groupId,
        boolean isSimpleConsumerGroup,
        Optional<ConsumerGroupState> state,
        Optional<GroupType> type
    ) {
        this(groupId, Objects.requireNonNull(state).map(state0 -> GroupState.parse(state0.toString())), type, isSimpleConsumerGroup);
    }

    /**
     * 创建一个新版本的消费者组列表项实例
     * 包含组ID、状态和简单消费者组标识，但不包含类型信息
     *
     * @param groupId                   消费者组ID
     * @param groupState                消费者组状态
     * @param isSimpleConsumerGroup     是否为简单消费者组
     */
    public ConsumerGroupListing(
            String groupId,
            Optional<GroupState> groupState,
            boolean isSimpleConsumerGroup
    ) {
        this(groupId, groupState, Optional.empty(), isSimpleConsumerGroup);
    }

    /**
     * 创建一个完整的消费者组列表项实例
     * 这是推荐使用的构造函数，支持所有可用的属性
     *
     * @param groupId                   消费者组ID
     * @param groupState                消费者组状态
     * @param type                      消费者组类型
     * @param isSimpleConsumerGroup     是否为简单消费者组
     */
    public ConsumerGroupListing(
            String groupId,
            Optional<GroupState> groupState,
            Optional<GroupType> type,
            boolean isSimpleConsumerGroup
    ) {
        this.groupId = groupId;
        this.groupState = Objects.requireNonNull(groupState);
        this.type = Objects.requireNonNull(type);
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    }

    /**
     * 获取消费者组ID
     * 返回此消费者组的唯一标识符
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 判断是否为简单消费者组
     * 简单消费者组不参与组协调和成员管理
     */
    public boolean isSimpleConsumerGroup() {
        return isSimpleConsumerGroup;
    }

    /**
     * 获取消费者组状态
     * 返回当前消费者组的运行状态，如Empty、Stable等
     */
    public Optional<GroupState> groupState() {
        return groupState;
    }

    /**
     * 获取消费者组状态（已废弃的方法）
     * 将新的GroupState转换为旧的ConsumerGroupState
     * @deprecated 从4.0版本开始废弃。请使用 {@link #groupState()} 代替
     */
    @Deprecated
    public Optional<ConsumerGroupState> state() {
        return groupState.map(state0 -> ConsumerGroupState.parse(state0.toString()));
    }

    /**
     * 获取消费者组类型
     * 返回此消费者组的类型信息，如CLASSIC或CONSUMER
     *
     * @return 包含组类型的Optional对象，如果类型不可用则为empty
     */
    public Optional<GroupType> type() {
        return type;
    }

    @Override
    public String toString() {
        return "(" +
            "groupId='" + groupId + '\'' +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ", groupState=" + groupState +
            ", type=" + type +
            ')';
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, isSimpleConsumerGroup(), groupState, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerGroupListing)) return false;
        ConsumerGroupListing that = (ConsumerGroupListing) o;
        return isSimpleConsumerGroup() == that.isSimpleConsumerGroup() &&
            Objects.equals(groupId, that.groupId) &&
            Objects.equals(groupState, that.groupState) &&
            Objects.equals(type, that.type);
    }
}
