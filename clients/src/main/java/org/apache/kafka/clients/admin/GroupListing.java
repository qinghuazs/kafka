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
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;
import java.util.Optional;

/**
 * 表示Kafka集群中的一个消费者组的列表项。
 * 该类提供了消费者组的基本信息，包括组ID、类型、协议和状态。
 */
@InterfaceStability.Evolving
public class GroupListing {
    // 消费者组的唯一标识符
    private final String groupId;
    
    // 消费者组的类型，使用Optional包装以处理不可用的情况
    private final Optional<GroupType> type;
    
    // 消费者组使用的协议
    private final String protocol;
    
    // 消费者组的当前状态，使用Optional包装以处理不可用的情况
    private final Optional<GroupState> groupState;

    /**
     * 使用指定参数创建GroupListing实例
     *
     * @param groupId    消费者组ID
     * @param type       消费者组类型
     * @param protocol   协议
     * @param groupState 消费者组状态
     */
    public GroupListing(String groupId, Optional<GroupType> type, String protocol, Optional<GroupState> groupState) {
        // 初始化组ID
        this.groupId = groupId;
        // 确保type不为null，否则抛出NullPointerException
        this.type = Objects.requireNonNull(type);
        // 初始化协议
        this.protocol = protocol;
        // 初始化组状态
        this.groupState = groupState;
    }

    /**
     * 获取消费者组ID
     *
     * @return 返回消费者组ID
     */
    public String groupId() {
        // 返回组ID字段
        return groupId;
    }

    /**
     * 获取消费者组类型
     * <p>
     * 如果broker返回了一个无法识别的组类型（可能是因为与更高版本的broker通信），
     * 类型将被设置为<code>Optional.of(GroupType.UNKNOWN)</code>。
     * 如果broker版本早于2.6.0，组类型将不可用，此时返回<code>Optional.empty()</code>。
     *
     * @return 返回包含组类型的Optional对象（如果可用）
     */
    public Optional<GroupType> type() {
        // 返回组类型字段
        return type;
    }

    /**
     * 获取消费者组使用的协议
     *
     * @return 返回协议名称
     */
    public String protocol() {
        // 返回协议字段
        return protocol;
    }

    /**
     * 获取消费者组状态
     * <p>
     * 如果broker返回了一个无法识别的组状态（可能是因为与更高版本的broker通信），
     * 状态将被设置为<code>Optional.of(GroupState.UNKNOWN)</code>。
     *
     * @return 返回包含组状态的Optional对象（如果可用）
     */
    public Optional<GroupState> groupState() {
        // 返回组状态字段
        return groupState;
    }

    /**
     * 判断是否为简单消费者组
     * 简单消费者组的特征是：类型为CLASSIC且协议为空
     * 
     * @return 如果是简单消费者组返回true，否则返回false
     */
    public boolean isSimpleConsumerGroup() {
        // 检查组类型是否为CLASSIC且协议是否为空
        return type.filter(gt -> gt == GroupType.CLASSIC).isPresent() && protocol.isEmpty();
    }

    @Override
    public String toString() {
        return "(" +
            "groupId='" + groupId + '\'' +
            ", type=" + type.map(GroupType::toString).orElse("none") +
            ", protocol='" + protocol + '\'' +
            ", groupState=" + groupState.map(GroupState::toString).orElse("none") +
            ')';
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, type, protocol, groupState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupListing)) return false;
        GroupListing that = (GroupListing) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(type, that.type) &&
            Objects.equals(protocol, that.protocol) &&
            Objects.equals(groupState, that.groupState);
    }
}
