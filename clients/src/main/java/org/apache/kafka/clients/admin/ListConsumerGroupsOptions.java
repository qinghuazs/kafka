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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 用于Admin#listConsumerGroups()方法的选项类。
 * 
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsOptions extends AbstractOptions<ListConsumerGroupsOptions> {

    /**
     * 要查询的消费者组状态集合
     * 默认为空集合，表示查询所有状态的消费者组
     */
    private Set<GroupState> groupStates = Collections.emptySet();

    /**
     * 要查询的消费者组类型集合
     * 默认为空集合，表示查询所有类型的消费者组
     */
    private Set<GroupType> types = Collections.emptySet();

    /**
     * 设置要查询的消费者组状态集合
     * 如果设置了groupStates，则只返回这些状态的消费者组
     * 否则返回所有消费者组
     * 此操作需要broker版本2.6.0或更高版本支持
     *
     * @param groupStates 要查询的消费者组状态集合
     * @return 返回当前对象以支持方法链式调用
     */
    public ListConsumerGroupsOptions inGroupStates(Set<GroupState> groupStates) {
        // 如果参数为null或空集合，则使用空集合；否则创建参数集合的不可变副本
        this.groupStates = (groupStates == null || groupStates.isEmpty()) ? Collections.emptySet() : Set.copyOf(groupStates);
        return this;
    }

    /**
     * 设置要查询的消费者组状态集合（已废弃的方法）
     * 如果设置了states，则只返回这些状态的消费者组
     * 否则返回所有消费者组
     * 此操作需要broker版本2.6.0或更高版本支持
     *
     * @param states 要查询的消费者组状态集合
     * @return 返回当前对象以支持方法链式调用
     * @deprecated 从4.0版本开始废弃。请使用{@link #inGroupStates(Set)}替代
     */
    @Deprecated
    public ListConsumerGroupsOptions inStates(Set<ConsumerGroupState> states) {
        // 如果参数为null或空集合，则使用空集合
        // 否则将ConsumerGroupState转换为GroupState并收集为新的集合
        this.groupStates = (states == null || states.isEmpty())
            ? Collections.emptySet()
            : states.stream().map(state -> GroupState.parse(state.toString())).collect(Collectors.toSet());
        return this;
    }

    /**
     * 设置要查询的消费者组类型集合
     * 如果设置了types，则只返回这些类型的消费者组
     * 否则返回所有消费者组
     *
     * @param types 要查询的消费者组类型集合
     * @return 返回当前对象以支持方法链式调用
     */
    public ListConsumerGroupsOptions withTypes(Set<GroupType> types) {
        // 如果参数为null或空集合，则使用空集合；否则创建参数集合的可变副本
        this.types = (types == null || types.isEmpty()) ? Collections.emptySet() : new HashSet<>(types);
        return this;
    }

    /**
     * 获取已请求的消费者组状态集合
     * 如果未指定任何状态，则返回空集合
     *
     * @return 返回消费者组状态集合
     */
    public Set<GroupState> groupStates() {
        return groupStates;
    }

    /**
     * 获取已请求的消费者组状态集合（已废弃的方法）
     * 如果未指定任何状态，则返回空集合
     *
     * @return 返回消费者组状态集合
     * @deprecated 从4.0版本开始废弃。请使用{@link #groupStates()}替代
     */
    @Deprecated
    public Set<ConsumerGroupState> states() {
        // 将GroupState转换回ConsumerGroupState并收集为新的集合
        return groupStates.stream().map(groupState -> ConsumerGroupState.parse(groupState.toString())).collect(Collectors.toSet());
    }

    /**
     * 获取已请求的消费者组类型集合
     * 如果未指定任何类型，则返回空集合
     *
     * @return 返回消费者组类型集合
     */
    public Set<GroupType> types() {
        return types;
    }
}
