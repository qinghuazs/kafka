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

import java.util.Collections;
import java.util.Set;

/**
 * 用于Admin#listGroups()方法的选项类。
 * <p>
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListGroupsOptions extends AbstractOptions<ListGroupsOptions> {

    /**
     * 要查询的组状态集合
     * 默认为空集合，表示查询所有状态的组
     */
    private Set<GroupState> groupStates = Collections.emptySet();

    /**
     * 要查询的组类型集合
     * 默认为空集合，表示查询所有类型的组
     */
    private Set<GroupType> types = Collections.emptySet();

    /**
     * 设置要查询的组状态集合
     * 如果设置了groupStates，则只返回这些状态的组
     * 否则返回所有组
     * 此操作需要broker版本2.6.0或更高版本支持
     *
     * @param groupStates 要查询的组状态集合
     * @return 返回当前对象以支持方法链式调用
     */
    public ListGroupsOptions inGroupStates(Set<GroupState> groupStates) {
        // 如果参数为null或空集合，则使用空集合；否则创建参数集合的不可变副本
        this.groupStates = (groupStates == null || groupStates.isEmpty()) ? Collections.emptySet() : Set.copyOf(groupStates);
        // 返回this以支持方法链式调用
        return this;
    }

    /**
     * 设置要查询的组类型集合
     * 如果设置了types，则只返回这些类型的组
     * 否则返回所有组
     *
     * @param types 要查询的组类型集合
     * @return 返回当前对象以支持方法链式调用
     */
    public ListGroupsOptions withTypes(Set<GroupType> types) {
        // 如果参数为null或空集合，则使用空集合；否则创建参数集合的不可变副本
        this.types = (types == null || types.isEmpty()) ? Set.of() : Set.copyOf(types);
        // 返回this以支持方法链式调用
        return this;
    }

    /**
     * 获取已请求的组状态集合
     * 如果未指定任何状态，则返回空集合
     *
     * @return 返回组状态集合
     */
    public Set<GroupState> groupStates() {
        // 返回组状态集合
        return groupStates;
    }

    /**
     * 获取已请求的组类型集合
     * 如果未指定任何类型，则返回空集合
     *
     * @return 返回组类型集合
     */
    public Set<GroupType> types() {
        // 返回组类型集合
        return types;
    }
}
