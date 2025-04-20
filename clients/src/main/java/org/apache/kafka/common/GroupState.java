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

package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 消费者组状态。
 * <p>
 * 下表展示了不同组类型对应的状态：
 * <table>
 *     <thead>
 *         <tr><th>状态</th><th>经典消费者组</th><th>新版消费者组</th><th>共享消费者组</th></tr>
 *     </thead>
 *     <tbody>
 *         <tr><td>UNKNOWN</td><td>是</td><td>是</td><td>是</td></tr>
 *         <tr><td>PREPARING_REBALANCE</td><td>是</td><td>是</td><td></td></tr>
 *         <tr><td>COMPLETING_REBALANCE</td><td>是</td><td>是</td><td></td></tr>
 *         <tr><td>STABLE</td><td>是</td><td>是</td><td>是</td></tr>
 *         <tr><td>DEAD</td><td>是</td><td>是</td><td>是</td></tr>
 *         <tr><td>EMPTY</td><td>是</td><td>是</td><td>是</td></tr>
 *         <tr><td>ASSIGNING</td><td></td><td>是</td><td></td></tr>
 *         <tr><td>RECONCILING</td><td></td><td>是</td><td></td></tr>
 *     </tbody>
 * </table>
 */
@InterfaceStability.Evolving
public enum GroupState {
    // 未知状态
    UNKNOWN("Unknown"),
    // 准备重平衡状态 - 组成员变化时进入此状态
    PREPARING_REBALANCE("PreparingRebalance"),
    // 完成重平衡状态 - 等待所有成员加入组
    COMPLETING_REBALANCE("CompletingRebalance"),
    // 稳定状态 - 所有成员都已加入且分区分配完成
    STABLE("Stable"),
    // 死亡状态 - 组已不可用
    DEAD("Dead"),
    // 空状态 - 组中没有任何成员
    EMPTY("Empty"),
    // 分配状态 - 新版消费者组专用，正在进行分区分配
    ASSIGNING("Assigning"),
    // 协调状态 - 新版消费者组专用，正在协调组成员状态
    RECONCILING("Reconciling");

    // 状态名称到枚举值的映射，用于字符串解析
    private static final Map<String, GroupState> NAME_TO_ENUM = Arrays.stream(values())
            .collect(Collectors.toMap(state -> state.name.toUpperCase(Locale.ROOT), Function.identity()));

    // 状态的显示名称
    private final String name;

    /**
     * 构造函数
     * @param name 状态的显示名称
     */
    GroupState(String name) {
        this.name = name;
    }

    /**
     * 将字符串解析为GroupState枚举值(不区分大小写)
     * @param name 状态名称
     * @return 对应的GroupState枚举值，如果未找到则返回UNKNOWN
     */
    public static GroupState parse(String name) {
        GroupState state = NAME_TO_ENUM.get(name.toUpperCase(Locale.ROOT));
        return state == null ? UNKNOWN : state;
    }

    /**
     * 获取指定组类型支持的所有状态
     * @param type 组类型
     * @return 该类型支持的状态集合
     * @throws IllegalArgumentException 如果组类型未知
     */
    public static Set<GroupState> groupStatesForType(GroupType type) {
        if (type == GroupType.CLASSIC) {
            return Set.of(PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD, EMPTY);
        } else if (type == GroupType.CONSUMER) {
            return Set.of(PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD, EMPTY, ASSIGNING, RECONCILING);
        } else if (type == GroupType.SHARE) {
            return Set.of(STABLE, DEAD, EMPTY);
        } else {
            throw new IllegalArgumentException("组类型未知");
        }
    }

    /**
     * 返回状态的字符串表示
     */
    @Override
    public String toString() {
        return name;
    }
}
