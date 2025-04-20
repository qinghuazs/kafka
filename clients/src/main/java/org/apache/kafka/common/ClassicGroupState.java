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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Kafka经典消费者组的状态。
 * 
 * 包含以下状态：
 * - UNKNOWN: 未知状态
 * - PREPARING_REBALANCE: 准备重平衡，组成员发生变化时的初始状态
 * - COMPLETING_REBALANCE: 完成重平衡，等待成员接收分配方案
 * - STABLE: 稳定状态，所有成员都在正常工作
 * - DEAD: 已死亡状态，没有任何成员且元数据已被删除
 * - EMPTY: 空状态，没有任何活跃成员但元数据仍然保留
 */
public enum ClassicGroupState {
    // 未知状态
    UNKNOWN("Unknown"),
    // 准备进行重平衡的状态
    PREPARING_REBALANCE("PreparingRebalance"),
    // 完成重平衡等待成员确认的状态
    COMPLETING_REBALANCE("CompletingRebalance"),
    // 消费者组稳定工作的状态
    STABLE("Stable"),
    // 消费者组已死亡的状态
    DEAD("Dead"),
    // 消费者组为空的状态
    EMPTY("Empty");

    // 存储状态名称到枚举值的映射关系，用于状态名称的大小写不敏感查找
    private static final Map<String, ClassicGroupState> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toMap(state -> state.name.toUpperCase(Locale.ROOT), Function.identity()));

    // 状态的字符串表示
    private final String name;

    // 构造函数，初始化状态名称
    ClassicGroupState(String name) {
        this.name = name;
    }

    /**
     * 根据状态名称查找对应的消费者组状态，大小写不敏感。
     * 
     * @param name 状态名称
     * @return 如果找到对应状态则返回该状态，否则返回UNKNOWN
     */
    public static ClassicGroupState parse(String name) {
        ClassicGroupState state = NAME_TO_ENUM.get(name.toUpperCase(Locale.ROOT));
        return state == null ? UNKNOWN : state;
    }

    @Override
    public String toString() {
        return name;
    }
}
