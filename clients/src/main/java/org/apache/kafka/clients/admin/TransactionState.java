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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Kafka事务状态枚举类，用于表示事务的不同生命周期状态。
 * 事务状态转换图：
 * 
 * +----------------+     +------------------+     +------------------+
 * |    ONGOING     | --> | PREPARE_COMMIT   | --> | COMPLETE_COMMIT  |
 * +----------------+     +------------------+     +------------------+
 *         |
 *         |             +------------------+     +------------------+
 *         +-----------> | PREPARE_ABORT    | --> | COMPLETE_ABORT   |
 *                      +------------------+     +------------------+
 *
 * 其他状态：
 * - EMPTY: 事务初始状态
 * - PREPARE_EPOCH_FENCE: 准备进行epoch隔离
 * - UNKNOWN: 未知状态
 */
@InterfaceStability.Evolving
public enum TransactionState {
    /**
     * 事务正在进行中的状态
     * 此状态表示生产者已经开始了一个事务，正在写入消息，但尚未提交或中止
     */
    ONGOING("Ongoing"),

    /**
     * 事务准备中止的状态
     * 生产者调用abortTransaction()后，事务协调者将状态设置为此状态
     * 表示事务即将被中止，但尚未完成中止操作
     */
    PREPARE_ABORT("PrepareAbort"),

    /**
     * 事务准备提交的状态
     * 生产者调用commitTransaction()后，事务协调者将状态设置为此状态
     * 表示事务即将被提交，但尚未完成提交操作
     */
    PREPARE_COMMIT("PrepareCommit"),

    /**
     * 事务完成中止的状态
     * 表示事务已经成功中止，所有相关的数据都已被回滚
     */
    COMPLETE_ABORT("CompleteAbort"),

    /**
     * 事务完成提交的状态
     * 表示事务已经成功提交，所有相关的数据修改都已持久化
     */
    COMPLETE_COMMIT("CompleteCommit"),

    /**
     * 事务的初始状态
     * 表示事务尚未开始或已被清理
     */
    EMPTY("Empty"),

    /**
     * 准备进行epoch隔离的状态
     * 用于处理生产者故障恢复场景，防止僵尸生产者的消息写入
     */
    PREPARE_EPOCH_FENCE("PrepareEpochFence"),

    /**
     * 未知状态
     * 当无法确定事务的具体状态时使用此状态
     */
    UNKNOWN("Unknown");

    /**
     * 状态名称到枚举值的映射
     * 用于快速查找和状态转换，提高性能
     */
    private static final Map<String, TransactionState> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toMap(state -> state.name, Function.identity()));

    /**
     * 状态的字符串表示
     */
    private final String name;

    /**
     * 构造函数
     * @param name 状态的字符串表示
     */
    TransactionState(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * 将字符串解析为对应的事务状态
     * @param name 状态的字符串表示
     * @return 对应的TransactionState枚举值，如果找不到对应的状态则返回UNKNOWN
     */
    public static TransactionState parse(String name) {
        return NAME_TO_ENUM.getOrDefault(name, UNKNOWN);
    }

}
