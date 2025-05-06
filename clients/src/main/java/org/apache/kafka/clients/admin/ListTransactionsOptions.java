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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 用于配置{@link Admin#listTransactions()}方法的选项类。
 * 该类提供了对事务列表查询的过滤功能，包括按事务状态、生产者ID和事务持续时间进行过滤。
 * 这些过滤器可以单独使用，也可以组合使用来精确定位所需的事务。
 * 
 * 该类的API仍在演进中，详细信息请参见{@link Admin}接口的说明。
 */
@InterfaceStability.Evolving
public class ListTransactionsOptions extends AbstractOptions<ListTransactionsOptions> {
    /**
     * 用于存储要过滤的事务状态集合
     * 默认为空集合，表示不进行状态过滤，返回所有状态的事务
     */
    private Set<TransactionState> filteredStates = Collections.emptySet();

    /**
     * 用于存储要过滤的生产者ID集合
     * 默认为空集合，表示不进行生产者过滤，返回所有生产者的事务
     */
    private Set<Long> filteredProducerIds = Collections.emptySet();

    /**
     * 用于存储事务持续时间的过滤阈值（毫秒）
     * 默认值为-1，表示不进行持续时间过滤
     */
    private long filteredDuration = -1L;
    /**
     * 设置要过滤的事务状态集合。
     * 此方法用于筛选特定状态的事务，例如可以只查询正在进行中或已完成的事务。
     * 
     * 使用场景：
     * 1. 监控特定状态的事务
     * 2. 排查特定状态的事务问题
     * 3. 统计不同状态的事务数量
     *
     * @param states 要过滤的事务状态集合。如果为空或未指定，将返回所有状态的事务
     * @return 返回当前对象以支持方法链式调用
     */
    public ListTransactionsOptions filterStates(Collection<TransactionState> states) {
        // 创建一个新的HashSet来存储过滤状态，确保线程安全和不可变性
        this.filteredStates = new HashSet<>(states);
        return this;
    }

    /**
     * 设置要过滤的生产者ID集合。
     * 此方法用于筛选特定生产者的事务，可以追踪和监控指定生产者的事务活动。
     * 
     * 使用场景：
     * 1. 监控特定生产者的事务行为
     * 2. 排查特定生产者的事务问题
     * 3. 分析生产者的事务模式
     *
     * @param producerIdFilters 要过滤的生产者ID集合。如果为空或未指定，将返回所有生产者的事务
     * @return 返回当前对象以支持方法链式调用
     */
    public ListTransactionsOptions filterProducerIds(Collection<Long> producerIdFilters) {
        // 创建一个新的HashSet来存储生产者ID，确保线程安全和不可变性
        this.filteredProducerIds = new HashSet<>(producerIdFilters);
        return this;
    }

    /**
     * 设置事务持续时间的过滤阈值。
     * 此方法用于筛选运行时间超过指定持续时间的事务，有助于发现长时间运行的事务。
     * 
     * 使用场景：
     * 1. 发现可能的事务卡死或超时问题
     * 2. 监控长时间运行的事务
     * 3. 性能调优和资源管理
     *
     * @param durationMs 过滤的持续时间阈值（毫秒）。如果小于0或未指定，将返回所有事务
     * @return 返回当前对象以支持方法链式调用
     */
    public ListTransactionsOptions filterOnDuration(long durationMs) {
        // 直接设置持续时间阈值，不需要额外的防御性复制
        this.filteredDuration = durationMs;
        return this;
    }

    /**
     * 获取当前设置的事务状态过滤集合。
     * 此方法用于查询当前配置的事务状态过滤条件。
     *
     * @return 返回当前的事务状态过滤集合。如果集合为空，表示不进行状态过滤，
     *         将返回所有状态的事务
     */
    public Set<TransactionState> filteredStates() {
        return filteredStates;
    }

    /**
     * 获取当前设置的生产者ID过滤集合。
     * 此方法用于查询当前配置的生产者ID过滤条件。
     *
     * @return 返回当前的生产者ID过滤集合。如果集合为空，表示不进行生产者过滤，
     *         将返回所有生产者的事务
     */
    public Set<Long> filteredProducerIds() {
        return filteredProducerIds;
    }

    /**
     * 获取当前设置的事务持续时间过滤阈值。
     * 此方法用于查询当前配置的事务持续时间过滤条件。
     *
     * @return 返回当前的持续时间过滤阈值（毫秒）。如果值为负数，表示不进行持续时间过滤，
     *         将返回所有事务
     */
    public long filteredDuration() {
        return filteredDuration;
    }

    @Override
    public String toString() {
        return "ListTransactionsOptions(" +
            "filteredStates=" + filteredStates +
            ", filteredProducerIds=" + filteredProducerIds +
            ", filteredDuration=" + filteredDuration +
            ", timeoutMs=" + timeoutMs +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListTransactionsOptions that = (ListTransactionsOptions) o;
        return Objects.equals(filteredStates, that.filteredStates) &&
            Objects.equals(filteredProducerIds, that.filteredProducerIds) &&
            Objects.equals(filteredDuration, that.filteredDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filteredStates, filteredProducerIds, filteredDuration);
    }
}
