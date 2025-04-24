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
package org.apache.kafka.clients.consumer.internals;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class holds the data that is needed to participate in the Streams rebalance protocol.
 * 该类持有参与Streams重平衡协议所需的数据。
 */
public class StreamsRebalanceData {

    /**
     * 任务ID类，用于唯一标识Kafka Streams中的任务。
     * 每个任务ID由子拓扑ID(subtopologyId)和分区ID(partitionId)组成，
     * 实现了Comparable接口以支持任务的排序。
     */
    public static class TaskId implements Comparable<TaskId> {

        // 子拓扑ID，用于标识任务所属的子拓扑
        private final String subtopologyId;
        // 分区ID，用于标识任务处理的具体分区
        private final int partitionId;

        /**
         * 创建TaskId实例
         * @param subtopologyId 子拓扑ID，不能为null
         * @param partitionId 分区ID
         */
        public TaskId(final String subtopologyId, final int partitionId) {
            // 确保subtopologyId不为null，这是任务标识的必要组成部分
            this.subtopologyId = Objects.requireNonNull(subtopologyId, "Subtopology ID cannot be null");
            this.partitionId = partitionId;
        }

        public int partitionId() {
            return partitionId;
        }

        public String subtopologyId() {
            return subtopologyId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskId taskId = (TaskId) o;
            return partitionId == taskId.partitionId && Objects.equals(subtopologyId, taskId.subtopologyId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtopologyId, partitionId);
        }

        @Override
        public int compareTo(TaskId taskId) {
            Objects.requireNonNull(taskId, "taskId cannot be null");
            return Comparator.comparing(TaskId::subtopologyId)
                .thenComparingInt(TaskId::partitionId).compare(this, taskId);
        }

        @Override
        public String toString() {
            return "TaskId{" +
                "subtopologyId=" + subtopologyId +
                ", partitionId=" + partitionId +
                '}';
        }
    }

    /**
     * 任务分配类，用于管理Kafka Streams中不同类型的任务分配。
     * 包含三种类型的任务：
     * 1. 活跃任务(activeTasks)：直接处理数据的主任务
     * 2. 备用任务(standbyTasks)：用于故障转移的备份任务
     * 3. 预热任务(warmupTasks)：正在准备成为活跃任务的任务
     */
    public static class Assignment {

        // 空任务分配的常量实例，用于初始化和重置
        public static final Assignment EMPTY = new Assignment();

        // 活跃任务集合，这些任务直接处理输入数据
        private final Set<TaskId> activeTasks;

        // 备用任务集合，这些任务维护状态副本以支持快速故障恢复
        private final Set<TaskId> standbyTasks;

        // 预热任务集合，这些任务正在准备接管处理职责
        private final Set<TaskId> warmupTasks;

        /**
         * 私有构造函数，创建一个空的任务分配实例
         * 所有任务集合都被初始化为空集合
         */
        private Assignment() {
            this.activeTasks = Set.of();
            this.standbyTasks = Set.of();
            this.warmupTasks = Set.of();
        }

        public Assignment(final Set<TaskId> activeTasks,
                          final Set<TaskId> standbyTasks,
                          final Set<TaskId> warmupTasks) {
            this.activeTasks = Set.copyOf(Objects.requireNonNull(activeTasks, "Active tasks cannot be null"));
            this.standbyTasks = Set.copyOf(Objects.requireNonNull(standbyTasks, "Standby tasks cannot be null"));
            this.warmupTasks = Set.copyOf(Objects.requireNonNull(warmupTasks, "Warmup tasks cannot be null"));
        }

        public Set<TaskId> activeTasks() {
            return activeTasks;
        }

        public Set<TaskId> standbyTasks() {
            return standbyTasks;
        }

        public Set<TaskId> warmupTasks() {
            return warmupTasks;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Assignment that = (Assignment) o;
            return Objects.equals(activeTasks, that.activeTasks)
                && Objects.equals(standbyTasks, that.standbyTasks)
                && Objects.equals(warmupTasks, that.warmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(activeTasks, standbyTasks, warmupTasks);
        }

        public Assignment copy() {
            return new Assignment(activeTasks, standbyTasks, warmupTasks);
        }

        @Override
        public String toString() {
            return "Assignment{" +
                "activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                ", warmupTasks=" + warmupTasks +
                '}';
        }
    }

    /**
     * 子拓扑类，描述了Kafka Streams处理器拓扑中的一个子图。
     * 包含了处理器拓扑中的各种主题类型和它们之间的关系：
     * - 源主题：数据的输入来源
     * - 重分区接收主题：用于数据重分区的目标主题
     * - 状态变更日志主题：用于记录状态存储的变更
     * - 重分区源主题：重分区后的数据源主题
     * - 协同分区组：需要保持相同分区策略的主题组
     */
    public static class Subtopology {

        // 源主题集合，表示数据流的输入来源
        private final Set<String> sourceTopics;
        // 重分区接收主题集合，用于数据重新分区的目标主题
        private final Set<String> repartitionSinkTopics;
        // 状态变更日志主题映射，记录状态存储的变更信息
        private final Map<String, TopicInfo> stateChangelogTopics;
        // 重分区源主题映射，存储重分区后的数据源信息
        private final Map<String, TopicInfo> repartitionSourceTopics;
        // 协同分区组集合，确保组内主题具有相同的分区策略
        private final Collection<Set<String>> copartitionGroups;

        /**
         * 创建子拓扑实例
         * @param sourceTopics 源主题集合
         * @param repartitionSinkTopics 重分区接收主题集合
         * @param repartitionSourceTopics 重分区源主题信息映射
         * @param stateChangelogTopics 状态变更日志主题信息映射
         * @param copartitionGroups 协同分区组集合
         */
        public Subtopology(final Set<String> sourceTopics,
                           final Set<String> repartitionSinkTopics,
                           final Map<String, TopicInfo> repartitionSourceTopics,
                           final Map<String, TopicInfo> stateChangelogTopics,
                           final Collection<Set<String>> copartitionGroups
        ) {
            this.sourceTopics = Set.copyOf(Objects.requireNonNull(sourceTopics, "Subtopology ID cannot be null"));
            this.repartitionSinkTopics =
                Set.copyOf(Objects.requireNonNull(repartitionSinkTopics, "Repartition sink topics cannot be null"));
            this.repartitionSourceTopics =
                Map.copyOf(Objects.requireNonNull(repartitionSourceTopics, "Repartition source topics cannot be null"));
            this.stateChangelogTopics =
                Map.copyOf(Objects.requireNonNull(stateChangelogTopics, "State changelog topics cannot be null"));
            this.copartitionGroups =
                Collections.unmodifiableCollection(Objects.requireNonNull(
                    copartitionGroups,
                    "Co-partition groups cannot be null"
                    )
                );
        }

        public Set<String> sourceTopics() {
            return sourceTopics;
        }

        public Set<String> repartitionSinkTopics() {
            return repartitionSinkTopics;
        }

        public Map<String, TopicInfo> stateChangelogTopics() {
            return stateChangelogTopics;
        }

        public Map<String, TopicInfo> repartitionSourceTopics() {
            return repartitionSourceTopics;
        }

        public Collection<Set<String>> copartitionGroups() {
            return copartitionGroups;
        }

        @Override
        public String toString() {
            return "Subtopology{" +
                "sourceTopics=" + sourceTopics +
                ", repartitionSinkTopics=" + repartitionSinkTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", copartitionGroups=" + copartitionGroups +
                '}';
        }
    }

    /**
     * 主题信息类，包含了Kafka主题的关键配置信息：
     * - 分区数：主题的分区数量
     * - 副本因子：主题的副本数量
     * - 主题配置：其他特定的主题级别配置
     */
    public static class TopicInfo {

        // 主题分区数，可选值
        private final Optional<Integer> numPartitions;
        // 主题副本因子，可选值
        private final Optional<Short> replicationFactor;
        // 主题特定配置映射
        private final Map<String, String> topicConfigs;

        /**
         * 创建主题信息实例
         * @param numPartitions 分区数（可选）
         * @param replicationFactor 副本因子（可选）
         * @param topicConfigs 主题配置映射
         */
        public TopicInfo(final Optional<Integer> numPartitions,
                         final Optional<Short> replicationFactor,
                         final Map<String, String> topicConfigs) {
            this.numPartitions = Objects.requireNonNull(numPartitions, "Number of partitions cannot be null");
            this.replicationFactor = Objects.requireNonNull(replicationFactor, "Replication factor cannot be null");
            this.topicConfigs =
                Map.copyOf(Objects.requireNonNull(topicConfigs, "Additional topic configs cannot be null"));
        }

        public Optional<Integer> numPartitions() {
            return numPartitions;
        }

        public Optional<Short> replicationFactor() {
            return replicationFactor;
        }

        public Map<String, String> topicConfigs() {
            return topicConfigs;
        }

        @Override
        public String toString() {
            return "TopicInfo{" +
                "numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", topicConfigs=" + topicConfigs +
                '}';
        }
    }

    // 子拓扑映射，存储所有子拓扑的信息，key为子拓扑ID
    private final Map<String, Subtopology> subtopologies;

    // 已协调的任务分配，使用AtomicReference保证线程安全
    // 初始值为空分配(EMPTY)，表示还未进行任务分配
    private final AtomicReference<Assignment> reconciledAssignment = new AtomicReference<>(Assignment.EMPTY);

    /**
     * 创建StreamsRebalanceData实例
     * @param subtopologies 子拓扑映射，包含所有子拓扑的信息
     */
    public StreamsRebalanceData(Map<String, Subtopology> subtopologies) {
        // 创建子拓扑映射的不可变副本，确保线程安全
        this.subtopologies = Map.copyOf(Objects.requireNonNull(subtopologies, "Subtopologies cannot be null"));
    }

    /**
     * 获取所有子拓扑信息
     * @return 子拓扑ID到子拓扑对象的映射
     */
    public Map<String, Subtopology> subtopologies() {
        return subtopologies;
    }

    /**
     * 设置已协调的任务分配
     * 在重平衡协议完成后，更新最终的任务分配结果
     * @param assignment 新的任务分配
     */
    public void setReconciledAssignment(final Assignment assignment) {
        reconciledAssignment.set(assignment);
    }

    /**
     * 获取当前已协调的任务分配
     * @return 当前的任务分配，如果还未分配则返回EMPTY
     */
    public Assignment reconciledAssignment() {
        return reconciledAssignment.get();
    }
}
