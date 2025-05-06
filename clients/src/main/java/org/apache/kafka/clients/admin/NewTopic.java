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

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.requests.CreateTopicsRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

/**
 * 通过 {@link Admin#createTopics(Collection)} 创建新的主题。
 * 这个类用于定义新主题的配置，包括分区数、副本因子、副本分配方案和主题级别配置。
 * 应用场景：
 * 1. 创建具有指定分区数和副本因子的主题
 * 2. 创建使用默认分区数和副本因子的主题
 * 3. 创建具有自定义副本分配方案的主题
 * 4. 创建带有特定配置参数的主题
 */
public class NewTopic {

    // 主题名称
    private final String name;
    // 分区数，如果为空则使用broker默认配置
    private final Optional<Integer> numPartitions;
    // 副本因子，如果为空则使用broker默认配置
    private final Optional<Short> replicationFactor;
    // 自定义副本分配方案，key为分区ID，value为该分区的副本所在broker列表
    private final Map<Integer, List<Integer>> replicasAssignments;
    // 主题级别配置参数
    private Map<String, String> configs = null;

    /**
     * 创建具有指定分区数和副本因子的新主题。
     * 
     * @param name 主题名称
     * @param numPartitions 分区数
     * @param replicationFactor 副本因子
     */
    public NewTopic(String name, int numPartitions, short replicationFactor) {
        // 将参数转换为Optional类型并调用另一个构造函数
        this(name, Optional.of(numPartitions), Optional.of(replicationFactor));
    }

    /**
     * 创建新主题，可选择使用broker默认的分区数和副本因子配置。
     * 如果numPartitions为空，将使用broker的num.partitions配置
     * 如果replicationFactor为空，将使用broker的default.replication.factor配置
     * 
     * @param name 主题名称
     * @param numPartitions 分区数（可选）
     * @param replicationFactor 副本因子（可选）
     */
    public NewTopic(String name, Optional<Integer> numPartitions, Optional<Short> replicationFactor) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        // 使用默认分区和副本配置时，不需要自定义副本分配
        this.replicasAssignments = null;
    }

    /**
     * 创建具有自定义副本分配方案的新主题。
     * 这种方式允许用户精确控制每个分区的副本分布。
     *
     * @param name 主题名称
     * @param replicasAssignments 副本分配方案，key为分区ID，value为副本所在的broker ID列表
     *                           建议所有分区的副本数量保持一致
     *                           列表中的第一个副本将被视为首选leader
     */
    public NewTopic(String name, Map<Integer, List<Integer>> replicasAssignments) {
        this.name = name;
        // 使用自定义副本分配时，不需要指定分区数和副本因子
        this.numPartitions = Optional.empty();
        this.replicationFactor = Optional.empty();
        // 创建不可修改的副本分配方案Map
        this.replicasAssignments = Collections.unmodifiableMap(replicasAssignments);
    }

    /**
     * 获取主题名称。
     *
     * @return 主题名称
     */
    public String name() {
        return name;
    }

    /**
     * 获取主题的分区数。
     * 如果使用了自定义副本分配，则返回-1
     *
     * @return 分区数，或者在使用自定义副本分配时返回-1
     */
    public int numPartitions() {
        return numPartitions.orElse(CreateTopicsRequest.NO_NUM_PARTITIONS);
    }

    /**
     * 获取主题的副本因子。
     * 如果使用了自定义副本分配，则返回-1
     *
     * @return 副本因子，或者在使用自定义副本分配时返回-1
     */
    public short replicationFactor() {
        return replicationFactor.orElse(CreateTopicsRequest.NO_REPLICATION_FACTOR);
    }

    /**
     * 获取自定义副本分配方案。
     * 如果使用了分区数和副本因子来创建主题，则返回null
     *
     * @return 副本分配方案Map，或者在使用分区数和副本因子时返回null
     */
    public Map<Integer, List<Integer>> replicasAssignments() {
        return replicasAssignments;
    }

    /**
     * 设置主题级别的配置参数。
     * 用于配置主题特定的参数，如清理策略、消息保留时间等
     *
     * @param configs 配置参数Map，key为配置名，value为配置值
     * @return 当前NewTopic对象（支持链式调用）
     */
    public NewTopic configs(Map<String, String> configs) {
        this.configs = configs;
        return this;
    }

    /**
     * 获取主题的配置参数。
     *
     * @return 配置参数Map，如果未设置则返回null
     */
    public Map<String, String> configs() {
        return configs;
    }

    /**
     * 将NewTopic对象转换为CreatableTopic对象。
     * 该方法用于内部创建主题请求的处理。
     *
     * @return 用于创建主题请求的CreatableTopic对象
     */
    CreatableTopic convertToCreatableTopic() {
        // 创建CreatableTopic对象并设置基本属性
        CreatableTopic creatableTopic = new CreatableTopic().
            setName(name).
            setNumPartitions(numPartitions.orElse(CreateTopicsRequest.NO_NUM_PARTITIONS)).
            setReplicationFactor(replicationFactor.orElse(CreateTopicsRequest.NO_REPLICATION_FACTOR));
        
        // 如果存在自定义副本分配，添加到CreatableTopic中
        if (replicasAssignments != null) {
            for (Entry<Integer, List<Integer>> entry : replicasAssignments.entrySet()) {
                creatableTopic.assignments().add(
                    new CreatableReplicaAssignment().
                        setPartitionIndex(entry.getKey()).
                        setBrokerIds(entry.getValue()));
            }
        }
        
        // 如果存在配置参数，添加到CreatableTopic中
        if (configs != null) {
            for (Entry<String, String> entry : configs.entrySet()) {
                creatableTopic.configs().add(
                    new CreatableTopicConfig().
                        setName(entry.getKey()).
                        setValue(entry.getValue()));
            }
        }
        return creatableTopic;
    }

    @Override
    public String toString() {
        return "(name=" + name +
                ", numPartitions=" + numPartitions.map(String::valueOf).orElse("default") +
                ", replicationFactor=" + replicationFactor.map(String::valueOf).orElse("default") +
                ", replicasAssignments=" + replicasAssignments +
                ", configs=" + configs +
                ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final NewTopic that = (NewTopic) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(numPartitions, that.numPartitions) &&
            Objects.equals(replicationFactor, that.replicationFactor) &&
            Objects.equals(replicasAssignments, that.replicasAssignments) &&
            Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, numPartitions, replicationFactor, replicasAssignments, configs);
    }
}
