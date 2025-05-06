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

import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kafka集群中单个主题的详细描述。
 * 该类用于表示Kafka主题的元数据信息，包括主题名称、分区信息、副本分配、
 * 授权操作等。它在Kafka管理工具和客户端中被广泛使用，用于主题的创建、
 * 修改和监控等操作。
 */
public class TopicDescription {
    /**
     * 主题名称
     * 在Kafka集群中唯一标识一个主题
     */
    private final String name;

    /**
     * 是否为Kafka内部主题的标志
     * 内部主题（如__consumer_offsets）用于存储Kafka自身的元数据信息
     */
    private final boolean internal;

    /**
     * 主题的分区信息列表
     * 包含每个分区的详细信息，如分区ID、leader副本、ISR集合等
     */
    private final List<TopicPartitionInfo> partitions;

    /**
     * 该主题允许执行的操作集合
     * 用于访问控制，定义了客户端对该主题可以执行的操作（如读、写等）
     */
    private final Set<AclOperation> authorizedOperations;

    /**
     * 主题的唯一标识符
     * 在Kafka 2.8.0及以后版本中引入，用于在集群范围内唯一标识主题
     */
    private final Uuid topicId;

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TopicDescription that = (TopicDescription) o;
        return internal == that.internal &&
            Objects.equals(name, that.name) &&
            Objects.equals(partitions, that.partitions) &&
            Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, internal, partitions, authorizedOperations);
    }

    /**
     * 使用指定参数创建TopicDescription实例
     * 这是一个简化的构造函数，不包含授权操作和主题ID信息
     *
     * @param name 主题名称，用于在Kafka集群中唯一标识该主题
     * @param internal 是否为Kafka内部主题的标志
     * @param partitions 分区信息列表，其中索引表示分区ID，元素包含该分区的leader和副本信息
     */
    public TopicDescription(String name, boolean internal, List<TopicPartitionInfo> partitions) {
        this(name, internal, partitions, Collections.emptySet());
    }

    /**
     * 使用指定参数创建TopicDescription实例
     * 这个构造函数包含了授权操作信息，但使用默认的ZERO_UUID作为主题ID
     *
     * @param name 主题名称，用于在Kafka集群中唯一标识该主题
     * @param internal 是否为Kafka内部主题的标志
     * @param partitions 分区信息列表，其中索引表示分区ID，元素包含该分区的leader和副本信息
     * @param authorizedOperations 该主题允许执行的操作集合，如果未知则为空集合
     */
    public TopicDescription(String name, boolean internal, List<TopicPartitionInfo> partitions,
                            Set<AclOperation> authorizedOperations) {
        this(name, internal, partitions, authorizedOperations, Uuid.ZERO_UUID);
    }

    /**
     * 使用指定参数创建TopicDescription实例
     * 这是最完整的构造函数，包含了所有主题相关的元数据信息
     *
     * @param name 主题名称，用于在Kafka集群中唯一标识该主题
     * @param internal 是否为Kafka内部主题的标志
     * @param partitions 分区信息列表，其中索引表示分区ID，元素包含该分区的leader和副本信息
     * @param authorizedOperations 该主题允许执行的操作集合，如果未知则为空集合
     * @param topicId 主题的唯一标识符，在Kafka 2.8.0及以后版本中使用
     */
    public TopicDescription(String name, boolean internal, List<TopicPartitionInfo> partitions,
                            Set<AclOperation> authorizedOperations, Uuid topicId) {
        this.name = name;
        this.internal = internal;
        this.partitions = partitions;
        this.authorizedOperations = authorizedOperations;
        this.topicId = topicId;
    }

    /**
     * 获取主题名称
     * 返回在Kafka集群中唯一标识该主题的名称字符串
     */
    public String name() {
        return name;
    }

    /**
     * 判断是否为Kafka内部主题
     * 内部主题（如消费者偏移量和组管理主题__consumer_offsets）用于存储Kafka自身的元数据信息
     * 这些主题对于Kafka的正常运行至关重要，通常由Kafka自动管理
     */
    public boolean isInternal() {
        return internal;
    }

    /**
     * 获取主题的唯一标识符
     * 返回在Kafka 2.8.0及以后版本中用于唯一标识主题的UUID
     */
    public Uuid topicId() {
        return topicId;
    }

    /**
     * 获取主题的分区信息列表
     * 返回包含所有分区详细信息的列表，每个元素包含分区的leader副本、ISR集合等信息
     * 列表的索引对应分区ID，这对于分区级别的操作（如分区重分配）非常重要
     */
    public List<TopicPartitionInfo> partitions() {
        return partitions;
    }

    /**
     * 获取主题允许执行的操作集合
     * 返回当前客户端被授权对该主题执行的操作集合（如读、写等）
     * 如果权限信息未知，则返回空集合
     */
    public Set<AclOperation>  authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public String toString() {
        return "(name=" + name + ", internal=" + internal + ", partitions=" +
                partitions.stream().map(TopicPartitionInfo::toString).collect(Collectors.joining(",")) + ", authorizedOperations=" + authorizedOperations + ")";
    }
}
