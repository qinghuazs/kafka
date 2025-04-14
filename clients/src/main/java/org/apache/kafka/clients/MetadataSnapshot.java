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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Kafka集群元数据的内部不可变快照，包含了节点、主题和分区的信息。该类维护了一个优化用于读取访问的最新集群实例。
 * 相比使用公共的{@link Cluster}类，更推荐扩展MetadataSnapshot的API用于内部客户端使用。
 * 
 * 该类的主要功能：
 * 1. 维护集群节点、主题、分区的最新状态
 * 2. 提供不可变的数据视图，保证线程安全
 * 3. 支持元数据的增量更新和合并
 * 4. 优化读取性能的缓存设计
 */
public class MetadataSnapshot {
    // 集群唯一标识符
    private final String clusterId;
    // 集群中的所有节点，key为节点ID
    private final Map<Integer, Node> nodes;
    // 当前客户端未被授权访问的主题集合
    private final Set<String> unauthorizedTopics;
    // 无效或不存在的主题集合
    private final Set<String> invalidTopics;
    // Kafka内部使用的主题集合
    private final Set<String> internalTopics;
    // 集群控制器节点
    private final Node controller;
    // 主题分区的元数据信息，key为主题分区
    private final Map<TopicPartition, PartitionMetadata> metadataByPartition;
    // 主题名称到主题ID的映射
    private final Map<String, Uuid> topicIds;
    // 主题ID到主题名称的映射
    private final Map<Uuid, String> topicNames;
    // 缓存的集群实例，用于优化读取性能
    private Cluster clusterInstance;

    public MetadataSnapshot(String clusterId,
                  Map<Integer, Node> nodes,
                  Collection<PartitionMetadata> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller,
                  Map<String, Uuid> topicIds) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, topicIds, null);
    }

    // Visible for testing
    public MetadataSnapshot(String clusterId,
        Map<Integer, Node> nodes,
        Collection<PartitionMetadata> partitions,
        Set<String> unauthorizedTopics,
        Set<String> invalidTopics,
        Set<String> internalTopics,
        Node controller,
        Map<String, Uuid> topicIds,
        Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = Collections.unmodifiableMap(nodes);
        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
        this.topicIds = Collections.unmodifiableMap(topicIds);
        this.topicNames = Collections.unmodifiableMap(
            topicIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey))
        );

        Map<TopicPartition, PartitionMetadata> tmpMetadataByPartition = new HashMap<>(partitions.size());
        for (PartitionMetadata p : partitions) {
            tmpMetadataByPartition.put(p.topicPartition, p);
        }
        this.metadataByPartition = Collections.unmodifiableMap(tmpMetadataByPartition);

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    Optional<PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition));
    }

    Map<String, Uuid> topicIds() {
        return topicIds;
    }

    Map<Uuid, String> topicNames() {
        return topicNames;
    }

    Optional<Node> nodeById(int id) {
        return Optional.ofNullable(nodes.get(id));
    }

    public Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    /**
     * 获取指定分区的leader epoch值
     * leader epoch用于标识分区leader的版本号，每次leader变更都会递增
     * 主要用于：
     * 1. 防止脑裂情况下出现多个leader
     * 2. 帮助follower判断自己的日志是否需要截断
     * 3. 确保消费者能够获取到正确的数据
     *
     * @param tp 目标分区
     * @return 如果知道leader epoch则返回其值，否则返回OptionalInt.empty()
     */
    public OptionalInt leaderEpochFor(TopicPartition tp) {
        PartitionMetadata partitionMetadata = metadataByPartition.get(tp);
        if (partitionMetadata == null || partitionMetadata.leaderEpoch.isEmpty()) {
            return OptionalInt.empty();
        } else {
            return OptionalInt.of(partitionMetadata.leaderEpoch.get());
        }
    }

    ClusterResource clusterResource() {
        return new ClusterResource(clusterId);
    }

    /**
     * 将当前元数据快照与新提供的元数据进行合并，返回一个新的元数据快照
     * 新提供的元数据被认为比当前快照更新，因此所有重叠的元数据都会被覆盖
     * 
     * 合并策略：
     * 1. 保留指定需要保留的旧主题元数据
     * 2. 使用新的节点信息替换旧节点
     * 3. 添加新的分区信息
     * 4. 更新主题ID映射
     * 5. 合并特殊主题集合（未授权、无效、内部主题）
     *
     * @param newClusterId 新的集群ID
     * @param newNodes 新的节点集合
     * @param addPartitions 要添加的分区
     * @param addUnauthorizedTopics 要添加的未授权主题
     * @param addInternalTopics 要添加的内部主题
     * @param newController 新的控制器节点
     * @param addTopicIds 新分区对应的主题名称到主题ID的映射
     * @param retainTopic 判断是否需要保留已存在主题元数据的函数
     * @return 合并后的新元数据快照
     */
    MetadataSnapshot mergeWith(String newClusterId,
                            Map<Integer, Node> newNodes,
                            Collection<PartitionMetadata> addPartitions,
                            Set<String> addUnauthorizedTopics,
                            Set<String> addInvalidTopics,
                            Set<String> addInternalTopics,
                            Node newController,
                            Map<String, Uuid> addTopicIds,
                            BiPredicate<String, Boolean> retainTopic) {

        Predicate<String> shouldRetainTopic = topic -> retainTopic.test(topic, internalTopics.contains(topic));

        Map<TopicPartition, PartitionMetadata> newMetadataByPartition = new HashMap<>(addPartitions.size());

        // We want the most recent topic ID. We start with the previous ID stored for retained topics and then
        // update with newest information from the MetadataResponse. We always take the latest state, removing existing
        // topic IDs if the latest state contains the topic name but not a topic ID.
        Map<String, Uuid> newTopicIds = this.topicIds.entrySet().stream()
                .filter(entry -> shouldRetainTopic.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        for (PartitionMetadata partition : addPartitions) {
            newMetadataByPartition.put(partition.topicPartition, partition);
            Uuid id = addTopicIds.get(partition.topic());
            if (id != null)
                newTopicIds.put(partition.topic(), id);
            else
                // Remove if the latest metadata does not have a topic ID
                newTopicIds.remove(partition.topic());
        }
        for (Map.Entry<TopicPartition, PartitionMetadata> entry : metadataByPartition.entrySet()) {
            if (shouldRetainTopic.test(entry.getKey().topic())) {
                newMetadataByPartition.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        Set<String> newUnauthorizedTopics = fillSet(addUnauthorizedTopics, unauthorizedTopics, shouldRetainTopic);
        Set<String> newInvalidTopics = fillSet(addInvalidTopics, invalidTopics, shouldRetainTopic);
        Set<String> newInternalTopics = fillSet(addInternalTopics, internalTopics, shouldRetainTopic);

        return new MetadataSnapshot(newClusterId, newNodes, newMetadataByPartition.values(), newUnauthorizedTopics,
                newInvalidTopics, newInternalTopics, newController, newTopicIds);
    }

    /**
     * Copies {@code baseSet} and adds all non-existent elements in {@code fillSet} such that {@code predicate} is true.
     * In other words, all elements of {@code baseSet} will be contained in the result, with additional non-overlapping
     * elements in {@code fillSet} where the predicate is true.
     *
     * @param baseSet the base elements for the resulting set
     * @param fillSet elements to be filled into the resulting set
     * @param predicate tested against the fill set to determine whether elements should be added to the base set
     */
    private static <T> Set<T> fillSet(Set<T> baseSet, Set<T> fillSet, Predicate<T> predicate) {
        Set<T> result = new HashSet<>(baseSet);
        for (T element : fillSet) {
            if (predicate.test(element)) {
                result.add(element);
            }
        }
        return result;
    }

    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata, nodes))
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes.values(), partitionInfos, unauthorizedTopics,
                invalidTopics, internalTopics, controller, topicIds);
    }

    static MetadataSnapshot bootstrap(List<InetSocketAddress> addresses) {
        Map<Integer, Node> nodes = new HashMap<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses) {
            nodes.put(nodeId, new Node(nodeId, address.getHostString(), address.getPort()));
            nodeId--;
        }
        return new MetadataSnapshot(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                null, Collections.emptyMap(), Cluster.bootstrap(addresses));
    }

    static MetadataSnapshot empty() {
        return new MetadataSnapshot(null, Collections.emptyMap(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap(), Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataSnapshot{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values() +
                ", controller=" + controller +
                '}';
    }

}
