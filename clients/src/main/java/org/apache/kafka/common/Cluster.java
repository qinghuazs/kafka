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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Kafka集群中节点、主题和分区的不可变表示。
 * 这个类提供了对Kafka集群元数据的只读访问，包括集群中的节点、主题、分区信息等。
 * 所有的字段都是final的，确保了集群状态的不可变性，任何更新都会创建新的Cluster实例。
 */
public final class Cluster {

    // 标识是否是通过bootstrap方式配置的集群
    private final boolean isBootstrapConfigured;
    
    // 集群中的所有节点列表
    private final List<Node> nodes;
    
    // 客户端没有权限访问的主题集合
    private final Set<String> unauthorizedTopics;
    
    // 无效或不存在的主题集合
    private final Set<String> invalidTopics;
    
    // Kafka内部使用的系统主题集合
    private final Set<String> internalTopics;
    
    // 集群的控制器节点，负责管理分区leader选举等操作
    private final Node controller;
    
    // 按主题分区索引的分区信息映射，key是TopicPartition，value是对应的分区信息
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    
    // 按主题名称索引的分区列表映射，key是主题名，value是该主题的所有分区列表
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    
    // 按主题名称索引的可用分区列表映射，只包含有leader的分区
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    
    // 按节点ID索引的分区列表映射，记录每个节点作为leader的分区
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    
    // 按节点ID索引的节点映射，用于快速查找节点
    private final Map<Integer, Node> nodesById;
    
    // 集群资源标识，包含集群ID等信息
    private final ClusterResource clusterResource;
    
    // 主题名称到主题ID的映射
    private final Map<String, Uuid> topicIds;
    
    // 主题ID到主题名称的映射
    private final Map<Uuid, String> topicNames;

    /**
     * 创建一个新的集群实例
     * @param clusterId 集群的唯一标识符
     * @param nodes 集群中的所有节点集合
     * @param partitions 集群中主题分区的信息集合
     * @param unauthorizedTopics 客户端没有权限访问的主题集合
     * @param internalTopics Kafka内部使用的系统主题集合
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, null, Collections.emptyMap());
    }

    /**
     * 创建一个新的集群实例，包含控制器节点信息
     * @param clusterId 集群的唯一标识符
     * @param nodes 集群中的所有节点集合
     * @param partitions 集群中主题分区的信息集合
     * @param unauthorizedTopics 客户端没有权限访问的主题集合
     * @param internalTopics Kafka内部使用的系统主题集合
     * @param controller 集群的控制器节点，负责管理分区leader选举等操作
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, controller, Collections.emptyMap());
    }

    /**
     * 创建一个新的集群实例，包含无效主题信息
     * @param clusterId 集群的唯一标识符
     * @param nodes 集群中的所有节点集合
     * @param partitions 集群中主题分区的信息集合
     * @param unauthorizedTopics 客户端没有权限访问的主题集合
     * @param invalidTopics 无效或不存在的主题集合
     * @param internalTopics Kafka内部使用的系统主题集合
     * @param controller 集群的控制器节点
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, Collections.emptyMap());
    }

    /**
     * 创建一个新的集群实例，包含主题ID信息
     * @param clusterId 集群的唯一标识符
     * @param nodes 集群中的所有节点集合
     * @param partitions 集群中主题分区的信息集合
     * @param unauthorizedTopics 客户端没有权限访问的主题集合
     * @param invalidTopics 无效或不存在的主题集合
     * @param internalTopics Kafka内部使用的系统主题集合
     * @param controller 集群的控制器节点
     * @param topicIds 主题名称到主题ID的映射关系
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller,
                   Map<String, Uuid> topicIds) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, topicIds);
    }

    /**
     * 私有构造函数，用于创建一个新的Cluster实例。
     * 该构造函数负责初始化集群的所有元数据信息，包括节点列表、分区信息、主题映射等。
     * 所有的集合类型字段都会被转换为不可变集合，以保证集群元数据的线程安全性。
     *
     * @param clusterId 集群的唯一标识符
     * @param isBootstrapConfigured 是否是通过bootstrap方式配置的集群
     * @param nodes 集群中的所有节点集合
     * @param partitions 集群中所有主题分区的信息集合
     * @param unauthorizedTopics 客户端没有权限访问的主题集合
     * @param invalidTopics 无效或不存在的主题集合
     * @param internalTopics Kafka内部使用的系统主题集合
     * @param controller 集群的控制器节点
     * @param topicIds 主题名称到主题ID的映射关系
     */
    private Cluster(String clusterId,
                    boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics,
                    Set<String> invalidTopics,
                    Set<String> internalTopics,
                    Node controller,
                    Map<String, Uuid> topicIds) {
        // 设置集群的基本属性
        this.isBootstrapConfigured = isBootstrapConfigured;
        this.clusterResource = new ClusterResource(clusterId);
        
        // 创建节点列表的随机排序副本，并转换为不可变列表
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy); // 随机打乱节点顺序，避免总是使用相同的节点顺序
        this.nodes = Collections.unmodifiableList(copy);

        // 构建节点ID到节点对象的映射，用于快速查找节点
        Map<Integer, Node> tmpNodesById = new HashMap<>();
        // 构建节点ID到该节点作为leader的分区列表的映射
        Map<Integer, List<PartitionInfo>> tmpPartitionsByNode = new HashMap<>(nodes.size());
        for (Node node : nodes) {
            tmpNodesById.put(node.id(), node);
            // 为每个节点初始化一个空的分区列表，后续会填充该节点作为leader的分区
            tmpPartitionsByNode.put(node.id(), new ArrayList<>());
        }
        this.nodesById = Collections.unmodifiableMap(tmpNodesById);

        // 构建分区信息的多个索引映射，这部分代码对性能敏感，需要避免不必要的操作
        Map<TopicPartition, PartitionInfo> tmpPartitionsByTopicPartition = new HashMap<>(partitions.size());
        Map<String, List<PartitionInfo>> tmpPartitionsByTopic = new HashMap<>();
        for (PartitionInfo p : partitions) {
            // 构建主题分区到分区信息的映射
            tmpPartitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
            // 构建主题到该主题所有分区的映射
            tmpPartitionsByTopic.computeIfAbsent(p.topic(), topic -> new ArrayList<>()).add(p);

            // 如果分区的leader未知，则跳过后续处理
            if (p.leader() == null || p.leader().isEmpty())
                continue;

            // 将分区添加到其leader节点的分区列表中
            List<PartitionInfo> partitionsForNode = Objects.requireNonNull(tmpPartitionsByNode.get(p.leader().id()));
            partitionsForNode.add(p);
        }

        // 将每个节点的分区列表转换为不可变列表
        for (Map.Entry<Integer, List<PartitionInfo>> entry : tmpPartitionsByNode.entrySet()) {
            tmpPartitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }

        // 构建可用分区的映射（只包含有leader的分区），并将所有分区列表转换为不可变列表
        Map<String, List<PartitionInfo>> tmpAvailablePartitionsByTopic = new HashMap<>(tmpPartitionsByTopic.size());
        for (Map.Entry<String, List<PartitionInfo>> entry : tmpPartitionsByTopic.entrySet()) {
            String topic = entry.getKey();
            // 将主题的分区列表转换为不可变列表
            List<PartitionInfo> partitionsForTopic = Collections.unmodifiableList(entry.getValue());
            tmpPartitionsByTopic.put(topic, partitionsForTopic);
            
            // 检查是否存在没有leader的分区
            boolean foundUnavailablePartition = partitionsForTopic.stream().anyMatch(p -> p.leader() == null);
            List<PartitionInfo> availablePartitionsForTopic;
            if (foundUnavailablePartition) {
                // 如果存在没有leader的分区，创建一个只包含有leader分区的列表
                availablePartitionsForTopic = new ArrayList<>(partitionsForTopic.size());
                for (PartitionInfo p : partitionsForTopic) {
                    if (p.leader() != null)
                        availablePartitionsForTopic.add(p);
                }
                availablePartitionsForTopic = Collections.unmodifiableList(availablePartitionsForTopic);
            } else {
                // 如果所有分区都有leader，直接使用原列表
                availablePartitionsForTopic = partitionsForTopic;
            }
            tmpAvailablePartitionsByTopic.put(topic, availablePartitionsForTopic);
        }

        // 将所有临时映射转换为不可变映射
        this.partitionsByTopicPartition = Collections.unmodifiableMap(tmpPartitionsByTopicPartition);
        this.partitionsByTopic = Collections.unmodifiableMap(tmpPartitionsByTopic);
        this.availablePartitionsByTopic = Collections.unmodifiableMap(tmpAvailablePartitionsByTopic);
        this.partitionsByNode = Collections.unmodifiableMap(tmpPartitionsByNode);
        this.topicIds = Collections.unmodifiableMap(topicIds);
        
        // 构建主题ID到主题名称的反向映射
        Map<Uuid, String> tmpTopicNames = new HashMap<>();
        topicIds.forEach((key, value) -> tmpTopicNames.put(value, key));
        this.topicNames = Collections.unmodifiableMap(tmpTopicNames);

        // 设置主题相关的集合，全部转换为不可变集合
        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
    }

    /**
     * 创建一个空的集群实例，不包含任何节点和主题分区。
     * 这个方法通常用于初始化一个新的集群，或者在测试场景中使用。
     * 
     * @return 返回一个没有节点和分区的空集群实例
     */
    public static Cluster empty() {
        // 创建一个完全空的集群实例，所有集合都是空的，clusterId和controller都是null
        return new Cluster(null, new ArrayList<>(0), new ArrayList<>(0), Collections.emptySet(),
            Collections.emptySet(), null);
    }

    /**
     * 使用给定的主机/端口列表创建一个引导（bootstrap）集群。
     * 这个方法用于客户端初始连接Kafka集群时，通过配置的bootstrap.servers创建初始集群视图。
     * 
     * @param addresses 包含主机名和端口的地址列表
     * @return 返回一个包含指定地址节点的引导集群实例
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;  // 使用负数作为临时节点ID，避免与实际节点ID冲突
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        // 创建一个引导集群，isBootstrapConfigured设置为true，表示这是一个引导集群
        return new Cluster(null, true, nodes, new ArrayList<>(0),
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
    }

    /**
     * 返回一个包含新增分区信息的集群副本。
     * 这个方法用于在现有集群基础上添加新的分区信息，而不修改原有集群实例。
     * 
     * @param partitions 要添加的新分区信息映射
     * @return 返回一个包含合并后分区信息的新集群实例
     */
    public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
        // 创建现有分区映射的副本
        Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
        // 添加新的分区信息
        combinedPartitions.putAll(partitions);
        // 创建新的集群实例，包含合并后的分区信息
        return new Cluster(clusterResource.clusterId(), this.nodes, combinedPartitions.values(),
                new HashSet<>(this.unauthorizedTopics), new HashSet<>(this.invalidTopics),
                new HashSet<>(this.internalTopics), this.controller);
    }

    /**
     * 获取集群中所有已知的节点列表。
     * 
     * @return 返回集群中的所有节点列表（不可修改）
     */
    public List<Node> nodes() {
        return this.nodes;
    }

    /**
     * 根据节点ID获取对应的节点信息。
     * 
     * @param id 要查询的节点ID
     * @return 如果节点存在且在线则返回节点对象，否则返回null
     */
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * 检查指定分区的副本节点是否在线。
     * 这个方法用于确认某个分区的特定副本节点是否可用。
     * 
     * @param partition 要检查的主题分区
     * @param id 要检查的节点ID
     * @return 如果节点在线且是指定分区的有效副本，则返回该节点，否则返回空
     */
    public Optional<Node> nodeIfOnline(TopicPartition partition, int id) {
        // 获取节点和分区信息
        Node node = nodeById(id);
        PartitionInfo partitionInfo = partition(partition);

        // 检查节点是否在线且是分区的有效副本
        if (node != null && partitionInfo != null &&
            !Arrays.asList(partitionInfo.offlineReplicas()).contains(node) &&  // 节点不在离线副本列表中
            Arrays.asList(partitionInfo.replicas()).contains(node)) {         // 节点是分区的副本之一

            return Optional.of(node);
        } else {
            return Optional.empty();
        }
    }

    /**
     * 获取指定主题分区的当前leader节点。
     * 
     * @param topicPartition 要查询leader的主题分区
     * @return 返回该分区的leader节点，如果没有leader则返回null
     */
    public Node leaderFor(TopicPartition topicPartition) {
        // 获取分区信息
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return info.leader();  // 返回分区的leader节点
    }

    /**
     * 获取指定主题分区的元数据信息。
     * 
     * @param topicPartition 要查询的主题分区
     * @return 返回分区的元数据信息，如果分区不存在则返回null
     */
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * 获取指定主题的所有分区列表。
     * 
     * @param topic 主题名称
     * @return 返回主题的所有分区列表，如果主题不存在则返回空列表
     */
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return partitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }

    /**
     * 获取指定主题的分区数量。
     * 
     * @param topic 要查询的主题名称
     * @return 返回主题的分区数量，如果主题不存在则返回null
     */
    public Integer partitionCountForTopic(String topic) {
        List<PartitionInfo> partitions = this.partitionsByTopic.get(topic);
        return partitions == null ? null : partitions.size();
    }

    /**
     * 获取指定主题的所有可用分区列表。
     * 可用分区指的是当前有leader的分区。
     * 
     * @param topic 主题名称
     * @return 返回主题的所有可用分区列表，如果主题不存在则返回空列表
     */
    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        return availablePartitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }

    /**
     * 获取指定节点作为leader的所有分区列表。
     * 
     * @param nodeId 节点ID
     * @return 返回该节点作为leader的所有分区列表，如果节点不存在则返回空列表
     */
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return partitionsByNode.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     * 获取集群中所有主题的集合。
     * 
     * @return 返回所有主题名称的集合
     */
    public Set<String> topics() {
        return partitionsByTopic.keySet();
    }

    /**
     * 获取客户端没有权限访问的主题集合。
     * 这些主题虽然存在于集群中，但当前客户端由于权限限制无法访问它们。
     * 
     * @return 返回一个不可修改的Set，包含所有未授权主题的名称
     */
    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    /**
     * 获取无效或不存在的主题集合。
     * 这些主题可能是由于配置错误或已被删除而变得无效。
     * 
     * @return 返回一个不可修改的Set，包含所有无效主题的名称
     */
    public Set<String> invalidTopics() {
        return invalidTopics;
    }

    /**
     * 获取Kafka内部使用的系统主题集合。
     * 这些主题是Kafka为了实现特定功能而创建的，比如消费者偏移量存储主题等。
     * 
     * @return 返回一个不可修改的Set，包含所有内部主题的名称
     */
    public Set<String> internalTopics() {
        return internalTopics;
    }

    /**
     * 检查当前集群实例是否是通过bootstrap方式配置的。
     * bootstrap配置通常用于客户端首次连接集群时，只包含基本的连接信息。
     * 
     * @return 如果是bootstrap配置的集群则返回true，否则返回false
     */
    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    /**
     * 获取集群资源对象，包含集群的唯一标识符等信息。
     * 这个对象可用于集群级别的操作和管理。
     * 
     * @return 返回代表当前集群资源的ClusterResource对象
     */
    public ClusterResource clusterResource() {
        return clusterResource;
    }

    /**
     * 获取集群的控制器节点。
     * 控制器节点负责管理分区leader的选举、主题的创建/删除等管理操作。
     * 
     * @return 返回当前的控制器节点，如果没有控制器则返回null
     */
    public Node controller() {
        return controller;
    }

    /**
     * 获取集群中所有主题的ID集合。
     * 主题ID是主题的唯一标识符，在整个集群中保持不变。
     * 
     * @return 返回包含所有主题ID的集合
     */
    public Collection<Uuid> topicIds() {
        return topicIds.values();
    }

    /**
     * 根据主题名称获取对应的主题ID。
     * 
     * @param topic 要查询的主题名称
     * @return 返回主题对应的ID，如果主题不存在则返回ZERO_UUID
     */
    public Uuid topicId(String topic) {
        return topicIds.getOrDefault(topic, Uuid.ZERO_UUID);
    }

    /**
     * 根据主题ID获取对应的主题名称。
     * 
     * @param topicId 要查询的主题ID
     * @return 返回主题ID对应的名称，如果ID不存在则返回null
     */
    public String topicName(Uuid topicId) {
        return topicNames.get(topicId);
    }

    @Override
    public String toString() {
        return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes +
            ", partitions = " + this.partitionsByTopicPartition.values() + ", controller = " + controller + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return isBootstrapConfigured == cluster.isBootstrapConfigured &&
                Objects.equals(nodes, cluster.nodes) &&
                Objects.equals(unauthorizedTopics, cluster.unauthorizedTopics) &&
                Objects.equals(invalidTopics, cluster.invalidTopics) &&
                Objects.equals(internalTopics, cluster.internalTopics) &&
                Objects.equals(controller, cluster.controller) &&
                Objects.equals(partitionsByTopicPartition, cluster.partitionsByTopicPartition) &&
                Objects.equals(clusterResource, cluster.clusterResource) &&
                Objects.equals(topicIds, cluster.topicIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isBootstrapConfigured, nodes, unauthorizedTopics, invalidTopics, internalTopics, controller,
                partitionsByTopicPartition, clusterResource, topicIds);
    }
}
