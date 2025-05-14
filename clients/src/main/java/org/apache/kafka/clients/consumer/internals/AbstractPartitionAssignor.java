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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 抽象分配器实现，它执行一些常见的繁重工作（特别是收集分区计数，这在分配器中总是需要的）。
 * <p>
 * 应用场景：作为所有具体分区分配策略（如 RangeAssignor, StickyAssignor）的基类，提供通用的分区分配逻辑和辅助功能。
 * 实现细节：定义了分区分配的核心流程和一些辅助方法，如获取主题分区信息、处理消费者订阅等。
 * 设计考虑：通过抽象类减少重复代码，提供统一的接口和基础实现，方便扩展新的分配策略。
 */
public abstract class AbstractPartitionAssignor implements ConsumerPartitionAssignor {
    // 日志记录器，用于记录分配过程中的信息和错误
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);
    // 表示没有节点的空节点数组，用于某些不需要节点信息的场景
    private static final Node[] NO_NODES = new Node[] {Node.noNode()};

    // 仅在单元测试中使用，用于当所有机架都拥有所有分区时验证机架感知分配逻辑。
    // 设计考虑：此标志允许测试在特定条件下强制启用或测试机架感知行为，即使在常规情况下可能不会触发。
    boolean preferRackAwareLogic;

    /**
     * 根据分区计数和成员订阅执行组分配。
     * <p>
     * 应用场景：这是分区分配的核心抽象方法，由具体的分配策略实现。
     * 实现细节：子类需要根据自身逻辑，将主题分区分配给消费者。
     * 设计考虑：定义了分配策略必须实现的接口，使得不同的分配算法可以插入到分配流程中。
     *
     * @param partitionsPerTopic 每个已订阅主题的分区数。元数据中不存在的主题将从此映射中排除。
     * @param subscriptions 从成员 ID 到其各自主题订阅的映射。
     * @return 从每个成员到分配给他们的分区列表的映射。
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);

    /**
     * {@code assignPartitions()} 的默认实现，不包括机架信息。包含此方法仅为避免破坏任何扩展了 {@code AbstractPartitionAssignor} 的自定义实现。
     * 请注意，此类是内部类，但为安全起见，我们保持了兼容性。
     * <p>
     * 应用场景：为不关心机架信息的旧版或简单分配器提供一个默认实现。
     * 实现细节：将包含 {@link PartitionInfo} 的映射转换为仅包含分区计数的映射，然后调用抽象的 {@code assign} 方法。
     * 设计考虑：向后兼容旧的分配器实现，这些实现可能没有考虑到机架感知分配。
     *
     * @param partitionsPerTopic 每个主题的分区信息列表的映射。键是主题名称，值是该主题的 {@link PartitionInfo} 列表。
     * @param subscriptions 从成员 ID 到其各自主题订阅的映射。
     * @return 从每个成员到分配给他们的 {@link TopicPartition} 列表的映射。
     */
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {
        // 将包含 PartitionInfo 对象的 partitionsPerTopic 映射转换为仅包含每个主题分区数量的映射
        // 实现细节：使用 Java Stream API 进行转换，键是主题名称，值是该主题的分区列表的大小（即分区数）
        Map<String, Integer> partitionCountPerTopic = partitionsPerTopic.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().size()));
        // 调用抽象的 assign 方法，传入转换后的分区计数映射和原始的订阅信息
        // 设计考虑：这是为了适配那些只需要分区数量而不需要详细分区信息的 assign 方法实现
        return assign(partitionCountPerTopic, subscriptions);
    }

    /**
     * 根据集群元数据和组订阅信息执行分区分配。
     * 这是 {@link ConsumerPartitionAssignor#assign(Cluster, GroupSubscription)} 接口的实现。
     * <p>
     * 应用场景：消费者协调器调用此方法来获取消费者组的分区分配方案。
     * 实现细节：
     * 1. 提取所有消费者订阅的主题。
     * 2. 获取这些主题的分区信息，并按分区号排序。
     * 3. 调用 {@code assignPartitions} 方法（可能被子类覆盖）来获取原始的分区分配结果。
     * 4. 将原始分配结果包装成 {@link GroupAssignment} 对象返回。
     * 设计考虑：提供了一个标准的分配流程框架，具体的分配逻辑由 {@code assignPartitions} 和 {@code assign} (抽象方法) 实现。
     *
     * @param metadata 集群元数据，包含主题、分区、Broker等信息。
     * @param groupSubscription 消费者组的订阅信息，包含每个消费者的订阅详情。
     * @return {@link GroupAssignment} 对象，表示最终的分区分配结果。
     */
    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        // 从 groupSubscription 中获取所有成员的订阅信息
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        // 创建一个集合，用于存储所有成员订阅的所有主题的名称
        Set<String> allSubscribedTopics = new HashSet<>();
        // 遍历每个成员的订阅条目
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            // 将当前成员订阅的主题列表添加到 allSubscribedTopics 集合中
            // 实现细节：addAll 方法确保了主题名称的唯一性，因为 Set 不允许重复元素
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        // 创建一个映射，用于存储每个主题对应的分区信息列表
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        // 遍历所有被订阅的主题
        for (String topic : allSubscribedTopics) {
            // 从集群元数据中获取当前主题的分区信息列表
            List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
            // 检查获取到的分区信息是否有效（不为 null且不为空）
            if (partitions != null && !partitions.isEmpty()) {
                // 如果分区信息有效，创建一个新的 ArrayList 来存储这些分区信息（确保可修改）
                partitions = new ArrayList<>(partitions);
                // 对分区列表按照分区号进行升序排序
                // 实现细节：使用 Comparator.comparingInt 和 PartitionInfo::partition 方法引用进行排序
                partitions.sort(Comparator.comparingInt(PartitionInfo::partition));
                // 将排序后的分区信息列表存入 partitionsPerTopic 映射中，键为主题名称
                partitionsPerTopic.put(topic, partitions);
            } else {
                // 如果主题的分区信息不可用（例如，主题不存在或元数据尚未同步），则记录一条调试日志
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
            }
        }

        // 调用 assignPartitions 方法（可能是子类重写的版本或本类的默认实现）来获取原始的分区分配结果
        // rawAssignments 是一个从消费者成员 ID 到其分配到的 TopicPartition 列表的映射
        Map<String, List<TopicPartition>> rawAssignments = assignPartitions(partitionsPerTopic, subscriptions);

        // 此类不维护用户数据，因此只需包装结果
        // 创建一个新的映射，用于存储最终的分配结果，格式为成员 ID 到 Assignment 对象的映射
        Map<String, Assignment> assignments = new HashMap<>();
        // 遍历原始分配结果中的每个条目（成员 ID -> 分区列表）
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            // 将每个成员的分区列表包装成一个 Assignment 对象，并存入 assignments 映射中
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        // 使用最终的 assignments 映射创建一个 GroupAssignment 对象并返回
        // 设计考虑：GroupAssignment 是消费者协调器期望的返回类型，它封装了整个消费者组的分配情况
        return new GroupAssignment(assignments);
    }

    /**
     * 将一个键值对放入一个值为列表类型的映射中。如果键不存在，则创建一个新的空列表并与该键关联。
     * <p>
     * 应用场景：方便地向一个 Map<K, List<V>> 结构中添加元素，避免手动检查键是否存在和初始化列表。
     * 实现细节：使用 {@link Map#computeIfAbsent(Object, java.util.function.Function)} 方法，如果键不存在，则使用提供的函数创建一个新列表。
     * 设计考虑：这是一个通用的辅助方法，简化了处理值为列表的映射时的常见操作。
     *
     * @param map 目标映射。
     * @param key 要添加元素的键。
     * @param value 要添加到与键关联的列表中的值。
     * @param <K> 映射中键的类型。
     * @param <V> 映射中列表元素的类型。
     */
    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        // 获取或创建与 key 关联的列表。如果 key 不存在，则创建一个新的 ArrayList。
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        // 将 value 添加到获取或创建的列表中。
        list.add(value);
    }

    /**
     * 为给定的主题和分区数量生成一个 {@link TopicPartition} 列表。
     * <p>
     * 应用场景：当需要表示一个主题的所有分区时，例如在测试或某些分配逻辑中。
     * 实现细节：创建一个初始容量为 {@code numPartitions} 的列表，然后循环从0到 {@code numPartitions-1} 创建 {@link TopicPartition} 对象并添加到列表中。
     * 设计考虑：提供一个便捷的方法来快速生成一个主题的所有分区对象。
     *
     * @param topic 主题名称。
     * @param numPartitions 该主题的分区数量。
     * @return 包含该主题所有分区的 {@link TopicPartition} 列表。
     */
    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        // 创建一个初始容量为 numPartitions 的 ArrayList，用于存储 TopicPartition 对象
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        // 循环从 0 到 numPartitions - 1
        for (int i = 0; i < numPartitions; i++)
            // 为当前主题和分区号 i 创建一个新的 TopicPartition 对象，并将其添加到列表中
            partitions.add(new TopicPartition(topic, i));
        // 返回包含所有 TopicPartition 对象的列表
        return partitions;
    }

    /**
     * 根据每个主题的分区数量，生成一个不包含机架信息的 {@link PartitionInfo} 列表的映射。
     * <p>
     * 应用场景：当需要 {@link PartitionInfo} 对象但机架信息不可用或不重要时，例如在某些旧的或简化的分配逻辑中，或者在测试中模拟没有机架信息的场景。
     * 实现细节：遍历输入的 {@code partitionsPerTopic} 映射，对于每个主题，根据其分区数量创建相应数量的 {@link PartitionInfo} 对象。
     *           这些 {@link PartitionInfo} 对象的 leader、replicas 和 inSyncReplicas 都被设置为空节点或空节点数组。
     * 设计考虑：提供一个辅助方法，用于在缺乏完整集群元数据（特别是机架信息）的情况下，构建基本的 {@link PartitionInfo} 结构。
     *
     * @param partitionsPerTopic 一个映射，键是主题名称，值是该主题的分区数量。
     * @return 一个映射，键是主题名称，值是该主题的 {@link PartitionInfo} 列表（不含机架信息）。
     */
    protected static Map<String, List<PartitionInfo>> partitionInfosWithoutRacks(Map<String, Integer> partitionsPerTopic) {
        // 使用 Java Stream API 将输入的 partitionsPerTopic 映射转换为新的映射
        return partitionsPerTopic.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
            // 获取当前条目的主题名称
            String topic = e.getKey();
            // 获取当前主题的分区数量
            int numPartitions = e.getValue();
            // 创建一个初始容量为 numPartitions 的 ArrayList，用于存储 PartitionInfo 对象
            List<PartitionInfo> partitionInfos = new ArrayList<>(numPartitions);
            // 循环从 0 到 numPartitions - 1
            for (int i = 0; i < numPartitions; i++)
                // 为当前主题和分区号 i 创建一个新的 PartitionInfo 对象，其中 leader、replicas 和 inSyncReplicas 均设置为无节点信息
                // Node.noNode() 返回一个表示不存在或未知的节点
                // NO_NODES 是一个包含单个 Node.noNode() 的数组，用于表示空的副本集或 ISR 集
                partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), NO_NODES, NO_NODES));
            // 返回为当前主题生成的 PartitionInfo 列表
            return partitionInfos;
        }));
    }

    /**
     * 判断是否应该使用机架感知分配策略。
     * <p>
     * 应用场景：在分区分配过程中，决定是否要考虑消费者和分区副本所在的机架信息，以优化网络延迟和提高数据局部性。
     * 实现细节：
     * 1. 如果消费者没有机架信息，或者消费者的机架与分区副本的机架完全没有交集，则不使用机架感知分配。
     * 2. 如果 {@code preferRackAwareLogic} 标志为 true（通常用于测试），则强制使用机架感知分配。
     * 3. 否则，检查是否所有分区的副本都分布在所有已知的机架上。如果不是（即某些分区只在部分机架上有副本），则使用机架感知分配。
     *    这意味着如果所有分区在所有机架上都有副本，那么机架感知分配的意义不大，因为任何消费者都可以访问任何分区而无需跨机架。
     * 设计考虑：此方法提供了一种灵活的机制来启用或禁用机架感知分配，基于可用的机架信息和特定的分配需求。
     *
     * @param consumerRacks 消费者所在机架的集合。
     * @param partitionRacks 分区副本所在机架的集合。
     * @param racksPerPartition 每个分区及其副本所在机架的映射。
     * @return 如果应该使用机架感知分配，则返回 true；否则返回 false。
     */
    protected boolean useRackAwareAssignment(Set<String> consumerRacks, Set<String> partitionRacks, Map<TopicPartition, Set<String>> racksPerPartition) {
        // 如果消费者的机架信息为空，或者消费者的机架集合与分区副本的机架集合没有交集，则不使用机架感知分配
        if (consumerRacks.isEmpty() || Collections.disjoint(consumerRacks, partitionRacks))
            // 返回 false，表示不进行机架感知分配
            return false;
        // 如果 preferRackAwareLogic 标志为 true（通常在测试中设置，用于强制启用机架感知逻辑）
        else if (preferRackAwareLogic)
            // 返回 true，表示进行机架感知分配
            return true;
        // 其他情况，需要进一步判断
        else {
            // 检查是否 racksPerPartition（每个分区对应的机架集合）中的所有值（即每个分区的机架集合）都与 partitionRacks（所有分区副本的总机架集合）相同
            // 如果所有分区的副本都均匀分布在所有已知的机架上（即每个分区的机架集合都等于总的机架集合），那么机架感知分配的意义不大
            // 因此，如果不是所有分区的机架集合都等于总机架集合（即存在某些分区只在部分机架上有副本），则应该使用机架感知分配
            return !racksPerPartition.values().stream().allMatch(partitionRacks::equals);
        }
    }

    /**
     * 表示消费者组成员的信息，用于分区分配过程。
     * <p>
     * 应用场景：在分区分配算法中，需要对消费者成员进行排序和识别，特别是在处理静态成员和机架感知分配时。
     * 实现细节：包含成员ID、可选的组实例ID（用于静态成员）和可选的机架ID。
     *           实现了 {@link Comparable} 接口，以便对成员列表进行排序。
     * 设计考虑：封装了成员的关键信息，简化了分配器中对成员数据的处理。
     *           排序逻辑优先考虑静态成员（有 {@code groupInstanceId} 的成员），然后是动态成员（按 {@code memberId} 排序）。
     */
    public static class MemberInfo implements Comparable<MemberInfo> {
        // 消费者的成员ID，在消费者组内唯一
        public final String memberId;
        // 消费者的组实例ID，用于静态成员资格。如果不是静态成员，则为空 Optional。
        public final Optional<String> groupInstanceId;
        // 消费者所在的机架ID。如果未配置或不可用，则为空 Optional。
        public final Optional<String> rackId;

        /**
         * {@link MemberInfo} 的构造函数。
         *
         * @param memberId 消费者的成员ID。
         * @param groupInstanceId 消费者的组实例ID (Optional)。
         * @param rackId 消费者所在的机架ID (Optional)。
         */
        public MemberInfo(String memberId, Optional<String> groupInstanceId, Optional<String> rackId) {
            // 初始化成员ID
            this.memberId = memberId;
            // 初始化组实例ID
            this.groupInstanceId = groupInstanceId;
            // 初始化机架ID
            this.rackId = rackId;
        }

        /**
         * {@link MemberInfo} 的构造函数，不带机架ID (默认为空)。
         *
         * @param memberId 消费者的成员ID。
         * @param groupInstanceId 消费者的组实例ID (Optional)。
         */
        public MemberInfo(String memberId, Optional<String> groupInstanceId) {
            // 调用另一个构造函数，将机架ID设置为空 Optional
            this(memberId, groupInstanceId, Optional.empty());
        }

        /**
         * 比较此 {@link MemberInfo} 对象与另一个 {@link MemberInfo} 对象的顺序。
         * 排序逻辑：
         * 1. 如果两个成员都有 {@code groupInstanceId}（即都是静态成员），则按 {@code groupInstanceId} 的字典序比较。
         * 2. 如果此成员有 {@code groupInstanceId} 而另一个没有，则此成员排在前面（返回 -1）。
         * 3. 如果另一个成员有 {@code groupInstanceId} 而此成员没有，则此成员排在后面（返回 1）。
         * 4. 如果两个成员都没有 {@code groupInstanceId}（即都是动态成员），则按 {@code memberId} 的字典序比较。
         * <p>
         * 应用场景：在分区分配前对消费者成员列表进行排序，以确保分配的确定性和一致性，特别是对于静态成员。
         * 设计考虑：优先静态成员，并确保静态成员之间的排序是稳定的，然后才是动态成员。
         *
         * @param otherMemberInfo 要比较的另一个 {@link MemberInfo} 对象。
         * @return 一个负整数、零或一个正整数，表示此对象小于、等于或大于指定的对象。
         */
        @Override
        public int compareTo(MemberInfo otherMemberInfo) {
            // 检查当前 MemberInfo 和另一个 MemberInfo 是否都有 groupInstanceId
            if (this.groupInstanceId.isPresent() &&
                    otherMemberInfo.groupInstanceId.isPresent()) {
                // 如果两者都有 groupInstanceId（都是静态成员），则按 groupInstanceId 的字典序进行比较
                return this.groupInstanceId.get()
                        .compareTo(otherMemberInfo.groupInstanceId.get());
            // 如果当前 MemberInfo 有 groupInstanceId，而另一个没有（当前是静态成员，另一个是动态成员）
            } else if (this.groupInstanceId.isPresent()) {
                // 当前 MemberInfo（静态成员）应排在前面
                return -1;
            // 如果另一个 MemberInfo 有 groupInstanceId，而当前没有（另一个是静态成员，当前是动态成员）
            } else if (otherMemberInfo.groupInstanceId.isPresent()) {
                // 当前 MemberInfo（动态成员）应排在后面
                return 1;
            // 如果两者都没有 groupInstanceId（都是动态成员）
            } else {
                // 按 memberId 的字典序进行比较
                return this.memberId.compareTo(otherMemberInfo.memberId);
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MemberInfo && this.memberId.equals(((MemberInfo) o).memberId);
        }

        /**
         * 我们可以直接使用 member.id 作为哈希码，因为它在组内是唯一的。
         */
        @Override
        public int hashCode() {
            return memberId.hashCode();
        }

        @Override
        public String toString() {
            return "MemberInfo [member.id: " + memberId
                    + ", group.instance.id: " + groupInstanceId.orElse("{}")
                    + "]";
        }
    }
}
