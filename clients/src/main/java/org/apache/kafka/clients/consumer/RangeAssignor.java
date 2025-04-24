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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and
 * <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>,
 * <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t1p0, t1p1]</code></li>
 * <li><code>C1: [t0p2, t1p2]</code></li>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>group.instance.id</code> to make the assignment behavior more sticky.
 * For the above example, after one rolling bounce, group coordinator will attempt to assign new <code>member.id</code> towards consumers,
 * for example <code>C0</code> -&gt; <code>C3</code> <code>C1</code> -&gt; <code>C2</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])</code>
 * <li><code>C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])</code>
 * </ul>
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the group.instance.id.
 * Consumers will have individual instance ids <code>I1</code>, <code>I2</code>. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>I0: [t0p0, t0p1, t1p0, t1p1]</code>
 * <li><code>I1: [t0p2, t1p2]</code>
 * </ul>
 * <p>
 * Rack-aware assignment is used if both consumer and partition replica racks are available and
 * some partitions have replicas only on a subset of racks. We attempt to match consumer racks with
 * partition replica racks on a best-effort basis, prioritizing balanced assignment over rack alignment.
 * Topics with equal partition count and same set of subscribers guarantee co-partitioning by prioritizing
 * co-partitioning over rack alignment. In this case, aligning partition replicas of these topics on the
 * same racks will improve locality for consumers. For example, if partitions 0 of all topics have a replica
 * on rack 'a', partition 1 on rack 'b' etc., partition 0 of all topics can be assigned to a consumer
 * on rack 'a', partition 1 to a consumer on rack 'b' and so on.
 * <p>
 * Note that rack-aware assignment currently takes all replicas into account, including any offline replicas
 * and replicas that are not in the ISR. This is based on the assumption that these replicas are likely
 * to join the ISR relatively soon. Since consumers don't rebalance on ISR change, this avoids unnecessary
 * cross-rack traffic for long durations after replicas rejoin the ISR. In the future, we may consider
 * rebalancing when replicas are added or removed to improve consumer rack alignment.
 * </p>
 */
/**
 * RangeAssignor实现了Kafka消费者组的分区分配策略，采用按主题范围分配的方式。
 * 主要特点：
 * 1. 按主题进行分配：对每个主题单独处理，将分区按数字顺序排列，消费者按字典序排列
 * 2. 均衡分配：将每个主题的分区数除以消费者数，得到每个消费者应分配的分区数
 * 3. 处理余数：如果分区数不能被消费者数整除，前面的消费者会多分配一个分区
 * 4. 支持静态成员：通过group.instance.id实现分配的稳定性
 * 5. 支持机架感知：尝试将分区分配给与其副本位于同一机架的消费者
 */
public class RangeAssignor extends AbstractPartitionAssignor {
    // 分配器的名称，用于标识该分配策略
    public static final String RANGE_ASSIGNOR_NAME = "range";
    // 用于对TopicPartition进行排序的比较器
    private static final TopicPartitionComparator PARTITION_COMPARATOR = new TopicPartitionComparator();

    /**
     * 返回分配器的名称
     * @return 返回"range"，表示这是范围分配器
     */
    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    /**
     * 构建每个主题的消费者列表映射
     * @param consumerMetadata 消费者元数据，包含消费者ID和订阅信息
     * @return 返回主题到消费者列表的映射
     * 
     * 实现细节：
     * 1. 创建主题到消费者列表的映射
     * 2. 遍历每个消费者的订阅信息
     * 3. 为每个消费者创建包含其ID、实例ID和机架ID的MemberInfo对象
     * 4. 将消费者信息添加到其订阅的每个主题的消费者列表中
     */
    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        consumerMetadata.forEach((consumerId, subscription) -> {
            MemberInfo memberInfo = new MemberInfo(consumerId, subscription.groupInstanceId(), subscription.rackId());
            subscription.topics().forEach(topic -> put(topicToConsumers, topic, memberInfo));
        });
        return topicToConsumers;
    }

    /**
     * 执行分区分配，将指定的分区分配给订阅的消费者
     * 
     * @param partitionsPerTopic 每个主题的分区信息映射
     * @param subscriptions 消费者的订阅信息映射
     * @return 返回消费者到分区列表的分配结果映射
     * 
     * 实现细节：
     * 1. 准备阶段：
     *    - 构建每个主题的消费者列表映射
     *    - 获取消费者的机架信息
     *    - 为每个非空主题创建分配状态对象
     * 
     * 2. 初始化分配结果：
     *    - 为每个消费者创建空的分区列表
     * 
     * 3. 机架感知分配：
     *    - 检查是否需要机架感知分配
     *    - 如果需要，先执行机架感知分配
     *    - 优先将分区分配给与其副本位于同一机架的消费者
     * 
     * 4. 标准范围分配：
     *    - 对每个主题执行范围分配
     *    - 将剩余未分配的分区按范围分配给消费者
     * 
     * 5. 结果处理：
     *    - 如果使用了机架感知分配，对分配结果进行排序
     *    - 返回最终的分配结果
     */
    @Override
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                              Map<String, Subscription> subscriptions) {
        // 构建每个主题的消费者列表映射
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);
        // 获取消费者的机架信息
        Map<String, String> consumerRacks = consumerRacks(subscriptions);
        // 为每个非空主题创建分配状态对象
        List<TopicAssignmentState> topicAssignmentStates = partitionsPerTopic.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .map(e -> new TopicAssignmentState(e.getKey(), e.getValue(), consumersPerTopic.get(e.getKey()), consumerRacks))
                .collect(Collectors.toList());

        // 初始化分配结果映射
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        subscriptions.keySet().forEach(memberId -> assignment.put(memberId, new ArrayList<>()));

        // 检查是否需要机架感知分配
        boolean useRackAware = topicAssignmentStates.stream().anyMatch(t -> t.needsRackAwareAssignment);
        if (useRackAware)
            // 执行机架感知分配
            assignWithRackMatching(topicAssignmentStates, assignment);

        // 执行标准范围分配
        topicAssignmentStates.forEach(t -> assignRanges(t, (c, tp) -> true, assignment));

        // 如果使用了机架感知分配，对结果进行排序
        if (useRackAware)
            assignment.values().forEach(list -> list.sort(PARTITION_COMPARATOR));
        return assignment;
    }

    // This method is not used, but retained for compatibility with any custom assignors that extend this class.
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        return assignPartitions(partitionInfosWithoutRacks(partitionsPerTopic), subscriptions);
    }

    /**
     * 为指定主题的消费者分配分区范围
     * @param assignmentState 主题分配状态，包含未分配的分区和消费者信息
     * @param mayAssign 判断是否可以将分区分配给消费者的函数
     * @param assignment 最终的分配结果映射
     * 
     * 实现细节：
     * 1. 遍历每个消费者
     * 2. 从未分配的分区中筛选出可以分配给当前消费者的分区
     * 3. 限制分配数量不超过消费者应得的配额
     * 4. 将筛选出的分区分配给消费者
     */
    private void assignRanges(TopicAssignmentState assignmentState,
                              BiFunction<String, TopicPartition, Boolean> mayAssign,
                              Map<String, List<TopicPartition>> assignment) {
        for (String consumer : assignmentState.consumers.keySet()) {
            if (assignmentState.unassignedPartitions.isEmpty())
                break;
            List<TopicPartition> assignablePartitions = assignmentState.unassignedPartitions.stream()
                    .filter(tp -> mayAssign.apply(consumer, tp))
                    .limit(assignmentState.maxAssignable(consumer))
                    .collect(Collectors.toList());
            if (assignablePartitions.isEmpty())
                continue;

            assign(consumer, assignablePartitions, assignmentState, assignment);
        }
    }

    /**
     * 执行机架感知的分区分配
     * @param assignmentStates 所有主题的分配状态集合
     * @param assignment 最终的分配结果映射
     * 
     * 实现细节：
     * 1. 按消费者组对主题状态进行分组
     * 2. 对每组主题，按分区数进行分组
     * 3. 对具有相同分区数的主题组：
     *    - 如果有多个主题，执行协同分区的机架匹配分配
     *    - 如果只有一个主题且需要机架感知，执行普通的机架感知分配
     */
    private void assignWithRackMatching(Collection<TopicAssignmentState> assignmentStates,
                                        Map<String, List<TopicPartition>> assignment) {

        assignmentStates.stream().collect(Collectors.groupingBy(t -> t.consumers)).forEach((consumers, states) ->
            states.stream().collect(Collectors.groupingBy(t -> t.partitionRacks.size())).forEach((numPartitions, coPartitionedStates) -> {
                if (coPartitionedStates.size() > 1)
                    assignCoPartitionedWithRackMatching(consumers, numPartitions, coPartitionedStates, assignment);
                else {
                    TopicAssignmentState state = coPartitionedStates.get(0);
                    if (state.needsRackAwareAssignment)
                        assignRanges(state, state::racksMatch, assignment);
                }
            })
        );
    }

    /**
     * 为具有相同分区数的多个主题执行协同分区的机架感知分配
     * 
     * @param consumers 消费者及其机架信息的映射，使用LinkedHashMap保持消费者顺序
     * @param numPartitions 每个主题的分区数
     * @param assignmentStates 需要协同分配的主题状态集合
     * @param assignment 最终的分配结果映射
     * 
     * 实现细节：
     * 1. 创建一个剩余可分配消费者集合，初始包含所有消费者
     * 2. 按分区号顺序遍历（0到numPartitions-1）：
     *    - 在剩余消费者中寻找符合条件的消费者（机架匹配且未达到分配上限）
     *    - 如果找到匹配的消费者，将所有主题的当前分区号分配给该消费者
     *    - 检查该消费者是否已达到分配上限，如果是则从剩余消费者集合中移除
     * 3. 通过这种方式确保：
     *    - 相同分区号的分区被分配给同一个消费者，实现协同分区
     *    - 优先考虑机架位置匹配的消费者，提高数据本地性
     *    - 在满足上述条件的同时保持分配的均衡性
     */
    private void assignCoPartitionedWithRackMatching(LinkedHashMap<String, Optional<String>> consumers,
                                                     int numPartitions,
                                                     Collection<TopicAssignmentState> assignmentStates,
                                                     Map<String, List<TopicPartition>> assignment) {
        // 创建剩余可分配消费者集合，初始包含所有消费者
        Set<String> remainingConsumers = new LinkedHashSet<>(consumers.keySet());
        
        // 按分区号顺序遍历
        for (int i = 0; i < numPartitions; i++) {
            int p = i;

            // 在剩余消费者中寻找第一个符合条件的消费者：
            // 1. 机架位置与所有主题的当前分区号匹配
            // 2. 在所有主题上都还能分配更多分区
            Optional<String> matchingConsumer = remainingConsumers.stream()
                    .filter(c -> assignmentStates.stream().allMatch(t -> t.racksMatch(c, new TopicPartition(t.topic, p)) && t.maxAssignable(c) > 0))
                    .findFirst();
            
            if (matchingConsumer.isPresent()) {
                String consumer = matchingConsumer.get();
                // 将所有主题的当前分区号分配给找到的消费者
                assignmentStates.forEach(t -> assign(consumer, Collections.singletonList(new TopicPartition(t.topic, p)), t, assignment));

                // 检查消费者是否已达到所有主题的分配上限
                if (assignmentStates.stream().noneMatch(t -> t.maxAssignable(consumer) > 0)) {
                    // 如果达到上限，从剩余消费者集合中移除
                    remainingConsumers.remove(consumer);
                    // 如果没有剩余消费者，提前结束分配
                    if (remainingConsumers.isEmpty())
                        break;
                }
            }
        }
    }

    /**
     * 执行具体的分区分配操作
     * 
     * @param consumer 目标消费者的ID
     * @param partitions 要分配给消费者的分区列表
     * @param assignmentState 主题的分配状态对象
     * @param assignment 全局的分配结果映射
     * 
     * 实现细节：
     * 1. 将指定的分区列表添加到消费者的已分配分区集合中
     * 2. 更新主题分配状态，包括：
     *    - 更新消费者已分配的分区数量
     *    - 从未分配分区集合中移除已分配的分区
     *    - 必要时更新剩余可获得额外分区的消费者数量
     */
    private void assign(String consumer, List<TopicPartition> partitions, TopicAssignmentState assignmentState, Map<String, List<TopicPartition>> assignment) {
        // 将分区添加到消费者的分配结果中
        assignment.get(consumer).addAll(partitions);
        // 更新主题分配状态
        assignmentState.onAssigned(consumer, partitions);
    }

    /**
     * 获取消费者到机架的映射关系
     * @param subscriptions 消费者订阅信息
     * @return 返回消费者ID到机架ID的映射
     * 
     * 实现细节：
     * 1. 创建消费者到机架的映射
     * 2. 遍历所有消费者的订阅信息
     * 3. 如果消费者指定了非空的机架ID，添加到映射中
     */
    private Map<String, String> consumerRacks(Map<String, Subscription> subscriptions) {
        Map<String, String> consumerRacks = new HashMap<>(subscriptions.size());
        subscriptions.forEach((memberId, subscription) ->
                subscription.rackId().filter(r -> !r.isEmpty()).ifPresent(rackId -> consumerRacks.put(memberId, rackId)));
        return consumerRacks;
    }

    /**
     * 主题分配状态内部类，维护单个主题的分配状态信息
     * 包含：
     * 1. 主题的基本信息
     * 2. 消费者列表及其机架信息
     * 3. 分区的机架分布信息
     * 4. 分配进度和配额计算
     */
    private class TopicAssignmentState {
        private final String topic;
        private final LinkedHashMap<String, Optional<String>> consumers;
        private final boolean needsRackAwareAssignment;
        private final Map<TopicPartition, Set<String>> partitionRacks;

        private final Set<TopicPartition> unassignedPartitions;
        private final Map<String, Integer> numAssignedByConsumer;
        private final int numPartitionsPerConsumer;
        private int remainingConsumersWithExtraPartition;

        public TopicAssignmentState(String topic, List<PartitionInfo> partitionInfos, List<MemberInfo> membersOrNull, Map<String, String> consumerRacks) {
            this.topic = topic;
            List<MemberInfo> members = membersOrNull == null ? Collections.emptyList() : membersOrNull;
            Collections.sort(members);
            consumers = members.stream().map(c -> c.memberId)
                    .collect(Collectors.toMap(Function.identity(), c -> Optional.ofNullable(consumerRacks.get(c)), (a, b) -> a, LinkedHashMap::new));

            this.unassignedPartitions = partitionInfos.stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            this.numAssignedByConsumer = consumers.keySet().stream().collect(Collectors.toMap(Function.identity(), c -> 0));
            numPartitionsPerConsumer = consumers.isEmpty() ? 0 : partitionInfos.size() / consumers.size();
            remainingConsumersWithExtraPartition = consumers.isEmpty() ? 0 : partitionInfos.size() % consumers.size();

            Set<String> allConsumerRacks = new HashSet<>();
            Set<String> allPartitionRacks = new HashSet<>();
            members.stream().map(m -> m.memberId).filter(consumerRacks::containsKey)
                    .forEach(memberId -> allConsumerRacks.add(consumerRacks.get(memberId)));
            if (!allConsumerRacks.isEmpty()) {
                partitionRacks = new HashMap<>(partitionInfos.size());
                partitionInfos.forEach(p -> {
                    TopicPartition tp = new TopicPartition(p.topic(), p.partition());
                    Set<String> racks = Arrays.stream(p.replicas())
                            .map(Node::rack)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet());
                    partitionRacks.put(tp, racks);
                    allPartitionRacks.addAll(racks);
                });
            } else {
                partitionRacks = Collections.emptyMap();
            }

            needsRackAwareAssignment = useRackAwareAssignment(allConsumerRacks, allPartitionRacks, partitionRacks);
        }

        boolean racksMatch(String consumer, TopicPartition tp) {
            Optional<String> consumerRack = consumers.get(consumer);
            Set<String> replicaRacks = partitionRacks.get(tp);
            return consumerRack.isEmpty() || (replicaRacks != null && replicaRacks.contains(consumerRack.get()));
        }

        int maxAssignable(String consumer) {
            int maxForConsumer = numPartitionsPerConsumer + (remainingConsumersWithExtraPartition > 0 ? 1 : 0) - numAssignedByConsumer.get(consumer);
            return Math.max(0, maxForConsumer);
        }

        void onAssigned(String consumer, List<TopicPartition> newlyAssignedPartitions) {
            int numAssigned = numAssignedByConsumer.compute(consumer, (c, n) -> n + newlyAssignedPartitions.size());
            if (numAssigned > numPartitionsPerConsumer)
                remainingConsumersWithExtraPartition--;
            unassignedPartitions.removeAll(newlyAssignedPartitions);
        }

        @Override
        public String toString() {
            return "TopicAssignmentState(" +
                    "topic=" + topic +
                    ", consumers=" + consumers +
                    ", partitionRacks=" + partitionRacks +
                    ", unassignedPartitions=" + unassignedPartitions +
                    ")";
        }
    }
}
