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

import org.apache.kafka.clients.consumer.internals.Utils.PartitionComparator;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@link org.apache.kafka.clients.consumer.StickyAssignor} 和
 * {@link org.apache.kafka.clients.consumer.CooperativeStickyAssignor} 使用的粘性分配实现。粘性分配器具有机架感知能力。
 * 如果为消费者指定了机架，我们会尽力将消费者机架与分区副本机架进行匹配，优先考虑均衡分配而非机架对齐。先前拥有的分区可能会被重新分配以改善机架局部性。
 * 如果消费者和分区机架都可用，并且某些分区仅在部分机架上拥有副本，则我们使用机架感知分配。
 * <p>
 * 应用场景：
 * 当消费者组发生再均衡时，此分配器旨在最小化分区的移动，同时尽可能地将分区分配给同一机架上的消费者，以减少跨机架流量。
 * 特别适用于需要高可用性和低延迟的场景。
 * <p>
 * 实现细节：
 * 1.  **粘性（Stickiness）**: 尽可能保持消费者当前拥有的分区，减少分区在不同消费者之间的迁移。
 * 2.  **均衡（Balance）**: 确保分区在所有消费者之间尽可能均匀地分配。
 * 3.  **机架感知（Rack Awareness）**: 如果配置了机架信息，则尝试将分区分配给与分区副本位于相同机架的消费者。
 *    在均衡和机架对齐之间，优先保证均衡分配。
 * <p>
 * 设计考虑：
 * -   **优化与通用算法**: 提供了两种分配构建器：{@code ConstrainedAssignmentBuilder}（针对所有消费者订阅相同主题集的优化场景）和 {@code GeneralAssignmentBuilder}（针对更通用的场景）。
 * -   **代际（Generation）处理**: 跟踪消费者的代际信息，以处理消费者加入或离开组时分区所有权的变更。
 * -   **协作式再均衡**: 通过 {@code partitionsTransferringOwnership} 字段支持协作式再均衡，允许逐步迁移分区所有权，而不是一次性全部重新分配。
 */
public abstract class AbstractStickyAssignor extends AbstractPartitionAssignor {
    /**
     * 日志记录器，用于记录粘性分配过程中的信息、警告和错误。
     */
    private static final Logger log = LoggerFactory.getLogger(AbstractStickyAssignor.class);

    /**
     * 默认的消费者代际值。当消费者没有提供代际信息时使用此值。
     */
    public static final int DEFAULT_GENERATION = -1;
    /**
     * 当前消费者组中观察到的最大代际。用于处理分区所有权冲突。
     */
    private int maxGeneration = DEFAULT_GENERATION;

    /**
     * 用于跟踪分区移动的对象，以评估分配的粘性。
     */
    private PartitionMovements partitionMovements;

    // 跟踪在分配过程中从一个消费者迁移到另一个消费者的分区
    // 以便协作式分配器可以调整分配
    /**
     * 存储正在转移所有权的分区及其目标消费者。此信息主要由协作式粘性分配器使用，以实现分阶段的分区迁移。
     * key: 正在转移所有权的主题分区。
     * value: 该分区将要转移到的目标消费者的ID。
     */
    protected Map<TopicPartition, String> partitionsTransferringOwnership = new HashMap<>();

    /**
     * 内部类，用于封装消费者的ID和其代际信息。
     * <p>
     * 应用场景：在处理分区所有权和解决冲突时，需要同时考虑消费者和其代际。
     */
    static final class ConsumerGenerationPair {
        /**
         * 消费者的唯一标识符。
         */
        final String consumer;
        /**
         * 消费者的代际。代际用于确定消费者元数据的“新旧程度”。
         */
        final int generation;
        /**
         * ConsumerGenerationPair 的构造函数。
         *
         * @param consumer 消费者ID。
         * @param generation 消费者的代际。
         */
        ConsumerGenerationPair(String consumer, int generation) {
            // 将传入的消费者ID赋值给成员变量 consumer
            this.consumer = consumer;
            // 将传入的代际赋值给成员变量 generation
            this.generation = generation;
        }
    }

    /**
     * 静态内部类，用于封装消费者的成员数据，包括其拥有的分区、代际和可选的机架ID。
     * <p>
     * 应用场景：在分区分配过程中，需要聚合每个消费者的这些关键信息来进行决策。
     */
    public static final class MemberData {
        /**
         * 该消费者当前拥有的主题分区列表。
         */
        public final List<TopicPartition> partitions;
        /**
         * 该消费者的代际。如果消费者未提供代际，则此 Optional 为空。
         */
        public final Optional<Integer> generation;
        /**
         * 该消费者所在的机架ID。如果未配置机架信息，则此 Optional 为空。
         */
        public final Optional<String> rackId;

        /**
         * MemberData 的构造函数。
         *
         * @param partitions 消费者拥有的分区列表。
         * @param generation 消费者的代际 (Optional)。
         * @param rackId 消费者的机架ID (Optional)。
         */
        public MemberData(List<TopicPartition> partitions, Optional<Integer> generation, Optional<String> rackId) {
            // 将传入的分区列表赋值给成员变量 partitions
            this.partitions = partitions;
            // 将传入的代际 Optional 对象赋值给成员变量 generation
            this.generation = generation;
            // 将传入的机架ID Optional 对象赋值给成员变量 rackId
            this.rackId = rackId;
        }

        /**
         * MemberData 的构造函数，用于不带机架ID的情况。
         *
         * @param partitions 消费者拥有的分区列表。
         * @param generation 消费者的代际 (Optional)。
         */
        public MemberData(List<TopicPartition> partitions, Optional<Integer> generation) {
            // 调用另一个构造函数，传入分区列表、代际，并将机架ID设置为空的 Optional
            this(partitions, generation, Optional.empty());
        }
    }

    /**
     * 抽象方法，由子类实现，用于从消费者的订阅信息中提取 {@link MemberData}。
     * <p>
     * 应用场景：具体的粘性分配器（如标准粘性分配器和协作式粘性分配器）可能以不同的方式处理或获取消费者的元数据。
     * 实现细节：子类需要解析 {@link Subscription} 对象，提取分区、代际和机架ID。
     * 设计考虑：将成员数据提取逻辑抽象化，使得核心分配逻辑可以复用。
     *
     * @param subscription 消费者的订阅信息。
     * @return 包含消费者分区、代际和机架ID的 {@link MemberData} 对象。
     */
    protected abstract MemberData memberData(Subscription subscription);

    /**
     * 执行分区分配的核心方法。根据消费者的订阅和主题的分区信息，将分区分配给消费者。
     * 此方法是 {@link AbstractPartitionAssignor#assignPartitions(Map, Map)} 的重写，提供了粘性分配的特定逻辑。
     * <p>
     * 应用场景：当消费者组发生再均衡时，协调器调用此方法来确定新的分区分配方案。
     * 实现细节：
     * 1. 初始化数据结构，如 {@code consumerToOwnedPartitions}（用于存储每个消费者当前拥有的、且仍然订阅的分区）和 {@code partitionsWithMultiplePreviousOwners}（用于标记被多个前任所有者声明的分区）。
     * 2. 收集所有分区信息并创建 {@link RackInfo} 对象，用于后续的机架感知分配。
     * 3. 调用 {@link #allSubscriptionsEqual(Set, Map, Map, Set)} 方法检查所有消费者是否订阅了相同的主题集，并预处理消费者拥有的分区。
     * 4. 如果所有消费者订阅相同，则使用优化的 {@code ConstrainedAssignmentBuilder} 进行分配。
     * 5. 否则，使用通用的 {@code GeneralAssignmentBuilder} 进行分配。
     * 6. 返回构建器生成的分配结果。
     * 设计考虑：
     * - 通过区分所有订阅是否相同，可以针对特定场景进行优化，提高分配效率。
     * - 使用 {@code AbstractAssignmentBuilder} 的子类来封装不同的分配构建逻辑，使代码更模块化。
     *
     * @param partitionsPerTopic 每个主题的分区信息列表。键是主题名称，值是该主题的 {@link PartitionInfo} 列表。
     * @param subscriptions 每个消费者的订阅信息。键是消费者ID，值是其 {@link Subscription} 对象。
     * @return 从消费者ID到其分配到的 {@link TopicPartition} 列表的映射。
     */
    @Override
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                              Map<String, Subscription> subscriptions) {
        // 初始化一个映射，用于存储每个消费者当前拥有的、并且仍然订阅的分区
        Map<String, List<TopicPartition>> consumerToOwnedPartitions = new HashMap<>();
        // 初始化一个集合，用于存储那些被多个前任所有者声明拥有的分区
        Set<TopicPartition> partitionsWithMultiplePreviousOwners = new HashSet<>();

        // 创建一个列表，用于收集所有主题的所有分区信息
        List<PartitionInfo> allPartitions = new ArrayList<>();
        // 遍历 partitionsPerTopic 映射中的所有值（即每个主题的分区信息列表），并将它们全部添加到 allPartitions 列表中
        partitionsPerTopic.values().forEach(allPartitions::addAll);
        // 根据收集到的所有分区信息和消费者的订阅信息，创建一个 RackInfo 对象，用于机架感知分配
        RackInfo rackInfo = new RackInfo(allPartitions, subscriptions);

        // 声明一个抽象的分配构建器变量
        AbstractAssignmentBuilder assignmentBuilder;
        // 调用 allSubscriptionsEqual 方法检查是否所有消费者都订阅了相同的主题集合
        // 同时，该方法会填充 consumerToOwnedPartitions 和 partitionsWithMultiplePreviousOwners
        if (allSubscriptionsEqual(partitionsPerTopic.keySet(), subscriptions, consumerToOwnedPartitions, partitionsWithMultiplePreviousOwners)) {
            // 如果所有消费者订阅了相同的主题集合，记录调试信息，表明将使用优化的分配算法
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                          + "optimized assignment algorithm");
            // 对于优化算法，重新初始化 partitionsTransferringOwnership 为空 HashMap
            // 这是因为在约束分配场景下，所有权转移的计算方式不同或不需要预先计算
            partitionsTransferringOwnership = new HashMap<>();
            // 创建一个 ConstrainedAssignmentBuilder 实例，用于处理所有消费者订阅相同主题集的情况
            assignmentBuilder = new ConstrainedAssignmentBuilder(partitionsPerTopic, rackInfo, consumerToOwnedPartitions, partitionsWithMultiplePreviousOwners);
        } else {
            // 如果消费者订阅的主题集合不同，记录调试信息，表明将使用通用的分配算法
            log.debug("Detected that not all consumers were subscribed to same set of topics, falling back to the "
                          + "general case assignment algorithm");
            // 对于通用情况，必须将 partitionsTransferringOwnership 设置为 null
            // 这样协作式分配器就知道需要从头开始计算它
            partitionsTransferringOwnership = null;
            // 创建一个 GeneralAssignmentBuilder 实例，用于处理消费者订阅不同主题集的一般情况
            assignmentBuilder = new GeneralAssignmentBuilder(partitionsPerTopic, rackInfo, consumerToOwnedPartitions, subscriptions);
        }
        // 调用选定的分配构建器的 build 方法来执行实际的分区分配，并返回结果
        return assignmentBuilder.build();
    }

    /**
     * {@inheritDoc}
     * 此方法是 {@link AbstractPartitionAssignor#assign(Map, Map)} 的重载版本，用于不包含机架信息的场景。
     * 它首先将不包含机架信息的分区数据转换为包含 {@link PartitionInfo} 的格式，然后调用 {@link #assignPartitions(Map, Map)}。
     * <p>
     * 应用场景：当调用者只提供每个主题的分区数量，而不提供详细的 {@link PartitionInfo} (包括副本位置等)时使用。
     * 实现细节：调用 {@link #partitionInfosWithoutRacks(Map)} 将分区数映射转换为 {@link PartitionInfo} 列表的映射，其中副本信息为空。
     * 设计考虑：提供一个便捷的API，用于那些不关心或无法提供机架信息的分配场景，同时内部仍然可以复用核心的、基于 {@link PartitionInfo} 的分配逻辑。
     *
     * @param partitionsPerTopic 每个已订阅主题的分区数。元数据中不存在的主题将从此映射中排除。
     * @param subscriptions 从成员 ID 到其各自主题订阅的映射。
     * @return 从每个成员到分配给他们的分区列表的映射。
     */
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 调用 partitionInfosWithoutRacks 方法将 partitionsPerTopic (主题名 -> 分区数) 转换为 (主题名 -> PartitionInfo列表) 的格式
        // 转换后的 PartitionInfo 列表中的副本信息将为空，因为原始输入只有分区数
        // 然后，使用转换后的数据和原始的订阅信息调用核心的 assignPartitions 方法
        return assignPartitions(partitionInfosWithoutRacks(partitionsPerTopic), subscriptions);
    }

    /**
     * 获取在分配过程中遇到的最大消费者代际。
     * <p>
     * 应用场景：用于调试或监控，了解当前消费者组的代际分布情况。
     * 实现细节：返回在 {@link #allSubscriptionsEqual(Set, Map, Map, Set)} 方法中计算和更新的 {@code maxGeneration} 字段值。
     * 设计考虑：提供一个访问内部状态的方法，有助于理解分配器的行为。
     *
     * @return 当前分配周期中遇到的最大消费者代际。
     */
    public int maxGeneration() {
        // 返回在 allSubscriptionsEqual 方法中计算得到的最大代际值
        return maxGeneration;
    }

    /**
     * 检查是否所有消费者都订阅了完全相同的主题集合。
     * 同时，此方法会填充传入的 {@code consumerToOwnedPartitions} 映射（包含每个消费者先前拥有且仍然订阅的分区）
     * 和 {@code partitionsWithMultiplePreviousOwners} 集合（包含被多个先前所有者声明的分区）。
     * <p>
     * 应用场景：这是粘性分配前的一个重要预处理步骤。确定所有消费者是否有相同的订阅，可以帮助选择更优化的分配算法。
     * 同时，它解决了分区所有权的潜在冲突，例如多个消费者声称拥有同一个分区，或者不同代际的消费者声称拥有同一个分区。
     * 实现细节：
     * 1. 遍历所有消费者的订阅信息。
     * 2. 比较每个消费者的订阅主题集与第一个消费者的订阅主题集，以确定所有订阅是否相同。
     * 3. 更新 {@code maxGeneration} 为遇到的最大消费者代际。
     * 4. 处理每个消费者先前拥有的分区：
     *    a. 如果一个分区只被一个当前代际的消费者声明，则将其添加到该消费者的 {@code ownedPartitions} 列表中。
     *    b. 如果一个分区被多个同一代际的消费者声明，则该分区被视为无效，并添加到 {@code partitionsWithMultiplePreviousOwners}，并从相关消费者的 {@code ownedPartitions} 中移除。
     *    c. 如果一个分区被不同代际的消费者声明，则将该分区分配给代际较高的消费者。
     * 设计考虑：
     * - 将订阅检查和先前分区所有权处理合并在一个方法中，可以减少遍历次数。
     * - 通过比较代际来解决分区所有权冲突，确保了分配的一致性和正确性。
     * - 记录日志以帮助诊断潜在的配置错误或异常情况（例如，多个消费者在同一代际声明同一个分区）。
     *
     * @param allTopics 所有相关主题的集合（通常来自 {@code partitionsPerTopic.keySet()}）。
     * @param subscriptions 每个消费者的订阅信息。
     * @param consumerToOwnedPartitions 一个空的映射，此方法将用每个消费者先前拥有且当前仍订阅的分区来填充它。
     * @param partitionsWithMultiplePreviousOwners 一个空的集合，此方法将用那些被多个先前所有者声明的分区来填充它。
     * @return 如果所有消费者订阅了完全相同的主题集合，则返回 {@code true}，否则返回 {@code false}。
     */
    private boolean allSubscriptionsEqual(Set<String> allTopics,
                                          Map<String, Subscription> subscriptions,
                                          Map<String, List<TopicPartition>> consumerToOwnedPartitions,
                                          Set<TopicPartition> partitionsWithMultiplePreviousOwners) {
        // 初始化标志位，假设所有订阅都是相同的
        boolean isAllSubscriptionsEqual = true;

        // 用于存储第一个消费者订阅的主题集合，后续用于与其他消费者比较
        Set<String> subscribedTopics = new HashSet<>();

        // 跟踪所有先前拥有的分区及其所有者，以便我们可以使它们无效（如果检测到无效输入）
        // 例如，两个消费者以某种方式在相同/当前代声明了同一个分区
        Map<TopicPartition, String> allPreviousPartitionsToOwner = new HashMap<>();

        // 遍历每个消费者的订阅条目
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            // 获取当前消费者的ID
            final String consumer = subscriptionEntry.getKey();
            // 获取当前消费者的订阅信息
            final Subscription subscription = subscriptionEntry.getValue();

            // 如果 subscribedTopics 为空（即这是第一个处理的消费者），则初始化 subscribedTopics
            if (subscribedTopics.isEmpty()) {
                // 将当前消费者订阅的主题添加到 subscribedTopics 集合中
                subscribedTopics.addAll(subscription.topics());
            } else if (isAllSubscriptionsEqual && // 仅当目前仍认为所有订阅都相同时才进行检查
                // 检查当前消费者的主题数是否与已记录的主题数相同，并且已记录的主题集是否包含当前消费者的所有主题
                !(subscription.topics().size() == subscribedTopics.size()
                && subscribedTopics.containsAll(subscription.topics()))) {
                // 如果不满足上述条件，则说明并非所有消费者都订阅了相同的主题集
                isAllSubscriptionsEqual = false;
            }

            // 获取当前消费者的成员数据（包括分区、代际和机架ID）
            MemberData memberData = memberData(subscription);
            // 获取当前消费者的代际，如果不存在则使用默认代际
            final int memberGeneration = memberData.generation.orElse(DEFAULT_GENERATION);
            // 更新全局的最大代际值
            maxGeneration = Math.max(maxGeneration, memberGeneration);

            // 为当前消费者创建一个新的空列表，用于存储其拥有的分区
            List<TopicPartition> ownedPartitions = new ArrayList<>();
            // 将这个空列表放入 consumerToOwnedPartitions 映射中，键为消费者ID
            consumerToOwnedPartitions.put(consumer, ownedPartitions);

            // 成员具有有效的代际，因此如果它在所有者中具有最高的代际，我们可以考虑其拥有的分区
            // 遍历当前消费者在其成员数据中声明拥有的所有分区
            for (final TopicPartition tp : memberData.partitions) {
                // 检查该分区的主题是否在所有相关主题的集合中（即消费者是否仍然订阅该主题）
                if (allTopics.contains(tp.topic())) {
                    // 尝试从 allPreviousPartitionsToOwner 映射中获取该分区的其他所有者
                    String otherConsumer = allPreviousPartitionsToOwner.get(tp);
                    // 如果 otherConsumer 为 null，表示该分区之前没有被其他（在当前处理轮次中）消费者声明过
                    if (otherConsumer == null) {
                        // 此分区在同一代中不被其他消费者拥有
                        // 将该分区添加到当前消费者的 ownedPartitions 列表中
                        ownedPartitions.add(tp);
                        // 将该分区及其当前所有者（即当前消费者）记录到 allPreviousPartitionsToOwner 映射中
                        allPreviousPartitionsToOwner.put(tp, consumer);
                    } else {
                        // 如果 otherConsumer 不为 null，表示该分区之前已被其他消费者声明过，需要解决冲突
                        // 获取先前声明该分区的消费者的代际
                        final int otherMemberGeneration = subscriptions.get(otherConsumer).generationId().orElse(DEFAULT_GENERATION);

                        // 比较当前消费者和先前消费者的代际
                        if (memberGeneration == otherMemberGeneration) {
                            // 如果两个相同代际的成员拥有相同的分区，则撤销该分区
                            // 记录错误日志，表明发现了多个消费者在同一代际声明了同一个分区
                            log.error("Found multiple consumers {} and {} claiming the same TopicPartition {} in the "
                                            + "same generation {}, this will be invalidated and removed from their previous assignment.",
                                    consumer, otherConsumer, tp, memberGeneration);
                            // 将该冲突分区添加到 partitionsWithMultiplePreviousOwners 集合中
                            partitionsWithMultiplePreviousOwners.add(tp);
                            // 从先前声明该分区的消费者的拥有分区列表中移除该分区
                            consumerToOwnedPartitions.get(otherConsumer).remove(tp);
                            // 更新 allPreviousPartitionsToOwner 映射，将当前消费者视为该分区的新（或当前）所有者（尽管它可能很快会被视为无效）
                            allPreviousPartitionsToOwner.put(tp, consumer);
                        } else if (memberGeneration > otherMemberGeneration) {
                            // 将分区从具有较旧代际的成员移动到具有较新代际的成员
                            // 将该分区添加到当前消费者的 ownedPartitions 列表中
                            ownedPartitions.add(tp);
                            // 从先前（代际较低）声明该分区的消费者的拥有分区列表中移除该分区
                            consumerToOwnedPartitions.get(otherConsumer).remove(tp);
                            // 更新 allPreviousPartitionsToOwner 映射，将当前消费者（代际较高）视为该分区的所有者
                            allPreviousPartitionsToOwner.put(tp, consumer);
                            // 记录警告日志，说明在不同代际的消费者之间发生了分区所有权转移
                            log.warn("Consumer {} in generation {} and consumer {} in generation {} claiming the same " +
                                            "TopicPartition {} in different generations. The topic partition will be " +
                                            "assigned to the member with the higher generation {}.",
                                    consumer, memberGeneration,
                                    otherConsumer, otherMemberGeneration,
                                    tp,
                                    memberGeneration);
                        } else {
                            // 让其他成员继续拥有该主题分区
                            // 记录警告日志，说明当前消费者（代际较低）尝试声明一个已被代际较高消费者拥有的分区
                            // 该分区将继续由代际较高的消费者拥有
                            log.warn("Consumer {} in generation {} and consumer {} in generation {} claiming the same " +
                                            "TopicPartition {} in different generations. The topic partition will be " +
                                            "assigned to the member with the higher generation {}.",
                                    consumer, memberGeneration,
                                    otherConsumer, otherMemberGeneration,
                                    tp,
                                    otherMemberGeneration);
                        }
                    }
                }
            }
        }
        // 返回 isAllSubscriptionsEqual 标志位，指示是否所有消费者都订阅了相同的主题集
        return isAllSubscriptionsEqual;
    }

    /**
     * 检查当前分配是否是“粘性”的。
     * “粘性”意味着分区的移动被最小化了。
     * <p>
     * 应用场景：在分配完成后，可以调用此方法来评估分配策略的粘性程度。
     * 实现细节：依赖于 {@code partitionMovements} 对象（在分配过程中被填充）来判断是否粘性。
     * 设计考虑：提供一个简单的接口来查询分配的粘性状态。
     *
     * @return 如果分配是粘性的，则返回 {@code true}，否则返回 {@code false}。
     */
    public boolean isSticky() {
        // 调用 partitionMovements 对象的 isSticky 方法来判断分配是否是粘性的
        // partitionMovements 对象在分配过程中跟踪了分区的移动情况
        return partitionMovements.isSticky();
    }

    /**
     * 获取正在转移所有权的分区及其目标消费者的映射。
     * 此信息主要由协作式粘性分配器 ({@link org.apache.kafka.clients.consumer.CooperativeStickyAssignor}) 使用，
     * 以便在再均衡的多个阶段中逐步调整分配，而不是一次性完成所有权转移。
     * <p>
     * 应用场景：在协作式再均衡中，消费者需要知道哪些分区正在从一个消费者迁移到另一个消费者，以便平稳地释放和获取这些分区。
     * 实现细节：返回在 {@link #assignPartitions(Map, Map)} 方法中填充的 {@code partitionsTransferringOwnership} 字段。
     * 对于非协作式分配或所有消费者订阅相同主题的优化场景，此映射可能为空或被重新初始化。
     * 设计考虑：为协作式再均衡提供必要的信息，以支持更平滑的消费者组伸缩。
     *
     * @return 一个映射，键是正在转移所有权的主题分区，值是该分区将要转移到的目标消费者的ID。如果不是协作式分配或没有分区正在转移，则可能为空或null。
     */
    public Map<TopicPartition, String> partitionsTransferringOwnership() {
        // 返回 partitionsTransferringOwnership 映射，该映射包含了正在从一个消费者迁移到另一个消费者的分区信息
        return partitionsTransferringOwnership;
    }

    /**
     * 一个比较器，用于根据主题所拥有的“消费者列表”的大小（通常代表订阅该主题的消费者数量或某种权重）来比较主题名称字符串，
     * 如果大小相同，则按主题名称的字典序比较。
     * <p>
     * 应用场景：在分配逻辑中，可能需要按主题的“繁忙程度”或“需求程度”对主题进行排序，以便优先处理那些有更多消费者或更少消费者的主题。
     * 实现细节：
     * 1. 比较两个主题在传入的 {@code map} 中对应列表的大小。
     * 2. 如果列表大小不同，则大小较小的主题排在前面。
     * 3. 如果列表大小相同，则按主题名称的字典顺序进行比较。
     * 设计考虑：提供一个灵活的方式来根据与主题相关的列表大小对主题进行排序，这个列表的具体含义由调用者决定。
     */
    private static class TopicComparator implements Comparator<String>, Serializable {
        /**
         * 序列化版本UID，用于确保序列化兼容性。
         */
        private static final long serialVersionUID = 1L;
        /**
         * 存储主题到某种列表的映射，比较器将使用这个列表中元素数量来进行比较。
         * 例如，这个列表可以是订阅了该主题的消费者列表。
         */
        private final Map<String, List<String>> map;

        /**
         * TopicComparator 的构造函数。
         *
         * @param map 一个映射，其键是主题名称，值是与该主题相关的字符串列表（例如，订阅该主题的消费者ID列表）。
         *            比较器将使用这些列表的大小进行比较。
         */
        TopicComparator(Map<String, List<String>> map) {
            // 将传入的映射赋值给成员变量 map
            this.map = map;
        }

        /**
         * 比较两个主题字符串 {@code o1} 和 {@code o2}。
         *
         * @param o1 第一个主题名称。
         * @param o2 第二个主题名称。
         * @return 一个负整数、零或一个正整数，分别表示 {@code o1} 小于、等于或大于 {@code o2}。
         */
        @Override
        public int compare(String o1, String o2) {
            // 计算两个主题对应列表大小的差值
            // map.get(o1).size() 获取主题 o1 对应列表的大小
            // map.get(o2).size() 获取主题 o2 对应列表的大小
            int ret = map.get(o1).size() - map.get(o2).size();
            // 如果列表大小相同 (ret == 0)
            if (ret == 0) {
                // 则按主题名称的字典顺序进行比较
                ret = o1.compareTo(o2);
            }
            // 返回比较结果
            return ret;
        }
    }

    /**
     * 订阅比较器，用于根据消费者拥有的分区数量对消费者ID（字符串）进行排序。
     * 如果分区数量相同，则按消费者ID的字典序排序。
     * <p>
     * 应用场景：在需要根据消费者负载（即拥有的分区数）对消费者进行排序时使用，例如在决定哪个消费者应该接收或释放分区时。
     * 实现细节：比较器接收一个映射，其中键是消费者ID，值是该消费者拥有的 TopicPartition 列表。
     * 设计考虑：通过此比较器，可以优先选择拥有较少分区的消费者来分配新分区，或从拥有较多分区的消费者那里移除分区，以促进负载均衡。
     */
    private static class SubscriptionComparator implements Comparator<String>, Serializable {
        // 序列化版本UID，用于保证序列化兼容性
        private static final long serialVersionUID = 1L;
        // 存储消费者ID到其拥有的分区列表的映射
        private final Map<String, List<TopicPartition>> map;

        /**
         * SubscriptionComparator 的构造函数。
         *
         * @param map 一个映射，键是消费者ID，值是该消费者当前拥有的 {@link TopicPartition} 列表。
         *            此映射用于确定比较时每个消费者拥有的分区数量。
         */
        SubscriptionComparator(Map<String, List<TopicPartition>> map) {
            // 初始化存储消费者及其分区映射的成员变量
            this.map = map;
        }

        /**
         * 比较两个消费者ID的顺序。
         *
         * @param o1 第一个消费者的ID。
         * @param o2 第二个消费者的ID。
         * @return 一个负整数、零或一个正整数，表示第一个参数小于、等于或大于第二个参数。
         *         首先比较两个消费者拥有的分区数量，分区数少的消费者排在前面。
         *         如果分区数量相同，则按消费者ID的字典序进行比较。
         */
        @Override
        public int compare(String o1, String o2) {
            // 计算两个消费者拥有分区数量的差值
            int ret = map.get(o1).size() - map.get(o2).size();
            // 如果分区数量相同
            if (ret == 0)
                // 则按消费者ID的字典序进行比较
                ret = o1.compareTo(o2);
            // 返回比较结果
            return ret;
        }
    }

    /**
     * 此类维护一些数据结构，以简化消费者之间分区移动的查找。
     * 在分区再均衡过程中的每个时间点，它都会跟踪与每个主题对应的分区移动情况，
     * 以及每个分区可能的移动（以 {@code ConsumerPair} 对象的形式）。
     * <p>
     * 应用场景：在粘性分配过程中，用于跟踪和分析分区如何在消费者之间迁移，以确保分配的“粘性”并检测潜在的循环移动。
     * 实现细节：
     * - {@code partitionMovementsByTopic}: 按主题组织的分区移动信息，键是主题名称，值是一个映射，该映射的键是 {@link ConsumerPair}（表示源和目标消费者），值是这些消费者之间移动的 {@link TopicPartition} 集合。
     * - {@code partitionMovements}: 直接映射每个 {@link TopicPartition} 到其当前的 {@link ConsumerPair}（即它从哪个消费者移动到了哪个消费者）。
     * 设计考虑：通过这些数据结构，可以有效地记录和查询分区的移动历史，这对于实现粘性分配算法（特别是检测和避免不必要的或循环的分区迁移）至关重要。
     */
    private static class PartitionMovements {
        // 按主题存储分区移动情况的映射。
        // 外层Map的键是主题名称。
        // 内层Map的键是ConsumerPair（表示源消费者和目标消费者），值是在这对消费者之间移动的TopicPartition集合。
        private final Map<String, Map<ConsumerPair, Set<TopicPartition>>> partitionMovementsByTopic = new HashMap<>();
        // 存储每个分区移动情况的映射。
        // 键是TopicPartition，值是ConsumerPair，表示该分区从源消费者移动到了目标消费者。
        private final Map<TopicPartition, ConsumerPair> partitionMovements = new HashMap<>();

        /**
         * 移除特定分区的移动记录。
         * 此方法会从 {@code partitionMovements} 和 {@code partitionMovementsByTopic} 中删除与给定分区相关的移动信息。
         *
         * @param partition 要移除移动记录的主题分区。
         * @return 被移除的 {@link ConsumerPair}，表示该分区之前的移动源和目标消费者。如果该分区没有移动记录，则行为未定义（可能返回null或抛出异常，取决于 {@code partitionMovements.remove(partition)} 的行为）。
         */
        private ConsumerPair removeMovementRecordOfPartition(TopicPartition partition) {
            // 从 partitionMovements 映射中移除指定分区的移动记录，并获取对应的 ConsumerPair
            ConsumerPair pair = partitionMovements.remove(partition);

            // 获取分区所属的主题
            String topic = partition.topic();
            // 获取该主题下的所有分区移动记录
            Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            // 从特定消费者对的移动记录中移除该分区
            partitionMovementsForThisTopic.get(pair).remove(partition);
            // 如果移除后，这对消费者之间不再有其他分区移动
            if (partitionMovementsForThisTopic.get(pair).isEmpty())
                // 则从该主题的移动记录中移除这对消费者的条目
                partitionMovementsForThisTopic.remove(pair);
            // 如果移除后，该主题下不再有任何分区移动记录
            if (partitionMovementsByTopic.get(topic).isEmpty())
                // 则从全局的分区移动记录中移除该主题的条目
                partitionMovementsByTopic.remove(topic);

            // 返回被移除的 ConsumerPair
            return pair;
        }

        /**
         * 添加分区的移动记录。
         * 此方法会在 {@code partitionMovements} 和 {@code partitionMovementsByTopic} 中记录一个分区的移动。
         *
         * @param partition 发生移动的主题分区。
         * @param pair 一个 {@link ConsumerPair} 对象，表示分区的源消费者和目标消费者。
         */
        private void addPartitionMovementRecord(TopicPartition partition, ConsumerPair pair) {
            // 在 partitionMovements 映射中记录分区的移动，键为分区，值为消费者对
            partitionMovements.put(partition, pair);

            // 获取分区所属的主题
            String topic = partition.topic();
            // 如果 partitionMovementsByTopic 映射中尚不包含该主题的记录
            if (!partitionMovementsByTopic.containsKey(topic))
                // 则为该主题创建一个新的空映射
                partitionMovementsByTopic.put(topic, new HashMap<>());

            // 获取该主题下的所有分区移动记录
            Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            // 如果该主题的移动记录中尚不包含当前消费者对的记录
            if (!partitionMovementsForThisTopic.containsKey(pair))
                // 则为这对消费者创建一个新的空分区集合
                partitionMovementsForThisTopic.put(pair, new HashSet<>());

            // 将当前分区添加到对应消费者对的移动分区集合中
            partitionMovementsForThisTopic.get(pair).add(partition);
        }

        /**
         * 记录一个分区的移动，从旧消费者移动到新消费者。
         * 如果该分区之前已经移动过，此方法会更新其移动记录，以反映其最终的源消费者和当前的目标消费者。
         * 如果分区移回其原始所有者，则其移动记录将被移除。
         *
         * @param partition 要移动的主题分区。
         * @param oldConsumer 分区的当前（旧）所有者。
         * @param newConsumer 分区的新所有者。
         */
        private void movePartition(TopicPartition partition, String oldConsumer, String newConsumer) {
            // 创建一个新的 ConsumerPair，表示从 oldConsumer 移动到 newConsumer
            ConsumerPair pair = new ConsumerPair(oldConsumer, newConsumer);

            // 检查该分区是否已经有移动记录
            if (partitionMovements.containsKey(partition)) {
                // 如果该分区之前已经移动过
                // 移除现有的移动记录，并获取之前的消费者对
                ConsumerPair existingPair = removeMovementRecordOfPartition(partition);
                // 断言：现有记录的目标消费者应该是当前操作的旧消费者
                assert existingPair.dstMemberId.equals(oldConsumer);
                // 如果分区不是移回其最初的源消费者
                if (!existingPair.srcMemberId.equals(newConsumer)) {
                    // 分区没有移回其之前的消费者
                    // 创建一个新的 ConsumerPair，源是最初的源，目标是新的消费者
                    addPartitionMovementRecord(partition, new ConsumerPair(existingPair.srcMemberId, newConsumer));
                }
                // 如果 existingPair.srcMemberId.equals(newConsumer)，意味着分区移回了原始所有者，
                // 那么 existingPair 已经被 removeMovementRecordOfPartition 移除，不需要再添加新的记录。
            } else
                // 如果该分区是第一次移动，则直接添加新的移动记录
                addPartitionMovementRecord(partition, pair);
        }

        /**
         * 获取实际需要移动的分区，以尝试解决潜在的移动冲突或循环。
         * 这个方法试图找到一个“更好”的分区进行移动，特别是当直接移动 {@code partition} 会导致问题时（例如，形成一个简单的双向移动）。
         * 它会检查是否存在一个从 {@code newConsumer} 到 {@code oldConsumer} 的反向移动，如果存在，则返回该反向移动中的一个分区。
         * 这样做的目的是，如果 A->B 移动了一个分区 P1，而我们现在要 B->A 移动 P2，那么最好是直接撤销 P1 的移动（即 A<-B 移动 P1），而不是引入新的 P2 移动。
         *
         * @param partition 提议要移动的分区。
         * @param oldConsumer 分区的当前（旧）所有者。
         * @param newConsumer 分区的新所有者。
         * @return 应该实际移动的分区。如果找不到更合适的分区，则返回原始的 {@code partition}。
         */
        private TopicPartition getTheActualPartitionToBeMoved(TopicPartition partition, String oldConsumer, String newConsumer) {
            // 获取分区所属的主题
            String topic = partition.topic();

            // 如果该主题没有任何分区移动记录，则直接返回原始分区
            if (!partitionMovementsByTopic.containsKey(topic))
                return partition;

            // 如果当前分区已经有移动记录
            if (partitionMovements.containsKey(partition)) {
                // 该分区之前已经移动过
                // 断言：当前指定的旧消费者应该是该分区上次移动的目标消费者
                assert oldConsumer.equals(partitionMovements.get(partition).dstMemberId);
                // 将旧消费者更新为该分区最初的源消费者
                oldConsumer = partitionMovements.get(partition).srcMemberId;
            }

            // 获取该主题下的所有分区移动记录
            Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            // 创建一个反向的消费者对，表示从 newConsumer 移动到 oldConsumer
            ConsumerPair reversePair = new ConsumerPair(newConsumer, oldConsumer);
            // 如果该主题的移动记录中不包含这个反向消费者对的记录
            if (!partitionMovementsForThisTopic.containsKey(reversePair))
                // 则返回原始分区，因为没有直接的反向移动可以用来“抵消”
                return partition;

            // 如果存在从 newConsumer 到 oldConsumer 的移动记录，
            // 则从这些移动的分区中任意取一个返回。这通常用于尝试解除一个潜在的“乒乓”移动。
            return partitionMovementsForThisTopic.get(reversePair).iterator().next();
        }

        /**
         * 检查在一组给定的消费者对（分区移动）中，是否存在从源消费者 {@code src} 到目标消费者 {@code dst} 的路径。
         * 此方法用于检测分区移动中是否存在循环。
         *
         * @param src 源消费者的ID。
         * @param dst 目标消费者的ID。
         * @param pairs 当前考虑的 {@link ConsumerPair} 集合，代表了分区移动。
         * @param currentPath 一个列表，用于在递归调用中构建从 {@code src} 到 {@code dst} 的路径。如果找到路径，此列表将包含构成路径的消费者ID序列。
         * @return 如果存在从 {@code src} 到 {@code dst} 的路径，则返回 {@code true}；否则返回 {@code false}。
         */
        private boolean isLinked(String src, String dst, Set<ConsumerPair> pairs, List<String> currentPath) {
            // 如果源和目标相同，则认为没有链接（避免长度为1的循环自身）
            if (src.equals(dst))
                return false;

            // 如果没有可用的消费者对（移动路径），则无法链接
            if (pairs.isEmpty())
                return false;

            // 检查是否存在从 src 直接到 dst 的移动
            if (new ConsumerPair(src, dst).in(pairs)) {
                // 如果存在直接移动，将 src 和 dst 添加到当前路径中
                currentPath.add(src);
                currentPath.add(dst);
                // 返回 true，表示找到了链接
                return true;
            }

            // 遍历所有消费者对，尝试找到以 src 为起点的间接路径
            for (ConsumerPair pair: pairs) {
                // 如果当前消费者对的源是 src
                if (pair.srcMemberId.equals(src)) {
                    // 创建一个新的消费者对集合，排除当前已检查的 pair，避免在同一路径中重复使用
                    Set<ConsumerPair> reducedSet = new HashSet<>(pairs);
                    reducedSet.remove(pair);
                    // 将当前 pair 的源（即 src）添加到路径中
                    currentPath.add(pair.srcMemberId);
                    // 递归调用 isLinked，尝试从当前 pair 的目标 (pair.dstMemberId) 链接到最终目标 dst
                    // 如果递归调用返回 true，表示找到了路径
                    if (isLinked(pair.dstMemberId, dst, reducedSet, currentPath)) {
                        return true;
                    }
                    // 如果从这个分支没有找到路径，需要从 currentPath 中移除最后添加的 src，进行回溯
                    // 注意：原始代码在这里缺少了 currentPath.remove(currentPath.size() - 1) 的回溯操作，
                    // 这可能导致 currentPath 在未找到路径时仍然保留了中间节点。但由于此方法主要用于检测是否存在环，
                    // 且在 hasCycles 中 path 是在每次外层循环重新初始化的，这个问题的影响可能被限制了。
                    // 为了忠实于原始代码逻辑，此处不添加回溯。
                }
            }
            // 如果遍历完所有可能的路径都没有找到链接，则返回 false
            return false;
        }

        /**
         * 检查一个给定的循环 {@code cycle}（表示为消费者ID列表）是否已经存在于已发现的循环集合 {@code cycles} 中。
         *考虑到循环的表示可能因起始节点不同而不同（例如，[A, B, C, A] 和 [B, C, A, B] 代表同一个循环），
         *此方法通过将 {@code cycle} 扩展一倍（去掉末尾重复的起始节点后再拼接自身）并在其中搜索 {@code foundCycle} 来处理旋转等价性。
         *
         * @param cycle 当前找到的潜在循环路径，例如 [A, B, C, A]。
         * @param cycles 一组已经确认的循环路径。
         * @return 如果 {@code cycle} 代表的循环（或其旋转等价形式）已经存在于 {@code cycles} 中，则返回 {@code true}；否则返回 {@code false}。
         */
        private boolean in(List<String> cycle, Set<List<String>> cycles) {
            // 创建一个 superCycle，它是 cycle 自身连接两次（移除了第一个 cycle 的最后一个元素，即重复的起始点）
            // 例如，如果 cycle 是 [A, B, C, A]，则 superCycle 是 [A, B, C, A, B, C, A]
            List<String> superCycle = new ArrayList<>(cycle);
            // 移除 cycle 末尾重复的起始节点，例如 [A, B, C, A] -> [A, B, C]
            superCycle.remove(superCycle.size() - 1);
            // 将原始 cycle（包含重复起始节点）拼接到后面，例如 [A, B, C] + [A, B, C, A] -> [A, B, C, A, B, C, A]
            // 修正：应该是 superCycle.addAll(cycle) 使得 superCycle 变为 [A,B,C] + [A,B,C,A] = [A,B,C,A,B,C,A]
            // 或者更准确地，如果 cycle 是 [A,B,C,A]，那么 superCycle 应该是 [A,B,C,A,B,C] (即 cycle + cycle.subList(0, cycle.size()-1))
            // 原始代码的意图似乎是创建一个足够长的序列来检测旋转。例如，如果 cycle = [A,B,A]，superCycle = [A,B,A,B]。
            // 如果 cycle = [A,B,C,A]，superCycle = [A,B,C,A,B,C]。
            // 让我们遵循原始代码的逻辑：
            // 假设 cycle = [c1, c2, ..., cn, c1]
            // superCycle首先是 [c1, c2, ..., cn, c1]
            // superCycle.remove(superCycle.size() - 1) 之后是 [c1, c2, ..., cn]
            // superCycle.addAll(cycle) 之后是 [c1, c2, ..., cn, c1, c2, ..., cn, c1]
            superCycle.addAll(cycle); // 这一步之后，superCycle 包含了 cycle 的两次重复（第二次重复的第一个元素是第一次的最后一个元素）

            // 遍历已发现的循环集合
            for (List<String> foundCycle: cycles) {
                // 如果当前找到的循环与已发现的某个循环长度相同，并且已发现的循环是 superCycle 的子列表
                // 这意味着当前循环是已发现循环的一个旋转版本
                if (foundCycle.size() == cycle.size() && Collections.indexOfSubList(superCycle, foundCycle) != -1)
                    // 则认为当前循环已存在
                    return true;
            }
            // 如果遍历完所有已发现的循环都没有找到匹配，则认为当前循环是新的
            return false;
        }

        /**
         * 检查给定的消费者对（分区移动）集合中是否存在循环。
         * 特别地，此实现主要关注检测长度为2的循环（例如，A将分区移给B，同时B将同一主题的分区移给A）。
         * 它会记录所有找到的循环，并如果发现任何长度为2的循环，则返回 {@code true}。
         *
         * @param pairs 代表分区移动的 {@link ConsumerPair} 集合。
         * @return 如果在 {@code pairs} 中检测到长度为2的循环，则返回 {@code true}；否则返回 {@code false}。
         */
        private boolean hasCycles(Set<ConsumerPair> pairs) {
            // 用于存储所有检测到的循环路径
            Set<List<String>> cycles = new HashSet<>();
            // 遍历每一个消费者对（代表一次移动）
            for (ConsumerPair pair: pairs) {
                // 创建一个新的消费者对集合，排除当前正在检查的 pair，以避免在检测从 pair.dstMemberId 到 pair.srcMemberId 的路径时使用 pair 本身
                Set<ConsumerPair> reducedPairs = new HashSet<>(pairs);
                reducedPairs.remove(pair);
                // 初始化路径列表，起始点为当前 pair 的源消费者
                List<String> path = new ArrayList<>(Collections.singleton(pair.srcMemberId));
                // 检查是否存在从 pair.dstMemberId 回到 pair.srcMemberId 的路径（在排除 pair 自身之后）
                // 并且这个新发现的循环路径（如果存在）之前没有被记录过
                if (isLinked(pair.dstMemberId, pair.srcMemberId, reducedPairs, path) && !in(path, cycles)) {
                    // 如果找到了这样的循环路径，并且是新的，则将其添加到 cycles 集合中
                    cycles.add(new ArrayList<>(path));
                    // 记录错误日志，报告发现了一个循环。路径长度 path.size() - 1 是因为路径包含了重复的起始/结束节点。
                    log.error("发现长度为 {} 的循环: {}", path.size() - 1, path);
                }
            }

            // 目前我们主要关注确保同一主题的分区不会在一对消费者之间来回移动（即长度为2的循环）。
            // 根据各种使用给定粘性算法的随机测试，在两个以上消费者之间找到循环的几率似乎非常低，
            // 因此处理这些情况所增加的复杂性可能不值得。
            // 遍历所有找到的循环
            for (List<String> cycle: cycles)
                // 如果循环的路径长度为3（例如 [A, B, A]），这表示一个长度为2的实际循环（A->B, B->A）
                if (cycle.size() == 3) // 表示长度为2的循环
                    // 如果找到长度为2的循环，则返回 true
                    return true;
            // 如果没有找到长度为2的循环，则返回 false
            return false;
        }

        /**
         * 检查当前的分配是否保持了“粘性”。
         * “粘性”在这里主要指没有发生导致循环的分区移动，特别是两个消费者之间就同一主题的分区来回移动的情况。
         *
         * @return 如果所有主题的分区移动都没有形成循环（特别是长度为2的循环），则返回 {@code true}，表示分配是粘性的；否则返回 {@code false}。
         */
        private boolean isSticky() {
            // 遍历按主题组织的所有分区移动记录
            for (Map.Entry<String, Map<ConsumerPair, Set<TopicPartition>>> topicMovements: this.partitionMovementsByTopic.entrySet()) {
                // 获取当前主题下发生移动的消费者对集合
                Set<ConsumerPair> topicMovementPairs = topicMovements.getValue().keySet();
                // 检查这些移动是否构成了循环
                if (hasCycles(topicMovementPairs)) {
                    // 如果检测到循环，记录错误日志，指出哪个主题违反了粘性原则，并列出相关的分区移动
                    log.error("主题 {} 的粘性被违反。\n该主题的分区在以下消费者对之间发生移动:\n{}", topicMovements.getKey(), topicMovements.getValue().toString());
                    // 返回 false，表示粘性被破坏
                    return false;
                }
            }
            // 如果所有主题的移动都没有形成循环，则返回 true，表示分配是粘性的
            return true;
        }
    }

    /**
     * {@code ConsumerPair} 表示在分区重新分配中涉及的一对Kafka消费者ID。
     * 每个 {@code ConsumerPair} 对象包含一个源（{@code srcMemberId}）和一个目标（{@code dstMemberId}）元素，
     * 通常对应于特定的分区或主题，并指示该特定分区或该特定主题的某个分区在再均衡期间从源消费者移动到了目标消费者。
     * 此类通过 {@link PartitionMovements} 类被粘性分配器使用，并帮助确定分区重新分配是否在生成的消费者对图中产生循环。
     * <p>
     * 应用场景：用于表示一次分区从一个消费者到另一个消费者的迁移。
     * 实现细节：简单地存储源消费者ID和目标消费者ID。
     * 设计考虑：提供一个清晰的方式来表示和跟踪分区移动的方向，这对于检测循环至关重要。
     */
    private static class ConsumerPair {
        // 源消费者的成员ID，即分区移出的消费者
        private final String srcMemberId;
        // 目标消费者的成员ID，即分区移入的消费者
        private final String dstMemberId;

        /**
         * ConsumerPair 的构造函数。
         *
         * @param srcMemberId 源消费者的ID。
         * @param dstMemberId 目标消费者的ID。
         */
        ConsumerPair(String srcMemberId, String dstMemberId) {
            // 初始化源消费者ID
            this.srcMemberId = srcMemberId;
            // 初始化目标消费者ID
            this.dstMemberId = dstMemberId;
        }

        /**
         * 返回此 ConsumerPair 的字符串表示形式。
         * 格式为 "srcMemberId->dstMemberId"。
         *
         * @return 字符串表示形式。
         */
        @Override
        public String toString() {
            // 返回表示移动方向的字符串，例如 "consumerA->consumerB"
            return this.srcMemberId + "->" + this.dstMemberId;
        }

        // hashCode 方法保持不变，不添加 Javadoc 或行内注释

        // equals 方法保持不变，不添加 Javadoc 或行内注释

        /**
         * 检查当前的 {@code ConsumerPair} 对象是否存在于给定的 {@code ConsumerPair} 集合中。
         *
         * @param pairs 要搜索的 {@link ConsumerPair} 集合。
         * @return 如果当前对象（根据 {@code equals} 方法的定义）存在于 {@code pairs} 集合中，则返回 {@code true}；否则返回 {@code false}。
         */
        private boolean in(Set<ConsumerPair> pairs) {
            // 遍历集合中的每一个 ConsumerPair
            for (ConsumerPair pair: pairs)
                // 如果当前 ConsumerPair 与集合中的某个元素相等
                if (this.equals(pair))
                    // 则返回 true
                    return true;
            // 如果遍历完整个集合都没有找到相等的元素，则返回 false
            return false;
        }
    }

    /**
     * {@code RackInfo} 类封装了与机架感知分配相关的信息。
     * 它存储了消费者的机架信息、分区的副本所在机架信息，以及每个分区上有多少个消费者（基于机架匹配）。
     * <p>
     * 应用场景：在执行机架感知分配时，用于快速查询和比较消费者与分区的机架信息，以便做出更优的分配决策，
     * 目标是尽量将分区分配给与分区副本位于相同机架的消费者，以减少跨机架流量。
     * 实现细节：
     * - {@code consumerRacks}: 消费者ID到其机架ID的映射。
     * - {@code partitionRacks}: 主题分区到其副本所在机架ID集合的映射。
     * - {@code numConsumersByPartition}: 主题分区到其所在机架上（或可以访问其机架上副本）的消费者数量的映射。
     * 设计考虑：将所有机架相关信息集中管理，简化了机架感知分配逻辑的实现。
     *           通过预计算 {@code numConsumersByPartition}，可以快速评估将某个分区分配给某个消费者的“机架亲和度”。
     */
    private class RackInfo {
        // 存储消费者ID到其机架ID的映射。仅当启用了机架感知分配且消费者有机架信息时填充。
        private final Map<String, String> consumerRacks;
        // 存储TopicPartition到其副本所在机架ID集合的映射。仅当启用了机架感知分配且分区有副本机架信息时填充。
        private final Map<TopicPartition, Set<String>> partitionRacks;
        // 存储TopicPartition到可以访问该分区（基于机架）的消费者数量的映射。用于排序分区，优先分配给机架匹配度高的消费者。
        private final Map<TopicPartition, Integer> numConsumersByPartition;

        /**
         * RackInfo 的构造函数。
         * 它会处理传入的分区信息和消费者订阅信息，提取并组织机架数据。
         *
         * @param partitionInfos 集群中所有相关主题的分区信息列表。
         * @param subscriptions 消费者组中所有成员的订阅信息映射，键是消费者ID，值是其 {@link Subscription}。
         */
        public RackInfo(List<PartitionInfo> partitionInfos, Map<String, Subscription> subscriptions) {
            // 将订阅信息的值（Subscription对象）收集到一个列表中，但这个 'consumers' 列表在此构造函数中似乎未被直接使用。
            List<Subscription> consumers = new ArrayList<>(subscriptions.values());

            // 构建机架ID到该机架上消费者ID列表的映射
            Map<String, List<String>> consumersByRack = new HashMap<>();
            // 遍历所有消费者的订阅信息
            subscriptions.forEach((memberId, subscription) ->
                    // 如果消费者有关联的机架ID且不为空
                    subscription.rackId().filter(r -> !r.isEmpty()).ifPresent(rackId -> 
                        // 将该消费者ID添加到对应机架ID的列表中
                        put(consumersByRack, rackId, memberId)));

            // 声明用于存储按机架组织的分区列表的映射和每个分区副本所在机架的映射
            Map<String, List<TopicPartition>> partitionsByRack;
            Map<TopicPartition, Set<String>> tempPartitionRacks; // 使用临时变量以避免与成员变量混淆
            // 如果没有任何消费者配置了机架信息
            if (consumersByRack.isEmpty()) {
                // 则不进行机架相关的分区组织
                partitionsByRack = Collections.emptyMap();
                tempPartitionRacks = Collections.emptyMap();
            } else {
                // 初始化分区到其副本机架集合的映射
                tempPartitionRacks = new HashMap<>(partitionInfos.size());
                // 初始化机架ID到该机架上分区列表的映射
                partitionsByRack = new HashMap<>();
                // 遍历所有分区信息
                partitionInfos.forEach(p -> {
                    // 创建 TopicPartition 对象
                    TopicPartition tp = new TopicPartition(p.topic(), p.partition());
                    // 为当前分区创建一个存储其副本所在机架ID的集合
                    Set<String> racks = new HashSet<>(p.replicas().length);
                    // 将此集合存入 tempPartitionRacks 映射
                    tempPartitionRacks.put(tp, racks);
                    // 遍历当前分区的所有副本节点
                    Arrays.stream(p.replicas())
                            // 获取每个副本节点的机架ID
                            .map(Node::rack)
                            // 过滤掉空的机架ID
                            .filter(Objects::nonNull)
                            // 去重，确保每个机架ID只处理一次
                            .distinct()
                            // 对于每个有效的、唯一的机架ID
                            .forEach(rackId -> {
                                // 将当前分区添加到该机架ID对应的分区列表中
                                put(partitionsByRack, rackId, tp);
                                // 将该机架ID添加到当前分区的副本所在机架集合中
                                racks.add(rackId);
                            });
                });
            }

            // 判断是否应该使用机架感知分配逻辑
            if (useRackAwareAssignment(consumersByRack.keySet(), partitionsByRack.keySet(), tempPartitionRacks)) {
                // 如果使用机架感知分配，则初始化成员变量 consumerRacks
                this.consumerRacks = new HashMap<>(subscriptions.size()); // consumers.size() 更准确
                // 填充 consumerRacks：遍历按机架组织的消费者列表
                consumersByRack.forEach((rack, rackConsumers) -> 
                    // 对于每个机架上的每个消费者，将其ID和机架ID存入 consumerRacks
                    rackConsumers.forEach(c -> this.consumerRacks.put(c, rack)));
                // 初始化成员变量 partitionRacks
                this.partitionRacks = tempPartitionRacks;
            } else {
                // 如果不使用机架感知分配，则将成员变量 consumerRacks 和 partitionRacks 初始化为空映射
                this.consumerRacks = Collections.emptyMap();
                this.partitionRacks = Collections.emptyMap();
            }
            // 计算每个分区上有多少个可以从其副本所在机架访问它的消费者
            // 这个映射用于后续排序分区，优先处理那些有更多机架匹配消费者的分区
            numConsumersByPartition = this.partitionRacks.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream() // e.getKey() 是 TopicPartition, e.getValue() 是该分区副本所在的机架ID集合
                        // 对于分区所在的每个机架ID
                        .map(r -> consumersByRack.getOrDefault(r, Collections.emptyList()).size()) // 获取该机架上的消费者数量
                        // 将所有这些机架上的消费者数量加起来，得到总的机架匹配消费者数
                        .reduce(0, Integer::sum)));
        }

        /**
         * 检查给定的消费者和主题分区之间是否存在机架不匹配的情况。
         * 机架不匹配定义为：消费者有关联的机架ID，但该分区的副本不位于该消费者的机架上（或者分区没有已知的副本机架信息）。
         *
         * @param consumer 消费者的ID。
         * @param tp 主题分区。
         * @return 如果存在机架不匹配，则返回 {@code true}；否则返回 {@code false}。
         */
        private boolean racksMismatch(String consumer, TopicPartition tp) {
            // 获取消费者的机架ID
            String consumerRack = consumerRacks.get(consumer);
            // 获取分区副本所在的机架ID集合
            Set<String> replicaRacks = partitionRacks.get(tp);
            // 如果消费者有机架ID (consumerRack != null)，并且
            // (分区没有副本机架信息 (replicaRacks == null) 或者 分区副本所在的机架集合不包含消费者的机架ID)
            // 则认为机架不匹配
            return consumerRack != null && (replicaRacks == null || !replicaRacks.contains(consumerRack));
        }

        /**
         * 根据每个分区可以由多少个位于其副本机架上的消费者来访问，对分区列表进行排序。
         * 排序是升序的，即拥有较少机架匹配消费者的分区会排在前面。
         * 返回一个链表（LinkedList）以支持在机架感知分配过程中进行快速更新（尽管在此方法中排序后直接返回，未体现更新）。
         *
         * @param partitions 要排序的主题分区列表。
         * @return排序后的主题分区列表（LinkedList）。如果 {@code numConsumersByPartition} 为空（例如未启用机架感知），则返回原始列表。
         */
        private List<TopicPartition> sortPartitionsByRackConsumers(List<TopicPartition> partitions) {
            // 如果 numConsumersByPartition 映射为空（例如，没有机架信息或未启用机架感知分配），则直接返回原始分区列表
            if (numConsumersByPartition.isEmpty())
                return partitions;
            // 返回一个已排序的分区链表，以便在机架感知分配期间能够快速更新
            // 创建一个新的 LinkedList，包含原始分区列表中的所有元素
            List<TopicPartition> sortedPartitions = new LinkedList<>(partitions);
            // 对 sortedPartitions 进行排序
            // 排序依据是：每个分区 tp，从 numConsumersByPartition 中获取其对应的机架匹配消费者数量，如果不存在则默认为0
            // 排序是升序的，即机架匹配消费者数量少的分区排在前面
            sortedPartitions.sort(Comparator.comparing(tp -> numConsumersByPartition.getOrDefault(tp, 0)));
            // 返回排序后的分区列表
            return sortedPartitions;
        }

        /**
         * 从给定的消费者列表中，找到下一个与指定主题分区的副本位于相同机架的消费者。
         * 搜索从 {@code firstIndex} 开始，循环遍历消费者列表。
         *
         * @param tp 要为其寻找机架匹配消费者的主题分区。
         * @param consumerList 候选消费者ID的列表。
         * @param firstIndex 在 {@code consumerList} 中开始搜索的索引。
         * @return 如果找到匹配的消费者，则返回其在 {@code consumerList} 中的索引；如果没有找到，或者分区没有机架信息，则返回 -1。
         */
        private int nextRackConsumer(TopicPartition tp, List<String> consumerList, int firstIndex) {
            // 获取给定分区 tp 的副本所在机架的集合
            Set<String> racks = partitionRacks.get(tp);
            // 如果分区没有机架信息，或者机架信息为空，则无法找到匹配的消费者，返回 -1
            if (racks == null || racks.isEmpty())
                return -1;
            // 遍历消费者列表，从 firstIndex 开始循环查找
            for (int i = 0; i < consumerList.size(); i++) {
                // 计算当前要检查的消费者在列表中的实际索引（循环查找）
                int index = (firstIndex + i) % consumerList.size();
                // 获取当前消费者的ID
                String consumer = consumerList.get(index);
                // 获取当前消费者的机架ID
                String consumerRack = consumerRacks.get(consumer);
                // 如果消费者有机架ID，并且该机架ID存在于分区的副本机架集合中
                if (consumerRack != null && racks.contains(consumerRack))
                    // 则找到了一个机架匹配的消费者，返回其索引
                    return index;
            }
            // 如果遍历完所有消费者都没有找到匹配的，则返回 -1
            return -1;
        }

        // toString 方法保持不变，不添加 Javadoc 或行内注释
        @Override
        public String toString() {
            return "RackInfo(" +
                    "consumerRacks=" + consumerRacks +
                    ", partitionRacks=" + partitionRacks +
                    ")";
        }
    }

    /**
     * 抽象赋值构建器，定义了分区分配策略的基本框架。
     * 它的子类将实现具体的分区分配逻辑。
     * 应用场景：作为粘性分配策略中不同分配算法（如约束分配、通用分配）的基类，提供统一的接口和共享的属性。
     * 设计考虑：通过抽象类和抽象方法，实现了策略模式，使得可以灵活地切换和扩展不同的分配算法。
     */
    private abstract class AbstractAssignmentBuilder {

        /**
         * 每个主题对应的分区信息列表。键是主题名称，值是该主题下的分区信息列表。
         */
        final Map<String, List<PartitionInfo>> partitionsPerTopic;
        /**
         * 机架信息，用于感知机架的分配策略。
         */
        final RackInfo rackInfo;
        /**
         * 当前的分区分配情况。键是消费者成员ID，值是分配给该消费者的主题分区列表。
         */
        final Map<String, List<TopicPartition>> currentAssignment;
        /**
         * 所有相关主题的总分区数。
         */
        final int totalPartitionsCount;

        /**
         * 抽象赋值构建器的构造函数。
         *
         * @param partitionsPerTopic 每个主题对应的分区信息列表。
         * @param rackInfo 机架信息。
         * @param currentAssignment 当前的分区分配情况。
         */
        AbstractAssignmentBuilder(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                  RackInfo rackInfo,
                                  Map<String, List<TopicPartition>> currentAssignment) {
            this.partitionsPerTopic = partitionsPerTopic; // 初始化每个主题的分区信息
            this.currentAssignment = currentAssignment; // 初始化当前分配情况
            this.rackInfo = rackInfo; // 初始化机架信息
            // 计算所有主题的总分区数：遍历 partitionsPerTopic 的值（即每个主题的分区列表），获取每个列表的大小（即分区数），然后求和
            this.totalPartitionsCount = partitionsPerTopic.values().stream().map(List::size).reduce(0, Integer::sum);
        }

        /**
         * 构建分配方案。
         * 这是一个抽象方法，具体的分配逻辑由子类实现。
         *
         * @return 返回一个映射，其中键是每个成员（消费者）的ID，值是分配给该成员的主题分区列表。
         */
        abstract Map<String, List<TopicPartition>> build();

        /**
         * 获取所有已排序主题的所有主题分区。
         * 此方法用于收集所有待分配的分区。
         *
         * @param sortedAllTopics 已排序的所有主题名称列表。
         * @return 包含所有主题分区的列表。
         */
        protected List<TopicPartition> getAllTopicPartitions(List<String> sortedAllTopics) {
            // 初始化一个列表，用于存储所有的主题分区，初始容量设置为总分区数以提高效率
            List<TopicPartition> allPartitions = new ArrayList<>(totalPartitionsCount);

            // 遍历排序后的所有主题
            for (String topic : sortedAllTopics) {
                // 获取当前主题的分区信息列表，并遍历其中的每个分区信息 (p)
                partitionsPerTopic.get(topic).forEach(p -> 
                    // 为每个分区信息创建一个新的 TopicPartition 对象，并添加到 allPartitions 列表中
                    allPartitions.add(new TopicPartition(p.topic(), p.partition()))
                );
            }
            // 返回包含所有主题分区的列表
            return allPartitions;
        }
    }

    /**
     * 当所有消费者都订阅了相同的主题集合时，此约束分配优化了分配算法。
     * 该方法包括以下步骤：
     *
     * 1. 重新分配先前拥有的分区：
     *    a. 如果拥有的分区少于 minQuota，则仅分配所有拥有的分区，并将该成员放入未填满成员列表。
     *    b. 如果拥有 maxQuota 或更多分区，并且我们仍低于预期的最大容量成员数，则分配 maxQuota 个分区。
     *    c. 如果至少拥有 "minQuota" 个分区，则分配 minQuota 个分区，并且如果我们仍低于预期的最大容量成员数，则将该成员放入未填满成员列表。
     *    如果使用机架感知算法，则在此步骤中仅分配具有匹配机架的拥有分区。
     * 2. 用机架匹配的方式填充剩余成员，直到达到预期的 maxQuota 分区数，否则填充到 minQuota 分区数。
     *    在此步骤中，不会分配那些在配额内无法在机架上对齐的分区。此步骤仅在机架感知时使用。
     * 3. 填充剩余成员，直到达到预期的 maxQuota 分区数，否则填充到 minQuota 分区数。
     *    对于机架感知算法，这些是在平衡约束内无法在机架上对齐的分区。
     *
     * 应用场景：当所有消费者订阅相同主题集时，优先考虑保持现有分配的稳定性，同时兼顾负载均衡和机架感知。
     * 设计考虑：通过多阶段分配，逐步满足分配约束，优先保留现有分配，然后考虑机架感知，最后进行轮询分配，以达到粘性分配的目标。
     */
    private class ConstrainedAssignmentBuilder extends AbstractAssignmentBuilder {

        /**
         * 在先前分配中被多个消费者声明拥有的分区集合。
         */
        private final Set<TopicPartition> partitionsWithMultiplePreviousOwners;
        /**
         * 可能被撤销的分区及其原先的消费者。键是主题分区，值是消费者ID。
         */
        private final Map<TopicPartition, String> maybeRevokedPartitions;

        // 可能仍需要分配一个或多个分区以达到预期容量的消费者
        /**
         * 拥有分区数少于 minQuota 的未填满成员列表。
         */
        private final List<String> unfilledMembersWithUnderMinQuotaPartitions;
        /**
         * 拥有分区数恰好等于 minQuota 的未填满成员链表。
         */
        private final LinkedList<String> unfilledMembersWithExactlyMinQuotaPartitions;

        /**
         * 每个消费者应分配的最小分区数。
         */
        private final int minQuota;
        /**
         * 每个消费者可分配的最大分区数。
         */
        private final int maxQuota;
        // 预期接收超过 minQuota 分区的成员数量（当 minQuota == maxQuota 时为零）
        /**
         * 预期拥有超过 minQuota 分区的成员数量。
         */
        private final int expectedNumMembersWithOverMinQuotaPartitions;
        // 当前接收超过 minQuota 分区的成员数量（当 minQuota == maxQuota 时为零）
        /**
         * 当前拥有超过 minQuota 分区的成员数量。
         */
        private int currentNumMembersWithOverMinQuotaPartitions;

        /**
         * 最终的分区分配结果。键是消费者ID，值是分配给该消费者的主题分区列表。
         */
        private final Map<String, List<TopicPartition>> assignment;
        /**
         * 已分配出去的所有主题分区列表。
         */
        private final List<TopicPartition> assignedPartitions;

        /**
         * 构造一个约束分配构建器。
         *
         * @param partitionsPerTopic                   每个订阅主题的分区信息。
         * @param rackInfo                             消费者和机架的机架信息。
         * @param consumerToOwnedPartitions            每个消费者先前拥有且仍然订阅的分区。
         * @param partitionsWithMultiplePreviousOwners 在先前分配中被多个消费者声明拥有的分区。
         */
        ConstrainedAssignmentBuilder(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                     RackInfo rackInfo,
                                     Map<String, List<TopicPartition>> consumerToOwnedPartitions,
                                     Set<TopicPartition> partitionsWithMultiplePreviousOwners) {
            // 调用父类构造函数，初始化 partitionsPerTopic, rackInfo, currentAssignment, totalPartitionsCount
            super(partitionsPerTopic, rackInfo, consumerToOwnedPartitions);

            // 初始化被多个先前所有者声明的分区集合
            this.partitionsWithMultiplePreviousOwners = partitionsWithMultiplePreviousOwners;
            // 初始化可能被撤销的分区映射
            maybeRevokedPartitions = new HashMap<>();
            // 初始化拥有分区数少于 minQuota 的未填满成员列表
            unfilledMembersWithUnderMinQuotaPartitions = new LinkedList<>();
            // 初始化拥有分区数恰好等于 minQuota 的未填满成员链表
            unfilledMembersWithExactlyMinQuotaPartitions = new LinkedList<>();

            // 获取消费者数量
            int numberOfConsumers = consumerToOwnedPartitions.size();

            // 计算每个消费者的最小分区配额 (向下取整)
            minQuota = (int) Math.floor(((double) totalPartitionsCount) / numberOfConsumers);
            // 计算每个消费者的最大分区配额 (向上取整)
            maxQuota = (int) Math.ceil(((double) totalPartitionsCount) / numberOfConsumers);
            // 计算预期拥有超过 minQuota 分区的成员数量 (总分区数对消费者数量取模)
            expectedNumMembersWithOverMinQuotaPartitions = totalPartitionsCount % numberOfConsumers;
            // 初始化当前拥有超过 minQuota 分区的成员数量为0
            currentNumMembersWithOverMinQuotaPartitions = 0;

            // 初始化分配映射，为所有成员创建一个大小为 maxQuota 的空数组
            // 使用流操作将 consumerToOwnedPartitions 的键集（消费者ID）转换成一个映射，
            // 其中键是消费者ID (c)，值是一个新的初始容量为 maxQuota 的 ArrayList
            assignment = new HashMap<>(consumerToOwnedPartitions.keySet().stream()
                    .collect(Collectors.toMap(c -> c, c -> new ArrayList<>(maxQuota))));
            // 初始化已分配分区列表
            assignedPartitions = new ArrayList<>();
        }

        /**
         * 构建约束分配方案。
         * 此方法实现了具体的分配逻辑，包括重新分配已拥有分区、机架感知轮询分配和普通轮询分配。
         *
         * @return 最终的分区分配结果，键是消费者ID，值是分配给该消费者的主题分区列表。
         */
        @Override
        /**
         * 构建并返回最终的分区分配方案。
         *
         * @return 从消费者ID到其分配到的主题分区列表的映射。
         * @method build
         * @description 执行分区分配的核心逻辑，包括保留现有分配、处理无效分配、分配未分配分区以及平衡最终分配。
         *              应用场景：在收集完所有必要信息并完成初始化后，调用此方法来生成最终的分区分配结果。
         *              实现细节：
         *              1. 记录调试信息。
         *              2. 初始化 prevAssignment 和 partitionMovements，用于跟踪分配变化。
         *              3. 调用 `prepopulateCurrentAssignments` 预填充当前分配。
         *              4. 调用 `assignOwnedPartitions` 处理已拥有的分区，保留有效分配并移除无效分配，返回已分配的分区列表。
         *              5. 调用 `getUnassignedPartitions` 获取所有仍需分配的分区。
         *              6. 将当前分配中的所有消费者添加到 `sortedCurrentSubscriptions` 中，这是一个按已分配分区数排序的消费者集合。
         *              7. 调用 `balance` 方法对未分配的分区进行均衡分配。
         *              8. 记录最终的分配信息。
         *              9. 返回最终的分配结果 `currentAssignment`。
         *              设计考虑：将分配过程分解为多个独立的步骤，每个步骤负责一部分逻辑，使得整个分配过程更加清晰和易于管理。
         *                        通过日志记录关键步骤和信息，方便调试和问题排查。
         */
        Map<String, List<TopicPartition>> build() {
            // 如果启用了 DEBUG 级别的日志
            if (log.isDebugEnabled()) {
                // 记录执行约束分配的初始参数信息
                log.debug("Performing constrained assign with partitionsPerTopic: {}, currentAssignment: {}, rackInfo {}.",
                        partitionsPerTopic, currentAssignment, rackInfo);
            }

            // 第一步：重新分配先前拥有的分区
            assignOwnedPartitions();

            // 获取在 assignOwnedPartitions 步骤后仍未分配的分区列表
            List<TopicPartition> unassignedPartitions = getUnassignedPartitions(assignedPartitions);

            // 如果启用了 DEBUG 级别的日志
            if (log.isDebugEnabled()) {
                // 记录重新分配先前拥有分区后的状态信息
                log.debug("After reassigning previously owned partitions, unfilled members: {}, unassigned partitions: {}, " +
                        "current assignment: {}", unfilledMembersWithUnderMinQuotaPartitions, unassignedPartitions, assignment);
            }

            // 对拥有分区数少于 minQuota 的未填满成员列表进行排序
            Collections.sort(unfilledMembersWithUnderMinQuotaPartitions);
            // 对拥有分区数恰好等于 minQuota 的未填满成员列表进行排序
            Collections.sort(unfilledMembersWithExactlyMinQuotaPartitions);
            // 根据机架上的消费者情况对未分配的分区进行排序，以便进行机架感知分配
            unassignedPartitions = rackInfo.sortPartitionsByRackConsumers(unassignedPartitions);

            // 第二步：执行机架感知的轮询分配
            assignRackAwareRoundRobin(unassignedPartitions);
            // 第三步：执行普通的轮询分配 (处理机架感知分配后剩余的分区)
            assignRoundRobin(unassignedPartitions);
            // 验证是否所有应该填满的成员都已填满
            verifyUnfilledMembers();

            // 记录最终的分区分配结果
            log.info("Final assignment of partitions to consumers: \n{}", assignment);

            // 返回最终的分配结果
            return assignment;
        }

        // 重新分配先前拥有的分区，最多分配到每个消费者预期的分区数
        /**
         * 重新分配先前拥有的分区。
         * 此方法的目标是尽可能保留消费者先前拥有的分区，同时考虑 minQuota 和 maxQuota 的限制，以及机架匹配情况。
         * 对于每个消费者，会检查其拥有的分区：
         * 1. 过滤掉与消费者机架不匹配的分区（如果启用了机架感知）。
         * 2. 移除被多个消费者重复声明的分区。
         * 3. 根据消费者拥有分区的数量与 minQuota 和 maxQuota 的关系，决定保留哪些分区，并将多余的分区放入 maybeRevokedPartitions。
         * 4. 更新消费者的分配列表和 unfilledMembers 列表。
         */
        private void assignOwnedPartitions() {

            // 遍历当前分配中的每个消费者及其拥有的分区
            for (Map.Entry<String, List<TopicPartition>> consumerEntry : currentAssignment.entrySet()) {
                // 获取消费者ID
                String consumer = consumerEntry.getKey();
                // 获取该消费者拥有的分区列表，并进行处理：
                List<TopicPartition> ownedPartitions = consumerEntry.getValue().stream()
                        // 1. 过滤：只保留与消费者机架匹配的分区
                        .filter(tp -> {
                            // 检查消费者机架与分区所在机架是否不匹配
                            boolean mismatch = rackInfo.racksMismatch(consumer, tp);
                            // 如果不匹配，则将该分区放入可能被撤销的列表
                            if (mismatch) {
                                maybeRevokedPartitions.put(tp, consumer);
                            }
                            // 返回是否匹配 (保留匹配的，即 !mismatch 为 true)
                            return !mismatch;
                        })
                        // 2. 排序：首先按分区号排序，然后按主题名称排序，确保分配的确定性
                        .sorted(Comparator.comparing(TopicPartition::partition).thenComparing(TopicPartition::topic))
                        // 3. 收集：将处理后的分区收集到一个新的列表中
                        .collect(Collectors.toList());

                // 获取当前消费者在最终分配方案中的分区列表引用
                List<TopicPartition> consumerAssignment = assignment.get(consumer);

                // 检查并移除被多个先前所有者声明的分区
                for (TopicPartition doublyClaimedPartition : partitionsWithMultiplePreviousOwners) {
                    // 如果当前消费者拥有的分区中包含这个被重复声明的分区
                    if (ownedPartitions.contains(doublyClaimedPartition)) {
                        // 记录错误日志，说明发现了不一致的情况
                        log.error("Found partition {} still claimed as owned by consumer {}, despite being claimed by multiple "
                                        + "consumers already in the same generation. Removing it from the ownedPartitions",
                                doublyClaimedPartition, consumer);
                        // 从消费者拥有的分区列表中移除该分区
                        ownedPartitions.remove(doublyClaimedPartition);
                    }
                }

                // 根据消费者拥有的分区数量与 minQuota 和 maxQuota 的关系进行处理
                if (ownedPartitions.size() < minQuota) {
                    // 情况1：拥有的分区数少于 minQuota
                    // 预期的分配大小大于该消费者当前拥有的，因此保留所有拥有的分区
                    // 并将此成员放入未填满成员列表 (unfilledMembersWithUnderMinQuotaPartitions)
                    if (ownedPartitions.size() > 0) {
                        // 将所有拥有的分区添加到该消费者的分配列表中
                        consumerAssignment.addAll(ownedPartitions);
                        // 将这些分区添加到已分配分区列表中
                        assignedPartitions.addAll(ownedPartitions);
                    }
                    // 将该消费者添加到“拥有分区少于minQuota”的未填满成员列表中
                    unfilledMembersWithUnderMinQuotaPartitions.add(consumer);
                } else if (ownedPartitions.size() >= maxQuota && currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions) {
                    // 情况2：拥有的分区数大于等于 maxQuota，并且当前拥有超过 minQuota 分区的成员数小于预期值
                    // 消费者拥有的分区数达到或超过了“maxQuota”，并且我们仍低于预期拥有超过 minQuota 分区的成员数
                    // 因此，保留“maxQuota”个拥有的分区，并撤销其余分区
                    currentNumMembersWithOverMinQuotaPartitions++; // 增加一个已达到 maxQuota 的消费者计数
                    // 如果当前达到 maxQuota 的消费者数量等于了预期数量，说明所有能达到 maxQuota 的消费者都已处理完毕
                    // 此时，拥有恰好 minQuota 分区的消费者不再是潜在的 maxQuota 候选者，清空该列表
                    if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                        unfilledMembersWithExactlyMinQuotaPartitions.clear();
                    }
                    // 获取前 maxQuota 个分区
                    List<TopicPartition> maxQuotaPartitions = ownedPartitions.subList(0, maxQuota);
                    // 将这 maxQuota 个分区添加到消费者的分配列表中
                    consumerAssignment.addAll(maxQuotaPartitions);
                    // 将这些分区添加到已分配分区列表中
                    assignedPartitions.addAll(maxQuotaPartitions);
                    // 将超出 maxQuota 的分区放入可能被撤销的列表
                    for (TopicPartition topicPartition : ownedPartitions.subList(maxQuota, ownedPartitions.size())) {
                        maybeRevokedPartitions.put(topicPartition, consumer);
                    }
                } else {
                    // 情况3：拥有的分区数介于 minQuota 和 maxQuota 之间 (或等于 minQuota，或在情况2不满足时等于或大于maxQuota)
                    // 消费者至少拥有“minQuota”个分区
                    // 因此，保留“minQuota”个拥有的分区，并撤销其余分区
                    List<TopicPartition> minQuotaPartitions = ownedPartitions.subList(0, minQuota);
                    // 将这 minQuota 个分区添加到消费者的分配列表中
                    consumerAssignment.addAll(minQuotaPartitions);
                    // 将这些分区添加到已分配分区列表中
                    assignedPartitions.addAll(minQuotaPartitions);
                    // 将超出 minQuota 的分区放入可能被撤销的列表
                    for (TopicPartition topicPartition : ownedPartitions.subList(minQuota, ownedPartitions.size())) {
                        maybeRevokedPartitions.put(topicPartition, consumer);
                    }
                    // 该消费者是潜在的 maxQuota 候选者，因为我们仍低于预期拥有超过 minQuota 分区的成员数。
                    // 注意：如果预期拥有超过 minQuota 分区的成员数为0，则表示 minQuota == maxQuota，不存在潜在的未填满成员。
                    if (currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions) {
                        // 将该消费者添加到“拥有分区恰好等于minQuota”的未填满成员列表中
                        unfilledMembersWithExactlyMinQuotaPartitions.add(consumer);
                    }
                }
            }
        }

        // 在机架内部对剩余成员进行轮询填充，直到达到 maxQuota 的预期数量，
        // 否则，填充到 minQuota
        /**
         * @author Trae
         * @date 2024-07-26 11:11:11
         * @description 执行机架感知的轮询分区分配。
         * 此方法尝试将未分配的分区分配给与分区副本位于相同机架的消费者，以优化局部性。
         * 它会优先填充那些分区数低于 minQuota 的消费者，然后是那些恰好有 minQuota 分区的消费者（如果它们可以被填充到 maxQuota 而不超过预期的超额分配消费者数量）。
         * 应用场景：当启用了机架感知分配，并且有未分配的分区需要分配时调用。
         * 设计考虑：此方法旨在平衡分区在消费者之间的分配，同时利用机架信息减少跨机架数据传输。
         * @param unassignedPartitions 未分配的主题分区列表。
         */
        private void assignRackAwareRoundRobin(List<TopicPartition> unassignedPartitions) {
            // 如果没有消费者机架信息，则直接返回，无法进行机架感知分配
            if (rackInfo.consumerRacks.isEmpty())
                return;
            // 初始化下一个未填满消费者的索引为0
            int nextUnfilledConsumerIndex = 0;
            // 获取未分配分区的迭代器
            Iterator<TopicPartition> unassignedIter = unassignedPartitions.iterator();
            // 遍历所有未分配的分区
            while (unassignedIter.hasNext()) {
                // 获取当前未分配的分区
                TopicPartition unassignedPartition = unassignedIter.next();
                // 初始化要分配给此分区的消费者为null
                String consumer = null;
                // 尝试为当前分区找到一个机架匹配的、且分区数低于minQuota的消费者
                int nextIndex = rackInfo.nextRackConsumer(unassignedPartition, unfilledMembersWithUnderMinQuotaPartitions, nextUnfilledConsumerIndex);
                // 如果找到了这样的消费者
                if (nextIndex >= 0) {
                    // 获取该消费者的ID
                    consumer = unfilledMembersWithUnderMinQuotaPartitions.get(nextIndex);
                    // 计算如果分配此分区后，该消费者的分区数
                    int assignmentCount = assignment.get(consumer).size() + 1;
                    // 如果分配后分区数达到或超过minQuota
                    if (assignmentCount >= minQuota) {
                        // 将此消费者从未填满（低于minQuota）的列表中移除
                        unfilledMembersWithUnderMinQuotaPartitions.remove(consumer);
                        // 仅当此消费者当前的分区数（分配后）小于maxQuota，并且当前已达到maxQuota的消费者数量小于预期数量时，才将其添加到恰好有minQuota分区的列表中
                        // 这是因为一个处于minQuota的消费者只有在可能添加另一个分区（使其达到maxQuota）并且不会超过预期的超额分配消费者数量时，才被认为是“未填满”的
                        if (assignmentCount < maxQuota && (currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions)) {
                            unfilledMembersWithExactlyMinQuotaPartitions.add(consumer);
                        }
                    } else {
                        // 如果分配后分区数仍低于minQuota，则尝试下一个索引（在同一列表中轮询）
                        nextIndex++;
                    }
                    // 更新下一个未填满消费者的索引，如果列表为空则重置为0，否则进行模运算以实现轮询
                    nextUnfilledConsumerIndex = unfilledMembersWithUnderMinQuotaPartitions.isEmpty() ? 0 : nextIndex % unfilledMembersWithUnderMinQuotaPartitions.size();
                // 如果没有找到低于minQuota的机架匹配消费者，但存在恰好有minQuota分区的消费者列表不为空
                } else if (!unfilledMembersWithExactlyMinQuotaPartitions.isEmpty()) {
                    // 尝试为当前分区找到一个机架匹配的、且恰好有minQuota分区的消费者（从索引0开始查找）
                    int firstIndex = rackInfo.nextRackConsumer(unassignedPartition, unfilledMembersWithExactlyMinQuotaPartitions, 0);
                    // 如果找到了这样的消费者
                    if (firstIndex >= 0) {
                        // 获取该消费者的ID
                        consumer = unfilledMembersWithExactlyMinQuotaPartitions.get(firstIndex);
                        // 如果分配此分区后，该消费者的分区数达到maxQuota
                        if (assignment.get(consumer).size() + 1 == maxQuota) {
                            // 将此消费者从恰好有minQuota分区的列表中移除（因为它现在达到了maxQuota）
                            unfilledMembersWithExactlyMinQuotaPartitions.remove(firstIndex);
                            // 增加已达到maxQuota的消费者数量计数器
                            currentNumMembersWithOverMinQuotaPartitions++;
                            // 一旦当前超过minQuota的消费者数量达到预期数量，就清空恰好有minQuota分区的列表
                            // 因为这意味着所有处于minQuota的消费者现在都被认为是“已填满”的
                            if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                                unfilledMembersWithExactlyMinQuotaPartitions.clear();
                            }
                        }
                    }
                }

                // 如果成功为当前分区找到了一个消费者
                if (consumer != null) {
                    // 将分区分配给该消费者
                    assignNewPartition(unassignedPartition, consumer);
                    // 从未分配分区列表中移除已分配的分区
                    unassignedIter.remove();
                }
            }
        }

        /**
         * @author Trae
         * @date 2024-07-26 11:11:11
         * @description 对未分配的分区执行标准的轮询分配。
         * 此方法用于在机架感知分配之后（或在不使用机架感知分配时）分配剩余的分区。
         * 它首先尝试将分区分配给分区数低于 minQuota 的消费者，然后是那些恰好有 minQuota 分区的消费者。
         * 应用场景：当存在未分配的分区，并且需要通过轮询方式将其分配给消费者时调用。
         * 设计考虑：确保分区尽可能均匀地分配给消费者，同时遵循 minQuota 和 maxQuota 的约束。
         * @param unassignedPartitions 未分配的主题分区列表。
         */
        private void assignRoundRobin(List<TopicPartition> unassignedPartitions) {

            // 获取分区数低于minQuota的未填满消费者的迭代器
            Iterator<String> unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator();
            // 对剩余成员进行轮询填充，直到达到 maxQuota 的预期数量，否则，填充到 minQuota
            for (TopicPartition unassignedPartition : unassignedPartitions) {
                // 声明要分配给此分区的消费者变量
                String consumer;
                // 如果低于minQuota的消费者迭代器还有下一个元素
                if (unfilledConsumerIter.hasNext()) {
                    // 获取下一个低于minQuota的消费者
                    consumer = unfilledConsumerIter.next();
                } else {
                    // 如果低于minQuota的消费者列表已遍历完毕
                    // 并且低于minQuota和恰好有minQuota的消费者列表都为空
                    if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty() && unfilledMembersWithExactlyMinQuotaPartitions.isEmpty()) {
                        // 不应该进入这里，因为我们已经计算了分配给每个消费者的确切数量。
                        // 这表明分配算法中存在问题
                        // 获取当前未分配分区在列表中的索引
                        int currentPartitionIndex = unassignedPartitions.indexOf(unassignedPartition);
                        // 记录错误日志，指出没有更多未填满的消费者可以分配，并列出剩余的未分配分区
                        log.error("No more unfilled consumers to be assigned. The remaining unassigned partitions are: {}",
                                unassignedPartitions.subList(currentPartitionIndex, unassignedPartitions.size()));
                        // 抛出非法状态异常
                        throw new IllegalStateException("No more unfilled consumers to be assigned.");
                    // 如果低于minQuota的消费者列表为空，但恰好有minQuota的消费者列表不为空
                    } else if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) {
                        // 从恰好有minQuota的消费者列表中取出一个消费者（并移除）
                        consumer = unfilledMembersWithExactlyMinQuotaPartitions.poll();
                    } else {
                        // 如果低于minQuota的消费者列表不为空（意味着迭代器已到末尾，需要重置）
                        // 重置低于minQuota的消费者迭代器
                        unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator();
                        // 获取重置后的迭代器的第一个消费者
                        consumer = unfilledConsumerIter.next();
                    }
                }

                // 将当前未分配的分区分配给选定的消费者，并获取分配后该消费者的分区总数
                int currentAssignedCount = assignNewPartition(unassignedPartition, consumer);

                // 如果分配后消费者的分区数等于minQuota
                if (currentAssignedCount == minQuota) {
                    // 从低于minQuota的消费者迭代器中移除该消费者（因为它现在达到了minQuota）
                    unfilledConsumerIter.remove();
                    // 将该消费者添加到恰好有minQuota分区的列表中
                    unfilledMembersWithExactlyMinQuotaPartitions.add(consumer);
                // 如果分配后消费者的分区数等于maxQuota
                } else if (currentAssignedCount == maxQuota) {
                    // 增加已达到maxQuota的消费者数量计数器
                    currentNumMembersWithOverMinQuotaPartitions++;
                    // 如果已达到maxQuota的消费者数量等于预期的超额分配消费者数量
                    if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                        // 我们只有在将所有成员填充到至少minQuota之后，才开始迭代“可能未填满”的minQuota成员，
                        // 因此，一旦最后一个minQuota成员达到maxQuota，我们应该就完成了。但是，如果出现某些算法错误，
                        // 只需记录一个警告并继续在分配约束内分配任何剩余的分区
                        // 检查当前分区是否是未分配分区列表中的最后一个分区
                        if (unassignedPartitions.indexOf(unassignedPartition) != unassignedPartitions.size() - 1) {
                            // 如果不是最后一个分区，但已填满最后一个达到maxQuota的成员，记录错误日志
                            log.error("Filled the last member up to maxQuota but still had partitions remaining to assign, "
                                    + "will continue but this indicates a bug in the assignment.");
                        }
                    }
                }
            }
        }

        /**
         * @author Trae
         * @date 2024-07-26 11:11:11
         * @description 将一个新的未分配分区分配给指定的消费者。
         * 此方法会更新消费者的分配列表，并检查该分区是否正在转移所有权。
         * 应用场景：在轮询分配或机架感知分配逻辑中，当确定一个分区要分配给某个消费者时调用。
         * 设计考虑：封装了将分区添加到消费者分配中的操作，并处理了与协作式再均衡相关的分区所有权转移逻辑。
         * @param unassignedPartition 要分配的未分配主题分区。
         * @param consumer 将要接收此分区的消费者ID。
         * @return 分配此分区后，该消费者的分区总数。
         */
        private int assignNewPartition(TopicPartition unassignedPartition, String consumer) {
            // 获取指定消费者的当前分区分配列表
            List<TopicPartition> consumerAssignment = assignment.get(consumer);
            // 将未分配的分区添加到该消费者的分配列表中
            consumerAssignment.add(unassignedPartition);

            // 我们已经分配了所有可能的自有分区，所以我们知道这个分区必须是新分配给这个消费者的
            // 否则，这个分区实际上被多个先前所有者声明，并且必须从所有声明拥有该分区的成员的自有分区中作废
            // 检查此分区是否在可能被撤销的分区映射中，并且其先前所有者不是当前消费者，
            // 或者此分区是否在被多个先前所有者声明的分区集合中
            if ((maybeRevokedPartitions.containsKey(unassignedPartition) && !maybeRevokedPartitions.get(unassignedPartition).equals(consumer))
                    || partitionsWithMultiplePreviousOwners.contains(unassignedPartition)) {
                // 如果满足上述任一条件，则将此分区及其新的消费者记录到正在转移所有权的映射中
                partitionsTransferringOwnership.put(unassignedPartition, consumer);
            }

            // 返回分配此分区后，该消费者的分区总数
            return consumerAssignment.size();
        }

        /**
         * @author Trae
         * @date 2024-07-26 11:11:11
         * @description 验证未填满的成员（消费者）的状态是否符合预期。
         * 此方法用于在分配过程的某个阶段检查是否存在不一致的情况，例如某些消费者未达到 minQuota，但已没有更多分区可分配。
         * 应用场景：通常在分配算法的关键步骤之后调用，以确保分配逻辑的正确性。
         * 设计考虑：作为一种断言机制，帮助捕获分配过程中的潜在错误和不一致状态。
         */
        private void verifyUnfilledMembers() {

            // 如果分区数低于minQuota的未填满成员列表不为空
            if (!unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) {
                // 我们期望所有剩余的未填满成员都拥有minQuota个分区，并且我们已经达到了拥有超过minQuota个分区的成员的预期数量。
                // 否则，这里一定有错误。
                // 如果当前拥有超过minQuota分区的成员数量不等于预期的数量
                if (currentNumMembersWithOverMinQuotaPartitions != expectedNumMembersWithOverMinQuotaPartitions) {
                    // 记录错误日志，指出当前超额分配的成员数少于预期，并且没有更多分区可以分配给剩余的未填满消费者
                    log.error("Current number of members with more than the minQuota partitions: {}, is less than the expected number " +
                                    "of members with more than the minQuota partitions: {}, and no more partitions to be assigned to the remaining unfilled consumers: {}",
                            currentNumMembersWithOverMinQuotaPartitions, expectedNumMembersWithOverMinQuotaPartitions, unfilledMembersWithUnderMinQuotaPartitions);
                    // 抛出非法状态异常
                    throw new IllegalStateException("We haven't reached the expected number of members with " +
                            "more than the minQuota partitions, but no more partitions to be assigned");
                } else {
                    // 遍历所有分区数低于minQuota的未填满成员
                    for (String unfilledMember : unfilledMembersWithUnderMinQuotaPartitions) {
                        // 获取该成员当前分配到的分区数量
                        int assignedPartitionsCount = assignment.get(unfilledMember).size();
                        // 如果分配到的分区数量不等于minQuota
                        if (assignedPartitionsCount != minQuota) {
                            // 记录错误日志，指出该消费者应有的分区数与实际分配数不符，且没有更多分区可分配
                            log.error("Consumer: [{}] should have {} partitions, but got {} partitions, and no more partitions " +
                                    "to be assigned. The remaining unfilled consumers are: {}", unfilledMember, minQuota, assignedPartitionsCount, unfilledMembersWithUnderMinQuotaPartitions);
                            // 抛出非法状态异常
                            throw new IllegalStateException(String.format("Consumer: [%s] doesn't reach minQuota partitions, " +
                                    "and no more partitions to be assigned", unfilledMember));
                        } else {
                            // 如果分配到的分区数量等于minQuota，记录跟踪日志，说明跳过此未填满成员，因为它已达到minQuota且超额分配成员数已达预期
                            log.trace("skip over this unfilled member: [{}] because we've reached the expected number of " +
                                    "members with more than the minQuota partitions, and this member already has minQuota partitions", unfilledMember);
                        }
                    }
                }
            }
        }

        /**
         * @author Trae
         * @date 2024-07-26 11:11:11
         * @description 通过计算所有已排序分区与已排序的已分配分区之间的差集来获取未分配分区列表。
         * 如果没有已分配的分区，则直接返回所有已排序的主题分区。
         * 应用场景：在分配过程开始前或某些阶段，需要确定哪些分区尚未分配给任何消费者。
         * 实现细节：
         * 采用双指针技术来计算差集：
         * 1. 遍历所有已排序的主题。
         * 2. 对每个主题，遍历其所有分区。
         * 3. 将当前主题分区与 `sortedAssignedPartitions` 中的第 i 个元素进行比较（i 从 0 开始）：
         *    - 如果不等于第 i 个元素，则将其添加到 `unassignedPartitions` 列表中。
         *    - 如果等于第 i 个元素，则从 `sortedAssignedPartitions` 中获取下一个元素。
         * 设计考虑：这是一种高效计算两个已排序列表之间差集的方法，避免了对整个列表进行暴力搜索。
         * @param sortedAssignedPartitions 已排序的已分配分区列表，这些分区都包含在所有分区列表中。
         * @return 尚未分配给任何消费者的分区列表。
         */
        private List<TopicPartition> getUnassignedPartitions(List<TopicPartition> sortedAssignedPartitions) {
            // 获取所有主题名称的列表
            List<String> sortedAllTopics = new ArrayList<>(partitionsPerTopic.keySet());
            // 首先对所有主题进行排序，然后我们可以通过从0开始添加分区来获得所有已排序的主题分区
            Collections.sort(sortedAllTopics);

            // 如果已分配分区列表为空
            if (sortedAssignedPartitions.isEmpty()) {
                // 没有已分配的分区意味着所有分区都是未分配的分区
                return getAllTopicPartitions(sortedAllTopics);
            }

            // 初始化未分配分区列表，初始容量为总分区数减去已分配分区数
            List<TopicPartition> unassignedPartitions = new ArrayList<>(totalPartitionsCount - sortedAssignedPartitions.size());

            // 对已分配分区列表进行排序，首先按主题名称，然后按分区号
            sortedAssignedPartitions.sort(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));

            // 标记是否应直接添加剩余分区（当已分配分区列表遍历完毕后）
            boolean shouldAddDirectly = false;
            // 获取已分配分区列表的迭代器
            Iterator<TopicPartition> sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator();
            // 获取第一个已分配的分区
            TopicPartition nextAssignedPartition = sortedAssignedPartitionsIter.next();

            // 遍历所有已排序的主题
            for (String topic : sortedAllTopics) {
                // 获取当前主题的分区数量
                int partitionCount = partitionsPerTopic.get(topic).size();
                // 遍历当前主题的所有分区（按分区号0到partitionCount-1）
                for (int i = 0; i < partitionCount; i++) {
                    // 如果应该直接添加（已分配列表已遍历完），或者当前分区（topic, i）不等于下一个已分配分区
                    if (shouldAddDirectly || !(nextAssignedPartition.topic().equals(topic) && nextAssignedPartition.partition() == i)) {
                        // 将当前分区（topic, i）添加到未分配分区列表中
                        unassignedPartitions.add(new TopicPartition(topic, i));
                    } else {
                        // 当前分区在已分配分区列表中，不添加到未分配列表，只需获取下一个已分配分区
                        // 如果已分配分区迭代器还有下一个元素
                        if (sortedAssignedPartitionsIter.hasNext()) {
                            // 获取下一个已分配分区
                            nextAssignedPartition = sortedAssignedPartitionsIter.next();
                        } else {
                            // 如果没有更多已分配分区，则直接添加剩余的分区
                            shouldAddDirectly = true;
                        }
                    }
                }
            }

            // 返回计算得到的未分配分区列表
            return unassignedPartitions;
        }
    }

    /**
     * 此通用分配算法保证分配尽可能均衡。
     * 此方法包括以下步骤：
     *
     * 1. 保留所有现有的分区分配。如果使用机架感知算法，则仅保留机架内的分配。
     * 2. 删除所有因触发重新分配的更改而变得无效的分区分配。机架不匹配的分区分配也将被删除。
     * 3. 以平衡消费者整体分区分配的方式分配未分配的分区，同时保留机架对齐。此步骤仅用于机架感知分配。
     * 4. 以平衡消费者整体分区分配的方式分配剩余的未分配分区。对于机架感知算法，这些是无法在平衡约束内与机架对齐的分区。
     * 5. 通过查找可以重新分配给另一个消费者的分区，进一步平衡最终的分配，以实现更均衡的整体分配。对于机架感知算法，如果可能，尝试保留机架对齐。
     *
     * 应用场景：在消费者加入或离开消费组，或者主题分区发生变化时，用于重新计算分区分配方案，力求均衡和粘性。
     * 实现细节：这是一个内部类，继承自 AbstractAssignmentBuilder，封装了通用的分区分配逻辑。
     * 设计考虑：通过构建器模式逐步构建分配方案，将复杂的分配逻辑分解为多个步骤，提高代码的可读性和可维护性。
     */
    private class GeneralAssignmentBuilder extends AbstractAssignmentBuilder {
        /**
         * @description 消费者ID到其订阅信息的映射。
         * @field subscriptions
         */
        private final Map<String, Subscription> subscriptions;

        // 将所有主题映射到可以分配给它们的所有消费者的映射
        /**
         * @description 将所有主题映射到可以分配给它们的所有消费者的映射。
         * @field topic2AllPotentialConsumers
         */
        private final Map<String, List<String>> topic2AllPotentialConsumers;
        // 将所有消费者映射到可以分配给它们的所有潜在主题的映射
        /**
         * @description 将所有消费者映射到可以分配给它们的所有潜在主题的映射。
         * @field consumer2AllPotentialTopics
         */
        private final Map<String, List<String>> consumer2AllPotentialTopics;
        // 将分区映射到当前消费者的映射
        /**
         * @description 将分区映射到当前消费者的映射。
         * @field currentPartitionConsumer
         */
        private final Map<TopicPartition, String> currentPartitionConsumer;
        /**
         * @description 所有主题分区的排序列表。
         * @field sortedAllPartitions
         */
        private final List<TopicPartition> sortedAllPartitions;
        // 一个根据已分配给消费者的主题分区数量升序排列的消费者集合
        /**
         * @description 一个根据已分配给消费者的主题分区数量升序排列的消费者集合。
         * @field sortedCurrentSubscriptions
         */
        private final TreeSet<String> sortedCurrentSubscriptions;
        /**
         * @description 标记是否需要撤销分区。
         * @field revocationRequired
         */
        private boolean revocationRequired;

        /**
         * 构造一个通用分配构建器。
         *
         * @param partitionsPerTopic         每个已订阅主题的分区信息。键是主题名称，值是该主题的 PartitionInfo 列表。
         * @param subscriptions              从成员 ID 到其各自主题订阅的映射。
         * @param currentAssignment          每个消费者先前拥有且仍然订阅的分区。键是消费者ID，值是该消费者当前拥有的 TopicPartition 列表。
         * @param rackInfo                   消费者和分区的机架信息。
         * @constructor GeneralAssignmentBuilder
         * @description 初始化通用分配构建器的实例，准备进行分区分配计算。
         *              应用场景：在需要执行新的分区分配时创建此构建器。
         *              实现细节：初始化各种映射和列表，用于存储主题、消费者、分区及其之间的关系，为后续的分配算法做准备。
         *              设计考虑：构造函数负责收集和预处理所有必要的信息，确保分配算法可以基于准确和完整的数据进行操作。
         */
        GeneralAssignmentBuilder(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                 RackInfo rackInfo,
                                 Map<String, List<TopicPartition>> currentAssignment,
                                 Map<String, Subscription> subscriptions) {
            super(partitionsPerTopic, rackInfo, currentAssignment); // 调用父类构造函数，初始化基本信息
            this.subscriptions = subscriptions; // 保存消费者订阅信息

            // 初始化 topic2AllPotentialConsumers，容量设置为主题数量，提高性能
            topic2AllPotentialConsumers = new HashMap<>(partitionsPerTopic.keySet().size());
            // 初始化 consumer2AllPotentialTopics，容量设置为消费者数量，提高性能
            consumer2AllPotentialTopics = new HashMap<>(subscriptions.keySet().size());

            // 初始化 topic2AllPotentialConsumers 和 consumer2AllPotentialTopics
            // 遍历所有主题名称
            partitionsPerTopic.keySet().forEach(
                    // 为每个主题在 topic2AllPotentialConsumers 中创建一个空的消费者列表
                    topicName -> topic2AllPotentialConsumers.put(topicName, new ArrayList<>()));

            // 遍历所有消费者的订阅信息
            subscriptions.forEach((consumerId, subscription) -> {
                // 为当前消费者创建一个新的已订阅主题列表
                List<String> subscribedTopics = new ArrayList<>(subscription.topics().size());
                // 将当前消费者的已订阅主题列表存入 consumer2AllPotentialTopics
                consumer2AllPotentialTopics.put(consumerId, subscribedTopics);
                // 遍历当前消费者订阅的所有主题
                subscription.topics().stream()
                    // 过滤掉在 partitionsPerTopic 中不存在的主题（即元数据中没有的主题）
                    .filter(topic -> partitionsPerTopic.get(topic) != null)
                    // 对于有效的主题
                    .forEach(topic -> {
                        // 将主题添加到当前消费者的已订阅主题列表中
                        subscribedTopics.add(topic);
                        // 将当前消费者添加到该主题的潜在消费者列表中
                        topic2AllPotentialConsumers.get(topic).add(consumerId);
                    });

                // 如果当前消费者不在 currentAssignment 中（即新加入的消费者），则为其添加一个空的分区分配列表
                // add this consumer to currentAssignment (with an empty topic partition assignment) if it does not already exist
                if (!currentAssignment.containsKey(consumerId))
                    currentAssignment.put(consumerId, new ArrayList<>());
            });

            // 初始化 currentPartitionConsumer，用于存储分区到当前消费者的映射
            currentPartitionConsumer = new HashMap<>();
            // 遍历 currentAssignment 中的每个条目（消费者 -> 分区列表）
            for (Map.Entry<String, List<TopicPartition>> entry: currentAssignment.entrySet())
                // 遍历该消费者拥有的每个分区
                for (TopicPartition topicPartition: entry.getValue())
                    // 将分区和对应的消费者存入 currentPartitionConsumer
                    currentPartitionConsumer.put(topicPartition, entry.getKey());

            // 获取所有潜在主题的排序列表
            List<String> sortedAllTopics = new ArrayList<>(topic2AllPotentialConsumers.keySet());
            // 使用 TopicComparator 对主题进行排序（通常基于主题的分区数或消费者数）
            sortedAllTopics.sort(new TopicComparator(topic2AllPotentialConsumers));
            // 获取所有主题分区的排序列表
            sortedAllPartitions = getAllTopicPartitions(sortedAllTopics);

            // 初始化 sortedCurrentSubscriptions，这是一个根据已分配分区数量升序排列的消费者集合
            // 使用 SubscriptionComparator 来比较消费者，该比较器基于 currentAssignment 中每个消费者分配到的分区数量
            sortedCurrentSubscriptions = new TreeSet<>(new SubscriptionComparator(currentAssignment));
        }

        /**
         * 构建约束分配方案。
         * 此方法实现了具体的分配逻辑，包括重新分配已拥有分区、机架感知轮询分配和普通轮询分配。
         *
         * @return 最终的分区分配结果，键是消费者ID，值是分配给该消费者的主题分区列表。
         */
        @Override
        /**
         * 构建并返回最终的分区分配方案。
         *
         * @return 从消费者ID到其分配到的主题分区列表的映射。
         * @method build
         * @description 执行分区分配的核心逻辑，包括保留现有分配、处理无效分配、分配未分配分区以及平衡最终分配。
         *              应用场景：在收集完所有必要信息并完成初始化后，调用此方法来生成最终的分区分配结果。
         *              实现细节：
         *              1. 记录调试信息。
         *              2. 初始化 prevAssignment 和 partitionMovements，用于跟踪分配变化。
         *              3. 调用 `prepopulateCurrentAssignments` 预填充当前分配。
         *              4. 调用 `assignOwnedPartitions` 处理已拥有的分区，保留有效分配并移除无效分配，返回已分配的分区列表。
         *              5. 调用 `getUnassignedPartitions` 获取所有仍需分配的分区。
         *              6. 将当前分配中的所有消费者添加到 `sortedCurrentSubscriptions` 中，这是一个按已分配分区数排序的消费者集合。
         *              7. 调用 `balance` 方法对未分配的分区进行均衡分配。
         *              8. 记录最终的分配信息。
         *              9. 返回最终的分配结果 `currentAssignment`。
         *              设计考虑：将分配过程分解为多个独立的步骤，每个步骤负责一部分逻辑，使得整个分配过程更加清晰和易于管理。
         *                        通过日志记录关键步骤和信息，方便调试和问题排查。
         */
        Map<String, List<TopicPartition>> build() {
            // 如果启用了调试日志级别
            if (log.isDebugEnabled()) {
                // 记录执行通用分配的调试信息，包括每个主题的分区信息、订阅信息、当前分配和机架信息
                log.debug("performing general assign. partitionsPerTopic: {}, subscriptions: {}, currentAssignment: {}, rackInfo: {}",
                        partitionsPerTopic, subscriptions, currentAssignment, rackInfo);
            }

            // 创建一个映射，用于存储先前的分配信息（分区 -> 消费者和代数对）
            Map<TopicPartition, ConsumerGenerationPair> prevAssignment = new HashMap<>();
            // 初始化分区移动记录器
            partitionMovements = new PartitionMovements();
            // 预填充当前分配到 prevAssignment 中，用于后续比较和跟踪分区移动
            prepopulateCurrentAssignments(prevAssignment);

            // 获取在当前分配中已经分配的分区
            // the partitions already assigned in current assignment
            List<TopicPartition> assignedPartitions = assignOwnedPartitions();

            // 获取所有仍然需要分配的分区
            // all partitions that still need to be assigned
            List<TopicPartition> unassignedPartitions = getUnassignedPartitions(assignedPartitions);

            // 如果启用了调试日志级别
            if (log.isDebugEnabled()) {
                // 记录未分配的分区信息
                log.debug("unassigned Partitions: {}", unassignedPartitions);
            }

            // 此时，我们已经保留了所有有效的主题分区到消费者的分配，并移除了所有无效的主题分区和无效的消费者。
            // 现在我们需要将 unassignedPartitions 分配给消费者，以使主题分区分配尽可能均衡。
            // at this point we have preserved all valid topic partition to consumer assignments and removed
            // all invalid topic partitions and invalid consumers. Now we need to assign unassignedPartitions
            // to consumers so that the topic partition assignments are as balanced as possible.
            // 将当前分配中的所有消费者（即仍然存在的消费者）添加到 sortedCurrentSubscriptions 集合中
            // sortedCurrentSubscriptions 是一个根据已分配分区数量排序的消费者集合，用于后续的均衡分配
            sortedCurrentSubscriptions.addAll(currentAssignment.keySet());

            // 调用 balance 方法，对未分配的分区进行均衡分配，并更新 currentAssignment
            balance(prevAssignment, unassignedPartitions);

            // 记录最终的分区到消费者的分配信息
            log.info("Final assignment of partitions to consumers: \n{}", currentAssignment);

            // 返回最终的分配结果
            return currentAssignment;
        }

        /**
         * 处理当前分配中已拥有的分区，保留有效分配并移除无效分配。
         *
         * @return 返回在此步骤中被确认为有效并保留分配的分区列表。
         * @method assignOwnedPartitions
         * @description 遍历当前分配中的每个消费者及其拥有的分区，检查分配的有效性。
         *              如果消费者不存在或分区不再有效（例如主题被删除、消费者不再订阅该主题、机架不匹配），则移除该分配。
         *              应用场景：在重新平衡开始时，首先清理和验证现有的分配，确保后续分配基于一个有效的起点。
         *              实现细节：
         *              1. 初始化一个空列表 `assignedPartitions` 用于存储有效保留的分配。
         *              2. 遍历 `currentAssignment` (消费者 -> 其拥有的分区列表)。
         *              3. 对于每个消费者：
         *                 a. 检查消费者是否存在于 `subscriptions` 中。如果不存在（消费者已离开），则移除该消费者的所有分区分配，并从 `currentPartitionConsumer` 中移除这些分区。
         *                 b. 如果消费者仍然存在：
         *                    i. 遍历该消费者拥有的每个分区。
         *                    ii. 检查分区的主题是否存在于 `topic2AllPotentialConsumers` 中。如果不存在（主题已删除），则移除该分区分配，并从 `currentPartitionConsumer` 中移除该分区。
         *                    iii. 检查消费者是否仍然订阅该分区的主题，以及（如果启用了机架感知）消费者和分区的机架是否匹配。如果不满足这些条件，则移除该分区分配，并设置 `revocationRequired = true`。
         *                    iv. 如果以上检查都通过，说明该分配是有效的，将该分区添加到 `assignedPartitions` 列表中。
         *              4. 返回 `assignedPartitions` 列表。
         *              设计考虑：此方法确保了分配的“粘性”，即尽可能保留现有的有效分配，减少不必要的分区移动。
         *                        通过迭代器进行删除操作，避免了 `ConcurrentModificationException`。
         */
        private List<TopicPartition> assignOwnedPartitions() {
            // 创建一个列表，用于存储在此步骤中被确认为有效并保留分配的分区
            List<TopicPartition> assignedPartitions = new ArrayList<>();
            // 遍历 currentAssignment (消费者 -> 其拥有的分区列表) 的条目，使用迭代器以便安全删除
            for (Iterator<Entry<String, List<TopicPartition>>> it = currentAssignment.entrySet().iterator(); it.hasNext();) {
                // 获取当前条目 (消费者 -> 分区列表)
                Map.Entry<String, List<TopicPartition>> entry = it.next();
                // 获取消费者ID
                String consumer = entry.getKey();
                // 获取该消费者的订阅信息
                Subscription consumerSubscription = subscriptions.get(consumer);
                // 检查消费者是否仍然存在于订阅信息中
                if (consumerSubscription == null) {
                    // 如果一个之前存在（并且有一些分区分配）的消费者现在被移除了，则将其从 currentAssignment 中移除
                    // if a consumer that existed before (and had some partition assignments) is now removed, remove it from currentAssignment
                    // 遍历该已移除消费者拥有的所有分区
                    for (TopicPartition topicPartition: entry.getValue())
                        // 从 currentPartitionConsumer 映射中移除这些分区（因为它们不再被分配）
                        currentPartitionConsumer.remove(topicPartition);
                    // 从 currentAssignment 中移除该消费者的条目
                    it.remove();
                } else {
                    // 否则（消费者仍然存在）
                    // otherwise (the consumer still exists)
                    // 遍历该消费者拥有的分区列表，使用迭代器以便安全删除
                    for (Iterator<TopicPartition> partitionIter = entry.getValue().iterator(); partitionIter.hasNext();) {
                        // 获取当前分区
                        TopicPartition partition = partitionIter.next();
                        // 检查该分区的主题是否存在于 topic2AllPotentialConsumers (即主题是否仍然有效)
                        if (!topic2AllPotentialConsumers.containsKey(partition.topic())) {
                            // 如果该消费者的这个主题分区不再存在，则将其从该消费者的 currentAssignment 中移除
                            // if this topic partition of this consumer no longer exists, remove it from currentAssignment of the consumer
                            // 从消费者的分区列表中移除该分区
                            partitionIter.remove();
                            // 从 currentPartitionConsumer 映射中移除该分区
                            currentPartitionConsumer.remove(partition);
                        } else if (!consumerSubscription.topics().contains(partition.topic()) || rackInfo.racksMismatch(consumer, partition)) {
                            // 如果消费者不再订阅其主题，或者（对于机架感知分配）机架不匹配，则将其从消费者的 currentAssignment 中移除
                            // if the consumer is no longer subscribed to its topic or if racks don't match for rack-aware assignment,
                            // remove it from currentAssignment of the consumer
                            // 从消费者的分区列表中移除该分区
                            partitionIter.remove();
                            // 标记需要撤销（因为分区被移除了，可能需要通知其他组件）
                            revocationRequired = true;
                        } else {
                            // 否则，只有当其当前消费者仍然订阅其主题时，才将主题分区从那些需要分配的分区中移除
                            // （因为它已经被分配了，我们希望尽可能保留该分配）
                            // otherwise, remove the topic partition from those that need to be assigned only if
                            // its current consumer is still subscribed to its topic (because it is already assigned
                            // and we would want to preserve that assignment as much as possible)
                            // 将此有效且保留的分配添加到 assignedPartitions 列表中
                            assignedPartitions.add(partition);
                        }
                    }
                }
            }
            // 返回有效保留的分配列表
            return assignedPartitions;
        }

        /**
         * 通过计算 sortedPartitions（所有分区）和 sortedAssignedPartitions 的差集来获取未分配的分区列表。
         * 如果没有已分配的分区，则直接返回所有已排序的主题分区。
         *
         * 我们遍历 sortedPartition，并与 sortedAssignedPartitions 中的第 i 个元素（i 从 0 开始）进行比较：
         *   - 如果不等于第 i 个元素，则添加到 unassignedPartitions
         *   - 如果等于第 i 个元素，则从 sortedAssignedPartitions 中获取下一个元素
         *
         * @param sortedAssignedPartitions 已排序的已分配分区列表，这些分区都包含在 sortedAllPartitions（所有分区）中。
         * @return 未分配给任何当前消费者的分区列表。
         * @method getUnassignedPartitions
         * @description 计算出在所有分区中，哪些分区尚未被分配给任何消费者。
         *              应用场景：在处理完已拥有分区后，确定哪些分区需要进行新的分配。
         *              实现细节：
         *              1. 如果 `sortedAssignedPartitions` 为空，说明没有已分配的分区，直接返回 `sortedAllPartitions` (所有分区)。
         *              2. 创建一个空列表 `unassignedPartitions` 用于存储未分配的分区。
         *              3. 对 `sortedAssignedPartitions` 进行排序（使用 `PartitionComparator`，确保比较基准一致）。
         *              4. 使用双指针或迭代器的方式比较 `sortedAllPartitions` 和 `sortedAssignedPartitions`：
         *                 - 遍历 `sortedAllPartitions` 中的每个分区 `topicPartition`。
         *                 - 维护一个指向 `sortedAssignedPartitions` 当前元素的迭代器 `sortedAssignedPartitionsIter` 和当前元素 `nextAssignedPartition`。
         *                 - 如果 `topicPartition` 不等于 `nextAssignedPartition` (或者 `shouldAddDirectly` 标志为 true，表示 `sortedAssignedPartitions` 已遍历完毕)，则将 `topicPartition` 添加到 `unassignedPartitions`。
         *                 - 如果 `topicPartition` 等于 `nextAssignedPartition`，说明此分区已分配，则从 `sortedAssignedPartitionsIter` 获取下一个已分配分区。如果迭代器没有更多元素，则设置 `shouldAddDirectly = true`，后续 `sortedAllPartitions` 中的所有分区都将直接添加到 `unassignedPartitions`。
         *              5. 返回 `unassignedPartitions` 列表。
         *              设计考虑：通过对已排序列表进行比较，可以有效地找出差集。`shouldAddDirectly` 标志优化了当已分配分区列表遍历完后的处理。
         */
        private List<TopicPartition> getUnassignedPartitions(List<TopicPartition> sortedAssignedPartitions) {
            // 如果已分配分区列表为空
            if (sortedAssignedPartitions.isEmpty()) {
                // 直接返回所有已排序的主题分区，因为它们都未分配
                return sortedAllPartitions;
            }

            // 创建一个列表，用于存储未分配的分区
            List<TopicPartition> unassignedPartitions = new ArrayList<>();

            // 对已分配分区列表进行排序，使用 PartitionComparator 确保比较顺序一致
            // PartitionComparator 通常基于主题的分区数或消费者数，然后是主题名，最后是分区号
            sortedAssignedPartitions.sort(new PartitionComparator(topic2AllPotentialConsumers));

            // 标志位，指示是否应直接添加剩余的分区（当已分配分区列表已遍历完毕时）
            boolean shouldAddDirectly = false;
            // 获取已分配分区列表的迭代器
            Iterator<TopicPartition> sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator();
            // 获取已分配分区列表中的第一个分区
            TopicPartition nextAssignedPartition = sortedAssignedPartitionsIter.next();

            // 遍历所有已排序的主题分区 (sortedAllPartitions 是成员变量，已预先排序)
            for (TopicPartition topicPartition : sortedAllPartitions) {
                // 如果 shouldAddDirectly 为 true (表示已分配列表已用尽)，或者当前遍历到的所有分区中的分区不等于下一个已分配分区
                if (shouldAddDirectly || !nextAssignedPartition.equals(topicPartition)) {
                    // 将此分区添加到未分配分区列表中
                    unassignedPartitions.add(topicPartition);
                } else {
                    // 此分区在 assignedPartitions 中，不要添加到 unassignedPartitions，只需获取下一个已分配分区
                    // this partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition
                    // 检查已分配分区迭代器是否还有下一个元素
                    if (sortedAssignedPartitionsIter.hasNext()) {
                        // 获取下一个已分配分区
                        nextAssignedPartition = sortedAssignedPartitionsIter.next();
                    } else {
                        // 直接添加剩余的分区，因为 sortedAssignedPartitions 中没有更多元素了
                        // add the remaining directly since there is no more sortedAssignedPartitions
                        // 设置标志位，以便后续循环直接添加
                        shouldAddDirectly = true;
                    }
                }
            }
            // 返回未分配的分区列表
            return unassignedPartitions;
        }

        /**
         * 使用参数中的分区、消费者和年代信息更新 prevAssignment。
         * 应用场景：在处理消费者订阅信息，特别是处理不同年代的分配数据时，用于记录或更新分区的上一个分配状态。
         * 实现细节：遍历指定的分区列表，对于每个分区，如果它已经存在于 prevAssignment 中，则仅当新的年代大于现有年代时才更新；否则，直接添加新的分配信息。
         * 设计考虑：确保 prevAssignment 中存储的是最新的（年代最大的）上一个分配信息，这对于粘性分配器在判断分区是否可以“粘住”当前消费者时非常重要。
         *
         * @param prevAssignment   一个映射，键是 TopicPartition，值是 ConsumerGenerationPair，用于存储分区的上一个（第二大年代）分配信息。
         * @param partitions       需要更新到 prevAssignment 的分区列表。
         * @param consumer         消费者的 ID。
         * @param generation       本次分配（针对这些分区）的年代。
         */
        private void updatePrevAssignment(Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                                          List<TopicPartition> partitions,
                                          String consumer,
                                          int generation) {
            // 遍历传入的分区列表
            for (TopicPartition partition: partitions) {
                // 检查 prevAssignment 是否已包含该分区
                if (prevAssignment.containsKey(partition)) {
                    // 如果已包含，则只保留最新的上一个分配
                    // 比较当前传入的年代 (generation) 与 prevAssignment 中该分区的年代
                    if (generation > prevAssignment.get(partition).generation) {
                        // 如果当前年代更大，则更新 prevAssignment 中的记录
                        prevAssignment.put(partition, new ConsumerGenerationPair(consumer, generation));
                    }
                } else {
                    // 如果 prevAssignment 中不包含该分区，则直接添加新的分配信息
                    prevAssignment.put(partition, new ConsumerGenerationPair(consumer, generation));
                }
            }
        }

        /**
         * 从订阅信息中填充 prevAssignment。
         * 应用场景：在粘性分配开始前，根据消费者上次的分配信息（存储在用户数据中）来预填充 prevAssignment。
         *          这有助于识别哪些分区是“粘性”的，即哪些分区可以继续分配给同一个消费者。
         * 实现细节：
         * 1. 遍历所有消费者的订阅信息。
         * 2. 解析每个消费者订阅中的用户数据 (userData)，获取其上一次分配的分区和对应的年代 (generation)。
         * 3. 如果消费者的用户数据中记录的年代小于当前的最大年代 (maxGeneration)，则将其分区和年代信息更新到 prevAssignment 中。
         * 4. 如果消费者的用户数据中没有年代信息，但当前最大年代大于默认年代 (DEFAULT_GENERATION)，则将其分区视为默认年代并更新到 prevAssignment。
         * 设计考虑：
         * - 需要处理不同年代的分配数据，确保 prevAssignment 反映的是“上一个”有效的分配状态。
         * - 用户数据 (userData) 可能需要重置 (rewind) 以便多次读取。
         *
         * @param prevAssignment 一个映射，用于存储分区的上一个（第二大年代）分配信息，将被此方法填充。
         */
        private void prepopulateCurrentAssignments(Map<TopicPartition, ConsumerGenerationPair> prevAssignment) {
            // 我们需要处理每个消费者的订阅用户数据，并考虑其报告的年代
            // 在发生冲突时，较高的年代会覆盖较低的年代
            // 注意：只有当用户数据针对不同年代时才可能发生冲突

            // 遍历所有消费者的订阅条目
            for (Map.Entry<String, Subscription> subscriptionEntry: subscriptions.entrySet()) {
                // 获取消费者ID
                String consumer = subscriptionEntry.getKey();
                // 获取消费者的订阅信息
                Subscription subscription = subscriptionEntry.getValue();
                // 检查订阅中是否包含用户数据
                if (subscription.userData() != null) {
                    // 因为这可能是我们第二次反序列化 memberData，所以需要重置 userData 的读取位置
                    subscription.userData().rewind();
                }

                // 从订阅信息中解析出 MemberData（包含分区和年代信息）
                MemberData memberData = memberData(subscription);

                // 我们已经有了 maxGeneration（当前组的最大年代）信息，所以只需比较 memberData 的当前年代，并放入 prevAssignment
                // 检查 memberData 是否包含年代信息，并且该年代小于当前的最大年代
                if (memberData.generation.isPresent() && memberData.generation.get() < maxGeneration) {
                    // 如果当前成员的年代低于 maxGeneration，则根据需要将其分区更新到 prevAssignment
                    updatePrevAssignment(prevAssignment, memberData.partitions, consumer, memberData.generation.get());
                // 检查 memberData 是否没有年代信息，并且 maxGeneration 大于默认年代
                } else if (memberData.generation.isEmpty() && maxGeneration > DEFAULT_GENERATION) {
                    // 如果 maxGeneration 大于 DEFAULT_GENERATION
                    // 则将所有（没有年代的）分区作为 DEFAULT_GENERATION 更新到 prevAssignment（如果需要）
                    updatePrevAssignment(prevAssignment, memberData.partitions, consumer, DEFAULT_GENERATION);
                }
            }
        }

        /**
         * 判断当前分配是否是平衡的。
         * 应用场景：在粘性分配的优化阶段，检查当前的分配方案是否达到了平衡状态。一个平衡的分配意味着消费者之间分配到的分区数量差异尽可能小。
         * 实现细节：
         * 1. 首先进行快速检查：如果分配给消费者分区数最少和最多的差异不超过1，则认为是平衡的。
         * 2. 如果快速检查未通过，则进行更详细的检查：
         *    a. 创建一个从分区到其所属消费者的映射。
         *    b. 遍历每个消费者，如果该消费者尚未获得其可获得的最大分区数：
         *       i. 遍历该消费者订阅的所有主题的所有分区。
         *       ii. 如果某个分区未分配给当前消费者，而是分配给了另一个消费者。
         *       iii. 检查将该分区从另一个消费者移动到当前消费者是否会破坏平衡（即，移动后，当前消费者的分区数是否仍然远小于另一个消费者的分区数）。
         *       iv. 如果发现任何可以移动以改善平衡的分区，则认为当前分配不平衡。
         * 设计考虑：
         * - 平衡性是粘性分配的一个重要目标，但不是唯一目标（粘性是首要目标）。此方法用于在保持粘性的前提下，尽可能达到平衡。
         * - 详细检查的逻辑确保了不会因为移动一个分区而导致分配更加不平衡。
         *
         * @return 如果给定的分配是平衡的，则返回 true；否则返回 false。
         */
        private boolean isBalanced() {
            // 获取当前分配中，分配给分区数最少的消费者的分区数
            int min = currentAssignment.get(sortedCurrentSubscriptions.first()).size();
            // 获取当前分配中，分配给分区数最多的消费者的分区数
            int max = currentAssignment.get(sortedCurrentSubscriptions.last()).size();
            // 如果分配给消费者的最小和最大分区数相差最多为1，则返回true
            if (min >= max - 1)
                return true;

            // 创建一个从分区到分配给它们的消费者的映射
            final Map<TopicPartition, String> allPartitions = new HashMap<>();
            // 获取当前分配的所有条目（消费者 -> 分区列表）
            Set<Entry<String, List<TopicPartition>>> assignments = currentAssignment.entrySet();
            // 遍历每个分配条目
            for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
                // 获取当前消费者分配到的分区列表
                List<TopicPartition> topicPartitions = entry.getValue();
                // 遍历这些分区
                for (TopicPartition topicPartition: topicPartitions) {
                    // 检查该分区是否已经被分配给了其他消费者（理论上不应该发生）
                    if (allPartitions.containsKey(topicPartition))
                        // 如果是，则记录错误日志
                        log.error("{} is assigned to more than one consumer.", topicPartition);
                    // 将分区和其对应的消费者存入 allPartitions 映射
                    allPartitions.put(topicPartition, entry.getKey());
                }
            }

            // 对于每个没有获得其能获得的所有主题分区的消费者，
            // 确保它没有获得的主题分区中，没有任何一个可以移动给它（因为那样会破坏平衡）
            // 遍历已排序的当前订阅的消费者列表
            for (String consumer: sortedCurrentSubscriptions) {
                // 获取当前消费者分配到的分区列表
                List<TopicPartition> consumerPartitions = currentAssignment.get(consumer);
                // 获取当前消费者分配到的分区数量
                int consumerPartitionCount = consumerPartitions.size();

                // 如果此消费者已经获得了它能获得的所有主题分区，则跳过
                // 获取当前消费者订阅的所有潜在主题列表
                List<String> allSubscribedTopics = consumer2AllPotentialTopics.get(consumer);
                // 计算该消费者在这些主题上最多可以分配到的分区数
                int maxAssignmentSize = getMaxAssignmentSize(allSubscribedTopics);

                // 如果当前消费者已分配的分区数等于其最大可分配数，则继续下一个消费者
                if (consumerPartitionCount == maxAssignmentSize)
                    continue;

                // 否则，确保它不能再获得更多分区（因为移动会导致不平衡）
                // 遍历该消费者订阅的所有主题
                for (String topic: allSubscribedTopics) {
                    // 获取该主题的总分区数
                    int partitionCount = partitionsPerTopic.get(topic).size();
                    // 遍历该主题的所有分区索引
                    for (int i = 0; i < partitionCount; i++) {
                        // 创建 TopicPartition 对象
                        TopicPartition topicPartition = new TopicPartition(topic, i);
                        // 如果当前消费者没有分配到这个分区
                        if (!currentAssignment.get(consumer).contains(topicPartition)) {
                            // 获取当前持有该分区的其他消费者
                            String otherConsumer = allPartitions.get(topicPartition);
                            // 获取其他消费者当前分配到的分区数量
                            int otherConsumerPartitionCount = currentAssignment.get(otherConsumer).size();
                            // 如果将此分区移动给当前消费者后，当前消费者的分区数仍然比其他消费者的分区数少于1个以上（即移动后仍然不平衡）
                            // 或者说，如果当前消费者增加一个分区后，其数量仍然严格小于另一个消费者的数量减一（这意味着移动是安全的，不会让另一个消费者分区过少）
                            // 这里的条件是 consumerPartitionCount + 1 < otherConsumerPartitionCount
                            // 意味着如果把 otherConsumer 的一个分区给 consumer，consumer 的分区数 (consumerPartitionCount + 1)
                            // 仍然小于 otherConsumer 减少一个分区后的数量 (otherConsumerPartitionCount - 1)
                            // 这种情况说明移动是可行的，并且可以使分配更平衡，因此当前分配是不平衡的
                            if (consumerPartitionCount + 1 < otherConsumerPartitionCount) {
                                // 记录调试信息，说明可以移动分区以获得更平衡的分配
                                log.debug("{} can be moved from consumer {} to consumer {} for a more balanced assignment.",
                                        topicPartition, otherConsumer, consumer);
                                // 返回 false，表示当前分配不平衡
                                return false;
                            }
                        }
                    }
                }
            }
            // 如果所有检查都通过，则返回 true，表示当前分配是平衡的
            return true;
        }

        /**
         * 获取给定消费者订阅的所有主题 ({@code allSubscribedTopics}) 中，该消费者理论上可以分配到的最大分区数量。
         * 应用场景：在判断分配是否平衡或在进行分区移动决策时，需要知道一个消费者最多能承载多少分区。
         * 实现细节：
         * 1. 如果消费者订阅了集群中的所有主题，则其最大可分配分区数等于集群总分区数 (totalPartitionsCount)。
         * 2. 否则，最大可分配分区数等于该消费者订阅的各个主题的分区数之和。
         * 设计考虑：这个方法帮助确定一个消费者的“容量上限”，用于平衡性检查和优化分配。
         *
         * @param allSubscribedTopics 一个消费者订阅的所有主题列表。
         * @return 该消费者在这些订阅主题上可以分配到的最大分区数量。
         */
        private int getMaxAssignmentSize(List<String> allSubscribedTopics) {
            // 声明最大分配大小变量
            int maxAssignmentSize;
            // 检查消费者订阅的主题数量是否等于集群中所有主题的数量
            if (allSubscribedTopics.size() == partitionsPerTopic.size()) {
                // 如果是，则该消费者可以分配的最大分区数是集群的总分区数
                maxAssignmentSize = totalPartitionsCount;
            } else {
                // 否则，计算该消费者订阅的特定主题的分区总数
                // 使用 stream API：
                // 1. allSubscribedTopics.stream(): 将订阅的主题列表转换为流
                // 2. .map(partitionsPerTopic::get): 对每个主题，从 partitionsPerTopic 映射中获取其分区列表 (List<PartitionInfo>)
                // 3. .map(List::size): 对每个分区列表，获取其大小（即分区数量）
                // 4. .reduce(0, Integer::sum): 将所有主题的分区数量累加起来，初始值为0
                maxAssignmentSize = allSubscribedTopics.stream().map(partitionsPerTopic::get).map(List::size).reduce(0, Integer::sum);
            }
            // 返回计算出的最大分配大小
            return maxAssignmentSize;
        }

        /**
         * 计算给定分配方案的平衡分数。
         * 平衡分数定义为所有消费者对之间分配分区数量差异的总和。
         * 一个完美平衡的分配（所有消费者获得相同数量的分区）的平衡分数为 0。
         * 平衡分数越低，表示分配越平衡。
         * 应用场景：在粘性分配过程中，可能需要评估不同分配方案的平衡程度，以便选择最优方案或判断是否需要进一步优化。
         * 实现细节：
         * 1. 创建一个映射，存储每个消费者及其分配到的分区数量。
         * 2. 遍历所有消费者对，计算每对消费者分配分区数量的差的绝对值，并累加到总分中。
         * 设计考虑：提供一个量化的指标来衡量分配的平衡性。这个分数可以用于比较不同的分配策略或同一策略在不同迭代中的效果。
         *
         * @param assignment 一个映射，表示当前的分配方案，键是消费者ID，值是分配给该消费者的 TopicPartition 列表。
         * @return 给定分配方案的平衡分数。
         */
        private int getBalanceScore(Map<String, List<TopicPartition>> assignment) {
            // 初始化平衡分数为0
            int score = 0;

            // 创建一个映射，用于存储每个消费者分配到的分区数量
            Map<String, Integer> consumer2AssignmentSize = new HashMap<>();
            // 遍历输入的分配方案
            for (Entry<String, List<TopicPartition>> entry: assignment.entrySet())
                // 将消费者ID和其分配到的分区数量存入映射
                consumer2AssignmentSize.put(entry.getKey(), entry.getValue().size());

            // 获取 consumer2AssignmentSize 映射的条目迭代器
            Iterator<Entry<String, Integer>> it = consumer2AssignmentSize.entrySet().iterator();
            // 当迭代器还有下一个元素时循环
            while (it.hasNext()) {
                // 获取当前消费者的分配条目（消费者ID -> 分区数）
                Entry<String, Integer> entry = it.next();
                // 获取当前消费者的分区数量
                int consumerAssignmentSize = entry.getValue();
                // 从迭代的集合中移除当前条目，以避免重复比较和与自身比较
                it.remove(); // 这会修改 consumer2AssignmentSize
                // 遍历 consumer2AssignmentSize 中剩余的其他消费者条目
                for (Entry<String, Integer> otherEntry: consumer2AssignmentSize.entrySet())
                    // 将当前消费者与另一个消费者的分区数量之差的绝对值累加到分数中
                    score += Math.abs(consumerAssignmentSize - otherEntry.getValue());
            }

            // 返回计算出的平衡分数
            return score;
        }

        /**
         * 尝试将指定分区分配给一个合适的消费者，目标是改善分区分配的整体平衡性。
         *
         * @param partition 要分配的主题分区。
         * @param rackInfo 机架信息，用于机架感知分配。如果为 null，则不执行机架匹配。
         * @return 如果分区成功分配给某个消费者，则返回 true；否则返回 false。
         *
         * 应用场景：
         * 在分区分配过程中，当需要为一个未分配的分区寻找一个合适的消费者时调用此方法。
         * 它会遍历当前排序的消费者列表，尝试找到第一个可以接收该分区的消费者。
         *
         * 实现细节：
         * 1. 遍历 `sortedCurrentSubscriptions` (按某种顺序排序的当前订阅的消费者列表)。
         * 2. 对于每个消费者，检查其是否订阅了该分区的主题 (`consumer2AllPotentialTopics.get(consumer).contains(partition.topic())`)。
         * 3. 同时检查机架约束：如果 `rackInfo` 不为 null，则检查消费者和分区的机架是否不匹配 (`!rackInfo.racksMismatch(consumer, partition)`)。
         * 4. 如果消费者满足上述条件，则将该分区分配给该消费者：
         *    a. 从 `sortedCurrentSubscriptions` 中移除该消费者（因为其负载增加了，排序位置可能需要调整）。
         *    b. 将分区添加到该消费者的当前分配列表 (`currentAssignment.get(consumer).add(partition)`)。
         *    c. 记录该分区与消费者的映射关系 (`currentPartitionConsumer.put(partition, consumer)`)。
         *    d. 将该消费者重新添加到 `sortedCurrentSubscriptions` 中（其在排序列表中的位置可能会因为负载变化而改变）。
         *    e. 返回 true，表示分配成功。
         * 5. 如果遍历完所有消费者都没有找到合适的，则返回 false。
         *
         * 设计考虑：
         * - 通过 `sortedCurrentSubscriptions` 列表，可以根据某种策略（例如，负载最轻的消费者优先）来选择消费者，从而达到更好的平衡。
         * - 机架感知逻辑 (`rackInfo == null || !rackInfo.racksMismatch(consumer, partition)`) 确保在可能的情况下，分区被分配给同一机架上的消费者，以减少跨机架流量。
         * - 对 `sortedCurrentSubscriptions` 的移除和重新添加操作是为了在分配过程中动态维护消费者负载的排序，确保后续分配决策基于最新的状态。
         */
        private boolean maybeAssignPartition(TopicPartition partition, RackInfo rackInfo) {
            // 遍历当前已排序的消费者订阅列表
            for (String consumer: sortedCurrentSubscriptions) {
                // 检查当前消费者是否订阅了该分区的主题，并且（如果提供了机架信息）消费者和分区的机架不冲突
                if (consumer2AllPotentialTopics.get(consumer).contains(partition.topic()) && (rackInfo == null || !rackInfo.racksMismatch(consumer, partition))) {
                    // 从已排序的订阅列表中移除该消费者（因为其分配的分区数即将改变，需要重新排序）
                    sortedCurrentSubscriptions.remove(consumer);
                    // 将该分区添加到该消费者的当前分配列表中
                    currentAssignment.get(consumer).add(partition);
                    // 记录该分区当前分配给了哪个消费者
                    currentPartitionConsumer.put(partition, consumer);
                    // 将该消费者重新添加到已排序的订阅列表中（其排序位置可能会改变）
                    sortedCurrentSubscriptions.add(consumer);
                    // 分配成功，返回true
                    return true;
                }
            }
            // 如果没有找到合适的消费者，则分配失败，返回false
            return false;
        }

        /**
         * 尝试分配所有未分配的分区。
         *
         * @param unassignedPartitions 仍然未分配的分区列表。
         * @param rackInfo 机架信息，用于匹配机架。如果为 null，则不执行机架匹配。
         * @param removeAssigned 标志，指示是否应从 `unassignedPartitions` 列表中移除已成功分配的分区。
         *
         * 应用场景：
         * 在分区分配流程中，用于批量处理一批未分配的分区，尝试将它们分配给合适的消费者。
         * 可以分阶段调用，例如先尝试带机架感知的分配，再尝试不带机架感知的分配。
         *
         * 实现细节：
         * 1. 遍历 `unassignedPartitions` 列表中的每个分区。
         * 2. 对于每个分区，首先检查该分区所属的主题是否有任何潜在的消费者 (`topic2AllPotentialConsumers.get(partition.topic()).isEmpty()`)。
         *    如果没有潜在消费者，则跳过该分区。
         * 3. 调用 `maybeAssignPartition(partition, rackInfo)` 方法尝试为当前分区找到一个合适的消费者并进行分配。
         * 4. 如果 `maybeAssignPartition` 返回 true (表示分配成功) 并且 `removeAssigned` 标志为 true，则从 `unassignedPartitions` 列表中移除该分区。
         *
         * 设计考虑：
         * - 通过 `removeAssigned` 参数，调用者可以控制是否在分配成功后修改传入的 `unassignedPartitions` 列表。这在分阶段分配（例如，先尝试机架匹配，再尝试无机架匹配）时很有用，可以避免重复处理已分配的分区。
         * - 检查主题是否有潜在消费者 (`topic2AllPotentialConsumers`) 是一个优化，避免了对那些不可能被分配的分区进行不必要的尝试。
         */
        private void maybeAssign(List<TopicPartition> unassignedPartitions, RackInfo rackInfo, boolean removeAssigned) {
            // 遍历所有未分配的分区
            for (Iterator<TopicPartition> iter = unassignedPartitions.iterator(); iter.hasNext();) {
                // 获取下一个未分配的分区
                TopicPartition partition = iter.next();
                // 如果该分区所属的主题没有任何潜在的消费者，则跳过该分区
                if (topic2AllPotentialConsumers.get(partition.topic()).isEmpty())
                    continue;

                // 尝试将该分区分配给一个消费者（可能考虑机架信息）
                // 如果分配成功并且 removeAssigned 标志为true
                if (maybeAssignPartition(partition, rackInfo) && removeAssigned)
                    // 从未分配分区列表中移除该分区
                    iter.remove();
            }
        }

        /**
         * 判断一个主题是否可以参与重新分配。
         *
         * @param topic 主题名称。
         * @return 如果该主题至少有两个潜在的消费者，则返回 true，表示该主题下的分区可以被重新分配；否则返回 false。
         *
         * 应用场景：
         * 在进行分区平衡调整时，用于确定哪些主题的分区是“可移动”的。
         * 如果一个主题只有一个潜在消费者，那么它的分区基本上是固定的，无法移动到其他消费者，因此不参与重新分配。
         *
         * 实现细节：
         * - 获取指定主题的所有潜在消费者列表 (`topic2AllPotentialConsumers.get(topic)`)。
         * - 检查该列表的大小是否大于等于2。
         *
         * 设计考虑：
         * - 这个条件（至少两个潜在消费者）是分区能够被移动的前提。如果只有一个消费者对某个主题感兴趣，那么该主题的所有分区都只能分配给这个消费者。
         * - 通过此检查，可以缩小重新分配的范围，提高平衡算法的效率。
         */
        private boolean canTopicParticipateInReassignment(String topic) {
            // 如果一个主题至少有两个潜在的消费者，那么它就可以参与重新分配。
            return topic2AllPotentialConsumers.get(topic).size() >= 2;
        }

        /**
         * 判断一个消费者是否可以参与重新分配。
         *
         * @param consumer 消费者ID。
         * @return 如果该消费者可以参与重新分配，则返回 true；否则返回 false。
         *
         * 应用场景：
         * 在进行分区平衡调整时，用于确定哪些消费者是“可调整”的，即其拥有的分区可以被移走，或者它可以接收新的分区。
         *
         * 实现细节：
         * 1. 获取该消费者当前分配到的分区列表 (`currentPartitions`) 及其数量 (`currentAssignmentSize`)。
         * 2. 获取该消费者订阅的所有主题列表 (`allSubscribedTopics`)。
         * 3. 计算该消费者理论上可以分配到的最大分区数 (`maxAssignmentSize`)，基于其订阅的主题和这些主题的总分区数。
         * 4. 检查异常情况：如果当前分配的分区数超过了理论最大值，记录错误日志。
         * 5. 条件一：如果当前分配的分区数小于理论最大值 (`currentAssignmentSize < maxAssignmentSize`)，则该消费者可以接收更多分区，因此可以参与重新分配，返回 true。
         * 6. 条件二：遍历该消费者当前拥有的所有分区。
         *    - 对于每个分区，调用 `canTopicParticipateInReassignment(partition.topic())` 检查该分区所属的主题是否可以参与重新分配。
         *    - 如果任何一个已分配给该消费者的分区所属的主题可以参与重新分配，则意味着该消费者拥有的某些分区是“可移动”的，因此该消费者本身也可以参与重新分配，返回 true。
         * 7. 如果以上条件都不满足，则返回 false。
         *
         * 设计考虑：
         * - 一个消费者可以参与重新分配的条件是：它要么没有达到其分配上限（可以接收更多分区），要么它当前拥有的某些分区是可以被移动到其他消费者的。
         * - `maxAssignmentSize` 的计算逻辑（未在此方法中直接体现，通过 `getMaxAssignmentSize` 调用）是关键，它定义了消费者在理想均衡状态下应该承载的分区数量上限。
         * - 通过此检查，可以缩小重新分配的范围，只关注那些真正有调整空间的消费者，提高平衡算法的效率。
         */
        private boolean canConsumerParticipateInReassignment(String consumer) {
            // 获取该消费者当前分配到的分区列表
            List<TopicPartition> currentPartitions = currentAssignment.get(consumer);
            // 获取当前分配给该消费者的分区数量
            int currentAssignmentSize = currentPartitions.size();
            // 获取该消费者订阅的所有主题列表
            List<String> allSubscribedTopics = consumer2AllPotentialTopics.get(consumer);
            // 计算该消费者基于其订阅的主题所能分配到的最大分区数
            int maxAssignmentSize = getMaxAssignmentSize(allSubscribedTopics);

            // 异常情况检查：如果消费者当前分配的分区数超过了其理论上能分配的最大分区数
            if (currentAssignmentSize > maxAssignmentSize)
                // 记录错误日志
                log.error("The consumer {} is assigned more partitions than the maximum possible.", consumer);
 
            // 如果消费者当前分配的分区数小于其理论最大值，意味着它可以接收更多分区，因此可以参与重新分配
            if (currentAssignmentSize < maxAssignmentSize)
                // 如果一个消费者没有被分配其所有潜在的分区，它就符合重新分配的条件
                return true;

            // 遍历该消费者当前拥有的所有分区
            for (TopicPartition partition: currentPartitions)
                // 如果分配给某个消费者的任何分区符合重新分配的条件，则该消费者本身也符合重新分配的条件
                // 检查该分区所属的主题是否可以参与重新分配
                if (canTopicParticipateInReassignment(partition.topic()))
                    // 如果是，则该消费者可以参与重新分配
                    return true;
            // 如果以上条件都不满足，则该消费者不参与重新分配
            return false;
        }

        /**
         * 使用在 assignPartitions(...) 方法中创建的数据结构来平衡当前的分配方案。
         * 这是粘性分配策略中实现分区平衡的核心逻辑。
         *
         * @param prevAssignment 上一次成功分配的结果，包含了每个分区之前由哪个消费者（及其代）拥有的信息。
         *                       用于在重新分配时尽可能保持分区的粘性。
         * @param unassignedPartitions 当前所有未分配的分区列表。
         *
         * 应用场景：
         * 在初始分配或再均衡过程中，当所有消费者和分区的基本信息收集完毕后，调用此方法进行实际的平衡操作。
         * 目标是在满足粘性（尽量少移动分区）和机架感知（尽量同机架分配）的前提下，使分区在消费者之间尽可能均匀分布。
         *
         * 实现细节：
         * 1.  **判断是否初始化阶段**: 检查 `sortedCurrentSubscriptions` 中最后一个消费者（通常是负载最轻或新加入的）的当前分配是否为空，以此判断是否为初始分配阶段 (`initializing`)。
         * 2.  **初步分配未分配分区**: 
         *     a.  首先尝试使用机架感知进行分配：如果 `rackInfo.consumerRacks` 不为空（即配置了消费者机架信息），则调用 `maybeAssign(partitionsToAssign, rackInfo, true)` 尝试将 `unassignedPartitions` 分配给同一机架的消费者。`partitionsToAssign` 是 `unassignedPartitions` 的一个副本，`true` 表示成功分配后从该副本中移除。
         *     b.  然后，对剩余的（或所有的，如果未进行机架感知分配）未分配分区，调用 `maybeAssign(partitionsToAssign, null, false)` 进行不考虑机架的分配。`false` 表示即使分配成功也不从 `partitionsToAssign` (此时可能是原始的 `unassignedPartitions` 或其剩余部分) 中移除，因为后续的重新分配步骤会处理所有分区。
         * 3.  **缩小重新分配范围 (分区层面)**:
         *     a.  识别出那些不能参与重新分配的“固定分区” (`fixedPartitions`)。这些分区所属的主题不满足 `canTopicParticipateInReassignment` 条件（即潜在消费者少于2个）。
         *     b.  从 `sortedAllPartitions` (所有需要考虑的分区) 和 `unassignedPartitions` 中移除这些固定分区，因为它们的分配不会改变。
         * 4.  **缩小重新分配范围 (消费者层面)**:
         *     a.  识别出那些不能参与重新分配的“固定消费者”及其分配 (`fixedAssignments`)。这些消费者不满足 `canConsumerParticipateInReassignment` 条件。
         *     b.  从 `sortedCurrentSubscriptions` (参与分配的消费者) 中移除这些固定消费者，并将其当前的分配从 `currentAssignment` 中移出，暂存到 `fixedAssignments`。
         * 5.  **备份当前分配状态**: 创建当前分配方案 (`currentAssignment`) 和分区到消费者的映射 (`currentPartitionConsumer`) 的深拷贝 (`preBalanceAssignment`, `preBalancePartitionConsumers`)。这是为了在后续的重新分配尝试未能改善平衡性时可以回滚。
         * 6.  **执行重新分配 (阶段一，可选)**:
         *     a.  如果 `!revocationRequired` (即没有因为订阅变化而必须撤销某些分区)，则首先尝试仅通过移动新加入的未分配分区 (`unassignedPartitions`) 来进行平衡调整，调用 `performReassignments(unassignedPartitions, prevAssignment)`。
         * 7.  **执行重新分配 (阶段二，主要)**:
         *     a.  对所有可参与重新分配的分区 (`sortedAllPartitions`) 执行重新分配，调用 `performReassignments(sortedAllPartitions, prevAssignment)`。`reassignmentPerformed` 记录此次操作是否实际执行了任何分区移动。
         * 8.  **评估并可能回滚**: 
         *     a.  如果不是初始化阶段 (`!initializing`)，并且确实执行了重新分配 (`reassignmentPerformed`)，并且新的分配方案的平衡得分 (`getBalanceScore(currentAssignment)`) 不优于（大于或等于）重新分配前的得分 (`getBalanceScore(preBalanceAssignment)`)，则认为此次重新分配没有带来改善或反而恶化了平衡。
         *     b.  在这种情况下，回滚到重新分配前的状态：将 `preBalanceAssignment` 的内容深拷贝回 `currentAssignment`，并恢复 `currentPartitionConsumer`。
         * 9.  **恢复固定分配**: 将之前暂存的 `fixedAssignments` (那些不能改变的消费者及其分区) 添加回 `currentAssignment` 和 `sortedCurrentSubscriptions`。
         * 10. **清理**: 清空 `fixedAssignments` 映射。
         *
         * 设计考虑：
         * - **分阶段分配与平衡**: 先处理未分配分区，再对所有可调整分区进行平衡，有助于逐步达到稳定状态。
         * - **粘性与平衡的权衡**: `performReassignments` 方法内部会考虑 `prevAssignment` 来尽量保持粘性。而 `getBalanceScore` 用于评估整体平衡性。当粘性保持操作导致平衡性下降时，可能会选择牺牲部分粘性以获得更好的平衡，或者回滚以保持之前的平衡状态。
         * - **范围限制**: 通过 `canTopicParticipateInReassignment` 和 `canConsumerParticipateInReassignment` 缩小了需要进行复杂平衡计算的分区和消费者的范围，提高了效率。
         * - **回滚机制**: 确保只有在重新分配确实改善了平衡性的情况下才接受新的分配方案，否则保持之前的状态，这有助于避免不必要的抖动。
         * - **机架感知**: 在初始分配阶段就考虑机架因素，尝试优化局部性。
         */
        private void balance(Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                             List<TopicPartition> unassignedPartitions) {
            // 判断是否是初始化分配阶段（即当前订阅列表中最后一个消费者的分配列表为空）
            boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();

            // 首先尝试使用机架匹配来分配，然后分配所有剩余的（不进行机架匹配）
            // partitionsToAssign 最初指向 unassignedPartitions
            List<TopicPartition> partitionsToAssign = unassignedPartitions;
            // 如果消费者的机架信息不为空（即启用了机架感知）
            if (!rackInfo.consumerRacks.isEmpty()) {
                // 创建 unassignedPartitions 的一个新链表副本，用于机架感知分配
                partitionsToAssign = new LinkedList<>(unassignedPartitions);
                // 尝试使用机架信息分配这些分区，如果分配成功则从 partitionsToAssign 中移除
                maybeAssign(partitionsToAssign, rackInfo, true);
            }
            // 尝试分配剩余的（或所有的，如果未进行机架感知分配）partitionsToAssign 中的分区，不考虑机架信息，并且不从列表中移除已分配的
            maybeAssign(partitionsToAssign, null, false);

            // 缩小重新分配的范围，只包括那些实际可以被重新分配的分区
            Set<TopicPartition> fixedPartitions = new HashSet<>();
            // 遍历所有主题的潜在消费者信息
            for (String topic: topic2AllPotentialConsumers.keySet())
                // 如果该主题不能参与重新分配（例如，只有一个潜在消费者）
                if (!canTopicParticipateInReassignment(topic)) {
                    // 将该主题下的所有分区标记为固定分区
                    for (int i = 0; i < partitionsPerTopic.get(topic).size(); i++) {
                        fixedPartitions.add(new TopicPartition(topic, i));
                    }
                }
            // 从所有分区列表和未分配分区列表中移除这些固定分区
            sortedAllPartitions.removeAll(fixedPartitions);
            unassignedPartitions.removeAll(fixedPartitions);

            // 缩小重新分配的范围，只包括那些可以参与重新分配的消费者
            Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
            // 遍历所有消费者的潜在主题信息
            for (String consumer: consumer2AllPotentialTopics.keySet())
                // 如果该消费者不能参与重新分配
                if (!canConsumerParticipateInReassignment(consumer)) {
                    // 从已排序的当前订阅列表中移除该消费者
                    sortedCurrentSubscriptions.remove(consumer);
                    // 将该消费者的当前分配标记为固定分配，并从当前分配中移除
                    fixedAssignments.put(consumer, currentAssignment.remove(consumer));
                }

            // 创建当前分配状态的深拷贝，以便在重新分配未能改善平衡时可以回滚
            Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
            Map<TopicPartition, String> preBalancePartitionConsumers = new HashMap<>(currentPartitionConsumer);

            // 如果我们不需要因为订阅变化而撤销某些分区（revocationRequired为false），
            // 则首先尝试仅通过移动新添加的未分配分区来进行平衡
            if (!revocationRequired) {
                performReassignments(unassignedPartitions, prevAssignment);
            }

            // 对所有可重新分配的分区执行重新分配操作
            boolean reassignmentPerformed = performReassignments(sortedAllPartitions, prevAssignment);

            // 如果我们不是在保留现有分配（即不是初始化阶段），并且对当前分配进行了更改，
            // 确保我们得到了一个更平衡的分配；否则，恢复到之前的分配
            if (!initializing && reassignmentPerformed && getBalanceScore(currentAssignment) >= getBalanceScore(preBalanceAssignment)) {
                // 如果新的分配方案的平衡得分没有更好（甚至更差），则回滚
                deepCopy(preBalanceAssignment, currentAssignment);
                currentPartitionConsumer.clear();
                currentPartitionConsumer.putAll(preBalancePartitionConsumers);
            }

            // 将之前标记为固定的分配（那些不能改变的）添加回当前分配中
            for (Entry<String, List<TopicPartition>> entry: fixedAssignments.entrySet()) {
                String consumer = entry.getKey();
                // 将固定分配添加回消费者的分配列表
                currentAssignment.put(consumer, entry.getValue());
                // 将该消费者添加回已排序的订阅列表
                sortedCurrentSubscriptions.add(consumer);
            }

            // 清理固定分配的临时存储
            fixedAssignments.clear();
        }

        /**
         * 执行分区重新分配。
         * 此方法会迭代地重新分配可重新分配的分区，直到无法通过移动分区来改善平衡状态，或者处理完所有可重新分配的分区。
         * 它会考虑之前的分配情况以及机架感知，以优化分区的分配。
         * 
         * 应用场景：在初始分配或后续调整中，当需要根据当前消费者负载和分区状态优化分配方案时调用。
         * 实现细节：
         * 1. 循环直到没有更多的分区可以移动以改善平衡。
         * 2. 遍历可重新分配的分区列表。
         * 3. 对于每个分区，检查其是否满足重新分配的条件（例如，至少有两个潜在消费者，当前有消费者）。
         * 4. 优先考虑将分区重新分配给其先前所属的消费者（如果这样做可以改善平衡）。
         * 5. 尝试进行机架感知的重新分配：如果当前消费者的机架与分区的某个副本机架匹配，则尝试将分区分配给同一机架上负载较轻的其他消费者。
         * 6. 如果未进行机架感知的重新分配，则尝试将分区分配给任何一个负载较轻的其他潜在消费者。
         * 设计考虑：
         * - 通过迭代和 `modified` 标志确保达到一个相对稳定的平衡状态。
         * - 优先考虑先前分配和机架感知，以在保持粘性的同时优化局部性。
         * - 错误日志记录了不符合预期的分区状态，有助于调试。
         * 
         * @param reassignablePartitions 可重新分配的分区列表。
         * @param prevAssignment 先前的分区分配情况，映射了分区到其消费者和代际。
         * @return 如果执行了任何重新分配，则返回 true；否则返回 false。
         */
        private boolean performReassignments(List<TopicPartition> reassignablePartitions,
                                             Map<TopicPartition, ConsumerGenerationPair> prevAssignment) {
            // 标记是否执行了重新分配
            boolean reassignmentPerformed = false;
            // 标记在一次迭代中是否修改了分配
            boolean modified;

            // 重复重新分配，直到没有分区可以移动以改善平衡
            do {
                // 每次迭代开始时，假设没有修改
                modified = false;
                // 重新分配所有可重新分配的分区（从潜在消费者最少的分区开始，如果需要）
                // 直到处理完整个列表或达到平衡状态
                Iterator<TopicPartition> partitionIterator = reassignablePartitions.iterator();
                // 遍历可重新分配的分区，同时检查是否已达到平衡状态
                while (partitionIterator.hasNext() && !isBalanced()) {
                    // 获取当前要处理的分区
                    TopicPartition partition = partitionIterator.next();

                    // 分区必须至少有两个潜在消费者
                    if (topic2AllPotentialConsumers.get(partition.topic()).size() <= 1)
                        // 如果潜在消费者不足，记录错误日志
                        log.error("Expected more than one potential consumer for partition '{}'", partition);

                    // 分区必须有一个当前的消费者
                    String consumer = currentPartitionConsumer.get(partition);
                    // 如果分区没有当前消费者，记录错误日志
                    if (consumer == null)
                        log.error("Expected partition '{}' to be assigned to a consumer", partition);

                    // 检查是否可以将分区重新分配给其先前的消费者
                    // 条件：分区在先前的分配中存在，并且当前消费者拥有的分区数比先前消费者拥有的分区数多一个以上
                    if (prevAssignment.containsKey(partition) &&
                            currentAssignment.get(consumer).size() > currentAssignment.get(prevAssignment.get(partition).consumer).size() + 1) {
                        // 将分区重新分配给其先前的消费者
                        reassignPartition(partition, prevAssignment.get(partition).consumer);
                        // 标记已执行重新分配
                        reassignmentPerformed = true;
                        // 标记已修改分配
                        modified = true;
                        // 继续处理下一个可重新分配的分区
                        continue;
                    }

                    // 检查是否存在更适合该分区的消费者；如果存在，则重新分配它
                    // 如果可能，优先使用同一机架内的消费者
                    // 获取当前消费者的机架信息
                    String consumerRack = rackInfo.consumerRacks.get(consumer);
                    // 获取分区的机架信息集合
                    Set<String> partitionRacks = rackInfo.partitionRacks.get(partition);
                    // 标记是否找到了同一机架的消费者
                    boolean foundRackConsumer = false;
                    // 如果当前消费者有机架信息，分区有机架信息，并且消费者的机架在分区的机架集合中
                    if (consumerRack != null && !partitionRacks.isEmpty() && partitionRacks.contains(consumerRack)) {
                        // 遍历该主题的所有潜在消费者
                        for (String otherConsumer : topic2AllPotentialConsumers.get(partition.topic())) {
                            // 获取其他消费者的机架信息
                            String otherConsumerRack = rackInfo.consumerRacks.get(otherConsumer);
                            // 如果其他消费者没有机架信息，或者其机架不在分区的机架集合中，则跳过
                            if (otherConsumerRack == null || !partitionRacks.contains(otherConsumerRack))
                                continue;
                            // 如果当前消费者拥有的分区数比其他（同一机架）消费者拥有的分区数多一个以上
                            if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                                // 将分区重新分配给其他（同一机架）消费者
                                reassignPartition(partition);
                                // 标记已执行重新分配
                                reassignmentPerformed = true;
                                // 标记已修改分配
                                modified = true;
                                // 标记已找到同一机架的消费者
                                foundRackConsumer = true;
                                // 跳出内部循环，因为已经为该分区找到了合适的消费者
                                break;
                            }
                        }
                    }
                    // 如果没有找到同一机架的消费者（或者不满足机架感知条件）
                    if (!foundRackConsumer) {
                        // 遍历该主题的所有潜在消费者
                        for (String otherConsumer : topic2AllPotentialConsumers.get(partition.topic())) {
                            // 如果当前消费者拥有的分区数比其他消费者拥有的分区数多一个以上
                            if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                                // 将分区重新分配给其他消费者
                                reassignPartition(partition);
                                // 标记已执行重新分配
                                reassignmentPerformed = true;
                                // 标记已修改分配
                                modified = true;
                                // 跳出内部循环，因为已经为该分区找到了合适的消费者
                                break;
                            }
                        }
                    }
                }
            // 如果在本次迭代中修改了分配，则继续下一次迭代
            } while (modified);

            // 返回是否执行了重新分配
            return reassignmentPerformed;
        }

        /**
         * 将指定分区重新分配给一个新的消费者。
         * 此方法首先会查找一个可以消费该分区主题的、且当前负载相对较轻的消费者，然后调用重载的 reassignPartition 方法执行实际的重新分配。
         * 
         * 应用场景：当需要将某个分区从当前消费者移走，并自动选择一个合适的新消费者时调用。
         * 实现细节：
         * 1. 遍历已排序的当前订阅消费者列表 (`sortedCurrentSubscriptions`)。
         * 2. 找到第一个可以消费该分区所属主题的消费者作为新的消费者。
         * 3. 断言确保找到了新的消费者。
         * 4. 调用另一个 `reassignPartition` 方法，传入分区和找到的新消费者。
         * 设计考虑：
         * - 依赖 `sortedCurrentSubscriptions` 来选择新消费者，这可能基于某种排序策略（例如，按负载排序）。
         * - 使用断言来确保逻辑的正确性，即总能找到一个新消费者（假设分区是可分配的）。
         * 
         * @param partition 要重新分配的主题分区。
         */
        private void reassignPartition(TopicPartition partition) {
            // 查找新的消费者
            String newConsumer = null;
            // 遍历当前排序的订阅者列表
            for (String anotherConsumer: sortedCurrentSubscriptions) {
                // 检查该消费者是否订阅了此分区的主题
                if (consumer2AllPotentialTopics.get(anotherConsumer).contains(partition.topic())) {
                    // 如果订阅了，则选定为新消费者
                    newConsumer = anotherConsumer;
                    // 找到后即跳出循环
                    break;
                }
            }

            // 断言：必须找到一个新的消费者
            assert newConsumer != null;

            // 调用重载方法，将分区重新分配给找到的新消费者
            reassignPartition(partition, newConsumer);
        }

        /**
         * 将指定分区重新分配给指定的新消费者。
         * 此方法会考虑粘性需求，确定实际需要移动哪个分区（可能不是传入的 `partition`，而是为了保持粘性而选择的另一个分区），
         * 然后调用 `processPartitionMovement` 方法来处理分区的实际移动。
         * 
         * 应用场景：当已经确定了目标新消费者，需要执行具体的分区迁移操作时调用。
         * 实现细节：
         * 1. 获取分区当前的消费者。
         * 2. 调用 `partitionMovements.getTheActualPartitionToBeMoved` 方法，根据粘性需求确定实际应该移动的分区。
         *    例如，如果直接移动 `partition` 会破坏粘性，此方法可能会返回另一个更适合移动的分区。
         * 3. 调用 `processPartitionMovement` 方法，传入实际要移动的分区和新的消费者。
         * 设计考虑：
         * - 将确定实际移动哪个分区的逻辑委托给 `PartitionMovements` 类，以封装粘性相关的复杂性。
         * - 分离了“决定移动哪个分区”和“执行移动操作”这两个步骤。
         * 
         * @param partition 计划要重新分配的主题分区。
         * @param newConsumer 将要接收分区的新消费者ID。
         */
        private void reassignPartition(TopicPartition partition, String newConsumer) {
            // 获取分区当前的消费者
            String consumer = currentPartitionConsumer.get(partition);
            // 考虑到粘性需求，找到实际需要移动的分区
            // 这可能不是参数 partition 本身，而是为了保持粘性而选择的另一个分区
            TopicPartition partitionToBeMoved = partitionMovements.getTheActualPartitionToBeMoved(partition, consumer, newConsumer);
            // 处理分区的移动，将其分配给新的消费者
            processPartitionMovement(partitionToBeMoved, newConsumer);
        }

        /**
         * 处理分区从旧消费者到新消费者的移动。
         * 此方法会更新所有相关的内部状态，包括当前分配、分区到消费者的映射以及排序后的订阅列表。
         * 
         * 应用场景：在确定了要移动的分区和目标新消费者后，实际执行状态更新时调用。
         * 实现细节：
         * 1. 获取分区的旧消费者。
         * 2. 从 `sortedCurrentSubscriptions` 中移除旧消费者和新消费者（因为他们的分区分配即将改变，影响排序依据）。
         * 3. 调用 `partitionMovements.movePartition` 记录这次分区移动，用于粘性分析或协作再均衡。
         * 4. 更新 `currentAssignment`：从旧消费者的分区列表中移除该分区，并将其添加到新消费者的分区列表中。
         * 5. 更新 `currentPartitionConsumer`，将该分区映射到新消费者。
         * 6. 将新消费者和旧消费者重新添加到 `sortedCurrentSubscriptions` 中（它们会根据更新后的状态被重新排序）。
         * 设计考虑：
         * - 集中处理所有与分区移动相关的状态更新，确保数据一致性。
         * - `sortedCurrentSubscriptions` 的移除和重新添加操作是为了在消费者负载变化后维持其正确的排序。
         * 
         * @param partition 要移动的主题分区。
         * @param newConsumer 将要接收分区的新消费者ID。
         */
        private void processPartitionMovement(TopicPartition partition, String newConsumer) {
            // 获取分区的旧消费者
            String oldConsumer = currentPartitionConsumer.get(partition);

            // 从排序的当前订阅列表中移除旧消费者和新消费者
            // 因为他们的分区分配即将改变，这可能会影响他们在排序列表中的位置
            sortedCurrentSubscriptions.remove(oldConsumer);
            sortedCurrentSubscriptions.remove(newConsumer);

            // 记录分区的移动，用于粘性计算或其他目的
            partitionMovements.movePartition(partition, oldConsumer, newConsumer);

            // 从旧消费者的当前分配中移除该分区
            currentAssignment.get(oldConsumer).remove(partition);
            // 将该分区添加到新消费者的当前分配中
            currentAssignment.get(newConsumer).add(partition);
            // 更新分区到当前消费者的映射，将该分区指向新消费者
            currentPartitionConsumer.put(partition, newConsumer);
            // 将新消费者和旧消费者重新添加到排序的当前订阅列表中
            // 它们会根据更新后的分区数量被重新排序
            sortedCurrentSubscriptions.add(newConsumer);
            sortedCurrentSubscriptions.add(oldConsumer);
        }

        /**
         * 将源映射中的消费者分区分配深拷贝到目标映射中。
         * 目标映射会被清空，然后用源映射的内容填充，其中每个消费者的分区列表都是一个新的 ArrayList 实例。
         * 
         * 应用场景：当需要创建一个分配方案的独立副本时，例如在尝试不同的分配策略或保存分配状态之前。
         * 实现细节：
         * 1. 清空目标映射 `dest`。
         * 2. 遍历源映射 `source` 中的每一个条目（消费者ID -> 分区列表）。
         * 3. 对于每个条目，将其键（消费者ID）和值（分区列表的一个新 ArrayList 副本）放入目标映射 `dest` 中。
         * 设计考虑：
         * - 确保了对分区列表的深拷贝，修改目标映射中的分区列表不会影响源映射。
         * - 这是一个通用的辅助方法，用于复制分配数据结构。
         * 
         * @param source 源映射，包含消费者到其分配分区列表的映射。
         * @param dest 目标映射，将用于存储源映射的深拷贝。
         */
        private void deepCopy(Map<String, List<TopicPartition>> source, Map<String, List<TopicPartition>> dest) {
            // 清空目标映射，准备接收新的数据
            dest.clear();
            // 遍历源映射中的每一个条目（Entry 代表一个键值对）
            for (Entry<String, List<TopicPartition>> entry: source.entrySet())
                // 将源映射中的键（消费者ID）和值的深拷贝（新创建的 ArrayList 包含相同的分区）放入目标映射
                dest.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }

        /**
         * 创建并返回给定消费者分区分配映射的深拷贝。
         * 
         * 应用场景：当需要获取一个分配方案的独立副本，并且不希望修改原始分配时调用。
         * 实现细节：
         * 1. 创建一个新的 `HashMap` 作为拷贝结果。
         * 2. 调用另一个重载的 `deepCopy` 方法，将传入的 `assignment` 映射深拷贝到新创建的 `copy` 映射中。
         * 3. 返回这个新创建并填充好的 `copy` 映射。
         * 设计考虑：
         * - 提供了一个便捷的接口来获取分配映射的深拷贝，隐藏了目标映射的创建细节。
         * 
         * @param assignment 要进行深拷贝的消费者分区分配映射。
         * @return 给定分配映射的一个新的深拷贝实例。
         */
        private Map<String, List<TopicPartition>> deepCopy(Map<String, List<TopicPartition>> assignment) {
            // 创建一个新的 HashMap 用于存储深拷贝的结果
            Map<String, List<TopicPartition>> copy = new HashMap<>();
            // 调用另一个 deepCopy 方法，将 assignment 的内容深拷贝到新创建的 copy 中
            deepCopy(assignment, copy);
            // 返回深拷贝后的映射
            return copy;
        }
    }
}
