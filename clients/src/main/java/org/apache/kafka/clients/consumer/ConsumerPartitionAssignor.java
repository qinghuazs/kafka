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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;

/**
 * 该接口用于在{@link org.apache.kafka.clients.consumer.KafkaConsumer}中定义自定义的分区分配策略。
 * 消费者组的成员订阅他们感兴趣的主题，并将订阅信息转发给作为组协调器的Kafka broker。
 * 协调器选择一个成员执行组分配，并将所有成员的订阅信息传递给它。
 * 然后调用{@link #assign(Cluster, GroupSubscription)}执行分配，结果会返回给各个成员。
 * <p>
 * 在某些情况下，向分配器传递额外的元数据以做出分配决策是很有用的。
 * 为此，你可以重写{@link #subscriptionUserData(Set)}方法，并在返回的Subscription中提供自定义的userData。
 * 例如，要实现机架感知的分配器，实现可以使用这个用户数据来转发每个成员所属的机架ID。
 * <p>
 * 实现类可以扩展{@link Configurable}接口以从消费者获取配置。
 */
public interface ConsumerPartitionAssignor {

    /**
     * 返回序列化的数据，这些数据将被包含在发送给leader的{@link Subscription}中，
     * 并可以在{@link #assign(Cluster, GroupSubscription)}中被利用（例如本地主机/机架信息）
     *
     * @param topics 通过{@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection)}
     *               及其变体订阅的主题
     * @return 可为null的订阅用户数据
     */
    default ByteBuffer subscriptionUserData(Set<String> topics) {
        return null;
    }

    /**
     * 根据成员的订阅信息和当前集群元数据执行组分配。
     * @param metadata 消费者已知的当前主题/broker元数据
     * @param groupSubscription 所有成员的订阅信息，包括通过{@link #subscriptionUserData(Set)}提供的元数据
     * @return 从成员到其各自分配的映射。输入订阅映射中的每个成员都应该有一个对应的条目。
     */
    GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription);

    /**
     * 当组成员从leader接收到其分配时调用的回调函数。
     * @param assignment leader在{@link #assign(Cluster, GroupSubscription)}中提供的本地成员的分配
     * @param metadata 消费者的额外元数据（可选）
     */
    default void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
    }

    /**
     * 指示此分配器使用哪种重平衡协议；
     * 默认情况下，它应该始终使用{@link RebalanceProtocol#EAGER}。
     */
    default List<RebalanceProtocol> supportedProtocols() {
        return Collections.singletonList(RebalanceProtocol.EAGER);
    }

    /**
     * 返回分配器的版本，该版本表示用户元数据编码和分配算法如何演进。
     */
    default short version() {
        return (short) 0;
    }

    /**
     * 此分配器的唯一名称（例如"range"或"roundrobin"或"sticky"）。
     * 注意，这不需要与{@link ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}中指定的类名相同
     * @return 非空的唯一名称
     */
    String name();

    /**
     * 表示消费者的订阅信息，包含订阅的主题列表和可选的用户自定义数据
     */
    final class Subscription {
        // 消费者订阅的主题列表
        private final List<String> topics;
        // 用户自定义数据，可用于传递额外的元数据信息
        private final ByteBuffer userData;
        // 当前消费者已经拥有的分区列表
        private final List<TopicPartition> ownedPartitions;
        // 消费者所在的机架ID，用于机架感知的分区分配
        private final Optional<String> rackId;
        // 消费者的静态成员ID，用于静态成员管理
        private Optional<String> groupInstanceId;
        // 消费者组的世代ID，用于跟踪重平衡事件
        private final Optional<Integer> generationId;

        public Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions, int generationId, Optional<String> rackId) {
            this.topics = topics;
            this.userData = userData;
            this.ownedPartitions = ownedPartitions;
            this.groupInstanceId = Optional.empty();
            this.generationId = generationId < 0 ? Optional.empty() : Optional.of(generationId);
            this.rackId = rackId;
        }

        public Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions) {
            this(topics, userData, ownedPartitions, DEFAULT_GENERATION, Optional.empty());
        }

        public Subscription(List<String> topics, ByteBuffer userData) {
            this(topics, userData, Collections.emptyList(), DEFAULT_GENERATION, Optional.empty());
        }

        public Subscription(List<String> topics) {
            this(topics, null, Collections.emptyList(), DEFAULT_GENERATION, Optional.empty());
        }

        public List<String> topics() {
            return topics;
        }

        public ByteBuffer userData() {
            return userData;
        }

        public List<TopicPartition> ownedPartitions() {
            return ownedPartitions;
        }

        public Optional<String> rackId() {
            return rackId;
        }

        public void setGroupInstanceId(Optional<String> groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
        }

        public Optional<String> groupInstanceId() {
            return groupInstanceId;
        }

        public Optional<Integer> generationId() {
            return generationId;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                "topics=" + topics +
                (userData == null ? "" : ", userDataSize=" + userData.remaining()) +
                ", ownedPartitions=" + ownedPartitions +
                ", groupInstanceId=" + groupInstanceId.map(String::toString).orElse("null") +
                ", generationId=" + generationId.orElse(-1) +
                ", rackId=" + (rackId.orElse("null")) +
                ")";
        }
    }

    /**
     * 表示分区分配的结果，包含分配给消费者的分区列表和可选的用户自定义数据
     */
    final class Assignment {
        // 分配给消费者的主题分区列表
        private final List<TopicPartition> partitions;
        // 用户自定义数据，可用于传递额外的分配相关信息
        private final ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, null);
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                "partitions=" + partitions +
                (userData == null ? "" : ", userDataSize=" + userData.remaining()) +
                ')';
        }
    }

    /**
     * 表示消费者组的订阅信息，包含所有消费者的订阅详情
     */
    final class GroupSubscription {
        // 消费者ID到其订阅信息的映射
        private final Map<String, Subscription> subscriptions;

        public GroupSubscription(Map<String, Subscription> subscriptions) {
            this.subscriptions = subscriptions;
        }

        public Map<String, Subscription> groupSubscription() {
            return subscriptions;
        }

        @Override
        public String toString() {
            return "GroupSubscription(" +
                "subscriptions=" + subscriptions +
                ")";
        }
    }

    /**
     * 表示消费者组的分配结果，包含所有消费者的分区分配详情
     */
    final class GroupAssignment {
        // 消费者ID到其分配结果的映射
        private final Map<String, Assignment> assignments;

        public GroupAssignment(Map<String, Assignment> assignments) {
            this.assignments = assignments;
        }

        public Map<String, Assignment> groupAssignment() {
            return assignments;
        }

        @Override
        public String toString() {
            return "GroupAssignment(" +
                "assignments=" + assignments +
                ")";
        }
    }

    /**
     * 重平衡协议定义了分区分配和撤销的语义。其目的是建立一套一致的规则，
     * 使组内所有消费者在转移分区所有权时都遵循这些规则。
     * {@link ConsumerPartitionAssignor}的实现者可以通过{@link ConsumerPartitionAssignor#supportedProtocols()}
     * 声明支持一个或多个重平衡协议，并且他们有责任在其{@link ConsumerPartitionAssignor#assign(Cluster, GroupSubscription)}
     * 实现中遵守这些协议的规则。不遵守支持的协议规则将导致运行时错误或未定义的行为。
     *
     * {@link RebalanceProtocol#EAGER}重平衡协议要求消费者在参与重平衡事件之前必须撤销其拥有的所有分区。
     * 因此，它允许对分配进行完全重新洗牌。
     *
     * {@link RebalanceProtocol#COOPERATIVE}重平衡协议允许消费者在参与重平衡事件之前保留其当前拥有的分区。
     * 分配器不应立即重新分配任何已拥有的分区，而是可以向消费者指示需要撤销分区，以便在下一次重平衡事件中
     * 将撤销的分区重新分配给其他消费者。这是为粘性分配逻辑设计的，它试图通过协作调整来最小化分区重新分配。
     */
    enum RebalanceProtocol {
        EAGER((byte) 0), COOPERATIVE((byte) 1);

        private final byte id;

        RebalanceProtocol(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static RebalanceProtocol forId(byte id) {
            switch (id) {
                case 0:
                    return EAGER;
                case 1:
                    return COOPERATIVE;
                default:
                    throw new IllegalArgumentException("Unknown rebalance protocol id: " + id);
            }
        }
    }

    /**
     * 根据{@link org.apache.kafka.clients.consumer.ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     * 指定的类名/类型获取已配置的{@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor}实例列表
     */
    /**
     * 根据提供的分配器类名列表和配置，创建并返回分区分配器实例列表
     * 
     * @param assignorClasses 分配器类名列表，可以是类名字符串或Class对象
     * @param configs 分配器的配置信息
     * @return 已配置的分区分配器实例列表
     * @throws KafkaException 当分配器类加载失败或配置无效时
     */
    static List<ConsumerPartitionAssignor> getAssignorInstances(List<String> assignorClasses, Map<String, Object> configs) {
        // 用于存储创建的分配器实例
        List<ConsumerPartitionAssignor> assignors = new ArrayList<>();
        // 用于检查分配器名称是否重复的映射（分配器名称 -> 分配器类名）
        Map<String, String> assignorNameMap = new HashMap<>();

        // 如果没有提供分配器类列表，返回空列表
        if (assignorClasses == null)
            return assignors;

        // 遍历所有提供的分配器类
        for (Object klass : assignorClasses) {
            // 如果提供的是类名字符串，尝试加载对应的类
            if (klass instanceof String) {
                try {
                    klass = Utils.loadClass((String) klass, Object.class);
                } catch (ClassNotFoundException classNotFound) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", classNotFound);
                }
            }

            // 如果是Class类型，创建实例并进行配置
            if (klass instanceof Class<?>) {
                // 创建分配器实例
                Object assignor = Utils.newInstance((Class<?>) klass);
                // 如果实现了Configurable接口，应用配置
                if (assignor instanceof Configurable)
                    ((Configurable) assignor).configure(configs);

                // 检查是否是ConsumerPartitionAssignor的实例
                if (assignor instanceof ConsumerPartitionAssignor) {
                    // 获取分配器名称
                    String assignorName = ((ConsumerPartitionAssignor) assignor).name();
                    // 检查分配器名称是否重复
                    if (assignorNameMap.containsKey(assignorName)) {
                        throw new KafkaException("The assignor name: '" + assignorName + "' is used in more than one assignor: " +
                            assignorNameMap.get(assignorName) + ", " + assignor.getClass().getName());
                    }
                    // 记录分配器名称和类名的映射，并添加到结果列表
                    assignorNameMap.put(assignorName, assignor.getClass().getName());
                    assignors.add((ConsumerPartitionAssignor) assignor);
                } else {
                    throw new KafkaException(klass + " is not an instance of " + ConsumerPartitionAssignor.class.getName());
                }
            } else {
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            }
        }
        return assignors;
    }

}
