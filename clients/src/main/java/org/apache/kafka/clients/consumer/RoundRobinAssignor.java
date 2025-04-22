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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * <p>轮询分配器（RoundRobinAssignor）是Kafka消费者组的一种分区分配策略实现。它首先列出所有可用的分区和消费者，
 * 然后按照轮询的方式将分区依次分配给消费者。当所有消费者的订阅模式相同时，分区会被均匀地分配
 * （即每个消费者拥有的分区数量最多相差1个）。
 *
 * <p>基本分配示例：
 * 假设有两个消费者<code>C0</code>和<code>C1</code>，两个主题<code>t0</code>和<code>t1</code>，
 * 每个主题有3个分区，产生的分区为<code>t0p0</code>、<code>t0p1</code>、<code>t0p2</code>、
 * <code>t1p0</code>、<code>t1p1</code>和<code>t1p2</code>。
 *
 * <p>分配结果将会是：
 * <ul>
 * <li><code>C0: [t0p0, t0p2, t1p1]</code>
 * <li><code>C1: [t0p1, t1p0, t1p2]</code>
 * </ul>
 *
 * <p>差异订阅场景：
 * 当消费者的订阅模式不同时，分配过程仍然以轮询方式进行，但会跳过未订阅特定主题的消费者。
 * 这种情况可能导致分配不均衡。例如，有三个消费者<code>C0</code>、<code>C1</code>、<code>C2</code>，
 * 和三个主题<code>t0</code>、<code>t1</code>、<code>t2</code>，分别有1、2和3个分区。
 * 分区为<code>t0p0</code>、<code>t1p0</code>、<code>t1p1</code>、<code>t2p0</code>、<code>t2p1</code>、<code>t2p2</code>。
 * 订阅情况：
 * - <code>C0</code>订阅了<code>t0</code>
 * - <code>C1</code>订阅了<code>t0</code>、<code>t1</code>
 * - <code>C2</code>订阅了<code>t0</code>、<code>t1</code>、<code>t2</code>
 *
 * <p>差异订阅的分配结果：
 * <ul>
 * <li><code>C0: [t0p0]</code>
 * <li><code>C1: [t1p0]</code>
 * <li><code>C2: [t1p1, t2p0, t2p1, t2p2]</code>
 * </ul>
 *
 * <p>静态成员机制：
 * Kafka引入了静态成员机制，通过<code>group.instance.id</code>使分配行为更加稳定。
 * 例如，有三个消费者，分配的<code>member.id</code>为<code>C0</code>、<code>C1</code>、<code>C2</code>，
 * 两个主题<code>t0</code>和<code>t1</code>，每个主题有3个分区。基于临时的<code>member.id</code>排序顺序，
 * 分配结果为：
 * <ul>
 * <li><code>C0: [t0p0, t1p0]</code>
 * <li><code>C1: [t0p1, t1p1]</code>
 * <li><code>C2: [t0p2, t1p2]</code>
 * </ul>
 *
 * <p>重启影响：
 * 在一次滚动重启后，组协调器会为消费者分配新的<code>member.id</code>，例如：
 * - <code>C0</code>变为<code>C5</code>
 * - <code>C1</code>变为<code>C3</code>
 * - <code>C2</code>变为<code>C4</code>
 *
 * <p>这会导致分配完全重新洗牌：
 * <ul>
 * <li><code>C3（原C1）: [t0p0, t1p0]（之前是[t0p1, t1p1]）</code>
 * <li><code>C4（原C2）: [t0p1, t1p1]（之前是[t0p2, t1p2]）</code>
 * <li><code>C5（原C0）: [t0p2, t1p2]（之前是[t0p0, t1p0]）</code>
 * </ul>
 *
 * <p>静态成员解决方案：
 * 通过静态成员机制，消费者可以拥有固定的实例ID（<code>I1</code>、<code>I2</code>、<code>I3</code>）。
 * 只要满足以下条件：
 * 1. 消费者组成员数量在不同代之间保持不变
 * 2. 静态成员的身份在不同代之间保持不变
 * 3. 订阅模式不发生变化
 *
 * <p>就能保证分配始终稳定：
 * <ul>
 * <li><code>I0: [t0p0, t1p0]</code>
 * <li><code>I1: [t0p1, t1p1]</code>
 * <li><code>I2: [t0p2, t1p2]</code>
 * </ul>
 *
 * 实现要点：
 * 1. 轮询分配：按顺序将分区分配给消费者，确保均匀分布
 * 2. 订阅感知：跳过未订阅特定主题的消费者
 * 3. 静态成员支持：通过group.instance.id实现分配的稳定性
 * 4. 分配均衡：当所有消费者订阅相同主题时，分区数量差异最多为1
 */
public class RoundRobinAssignor extends AbstractPartitionAssignor {
    /**
     * 轮询分配器的名称标识
     */
    public static final String ROUNDROBIN_ASSIGNOR_NAME = "roundrobin";

    /**
     * 执行分区分配的核心方法
     * @param partitionsPerTopic 每个主题的分区数量映射
     * @param subscriptions 消费者的订阅信息映射
     * @return 返回消费者到分区列表的分配结果映射
     *
     * 实现细节：
     * 1. 初始化分配结果和成员信息列表
     * 2. 创建循环迭代器用于轮询分配
     * 3. 遍历排序后的分区列表进行分配
     * 4. 跳过未订阅特定主题的消费者
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 创建分配结果映射，用于存储每个消费者分配到的分区列表
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        // 创建成员信息列表，包含消费者ID和实例ID
        List<MemberInfo> memberInfoList = new ArrayList<>();
        // 初始化每个消费者的分配结果为空列表，并收集成员信息
        for (Map.Entry<String, Subscription> memberSubscription : subscriptions.entrySet()) {
            assignment.put(memberSubscription.getKey(), new ArrayList<>());
            memberInfoList.add(new MemberInfo(memberSubscription.getKey(),
                                              memberSubscription.getValue().groupInstanceId()));
        }

        // 创建成员信息的循环迭代器，用于轮询分配
        CircularIterator<MemberInfo> assigner = new CircularIterator<>(Utils.sorted(memberInfoList));

        // 遍历所有排序后的分区进行分配
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            final String topic = partition.topic();
            // 跳过未订阅当前主题的消费者
            while (!subscriptions.get(assigner.peek().memberId).topics().contains(topic))
                assigner.next();
            // 将分区分配给下一个符合条件的消费者
            assignment.get(assigner.next().memberId).add(partition);
        }
        return assignment;
    }

    /**
     * 获取所有已排序的分区列表
     * @param partitionsPerTopic 每个主题的分区数量映射
     * @param subscriptions 消费者的订阅信息映射
     * @return 返回已排序的TopicPartition列表
     *
     * 实现细节：
     * 1. 收集所有被订阅的主题并排序
     * 2. 为每个主题创建对应数量的分区对象
     */
    private List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                     Map<String, Subscription> subscriptions) {
        // 使用TreeSet确保主题有序
        SortedSet<String> topics = new TreeSet<>();
        // 收集所有消费者订阅的主题
        for (Subscription subscription : subscriptions.values())
            topics.addAll(subscription.topics());

        // 创建所有分区的列表
        List<TopicPartition> allPartitions = new ArrayList<>();
        // 为每个主题创建对应数量的分区
        for (String topic : topics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        return allPartitions;
    }

    /**
     * 返回分配器的名称
     * @return 返回"roundrobin"，表示这是轮询分配器
     */
    @Override
    public String name() {
        return ROUNDROBIN_ASSIGNOR_NAME;
    }

}
