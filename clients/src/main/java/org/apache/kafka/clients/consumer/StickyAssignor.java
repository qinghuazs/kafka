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

import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * <p>粘性分配器服务于两个目的。首先，它保证分配尽可能均衡，这意味着：
 * <ul>
 * <li>分配给消费者的主题分区数量最多相差一个；或者</li>
 * <li>如果一个消费者比其他消费者少2个或更多分区，那么它无法获得这些分区的转移。</li>
 * </ul>
 * 其次，在重新分配发生时，它尽可能保留现有的分配关系。这有助于减少分区从一个消费者转移到另一个消费者时的处理开销。</p>
 *
 * <p>从新开始分配时，它会尽可能均匀地在消费者之间分配分区。虽然这听起来可能与轮询分配器的工作方式类似，
 * 但下面的第二个示例表明它们是不同的。在重新分配时，它会以如下方式执行重新分配：
 * <ol>
 * <li>主题分区仍然尽可能均匀分布；</li>
 * <li>主题分区尽可能保持与之前分配的消费者的关系。</li>
 * </ol>
 * 当然，第一个目标的优先级高于第二个目标。</p>
 *
 * <p><b>示例1：</b>假设有三个消费者<code>C0</code>、<code>C1</code>、<code>C2</code>，
 * 四个主题<code>t0</code>、<code>t1</code>、<code>t2</code>、<code>t3</code>，每个主题有2个分区，
 * 产生的分区为<code>t0p0</code>、<code>t0p1</code>、<code>t1p0</code>、<code>t1p1</code>、<code>t2p0</code>、
 * <code>t2p1</code>、<code>t3p0</code>、<code>t3p1</code>。每个消费者都订阅了所有三个主题。
 *
 * 粘性分配器和轮询分配器的初始分配结果都是：
 * <ul>
 * <li><code>C0: [t0p0, t1p1, t3p0]</code></li>
 * <li><code>C1: [t0p1, t2p0, t3p1]</code></li>
 * <li><code>C2: [t1p0, t2p1]</code></li>
 * </ul>
 *
 * 现在，假设<code>C1</code>被移除，需要进行重新分配。轮询分配器会产生：
 * <ul>
 * <li><code>C0: [t0p0, t1p0, t2p0, t3p0]</code></li>
 * <li><code>C2: [t0p1, t1p1, t2p1, t3p1]</code></li>
 * </ul>
 *
 * 而粘性分配器会产生：
 * <ul>
 * <li><code>C0 [t0p0, t1p1, t3p0, t2p0]</code></li>
 * <li><code>C2 [t1p0, t2p1, t0p1, t3p1]</code></li>
 * </ul>
 * 保留了所有之前的分配关系（与轮询分配器不同）。
 *</p>
 * <p><b>示例2：</b>有三个消费者<code>C0</code>、<code>C1</code>、<code>C2</code>，
 * 三个主题<code>t0</code>、<code>t1</code>、<code>t2</code>，分别有1、2、3个分区。
 * 因此，分区为<code>t0p0</code>、<code>t1p0</code>、<code>t1p1</code>、<code>t2p0</code>、
 * <code>t2p1</code>、<code>t2p2</code>。<code>C0</code>订阅了<code>t0</code>；<code>C1</code>订阅了
 * <code>t0</code>、<code>t1</code>；<code>C2</code>订阅了<code>t0</code>、<code>t1</code>、<code>t2</code>。
 *
 * 轮询分配器会产生以下分配：
 * <ul>
 * <li><code>C0 [t0p0]</code></li>
 * <li><code>C1 [t1p0]</code></li>
 * <li><code>C2 [t1p1, t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * 这不如粘性分配器建议的分配均衡：
 * <ul>
 * <li><code>C0 [t0p0]</code></li>
 * <li><code>C1 [t1p0, t1p1]</code></li>
 * <li><code>C2 [t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * 现在，如果消费者<code>C0</code>被移除，这两个分配器会产生以下分配。
 * 轮询分配器（保留3个分区分配）：
 * <ul>
 * <li><code>C1 [t0p0, t1p1]</code></li>
 * <li><code>C2 [t1p0, t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * 粘性分配器（保留5个分区分配）：
 * <ul>
 * <li><code>C1 [t1p0, t1p1, t0p0]</code></li>
 * <li><code>C2 [t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *</p>
 * <h3>对<code>ConsumerRebalanceListener</code>的影响</h3>
 * 粘性分配策略可以为那些在<code>onPartitionsRevoked()</code>回调监听器中有分区清理代码的消费者提供优化。
 * 清理代码被放在该回调监听器中，是因为在使用范围或轮询分配器时，消费者在重平衡后无法保留任何已分配的分区。
 * 监听器代码如下所示：
 * <pre>
 * {@code
 * class TheOldRebalanceListener implements ConsumerRebalanceListener {
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       commitOffsets(partition);  // 提交偏移量
 *       cleanupState(partition);   // 清理状态
 *     }
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       initializeState(partition);   // 初始化状态
 *       initializeOffset(partition);  // 初始化偏移量
 *     }
 *   }
 * }
 * }
 * </pre>
 *
 * 如上所述，粘性分配器的一个优势是，通常情况下，它减少了在重新分配期间实际从一个消费者移动到另一个消费者的分区数量。
 * 因此，它允许消费者更高效地进行清理。当然，它们仍然可以在<code>onPartitionsRevoked()</code>监听器中执行分区清理，
 * 但它们可以更高效，记录重平衡前后的分区，并且只对失去的分区进行清理（通常数量不多）。下面的代码片段说明了这一点：
 * <pre>
 * {@code
 * class TheNewRebalanceListener implements ConsumerRebalanceListener {
 *   Collection<TopicPartition> lastAssignment = Collections.emptyList();  // 上次分配的分区
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions)
 *       commitOffsets(partition);  // 只提交偏移量
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> assignment) {
 *     for (TopicPartition partition: difference(lastAssignment, assignment))
 *       cleanupState(partition);  // 清理失去的分区的状态
 *
 *     for (TopicPartition partition: difference(assignment, lastAssignment))
 *       initializeState(partition);  // 初始化新获得的分区的状态
 *
 *     for (TopicPartition partition: assignment)
 *       initializeOffset(partition);  // 初始化所有分区的偏移量
 *
 *     this.lastAssignment = assignment;  // 更新上次分配记录
 *   }
 * }
 * }
 * </pre>
 *
 * 任何使用粘性分配的消费者都可以这样使用这个监听器：
 * <code>consumer.subscribe(topics, new TheNewRebalanceListener());</code>
 *
 * 注意，你可以使用{@link CooperativeStickyAssignor}，这样只有被重新分配给另一个消费者的分区才会被撤销。
 * 这是较新集群的首选分配器。有关协作式重平衡的详细说明，请参见{@link ConsumerPartitionAssignor.RebalanceProtocol}。
 *
 * @see AbstractStickyAssignor
 * @see CooperativeStickyAssignor
 */
public class StickyAssignor extends AbstractStickyAssignor {
    /**
     * 分配器的名称常量
     */
    public static final String STICKY_ASSIGNOR_NAME = "sticky";

    // 这些Schema用于在重平衡期间保存消费者之前分配的分区列表，并作为用户数据发送给leader
    /**
     * 存储之前分配信息的键名
     */
    static final String TOPIC_PARTITIONS_KEY_NAME = "previous_assignment";
    /**
     * 主题名称的键名
     */
    static final String TOPIC_KEY_NAME = "topic";
    /**
     * 分区列表的键名
     */
    static final String PARTITIONS_KEY_NAME = "partitions";
    /**
     * 消费者组代数的键名
     */
    private static final String GENERATION_KEY_NAME = "generation";

    /**
     * 主题分配的Schema定义，包含主题名称和分区列表
     */
    static final Schema TOPIC_ASSIGNMENT = new Schema(
        new Field(TOPIC_KEY_NAME, Type.STRING),
        new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)));
    
    /**
     * 粘性分配器用户数据的V0版本Schema，只包含之前的分配信息
     */
    static final Schema STICKY_ASSIGNOR_USER_DATA_V0 = new Schema(
        new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT)));
    
    /**
     * 粘性分配器用户数据的V1版本Schema，增加了generation字段
     */
    private static final Schema STICKY_ASSIGNOR_USER_DATA_V1 = new Schema(
        new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT)),
        new Field(GENERATION_KEY_NAME, Type.INT32));

    /**
     * 当前成员的分区分配结果
     */
    private List<TopicPartition> memberAssignment = null;
    
    /**
     * 消费者组的代数，用于跟踪重平衡的版本
     */
    private int generation = DEFAULT_GENERATION; // 消费者组代数

    /**
     * 获取分配器的名称
     * @return 返回分配器的名称常量"sticky"
     */
    @Override
    public String name() {
        return STICKY_ASSIGNOR_NAME;
    }

    /**
     * 当分配完成时被调用，用于更新成员的分配信息和代数
     * @param assignment 分配结果
     * @param metadata 消费者组元数据
     */
    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        // 保存分配给该成员的分区列表
        memberAssignment = assignment.partitions();
        // 更新消费者组代数
        this.generation = metadata.generationId();
    }

    /**
     * 生成订阅的用户数据，包含之前的分配信息
     * @param topics 订阅的主题集合
     * @return 序列化后的用户数据
     */
    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        // 如果没有之前的分配信息，返回null
        if (memberAssignment == null)
            return null;

        // 序列化成员数据，包含分区分配信息和代数
        return serializeTopicPartitionAssignment(new MemberData(memberAssignment, Optional.of(generation)));
    }

    /**
     * 从订阅信息中解析成员数据
     * @param subscription 订阅信息
     * @return 成员数据，包含已拥有的分区和代数信息
     */
    @Override
    protected MemberData memberData(Subscription subscription) {
        // 由于StickyAssignor是即时重平衡协议，在加入组之前会撤销所有现有分区
        // 因此总是需要从用户数据中反序列化已拥有的分区和代数ID
        ByteBuffer userData = subscription.userData();
        // 如果没有用户数据，返回空的成员数据
        if (userData == null || !userData.hasRemaining()) {
            return new MemberData(Collections.emptyList(), Optional.empty(), subscription.rackId());
        }
        // 反序列化用户数据得到成员数据
        return deserializeTopicPartitionAssignment(userData);
    }

    /**
     * 序列化主题分区分配信息
     * 该方法可见性为包级私有，主要用于测试
     * @param memberData 要序列化的成员数据
     * @return 序列化后的ByteBuffer
     */
    static ByteBuffer serializeTopicPartitionAssignment(MemberData memberData) {
        // 创建V1版本的数据结构
        Struct struct = new Struct(STICKY_ASSIGNOR_USER_DATA_V1);
        List<Struct> topicAssignments = new ArrayList<>();
        // 按主题分组处理分区
        for (Map.Entry<String, List<Integer>> topicEntry : CollectionUtils.groupPartitionsByTopic(memberData.partitions).entrySet()) {
            // 为每个主题创建分配结构
            Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT);
            topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        // 设置主题分区分配信息
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
        // 如果有代数信息，则设置代数
        memberData.generation.ifPresent(integer -> struct.set(GENERATION_KEY_NAME, integer));
        // 分配缓冲区并写入数据
        ByteBuffer buffer = ByteBuffer.allocate(STICKY_ASSIGNOR_USER_DATA_V1.sizeOf(struct));
        STICKY_ASSIGNOR_USER_DATA_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    /**
     * 反序列化主题分区分配信息
     * @param buffer 包含序列化数据的ByteBuffer
     * @return 反序列化后的成员数据
     */
    private static MemberData deserializeTopicPartitionAssignment(ByteBuffer buffer) {
        Struct struct;
        // 复制缓冲区用于回退
        ByteBuffer copy = buffer.duplicate();
        try {
            // 尝试使用V1版本Schema读取
            struct = STICKY_ASSIGNOR_USER_DATA_V1.read(buffer);
        } catch (Exception e1) {
            try {
                // 如果失败，回退到V0版本Schema
                struct = STICKY_ASSIGNOR_USER_DATA_V0.read(copy);
            } catch (Exception e2) {
                // 如果无法解析，忽略消费者之前的分配，返回空列表和默认代数
                return new MemberData(Collections.emptyList(), Optional.of(DEFAULT_GENERATION));
            }
        }

        // 解析分区信息
        List<TopicPartition> partitions = new ArrayList<>();
        // 遍历每个主题的分配信息
        for (Object structObj : struct.getArray(TOPIC_PARTITIONS_KEY_NAME)) {
            Struct assignment = (Struct) structObj;
            String topic = assignment.getString(TOPIC_KEY_NAME);
            // 遍历主题的所有分区
            for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                Integer partition = (Integer) partitionObj;
                partitions.add(new TopicPartition(topic, partition));
            }
        }
        // 确保向后兼容性，获取代数信息
        Optional<Integer> generation = struct.hasField(GENERATION_KEY_NAME) ? 
            Optional.of(struct.getInt(GENERATION_KEY_NAME)) : Optional.empty();
        return new MemberData(partitions, generation);
    }
}