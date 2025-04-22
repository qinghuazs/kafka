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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 这是{@link AbstractStickyAssignor AbstractStickyAssignor}的协作式版本实现。它遵循与{@link StickyAssignor StickyAssignor}
 * 相同的粘性分配逻辑，但支持协作式重平衡，而{@link StickyAssignor StickyAssignor}则遵循即时重平衡协议。
 * 关于重平衡协议的详细说明，请参见{@link ConsumerPartitionAssignor.RebalanceProtocol}。
 * <p>
 * 对于较新的集群，建议使用此分配器。
 * <p>
 * 要启用协作式重平衡，你必须将所有消费者设置为使用此{@code PartitionAssignor}，
 * 或实现一个自定义的分配器，并在{@link CooperativeStickyAssignor#supportedProtocols supportedProtocols()}中
 * 返回{@code RebalanceProtocol.COOPERATIVE}。
 * <p>
 * 重要提示：如果从2.3或更早版本升级，你必须遵循特定的升级路径，以安全地启用协作式重平衡。
 * 详情请参见<a href="https://kafka.apache.org/documentation/#upgrade_240_notable">升级指南</a>。
 */
public class CooperativeStickyAssignor extends AbstractStickyAssignor {
    public static final String COOPERATIVE_STICKY_ASSIGNOR_NAME = "cooperative-sticky";

    // 这些模式用于保存分配的有用元数据，例如最后一个稳定的代数
    private static final String GENERATION_KEY_NAME = "generation";
    private static final Schema COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0 = new Schema(
        new Field(GENERATION_KEY_NAME, Type.INT32));

    private int generation = DEFAULT_GENERATION; // 消费者组代数

    @Override
    public String name() {
        // 返回分配器的名称
        return COOPERATIVE_STICKY_ASSIGNOR_NAME;
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        // 支持协作式和即时两种重平衡协议
        return Arrays.asList(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER);
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        // 当分配发生时，更新消费者组代数
        this.generation = metadata.generationId();
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        // 创建用户数据结构，包含当前代数信息
        Struct struct = new Struct(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0);

        // 设置代数值
        struct.set(GENERATION_KEY_NAME, generation);
        // 分配缓冲区并写入数据
        ByteBuffer buffer = ByteBuffer.allocate(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
        COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    @Override
    protected MemberData memberData(Subscription subscription) {
        // 对于ConsumerProtocolSubscription v2或更高版本，可以直接从字段获取成员数据
        if (subscription.generationId().isPresent()) {
            // 直接返回包含已拥有分区和代数的成员数据
            return new MemberData(subscription.ownedPartitions(), subscription.generationId());
        }

        // 获取用户数据缓冲区
        ByteBuffer buffer = subscription.userData();
        Optional<Integer> encodedGeneration;
        if (buffer == null) {
            // 如果没有用户数据，返回空的代数
            encodedGeneration = Optional.empty();
        } else {
            try {
                // 尝试从用户数据中读取代数信息
                Struct struct = COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.read(buffer);
                encodedGeneration = Optional.of(struct.getInt(GENERATION_KEY_NAME));
            } catch (Exception e) {
                // 如果读取失败，使用默认代数
                encodedGeneration = Optional.of(DEFAULT_GENERATION);
            }
        }
        // 返回包含已拥有分区、代数和机架ID的成员数据
        return new MemberData(subscription.ownedPartitions(), encodedGeneration, subscription.rackId());
    }

    @Override
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                              Map<String, Subscription> subscriptions) {
        // 首先使用父类的分配逻辑获取初始分配结果
        Map<String, List<TopicPartition>> assignments = super.assignPartitions(partitionsPerTopic, subscriptions);

        // 获取需要转移所有权的分区信息
        Map<TopicPartition, String> partitionsTransferringOwnership = super.partitionsTransferringOwnership == null ?
            // 如果父类没有计算，则计算需要转移所有权的分区
            computePartitionsTransferringOwnership(subscriptions, assignments) :
            // 否则使用父类已计算的结果
            super.partitionsTransferringOwnership;

        // 调整分配结果，移除需要先撤销的分区
        adjustAssignment(assignments, partitionsTransferringOwnership);
        return assignments;
    }

    // 遵循协作式重平衡协议，需要从分配中移除那些必须首先撤销的分区
    private void adjustAssignment(Map<String, List<TopicPartition>> assignments,
                                  Map<TopicPartition, String> partitionsTransferringOwnership) {
        // 遍历所有需要转移所有权的分区
        for (Map.Entry<TopicPartition, String> partitionEntry : partitionsTransferringOwnership.entrySet()) {
            // 从目标消费者的分配中移除该分区，因为它需要先被当前所有者撤销
            assignments.get(partitionEntry.getValue()).remove(partitionEntry.getKey());
        }
    }

    private Map<TopicPartition, String> computePartitionsTransferringOwnership(Map<String, Subscription> subscriptions,
                                                                               Map<String, List<TopicPartition>> assignments) {
        // 存储所有新增的分区及其目标消费者
        Map<TopicPartition, String> allAddedPartitions = new HashMap<>();
        // 存储所有将被撤销的分区
        Set<TopicPartition> allRevokedPartitions = new HashSet<>();

        // 遍历每个消费者的分配结果
        for (final Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            String consumer = entry.getKey();

            // 获取消费者当前拥有的分区和新分配的分区
            List<TopicPartition> ownedPartitions = subscriptions.get(consumer).ownedPartitions();
            List<TopicPartition> assignedPartitions = entry.getValue();

            // 找出新分配给该消费者的分区（之前未拥有的）
            Set<TopicPartition> ownedPartitionsSet = new HashSet<>(ownedPartitions);
            for (TopicPartition tp : assignedPartitions) {
                if (!ownedPartitionsSet.contains(tp))
                    allAddedPartitions.put(tp, consumer);
            }

            // 找出将从该消费者撤销的分区（之前拥有但新分配中没有的）
            Set<TopicPartition> assignedPartitionsSet = new HashSet<>(assignedPartitions);
            for (TopicPartition tp : ownedPartitions) {
                if (!assignedPartitionsSet.contains(tp))
                    allRevokedPartitions.add(tp);
            }
        }

        // 只保留那些既在新增列表中又在撤销列表中的分区
        // 这些分区需要等待当前所有者撤销后才能重新分配
        allAddedPartitions.keySet().retainAll(allRevokedPartitions);
        return allAddedPartitions;
    }
}
