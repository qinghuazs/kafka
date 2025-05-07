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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 机架感知的副本选择器实现，用于优化Kafka的数据本地性读取。
 * 
 * 选择策略：
 * 1. 优先选择与客户端位于同一机架的副本，以减少网络传输开销
 * 2. 如果客户端所在机架有多个副本，优先选择leader副本
 * 3. 如果客户端所在机架没有leader但有其他副本，选择最新的follower副本
 * 4. 如果找不到同机架的副本，则返回leader副本
 * 5. 如果客户端未指定机架ID，直接返回leader副本
 * 
 * 该实现通过就近读取原则来优化性能，特别适用于跨机架部署的场景。
 */
public class RackAwareReplicaSelector implements ReplicaSelector {

    /**
     * 根据机架感知策略选择最优的读取副本
     * 
     * @param topicPartition 目标主题分区
     * @param clientMetadata 客户端元数据，包含客户端的机架ID等信息
     * @param partitionView 分区的当前视图，包含所有副本的状态
     * @return 选中的副本视图
     */
    @Override
    public Optional<ReplicaView> select(TopicPartition topicPartition,
                                        ClientMetadata clientMetadata,
                                        PartitionView partitionView) {
        if (clientMetadata.rackId() != null && !clientMetadata.rackId().isEmpty()) {
            Set<ReplicaView> sameRackReplicas = partitionView.replicas().stream()
                    .filter(replicaInfo -> clientMetadata.rackId().equals(replicaInfo.endpoint().rack()))
                    .collect(Collectors.toSet());
            if (sameRackReplicas.isEmpty()) {
                return Optional.of(partitionView.leader());
            } else {
                if (sameRackReplicas.contains(partitionView.leader())) {
                    // Use the leader if it's in this rack
                    return Optional.of(partitionView.leader());
                } else {
                    // Otherwise, get the most caught-up replica
                    return sameRackReplicas.stream().max(ReplicaView.comparator());
                }
            }
        } else {
            return Optional.of(partitionView.leader());
        }
    }
}
