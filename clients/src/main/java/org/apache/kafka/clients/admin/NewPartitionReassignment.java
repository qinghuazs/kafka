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

package org.apache.kafka.clients.admin;

import java.util.List;
import java.util.Map;

/**
 * 分区副本重分配请求类，用于创建新的分区副本重分配任务。
 * 该类通过 {@link AdminClient#alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)} 方法来应用重分配。
 *
 * 应用场景：
 * 1. 负载均衡：当某些broker负载过重时，将分区副本重新分配到负载较轻的broker上
 * 2. 机器下线：需要下线某些broker时，将其上的分区副本迁移到其他broker上
 * 3. 机器扩容：新增broker后，将现有分区的副本重新分配到新的broker上
 * 4. 故障恢复：当某些broker发生故障后，将受影响的分区副本重新分配到健康的broker上
 */
public class NewPartitionReassignment {
    /**
     * 目标副本列表，包含重分配后分区副本所在的broker ID列表
     * 列表中的每个整数表示一个broker的ID，列表顺序很重要：
     * - 第一个元素将成为分区的首选副本（leader）
     * - 后续元素将按顺序成为follower副本
     */
    private final List<Integer> targetReplicas;

    /**
     * 创建一个新的分区重分配请求
     *
     * @param targetReplicas 目标broker ID列表，指定重分配后分区副本所在的broker
     * @throws IllegalArgumentException 当目标副本列表为空或null时抛出此异常
     */
    public NewPartitionReassignment(List<Integer> targetReplicas) {
        // 验证目标副本列表的有效性
        if (targetReplicas == null || targetReplicas.isEmpty())
            throw new IllegalArgumentException("Cannot create a new partition reassignment without any replicas");
        // 创建目标副本列表的不可变副本，防止外部修改
        this.targetReplicas = List.copyOf(targetReplicas);
    }

    /**
     * 获取目标副本列表
     *
     * @return 返回不可变的目标broker ID列表
     */
    public List<Integer> targetReplicas() {
        return targetReplicas;
    }
}
