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

import java.util.Arrays;
import java.util.Objects;

/**
 * 该类用于描述Kafka元数据响应中每个分区的状态信息。
 * 包含了分区的主题名称、分区号、领导者节点、副本列表、同步副本列表和离线副本列表等重要信息。
 * 这些信息对于客户端了解分区的当前状态、进行读写操作和故障转移非常重要。
 */
public class PartitionInfo {
    // 分区所属的主题名称
    private final String topic;
    // 分区号
    private final int partition;
    // 当前分区的领导者节点
    private final Node leader;
    // 该分区的所有副本节点数组
    private final Node[] replicas;
    // 当前处于同步状态的副本节点数组（这些副本与领导者保持同步，可以在领导者故障时接管领导权）
    private final Node[] inSyncReplicas;
    // 当前处于离线状态的副本节点数组
    private final Node[] offlineReplicas;

    /**
     * 构造函数 - 创建分区信息对象（不包含离线副本信息）
     * @param topic 主题名称
     * @param partition 分区号
     * @param leader 领导者节点
     * @param replicas 所有副本节点数组
     * @param inSyncReplicas 同步副本节点数组
     */
    public PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas) {
        // 调用完整的构造函数，将离线副本设置为空数组
        this(topic, partition, leader, replicas, inSyncReplicas, new Node[0]);
    }

    /**
     * 构造函数 - 创建完整的分区信息对象
     * @param topic 主题名称
     * @param partition 分区号
     * @param leader 领导者节点
     * @param replicas 所有副本节点数组
     * @param inSyncReplicas 同步副本节点数组
     * @param offlineReplicas 离线副本节点数组
     */
    public PartitionInfo(String topic,
                         int partition,
                         Node leader,
                         Node[] replicas,
                         Node[] inSyncReplicas,
                         Node[] offlineReplicas) {
        // 初始化所有字段
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
        this.offlineReplicas = offlineReplicas;
    }

    /**
     * The topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition id
     */
    public int partition() {
        return partition;
    }

    /**
     * The node id of the node currently acting as a leader for this partition or null if there is no leader
     */
    public Node leader() {
        return leader;
    }

    /**
     * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
     */
    public Node[] replicas() {
        return replicas;
    }

    /**
     * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if
     * the leader should fail
     */
    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }

    /**
     * The subset of the replicas that are offline
     */
    public Node[] offlineReplicas() {
        return offlineReplicas;
    }

    /**
     * 重写hashCode方法，用于计算对象的哈希值
     * 使用Objects.hash方法组合所有字段的哈希值
     * 对于数组类型的字段，使用Arrays.hashCode计算哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, leader, Arrays.hashCode(replicas),
            Arrays.hashCode(inSyncReplicas), Arrays.hashCode(offlineReplicas));
    }

    /**
     * 重写equals方法，用于比较两个PartitionInfo对象是否相等
     * 比较规则：
     * 1. 如果是同一个对象引用，返回true
     * 2. 如果比较对象为null或类型不同，返回false
     * 3. 比较所有字段是否相等：
     *    - 使用Objects.equals比较普通字段
     *    - 使用Objects.deepEquals比较数组字段
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionInfo other = (PartitionInfo) obj;
        return Objects.equals(topic, other.topic) &&
            partition == other.partition &&
            Objects.equals(leader, other.leader) &&
            Objects.deepEquals(replicas, other.replicas) &&
            Objects.deepEquals(inSyncReplicas, other.inSyncReplicas) &&
            Objects.deepEquals(offlineReplicas, other.offlineReplicas);
    }

    /**
     * 重写toString方法，返回分区信息的字符串表示
     * 格式化输出包含：
     * - 主题名称
     * - 分区号
     * - 领导者节点ID（如果没有领导者则显示"none"）
     * - 所有副本节点ID列表
     * - 同步副本节点ID列表
     * - 离线副本节点ID列表
     */
    @Override
    public String toString() {
        return String.format("Partition(topic = %s, partition = %d, leader = %s, replicas = %s, isr = %s, offlineReplicas = %s)",
                             topic,
                             partition,
                             leader == null ? "none" : leader.idString(),
                             formatNodeIds(replicas),
                             formatNodeIds(inSyncReplicas),
                             formatNodeIds(offlineReplicas));
    }

    /**
     * 将节点数组格式化为字符串
     * @param nodes 需要格式化的节点数组
     * @return 格式化后的字符串，格式为[id1,id2,...]
     */
    private String formatNodeIds(Node[] nodes) {
        // 创建StringBuilder并添加开始的方括号
        StringBuilder b = new StringBuilder("[");
        if (nodes != null) {
            // 遍历节点数组，将每个节点的ID添加到结果中
            for (int i = 0; i < nodes.length; i++) {
                b.append(nodes[i].idString());
                // 如果不是最后一个节点，添加逗号分隔符
                if (i < nodes.length - 1)
                    b.append(',');
            }
        }
        // 添加结束的方括号并返回结果
        b.append("]");
        return b.toString();
    }
}
