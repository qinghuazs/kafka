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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 该类包含了Kafka主题分区的领导者、副本和ISR（同步副本）信息。
 * 用于描述一个分区的完整状态，包括：
 * - 分区号
 * - 当前的领导者节点
 * - 所有副本节点列表
 * - 同步副本列表（ISR）
 * - 合格的领导者副本列表（ELR）
 * - 最后已知的合格领导者副本列表
 */
public class TopicPartitionInfo {
    // 分区号，用于唯一标识主题内的一个分区
    private final int partition;
    // 当前分区的领导者节点，如果没有领导者则为null
    private final Node leader;
    // 该分区的所有副本节点列表，按照副本分配的顺序排列，列表头部的副本是优先副本
    private final List<Node> replicas;
    // 当前处于同步状态的副本列表（ISR），这些副本与领导者保持同步，可以在领导者故障时接管领导权
    private final List<Node> isr;
    // 合格的领导者副本列表（ELR），这些副本有资格成为领导者
    private final List<Node> elr;
    // 最后已知的合格领导者副本列表，用于在集群发生变化时保持历史记录
    private final List<Node> lastKnownElr;

    /**
     * 创建TopicPartitionInfo实例，包含完整的分区状态信息
     *
     * @param partition 分区ID，用于唯一标识分区
     * @param leader 分区的领导者节点，如果没有领导者则为{@link Node#noNode()}
     * @param replicas 分区的副本列表，按照副本分配的顺序排列，列表头部的副本是优先副本
     * @param isr 同步副本列表，这些副本与领导者保持同步
     * @param elr 合格的领导者副本列表，这些副本有资格成为领导者
     * @param lastKnownElr 最后已知的合格领导者副本列表，用于跟踪历史状态
     */
    public TopicPartitionInfo(
        int partition,
        Node leader,
        List<Node> replicas,
        List<Node> isr,
        List<Node> elr,
        List<Node> lastKnownElr
    ) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = Collections.unmodifiableList(replicas);
        this.isr = Collections.unmodifiableList(isr);
        this.elr = Collections.unmodifiableList(elr);
        this.lastKnownElr = Collections.unmodifiableList(lastKnownElr);
    }

    public TopicPartitionInfo(int partition, Node leader, List<Node> replicas, List<Node> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = Collections.unmodifiableList(replicas);
        this.isr = Collections.unmodifiableList(isr);
        this.elr = null;
        this.lastKnownElr = null;
    }

    /**
     * 获取分区ID
     * @return 分区的唯一标识符
     */
    public int partition() {
        return partition;
    }

    /**
     * 获取分区的领导者节点
     * @return 当前的领导者节点，如果没有领导者则返回null
     */
    public Node leader() {
        return leader;
    }

    /**
     * 获取分区的所有副本列表
     * 副本按照分配顺序排列，列表的第一个元素（头部）是优先副本
     * 
     * 注意：由于bug的原因，0.11.0.0版本之前的broker返回的副本顺序是不确定的
     * 
     * @return 所有副本节点的不可修改列表
     */
    public List<Node> replicas() {
        return replicas;
    }

    /**
     * 获取同步副本（ISR）列表
     * ISR中的副本与领导者保持同步，可以在领导者故障时成为新的领导者
     * 
     * @return 同步副本的不可修改列表（列表顺序不确定）
     */
    public List<Node> isr() {
        return isr;
    }

    /**
     * 获取合格的领导者副本（ELR）列表
     * ELR中的副本满足成为领导者的条件
     * 
     * @return 合格领导者副本的不可修改列表（列表顺序不确定）
     */
    public List<Node> elr() {
        return elr;
    }

    /**
     * 获取最后已知的合格领导者副本列表
     * 用于在集群状态发生变化时保持历史记录
     * 
     * @return 最后已知的合格领导者副本的不可修改列表（列表顺序不确定）
     */
    public List<Node> lastKnownElr() {
        return lastKnownElr;
    }

    public String toString() {
        String elrString = elr != null ? elr.stream().map(Node::toString).collect(Collectors.joining(", ")) : "N/A";
        String lastKnownElrString = lastKnownElr != null ? lastKnownElr.stream().map(Node::toString).collect(Collectors.joining(", ")) : "N/A";
        return "(partition=" + partition + ", leader=" + leader + ", replicas=" +
            replicas.stream().map(Node::toString).collect(Collectors.joining(", ")) + ", isr=" + isr.stream().map(Node::toString).collect(Collectors.joining(", ")) +
            ", elr=" + elrString + ", lastKnownElr=" + lastKnownElrString + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicPartitionInfo that = (TopicPartitionInfo) o;

        return partition == that.partition &&
            Objects.equals(leader, that.leader) &&
            Objects.equals(replicas, that.replicas) &&
            Objects.equals(isr, that.isr) &&
            Objects.equals(elr, that.elr) &&
            Objects.equals(lastKnownElr, that.lastKnownElr);
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + (leader != null ? leader.hashCode() : 0);
        result = 31 * result + (replicas != null ? replicas.hashCode() : 0);
        result = 31 * result + (isr != null ? isr.hashCode() : 0);
        result = 31 * result + (elr != null ? elr.hashCode() : 0);
        result = 31 * result + (lastKnownElr != null ? lastKnownElr.hashCode() : 0);
        return result;
    }
}
