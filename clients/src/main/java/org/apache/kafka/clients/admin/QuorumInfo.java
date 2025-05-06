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

import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * 该类用于描述从DescribeQuorumResponse中接收到的仲裁组（Quorum）状态信息。
 * 在Kafka的Raft实现中，仲裁组负责维护集群的一致性和高可用性。
 * 应用场景：
 * 1. 监控集群健康状态：通过查看leader、voters和observers的状态
 * 2. 故障诊断：检查副本同步状态和延迟情况
 * 3. 集群管理：了解当前的leader选举状态和各节点角色
 */
public class QuorumInfo {
    // 当前leader节点的ID
    private final int leaderId;
    // 当前的leader纪元，用于标识leader的任期
    private final long leaderEpoch;
    // 高水位标记，表示所有副本都已经复制的最大偏移量
    private final long highWatermark;
    // 参与投票的副本列表，这些副本可以参与leader选举
    private final List<ReplicaState> voters;
    // 观察者副本列表，这些副本只接收数据但不参与投票
    private final List<ReplicaState> observers;
    // Raft集群中的节点信息映射，key为节点ID，value为节点详细信息
    private final Map<Integer, Node> nodes;

    QuorumInfo(
        int leaderId,
        long leaderEpoch,
        long highWatermark,
        List<ReplicaState> voters,
        List<ReplicaState> observers,
        Map<Integer, Node> nodes
    ) {
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.highWatermark = highWatermark;
        this.voters = voters;
        this.observers = observers;
        this.nodes = nodes;
    }

    public int leaderId() {
        return leaderId;
    }

    public long leaderEpoch() {
        return leaderEpoch;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public List<ReplicaState> voters() {
        return voters;
    }

    public List<ReplicaState> observers() {
        return observers;
    }

    /**
     * @return The voter nodes in the Raft cluster, or an empty map if KIP-853 is not enabled.
     */
    public Map<Integer, Node> nodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuorumInfo that = (QuorumInfo) o;
        return leaderId == that.leaderId
            && leaderEpoch == that.leaderEpoch
            && highWatermark == that.highWatermark
            && Objects.equals(voters, that.voters)
            && Objects.equals(observers, that.observers)
            && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderId, leaderEpoch, highWatermark, voters, observers, nodes);
    }

    @Override
    public String toString() {
        return "QuorumInfo(" +
            "leaderId=" + leaderId +
            ", leaderEpoch=" + leaderEpoch +
            ", highWatermark=" + highWatermark +
            ", voters=" + voters +
            ", observers=" + observers +
            ", nodes=" + nodes +
            ')';
    }

    /**
     * 副本状态类，用于描述Kafka集群中每个副本的详细状态信息
     * 应用场景：
     * 1. 监控副本同步进度
     * 2. 检测副本延迟情况
     * 3. 识别潜在的问题副本
     */
    public static class ReplicaState {
        // 副本的唯一标识ID
        private final int replicaId;
        // 副本的目录ID，用于在存储层面唯一标识副本
        private final Uuid replicaDirectoryId;
        // 副本的日志末端偏移量，表示副本当前的数据量
        private final long logEndOffset;
        // 最后一次从leader获取数据的时间戳
        private final OptionalLong lastFetchTimestamp;
        // 最后一次与leader完全同步的时间戳
        private final OptionalLong lastCaughtUpTimestamp;

        ReplicaState() {
            this(0, Uuid.ZERO_UUID, 0, OptionalLong.empty(), OptionalLong.empty());
        }

        ReplicaState(
            int replicaId,
            Uuid replicaDirectoryId,
            long logEndOffset,
            OptionalLong lastFetchTimestamp,
            OptionalLong lastCaughtUpTimestamp
        ) {
            this.replicaId = replicaId;
            this.replicaDirectoryId = replicaDirectoryId;
            this.logEndOffset = logEndOffset;
            this.lastFetchTimestamp = lastFetchTimestamp;
            this.lastCaughtUpTimestamp = lastCaughtUpTimestamp;
        }

        /**
         * Return the ID for this replica.
         * @return The ID for this replica
         */
        public int replicaId() {
            return replicaId;
        }

        /**
         * Return the directory id of the replica if configured, or Uuid.ZERO_UUID if not.
         */
        public Uuid replicaDirectoryId() {
            return replicaDirectoryId;
        }

        /**
         * Return the logEndOffset known by the leader for this replica.
         * @return The logEndOffset for this replica
         */
        public long logEndOffset() {
            return logEndOffset;
        }

        /**
         * Return the last millisecond timestamp that the leader received a
         * fetch from this replica.
         * @return The value of the lastFetchTime if known, empty otherwise
         */
        public OptionalLong lastFetchTimestamp() {
            return lastFetchTimestamp;
        }

        /**
         * Return the last millisecond timestamp at which this replica was known to be
         * caught up with the leader.
         * @return The value of the lastCaughtUpTime if known, empty otherwise
         */
        public OptionalLong lastCaughtUpTimestamp() {
            return lastCaughtUpTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaState that = (ReplicaState) o;
            return replicaId == that.replicaId
                && Objects.equals(replicaDirectoryId, that.replicaDirectoryId)
                && logEndOffset == that.logEndOffset
                && lastFetchTimestamp.equals(that.lastFetchTimestamp)
                && lastCaughtUpTimestamp.equals(that.lastCaughtUpTimestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicaId, replicaDirectoryId, logEndOffset, lastFetchTimestamp, lastCaughtUpTimestamp);
        }

        @Override
        public String toString() {
            return "ReplicaState(" +
                "replicaId=" + replicaId +
                ", replicaDirectoryId=" + replicaDirectoryId +
                ", logEndOffset=" + logEndOffset +
                ", lastFetchTimestamp=" + lastFetchTimestamp +
                ", lastCaughtUpTimestamp=" + lastCaughtUpTimestamp +
                ')';
        }
    }

    /**
     * 节点类，描述Raft集群中每个节点的信息
     * 应用场景：
     * 1. 集群成员管理
     * 2. 节点通信配置
     * 3. 集群扩缩容操作
     */
    public static class Node {
        // 节点的唯一标识ID
        private final int nodeId;
        // 节点的Raft投票端点列表，包含通信地址等信息
        private final List<RaftVoterEndpoint> endpoints;

        /**
         * 创建一个新的Node实例
         * @param nodeId 节点ID
         * @param endpoints Raft投票端点列表
         */
        Node(int nodeId, List<RaftVoterEndpoint> endpoints) {
            this.nodeId = nodeId;
            this.endpoints = endpoints;
        }

        public int nodeId() {
            return nodeId;
        }

        public List<RaftVoterEndpoint> endpoints() {
            return endpoints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return nodeId == node.nodeId && Objects.equals(endpoints, node.endpoints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, endpoints);
        }

        @Override
        public String toString() {
            return "Node{" +
                "nodeId=" + nodeId +
                ", endpoints=" + endpoints +
                '}';
        }
    }
}
