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

import org.apache.kafka.common.Node;

import java.util.Comparator;
import java.util.Objects;

/**
 * 副本视图，由{@link ReplicaSelector}用于确定首选副本。
 * 该接口提供了副本的关键信息，包括：
 * 1. 副本的网络端点信息（主机名、端口、机架等）
 * 2. 副本的日志末端偏移量（LEO）
 * 3. 副本追赶高水位的时间信息
 * 
 * 这些信息用于帮助选择器评估每个副本的状态和适用性。
 */
public interface ReplicaView {

    /**
     * 获取副本的网络端点信息（主机名、端口、机架等）
     * 用于网络连接和机架感知的副本选择
     * @return 包含副本网络信息的Node对象
     */
    Node endpoint();

    /**
     * 获取副本的日志末端偏移量（Log End Offset, LEO）
     * 表示副本当前已复制的最后一条消息的位置
     * @return 副本的LEO值
     */
    long logEndOffset();

    /**
     * 获取自副本最后一次追赶到高水位以来的毫秒数
     * 对于leader副本，该值始终为0
     * 该指标用于评估副本的同步状态，值越小表示副本越新鲜
     * @return 追赶延迟的毫秒数
     */
    long timeSinceLastCaughtUpMs();

    /**
     * 创建一个副本视图的比较器，按照"最新程度"排序
     * 当选择器遇到多个同等条件的副本时，使用此比较器进行确定性选择
     * 
     * 比较规则（按优先级排序）：
     * 1. 日志末端偏移量（LEO）更大的优先
     * 2. 追赶延迟更小的优先
     * 3. 节点ID更小的优先
     * 
     * @return 副本比较器
     */
    static Comparator<ReplicaView> comparator() {
        return Comparator.comparingLong(ReplicaView::logEndOffset)
            .thenComparing(Comparator.comparingLong(ReplicaView::timeSinceLastCaughtUpMs).reversed())
            .thenComparing(replicaInfo -> replicaInfo.endpoint().id());
    }

    class DefaultReplicaView implements ReplicaView {
        private final Node endpoint;
        private final long logEndOffset;
        private final long timeSinceLastCaughtUpMs;

        public DefaultReplicaView(Node endpoint, long logEndOffset, long timeSinceLastCaughtUpMs) {
            this.endpoint = endpoint;
            this.logEndOffset = logEndOffset;
            this.timeSinceLastCaughtUpMs = timeSinceLastCaughtUpMs;
        }

        @Override
        public Node endpoint() {
            return endpoint;
        }

        @Override
        public long logEndOffset() {
            return logEndOffset;
        }

        @Override
        public long timeSinceLastCaughtUpMs() {
            return timeSinceLastCaughtUpMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DefaultReplicaView that = (DefaultReplicaView) o;
            return logEndOffset == that.logEndOffset &&
                    Objects.equals(endpoint, that.endpoint) &&
                    Objects.equals(timeSinceLastCaughtUpMs, that.timeSinceLastCaughtUpMs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endpoint, logEndOffset, timeSinceLastCaughtUpMs);
        }

        @Override
        public String toString() {
            return "DefaultReplicaView{" +
                    "endpoint=" + endpoint +
                    ", logEndOffset=" + logEndOffset +
                    ", timeSinceLastCaughtUpMs=" + timeSinceLastCaughtUpMs +
                    '}';
        }
    }
}
