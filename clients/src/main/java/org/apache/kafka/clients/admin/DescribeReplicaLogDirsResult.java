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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * {@link Admin#describeReplicaLogDirs(Collection)} 调用的结果类。
 * 该类用于获取Kafka主题分区副本的日志目录信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeReplicaLogDirsResult {
    // 存储每个主题分区副本对应的日志目录信息Future的映射
    private final Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures;

    /**
     * 构造函数，初始化副本日志目录描述结果
     * 
     * @param futures 包含每个主题分区副本对应的日志目录信息Future的映射
     */
    DescribeReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures) {
        // 初始化futures字段，存储每个副本的异步日志目录信息结果
        this.futures = futures;
    }

    /**
     * 返回从副本到其日志目录信息Future的映射，用于检查各个副本的日志目录信息
     * 
     * @return 返回副本到日志目录信息Future的映射
     */
    public Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> values() {
        // 返回futures映射，允许调用者分别获取每个副本的日志目录信息
        return futures;
    }

    /**
     * 返回一个Future，只有当所有副本的日志目录信息都可用时才会成功完成
     * 
     * @return 包含所有副本日志目录信息的Future
     */
    public KafkaFuture<Map<TopicPartitionReplica, ReplicaLogDirInfo>> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(v -> {
                // 创建一个新的HashMap来存储所有副本的日志目录信息
                Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaLogDirInfos = new HashMap<>();
                // 遍历所有Future条目
                for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> entry : futures.entrySet()) {
                    try {
                        // 获取每个Future的结果并存入replicaLogDirInfos映射
                        replicaLogDirInfos.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况理论上不会发生，因为KafkaFuture.allOf已经确保所有Future都成功完成
                        throw new RuntimeException(e);
                    }
                }
                // 返回包含所有副本日志目录信息的映射
                return replicaLogDirInfos;
            });
    }

    /**
     * 表示副本日志目录信息的内部类
     */
    public static class ReplicaLogDirInfo {
        // 当前副本的日志目录路径
        private final String currentReplicaLogDir;
        // 当前副本的偏移量延迟（HW与LEO之间的差值）
        private final long currentReplicaOffsetLag;
        // 未来副本的日志目录路径（如果正在进行目录迁移）
        private final String futureReplicaLogDir;
        // 未来副本的偏移量延迟
        private final long futureReplicaOffsetLag;

        /**
         * 默认构造函数，创建一个所有字段都为无效值的实例
         */
        ReplicaLogDirInfo() {
            // 使用无效值初始化所有字段
            this(null, DescribeLogDirsResponse.INVALID_OFFSET_LAG, null, DescribeLogDirsResponse.INVALID_OFFSET_LAG);
        }

        /**
         * 构造函数，初始化副本日志目录信息
         * 
         * @param currentReplicaLogDir 当前副本的日志目录路径
         * @param currentReplicaOffsetLag 当前副本的偏移量延迟
         * @param futureReplicaLogDir 未来副本的日志目录路径
         * @param futureReplicaOffsetLag 未来副本的偏移量延迟
         */
        ReplicaLogDirInfo(String currentReplicaLogDir,
                          long currentReplicaOffsetLag,
                          String futureReplicaLogDir,
                          long futureReplicaOffsetLag) {
            // 初始化所有字段
            this.currentReplicaLogDir = currentReplicaLogDir;
            this.currentReplicaOffsetLag = currentReplicaOffsetLag;
            this.futureReplicaLogDir = futureReplicaLogDir;
            this.futureReplicaOffsetLag = futureReplicaOffsetLag;
        }

        /**
         * 获取当前副本在指定broker上的日志目录路径
         * 如果在指定broker上找不到该分区的副本，则返回null
         */
        public String getCurrentReplicaLogDir() {
            // 返回当前副本的日志目录路径
            return currentReplicaLogDir;
        }

        /**
         * 获取当前副本的偏移量延迟，定义为：max(分区HW - 副本LEO, 0)
         */
        public long getCurrentReplicaOffsetLag() {
            // 返回当前副本的偏移量延迟
            return currentReplicaOffsetLag;
        }

        /**
         * 获取未来副本在指定broker上的日志目录路径
         * 如果该分区的副本不在迁移过程中，则返回null
         */
        public String getFutureReplicaLogDir() {
            // 返回未来副本的日志目录路径
            return futureReplicaLogDir;
        }

        /**
         * 获取未来副本的偏移量延迟，定义为：副本LEO - 目标目录中未来日志的LEO
         * 如果不存在副本或副本不在迁移过程中，则返回-1
         */
        public long getFutureReplicaOffsetLag() {
            // 返回未来副本的偏移量延迟
            return futureReplicaOffsetLag;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (futureReplicaLogDir != null) {
                builder.append("(currentReplicaLogDir=")
                    .append(currentReplicaLogDir)
                    .append(", futureReplicaLogDir=")
                    .append(futureReplicaLogDir)
                    .append(", futureReplicaOffsetLag=")
                    .append(futureReplicaOffsetLag)
                    .append(")");
            } else {
                builder.append("ReplicaLogDirInfo(currentReplicaLogDir=").append(currentReplicaLogDir).append(")");
            }
            return builder.toString();
        }
    }
}
