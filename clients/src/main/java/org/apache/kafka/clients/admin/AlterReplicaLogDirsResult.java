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
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.UnknownServerException;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * {@link Admin#alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)}操作的结果。
 * 
 * 该类用于处理Kafka副本日志目录修改操作的异步结果。通过此类可以：
 * - 使用{@link #values()}方法获取每个指定{@link TopicPartitionReplica}的详细操作结果
 * - 使用{@link #all()}方法获取所有副本操作的整体结果
 * 
 * 应用场景：
 * 1. 在Kafka集群中修改某些分区副本的日志存储目录时使用
 * 2. 用于实现副本日志目录的负载均衡和磁盘容量管理
 * 3. 在需要调整副本存储位置以提升性能或解决存储问题时使用
 */
@InterfaceStability.Evolving
public class AlterReplicaLogDirsResult {
    /**
     * 存储每个分区副本的日志目录修改操作的Future结果
     * - Key: TopicPartitionReplica对象，标识具体的主题分区副本
     * - Value: KafkaFuture对象，代表异步操作的结果
     */
    private final Map<TopicPartitionReplica, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化副本日志目录修改操作的结果集
     * @param futures 包含所有副本操作Future的映射表
     */
    AlterReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * 返回一个从{@link TopicPartitionReplica}到{@link KafkaFuture}的映射，用于获取每个副本迁移操作的状态。
     * 
     * 使用方法：
     * 1. 通过返回的map获取特定副本的KafkaFuture对象
     * 2. 调用该Future的{@link KafkaFuture#get()}方法检查操作结果
     * 3. 如果操作成功，方法将静默返回；如果失败，将抛出以下异常：
     *
     * <ul>
     *   <li>{@link CancellationException}: 任务被取消</li>
     *   <li>{@link InterruptedException}: I/O线程连接过程中被中断</li>
     *   <li>{@link ExecutionException}: 执行失败，可能的原因包括：</li>
     *   <ul>
     *     <li>{@link ClusterAuthorizationException}: 集群授权失败 (错误码：31)</li>
     *     <li>{@link InvalidTopicException}: 主题名称过长 (错误码：17)</li>
     *     <li>{@link LogDirNotFoundException}: 在broker上未找到指定的日志目录 (错误码：57)</li>
     *     <li>{@link ReplicaNotAvailableException}: broker上不存在该副本 (错误码：9)</li>
     *     <li>{@link KafkaStorageException}: 发生磁盘错误 (错误码：56)</li>
     *     <li>{@link UnknownServerException}: 未知服务器错误 (错误码：-1)</li>
     *   </ul>
     * </ul>
     * 
     * @return 包含所有副本操作结果的映射表
     */
    public Map<TopicPartitionReplica, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * 返回一个{@link KafkaFuture}对象，用于检查所有副本迁移操作的整体结果。
     * 
     * 实现细节：
     * 1. 使用KafkaFuture.allOf()方法聚合所有副本操作的Future
     * 2. 只有当所有副本迁移都成功完成时，调用{@link KafkaFuture#get()}才会成功返回
     * 3. 如果任何副本迁移失败，将抛出在{@link #values()}方法中描述的异常
     * 
     * @return 代表所有副本迁移操作最终结果的Future对象
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
