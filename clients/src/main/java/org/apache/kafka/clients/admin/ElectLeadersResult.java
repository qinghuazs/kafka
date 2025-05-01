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


import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link Admin#electLeaders(ElectionType, Set, ElectLeadersOptions)} 调用的结果类。
 * 该类用于获取Kafka主题分区leader选举的结果信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public final class ElectLeadersResult {
    // 存储选举结果的Future，映射包含每个主题分区的选举结果或错误信息
    private final KafkaFuture<Map<TopicPartition, Optional<Throwable>>> electionFuture;

    /**
     * 构造函数，初始化选举结果
     * 
     * @param electionFuture 包含主题分区选举结果的Future映射
     */
    ElectLeadersResult(KafkaFuture<Map<TopicPartition, Optional<Throwable>>> electionFuture) {
        // 初始化electionFuture字段，存储选举结果
        this.electionFuture = electionFuture;
    }

    /**
     * 获取尝试进行leader选举的主题分区的Future结果
     * 如果选举成功，对应主题分区的值将是空Optional
     * 如果选举失败，Optional中将包含错误信息
     * 
     * @return 返回包含所有主题分区选举结果的Future映射
     */
    public KafkaFuture<Map<TopicPartition, Optional<Throwable>>> partitions() {
        // 直接返回选举结果Future
        return electionFuture;
    }

    /**
     * 返回一个Future，只有当所有主题分区的选举都成功时才会成功完成
     * 
     * @return 返回一个表示所有选举是否成功的Future
     */
    public KafkaFuture<Void> all() {
        // 创建一个新的KafkaFutureImpl实例用于返回结果
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        // 当分区选举结果Future完成时执行回调
        partitions().whenComplete(
                (topicPartitions, throwable) -> {
                    if (throwable != null) {
                        // 如果发生异常，使用该异常完成返回的Future
                        result.completeExceptionally(throwable);
                    } else {
                        // 检查每个分区的选举结果
                        for (Optional<Throwable> exception : topicPartitions.values()) {
                            if (exception.isPresent()) {
                                // 如果任何分区选举失败，使用第一个遇到的异常完成Future
                                result.completeExceptionally(exception.get());
                                return;
                            }
                        }
                        // 所有分区选举都成功，完成Future
                        result.complete(null);
                    }
                });

        // 返回结果Future
        return result;
    }
}
