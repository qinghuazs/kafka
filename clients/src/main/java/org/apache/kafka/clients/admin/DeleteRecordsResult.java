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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * {@link Admin#deleteRecords(Map)}调用的结果类。
 *
 * 此类用于处理删除消息记录操作的结果：
 * 1. 包含每个主题分区的删除操作的异步结果
 * 2. 提供查询单个分区删除状态的方法
 * 3. 支持等待所有删除操作完成
 * 
 * 应用场景：
 * - 监控消息删除操作的进度
 * - 获取删除操作的最终结果
 * - 处理删除操作可能的异常情况
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteRecordsResult {

    /**
     * 存储每个主题分区的删除操作的异步结果
     * Key: 主题分区信息
     * Value: 该分区的删除操作的Future对象，完成时返回DeletedRecords
     */
    private final Map<TopicPartition, KafkaFuture<DeletedRecords>> futures;

    /**
     * 构造函数，初始化删除操作的结果集
     * @param futures 包含所有主题分区删除操作的Future映射
     */
    public DeleteRecordsResult(Map<TopicPartition, KafkaFuture<DeletedRecords>> futures) {
        this.futures = futures;
    }

    /**
     * 返回主题分区到对应Future的映射，用于检查各个分区的删除状态
     * 
     * @return 返回一个Map，其中：
     *         - Key为TopicPartition，表示特定的主题分区
     *         - Value为KafkaFuture<DeletedRecords>，可用于获取该分区的删除结果
     */
    public Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks() {
        return futures;
    }

    /**
     * 返回一个Future，只有当所有记录删除操作都成功时才会成功完成
     * 
     * @return 返回KafkaFuture<Void>，可用于等待所有删除操作完成
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
