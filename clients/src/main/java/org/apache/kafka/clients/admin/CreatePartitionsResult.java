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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * {@link Admin#createPartitions(Map)}调用的结果类。
 * 该类用于处理Kafka主题分区创建操作的异步结果，每个主题的分区创建操作都会返回一个Future对象。
 * 
 * 应用场景：
 * 1. 当需要为多个主题同时增加分区时，可以通过此类异步获取每个主题的操作结果
 * 2. 支持批量检查所有分区创建操作的完成状态
 * 
 * 设计考虑：
 * 1. 使用Map存储每个主题的Future，支持并发操作和异步结果获取
 * 2. 提供聚合方法简化多个主题的结果处理
 * 
 * 注意：该类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class CreatePartitionsResult {

    /**
     * 存储每个主题的分区创建操作的Future结果
     * Key: 主题名称
     * Value: 对应主题的分区创建操作的Future
     * KafkaFuture<Void>表示操作成功时返回null，失败时抛出异常
     */
    private final Map<String, KafkaFuture<Void>> values;

    /**
     * 构造函数，初始化分区创建结果映射
     * @param values 主题名称到对应Future的映射，包含每个主题的分区创建操作的异步结果
     */
    CreatePartitionsResult(Map<String, KafkaFuture<Void>> values) {
        this.values = values;
    }

    /**
     * 获取所有主题的分区创建操作的Future结果映射
     * 
     * @return 返回一个Map，其中：
     *         - Key为主题名称
     *         - Value为该主题分区创建操作的Future
     *         - Future完成时，成功返回null，失败抛出异常
     */
    public Map<String, KafkaFuture<Void>> values() {
        return values;
    }

    /**
     * 获取一个聚合的Future，用于检查所有分区创建操作的完成状态
     * 
     * 实现细节：
     * 1. 使用KafkaFuture.allOf方法组合所有主题的Future
     * 2. 将Map中的Future集合转换为数组
     * 3. 只有当所有操作都成功完成时，返回的Future才会成功完成
     * 4. 如果任何一个操作失败，返回的Future将抛出异常
     * 
     * @return 返回一个KafkaFuture<Void>，当所有分区创建操作都成功时完成
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(values.values().toArray(new KafkaFuture[0]));
    }
}
