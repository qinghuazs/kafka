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

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link Admin#fenceProducers(Collection)} 调用的结果类。
 * 该类用于处理生产者隔离操作的结果，包括生产者ID和纪元信息的获取。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class FenceProducersResult {
    // 存储协调器键到生产者ID和纪元Future的映射
    private final Map<CoordinatorKey, KafkaFuture<ProducerIdAndEpoch>> futures;

    /**
     * 构造函数，初始化生产者隔离结果
     * 
     * @param futures 包含协调器键到生产者ID和纪元Future的映射
     */
    FenceProducersResult(Map<CoordinatorKey, KafkaFuture<ProducerIdAndEpoch>> futures) {
        // 初始化futures字段，存储隔离操作的Future结果
        this.futures = futures;
    }

    /**
     * 获取事务ID到隔离状态Future的映射
     * 该映射可用于检查各个生产者的隔离状态
     * 
     * @return 返回事务ID到隔离状态Future的映射
     */
    public Map<String, KafkaFuture<Void>> fencedProducers() {
        // 将协调器键映射转换为事务ID映射
        return futures.entrySet().stream().collect(Collectors.toMap(
            // 使用事务ID作为键
            e -> e.getKey().idValue,
            // 将ProducerIdAndEpoch转换为Void，因为我们只关心操作是否成功
            e -> e.getValue().thenApply(p -> null)
        ));
    }

    /**
     * 获取指定事务初始化时生成的生产者ID
     * 
     * @param transactionalId 事务ID
     * @return 返回包含生产者ID的Future
     */
    public KafkaFuture<Long> producerId(String transactionalId) {
        // 使用findAndApply获取生产者ID
        return findAndApply(transactionalId, p -> p.producerId);
    }

    /**
     * 获取指定事务初始化时生成的纪元ID
     * 
     * @param transactionalId 事务ID
     * @return 返回包含纪元ID的Future
     */
    public KafkaFuture<Short> epochId(String transactionalId) {
        // 使用findAndApply获取纪元ID
        return findAndApply(transactionalId, p -> p.epoch);
    }

    /**
     * 获取一个Future，只有当所有生产者隔离操作都成功时才会成功完成
     * 
     * @return 返回表示所有隔离操作是否成功的Future
     */
    public KafkaFuture<Void> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }

    /**
     * 查找并应用转换函数到指定事务的Future结果
     * 
     * @param transactionalId 事务ID
     * @param followup 转换函数，将ProducerIdAndEpoch转换为目标类型
     * @return 返回转换后的Future结果
     * @throws IllegalArgumentException 如果事务ID不在请求中
     */
    private <T> KafkaFuture<T> findAndApply(String transactionalId, KafkaFuture.BaseFunction<ProducerIdAndEpoch, T> followup) {
        // 根据事务ID创建协调器键
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        // 获取对应的Future
        KafkaFuture<ProducerIdAndEpoch> future = futures.get(key);
        // 如果Future为null，说明事务ID不在请求中
        if (future == null) {
            throw new IllegalArgumentException("TransactionalId " +
                "`" + transactionalId + "` was not included in the request");
        }
        // 应用转换函数并返回结果
        return future.thenApply(followup);
    }
}
