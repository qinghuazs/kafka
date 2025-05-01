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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * {@link Admin#describeTransactions(Collection)} 调用的结果类。
 * 该类用于获取Kafka事务的描述信息，包括事务状态、协调器位置等。
 */
@InterfaceStability.Evolving
public class DescribeTransactionsResult {
    // 存储事务协调器键到其事务描述信息Future的映射
    private final Map<CoordinatorKey, KafkaFuture<TransactionDescription>> futures;

    /**
     * 构造函数，初始化事务描述结果
     * 
     * @param futures 包含事务协调器键到其事务描述信息Future的映射
     */
    DescribeTransactionsResult(Map<CoordinatorKey, KafkaFuture<TransactionDescription>> futures) {
        // 初始化futures字段，存储每个事务的异步描述结果
        this.futures = futures;
    }

    /**
     * 获取指定事务ID的描述信息
     * 
     * @param transactionalId 要描述的事务ID
     * @return 返回包含特定事务ID描述信息的Future
     * @throws IllegalArgumentException 如果请求中不包含指定的事务ID
     */
    public KafkaFuture<TransactionDescription> description(String transactionalId) {
        // 根据事务ID创建协调器键
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        // 从futures映射中获取对应的Future
        KafkaFuture<TransactionDescription> future = futures.get(key);
        // 如果Future为null，说明请求中不包含该事务ID
        if (future == null) {
            throw new IllegalArgumentException("TransactionalId " +
                "`" + transactionalId + "` was not included in the request");
        }
        // 返回该事务ID的Future结果
        return future;
    }

    /**
     * 获取包含所有请求的事务描述的Future映射
     * 如果任何事务ID的描述获取失败，该Future也会失败
     * 
     * @return 返回一个Future，当所有事务描述完成时成功，或在任何描述无法获取时失败
     */
    public KafkaFuture<Map<String, TransactionDescription>> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(nil -> {
                // 创建结果映射，预设容量以优化性能
                Map<String, TransactionDescription> results = new HashMap<>(futures.size());
                // 遍历所有Future条目
                for (Map.Entry<CoordinatorKey, KafkaFuture<TransactionDescription>> entry : futures.entrySet()) {
                    try {
                        // 获取Future的结果并存入results映射
                        results.put(entry.getKey().idValue, entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况理论上不会发生，因为KafkaFuture.allOf已经确保所有Future都成功完成
                        throw new RuntimeException(e);
                    }
                }
                // 返回包含所有事务描述的映射
                return results;
            });
    }
}
