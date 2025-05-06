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
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link Admin#listTransactions()} 调用的结果类。
 * <p>
 * 该类的API仍在演进中，详细信息请参见{@link Admin}。
 *
 * 该类用于处理Kafka集群中事务的查询结果。它提供了多种方式来获取事务列表：
 * 1. 获取所有事务的完整列表
 * 2. 按broker ID分组获取事务列表
 * 3. 获取每个broker的独立Future以支持细粒度的错误处理
 */
@InterfaceStability.Evolving
public class ListTransactionsResult {
    /**
     * 存储事务查询结果的Future对象
     * Map的键为broker ID，值为包含该broker上事务列表的Future
     * 使用KafkaFutureImpl确保异步操作的可靠性和线程安全性
     */
    private final KafkaFuture<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future;

    /**
     * 构造函数，初始化事务查询结果对象
     *
     * @param future 包含所有broker事务信息的Future对象
     */
    ListTransactionsResult(KafkaFuture<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future) {
        this.future = future;
    }

    /**
     * 获取所有事务的列表。如果任何底层请求失败，该方法返回的Future将携带第一个遇到的错误失败。
     * 
     * 实现细节：
     * 1. 调用allByBrokerId()方法获取按broker分组的事务列表
     * 2. 使用thenApply转换结果，将所有broker的事务列表合并成一个列表
     * 3. 创建一个新的ArrayList来存储所有事务
     * 4. 遍历每个broker的事务列表并添加到结果列表中
     *
     * @return 返回包含所有事务列表的Future。当所有事务列表都可用时Future完成，
     *         如果遇到不可重试的错误则失败
     */
    public KafkaFuture<Collection<TransactionListing>> all() {
        return allByBrokerId().thenApply(map -> {
            // 创建一个新的列表来存储所有事务
            List<TransactionListing> allListings = new ArrayList<>();
            // 遍历每个broker的事务列表并添加到结果列表中
            for (Collection<TransactionListing> listings : map.values()) {
                allListings.addAll(listings);
            }
            return allListings;
        });
    }

    /**
     * 获取一个Future，该Future返回一个Map，包含集群中每个broker的事务列表Future。
     * 这在以下场景特别有用：
     * 1. 只需要部分事务列表时
     * 2. 需要更细粒度的错误详情时
     * 
     * 实现细节：
     * 1. 创建一个新的KafkaFutureImpl来存储结果
     * 2. 为原始future添加完成回调
     * 3. 如果成功，创建一个新的Map复制broker futures
     * 4. 如果失败，使用异常完成结果Future
     *
     * @return 返回一个Future，其中包含按broker分组的Future Map。每个broker的Future在其对应的
     *         事务列表可用时独立完成。如果admin客户端无法查找集群中可用的broker，
     *         该方法返回的顶层Future可能会失败。
     */
    public KafkaFuture<Map<Integer, KafkaFuture<Collection<TransactionListing>>>> byBrokerId() {
        // 创建结果Future
        KafkaFutureImpl<Map<Integer, KafkaFuture<Collection<TransactionListing>>>> result = new KafkaFutureImpl<>();
        // 添加完成回调处理
        future.whenComplete((brokerFutures, exception) -> {
            if (brokerFutures != null) {
                // 创建一个新的Map来存储broker futures的副本
                Map<Integer, KafkaFuture<Collection<TransactionListing>>> brokerFuturesCopy =
                    new HashMap<>(brokerFutures.size());
                brokerFuturesCopy.putAll(brokerFutures);
                result.complete(brokerFuturesCopy);
            } else {
                // 如果发生异常，完成结果Future并携带异常
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    /**
     * 获取一个按broker ID分组的事务列表Map。如果任何底层请求失败，
     * 该方法返回的Future将携带第一个遇到的错误失败。
     * 
     * 实现细节：
     * 1. 创建结果Future和存储最终结果的Map
     * 2. 处理顶层异常，如果存在则直接失败
     * 3. 创建待处理响应集合，用于追踪未完成的broker响应
     * 4. 为每个broker的Future添加完成回调：
     *    - 如果发生异常，使用该异常完成结果Future
     *    - 如果成功，将结果添加到Map并更新待处理响应
     *    - 当所有响应都处理完成时，完成结果Future
     *
     * @return 返回一个Future，其中包含从broker ID到该broker管理的事务列表的映射。
     *         当所有事务列表都可用时Future完成，如果遇到不可重试的错误则失败。
     */
    public KafkaFuture<Map<Integer, Collection<TransactionListing>>> allByBrokerId() {
        // 创建结果Future和存储最终结果的Map
        KafkaFutureImpl<Map<Integer, Collection<TransactionListing>>> allFuture = new KafkaFutureImpl<>();
        Map<Integer, Collection<TransactionListing>> allListingsMap = new HashMap<>();

        // 添加完成回调处理
        future.whenComplete((map, topLevelException) -> {
            // 处理顶层异常
            if (topLevelException != null) {
                allFuture.completeExceptionally(topLevelException);
                return;
            }

            // 创建待处理响应集合
            Set<Integer> remainingResponses = new HashSet<>(map.keySet());
            // 处理每个broker的Future
            map.forEach((brokerId, future) ->
                future.whenComplete((listings, brokerException) -> {
                    if (brokerException != null) {
                        // 如果broker发生异常，完成结果Future并携带异常
                        allFuture.completeExceptionally(brokerException);
                    } else if (!allFuture.isDone()) {
                        // 将成功的结果添加到Map
                        allListingsMap.put(brokerId, listings);
                        remainingResponses.remove(brokerId);

                        // 当所有响应都处理完成时，完成结果Future
                        if (remainingResponses.isEmpty()) {
                            allFuture.complete(allListingsMap);
                        }
                    }
                })
            );
        });

        return allFuture;
    }

}
