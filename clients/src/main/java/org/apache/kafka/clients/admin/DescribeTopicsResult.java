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
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * {@link KafkaAdminClient#describeTopics(Collection)} 调用的结果类。
 * 该类用于获取Kafka主题的描述信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeTopicsResult {
    // 存储主题ID到其描述信息Future的映射
    private final Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures;
    // 存储主题名称到其描述信息Future的映射
    private final Map<String, KafkaFuture<TopicDescription>> nameFutures;

    /**
     * 构造函数，初始化主题描述结果
     * 注：此构造函数仅用于测试目的
     * 
     * @param topicIdFutures 主题ID到描述信息Future的映射
     * @param nameFutures 主题名称到描述信息Future的映射
     * @throws IllegalArgumentException 如果两个参数都为null或都不为null
     */
    protected DescribeTopicsResult(Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures, 
                                 Map<String, KafkaFuture<TopicDescription>> nameFutures) {
        // 检查参数有效性：两个参数不能同时存在
        if (topicIdFutures != null && nameFutures != null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be specified.");
        // 检查参数有效性：两个参数不能同时为null
        if (topicIdFutures == null && nameFutures == null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be null.");
        // 初始化字段
        this.topicIdFutures = topicIdFutures;
        this.nameFutures = nameFutures;
    }

    /**
     * 使用主题ID创建DescribeTopicsResult实例的工厂方法
     */
    static DescribeTopicsResult ofTopicIds(Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures) {
        // 创建一个只包含主题ID映射的实例
        return new DescribeTopicsResult(topicIdFutures, null);
    }

    /**
     * 使用主题名称创建DescribeTopicsResult实例的工厂方法
     */
    static DescribeTopicsResult ofTopicNames(Map<String, KafkaFuture<TopicDescription>> nameFutures) {
        // 创建一个只包含主题名称映射的实例
        return new DescribeTopicsResult(null, nameFutures);
    }

    /**
     * 获取主题ID到其描述信息Future的映射
     * 仅当使用TopicIdCollection进行查询时有效
     */
    public Map<Uuid, KafkaFuture<TopicDescription>> topicIdValues() {
        // 返回主题ID的Future映射
        return topicIdFutures;
    }

    /**
     * 获取主题名称到其描述信息Future的映射
     * 仅当使用TopicNameCollection进行查询时有效
     */
    public Map<String, KafkaFuture<TopicDescription>> topicNameValues() {
        // 返回主题名称的Future映射
        return nameFutures;
    }

    /**
     * 获取包含所有主题名称描述的Future映射
     * 仅当使用主题名称进行查询时有效，且所有描述操作都成功时才返回结果
     */
    public KafkaFuture<Map<String, TopicDescription>> allTopicNames() {
        // 调用通用的all方法处理主题名称Future
        return all(nameFutures);
    }

    /**
     * 获取包含所有主题ID描述的Future映射
     * 仅当使用主题ID进行查询时有效，且所有描述操作都成功时才返回结果
     */
    public KafkaFuture<Map<Uuid, TopicDescription>> allTopicIds() {
        // 调用通用的all方法处理主题ID Future
        return all(topicIdFutures);
    }

    /**
     * 返回一个Future，只有当所有主题描述操作都成功时才完成
     * 
     * @param futures 要处理的Future映射
     * @return 包含所有主题描述的Future映射
     */
    private static <T> KafkaFuture<Map<T, TopicDescription>> all(Map<T, KafkaFuture<TopicDescription>> futures) {
        // 如果输入为null，直接返回null
        if (futures == null) return null;
        // 创建一个等待所有Future完成的组合Future
        KafkaFuture<Void> future = KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
        // 当所有Future完成时，转换结果为描述映射
        return future.thenApply(v -> {
            // 创建结果映射，预设容量以优化性能
            Map<T, TopicDescription> descriptions = new HashMap<>(futures.size());
            // 遍历所有Future条目
            for (Map.Entry<T, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    // 获取Future的结果并存入映射
                    descriptions.put(entry.getKey(), entry.getValue().get());
                } catch (InterruptedException | ExecutionException e) {
                    // 这种情况理论上不会发生，因为KafkaFuture.allOf已经确保所有Future都成功完成
                    throw new RuntimeException(e);
                }
            }
            // 返回包含所有主题描述的映射
            return descriptions;
        });
    }
}
