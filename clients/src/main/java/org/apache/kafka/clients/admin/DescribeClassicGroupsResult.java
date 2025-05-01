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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * {@link Admin#describeClassicGroups(Collection, DescribeClassicGroupsOptions)}调用的结果类。
 * 用于异步获取Kafka经典消费者组的详细信息。
 * <p>
 * 该类的API仍在演进中，详见{@link Admin}。
 * 
 * 应用场景：
 * 1. 消费者组管理：异步获取多个消费者组的详细信息
 * 2. 监控告警：检查消费者组的状态和健康情况
 * 3. 问题诊断：分析消费者组的成员分布和消费进度
 */
@InterfaceStability.Evolving
public class DescribeClassicGroupsResult {

    /**
     * 存储消费者组查询结果的Future映射
     * key为消费者组ID，value为对应的Future对象
     * 每个Future完成时将返回对应消费者组的详细描述信息
     */
    private final Map<String, KafkaFuture<ClassicGroupDescription>> futures;

    /**
     * 构造函数，初始化包含查询结果的Future映射
     * 
     * @param futures 消费者组ID到其描述信息Future的映射
     */
    public DescribeClassicGroupsResult(final Map<String, KafkaFuture<ClassicGroupDescription>> futures) {
        this.futures = futures;
    }

    /**
     * 返回消费者组ID到其描述信息Future的映射
     * 
     * 实现说明：
     * - 返回futures的深拷贝，避免外部修改影响内部状态
     * - 每个Future完成时返回对应消费者组的ClassicGroupDescription对象
     * - 支持并发访问和异步处理
     * 
     * @return 返回一个新的Map，包含所有消费者组的Future对象
     */
    public Map<String, KafkaFuture<ClassicGroupDescription>> describedGroups() {
        return new HashMap<>(futures);
    }

    /**
     * 返回一个Future，当所有查询都完成时，返回所有消费者组的描述信息
     * 
     * 实现说明：
     * - 使用KafkaFuture.allOf等待所有Future完成
     * - 通过thenApply方法转换结果格式
     * - 异常处理：由于allOf确保所有Future都成功完成，get()方法抛出异常是不可能的
     * - 返回的Map包含所有消费者组的完整信息
     * 
     * @return 返回KafkaFuture对象，其结果为消费者组ID到描述信息的映射
     */
    public KafkaFuture<Map<String, ClassicGroupDescription>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            nil -> {
                Map<String, ClassicGroupDescription> descriptions = new HashMap<>(futures.size());
                futures.forEach((key, future) -> {
                    try {
                        descriptions.put(key, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况不应该发生，因为KafkaFuture#allOf已经确保
                        // 所有的futures都已成功完成
                        throw new RuntimeException(e);
                    }
                });
                return descriptions;
            });
    }
}
