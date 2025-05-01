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

/**
 * {@link Admin#deleteConsumerGroups(Collection)} 调用的结果类。
 * 该类用于处理批量删除消费者组的异步操作结果。
 * 
 * 应用场景：
 * 1. 当需要清理不再使用的消费者组时
 * 2. 在重新组织消费者组结构时删除旧的组
 * 3. 系统维护时批量清理过期的消费者组
 * 
 * 注意：该类的API仍在演进中，详见 {@link Admin}
 */
@InterfaceStability.Evolving
public class DeleteConsumerGroupsResult {
    /**
     * 存储每个消费者组删除操作的Future结果映射
     * - Key: 消费者组ID
     * - Value: 对应的删除操作的Future结果
     * - Future<Void>表示操作成功时不返回具体值，仅表示完成状态
     */
    private final Map<String, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化删除操作的结果集
     * @param futures 包含所有要删除的消费者组的Future操作映射
     */
    DeleteConsumerGroupsResult(final Map<String, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * 获取所有消费者组的删除操作状态
     * 
     * @return 返回一个Map，其中：
     *         - Key为消费者组ID
     *         - Value为对应的删除操作的Future
     *         通过返回的Map可以分别检查每个删除操作的状态
     * 
     * 实现细节：
     * 1. 创建一个新的HashMap，容量为原futures的大小，避免扩容
     * 2. 将原futures中的所有映射复制到新Map中
     * 3. 返回新Map，确保原futures不被外部修改
     */
    public Map<String, KafkaFuture<Void>> deletedGroups() {
        Map<String, KafkaFuture<Void>> deletedGroups = new HashMap<>(futures.size());
        deletedGroups.putAll(futures);
        return deletedGroups;
    }

    /**
     * 获取一个组合的Future，用于检查所有删除操作的整体状态
     * 
     * @return 返回一个KafkaFuture<Void>，只有当所有消费者组都成功删除时，该Future才会成功完成
     *         如果任何一个删除操作失败，该Future将抛出异常
     * 
     * 实现细节：
     * 1. 将futures中的所有Future提取为数组
     * 2. 使用KafkaFuture.allOf合并所有Future
     * 3. 只有当所有Future都成功完成时，返回的Future才会成功
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
