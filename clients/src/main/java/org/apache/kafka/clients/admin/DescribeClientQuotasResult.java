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
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.util.Map;

/**
 * {@link Admin#describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)}调用的结果类。
 *
 * 此类封装了客户端配额查询的异步操作结果。它使用KafkaFuture来处理异步操作，
 * 允许用户在查询完成后获取配额信息。
 * 
 * 应用场景：
 * 1. 异步获取客户端配额设置
 * 2. 批量检查多个客户端的资源限制
 * 3. 监控和管理系统中的资源使用限制
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeClientQuotasResult {

    /**
     * 存储查询结果的Future对象。Map的结构为：
     * - 键：ClientQuotaEntity（表示客户端实体，如用户、客户端ID等）
     * - 值：配额设置的Map，其中：
     *   - 键：配额类型（如生产速率、消费速率等）
     *   - 值：配额值（数值类型）
     */
    private final KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities;

    /**
     * 构造函数，初始化查询结果。
     * 
     * 将实体映射到其配置的配额值。注意：如果某个实体的某种配额类型没有定义值，
     * 则该配额类型不会包含在结果Map中。
     *
     * @param entities 匹配过滤条件的实体集合的Future对象
     */
    public DescribeClientQuotasResult(KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities) {
        this.entities = entities;
    }

    /**
     * 获取查询结果的Future对象。
     * 
     * 返回一个Map，其中：
     * - 键是配额实体（ClientQuotaEntity）
     * - 值是该实体的配额设置Map
     * 
     * 使用示例：
     * Map<ClientQuotaEntity, Map<String, Double>> quotas = result.entities().get();
     * 
     * @return 包含配额信息的Future对象
     */
    public KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities() {
        return entities;
    }
}
