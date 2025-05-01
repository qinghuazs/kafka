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
import org.apache.kafka.common.config.ConfigResource;

import java.util.Map;

/**
 * {@link Admin#incrementalAlterConfigs(Map, AlterConfigsOptions)} 调用的结果类。
 * 该类用于处理Kafka配置修改操作的异步结果，支持对单个资源的修改结果查询和所有资源的聚合结果查询。
 * 
 * 应用场景：
 * 1. 当需要修改Kafka主题、broker或其他资源的配置时使用
 * 2. 支持批量修改多个资源的配置并异步获取结果
 * 3. 提供对修改操作结果的细粒度控制和监控
 * 
 * 注意：该类的API仍在演进中，详见 {@link Admin} 文档。
 */
@InterfaceStability.Evolving
public class AlterConfigsResult {

    /**
     * 存储每个配置资源对应的修改操作的Future结果
     * Key: 配置资源（如主题、broker等）
     * Value: 对应资源的配置修改操作的异步结果
     * 实现说明：使用Map存储多个资源的修改结果，支持并发操作和异步处理
     */
    private final Map<ConfigResource, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化配置修改结果对象
     * @param futures 包含所有待修改资源的Future结果映射
     * 实现说明：直接保存传入的futures映射，用于后续结果查询
     */
    AlterConfigsResult(Map<ConfigResource, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * 返回资源到其对应Future的映射，用于检查每个资源的配置修改操作状态
     * @return 返回资源到Future的映射关系
     * 实现说明：直接返回内部futures映射，允许调用者分别查询每个资源的修改结果
     */
    public Map<ConfigResource, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * 返回一个聚合的Future，只有当所有配置修改操作都成功时才返回成功
     * @return 返回一个代表所有操作的聚合Future
     * 实现说明：
     * 1. 将所有资源的Future转换为数组
     * 2. 使用KafkaFuture.allOf方法创建一个聚合Future
     * 3. 只有当所有操作都成功时，该Future才会成功完成
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }

}
