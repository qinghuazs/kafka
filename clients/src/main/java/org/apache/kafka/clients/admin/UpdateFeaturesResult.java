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

import java.util.Map;

/**
 * {@link Admin#updateFeatures(Map, UpdateFeaturesOptions)}调用的结果类。
 * 用于异步处理Kafka特性更新操作的结果。
 * 
 * 应用场景：
 * 1. 特性管理：批量更新Kafka集群的特性配置
 * 2. 版本升级：在升级过程中启用或禁用特定特性
 * 3. 集群维护：动态调整集群功能和行为
 *
 * The API of this class is evolving, see {@link Admin} for details.
 * 该类的API仍在演进中，详见{@link Admin}。
 */
public class UpdateFeaturesResult {
    /**
     * 存储特性更新操作的Future映射
     * key为特性名称，value为对应的Future对象
     * 使用KafkaFuture<Void>表示更新操作的完成状态，不需要返回具体结果
     */
    private final Map<String, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化特性更新结果对象
     * 
     * @param futures 特性名称到Future的映射，用于检查每个特性更新的状态
     *               Map的key是特性名称，value是代表更新操作的Future
     */
    UpdateFeaturesResult(final Map<String, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * 获取特性更新操作的Future映射
     * 
     * 实现说明：
     * - 直接返回futures字段，允许调用者访问原始的Future映射
     * - 每个Future完成时表示对应特性的更新操作已完成
     * - 支持并发访问和异步处理
     * 
     * @return 返回特性名称到Future的映射
     */
    public Map<String, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * 返回一个Future，该Future在所有特性更新操作都成功完成时完成
     * 
     * 实现说明：
     * - 使用KafkaFuture.allOf合并所有特性更新的Future
     * - 将futures.values()转换为数组作为参数
     * - 返回的Future仅在所有特性都更新成功时才会成功完成
     * - 如果任何特性更新失败，返回的Future将携带第一个遇到的异常
     * 
     * @return 返回一个KafkaFuture对象，代表所有特性更新操作的组合结果
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
