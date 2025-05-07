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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;

/**
 * ShareConsumer委托接口
 * 这个扩展接口提供了一些方法来暴露{@link ShareConsumer}的内部实现，用于各种测试场景
 *
 * 应用场景：
 * 1. 单元测试中访问内部状态
 * 2. 集成测试中验证行为
 * 3. 性能测试中监控指标
 * 4. 调试过程中检查内部状态
 *
 * 重要说明：
 * 此接口仅供内部使用，不适用于最终用户。内部用户不应尝试确定底层实现，
 * 以避免依赖不稳定的接口。相反，应该使用{@link ShareConsumer} API契约
 * 作为调用者的接口。
 *
 * 设计考虑：
 * 1. 泛型支持，保持类型安全
 * 2. 最小化暴露的内部接口
 * 3. 清晰的测试边界
 * 4. 可控的内部访问
 *
 * @param <K> 消息键的类型
 * @param <V> 消息值的类型
 */
public interface ShareConsumerDelegate<K, V> extends ShareConsumer<K, V> {

    /**
     * 获取客户端ID
     * 用于标识和追踪特定的消费者实例
     *
     * @return 客户端的唯一标识符
     */
    String clientId();

    /**
     * 获取指标注册表
     * 用于访问和管理消费者的所有度量指标
     *
     * @return Metrics实例，包含所有注册的度量指标
     */
    Metrics metricsRegistry();

    /**
     * 获取Kafka共享消费者指标
     * 提供对特定于共享消费者的度量指标的访问
     *
     * @return KafkaShareConsumerMetrics实例，包含共享消费者的专用指标
     */
    KafkaShareConsumerMetrics kafkaShareConsumerMetrics();
}
