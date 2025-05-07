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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Timer;

/**
 * 消费者委托接口
 * 这个扩展接口提供了一些方法来暴露{@link Consumer}的内部实现，主要用于各种测试场景
 *
 * <p/>
 *
 * <em>注意</em>：这个接口仅供内部使用，不适合最终用户使用。内部用户也不应该尝试确定底层实现
 * 以避免依赖不稳定的接口。相反，应该使用{@link Consumer} API契约作为调用者的接口。
 */
public interface ConsumerDelegate<K, V> extends Consumer<K, V> {

    /**
     * 获取客户端ID
     * 用于标识消费者客户端的唯一标识符
     *
     * @return 客户端ID字符串
     */
    String clientId();

    /**
     * 获取度量注册表
     * 提供对消费者内部度量指标的访问
     *
     * @return Metrics对象，包含所有注册的度量指标
     */
    Metrics metricsRegistry();

    /**
     * 获取Kafka消费者度量指标
     * 提供特定于Kafka消费者的度量指标
     *
     * @return KafkaConsumerMetrics对象，包含消费者特定的度量指标
     */
    KafkaConsumerMetrics kafkaConsumerMetrics();

    /**
     * 根据需要更新分配元数据
     * 检查并更新消费者的分区分配元数据
     *
     * @param timer 用于限制操作时间的计时器
     * @return 如果元数据需要更新并且更新成功则返回true，否则返回false
     */
    boolean updateAssignmentMetadataIfNeeded(final Timer timer);
}
