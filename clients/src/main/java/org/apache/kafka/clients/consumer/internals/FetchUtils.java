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

import org.apache.kafka.common.TopicPartition;

/**
 * FetchUtils类为分散的获取逻辑提供了一个统一的存放位置。
 * 该类包含了与获取操作相关的工具方法，用于处理元数据更新和副本管理。
 */
public class FetchUtils {

    /**
     * 基于主题分区的状态执行两个组合操作：
     *
     * <ol>
     *     <li>
     *         调用{@link ConsumerMetadata#requestUpdate(boolean)}来通知元数据不正确
     *         并需要更新
     *     </li>
     *     <li>
     *         调用{@link SubscriptionState#clearPreferredReadReplica(TopicPartition)}
     *         来清除可能存在的任何读取副本信息
     *     </li>
     * </ol>
     *
     * 当客户端检测到（或被代理节点告知）尝试从非领导者或非首选副本的节点获取数据时，
     * 应该调用此工具方法。
     *
     * @param metadata 需要请求更新的消费者元数据
     * @param subscriptions 需要清除内部读取副本节点的订阅状态
     * @param topicPartition 与此状态更改相关的主题分区
     */
    static void requestMetadataUpdate(final ConsumerMetadata metadata,
                                      final SubscriptionState subscriptions,
                                      final TopicPartition topicPartition) {
        // 请求更新元数据，参数false表示这不是强制更新
        metadata.requestUpdate(false);
        // 清除指定主题分区的首选读取副本信息
        subscriptions.clearPreferredReadReplica(topicPartition);
    }
}
