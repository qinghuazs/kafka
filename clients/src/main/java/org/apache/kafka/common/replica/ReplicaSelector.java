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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * 可插拔的副本选择器接口，用于为客户端选择最优的读取副本。
 * 
 * 该接口允许实现自定义的副本选择策略，通过考虑以下因素来做出选择：
 * 1. 分区当前的副本集合状态
 * 2. 客户端的元数据（如机架位置、网络地址等）
 * 3. 副本的同步状态和性能指标
 * 
 * 实现此接口的选择器可以通过配置进行自定义，并且在不再需要时可以优雅关闭。
 */
public interface ReplicaSelector extends Configurable, Closeable {

    /**
     * 为客户端选择最优的读取副本
     * 
     * @param topicPartition 目标主题分区
     * @param clientMetadata 客户端元数据，包含客户端的位置信息等
     * @param partitionView 分区的当前视图，包含所有副本的状态信息
     * @return 如果找到合适的副本则返回其视图，否则返回空Optional
     */
    Optional<ReplicaView> select(TopicPartition topicPartition,
                                 ClientMetadata clientMetadata,
                                 PartitionView partitionView);
    @Override
    default void close() throws IOException {
        // No-op by default
    }

    @Override
    default void configure(Map<String, ?> configs) {
        // No-op by default
    }
}
