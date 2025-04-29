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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * Kafka管理客户端的指标管理器，用于管理和记录与Admin客户端相关的请求延迟指标。
 * 该类负责跟踪和记录针对特定Kafka节点的请求延迟时间，帮助监控和分析Admin客户端的性能表现。
 */
public class AdminFetchMetricsManager {
    /**
     * Metrics实例，用于创建和管理性能指标。
     * 这个字段存储了所有与Admin客户端相关的度量指标，包括各个节点的请求延迟传感器。
     */
    private final Metrics metrics;

    /**
     * 构造函数，初始化AdminFetchMetricsManager实例。
     *
     * @param metrics Metrics实例，用于管理性能指标
     */
    public AdminFetchMetricsManager(Metrics metrics) {
        this.metrics = metrics;
    }

    /**
     * 记录特定节点的请求延迟时间。
     * 该方法用于跟踪和记录Admin客户端对特定Kafka节点的请求延迟，有助于监控网络性能和节点响应时间。
     *
     * @param node 目标Kafka节点的标识符
     * @param requestLatencyMs 请求延迟时间（毫秒）
     */
    public void recordLatency(String node, long requestLatencyMs) {
        // 只有当节点标识符非空时才记录延迟
        if (!node.isEmpty()) {
            // 构造节点延迟指标的名称，格式为："node-{节点标识符}.latency"
            String nodeTimeName = "node-" + node + ".latency";
            // 获取该节点对应的延迟指标传感器
            Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
            // 如果传感器存在，则记录延迟时间
            if (nodeRequestTime != null)
                nodeRequestTime.record(requestLatencyMs);
        }
    }
}
