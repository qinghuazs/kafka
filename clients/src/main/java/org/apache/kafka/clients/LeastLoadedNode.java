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
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;

/**
 * 表示Kafka集群中负载最小的节点。
 * 这个类用于负载均衡，帮助客户端选择最适合处理新请求的broker节点。
 * 它不仅跟踪节点本身，还维护了连接状态信息。
 */
public class LeastLoadedNode {
    /**
     * 负载最小的节点实例。
     * 可能为null，这表示当前没有可用的节点（例如所有节点都过载）。
     */
    private final Node node;

    /**
     * 标记是否至少有一个到活跃节点的连接处于就绪状态。
     * 即使node为null，只要有就绪连接，此值也可能为true。
     */
    private final boolean atLeastOneConnectionReady;

    /**
     * 创建一个LeastLoadedNode实例。
     *
     * @param node 选中的负载最小节点，可能为null
     * @param atLeastOneConnectionReady 是否存在至少一个就绪连接的标记
     */
    public LeastLoadedNode(Node node, boolean atLeastOneConnectionReady) {
        // 保存节点引用，可以为null
        this.node = node;
        // 记录连接就绪状态
        this.atLeastOneConnectionReady = atLeastOneConnectionReady;
    }

    /**
     * 获取负载最小的节点实例。
     *
     * @return 返回负载最小的节点，如果没有可用节点则返回null
     */
    public Node node() {
        // 直接返回内部节点引用
        return node;
    }

    /**
     * 检查是否有可用节点或至少存在一个就绪连接。
     * 
     * 这个方法在以下两种情况下返回true：
     * 1. 存在一个可用的负载最小节点（node不为null）
     * 2. 虽然没有可用节点，但至少有一个到活跃节点的连接处于就绪状态
     *
     * 第二种情况通常发生在所有节点都有大量在途请求（过载）时，
     * 此时虽然没有最佳节点选择，但系统仍然可以工作。
     *
     * @return 如果有可用节点或存在就绪连接则返回true，否则返回false
     */
    public boolean hasNodeAvailableOrConnectionReady() {
        // 只要node不为null或存在就绪连接，就返回true
        return node != null || atLeastOneConnectionReady;
    }
}
