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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 管理已发送但尚未收到响应的请求集合
 * 这个类负责跟踪和管理Kafka客户端发送到各个节点的所有在途请求
 * 它提供了线程安全的请求计数，以及按节点分组的请求队列管理
 */
final class InFlightRequests {

    // 每个连接允许的最大并发请求数
    private final int maxInFlightRequestsPerConnection;
    // 按节点ID存储请求队列的映射表，每个节点都有一个双端队列存储其在途请求
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    /** 所有节点在途请求的总数，使用原子计数器确保线程安全 */
    private final AtomicInteger inFlightRequestCount = new AtomicInteger(0);

    /**
     * 构造函数
     * @param maxInFlightRequestsPerConnection 限制每个网络连接的最大并发请求数，用于流量控制
     */
    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * 将新的请求添加到目标节点的请求队列中
     * @param request 要添加的请求对象
     */
    public void add(NetworkClient.InFlightRequest request) {
        // 获取请求的目标节点ID
        String destination = request.destination;
        // 获取或创建该节点的请求队列，如果不存在则创建新的双端队列
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.computeIfAbsent(destination, k -> new ArrayDeque<>());
        // 将请求添加到队列头部，表示这是最新的请求
        reqs.addFirst(request);
        // 原子递增总请求计数
        inFlightRequestCount.incrementAndGet();
    }

    /**
     * 获取指定节点的请求队列
     * @param node 节点ID
     * @return 该节点的请求队列
     * @throws IllegalStateException 如果该节点没有在途请求则抛出异常
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        // 获取节点对应的请求队列
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        // 如果队列不存在或为空，说明该节点当前没有在途请求，抛出异常
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * 获取并移除指定节点最老的请求（即最早发送的请求）
     * @param node 节点ID
     * @return 最早发送的请求对象
     */
    public NetworkClient.InFlightRequest completeNext(String node) {
        // 从队列尾部移除并返回最老的请求（因为新请求是加在队列头部的）
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollLast();
        // 原子递减总请求计数
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * 查看（但不移除）发送给指定节点的最新请求
     * @param node 节点ID
     * @return 最新发送的请求对象
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        // 查看队列头部的请求（最新发送的请求）但不移除
        return requestQueue(node).peekFirst();
    }

    /**
     * 完成并移除发送给指定节点的最新请求
     * @param node 目标节点ID
     * @return 被移除的最新请求对象
     */
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        // 从队列头部移除并返回最新的请求
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollFirst();
        // 原子递减总请求计数
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * 判断是否可以向指定节点发送更多请求
     * 满足以下条件之一时返回true：
     * 1. 节点没有请求队列
     * 2. 请求队列为空
     * 3. 最新请求已完成发送且未超过每连接最大请求数限制
     *
     * @param node 要判断的节点ID
     * @return 如果可以发送更多请求则返回true
     */
    public boolean canSendMore(String node) {
        // 获取节点的请求队列
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        // 检查是否满足可发送条件
        return queue == null || queue.isEmpty() ||
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * 获取指定节点当前的在途请求数量
     * @param node 节点ID
     * @return 该节点的在途请求数量，如果节点不存在则返回0
     */
    public int count(String node) {
        // 获取节点的请求队列并返回其大小，如果队列不存在则返回0
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * 检查指定节点是否没有在途请求
     * @param node 节点ID
     * @return 如果节点没有在途请求则返回true
     */
    public boolean isEmpty(String node) {
        // 获取节点的请求队列，如果队列不存在或为空则返回true
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * 获取所有节点的在途请求总数
     * 注意：由于使用原子计数器，该方法是线程安全的，但可能会有轻微的滞后
     * @return 所有节点的在途请求总数
     */
    public int count() {
        // 返回原子计数器的当前值
        return inFlightRequestCount.get();
    }

    /**
     * 检查是否所有节点都没有在途请求
     * @return 如果所有节点都没有在途请求则返回true
     */
    public boolean isEmpty() {
        // 遍历所有节点的请求队列
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values()) {
            // 只要有一个队列不为空，就返回false
            if (!deque.isEmpty())
                return false;
        }
        // 所有队列都为空时返回true
        return true;
    }

    /**
     * 清除指定节点的所有在途请求并返回这些请求
     * 通常在节点断开连接或发生错误时调用此方法
     *
     * @param node 要清除请求的节点ID
     * @return 被清除的所有请求的迭代器，如果节点不存在则返回空列表
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        // 获取节点的请求队列
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null) {
            // 如果队列不存在，返回空列表
            return Collections.emptyList();
        } else {
            // 移除该节点的整个请求队列
            final Deque<NetworkClient.InFlightRequest> clearedRequests = requests.remove(node);
            // 从总计数中减去清除的请求数量
            inFlightRequestCount.getAndAdd(-clearedRequests.size());
            // 返回被清除请求的降序迭代器
            return clearedRequests::descendingIterator;
        }
    }

    /**
     * 检查请求队列中是否存在已超时的请求
     * 超时判断会考虑节流时间，即只有在节流时间过后才开始计算超时
     *
     * @param now 当前时间戳（毫秒）
     * @param deque 要检查的请求队列
     * @return 如果存在已超时的请求则返回true
     */
    private Boolean hasExpiredRequest(long now, Deque<NetworkClient.InFlightRequest> deque) {
        // 遍历队列中的所有请求
        for (NetworkClient.InFlightRequest request : deque) {
            // 计算请求是否超时，需要排除节流时间的影响
            // 实际等待时间 = 已经过时间 - 节流时间
            if (request.timeElapsedSinceSendMs(now) - request.throttleTimeMs() > request.requestTimeoutMs)
                return true;
        }
        return false;
    }

    /**
     * 获取所有存在超时请求的节点列表
     * 用于定期检查和清理超时请求
     *
     * @param now 当前时间戳（毫秒）
     * @return 包含超时请求的节点ID列表
     */
    public List<String> nodesWithTimedOutRequests(long now) {
        // 创建存储结果的列表
        List<String> nodeIds = new ArrayList<>();
        // 遍历所有节点的请求队列
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> requestEntry : requests.entrySet()) {
            String nodeId = requestEntry.getKey();
            Deque<NetworkClient.InFlightRequest> deque = requestEntry.getValue();
            // 如果该节点有超时请求，将其添加到结果列表
            if (hasExpiredRequest(now, deque))
                nodeIds.add(nodeId);
        }
        return nodeIds;
    }

    /**
     * 增加指定节点所有在途请求的节流时间
     * 当服务器返回节流响应时调用此方法
     *
     * @param nodeId 节点ID
     * @param throttleTimeMs 要增加的节流时间（毫秒）
     */
    void incrementThrottleTime(String nodeId, long throttleTimeMs) {
        // 获取节点的请求队列，如果不存在则使用空队列
        requests.getOrDefault(nodeId, new ArrayDeque<>())
                // 为队列中的每个请求增加节流时间
                .forEach(request -> request.incrementThrottleTime(throttleTimeMs));
    }
}
