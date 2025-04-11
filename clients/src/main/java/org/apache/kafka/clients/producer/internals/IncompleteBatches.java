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
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 一个线程安全的辅助类，用于管理尚未收到确认的消息批次（包括已发送和未发送的批次）。
 * 该类通过synchronized关键字实现线程安全，所有对incomplete集合的操作都在同步块中进行。
 * 主要用于Kafka生产者内部追踪消息批次的发送状态和结果。
 */
class IncompleteBatches {
    // 使用Set存储未完成的ProducerBatch对象，确保每个批次只被存储一次
    private final Set<ProducerBatch> incomplete;

    /**
     * 构造函数：初始化一个空的HashSet来存储未完成的消息批次
     */
    public IncompleteBatches() {
        this.incomplete = new HashSet<>();
    }

    /**
     * 添加一个新的消息批次到未完成集合中
     * @param batch 要添加的消息批次
     */
    public void add(ProducerBatch batch) {
        synchronized (incomplete) {  // 使用synchronized确保线程安全
            this.incomplete.add(batch);
        }
    }

    /**
     * 从未完成集合中移除一个已确认的消息批次
     * @param batch 要移除的消息批次
     * @throws IllegalStateException 如果要移除的批次不存在于集合中
     */
    public void remove(ProducerBatch batch) {
        synchronized (incomplete) {  // 使用synchronized确保线程安全
            boolean removed = this.incomplete.remove(batch);
            if (!removed)
                throw new IllegalStateException("从未完成集合中移除批次失败，这种情况不应该发生。");
        }
    }

    /**
     * 获取所有未完成批次的副本
     * @return 返回一个包含所有未完成批次的新ArrayList
     */
    public Iterable<ProducerBatch> copyAll() {
        synchronized (incomplete) {  // 使用synchronized确保线程安全
            return new ArrayList<>(this.incomplete);  // 返回副本以避免并发修改问题
        }
    }

    /**
     * 获取所有未完成批次的请求结果
     * @return 返回一个包含所有批次的ProduceRequestResult的列表
     */
    public Iterable<ProduceRequestResult> requestResults() {
        synchronized (incomplete) {  // 使用synchronized确保线程安全
            return incomplete.stream().map(batch -> batch.produceFuture).collect(Collectors.toList());
        }
    }

    /**
     * 检查是否存在未完成的批次
     * @return 如果没有未完成的批次返回true，否则返回false
     */
    public boolean isEmpty() {
        synchronized (incomplete) {  // 使用synchronized确保线程安全
            return incomplete.isEmpty();
        }
    }
}
