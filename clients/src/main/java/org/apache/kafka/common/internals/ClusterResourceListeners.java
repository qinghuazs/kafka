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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群资源监听器管理类，负责管理和通知Kafka集群资源的变更。
 * 该类维护了一个实现了ClusterResourceListener接口的监听器列表，
 * 当集群元数据发生变化时，会通知所有注册的监听器。
 * 
 * 应用场景：
 * 1. 在Kafka客户端中监控集群变更
 * 2. 实现自定义的集群资源监听逻辑
 * 3. 用于集群元数据更新时的回调通知
 */
public class ClusterResourceListeners {

    /**
     * 存储所有已注册的集群资源监听器
     * 使用ArrayList实现，支持动态添加监听器
     * 选择final修饰以确保线程安全性
     */
    private final List<ClusterResourceListener> clusterResourceListeners;

    /**
     * 构造函数
     * 初始化一个空的监听器列表
     */
    public ClusterResourceListeners() {
        this.clusterResourceListeners = new ArrayList<>();
    }

    /**
     * 尝试将对象添加到监听器列表中
     * 只有当对象实现了ClusterResourceListener接口时才会被添加
     * 
     * 实现细节：
     * 1. 使用instanceof检查对象是否实现了ClusterResourceListener接口
     * 2. 如果实现了接口，将对象转换为ClusterResourceListener类型并添加到列表
     * 3. 如果没有实现接口，则静默忽略
     * 
     * @param candidate 可能实现了ClusterResourceListener接口的候选对象
     */
    public void maybeAdd(Object candidate) {
        if (candidate instanceof ClusterResourceListener) {
            clusterResourceListeners.add((ClusterResourceListener) candidate);
        }
    }

    /**
     * 批量添加监听器
     * 遍历列表中的所有对象，将实现了ClusterResourceListener接口的对象添加到监听器列表
     * 
     * 实现细节：
     * 1. 接收一个泛型列表，支持任意类型的对象集合
     * 2. 遍历列表中的每个对象
     * 3. 通过maybeAdd方法尝试添加每个对象
     * 4. 不符合条件的对象会被静默忽略
     * 
     * @param candidateList 包含候选监听器对象的列表
     */
    public void maybeAddAll(List<?> candidateList) {
        for (Object candidate : candidateList) {
            this.maybeAdd(candidate);
        }
    }

    /**
     * 通知所有监听器集群元数据已更新
     * 当Kafka集群的元数据发生变化时，调用此方法通知所有注册的监听器
     * 
     * 实现细节：
     * 1. 遍历所有已注册的监听器
     * 2. 调用每个监听器的onUpdate方法
     * 3. 传递最新的集群元数据信息
     * 
     * 使用场景：
     * - 集群配置变更时
     * - Broker列表变化时
     * - 主题分区分配变更时
     * 
     * @param cluster 包含最新集群元数据的ClusterResource对象
     */
    public void onUpdate(ClusterResource cluster) {
        for (ClusterResourceListener clusterResourceListener : clusterResourceListeners) {
            clusterResourceListener.onUpdate(cluster);
        }
    }
}