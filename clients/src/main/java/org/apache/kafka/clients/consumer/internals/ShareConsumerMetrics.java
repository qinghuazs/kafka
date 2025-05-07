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

import java.util.HashSet;
import java.util.Set;

/**
 * 共享消费者指标类
 * 用于管理和收集共享消费者的各种性能指标
 * 
 * 应用场景：
 * 1. 监控共享消费者的性能
 * 2. 收集消费者的获取指标
 * 3. 支持指标分组和标签管理
 * 4. 提供性能数据分析基础
 */
public class ShareConsumerMetrics {
    /**
     * 共享获取指标注册表
     * 用于记录和管理与消息获取相关的指标
     */
    public ShareFetchMetricsRegistry shareFetchMetrics;

    /**
     * 构造函数
     * 创建一个新的共享消费者指标实例，支持自定义标签和指标组前缀
     *
     * @param metricsTags 指标标签集合，用于标识和分类指标
     * @param metricGrpPrefix 指标组前缀，用于组织指标层次结构
     */
    public ShareConsumerMetrics(Set<String> metricsTags, String metricGrpPrefix) {
        // 创建新的共享获取指标注册表实例，传入标签集合和组前缀
        this.shareFetchMetrics = new ShareFetchMetricsRegistry(metricsTags, metricGrpPrefix);
    }

    /**
     * 简化构造函数
     * 使用空标签集合创建共享消费者指标实例
     *
     * @param metricGroupPrefix 指标组前缀
     */
    public ShareConsumerMetrics(String metricGroupPrefix) {
        // 调用主构造函数，使用空HashSet作为标签集合
        this(new HashSet<>(), metricGroupPrefix);
    }
}
