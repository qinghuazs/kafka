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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 消费者度量指标类
 * 用于管理和收集Kafka消费者的各种度量指标，特别是获取操作相关的指标
 */
public class ConsumerMetrics {
    
    /**
     * 获取操作的度量指标注册表
     * 存储和管理与数据获取相关的所有度量指标
     */
    public FetchMetricsRegistry fetcherMetrics;
    
    /**
     * 构造函数
     * 使用指定的度量标签和度量组前缀初始化消费者度量指标
     *
     * @param metricsTags 度量标签集合，用于标识和分类度量指标
     * @param metricGrpPrefix 度量组前缀，用于组织度量指标的层次结构
     */
    public ConsumerMetrics(Set<String> metricsTags, String metricGrpPrefix) {
        // 创建新的获取度量指标注册表实例
        this.fetcherMetrics = new FetchMetricsRegistry(metricsTags, metricGrpPrefix);
    }

    /**
     * 简化版构造函数
     * 使用空的度量标签集合和指定的度量组前缀初始化消费者度量指标
     *
     * @param metricGroupPrefix 度量组前缀
     */
    public ConsumerMetrics(String metricGroupPrefix) {
        // 调用主构造函数，传入空的标签集合
        this(new HashSet<>(), metricGroupPrefix);
    }

    /**
     * 获取所有度量指标模板
     * 返回获取操作度量指标注册表中的所有度量指标模板
     *
     * @return 度量指标模板列表
     */
    private List<MetricNameTemplate> getAllTemplates() {
        // 返回获取度量指标注册表中所有模板的副本
        return new ArrayList<>(this.fetcherMetrics.getAllTemplates());
    }

    /**
     * 主方法
     * 用于演示和测试消费者度量指标的HTML表格输出
     *
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        // 创建度量标签集合
        Set<String> tags = new HashSet<>();
        // 添加客户端ID标签
        tags.add("client-id");
        // 创建消费者度量指标实例
        ConsumerMetrics metrics = new ConsumerMetrics(tags, "consumer");
        // 将度量指标转换为HTML表格并打印
        System.out.println(Metrics.toHtmlTable("kafka.consumer", metrics.getAllTemplates()));
    }
}
