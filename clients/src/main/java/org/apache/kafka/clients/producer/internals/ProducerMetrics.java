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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Kafka生产者的指标监控类
 * 
 * 该类负责管理和收集Kafka生产者的各项性能指标，主要包括：
 * 1. 消息发送相关的指标（如发送速率、延迟等）
 * 2. 批次处理相关的指标（如批次大小、压缩比等）
 * 3. 缓冲区使用情况的指标
 * 4. 请求和响应相关的指标
 * 
 * 通过这些指标，用户可以监控和分析生产者的性能表现，及时发现潜在问题
 */
public class ProducerMetrics {

    /**
     * 发送者指标注册表，用于注册和管理所有与消息发送相关的指标
     * 包含了如消息发送速率、延迟、重试次数等关键指标
     */
    public final SenderMetricsRegistry senderMetrics;

    /**
     * 构造函数，初始化生产者指标监控系统
     * 
     * @param metrics Kafka指标系统的核心组件，用于创建和管理各类指标
     */
    public ProducerMetrics(Metrics metrics) {
        this.senderMetrics = new SenderMetricsRegistry(metrics);
    }

    /**
     * 获取所有已注册的指标模板
     * 
     * @return 返回所有指标模板的列表，这些模板定义了指标的名称、标签和描述等信息
     */
    private List<MetricNameTemplate> getAllTemplates() {
        return new ArrayList<>(this.senderMetrics.allTemplates());
    }

    /**
     * 主方法，用于演示如何初始化生产者指标系统并生成HTML格式的指标报告
     * 
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        Map<String, String> metricTags = Collections.singletonMap("client-id", "client-id");
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        Metrics metrics = new Metrics(metricConfig);

        ProducerMetrics metricsRegistry = new ProducerMetrics(metrics);
        System.out.println(Metrics.toHtmlTable("kafka.producer", metricsRegistry.getAllTemplates()));
    }

}
