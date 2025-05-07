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

import java.util.HashSet;
import java.util.Set;

/**
 * 共享获取指标注册表类
 * 用于管理和注册Kafka共享消费者的获取操作相关的性能指标模板
 * 
 * 应用场景：
 * 1. 注册和管理获取操作的性能指标
 * 2. 提供指标命名和描述模板
 * 3. 支持指标分组和标签管理
 * 4. 监控消费者性能数据
 */
public class ShareFetchMetricsRegistry {
    /**
     * 每个请求获取字节数的平均值指标模板
     */
    public MetricNameTemplate fetchSizeAvg;
    
    /**
     * 每个请求获取字节数的最大值指标模板
     */
    public MetricNameTemplate fetchSizeMax;
    
    /**
     * 每秒获取字节数的速率指标模板
     */
    public MetricNameTemplate bytesFetchedRate;
    
    /**
     * 获取字节数的总量指标模板
     */
    public MetricNameTemplate bytesFetchedTotal;
    
    /**
     * 每个请求获取记录数的平均值指标模板
     */
    public MetricNameTemplate recordsPerRequestAvg;
    
    /**
     * 每个请求获取记录数的最大值指标模板
     */
    public MetricNameTemplate recordsPerRequestMax;
    
    /**
     * 每秒获取记录数的速率指标模板
     */
    public MetricNameTemplate recordsFetchedRate;
    
    /**
     * 获取记录数的总量指标模板
     */
    public MetricNameTemplate recordsFetchedTotal;
    
    /**
     * 每秒发送确认的速率指标模板
     */
    public MetricNameTemplate acknowledgementSendRate;
    
    /**
     * 发送确认的总量指标模板
     */
    public MetricNameTemplate acknowledgementSendTotal;
    
    /**
     * 每秒确认错误的速率指标模板
     */
    public MetricNameTemplate acknowledgementErrorRate;
    
    /**
     * 确认错误的总量指标模板
     */
    public MetricNameTemplate acknowledgementErrorTotal;
    
    /**
     * 获取请求延迟的平均值指标模板
     */
    public MetricNameTemplate fetchLatencyAvg;
    
    /**
     * 获取请求延迟的最大值指标模板
     */
    public MetricNameTemplate fetchLatencyMax;
    
    /**
     * 每秒获取请求的速率指标模板
     */
    public MetricNameTemplate fetchRequestRate;
    
    /**
     * 获取请求的总量指标模板
     */
    public MetricNameTemplate fetchRequestTotal;
    
    /**
     * 获取限流时间的平均值指标模板
     */
    public MetricNameTemplate fetchThrottleTimeAvg;
    
    /**
     * 获取限流时间的最大值指标模板
     */
    public MetricNameTemplate fetchThrottleTimeMax;

    /**
     * 默认构造函数
     * 使用空标签集合和空前缀创建注册表
     */
    public ShareFetchMetricsRegistry() {
        // 调用完整构造函数，使用空HashSet和空字符串作为参数
        this(new HashSet<>(), "");
    }

    /**
     * 带指标组前缀的构造函数
     * 
     * @param metricGrpPrefix 指标组前缀，用于组织指标的层次结构
     */
    public ShareFetchMetricsRegistry(String metricGrpPrefix) {
        // 调用完整构造函数，使用空HashSet和指定的前缀
        this(new HashSet<>(), metricGrpPrefix);
    }

    /**
     * 完整构造函数
     * 使用指定的标签集合和组前缀创建注册表
     *
     * @param tags 指标标签集合，用于标识和分类指标
     * @param metricGrpPrefix 指标组前缀，用于组织指标的层次结构
     */
    public ShareFetchMetricsRegistry(Set<String> tags, String metricGrpPrefix) {
        // 构造客户端级别的指标组名称
        String groupName = metricGrpPrefix + "-fetch-manager-metrics";

        // 初始化获取大小相关的指标模板
        this.fetchSizeAvg = new MetricNameTemplate("fetch-size-avg", groupName,
                "每个请求获取的字节数平均值", tags);
        this.fetchSizeMax = new MetricNameTemplate("fetch-size-max", groupName,
                "每个请求获取的字节数最大值", tags);
        
        // 初始化字节消费相关的指标模板
        this.bytesFetchedRate = new MetricNameTemplate("bytes-consumed-rate", groupName,
                "每秒消费的字节数平均值", tags);
        this.bytesFetchedTotal = new MetricNameTemplate("bytes-consumed-total", groupName,
                "消费的总字节数", tags);

        // 初始化记录数相关的指标模板
        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", groupName,
                "每个请求的记录数平均值", tags);
        this.recordsPerRequestMax = new MetricNameTemplate("records-per-request-max", groupName,
                "每个请求的记录数最大值", tags);
        this.recordsFetchedRate = new MetricNameTemplate("records-consumed-rate", groupName,
                "每秒消费的记录数平均值", tags);
        this.recordsFetchedTotal = new MetricNameTemplate("records-consumed-total", groupName,
                "消费的总记录数", tags);

        // 初始化确认相关的指标模板
        this.acknowledgementSendRate = new MetricNameTemplate("acknowledgements-send-rate", groupName,
                "每秒发送的确认数平均值", tags);
        this.acknowledgementSendTotal = new MetricNameTemplate("acknowledgements-send-total", groupName,
                "发送的总确认数", tags);
        this.acknowledgementErrorRate = new MetricNameTemplate("acknowledgements-error-rate", groupName,
                "每秒发生错误的确认数平均值", tags);
        this.acknowledgementErrorTotal = new MetricNameTemplate("acknowledgements-error-total", groupName,
                "发生错误的总确认数", tags);

        // 初始化延迟相关的指标模板
        this.fetchLatencyAvg = new MetricNameTemplate("fetch-latency-avg", groupName,
                "获取请求的平均延迟时间", tags);
        this.fetchLatencyMax = new MetricNameTemplate("fetch-latency-max", groupName,
                "获取请求的最大延迟时间", tags);
        this.fetchRequestRate = new MetricNameTemplate("fetch-rate", groupName,
                "每秒获取请求数", tags);
        this.fetchRequestTotal = new MetricNameTemplate("fetch-total", groupName,
                "获取请求总数", tags);

        // 初始化限流相关的指标模板
        this.fetchThrottleTimeAvg = new MetricNameTemplate("fetch-throttle-time-avg", groupName,
                "限流时间的平均值（毫秒）", tags);
        this.fetchThrottleTimeMax = new MetricNameTemplate("fetch-throttle-time-max", groupName,
                "限流时间的最大值（毫秒）", tags);
    }
}
