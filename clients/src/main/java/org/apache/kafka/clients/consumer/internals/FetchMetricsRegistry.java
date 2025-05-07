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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * FetchMetricsRegistry类负责管理和注册Kafka消费者的获取操作相关的度量指标
 * 包括客户端级别、主题级别和分区级别的各种度量指标
 */
public class FetchMetricsRegistry {

    /**
     * 已弃用的主题度量指标消息
     * 说明：对于包含句点(.)的主题名称，会额外发出一个使用下划线替换的度量指标
     * 但是这种替换句点的度量指标已被弃用，请使用实际主题名称的度量指标
     */
    private static final String DEPRECATED_TOPIC_METRICS_MESSAGE = "Note: For topic names with periods (.), an additional "
        + "metric with underscores is emitted. However, the periods replaced metric is deprecated. Please use the metric with actual topic name instead.";

    // 客户端级别的度量指标
    /** 每个请求获取的平均字节数 */
    public MetricNameTemplate fetchSizeAvg;
    /** 每个请求获取的最大字节数 */
    public MetricNameTemplate fetchSizeMax;
    /** 每秒消费的平均字节数 */
    public MetricNameTemplate bytesConsumedRate;
    /** 消费的总字节数 */
    public MetricNameTemplate bytesConsumedTotal;
    /** 每个请求的平均记录数 */
    public MetricNameTemplate recordsPerRequestAvg;
    /** 每秒消费的记录数 */
    public MetricNameTemplate recordsConsumedRate;
    /** 消费的总记录数 */
    public MetricNameTemplate recordsConsumedTotal;
    /** 获取请求的平均延迟 */
    public MetricNameTemplate fetchLatencyAvg;
    /** 获取请求的最大延迟 */
    public MetricNameTemplate fetchLatencyMax;
    /** 每秒获取请求数 */
    public MetricNameTemplate fetchRequestRate;
    /** 获取请求总数 */
    public MetricNameTemplate fetchRequestTotal;
    /** 任意分区在当前窗口的最大记录延迟数 */
    public MetricNameTemplate recordsLagMax;
    /** 任意分区在当前窗口的最小记录领先数 */
    public MetricNameTemplate recordsLeadMin;
    /** 平均限流时间(毫秒) */
    public MetricNameTemplate fetchThrottleTimeAvg;
    /** 最大限流时间(毫秒) */
    public MetricNameTemplate fetchThrottleTimeMax;

    // 主题级别的度量指标
    /** 每个主题每个请求获取的平均字节数 */
    public MetricNameTemplate topicFetchSizeAvg;
    /** 每个主题每个请求获取的最大字节数 */
    public MetricNameTemplate topicFetchSizeMax;
    /** 每个主题每秒消费的平均字节数 */
    public MetricNameTemplate topicBytesConsumedRate;
    /** 每个主题消费的总字节数 */
    public MetricNameTemplate topicBytesConsumedTotal;
    /** 每个主题每个请求的平均记录数 */
    public MetricNameTemplate topicRecordsPerRequestAvg;
    /** 每个主题每秒消费的记录数 */
    public MetricNameTemplate topicRecordsConsumedRate;
    /** 每个主题消费的总记录数 */
    public MetricNameTemplate topicRecordsConsumedTotal;

    // 分区级别的度量指标
    /** 分区的最新记录延迟数 */
    public MetricNameTemplate partitionRecordsLag;
    /** 分区的最大记录延迟数 */
    public MetricNameTemplate partitionRecordsLagMax;
    /** 分区的平均记录延迟数 */
    public MetricNameTemplate partitionRecordsLagAvg;
    /** 分区的最新记录领先数 */
    public MetricNameTemplate partitionRecordsLead;
    /** 分区的最小记录领先数 */
    public MetricNameTemplate partitionRecordsLeadMin;
    /** 分区的平均记录领先数 */
    public MetricNameTemplate partitionRecordsLeadAvg;
    /** 分区的首选读取副本 */
    public MetricNameTemplate partitionPreferredReadReplica;

    /**
     * 默认构造函数
     * 使用空标签集和空前缀初始化注册表
     */
    public FetchMetricsRegistry() {
        // 调用带参构造函数，传入空HashSet和空字符串
        this(new HashSet<>(), "");
    }

    /**
     * 使用指定度量组前缀的构造函数
     *
     * @param metricGrpPrefix 度量组前缀
     */
    public FetchMetricsRegistry(String metricGrpPrefix) {
        // 调用带参构造函数，传入空HashSet和指定的度量组前缀
        this(new HashSet<>(), metricGrpPrefix);
    }

    /**
     * 主构造函数，使用指定的标签集和度量组前缀初始化所有度量指标
     *
     * @param tags 度量指标标签集
     * @param metricGrpPrefix 度量组前缀
     */
    public FetchMetricsRegistry(Set<String> tags, String metricGrpPrefix) {
        // 构造度量组名称
        String groupName = metricGrpPrefix + "-fetch-manager-metrics";

        // 初始化客户端级别的度量指标
        this.fetchSizeAvg = new MetricNameTemplate("fetch-size-avg", groupName,
                "每个请求获取的平均字节数", tags);
        this.fetchSizeMax = new MetricNameTemplate("fetch-size-max", groupName,
                "The maximum number of bytes fetched per request", tags);
        this.bytesConsumedRate = new MetricNameTemplate("bytes-consumed-rate", groupName,
                "The average number of bytes consumed per second", tags);
        this.bytesConsumedTotal = new MetricNameTemplate("bytes-consumed-total", groupName,
                "The total number of bytes consumed", tags);

        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", groupName,
                "The average number of records in each request", tags);
        this.recordsConsumedRate = new MetricNameTemplate("records-consumed-rate", groupName,
                "The average number of records consumed per second", tags);
        this.recordsConsumedTotal = new MetricNameTemplate("records-consumed-total", groupName,
                "The total number of records consumed", tags);

        this.fetchLatencyAvg = new MetricNameTemplate("fetch-latency-avg", groupName,
                "The average time taken for a fetch request.", tags);
        this.fetchLatencyMax = new MetricNameTemplate("fetch-latency-max", groupName,
                "The max time taken for any fetch request.", tags);
        this.fetchRequestRate = new MetricNameTemplate("fetch-rate", groupName,
                "The number of fetch requests per second.", tags);
        this.fetchRequestTotal = new MetricNameTemplate("fetch-total", groupName,
                "The total number of fetch requests.", tags);

        this.recordsLagMax = new MetricNameTemplate("records-lag-max", groupName,
                "The maximum lag in terms of number of records for any partition in this window. NOTE: This is based on current offset and not committed offset", tags);
        this.recordsLeadMin = new MetricNameTemplate("records-lead-min", groupName,
                "The minimum lead in terms of number of records for any partition in this window", tags);

        this.fetchThrottleTimeAvg = new MetricNameTemplate("fetch-throttle-time-avg", groupName,
                "The average throttle time in ms", tags);
        this.fetchThrottleTimeMax = new MetricNameTemplate("fetch-throttle-time-max", groupName,
                "The maximum throttle time in ms", tags);

        /*  Topic level */
        Set<String> topicTags = new LinkedHashSet<>(tags);
        topicTags.add("topic");  // 添加主题标签

        this.topicFetchSizeAvg = new MetricNameTemplate("fetch-size-avg", groupName,
                "The average number of bytes fetched per request for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);
        this.topicFetchSizeMax = new MetricNameTemplate("fetch-size-max", groupName,
                "The maximum number of bytes fetched per request for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);
        this.topicBytesConsumedRate = new MetricNameTemplate("bytes-consumed-rate", groupName,
                "The average number of bytes consumed per second for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);
        this.topicBytesConsumedTotal = new MetricNameTemplate("bytes-consumed-total", groupName,
                "The total number of bytes consumed for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);

        this.topicRecordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", groupName,
                "The average number of records in each request for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);
        this.topicRecordsConsumedRate = new MetricNameTemplate("records-consumed-rate", groupName,
                "The average number of records consumed per second for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);
        this.topicRecordsConsumedTotal = new MetricNameTemplate("records-consumed-total", groupName,
                "The total number of records consumed for a topic. " + DEPRECATED_TOPIC_METRICS_MESSAGE, topicTags);

        /* Partition level */
        Set<String> partitionTags = new HashSet<>(topicTags);
        partitionTags.add("partition");  // 添加分区标签

        this.partitionRecordsLag = new MetricNameTemplate("records-lag", groupName,
                "The latest lag of the partition. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
        this.partitionRecordsLagMax = new MetricNameTemplate("records-lag-max", groupName,
                "The max lag of the partition. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
        this.partitionRecordsLagAvg = new MetricNameTemplate("records-lag-avg", groupName,
                "The average lag of the partition. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
        this.partitionRecordsLead = new MetricNameTemplate("records-lead", groupName,
                "The latest lead of the partition. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
        this.partitionRecordsLeadMin = new MetricNameTemplate("records-lead-min", groupName,
                "The min lead of the partition. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
        this.partitionRecordsLeadAvg = new MetricNameTemplate("records-lead-avg", groupName,
                "The average lead of the partition. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
        this.partitionPreferredReadReplica = new MetricNameTemplate(
                "preferred-read-replica", groupName,
                "The current read replica for the partition, or -1 if reading from leader. " + DEPRECATED_TOPIC_METRICS_MESSAGE, partitionTags);
    }

    /**
     * 获取所有度量指标模板的列表
     *
     * @return 包含所有注册的度量指标模板的列表
     */
    public List<MetricNameTemplate> getAllTemplates() {
        // 返回包含所有度量指标模板的列表
        return Arrays.asList(
            fetchSizeAvg,
            fetchSizeMax,
            bytesConsumedRate,
            bytesConsumedTotal,
            recordsPerRequestAvg,
            recordsConsumedRate,
            recordsConsumedTotal,
            fetchLatencyAvg,
            fetchLatencyMax,
            fetchRequestRate,
            fetchRequestTotal,
            recordsLagMax,
            recordsLeadMin,
            fetchThrottleTimeAvg,
            fetchThrottleTimeMax,
            topicFetchSizeAvg,
            topicFetchSizeMax,
            topicBytesConsumedRate,
            topicBytesConsumedTotal,
            topicRecordsPerRequestAvg,
            topicRecordsConsumedRate,
            topicRecordsConsumedTotal,
            partitionRecordsLag,
            partitionRecordsLagAvg,
            partitionRecordsLagMax,
            partitionRecordsLead,
            partitionRecordsLeadMin,
            partitionRecordsLeadAvg,
            partitionPreferredReadReplica
        );
    }
}
