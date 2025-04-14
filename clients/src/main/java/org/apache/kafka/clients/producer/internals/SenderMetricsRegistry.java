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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka生产者的指标注册表类，负责管理和收集生产者的各种性能指标。
 * 该类维护两个层次的指标：
 * 1. 客户端级别指标：记录整个生产者客户端的性能数据
 * 2. 主题级别指标：记录每个主题的具体性能数据
 */
public class SenderMetricsRegistry {

    // 主题级别指标的组名
    static final String TOPIC_METRIC_GROUP_NAME = "producer-topic-metrics";

    // 存储所有指标模板的列表
    private final List<MetricNameTemplate> allTemplates;

    // 客户端级别指标 - 每个分区每次请求发送的平均字节数
    public final MetricName batchSizeAvg;
    // 客户端级别指标 - 每个分区每次请求发送的最大字节数
    public final MetricName batchSizeMax;
    // 客户端级别指标 - 记录批次的平均压缩率（压缩后大小/压缩前大小的比率）
    public final MetricName compressionRateAvg;
    // 客户端级别指标 - 记录批次在发送缓冲区中的平均停留时间（毫秒）
    public final MetricName recordQueueTimeAvg;
    // 客户端级别指标 - 记录批次在发送缓冲区中的最大停留时间（毫秒）
    public final MetricName recordQueueTimeMax;
    // 客户端级别指标 - 请求的平均延迟时间（毫秒）
    public final MetricName requestLatencyAvg;
    // 客户端级别指标 - 请求的最大延迟时间（毫秒）
    public final MetricName requestLatencyMax;   
    // 客户端级别指标 - 请求被broker限流的平均时间（毫秒）
    public final MetricName produceThrottleTimeAvg;
    // 客户端级别指标 - 请求被broker限流的最大时间（毫秒）
    public final MetricName produceThrottleTimeMax;
    // 客户端级别指标 - 每秒发送的平均记录数
    public final MetricName recordSendRate;
    // 客户端级别指标 - 发送的总记录数
    public final MetricName recordSendTotal;
    // 客户端级别指标 - 每个请求的平均记录数
    public final MetricName recordsPerRequestAvg;
    // 客户端级别指标 - 每秒重试发送的平均记录数
    public final MetricName recordRetryRate;
    // 客户端级别指标 - 重试发送的总记录数
    public final MetricName recordRetryTotal;
    // 客户端级别指标 - 每秒发送失败的平均记录数
    public final MetricName recordErrorRate;
    // 客户端级别指标 - 发送失败的总记录数
    public final MetricName recordErrorTotal;
    // 客户端级别指标 - 记录的最大大小
    public final MetricName recordSizeMax;
    // 客户端级别指标 - 记录的平均大小
    public final MetricName recordSizeAvg;
    // 客户端级别指标 - 当前正在等待响应的请求数
    public final MetricName requestsInFlight;
    // 客户端级别指标 - 当前使用的生产者元数据的年龄（秒）
    public final MetricName metadataAge;
    // 客户端级别指标 - 每秒批次分裂的平均次数
    public final MetricName batchSplitRate;
    // 客户端级别指标 - 批次分裂的总次数
    public final MetricName batchSplitTotal;

    // 主题级别指标 - 每个主题每秒发送的平均记录数
    private final MetricNameTemplate topicRecordSendRate;
    // 主题级别指标 - 每个主题发送的总记录数
    private final MetricNameTemplate topicRecordSendTotal;
    // 主题级别指标 - 每个主题每秒发送的平均字节数
    private final MetricNameTemplate topicByteRate;
    // 主题级别指标 - 每个主题发送的总字节数
    private final MetricNameTemplate topicByteTotal;
    // 主题级别指标 - 每个主题的记录批次平均压缩率
    private final MetricNameTemplate topicCompressionRate;
    // 主题级别指标 - 每个主题每秒重试发送的平均记录数
    private final MetricNameTemplate topicRecordRetryRate;
    // 主题级别指标 - 每个主题重试发送的总记录数
    private final MetricNameTemplate topicRecordRetryTotal;
    // 主题级别指标 - 每个主题每秒发送失败的平均记录数
    private final MetricNameTemplate topicRecordErrorRate;
    // 主题级别指标 - 每个主题发送失败的总记录数
    private final MetricNameTemplate topicRecordErrorTotal;
    
    // 指标管理器实例
    private final Metrics metrics;
    // 客户端级别指标的标签集合
    private final Set<String> tags;
    // 主题级别指标的标签集合（包含topic标签）
    private final LinkedHashSet<String> topicTags;

    /**
     * 构造函数，初始化生产者的指标注册表
     * @param metrics 指标管理器实例
     */
    public SenderMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<>();
        
        /* Client level */
        
        this.batchSizeAvg = createMetricName("batch-size-avg",
                "The average number of bytes sent per partition per-request.");
        this.batchSizeMax = createMetricName("batch-size-max",
                "The max number of bytes sent per partition per-request.");
        this.compressionRateAvg = createMetricName("compression-rate-avg",
                "The average compression rate of record batches, defined as the average ratio of the " +
                        "compressed batch size over the uncompressed size.");
        this.recordQueueTimeAvg = createMetricName("record-queue-time-avg",
                "The average time in ms record batches spent in the send buffer.");
        this.recordQueueTimeMax = createMetricName("record-queue-time-max",
                "The maximum time in ms record batches spent in the send buffer.");
        this.requestLatencyAvg = createMetricName("request-latency-avg", 
                "The average request latency in ms");
        this.requestLatencyMax = createMetricName("request-latency-max", 
                "The maximum request latency in ms");
        this.recordSendRate = createMetricName("record-send-rate", 
                "The average number of records sent per second.");
        this.recordSendTotal = createMetricName("record-send-total", 
                "The total number of records sent.");
        this.recordsPerRequestAvg = createMetricName("records-per-request-avg",
                "The average number of records per request.");
        this.recordRetryRate = createMetricName("record-retry-rate",
                "The average per-second number of retried record sends");
        this.recordRetryTotal = createMetricName("record-retry-total", 
                "The total number of retried record sends");
        this.recordErrorRate = createMetricName("record-error-rate",
                "The average per-second number of record sends that resulted in errors");
        this.recordErrorTotal = createMetricName("record-error-total",
                "The total number of record sends that resulted in errors");
        this.recordSizeMax = createMetricName("record-size-max", 
                "The maximum record size");
        this.recordSizeAvg = createMetricName("record-size-avg", 
                "The average record size");
        this.requestsInFlight = createMetricName("requests-in-flight",
                "The current number of in-flight requests awaiting a response.");
        this.metadataAge = createMetricName("metadata-age",
                "The age in seconds of the current producer metadata being used.");
        this.batchSplitRate = createMetricName("batch-split-rate", 
                "The average number of batch splits per second");
        this.batchSplitTotal = createMetricName("batch-split-total", 
                "The total number of batch splits");

        this.produceThrottleTimeAvg = createMetricName("produce-throttle-time-avg",
                "The average time in ms a request was throttled by a broker");
        this.produceThrottleTimeMax = createMetricName("produce-throttle-time-max",
                "The maximum time in ms a request was throttled by a broker");

        /* Topic level */
        this.topicTags = new LinkedHashSet<>(tags);
        this.topicTags.add("topic");

        // We can't create the MetricName up front for these, because we don't know the topic name yet.
        this.topicRecordSendRate = createTopicTemplate("record-send-rate",
                "The average number of records sent per second for a topic.");
        this.topicRecordSendTotal = createTopicTemplate("record-send-total",
                "The total number of records sent for a topic.");
        this.topicByteRate = createTopicTemplate("byte-rate",
                "The average number of bytes sent per second for a topic.");
        this.topicByteTotal = createTopicTemplate("byte-total", 
                "The total number of bytes sent for a topic.");
        this.topicCompressionRate = createTopicTemplate("compression-rate",
                "The average compression rate of record batches for a topic, defined as the average ratio " +
                        "of the compressed batch size over the uncompressed size.");
        this.topicRecordRetryRate = createTopicTemplate("record-retry-rate",
                "The average per-second number of retried record sends for a topic");
        this.topicRecordRetryTotal = createTopicTemplate("record-retry-total",
                "The total number of retried record sends for a topic");
        this.topicRecordErrorRate = createTopicTemplate("record-error-rate",
                "The average per-second number of record sends that resulted in errors for a topic");
        this.topicRecordErrorTotal = createTopicTemplate("record-error-total",
                "The total number of record sends that resulted in errors for a topic");

    }

    /**
     * 创建客户端级别的指标名称
     * @param name 指标名称
     * @param description 指标描述
     * @return 创建的MetricName实例
     */
    private MetricName createMetricName(String name, String description) {
        return this.metrics.metricInstance(createTemplate(name, KafkaProducerMetrics.GROUP, description, this.tags));
    }

    /**
     * 创建主题级别的指标模板
     * @param name 指标名称
     * @param description 指标描述
     * @return 创建的MetricNameTemplate实例
     */
    private MetricNameTemplate createTopicTemplate(String name, String description) {
        return createTemplate(name, TOPIC_METRIC_GROUP_NAME, description, this.topicTags);
    }

    /* topic level metrics */
    /**
     * 获取特定主题的记录发送速率指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicRecordSendRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordSendRate, tags);
    }

    /**
     * 获取特定主题的记录发送总数指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicRecordSendTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordSendTotal, tags);
    }

    /**
     * 获取特定主题的字节发送速率指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicByteRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicByteRate, tags);
    }

    /**
     * 获取特定主题的字节发送总数指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicByteTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicByteTotal, tags);
    }

    /**
     * 获取特定主题的压缩率指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicCompressionRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicCompressionRate, tags);
    }

    /**
     * 获取特定主题的记录重试速率指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicRecordRetryRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordRetryRate, tags);
    }

    /**
     * 获取特定主题的记录重试总数指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicRecordRetryTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordRetryTotal, tags);
    }

    /**
     * 获取特定主题的记录错误速率指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicRecordErrorRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordErrorRate, tags);
    }

    /**
     * 获取特定主题的记录错误总数指标
     * @param tags 指标标签，包含topic信息
     * @return 对应主题的MetricName实例
     */
    public MetricName topicRecordErrorTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordErrorTotal, tags);
    }

    /**
     * 获取所有指标模板的列表
     * @return 指标模板列表
     */
    public List<MetricNameTemplate> allTemplates() {
        return allTemplates;
    }

    /**
     * 创建新的传感器
     * @param name 传感器名称
     * @return 创建的Sensor实例
     */
    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    /**
     * 添加新的指标
     * @param m 指标名称
     * @param measurable 可测量对象
     */
    public void addMetric(MetricName m, Measurable measurable) {
        this.metrics.addMetric(m, measurable);
    }

    /**
     * 获取已存在的传感器
     * @param name 传感器名称
     * @return 获取的Sensor实例
     */
    public Sensor getSensor(String name) {
        return this.metrics.getSensor(name);
    }

    /**
     * 创建指标模板并添加到模板列表中
     * @param name 指标名称
     * @param group 指标组名
     * @param description 指标描述
     * @param tags 指标标签集合
     * @return 创建的MetricNameTemplate实例
     */
    private MetricNameTemplate createTemplate(String name, String group, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, description, tags);
        this.allTemplates.add(template);
        return template;
    }

}
