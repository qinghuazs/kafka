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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Kafka生产者的配置类。这个类包含了所有Kafka生产者客户端的配置项。
 * 详细的配置说明可以在Kafka官方文档中找到：http://kafka.apache.org/documentation.html#producerconfigs
 */
public class ProducerConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(ProducerConfig.class);

    /*
     * 注意：不要修改配置字符串或它们的Java变量名，因为这些是公共API的一部分，
     * 修改会破坏用户代码。
     */

    private static final ConfigDef CONFIG;

    /** 
     * <code>bootstrap.servers</code>
     * Kafka集群连接地址，格式为host1:port1,host2:port2,...
     * 这是生产者连接Kafka集群的初始连接点，不需要包含所有的broker地址，
     * 生产者会从初始连接中获取到完整的集群信息
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** 
     * <code>client.dns.lookup</code>
     * 配置DNS查找行为
     */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /** 
     * <code>metadata.max.age.ms</code>
     * 元数据最大有效期，超过这个时间后元数据会被强制刷新
     * 即使没有任何分区领导者变更
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /** 
     * <code>metadata.max.idle.ms</code>
     * 控制生产者对于空闲主题的元数据缓存时间
     * 如果一个主题在这个时间内没有被生产消息，其元数据会被清除，
     * 下次访问时会重新获取元数据
     */
    public static final String METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms";
    private static final String METADATA_MAX_IDLE_DOC =
            "Controls how long the producer will cache metadata for a topic that's idle. If the elapsed " +
            "time since a topic was last produced to exceeds the metadata idle duration, then the topic's " +
            "metadata is forgotten and the next access to it will force a metadata fetch request.";

    /** 
     * <code>batch.size</code>
     * 生产者发送批次的大小配置（单位：字节）
     * 当多条消息要发送到同一个分区时，生产者会尝试将消息打包在一起，
     * 以减少请求次数，提高客户端和服务器的性能
     * linger.ms默认值在Kafka 4.0中从0改为5，因为更大的批次通常能带来更好的性能
     */
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "当多条消息被发送到同一个分区时，生产者会尝试将这些记录合并到更少的请求中。这有助于提升客户端和服务器端的性能。该配置控制默认的批次大小（单位：字节）。"
                                                 + "<p>"
                                                 + "不会尝试对大于此大小的记录进行批处理。"
                                                 + "<p>"
                                                 + "发送到broker的请求会包含多个批次，每个具有可发送数据的分区对应一个批次。"
                                                 + "<p>"
                                                 + "较小的批次大小会使批处理变得不太常见，可能会降低吞吐量（批次大小为零会完全禁用批处理）。过大的批次大小可能会造成内存使用效率略低，因为我们总是会分配指定批次大小的缓冲区以期待更多的记录。"
                                                 + "<p>"
                                                 + "注意：此设置给出了要发送的批次大小的上限。如果我们为此分区累积的字节数少于这个值，我们会在<code>linger.ms</code>时间内等待更多记录加入。"
                                                 + "<code>linger.ms</code>的默认值为5，这意味着生产者会等待5ms或直到记录批次达到<code>batch.size</code>（以先发生者为准）才发送记录批次。请注意，broker的背压可能会导致实际的等待时间比这个设置更长。"
                                                 + "在Apache Kafka 4.0中，默认值从0改为5，因为更大批次带来的效率提升通常会导致生产者延迟相似或更低，尽管增加了等待时间。";

    /** 
     * <code>partitioner.adaptive.partitioning.enable</code>
     * 是否启用自适应分区策略
     * 启用后，生产者会根据broker的性能自动调整消息分配，
     * 向性能更好的broker分区发送更多消息
     * 注意：使用自定义分区器时此配置无效
     */
    public static final String PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG = "partitioner.adaptive.partitioning.enable";
    private static final String PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_DOC =
            "When set to 'true', the producer will try to adapt to broker performance and produce more messages to partitions hosted on faster brokers. "
            + "If 'false', producer will try to distribute messages uniformly. Note: this setting has no effect if a custom partitioner is used";

    /** 
     * <code>partitioner.availability.timeout.ms</code>
     * 分区可用性超时配置
     * 如果一个broker在这个时间内无法处理来自某个分区的生产请求，
     * 分区器会将该分区标记为不可用
     * 设置为0则禁用此功能
     * 注意：当使用自定义分区器或禁用自适应分区（partitioner.adaptive.partitioning.enable=false）时，
     * 此配置无效
     */
    public static final String PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG = "partitioner.availability.timeout.ms";
    private static final String PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC =
            "If a broker cannot process produce requests from a partition for <code>" + PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG + "</code> time, "
            + "the partitioner treats that partition as not available.  If the value is 0, this logic is disabled. "
            + "Note: this setting has no effect if a custom partitioner is used or <code>" + PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG
            + "</code> is set to 'false'";

    /** 
     * <code>partitioner.ignore.keys</code>
     * 是否忽略消息键进行分区分配
     * 设置为true时，生产者不会使用消息的key来选择分区
     * 设置为false时，当消息包含key时，生产者会根据key的哈希值选择分区
     * 注意：使用自定义分区器时此配置无效
     */
    public static final String PARTITIONER_IGNORE_KEYS_CONFIG = "partitioner.ignore.keys";
    private static final String PARTITIONER_IGNORE_KEYS_DOC = "When set to 'true' the producer won't use record keys to choose a partition. "
            + "If 'false', producer would choose a partition based on a hash of the key when a key is present. "
            + "Note: this setting has no effect if a custom partitioner is used.";

   /** 
     * <code>acks</code>
     * 生产者需要服务端确认的机制配置，用于控制消息的可靠性：
     * acks=0：生产者不等待服务端确认，消息发出即认为成功，可能丢失数据
     * acks=1：等待leader副本确认即可，如果leader宕机且follower未同步，消息可能丢失
     * acks=all：等待所有同步副本确认，提供最强的可靠性保证
     * 注意：启用幂等性时必须将此值设置为'all'
     */
    public static final String ACKS_CONFIG = "acks";
    private static final String ACKS_DOC = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the "
                                           + " durability of records that are sent. The following settings are allowed: "
                                           + " <ul>"
                                           + " <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the"
                                           + " server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be"
                                           + " made that the server has received the record in this case, and the <code>retries</code> configuration will not"
                                           + " take effect (as the client won't generally know of any failures). The offset given back for each record will"
                                           + " always be set to <code>-1</code>."
                                           + " <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond"
                                           + " without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after"
                                           + " acknowledging the record but before the followers have replicated it then the record will be lost."
                                           + " <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to"
                                           + " acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica"
                                           + " remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting."
                                           + "</ul>"
                                           + "<p>"
                                           + "Note that enabling idempotence requires this config value to be 'all'."
                                           + " If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled.";

   /** 
     * <code>linger.ms</code>
     * 发送延迟时间配置
     * 生产者在发送批次之前等待更多消息加入批次的时间
     * 增加此值可以提高吞吐量，但会增加延迟
     * Kafka 4.0中默认值从0ms改为5ms，因为实践证明轻微的延迟换来的批次效率提升是值得的
     */
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC = "The producer groups together any records that arrive in between request transmissions into a single batched request. "
                                                + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
                                                + "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
                                                + "of artificial delay&mdash;that is, rather than immediately sending out a record, the producer will wait for up to "
                                                + "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
                                                + "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
                                                + "we get <code>" + BATCH_SIZE_CONFIG + "</code> worth of records for a partition it will be sent immediately regardless of this "
                                                + "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
                                                + "specified time waiting for more records to show up. This setting defaults to 5 (i.e. 5ms delay). Increasing <code>" + LINGER_MS_CONFIG + "=50</code>, "
                                                + "for example, would have the effect of reducing the number of requests sent but would add up to 50ms of latency to records sent in the absence of load."
                                                + "The default changed from 0 to 5 in Apache Kafka 4.0 as the efficiency gains from larger batches typically result in "
                                                + "similar or lower producer latency despite the increased linger.";

   /** 
     * <code>request.timeout.ms</code>
     * 生产者等待请求响应的最大时间
     * 这个值应该比broker端的replica.lag.time.max.ms大
     * 以减少因不必要的生产者重试导致的消息重复
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
        + " This should be larger than <code>replica.lag.time.max.ms</code> (a broker configuration)"
        + " to reduce the possibility of message duplication due to unnecessary producer retries.";


    /** 
     * <code>buffer.memory</code>
     * 生产者可用于缓存等待发送记录的内存总字节数
     * 如果记录发送速度超过发送到服务器的速度，生产者会阻塞max.block.ms时间
     * 超时后会抛出异常
     */
    public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";

    /** 
     * <code>max.request.size</code>
     * 生产者发送的单个请求的最大大小
     * 这个配置限制了单个请求中批次记录的数量，以避免发送过大的请求
     * 同时也是未压缩的记录批次大小的上限
     */
    public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
    private static final String MAX_REQUEST_SIZE_DOC =
        "The maximum size of a request in bytes. This setting will limit the number of record " +
        "batches the producer will send in a single request to avoid sending huge requests. " +
        "This is also effectively a cap on the maximum uncompressed record batch size. Note that the server " +
        "has its own cap on the record batch size (after compression if compression is enabled) which may be different from this.";

    /** 
     * <code>delivery.timeout.ms</code>
     * 发送操作的最大允许时间，从send()调用到成功或失败的总时间限制
     * 包括重试时间，这个值应该大于request.timeout.ms + linger.ms
     */
    public static final String DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms";
    private static final String DELIVERY_TIMEOUT_MS_DOC = "An upper bound on the time to report success or failure "
    + "after a call to <code>send()</code> returns. This limits the total time that a record will be delayed "
    + "prior to sending, the time to await acknowledgement from the broker (if expected), and the time allowed "
    + "for retriable send failures. The producer may report failure to send a record earlier than this config if "
    + "either an unrecoverable error is encountered, the retries have been exhausted, "
    + "or the record is added to a batch which reached an earlier delivery expiration deadline. "
    + "The value of this config should be greater than or equal to the sum of <code>" + REQUEST_TIMEOUT_MS_CONFIG + "</code> "
    + "and <code>" + LINGER_MS_CONFIG + "</code>.";


    /** 
     * <code>max.block.ms</code>
     * send()方法阻塞的最大时间
     * 当缓冲区满或元数据不可用时，send()方法会阻塞
     * 超过这个时间会抛出TimeoutException
     */
    public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";

    /** 
     * <code>client.id</code>
     * 生产者客户端的标识ID
     * 用于在服务端日志中识别消息来源，便于追踪调试
     * 如果不设置则自动生成一个
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /** 
     * <code>send.buffer.bytes</code>
     * TCP发送缓冲区大小
     * 如果设置为-1，则使用操作系统默认值
     * 建议在高吞吐量场景下适当增大此值
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /** 
     * <code>receive.buffer.bytes</code>
     * TCP接收缓冲区大小
     * 如果设置为-1，则使用操作系统默认值
     * 建议在高吞吐量场景下适当增大此值
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /** 
     * <code>reconnect.backoff.ms</code>
     * 重新连接主机之前的等待时间
     * 避免在连接失败时立即重试，这样可以减轻服务器负载
     * 支持指数退避机制，实际等待时间会随着重试次数增加
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /** 
     * <code>reconnect.backoff.max.ms</code>
     * 重连退避时间的最大值
     * 用于限制指数退避机制的最大等待时间
     * 防止在网络问题持续存在时等待时间过长
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    private static final String MAX_BLOCK_MS_DOC = "The configuration controls how long the <code>KafkaProducer</code>'s <code>send()</code>, <code>partitionsFor()</code>, "
                                                    + "<code>initTransactions()</code>, <code>sendOffsetsToTransaction()</code>, <code>commitTransaction()</code> "
                                                    + "and <code>abortTransaction()</code> methods will block. "
                                                    + "For <code>send()</code> this timeout bounds the total time waiting for both metadata fetch and buffer allocation "
                                                    + "(blocking in the user-supplied serializers or partitioner is not counted against this timeout). "
                                                    + "For <code>partitionsFor()</code> this timeout bounds the time spent waiting for metadata if it is unavailable. "
                                                    + "The transaction-related methods always block, but may timeout if "
                                                    + "the transaction coordinator could not be discovered or did not respond within the timeout.";

    private static final String BUFFER_MEMORY_DOC = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are "
                                                    + "sent faster than they can be delivered to the server the producer will block for <code>" + MAX_BLOCK_MS_CONFIG + "</code> after which it will fail with an exception."
                                                    + "<p>"
                                                    + "This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since "
                                                    + "not all memory the producer uses is used for buffering. Some additional memory will be used for compression (if "
                                                    + "compression is enabled) as well as for maintaining in-flight requests.";

    /** 
     * <code>retry.backoff.ms</code>
     * 重试发送失败消息之前的等待时间
     * 避免在发送失败时立即重试，这样可以减轻服务器负载
     * 支持指数退避机制，实际等待时间会随着重试次数增加
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /** 
     * <code>retry.backoff.max.ms</code>
     * 重试退避时间的最大值
     * 用于限制指数退避机制的最大等待时间
     * 防止在发送失败持续存在时等待时间过长
     */
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG;

    /** 
     * <code>enable.metrics.push</code>
     * 是否启用指标推送功能
     * 当设置为true时，生产者会主动将性能指标推送到监控系统
     * 这对于大规模集群的监控和性能调优非常有用
     * 默认值为false，表示不启用指标推送
     */
    public static final String ENABLE_METRICS_PUSH_CONFIG = CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG;
    // 使用通用客户端配置中的文档说明
    public static final String ENABLE_METRICS_PUSH_DOC = CommonClientConfigs.ENABLE_METRICS_PUSH_DOC;

    /** 
     * <code>compression.type</code>
     * 生产者的消息压缩类型配置
     * 可选值：none, gzip, snappy, lz4, zstd
     * 压缩可以减少网络传输和存储开销，但会增加CPU开销
     * 默认值是none(不压缩)
     */
    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String COMPRESSION_TYPE_DOC = "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid "
                                                       + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or <code>zstd</code>. "
                                                       + "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).";

    /** 
     * <code>compression.gzip.level</code>
     * GZIP压缩级别配置
     * 当compression.type设置为gzip时，此配置决定压缩级别
     * 可选值范围：-1到9
     * -1：默认压缩级别（通常是6）
     * 0：不压缩
     * 1：最快压缩，压缩比最低
     * 9：最慢压缩，压缩比最高
     * 建议：对于对延迟敏感的场景使用较低的压缩级别（1-3），
     * 对于需要节省网络带宽的场景使用较高的压缩级别（7-9）
     */
    public static final String COMPRESSION_GZIP_LEVEL_CONFIG = "compression.gzip.level";
    private static final String COMPRESSION_GZIP_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>gzip</code>.";

    /** 
     * <code>compression.lz4.level</code>
     * LZ4压缩级别配置
     * 当compression.type设置为lz4时，此配置决定压缩级别
     * 可选值范围：1到12（LZ4-HC压缩模式）
     * 值越大，压缩比越高，但CPU消耗也越大
     * LZ4以其快速的压缩速度著称，即使在较低压缩级别下也能提供不错的压缩比
     * 建议：大多数场景使用默认值即可，如果CPU资源充足且需要更高压缩比，
     * 可以考虑使用更高的压缩级别
     */
    public static final String COMPRESSION_LZ4_LEVEL_CONFIG = "compression.lz4.level";
    private static final String COMPRESSION_LZ4_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>lz4</code>.";

    /** 
     * <code>compression.zstd.level</code>
     * Zstandard压缩级别配置
     * 当compression.type设置为zstd时，此配置决定压缩级别
     * 可选值范围：-131072到22
     * 负值：启用特殊的实时压缩模式，绝对值越大，压缩速度越快，但压缩比越低
     * 1-22：普通压缩模式，值越大压缩比越高，但压缩速度越慢
     * 建议：
     * - 对于实时性要求高的场景，可以使用负值
     * - 对于普通场景，使用1-3级别可以获得较好的压缩速度和压缩比平衡
     * - 对于存档或需要极高压缩比的场景，可以使用更高级别（10-22）
     */
    public static final String COMPRESSION_ZSTD_LEVEL_CONFIG = "compression.zstd.level";
    private static final String COMPRESSION_ZSTD_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>zstd</code>.";

    /** 
     * <code>metrics.sample.window.ms</code>
     * 性能指标采样的时间窗口大小
     * 用于计算性能指标的滑动窗口周期
     * 例如吞吐量等指标会在此窗口内进行计算
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /** 
     * <code>metrics.num.samples</code>
     * 性能指标采样的样本数量
     * 用于维护性能指标的样本数
     * 样本数越多，指标越平滑，但消耗的内存也越多
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /** 
     * <code>metrics.recording.level</code>
     * 指标记录级别配置
     * 控制生产者的指标收集详细程度，可选值：
     * - INFO：只记录基本指标，如消息发送速率、延迟等
     * - DEBUG：记录更详细的指标，包括每个主题和分区级别的指标
     * 默认为INFO级别，在需要更细粒度监控时可以设置为DEBUG
     * 注意：DEBUG级别会产生更多的指标数据，可能增加JMX开销
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /** 
     * <code>metric.reporters</code>
     * 指标报告器的实现类列表
     * 用于收集和报告生产者的性能指标
     * 可以实现自定义的指标收集器
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /** 
     * 当启用生产者幂等性时，每个连接的最大未完成请求数必须小于或等于5，这是为了确保消息顺序。
     * 这个限制值5与ProducerStateEntry#NUM_BATCHES_TO_RETAIN保持一致，原因如下：
     * 1. 幂等性生产者需要在服务端维护消息状态，以检测重复消息
     * 2. 服务端为每个生产者维护一个固定大小的状态缓存，大小由NUM_BATCHES_TO_RETAIN决定
     * 3. 如果未完成请求数超过这个限制，可能导致服务端状态缓存溢出，影响幂等性保证
     * 4. 同时这个限制也有助于减少内存使用和网络拥塞
     */
    private static final int MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE = 5;

    /** 
     * <code>max.in.flight.requests.per.connection</code>
     * 每个连接最大的未确认请求数
     * 限制客户端在单个连接上能够发送的未确认请求数量
     * 当启用幂等性时，此值必须小于等于5，以保证消息顺序
     * 如果未启用幂等性且此值大于1，可能会因为重试导致消息乱序
     */
    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
    private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC = "The maximum number of unacknowledged requests the client will send on a single connection before blocking."
                                                                            + " Note that if this configuration is set to be greater than 1 and <code>enable.idempotence</code> is set to false, there is a risk of"
                                                                            + " message reordering after a failed send due to retries (i.e., if retries are enabled); "
                                                                            + " if retries are disabled or if <code>enable.idempotence</code> is set to true, ordering will be preserved."
                                                                            + " Additionally, enabling idempotence requires the value of this configuration to be less than or equal to " + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE + ","
                                                                            + " because broker only retains at most 5 batches for each producer. If the value is more than 5, previous batches may be removed on broker side.";

    /** 
     * <code>retries</code>
     * 重试次数配置
     * 当设置大于0的值时，如果消息发送失败且错误是可重试的，客户端会重新发送消息
     * 注意事项：
     * 1. 重试行为与客户端收到错误后手动重发消息的效果相同
     * 2. 如果在重试耗尽前delivery.timeout.ms配置的超时时间到期，发送请求会失败
     * 3. 建议不要直接设置此配置，而是通过delivery.timeout.ms来控制重试行为
     * 4. 启用幂等性时，此值必须大于0
     * 5. 如果设置了冲突的配置且未显式启用幂等性，则幂等性将被禁用
     * 6. 当enable.idempotence=false且max.in.flight.requests.per.connection>1时，
     *    允许重试可能会改变消息顺序，因为如果向同一分区发送两个批次，第一个失败并重试，
     *    但第二个成功，则第二个批次的消息可能先出现
     */
    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;
    private static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error."
            + " Note that this retry is no different than if the client resent the record upon receiving the error."
            + " Produce requests will be failed before the number of retries has been exhausted if the timeout configured by"
            + " <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> expires first before successful acknowledgement. Users should generally"
            + " prefer to leave this config unset and instead use <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> to control"
            + " retry behavior."
            + "<p>"
            + "Enabling idempotence requires this config value to be greater than 0."
            + " If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled."
            + "<p>"
            + "Allowing retries while setting <code>enable.idempotence</code> to <code>false</code> and <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to greater than 1 will potentially change the"
            + " ordering of records because if two batches are sent to a single partition, and the first fails and is retried but the second"
            + " succeeds, then the records in the second batch may appear first.";


    /** 
     * <code>key.serializer</code>
     * 消息键的序列化器类
     * 必须实现org.apache.kafka.common.serialization.Serializer接口
     * 用于将消息的key转换为字节数组以便网络传输
     */
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /** 
     * <code>value.serializer</code>
     * 消息值的序列化器类
     * 必须实现org.apache.kafka.common.serialization.Serializer接口
     * 用于将消息的value转换为字节数组以便网络传输
     */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /** 
     * <code>socket.connection.setup.timeout.ms</code>
     * Socket连接建立超时时间
     * 建立TCP连接的超时时间，如果在此时间内无法建立连接，则连接失败
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /** 
     * <code>socket.connection.setup.timeout.max.ms</code>
     * Socket连接建立最大超时时间
     * 在重试连接时，超时时间会指数增加，此配置限制了最大超时时间
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /** 
     * <code>connections.max.idle.ms</code>
     * 连接最大空闲时间
     * 如果连接空闲超过此时间，将被关闭
     * 这有助于释放不活跃的连接占用的资源
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** 
     * <code>partitioner.class</code>
     * 分区器类配置
     * 决定消息发送到哪个分区的策略类。可用选项：
     * 1. 默认分区器（不设置此配置时使用）：
     *    - 当分区中累积的数据达到batch.size时发送
     *    - 如果指定了key，根据key的哈希值选择分区
     *    - 如果没有key，使用粘性分区策略（当达到batch.size时更换分区）
     * 2. RoundRobinPartitioner：
     *    - 轮询策略，消息依次发送到不同分区
     *    - 不考虑消息是否有key
     *    - 注意：新建批次时可能导致分布不均（参见KAFKA-9965）
     * 3. 自定义分区器：
     *    - 实现Partitioner接口来自定义分区逻辑
     */
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    private static final String PARTITIONER_CLASS_DOC = "Determines which partition to send a record to when records are produced. Available options are:" +
            "<ul>" +
            "<li>If not set, the default partitioning logic is used. " +
            "This strategy send records to a partition until at least " + BATCH_SIZE_CONFIG + " bytes is produced to the partition. It works with the strategy:" +
            "<ol>" +
            "<li>If no partition is specified but a key is present, choose a partition based on a hash of the key.</li>" +
            "<li>If no partition or key is present, choose the sticky partition that changes when at least " + BATCH_SIZE_CONFIG + " bytes are produced to the partition.</li>" +
            "</ol>" +
            "</li>" +
            "<li><code>org.apache.kafka.clients.producer.RoundRobinPartitioner</code>: A partitioning strategy where " +
            "each record in a series of consecutive records is sent to a different partition, regardless of whether the 'key' is provided or not, " +
            "until partitions run out and the process starts over again. Note: There's a known issue that will cause uneven distribution when a new batch is created. " +
            "See KAFKA-9965 for more detail." +
            "</li>" +
            "</ul>" +
            "<p>Implementing the <code>org.apache.kafka.clients.producer.Partitioner</code> interface allows you to plug in a custom partitioner.";

    /** 
     * <code>interceptor.classes</code>
     * 拦截器类配置
     * 指定一组拦截器类，用于在消息发送到Kafka集群之前进行拦截处理
     * 通过实现ProducerInterceptor接口，可以：
     * 1. 在消息发送前修改或转换消息
     * 2. 监控和跟踪消息发送过程
     * 3. 实现自定义的消息处理逻辑
     * 默认情况下没有配置任何拦截器
     */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
                                                        + "Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records "
                                                        + "received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.";

    /** 
     * <code>enable.idempotence</code>
     * 是否启用幂等性
     * 当设置为true时，生产者确保每条消息只会被写入一次，即使发生重试
     * 启用幂等性的要求：
     * 1. max.in.flight.requests.per.connection必须小于等于5
     * 2. retries必须大于0
     * 3. acks必须设置为'all'
     * 重要说明：
     * 1. 如果没有设置冲突配置，幂等性默认启用
     * 2. 如果设置了冲突配置且未显式启用幂等性，则幂等性被禁用
     * 3. 如果显式启用幂等性但存在冲突配置，将抛出ConfigException
     */
    public static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
    public static final String ENABLE_IDEMPOTENCE_DOC = "When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer "
                                                        + "retries due to broker failures, etc., may write duplicates of the retried message in the stream. "
                                                        + "Note that enabling idempotence requires <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to be less than or equal to " + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE
                                                        + " (with message ordering preserved for any allowable value), <code>" + RETRIES_CONFIG + "</code> to be greater than 0, and <code>"
                                                        + ACKS_CONFIG + "</code> must be 'all'. "
                                                        + "<p>"
                                                        + "Idempotence is enabled by default if no conflicting configurations are set. "
                                                        + "If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled. "
                                                        + "If idempotence is explicitly enabled and conflicting configurations are set, a <code>ConfigException</code> is thrown.";

    /** 
     * <code>transaction.timeout.ms</code>
     * 事务超时时间
     * 事务保持打开状态的最大时间，超过此时间协调器会主动中止事务
     * 事务开始时间点是添加第一个分区时
     * 注意：如果此值大于broker的transaction.max.timeout.ms设置，
     * 请求会失败并抛出InvalidTxnTimeoutException异常
     */
    public static final String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
    public static final String TRANSACTION_TIMEOUT_DOC = "The maximum amount of time in milliseconds that a transaction will remain open before the coordinator proactively aborts it. " +
            "The start of the transaction is set at the time that the first partition is added to it. " +
            "If this value is larger than the <code>transaction.max.timeout.ms</code> setting in the broker, the request will fail with a <code>InvalidTxnTimeoutException</code> error.";

    /** 
     * <code>transactional.id</code>
     * 事务ID配置
     * 用于实现跨多个生产者会话的可靠性语义
     * 重要特性：
     * 1. 允许确保使用相同事务ID的事务在开始新事务前已完成
     * 2. 如果未提供事务ID，生产者只能使用幂等性投递
     * 3. 配置事务ID会自动启用幂等性
     * 4. 默认不配置事务ID，意味着不能使用事务
     * 注意：默认情况下，事务需要至少三个broker的集群（生产环境推荐）
     * 开发环境可以通过调整broker的transaction.state.log.replication.factor来改变这个要求
     */
    public static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";
    public static final String TRANSACTIONAL_ID_DOC = "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. " +
            "If a TransactionalId is configured, <code>enable.idempotence</code> is implied. " +
            "By default the TransactionId is not configured, which means transactions cannot be used. " +
            "Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.";

    /** 
     * <code>security.providers</code>
     * 自定义安全提供者配置
     * 用于指定一组自定义的安全提供者实现类，这些提供者用于实现特定的安全机制
     * 配置格式为：provider_name:provider_class;provider_name2:provider_class2
     * 例如："CUSTOM_PROVIDER:com.example.CustomProvider"
     * 
     * 使用场景：
     * 1. 需要使用自定义加密算法时
     * 2. 需要实现特定的安全认证机制时
     * 3. 需要扩展Kafka默认的安全功能时
     * 
     * 注意事项：
     * 1. 提供者类必须实现java.security.Provider接口
     * 2. 多个提供者按照配置顺序进行优先级排序
     * 3. 自定义提供者会在JVM默认提供者之前被加载
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    // 用于生成唯一的生产者客户端ID的计数器
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * 静态初始化块，用于定义所有Kafka生产者的配置项
     * 使用ConfigDef来定义每个配置的类型、默认值、验证规则和重要性级别
     */
    static {
        // 初始化配置定义对象
        CONFIG = new ConfigDef()
                // 定义bootstrap.servers配置：Kafka集群连接地址列表
                .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(), Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                //DNS配置，默认是USE_ALL_DNS_IPS
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,
                                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                                //默认是32M
                                .define(BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), Importance.HIGH, BUFFER_MEMORY_DOC)
                                //重试次数 默认是Integer.MAX_VALUE
                                .define(RETRIES_CONFIG, Type.INT, Integer.MAX_VALUE, between(0, Integer.MAX_VALUE), Importance.HIGH, RETRIES_DOC)
                                //acks 默认是all
                                .define(ACKS_CONFIG,
                                        Type.STRING,
                                        "all",
                                        in("all", "-1", "0", "1"),
                                        Importance.LOW,
                                        ACKS_DOC)
                                //压缩类型 默认是 none 不压缩
                                .define(COMPRESSION_TYPE_CONFIG, Type.STRING, CompressionType.NONE.name, in(Utils.enumOptions(CompressionType.class)), Importance.HIGH, COMPRESSION_TYPE_DOC)
                                .define(COMPRESSION_GZIP_LEVEL_CONFIG, Type.INT, CompressionType.GZIP.defaultLevel(), CompressionType.GZIP.levelValidator(), Importance.MEDIUM, COMPRESSION_GZIP_LEVEL_DOC)
                                .define(COMPRESSION_LZ4_LEVEL_CONFIG, Type.INT, CompressionType.LZ4.defaultLevel(), CompressionType.LZ4.levelValidator(), Importance.MEDIUM, COMPRESSION_LZ4_LEVEL_DOC)
                                .define(COMPRESSION_ZSTD_LEVEL_CONFIG, Type.INT, CompressionType.ZSTD.defaultLevel(), CompressionType.ZSTD.levelValidator(), Importance.MEDIUM, COMPRESSION_ZSTD_LEVEL_DOC)
                                //默认16kb
                                .define(BATCH_SIZE_CONFIG, Type.INT, 16384, atLeast(0), Importance.MEDIUM, BATCH_SIZE_DOC)
                                .define(PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG, Type.BOOLEAN, true, Importance.LOW, PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_DOC)
                                .define(PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, Type.LONG, 0, atLeast(0), Importance.LOW, PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC)
                                .define(PARTITIONER_IGNORE_KEYS_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM, PARTITIONER_IGNORE_KEYS_DOC)
                                //linger.ms配置：消息发送前等待的时间，默认为5ms
                                .define(LINGER_MS_CONFIG, Type.LONG, 5, atLeast(0), Importance.MEDIUM, LINGER_MS_DOC)
                                //默认2分钟
                                .define(DELIVERY_TIMEOUT_MS_CONFIG, Type.INT, 120 * 1000, atLeast(0), Importance.MEDIUM, DELIVERY_TIMEOUT_MS_DOC)
                                //client_id 默认是空
                                .define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
                                .define(SEND_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG, Type.INT, 32 * 1024, atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(MAX_REQUEST_SIZE_CONFIG,
                                        Type.INT,
                                        1024 * 1024,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        MAX_REQUEST_SIZE_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 50L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG, Type.LONG, 1000L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                                //默认是100ms
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(RETRY_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
                                .define(ENABLE_METRICS_PUSH_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        ENABLE_METRICS_PUSH_DOC)
                                //默认是60000 ms 也就是 1分钟
                                .define(MAX_BLOCK_MS_CONFIG,
                                        Type.LONG,
                                        60 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        MAX_BLOCK_MS_DOC)
                                //默认是 30 秒
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        30 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        REQUEST_TIMEOUT_MS_DOC)
                                .define(METADATA_MAX_AGE_CONFIG, Type.LONG, 5 * 60 * 1000, atLeast(0), Importance.LOW, METADATA_MAX_AGE_DOC)
                                .define(METADATA_MAX_IDLE_CONFIG,
                                        Type.LONG,
                                        5 * 60 * 1000,
                                        atLeast(5000),
                                        Importance.LOW,
                                        METADATA_MAX_IDLE_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG, Type.INT, 2, atLeast(1), Importance.LOW, CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                                .define(METRICS_RECORDING_LEVEL_CONFIG,
                                        Type.STRING,
                                        Sensor.RecordingLevel.INFO.toString(),
                                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.TRACE.toString()),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        JmxReporter.class.getName(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                                .define(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                                        Type.INT,
                                        5,
                                        atLeast(1),
                                        Importance.LOW,
                                        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC)
                                .define(KEY_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        KEY_SERIALIZER_CLASS_DOC)
                                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_SERIALIZER_CLASS_DOC)
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,
                                        9 * 60 * 1000,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                                .define(PARTITIONER_CLASS_CONFIG,
                                        Type.CLASS,
                                        null,
                                        Importance.MEDIUM, PARTITIONER_CLASS_DOC)
                                .define(INTERCEPTOR_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        INTERCEPTOR_CLASSES_DOC)
                                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                        ConfigDef.CaseInsensitiveValidString
                                                .in(Utils.enumOptions(SecurityProtocol.class)),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .define(SECURITY_PROVIDERS_CONFIG,
                                        Type.STRING,
                                        null,
                                        Importance.LOW,
                                        SECURITY_PROVIDERS_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport()
                                .define(ENABLE_IDEMPOTENCE_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        ENABLE_IDEMPOTENCE_DOC)
                                .define(TRANSACTION_TIMEOUT_CONFIG,
                                        Type.INT,
                                        60000,
                                        Importance.LOW,
                                        TRANSACTION_TIMEOUT_DOC)
                                .define(TRANSACTIONAL_ID_CONFIG,
                                        Type.STRING,
                                        null,
                                        new ConfigDef.NonEmptyString(),
                                        Importance.LOW,
                                        TRANSACTIONAL_ID_DOC)
                                .define(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY,
                                        ConfigDef.CaseInsensitiveValidString
                                                .in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC)
                                .define(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC);
    }

    /**
     * 处理解析后的配置
     * 该方法在配置解析完成后被调用，用于执行额外的配置验证和处理
     *
     * @param parsedValues 解析后的配置值Map
     * @return 处理后的配置值Map
     */
    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        // 验证SASL机制配置
        CommonClientConfigs.postValidateSaslMechanismConfig(this);
        // 检查并警告禁用指数退避
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        // 处理重连退避相关配置
        Map<String, Object> refinedConfigs = CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        // 处理并验证幂等性相关配置
        postProcessAndValidateIdempotenceConfigs(refinedConfigs);
        // 处理客户端ID配置
        maybeOverrideClientId(refinedConfigs);
        return refinedConfigs;
    }

    /**
     * 处理客户端ID配置
     * 如果用户没有配置client.id，则自动生成一个
     * 生成规则：如果配置了transactional.id，使用其值作为后缀；否则使用自增序号
     *
     * @param configs 配置Map
     */
    private void maybeOverrideClientId(final Map<String, Object> configs) {
        String refinedClientId;
        // 检查用户是否配置了client.id
        boolean userConfiguredClientId = this.originals().containsKey(CLIENT_ID_CONFIG);
        if (userConfiguredClientId) {
            // 如果用户配置了，直接使用用户配置的值
            refinedClientId = this.getString(CLIENT_ID_CONFIG);
        } else {
            // 如果用户没有配置，则自动生成一个client.id  格式为：producer-[<transactionalId>|<序号>]
            String transactionalId = this.getString(TRANSACTIONAL_ID_CONFIG);
            refinedClientId = "producer-" + (transactionalId != null ? transactionalId : PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement());
        }
        configs.put(CLIENT_ID_CONFIG, refinedClientId);
    }

    /**
     * 处理和验证幂等性相关的配置
     * 主要检查：
     * 1. retries配置是否合适
     * 2. acks配置是否为'all'
     * 3. max.in.flight.requests.per.connection是否不超过5
     * 4. 事务ID配置是否合法
     *
     * @param configs 配置Map
     */
    private void postProcessAndValidateIdempotenceConfigs(final Map<String, Object> configs) {
        // 获取原始配置
        final Map<String, Object> originalConfigs = this.originals();
        // 解析acks配置
        final String acksStr = parseAcks(this.getString(ACKS_CONFIG));
        configs.put(ACKS_CONFIG, acksStr);
        // 检查用户是否显式配置了enable.idempotence
        final boolean userConfiguredIdempotence = this.originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG);
        boolean idempotenceEnabled = this.getBoolean(ENABLE_IDEMPOTENCE_CONFIG);
        boolean shouldDisableIdempotence = false;

        // For idempotence producers, values for `retries` and `acks` and `max.in.flight.requests.per.connection` need validation
        if (idempotenceEnabled) {
            final int retries = this.getInt(RETRIES_CONFIG);
            if (retries == 0) {
                if (userConfiguredIdempotence) {
                    throw new ConfigException("Must set " + RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
                }
                log.info("Idempotence will be disabled because {} is set to 0.", RETRIES_CONFIG);
                shouldDisableIdempotence = true;
            }

            final short acks = Short.parseShort(acksStr);
            if (acks != (short) -1) {
                if (userConfiguredIdempotence) {
                    throw new ConfigException("Must set " + ACKS_CONFIG + " to all in order to use the idempotent " +
                        "producer. Otherwise we cannot guarantee idempotence.");
                }
                log.info("Idempotence will be disabled because {} is set to {}, not set to 'all'.", ACKS_CONFIG, acks);
                shouldDisableIdempotence = true;
            }

            final int inFlightConnection = this.getInt(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
            if (MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE < inFlightConnection) {
                throw new ConfigException("To use the idempotent producer, " + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION +
                                          " must be set to at most 5. Current value is " + inFlightConnection + ".");
            }
        }

        if (shouldDisableIdempotence) {
            configs.put(ENABLE_IDEMPOTENCE_CONFIG, false);
            idempotenceEnabled = false;
        }

        // validate `transaction.id` after validating idempotence dependant configs because `enable.idempotence` config might be overridden
        boolean userConfiguredTransactions = originalConfigs.containsKey(TRANSACTIONAL_ID_CONFIG);
        if (!idempotenceEnabled && userConfiguredTransactions) {
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");
        }
    }

    /**
     * 解析acks配置值
     * 将字符串配置值转换为标准格式：
     * - "all" 转换为 "-1"
     * - 数字字符串保持不变
     * - 非法值抛出ConfigException
     *
     * @param acksString acks配置的字符串值
     * @return 标准化后的acks值
     * @throws ConfigException 当配置值非法时抛出
     */
    private static String parseAcks(String acksString) {
        try {
            // 去除空白字符并统一处理"all"值
            return acksString.trim().equalsIgnoreCase("all") ? "-1" : Short.parseShort(acksString.trim()) + "";
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * 将序列化器添加到配置中
     * 如果提供了序列化器实例，使用其类名作为配置值
     * 如果没有提供实例且配置中也没有指定，则抛出异常
     *
     * @param configs 原始配置Map
     * @param keySerializer 键序列化器实例，可以为null
     * @param valueSerializer 值序列化器实例，可以为null
     * @return 添加了序列化器配置的新Map
     * @throws ConfigException 当序列化器配置缺失时抛出
     */
    static Map<String, Object> appendSerializerToConfig(Map<String, Object> configs,
            Serializer<?> keySerializer,
            Serializer<?> valueSerializer) {
        // 创建新的配置Map，避免修改原始配置
        Map<String, Object> newConfigs = new HashMap<>(configs);
        
        // 处理键序列化器配置
        if (keySerializer != null)
            newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
        else if (newConfigs.get(KEY_SERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_SERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        
        // 处理值序列化器配置
        if (valueSerializer != null)
            newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
        else if (newConfigs.get(VALUE_SERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_SERIALIZER_CLASS_CONFIG, null, "must be non-null.");
            
        return newConfigs;
    }

    public ProducerConfig(Properties props) {
        super(CONFIG, props);
    }

    public ProducerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    ProducerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "producerconfigs_" + config));
    }

}
