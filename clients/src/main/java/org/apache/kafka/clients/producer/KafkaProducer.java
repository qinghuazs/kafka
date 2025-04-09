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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.clients.producer.internals.KafkaProducerMetrics;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("linger.ms", 1);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * 生产者包含一个缓冲池，用于存储尚未传输到服务器的记录，同时还有一个后台 I/O 线程负责将这些记录转换为请求并将其传输到集群。
 * 如果使用后未关闭生产者，将会导致这些资源泄漏。
 * - 缓冲池：暂存待发送的消息记录
 * - 后台 I/O 线程：处理实际的消息发送工作
 * 同时强调了在使用完生产者后必须调用 close() 方法来释放资源，否则会造成资源泄漏。
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called, it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The default setting "all"
 * will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry. The <code>retries</code> setting defaults to <code>Integer.MAX_VALUE</code>, and
 * it's recommended to use <code>delivery.timeout.ms</code> to control retry behavior, instead of <code>retries</code>.
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code>. So, under heavy load,
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it returns
 * a failed future with BufferExhaustedException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple byte or string types.
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional producer.
 * The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery. In particular
 * producer retries will no longer introduce duplicates. The transactional producer allows an application to send messages
 * to multiple partitions (and topics!) atomically.
 * </p>
 * <p>
 * From Kafka 3.0, the <code>enable.idempotence</code> configuration defaults to true. When enabling idempotence,
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will
 * not need to be modified to take advantage of this feature.
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>. Additionally, if a {@link #send(ProducerRecord)}
 * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
 * then it is recommended to shut down the producer and check the contents of the last produced message to ensure that
 * it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a single session.
 * </p>
 * <p>To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on. Further, topics which are included in transactions should be configured
 * for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and the
 * <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional guarantees
 * to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful, application.
 * As such, it should be unique to each producer instance running within a partitioned application.
 * </p>
 * <p>All the new transactional APIs are blocking and will throw exceptions on failure. The example
 * below illustrates how the new APIs are meant to be used. It is similar to the example above, except that all
 * 100 messages are part of a single transaction.
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required
 * to specify callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the
 * <code>producer.send()</code> or transactional calls hit an irrecoverable error during a transaction. See the {@link #send(ProducerRecord)}
 * documentation for more details about detecting errors from a transactional send.
 * </p>
 * </p>By calling
 * <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features.  For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive an
 * <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private final String clientId;
    // Visible for testing
    final Metrics metrics;
    private final KafkaProducerMetrics producerMetrics;
    private final Plugin<Partitioner> partitionerPlugin;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final ProducerMetadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final Compression compression;
    private final Sensor errors;
    private final Time time;
    private final Plugin<Serializer<K>> keySerializerPlugin;
    private final Plugin<Serializer<V>> valueSerializerPlugin;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final boolean partitionerIgnoreKeys;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, new ApiVersions(), Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Utils.propsToMap(properties), keySerializer, valueSerializer);
    }

    /**
     * KafkaProducer的核心构造函数，用于初始化生产者的所有组件
     * @param config 生产者配置
     * @param keySerializer 键序列化器
     * @param valueSerializer 值序列化器
     * @param metadata 元数据服务
     * @param kafkaClient Kafka网络客户端
     * @param interceptors 拦截器
     * @param apiVersions API版本信息
     * @param time 时间服务
     */
    @SuppressWarnings({"unchecked", "this-escape"})
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  ApiVersions apiVersions,
                  Time time) {
        try {
            // 保存生产者配置和时间服务实例
            this.producerConfig = config;
            this.time = time;

            // 获取事务ID，用于事务性生产者的标识
            // 如果配置了事务ID，表示这是一个事务性生产者
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);

            // 获取客户端ID，用于在日志和监控中标识此生产者实例
            this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);

            // 创建日志上下文，包含生产者的标识信息
            // 如果是事务性生产者，日志中会包含事务ID
            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            // 配置监控指标（metrics）
            // 1. 创建带有客户端ID的标签映射
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            // 2. 创建指标配置，包括：
            // - samples: 采样数
            // - timeWindow: 采样时间窗口
            // - recordLevel: 记录级别
            // - tags: 标签信息
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            // 3. 获取指标报告器列表，用于输出监控数据
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            // 4. 配置遥测报告器（如果启用）
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            // 5. 创建指标上下文，设置JMX前缀
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            // 6. 初始化指标系统
            this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            // 初始化生产者指标收集器
            this.producerMetrics = new KafkaProducerMetrics(metrics);

            // 配置分区器插件
            // 1. 从配置中获取分区器实例
            // 2. 使用Plugin包装分区器，以便收集相关指标
            this.partitionerPlugin = Plugin.wrapInstance(
                    config.getConfiguredInstance(
                        ProducerConfig.PARTITIONER_CLASS_CONFIG,
                        Partitioner.class,
                        Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)),
                    metrics,
                    ProducerConfig.PARTITIONER_CLASS_CONFIG);
            // 获取是否忽略消息key的配置，用于分区策略
            this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);

            // 获取重试相关的配置
            // retryBackoffMs: 重试之间的等待时间
            // retryBackoffMaxMs: 重试等待的最大时间
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);

            // 配置key的序列化器
            // 如果没有显式提供序列化器，则从配置中获取
            if (keySerializer == null) {
                keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
                // 配置序列化器，传入客户端ID，并标记这是key的序列化器
                keySerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                // 如果提供了序列化器，则忽略配置中的序列化器设置
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            }
            // 使用Plugin包装key序列化器，用于指标收集
            this.keySerializerPlugin = Plugin.wrapInstance(keySerializer, metrics, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);

            // 配置value的序列化器
            // 逻辑与key序列化器类似
            if (valueSerializer == null) {
                valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
                // 配置序列化器，传入客户端ID，并标记这是value的序列化器
                valueSerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            }
            // 使用Plugin包装value序列化器，用于指标收集
            this.valueSerializerPlugin = Plugin.wrapInstance(valueSerializer, metrics, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

            // 配置拦截器列表，拦截器可以在消息发送前后进行处理
            // 从配置中获取用户定义的拦截器类
            List<ProducerInterceptor<K, V>> interceptorList = ClientUtils.configuredInterceptors(config,
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            // 如果外部传入了拦截器就使用外部的，否则使用配置中的拦截器列表创建新的拦截器列表
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList, metrics);

            // 配置集群资源监听器，用于监控集群变化
            // 包含拦截器、指标报告器和序列化器的监听器
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    interceptorList,
                    reporters,
                    Arrays.asList(this.keySerializerPlugin.get(), this.valueSerializerPlugin.get()));

            // 获取最大请求大小配置，限制单个请求的大小
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            // 获取总内存大小配置，用于消息缓冲区
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            // 配置压缩类型（如GZIP、Snappy等）
            this.compression = configureCompression(config);

            // 获取最大阻塞时间配置，当缓冲区满时最多等待多长时间
            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            // 配置消息投递超时时间
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            // 设置API版本信息
            this.apiVersions = apiVersions;
            // 配置事务管理器，用于处理事务相关的操作
            this.transactionManager = configureTransactionState(config, logContext);

            // 配置自适应分区
            // 只有在没有自定义分区器的情况下才启用自适应分区
            boolean enableAdaptivePartitioning = partitionerPlugin.get() == null &&
                config.getBoolean(ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG);

            // 创建分区器配置，包含是否启用自适应分区和分区可用性超时时间
            RecordAccumulator.PartitionerConfig partitionerConfig = new RecordAccumulator.PartitionerConfig(
                enableAdaptivePartitioning,
                config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)
            );
            // As per Kafka producer configuration documentation batch.size may be set to 0 to explicitly disable
            // batching which in practice actually means using a batch size of 1.
            // 配置批次大小
            // 如果配置为0则使用1，表示禁用批处理
            int batchSize = Math.max(1, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG));

            // 创建消息累加器，用于缓存待发送的消息
            // 包含了批处理大小、压缩方式、延迟发送时间等配置
            this.accumulator = new RecordAccumulator(logContext,
                    batchSize,
                    compression,
                    lingerMs(config),
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    deliveryTimeoutMs,
                    partitionerConfig,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, batchSize, metrics, time, PRODUCER_METRIC_GROUP_NAME));

            // 解析并验证配置中的Kafka集群地址列表
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);

            // 初始化元数据服务
            // 如果外部传入了metadata实例就直接使用，否则创建新的实例
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                // 创建新的ProducerMetadata实例，用于管理集群元数据
                // 参数说明：
                // - retryBackoffMs: 重试等待时间
                // - retryBackoffMaxMs: 最大重试等待时间
                // - METADATA_MAX_AGE_CONFIG: 元数据最大有效期
                // - METADATA_MAX_IDLE_CONFIG: 元数据最大空闲时间
                // - logContext: 日志上下文
                // - clusterResourceListeners: 集群资源监听器列表
                // - Time.SYSTEM: 系统时间服务
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        retryBackoffMaxMs,
                        config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                        config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
                        logContext,
                        clusterResourceListeners,
                        Time.SYSTEM);
                // 使用配置的地址列表初始化元数据服务
                this.metadata.bootstrap(addresses);
            }

            // 创建错误度量传感器，用于监控错误情况
            this.errors = this.metrics.sensor("errors");

            // 创建消息发送器，负责实际的消息发送工作
            this.sender = newSender(logContext, kafkaClient, this.metadata);

            // 创建并启动I/O线程  格式：kafka-producer-network-thread | clientId
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();

            // 记录未使用的配置项
            config.logUnused();

            // 注册生产者信息到JMX，用于监控和管理
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());

            // 记录生产者启动完成的日志
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    KafkaProducer(ProducerConfig config,
                  LogContext logContext,
                  Metrics metrics,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  TransactionManager transactionManager,
                  Sender sender,
                  ProducerInterceptors<K, V> interceptors,
                  Partitioner partitioner,
                  Time time,
                  KafkaThread ioThread,
                  Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        this.producerConfig = config;
        this.time = time;
        this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        this.log = logContext.logger(KafkaProducer.class);
        this.metrics = metrics;
        this.producerMetrics = new KafkaProducerMetrics(metrics);
        this.partitionerPlugin = Plugin.wrapInstance(partitioner, metrics, ProducerConfig.PARTITIONER_CLASS_CONFIG);
        this.keySerializerPlugin = Plugin.wrapInstance(keySerializer, metrics, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        this.valueSerializerPlugin = Plugin.wrapInstance(valueSerializer, metrics, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        this.interceptors = interceptors;
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
        this.compression = configureCompression(config);
        this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
        this.apiVersions = new ApiVersions();
        this.transactionManager = transactionManager;
        this.accumulator = accumulator;
        this.errors = this.metrics.sensor("errors");
        this.metadata = metadata;
        this.sender = sender;
        this.ioThread = ioThread;
        this.clientTelemetryReporter = clientTelemetryReporter;
    }

    /**
     * 创建一个新的Sender实例，用于处理消息发送的核心组件
     * @param logContext 日志上下文
     * @param kafkaClient Kafka网络客户端
     * @param metadata 生产者元数据
     * @return 新的Sender实例
     */
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        // 获取每个连接允许的最大未完成请求数
        int maxInflightRequests = producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        // 获取请求超时时间配置
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        // 创建生产者指标注册表
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        // 创建限流时间传感器，用于监控限流情况
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        // 创建或使用提供的KafkaClient，用于网络通信
        KafkaClient client = kafkaClient != null ? kafkaClient : ClientUtils.createNetworkClient(producerConfig,
                this.metrics,
                "producer",
                logContext,
                apiVersions,
                time,
                maxInflightRequests,
                metadata,
                throttleTimeSensor,
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

        // 获取消息确认机制配置（acks）
        short acks = Short.parseShort(producerConfig.getString(ProducerConfig.ACKS_CONFIG));
        // 创建并返回新的Sender实例，它负责将消息转换为请求并发送到Kafka集群
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,  // 记录累加器，用于缓存待发送的消息
                maxInflightRequests == 1,  // 是否启用幂等性发送
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),  // 最大请求大小
                acks,  // 消息确认级别
                producerConfig.getInt(ProducerConfig.RETRIES_CONFIG),  // 重试次数
                metricsRegistry.senderMetrics,  // 发送者指标
                time,  // 时间工具
                requestTimeoutMs,  // 请求超时时间
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),  // 重试间隔时间
                this.transactionManager,  // 事务管理器
                apiVersions);  // API版本信息
    }

    /**
     * 配置生产者的消息压缩方式
     * 根据配置文件中指定的压缩类型（GZIP、LZ4、ZSTD等）创建对应的压缩器实例
     * 每种压缩类型都可以通过level参数来控制压缩级别，在压缩率和性能之间进行权衡
     *
     * @param config 生产者配置对象，包含压缩类型和压缩级别等配置信息
     * @return 返回配置好的Compression对象，用于消息压缩
     */
    private static Compression configureCompression(ProducerConfig config) {
        // 从配置中获取压缩类型名称并转换为CompressionType枚举
        CompressionType type = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        switch (type) {
            case GZIP: {
                // 配置GZIP压缩，可通过compression.gzip.level参数控制压缩级别
                return Compression.gzip()
                        .level(config.getInt(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                        .build();
            }
            case LZ4: {
                // 配置LZ4压缩，可通过compression.lz4.level参数控制压缩级别
                return Compression.lz4()
                        .level(config.getInt(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                        .build();
            }
            case ZSTD: {
                // 配置ZSTD压缩，可通过compression.zstd.level参数控制压缩级别
                return Compression.zstd()
                        .level(config.getInt(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                        .build();
            }
            default:
                // 对于其他压缩类型（如none或未知类型），使用默认配置创建压缩器
                return Compression.of(type).build();
        }
    }

    /**
     * 获取生产者的消息发送延迟时间配置
     * 该配置用于控制消息在发送前的等待时间，以便可以将多个消息打包在一起发送
     * 返回值不会超过Integer.MAX_VALUE
     */
    private static int lingerMs(ProducerConfig config) {
        return (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
    }

    /**
     * 配置消息投递超时时间
     * 该方法确保delivery.timeout.ms的值合理，必须大于等于linger.ms + request.timeout.ms
     * 如果用户显式设置了一个不合理的值，将抛出异常
     * 如果使用默认值且不合理，将自动调整为linger.ms + request.timeout.ms
     *
     * @param config 生产者配置
     * @param log 日志记录器
     * @return 经过验证和可能调整的投递超时时间（毫秒）
     * @throws ConfigException 当用户显式设置的delivery.timeout.ms值小于linger.ms + request.timeout.ms时
     */
    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        int lingerMs = lingerMs(config);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int lingerAndRequestTimeoutMs = (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE);

        if (deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerAndRequestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    /**
     * 配置生产者的事务状态
     * 当启用幂等性时，将创建TransactionManager实例
     * 如果设置了transactional.id，则自动启用幂等性并创建支持事务的生产者
     *
     * @param config 生产者配置
     * @param logContext 日志上下文
     * @return 事务管理器实例，如果未启用幂等性则返回null
     */
    private TransactionManager configureTransactionState(ProducerConfig config,
                                                         LogContext logContext) {
        TransactionManager transactionManager = null;

        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            final String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            final int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            final long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(
                logContext,
                transactionalId,
                transactionTimeoutMs,
                retryBackoffMs,
                apiVersions
            );

            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        } else {
            // ignore unretrieved configurations related to producer transaction
            config.ignore(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        }
        return transactionManager;
    }

    /**
     * 当配置了 {@code transactional.id} 时，在调用其他方法之前必须先调用此方法。
     * 此方法执行以下操作：
     * <ol>
     * <li>确保使用相同 {@code transactional.id} 的之前生产者实例的所有事务都已完成。
     *     如果之前的实例在事务进行中失败，该事务将被中止。如果最后一个事务已开始完成但尚未结束，
     *     此方法将等待其完成。</li>
     * <li>获取内部生产者ID和epoch值，用于生产者后续发送的所有事务消息。</li>
     * </ol>
     * 注意：如果事务状态在 {@code max.block.ms} 超时前无法初始化，此方法将抛出 {@link TimeoutException}。
     * 另外，如果方法被中断，将抛出 {@link InterruptException}。这两种情况下都可以安全重试，
     * 但一旦事务状态初始化成功，就不应再使用此方法。
     *
     * @throws IllegalStateException 如果未配置 {@code transactional.id}
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException 致命错误，表示broker不支持事务
     *         （即版本低于0.11.0.0）
     * @throws org.apache.kafka.common.errors.AuthorizationException 错误表明配置的transactional.id未获授权，
     *         或幂等性生产者ID不可用。详见异常信息。修复权限后用户可重试此方法。
     * @throws KafkaException 如果生产者遇到之前的致命错误或任何其他意外错误
     * @throws TimeoutException 如果初始化事务的时间超过了 <code>max.block.ms</code>
     * @throws InterruptException 如果线程在阻塞时被中断
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long now = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.initializeTransactions();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordInit(time.nanoseconds() - now);
        transactionManager.maybeUpdateTransactionV2Enabled(true);
    }

    /**
     * 开始一个新的事务。
     * 
     * 该方法用于开启一个新的事务，使得后续的所有生产操作都成为事务的一部分，直到调用commitTransaction()或abortTransaction()。
     * 
     * 使用场景：
     * 1. 需要将多个消息作为一个原子单元发送时
     * 2. 需要实现精确一次（exactly-once）语义时
     * 3. 需要跨多个分区/主题进行原子写入时
     * 
     * 前置条件：
     * 1. 生产者必须配置了transactional.id
     * 2. 必须已经调用过initTransactions()方法
     * 3. 当前没有正在进行的事务
     * 
     * 注意事项：
     * 1. 在一个事务完成（提交或中止）之前，不能开始新的事务
     * 2. 如果生产者实例被隔离（fenced），该方法将抛出ProducerFencedException
     * 3. 事务一旦开始，所有后续的发送操作都将是事务的一部分
     * 
     * @throws ProducerFencedException 如果生产者被隔离（即另一个具有相同transactional.id的生产者已经开始工作）
     * @throws IllegalStateException 如果生产者未配置事务功能，或者已经有一个进行中的事务
     * @throws KafkaException 如果在开始事务时发生其他错误
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long now = time.nanoseconds();
        //开始事务
        transactionManager.beginTransaction();
        //生产者指标记录事务开启检间隔？
        producerMetrics.recordBeginTxn(time.nanoseconds() - now);
    }

    /**
     * 将指定的偏移量列表发送到消费者组协调器，并将这些偏移量标记为当前事务的一部分。
     * 这些偏移量只有在事务成功提交后才会被视为已提交。已提交的偏移量应该是应用程序将要消费的下一条消息，
     * 即 {@code nextRecordToBeProcessed.offset()}（或 {@link ConsumerRecords#nextOffsets()}）。
     * 你还应该添加领导者纪元作为提交元数据，可以从 {@link ConsumerRecord#leaderEpoch()} 或 
     * {@link ConsumerRecords#nextOffsets()} 获取。
     * 
     * <p>
     * 此方法应在需要批量处理已消费和已生产的消息时使用，通常用于消费-转换-生产模式。因此，指定的
     * {@code groupMetadata} 应通过 {@link KafkaConsumer#groupMetadata()} 从使用的 
     * {@link KafkaConsumer consumer} 中提取，以利用消费者组元数据。这将提供比仅提供 
     * {@code consumerGroupId} 并传入 {@code new ConsumerGroupMetadata(consumerGroupId)} 
     * 更强的隔离性。但请注意，{@link KafkaConsumer#groupMetadata()} 返回的完整消费者组元数据
     * 需要 broker 版本在 2.5 或更高版本才能理解。
     *
     * <p>
     * 这是一个阻塞调用，会等待请求被消费者组协调器接收和确认；但这些偏移量直到事务本身通过
     * {@link #commitTransaction()} 调用成功提交后才会被视为已提交。
     *
     * <p>
     * 注意，消费者应该设置 {@code enable.auto.commit=false}，并且不应该手动提交偏移量
     * （通过 {@link KafkaConsumer#commitSync(Map) 同步} 或 
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) 异步} 提交）。
     * 如果生产者在 {@code max.block.ms} 过期前无法发送偏移量，此方法将抛出 {@link TimeoutException}。
     * 另外，如果被中断，它将抛出 {@link InterruptException}。
     *
     * @throws IllegalStateException 如果未配置 transactional.id 或未启动事务
     * @throws ProducerFencedException 致命错误，表示具有相同 transactional.id 的另一个生产者处于活动状态
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException 致命错误，表示 broker
     *         不支持事务（即版本低于 0.11.0.0）或 broker 不支持具有所有消费者组元数据的最新版本事务 API
     *         （即版本低于 2.5.0）
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException 致命错误，表示
     *         broker 上偏移量主题使用的消息格式不支持事务
     * @throws org.apache.kafka.common.errors.AuthorizationException 致命错误，表示配置的
     *         transactional.id 或消费者组 id 未获得授权
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交失败且无法重试
     *         （例如，如果消费者已被踢出组）。用户应通过中止事务来处理这种情况
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果此生产者实例由于组元数据中
     *         消费者实例 id 配置错误而被 broker 隔离
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException 如果生产者尝试使用旧的
     *         epoch 向分区领导者生产。有关详细信息，请参见异常
     * @throws KafkaException 如果生产者遇到先前的致命或可中止错误，或任何其他意外错误
     * @throws TimeoutException 如果发送偏移量所花费的时间超过了 <code>max.block.ms</code>
     * @throws InterruptException 如果线程在阻塞时被中断
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        throwIfInvalidGroupMetadata(groupMetadata);
        throwIfNoTransactionManager();
        throwIfProducerClosed();

        if (!offsets.isEmpty()) {
            // 记录发送偏移量操作的开始时间，用于性能监控
            long start = time.nanoseconds();
            // 调用事务管理器将消费者偏移量添加到当前事务中
            TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
            // 唤醒发送线程，确保请求能够立即被处理
            sender.wakeup();
            // 等待请求完成，最长等待时间由maxBlockTimeMs指定
            result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
            // 记录发送偏移量操作的执行时间，用于监控和性能分析
            producerMetrics.recordSendOffsets(time.nanoseconds() - start);
        }
    }

    /**
     * 提交当前正在进行的事务。本方法会在实际提交事务之前先刷新所有未发送的记录。
     * <p>
     * 如果事务中的任何 {@link #send(ProducerRecord)} 调用遇到不可恢复的错误，本方法会立即抛出最后收到的异常，
     * 且事务不会被提交。因此，事务中的所有 {@link #send(ProducerRecord)} 调用都必须成功，该方法才能成功执行。
     * <p>
     * 如果事务成功提交且本方法没有抛出异常，则保证事务中所有记录的 {@link Callback 回调函数} 都已被调用并完成。
     * 注意：回调函数抛出的异常会被忽略，生产者会继续提交事务。
     * <p>
     * 需要注意的是，如果事务无法在 {@code max.block.ms} 过期之前完成提交，本方法会抛出 {@link TimeoutException}，
     * 但这并不意味着请求没有到达broker。实际上，这只表示我们无法及时获得确认响应，因此如何处理超时情况取决于应用程序的逻辑。
     * 此外，如果线程被中断，会抛出 {@link InterruptException}。
     * 在这两种情况下重试都是安全的，但由于提交可能已经在进行中，此时不可能尝试其他操作（如abortTransaction）。
     * 如果不进行重试，唯一的选择就是关闭生产者。
     *
     * @throws IllegalStateException 如果未配置transactional.id或未启动事务
     * @throws ProducerFencedException 致命错误，表示另一个具有相同transactional.id的生产者处于活动状态
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException 致命错误，表示broker不支持事务
     *         （即版本低于0.11.0.0）
     * @throws org.apache.kafka.common.errors.AuthorizationException 致命错误，表示配置的transactional.id未被授权
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException 如果生产者尝试使用旧的epoch向分区leader发送消息
     * @throws KafkaException 如果生产者遇到之前的致命或可中止错误，或任何其他意外错误
     * @throws TimeoutException 如果提交事务所需时间超过了 <code>max.block.ms</code>
     * @throws InterruptException 如果线程在阻塞时被中断
     */
    public void commitTransaction() throws ProducerFencedException {
        // 检查事务管理器是否存在，如果不存在则抛出异常
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭，如果已关闭则抛出异常
        throwIfProducerClosed();
        // 记录事务提交开始时间，用于性能监控
        long commitStart = time.nanoseconds();
        // 调用事务管理器开始提交事务，返回事务请求结果对象
        TransactionalRequestResult result = transactionManager.beginCommit();
        // 唤醒发送线程，处理事务提交请求
        sender.wakeup();
        // 等待事务提交完成，如果超过最大阻塞时间则抛出超时异常
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        // 记录事务提交的耗时指标
        producerMetrics.recordCommitTxn(time.nanoseconds() - commitStart);
    }

    /**
     * 中止当前正在进行的事务。当调用此方法时，所有未刷新的生产消息都将被中止。
     * 如果之前的任何 {@link #send(ProducerRecord)} 调用失败并抛出了
     * {@link ProducerFencedException} 或 {@link org.apache.kafka.common.errors.AuthorizationException} 异常，
     * 此方法将立即抛出异常。
     * <p>
     * 注意：如果事务无法在 {@code max.block.ms} 过期之前中止，此方法将抛出 {@link TimeoutException}，
     * 但这并不意味着请求实际上没有到达代理服务器。事实上，这仅表示我们无法及时获得确认响应，
     * 因此如何处理超时取决于应用程序的逻辑。此外，如果线程被中断，它将抛出 {@link InterruptException}。
     * 在这两种情况下重试都是安全的，但由于中止操作可能已经在进行中，无法尝试其他操作（如 {@link #commitTransaction}）。
     * 如果不重试，唯一的选择就是关闭生产者。
     *
     * @throws IllegalStateException 如果未配置 transactional.id 或未启动事务
     * @throws ProducerFencedException 致命错误，表示具有相同 transactional.id 的另一个生产者处于活动状态
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException 如果生产者尝试使用旧的 epoch 向分区领导者生产消息
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException 致命错误，表示代理服务器不支持事务（即版本低于 0.11.0.0）
     * @throws org.apache.kafka.common.errors.AuthorizationException 致命错误，表示配置的 transactional.id 未获得授权
     * @throws KafkaException 如果生产者遇到先前的致命错误或任何其他意外错误
     * @throws TimeoutException 如果中止事务所花费的时间超过了 <code>max.block.ms</code>
     * @throws InterruptException 如果线程在阻塞时被中断
     */
    public void abortTransaction() throws ProducerFencedException {
        // 检查事务管理器是否存在，如果不存在则抛出异常
        throwIfNoTransactionManager();
        // 检查生产者是否已关闭，如果已关闭则抛出异常
        throwIfProducerClosed();
        // 记录中止事务的日志信息
        log.info("Aborting incomplete transaction");
        // 记录中止事务的开始时间（纳秒级）
        long abortStart = time.nanoseconds();
        // 调用事务管理器开始中止事务操作
        TransactionalRequestResult result = transactionManager.beginAbort();
        // 唤醒发送线程，确保中止请求能够立即被处理
        sender.wakeup();
        // 等待中止操作完成，最长等待时间由 maxBlockTimeMs 指定
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        // 记录事务中止操作的性能指标（耗时）
        producerMetrics.recordAbortTxn(time.nanoseconds() - abortStart);
    }

    /**
     * 异步发送一条消息记录到指定的主题。
     * 这是一个不带回调函数的简化版本，等同于调用 <code>send(record, null)</code>。
     * <p>
     * 该方法具有以下特点：
     * <ul>
     * <li>异步执行：调用后立即返回，不会阻塞等待消息发送完成</li>
     * <li>返回Future对象：可以通过Future获取发送结果，但不会主动通知</li>
     * <li>自动重试：发送失败时会根据配置自动重试</li>
     * <li>线程安全：可以从多个线程同时调用</li>
     * </ul>
     * 更多详细信息请参见 {@link #send(ProducerRecord, Callback)} 方法的文档。
     *
     * @param record 要发送的消息记录，包含目标主题、分区（可选）、时间戳（可选）、键（可选）和值
     * @return 返回一个Future对象，可用于获取发送结果的元数据。元数据包含消息的主题、分区、偏移量等信息
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        // 调用带回调参数的send方法，传入null作为回调函数
        // 这意味着发送完成后不会收到通知，需要通过返回的Future对象来获取结果
        return send(record, null);
    }

    /**
     * 异步发送消息记录到主题，并在发送确认后调用提供的回调函数。
     * <p>
     * 发送操作是异步的，该方法在消息记录被存储到发送缓冲区后会立即返回（除了以下罕见情况）。
     * 这允许并行发送多条消息而无需在每条消息后都阻塞等待响应。
     * 可能会阻塞的情况：
     * 1) 当客户端首次向指定主题发送消息时。在这种情况下，如果Kafka集群不可达，将阻塞最多{@code max.block.ms}毫秒；
     * 2) 当缓冲池没有空闲缓冲区时，在分配缓冲区时会阻塞。
     * <p>
     * 发送结果是一个{@link RecordMetadata}对象，指定了消息发送到的分区、分配的偏移量和时间戳。
     * 如果生产者配置acks=0，则{@link RecordMetadata}的offset将为-1，因为生产者不会等待来自broker的确认。
     * 时间戳的处理有两种情况：
     * - 如果主题使用{@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}，
     *   时间戳将是用户提供的时间戳或记录发送时间（如果用户未指定）。
     * - 如果主题使用{@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}，
     *   时间戳将是消息追加时Kafka broker的本地时间。
     * <p>
     * 由于send调用是异步的，它返回一个{@link java.util.concurrent.Future Future}对象，用于获取{@link RecordMetadata}。
     * 调用future的{@link java.util.concurrent.Future#get() get()}方法将阻塞直到请求完成，然后返回消息的元数据或抛出发送过程中发生的异常。
     * <p>
     * 如果想模拟同步调用，可以立即调用<code>get()</code>方法：
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * 完全非阻塞的用法可以使用{@link Callback}参数提供一个回调函数，该函数将在请求完成时被调用：
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * 发送到同一分区的消息的回调函数保证按顺序执行。例如，在以下示例中，
     * <code>callback1</code>保证在<code>callback2</code>之前执行：
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * 在事务中使用时，无需定义回调或检查future的结果来检测<code>send</code>的错误。
     * 如果任何send调用遇到不可恢复的错误，最终的{@link #commitTransaction()}调用将失败并抛出最后一个失败send的异常。
     * 当这种情况发生时，应用程序应调用{@link #abortTransaction()}来重置状态并继续发送数据。
     * </p>
     * <p>
     * 某些事务性发送错误无法通过调用{@link #abortTransaction()}来解决。特别是，
     * 如果事务性发送以{@link ProducerFencedException}、{@link org.apache.kafka.common.errors.OutOfOrderSequenceException}、
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}或
     * {@link org.apache.kafka.common.errors.AuthorizationException}结束，
     * 唯一的选择就是调用{@link #close()}。
     * 致命错误会导致生产者进入失效状态，未来的API调用将继续抛出包装在新的{@link KafkaException}中的相同底层错误。
     * </p>
     * <p>
     * 当启用幂等性但未配置<code>transactional.id</code>时情况类似。
     * 在这种情况下，{@link org.apache.kafka.common.errors.UnsupportedVersionException}和
     * {@link org.apache.kafka.common.errors.AuthorizationException}被视为致命错误。
     * 但是，不需要处理{@link ProducerFencedException}。
     * 此外，在收到{@link org.apache.kafka.common.errors.OutOfOrderSequenceException}后可以继续发送，
     * 但这样做可能导致待处理消息的乱序传递。为确保正确的顺序，应关闭生产者并创建新实例。
     * </p>
     * <p>
     * 如果目标主题的消息格式未升级到0.11.0.0，幂等和事务性生产请求将失败，
     * 并出现{@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}错误。
     * 如果在事务中遇到此错误，可以中止并继续。但请注意，在主题升级之前，
     * 发送到同一主题的后续请求将继续收到相同的异常。
     * </p>
     * <p>
     * 注意，回调通常在生产者的I/O线程中执行，因此应该相当快，
     * 否则会延迟其他线程的消息发送。如果要执行阻塞或计算密集型的回调，
     * 建议在回调体中使用自己的{@link java.util.concurrent.Executor}来并行处理。
     *
     * @param record 要发送的消息记录
     * @param callback 当服务器确认记录后要执行的用户提供的回调（null表示无回调）
     *
     * @throws IllegalStateException 如果配置了transactional.id但未启动事务，或在生产者关闭后调用send
     * @throws InterruptException 如果线程在阻塞时被中断
     * @throws SerializationException 如果key或value对于配置的序列化器而言不是有效对象
     * @throws KafkaException 如果发生不属于公共API异常的Kafka相关错误
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // 拦截记录，可能会被修改；此方法不会抛出异常
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        // 执行实际的发送操作
        return doSend(interceptedRecord, callback);
    }

    /**
     * 验证当前生产者实例是否已关闭。如果生产者已经关闭，则抛出IllegalStateException异常。
     * 
     * 该方法在执行生产者操作前进行状态检查，确保生产者处于可用状态。检查包括两个条件：
     * 1. sender对象不为null - sender是负责实际消息发送的后台线程，为null表示生产者未正确初始化或已被关闭
     * 2. sender处于运行状态 - 通过isRunning()方法检查sender线程是否正在运行
     * 
     * 当生产者关闭后，所有的生产者操作（如发送消息）都将失败，这是为了防止：
     * - 向已关闭的生产者发送新消息
     * - 访问已释放的资源
     * - 产生不一致的状态
     * 
     * @throws IllegalStateException 如果生产者已经关闭，表示当前操作无法执行
     */
    private void throwIfProducerClosed() {
        if (sender == null || !sender.isRunning())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * 异步发送消息记录到指定主题的实现方法。
     * 该方法实现了消息发送的完整流程，包括：
     * 1. 获取集群元数据
     * 2. 序列化消息的key和value
     * 3. 计算目标分区
     * 4. 将消息添加到累加器中
     * 5. 处理事务相关逻辑
     * 6. 异常处理
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        // 创建追加回调，用于处理以下功能：
        // - 在发送完成时调用拦截器和用户回调
        // - 记住在RecordAccumulator.append中计算的分区
        AppendCallbacks appendCallbacks = new AppendCallbacks(callback, this.interceptors, record);

        try {
            // 检查生产者是否已关闭
            throwIfProducerClosed();
            // 首先确保主题的元数据可用
            long nowMs = time.milliseconds();  // 获取当前时间戳
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                // 等待获取主题元数据，如果超时则抛出异常
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            // 更新当前时间并计算剩余等待时间
            nowMs += clusterAndWaitTime.waitedOnMetadataMs;
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            
            // 序列化消息的key
            byte[] serializedKey;
            try {
                serializedKey = keySerializerPlugin.get().serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            
            // 序列化消息的value
            byte[] serializedValue;
            try {
                serializedValue = valueSerializerPlugin.get().serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }

            // 计算消息的目标分区
            // 注意：这里返回的可能是UNKNOWN_PARTITION，此时RecordAccumulator会使用内置逻辑选择分区
            // （可能考虑broker负载、每个分区的数据量等因素）
            int partition = partition(record, serializedKey, serializedValue, cluster);

            // 设置消息头为只读并获取头部数组
            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            // 估算序列化后消息的大小上限
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(RecordBatch.CURRENT_MAGIC_VALUE,
                    compression.type(), serializedKey, serializedValue, headers);
            // 确保消息大小在有效范围内
            ensureValidRecordSize(serializedSize);
            // 获取消息时间戳，如果未指定则使用当前时间
            long timestamp = record.timestamp() == null ? nowMs : record.timestamp();

            // 将消息追加到累加器中
            // 注意：实际的分区可能在这里计算，可以通过appendCallbacks.topicPartition获取
            RecordAccumulator.RecordAppendResult result = accumulator.append(record.topic(), partition, timestamp, serializedKey,
                    serializedValue, headers, appendCallbacks, remainingWaitMs, nowMs, cluster);
            // 确保分区已经确定
            assert appendCallbacks.getPartition() != RecordMetadata.UNKNOWN_PARTITION;

            // 如果正在进行事务，将分区添加到事务中
            // 这必须在消息成功追加到累加器之后进行，因为之前分区可能未知
            // 注意：在分区被添加到事务之前，Sender不会从累加器中取出批次
            if (transactionManager != null) {
                transactionManager.maybeAddPartition(appendCallbacks.topicPartition());
            }

            // 如果批次已满或创建了新批次，唤醒发送线程
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), appendCallbacks.getPartition());
                this.sender.wakeup();
            }
            return result.future;
            
        // 异常处理部分
        // 对于API异常，将其包装在Future中返回
        // 对于其他异常，直接抛出
        } catch (ApiException e) {
            // 记录调试日志
            log.debug("Exception occurred during message send:", e);
            // 如果设置了回调，则调用回调通知发送失败
            if (callback != null) {
                TopicPartition tp = appendCallbacks.topicPartition();
                RecordMetadata nullMetadata = new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
                callback.onCompletion(nullMetadata, e);
            }
            // 记录错误并通知拦截器
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            // 如果在事务中，可能需要转换到错误状态
            if (transactionManager != null) {
                transactionManager.maybeTransitionToErrorState(e);
            }
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            // 处理中断异常
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw new InterruptException(e);
        } catch (KafkaException e) {
            // 处理Kafka异常
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        } catch (Exception e) {
            // 处理其他所有异常
            // 通知拦截器发生错误，因为onSend是在方法中最先调用的
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     * @param nowMs The current time in ms
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs) throws InterruptedException {
        Cluster cluster = metadata.fetch();

        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

        // add topic to metadata topic list if it is not there already and reset expiry
        metadata.add(topic, nowMs);

        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        long remainingWaitMs = maxWaitMs;
        long elapsed = 0;
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        long nowNanos = time.nanoseconds();
        do {
            if (partition != null) {
                log.trace("Requesting metadata update for partition {} of topic {}.", partition, topic);
            } else {
                log.trace("Requesting metadata update for topic {}.", topic);
            }
            metadata.add(topic, nowMs + elapsed);
            int version = metadata.requestUpdateForTopic(topic);
            sender.wakeup();
            try {
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                final String errorMessage = getErrorMessage(partitionsCount, topic, partition, maxWaitMs);
                if (metadata.getError(topic) != null) {
                    throw new TimeoutException(errorMessage, metadata.getError(topic).exception());
                }
                throw new TimeoutException(errorMessage);
            }
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - nowMs;
            if (elapsed >= maxWaitMs) {
                final String errorMessage = getErrorMessage(partitionsCount, topic, partition, maxWaitMs);
                if (metadata.getError(topic) != null && metadata.getError(topic).exception() instanceof RetriableException) {
                    throw new TimeoutException(errorMessage, metadata.getError(topic).exception());
                }
                throw new TimeoutException(errorMessage);
            }
            metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount));

        producerMetrics.recordMetadataWait(time.nanoseconds() - nowNanos);

        return new ClusterAndWaitTime(cluster, elapsed);
    }

    private String getErrorMessage(Integer partitionsCount, String topic, Integer partition, long maxWaitMs) {
        return partitionsCount == null ?
            String.format("Topic %s not present in metadata after %d ms.",
                topic, maxWaitMs) :
            String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.",
                partition, topic, partitionsCount, maxWaitMs);
    }
    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than " + maxRequestSize + ", which is the value of the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        if (size > totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>
     * and callbacks passed to {@link #send(ProducerRecord,Callback)} have been called).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commitSync();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * </p>
     * <p>
     * <b>Important:</b> This method must not be called from within the callback provided to
     * {@link #send(ProducerRecord, Callback)}. Invoking <code>flush()</code> in this context will result in a
     * {@link KafkaException} being thrown, as it will cause a deadlock.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws KafkaException If the method is invoked inside a {@link #send(ProducerRecord, Callback)} callback
     */
    @Override
    public void flush() {
        if (Thread.currentThread() == this.ioThread) {
            log.error("KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.");
            throw new KafkaException("KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.");
        }

        log.trace("Flushing accumulated records in producer.");

        long start = time.nanoseconds();
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        } finally {
            producerMetrics.recordFlush(time.nanoseconds() - start);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, time.milliseconds(), maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }


    /**
     * Add the provided application metric for subscription.
     * This metric will be added to this client's metrics
     * that are available for subscription and sent as
     * telemetry data to the broker.
     * The provided metric must map to an OTLP metric data point
     * type in the OpenTelemetry v1 metrics protobuf message types.
     * Specifically, the metric should be one of the following:
     * <ul>
     *  <li>
     *     `Sum`: Monotonic total count meter (Counter). Suitable for metrics like total number of X, e.g., total bytes sent.
     *  </li>
     *  <li>
     *     `Gauge`: Non-monotonic current value meter (UpDownCounter). Suitable for metrics like current value of Y, e.g., current queue count.
     *  </li>
     * </ul>
     * Metrics not matching these types are silently ignored.
     * Executing this method for a previously registered metric is a benign operation and results in updating that metrics entry.
     *
     * @param metric The application metric to register
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        }  else {
            log.debug("Skipping registration for metric {}. Existing producer metrics cannot be overwritten.", metric.metricName());
        }
    }

    /**
     * Remove the provided application metric for subscription.
     * This metric is removed from this client's metrics
     * and will not be available for subscription any longer.
     * Executing this method with a metric that has not been registered is a
     * benign operation and does not result in any action taken (no-op).
     *
     * @param metric The application metric to remove
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        } else {
            log.debug("Skipping unregistration for metric {}. Existing producer metrics cannot be removed.", metric.metricName());
        }
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the producer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ProducerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for producer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        producer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                               is set to `{@code false}`.
     * @return The client's assigned instance id used for metrics collection.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ProducerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately. It will also abort the ongoing transaction if it's not
     * already completing.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // Try to close gracefully.
                final Timer closeTimer = time.timer(timeout);
                clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
                closeTimer.update();

                if (this.sender != null) {
                    this.sender.initiateClose();
                    closeTimer.update();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(closeTimer.remainingMs());
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    } finally {
                        closeTimer.update();
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        Utils.closeQuietly(interceptors, "producer interceptors", firstException);
        Utils.closeQuietly(producerMetrics, "producer metrics wrapper", firstException);
        Utils.closeQuietly(metrics, "producer metrics", firstException);
        Utils.closeQuietly(keySerializerPlugin, "producer keySerializer", firstException);
        Utils.closeQuietly(valueSerializerPlugin, "producer valueSerializer", firstException);
        Utils.closeQuietly(partitionerPlugin, "producer partitioner", firstException);
        clientTelemetryReporter.ifPresent(reporter -> Utils.closeQuietly(reporter, "producer telemetry reporter", firstException));
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        log.debug("Kafka producer has been closed");
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * if custom partitioner is specified, call it to compute partition
     * otherwise try to calculate partition based on key.
     * If there is no key or key should be ignored return
     * RecordMetadata.UNKNOWN_PARTITION to indicate any partition
     * can be used (the partition is then calculated by built-in
     * partitioning logic).
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        if (record.partition() != null)
            return record.partition();

        if (partitionerPlugin.get() != null) {
            int customPartition = partitionerPlugin.get().partition(
                record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
            if (customPartition < 0) {
                throw new IllegalArgumentException(String.format(
                    "The partitioner generated an invalid partition number: %d. Partition number should always be non-negative.", customPartition));
            }
            return customPartition;
        }

        if (serializedKey != null && !partitionerIgnoreKeys) {
            // hash the keyBytes to choose a partition
            return BuiltInPartitioner.partitionForKey(serializedKey, cluster.partitionsForTopic(record.topic()).size());
        } else {
            return RecordMetadata.UNKNOWN_PARTITION;
        }
    }

    /**
     * 验证消费者组元数据的有效性
     * 
     * 此方法用于验证传入的消费者组元数据是否有效。主要进行两项检查：
     * 1. 确保元数据对象不为空
     * 2. 当generationId大于0时，确保memberId不是未知的（UNKNOWN_MEMBER_ID）
     * 
     * @param groupMetadata 待验证的消费者组元数据
     * @throws IllegalArgumentException 当元数据无效时抛出，可能的情况：
     *                                  - 元数据对象为null
     *                                  - generationId > 0但memberId未知
     */
    private void throwIfInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        if (groupMetadata == null) {
            throw new IllegalArgumentException("Consumer group metadata could not be null");
        } else if (groupMetadata.generationId() > 0
            && JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(groupMetadata.memberId())) {
            throw new IllegalArgumentException("Passed in group metadata " + groupMetadata + " has generationId > 0 but the member.id is unknown");
        }
    }

    /**
     * 检查事务管理器是否已初始化
     * 
     * 在执行事务相关操作前，此方法会检查事务管理器（TransactionManager）是否存在。
     * 如果要使用事务功能，必须通过配置transactional.id来启用事务支持。
     * 
     * @throws IllegalStateException 当尝试使用事务功能但事务管理器未初始化时抛出
     */
    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    /**
     * 获取clientId
     * @return
     */
    String getClientId() {
        return clientId;
    }

    // Visible for testing
    TransactionManager getTransactionManager() {
        return transactionManager;
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * Callbacks that are called by the RecordAccumulator append functions:
     *  - user callback
     *  - interceptor callbacks
     *  - partition callback
     */
    private class AppendCallbacks implements RecordAccumulator.AppendCallbacks {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final String topic;
        private final Integer recordPartition;
        private final String recordLogString;
        private volatile int partition = RecordMetadata.UNKNOWN_PARTITION;
        private volatile TopicPartition topicPartition;

        private AppendCallbacks(Callback userCallback, ProducerInterceptors<K, V> interceptors, ProducerRecord<K, V> record) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            // Extract record info as we don't want to keep a reference to the record during
            // whole lifetime of the batch.
            // We don't want to have an NPE here, because the interceptors would not be notified (see .doSend).
            topic = record != null ? record.topic() : null;
            recordPartition = record != null ? record.partition() : null;
            recordLogString = log.isTraceEnabled() && record != null ? record.toString() : "";
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata == null) {
                metadata = new RecordMetadata(topicPartition(), -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
            }
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }

        @Override
        public void setPartition(int partition) {
            assert partition != RecordMetadata.UNKNOWN_PARTITION;
            this.partition = partition;

            if (log.isTraceEnabled()) {
                // Log the message here, because we don't know the partition before that.
                log.trace("Attempting to append record {} with callback {} to topic {} partition {}", recordLogString, userCallback, topic, partition);
            }
        }

        public int getPartition() {
            return partition;
        }

        public TopicPartition topicPartition() {
            if (topicPartition == null && topic != null) {
                if (partition != RecordMetadata.UNKNOWN_PARTITION)
                    topicPartition = new TopicPartition(topic, partition);
                else if (recordPartition != null)
                    topicPartition = new TopicPartition(topic, recordPartition);
                else
                    topicPartition = new TopicPartition(topic, RecordMetadata.UNKNOWN_PARTITION);
            }
            return topicPartition;
        }
    }
}
