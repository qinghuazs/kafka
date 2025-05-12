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
package kafka.log.remote;

import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import kafka.server.DelayedRemoteListOffsets;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.internals.SecurityManagerCompatibility;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.StopPartition;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManager;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManagerConfig;
import org.apache.kafka.server.log.remote.quota.RLMQuotaMetrics;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.CustomMetadataSizeLimitExceededException;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.purgatory.TopicPartitionOperationKey;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.internals.log.RemoteIndexCache;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.internals.log.RemoteStorageThreadPool;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.server.config.ServerLogConfigs.LOG_DIR_CONFIG;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.quota.RLMQuotaManagerConfig.INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC;

/**
 * 该类负责以下功能:
 * - 初始化远程存储管理器(RemoteStorageManager)和远程日志元数据管理器(RemoteLogMetadataManager)实例
 * - 接收并处理领导者和追随者副本事件以及分区停止事件
 * - 提供API用于获取远程日志段的索引和元数据
 * - 将日志段复制到远程存储
 * - 根据保留大小或保留时间清理过期的日志段
 */
public class RemoteLogManager implements Closeable {

    // 日志记录器
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogManager.class);
    // 远程日志读取器线程名称模式
    private static final String REMOTE_LOG_READER_THREAD_NAME_PATTERN = "remote-log-reader-%d";
    // 远程日志管理器配置
    private final RemoteLogManagerConfig rlmConfig;
    // broker ID
    private final int brokerId;
    // 日志目录路径
    private final String logDir;
    // 时间实例
    private final Time time;
    // 获取指定主题分区的统一日志实例的函数
    private final Function<TopicPartition, Optional<UnifiedLog>> fetchLog;
    // 更新远程日志起始偏移量的函数
    private final BiConsumer<TopicPartition, Long> updateRemoteLogStartOffset;
    // broker主题统计信息
    private final BrokerTopicStats brokerTopicStats;
    // 度量指标
    private final Metrics metrics;

    // 远程存储管理器，负责实际的日志段存储操作
    private final RemoteStorageManager remoteLogStorageManager;

    // 远程日志元数据管理器，负责管理远程日志段的元数据
    private final RemoteLogMetadataManager remoteLogMetadataManager;

    // 复制配额管理器锁，用于控制复制操作的速率
    private final ReentrantLock copyQuotaManagerLock = new ReentrantLock(true);
    // 复制配额管理器条件变量
    private final Condition copyQuotaManagerLockCondition = copyQuotaManagerLock.newCondition();
    // 复制操作的配额管理器
    private final RLMQuotaManager rlmCopyQuotaManager;
    // 获取操作的配额管理器
    private final RLMQuotaManager rlmFetchQuotaManager;
    // 获取操作限流时间传感器
    private final Sensor fetchThrottleTimeSensor;
    // 复制操作限流时间传感器
    private final Sensor copyThrottleTimeSensor;

    // 远程索引缓存，用于缓存远程日志段的索引文件
    private final RemoteIndexCache indexCache;
    // 远程存储读取线程池，用于并发读取远程存储的数据
    private final RemoteStorageThreadPool remoteStorageReaderThreadPool;
    // 日志段复制线程池
    private final RLMScheduledThreadPool rlmCopyThreadPool;
    // 过期日志段清理线程池
    private final RLMScheduledThreadPool rlmExpirationThreadPool;
    // 追随者操作线程池
    private final RLMScheduledThreadPool followerThreadPool;

    // 任务执行延迟时间(毫秒)
    private final long delayInMs;

    // 领导者副本的日志段复制任务映射表
    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderCopyRLMTasks = new ConcurrentHashMap<>();
    // 领导者副本的日志段过期清理任务映射表
    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderExpirationRLMTasks = new ConcurrentHashMap<>();
    // 追随者副本的远程日志管理任务映射表
    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> followerRLMTasks = new ConcurrentHashMap<>();
    // 正在复制的日志段ID集合
    private final Set<RemoteLogSegmentId> segmentIdsBeingCopied = ConcurrentHashMap.newKeySet();

    // 领导者变更时接收到的主题ID映射表，在分区停止时清除
    private final ConcurrentMap<TopicPartition, Uuid> topicIdByPartitionMap = new ConcurrentHashMap<>();
    // 集群ID
    private final String clusterId;
    // 度量指标组
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    // 远程日志元数据管理器连接的端点
    private Optional<Endpoint> endpoint = Optional.empty();
    // 是否已关闭标志
    private boolean closed = false;

    // 远程日志管理器是否已配置标志
    private volatile boolean remoteLogManagerConfigured = false;
    // 远程读取操作计时器
    private final Timer remoteReadTimer;
    // 延迟远程列表偏移量操作清理器
    private volatile DelayedOperationPurgatory<DelayedRemoteListOffsets> delayedRemoteListOffsetsPurgatory;

    /**
     * 使用给定参数创建RemoteLogManager实例
     *
     * @param rlmConfig broker级别的远程日志子系统(分层存储)配置
     * @param brokerId  当前broker的ID
     * @param logDir    Kafka日志段的目录路径
     * @param time      时间实例
     * @param clusterId 集群ID
     * @param fetchLog  获取指定主题的UnifiedLog实例的函数
     * @param updateRemoteLogStartOffset 更新指定主题分区的日志起始偏移量的函数
     * @param brokerTopicStats 用于更新相关指标的BrokerTopicStats实例
     * @param metrics  指标实例
     */
    @SuppressWarnings({"this-escape"})
    public RemoteLogManager(RemoteLogManagerConfig rlmConfig,
                            int brokerId,
                            String logDir,
                            String clusterId,
                            Time time,
                            Function<TopicPartition, Optional<UnifiedLog>> fetchLog,
                            BiConsumer<TopicPartition, Long> updateRemoteLogStartOffset,
                            BrokerTopicStats brokerTopicStats,
                            Metrics metrics) throws IOException {
        // 初始化基本配置和参数
        this.rlmConfig = rlmConfig;
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.clusterId = clusterId;
        this.time = time;
        this.fetchLog = fetchLog;
        this.updateRemoteLogStartOffset = updateRemoteLogStartOffset;
        this.brokerTopicStats = brokerTopicStats;
        this.metrics = metrics;

        // 创建远程存储管理器和元数据管理器
        remoteLogStorageManager = createRemoteStorageManager();
        remoteLogMetadataManager = createRemoteLogMetadataManager();
        // 创建复制和获取操作的配额管理器
        rlmCopyQuotaManager = createRLMCopyQuotaManager();
        rlmFetchQuotaManager = createRLMFetchQuotaManager();

        // 创建获取和复制操作的限流时间传感器
        fetchThrottleTimeSensor = new RLMQuotaMetrics(metrics, "remote-fetch-throttle-time", RemoteLogManager.class.getSimpleName(),
            "The %s time in millis remote fetches was throttled by a broker", INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS).sensor();
        copyThrottleTimeSensor = new RLMQuotaMetrics(metrics, "remote-copy-throttle-time", RemoteLogManager.class.getSimpleName(),
            "The %s time in millis remote copies was throttled by a broker", INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS).sensor();

        // 初始化远程索引缓存和任务执行延迟时间
        indexCache = new RemoteIndexCache(rlmConfig.remoteLogIndexFileCacheTotalSizeBytes(), remoteLogStorageManager, logDir);
        delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs();
        
        // 创建各种线程池
        rlmCopyThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerCopierThreadPoolSize(),
            "RLMCopyThreadPool", "kafka-rlm-copy-thread-pool-%d");
        rlmExpirationThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerExpirationThreadPoolSize(),
            "RLMExpirationThreadPool", "kafka-rlm-expiration-thread-pool-%d");
        followerThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerThreadPoolSize(),
            "RLMFollowerScheduledThreadPool", "kafka-rlm-follower-thread-pool-%d");

        // 注册度量指标
        metricsGroup.newGauge(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC, rlmCopyThreadPool::getIdlePercent);
        remoteReadTimer = metricsGroup.newTimer(REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC,
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

        // 创建远程存储读取线程池
        remoteStorageReaderThreadPool = new RemoteStorageThreadPool(
                REMOTE_LOG_READER_THREAD_NAME_PATTERN,
                rlmConfig.remoteLogReaderThreads(),
                rlmConfig.remoteLogReaderMaxPendingTasks()
        );
    }

    /**
     * 设置延迟远程列表偏移量操作的清理器
     */
    public void setDelayedOperationPurgatory(DelayedOperationPurgatory<DelayedRemoteListOffsets> delayedRemoteListOffsetsPurgatory) {
        this.delayedRemoteListOffsetsPurgatory = delayedRemoteListOffsetsPurgatory;
    }

    /**
     * 调整远程日志索引文件缓存的大小
     */
    public void resizeCacheSize(long remoteLogIndexFileCacheSize) {
        indexCache.resizeCacheSize(remoteLogIndexFileCacheSize);
    }

    /**
     * 更新远程复制操作的配额(字节/秒)
     */
    public void updateCopyQuota(long quota) {
        LOGGER.info("Updating remote copy quota to {} bytes per second", quota);
        rlmCopyQuotaManager.updateQuota(new Quota(quota, true));
    }

    /**
     * 更新远程获取操作的配额(字节/秒)
     */
    public void updateFetchQuota(long quota) {
        LOGGER.info("Updating remote fetch quota to {} bytes per second", quota);
        rlmFetchQuotaManager.updateQuota(new Quota(quota, true));
    }

    /**
     * 调整复制线程池的大小
     */
    public void resizeCopierThreadPool(int newSize) {
        int currentSize = rlmCopyThreadPool.getCorePoolSize();
        LOGGER.info("Updating remote copy thread pool size from {} to {}", currentSize, newSize);
        rlmCopyThreadPool.setCorePoolSize(newSize);
    }

    /**
     * 调整过期清理线程池的大小
     */
    public void resizeExpirationThreadPool(int newSize) {
        int currentSize = rlmExpirationThreadPool.getCorePoolSize();
        LOGGER.info("Updating remote expiration thread pool size from {} to {}", currentSize, newSize);
        rlmExpirationThreadPool.setCorePoolSize(newSize);
    }

    /**
     * 调整读取线程池的大小
     */
    public void resizeReaderThreadPool(int newSize) {
        int currentSize = remoteStorageReaderThreadPool.getCorePoolSize();
        LOGGER.info("Updating remote reader thread pool size from {} to {}", currentSize, newSize);
        remoteStorageReaderThreadPool.setCorePoolSize(newSize);
    }

    /**
     * 移除所有度量指标
     */
    private void removeMetrics() {
        metricsGroup.removeMetric(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC);
        metricsGroup.removeMetric(REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC);
        remoteStorageReaderThreadPool.removeMetrics();
    }

    /**
     * 返回RLM任务等待配额可用的超时时间
     */
    Duration quotaTimeout() {
        return Duration.ofSeconds(1);
    }

    /**
     * 创建远程日志复制操作的配额管理器
     */
    RLMQuotaManager createRLMCopyQuotaManager() {
        return new RLMQuotaManager(copyQuotaManagerConfig(rlmConfig), metrics, QuotaType.RLM_COPY,
          "Tracking copy byte-rate for Remote Log Manager", time);
    }

    /**
     * 创建远程日志获取操作的配额管理器
     */
    RLMQuotaManager createRLMFetchQuotaManager() {
        return new RLMQuotaManager(fetchQuotaManagerConfig(rlmConfig), metrics, QuotaType.RLM_FETCH,
          "Tracking fetch byte-rate for Remote Log Manager", time);
    }

    /**
     * 获取远程获取操作的限流时间(毫秒)
     */
    public long getFetchThrottleTimeMs() {
        return rlmFetchQuotaManager.getThrottleTimeMs();
    }

    /**
     * 获取远程获取操作的限流时间传感器
     */
    public Sensor fetchThrottleTimeSensor() {
        return fetchThrottleTimeSensor;
    }

    /**
     * 创建远程日志复制操作的配额管理器配置
     */
    static RLMQuotaManagerConfig copyQuotaManagerConfig(RemoteLogManagerConfig rlmConfig) {
        return new RLMQuotaManagerConfig(rlmConfig.remoteLogManagerCopyMaxBytesPerSecond(),
          rlmConfig.remoteLogManagerCopyNumQuotaSamples(),
          rlmConfig.remoteLogManagerCopyQuotaWindowSizeSeconds());
    }

    /**
     * 创建远程日志获取操作的配额管理器配置
     */
    static RLMQuotaManagerConfig fetchQuotaManagerConfig(RemoteLogManagerConfig rlmConfig) {
        return new RLMQuotaManagerConfig(rlmConfig.remoteLogManagerFetchMaxBytesPerSecond(),
          rlmConfig.remoteLogManagerFetchNumQuotaSamples(),
          rlmConfig.remoteLogManagerFetchQuotaWindowSizeSeconds());
    }

    /**
     * 通用的委托对象创建方法，用于创建远程存储管理器和远程日志元数据管理器的实例
     *
     * @param classLoader 用于加载指定类的类加载器
     * @param className  要创建的类的全限定名
     * @return 创建的委托对象实例
     * @throws KafkaException 如果实例化过程中发生任何异常
     */
    @SuppressWarnings("unchecked")
    private <T> T createDelegate(ClassLoader classLoader, String className) {
        try {
            // 使用指定的类加载器加载类，并通过反射创建实例
            return (T) classLoader.loadClass(className)
                    .getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
                 ClassNotFoundException e) {
            // 将所有异常包装为KafkaException抛出
            throw new KafkaException(e);
        }
    }

    /**
     * 创建远程存储管理器实例
     * 该管理器负责处理日志段的实际存储操作，包括上传、下载和删除等
     *
     * @return 创建的远程存储管理器实例
     */
    RemoteStorageManager createRemoteStorageManager() {
        // 使用安全管理器执行特权操作
        return SecurityManagerCompatibility.get().doPrivileged(() -> {
            // 获取远程存储管理器的类路径
            final String classPath = rlmConfig.remoteStorageManagerClassPath();
            if (classPath != null && !classPath.trim().isEmpty()) {
                // 如果指定了类路径，创建子优先类加载器
                ChildFirstClassLoader classLoader = new ChildFirstClassLoader(classPath, this.getClass().getClassLoader());
                // 创建远程存储管理器委托实例
                RemoteStorageManager delegate = createDelegate(classLoader, rlmConfig.remoteStorageManagerClassName());
                // 包装为类加载器感知的远程存储管理器
                return (RemoteStorageManager) new ClassLoaderAwareRemoteStorageManager(delegate, classLoader);
            } else {
                // 如果没有指定类路径，使用当前类的类加载器创建实例
                return createDelegate(this.getClass().getClassLoader(), rlmConfig.remoteStorageManagerClassName());
            }
        });
    }

    /**
     * 配置远程存储管理器
     * 设置必要的配置属性，包括broker ID和其他自定义配置
     */
    private void configureRSM() {
        // 创建配置属性映射表，包含远程存储管理器的所有配置
        final Map<String, Object> rsmProps = new HashMap<>(rlmConfig.remoteStorageManagerProps());
        // 添加broker ID配置
        rsmProps.put(ServerConfigs.BROKER_ID_CONFIG, brokerId);
        // 使用配置属性初始化远程存储管理器
        remoteLogStorageManager.configure(rsmProps);
    }

    /**
     * 创建远程日志元数据管理器实例
     * 该管理器负责管理远程日志段的元数据信息，包括日志段的状态、偏移量范围等
     *
     * @return 创建的远程日志元数据管理器实例
     */
    RemoteLogMetadataManager createRemoteLogMetadataManager() {
        // 使用安全管理器执行特权操作
        return SecurityManagerCompatibility.get().doPrivileged(() -> {
            // 获取远程日志元数据管理器的类路径
            final String classPath = rlmConfig.remoteLogMetadataManagerClassPath();
            if (classPath != null && !classPath.trim().isEmpty()) {
                // 如果指定了类路径，创建子优先类加载器
                ClassLoader classLoader = new ChildFirstClassLoader(classPath, this.getClass().getClassLoader());
                // 创建远程日志元数据管理器委托实例
                RemoteLogMetadataManager delegate = createDelegate(classLoader, rlmConfig.remoteLogMetadataManagerClassName());
                // 包装为类加载器感知的远程日志元数据管理器
                return (RemoteLogMetadataManager) new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader);
            } else {
                // 如果没有指定类路径，使用当前类的类加载器创建实例
                return createDelegate(this.getClass().getClassLoader(), rlmConfig.remoteLogMetadataManagerClassName());
            }
        });
    }

    /**
     * 当远程日志元数据管理器的端点创建完成时调用此方法
     * 用于设置与远程日志元数据管理器通信的端点信息
     *
     * @param endpoint 远程日志元数据管理器的通信端点
     */
    public void onEndPointCreated(Endpoint endpoint) {
        // 保存端点信息
        this.endpoint = Optional.of(endpoint);
    }

    /**
     * 配置远程日志元数据管理器
     * 设置必要的配置属性，包括通信端点、安全协议、broker ID等
     */
    private void configureRLMM() {
        // 创建配置属性映射表
        final Map<String, Object> rlmmProps = new HashMap<>();
        // 如果存在端点信息，设置bootstrap servers和安全协议
        endpoint.ifPresent(e -> {
            rlmmProps.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers", e.host() + ":" + e.port());
            rlmmProps.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol", e.securityProtocol().name);
        });
        // 添加自定义配置，可能会覆盖端点配置
        rlmmProps.putAll(rlmConfig.remoteLogMetadataManagerProps());

        // 添加broker ID、日志目录和集群ID配置
        rlmmProps.put(ServerConfigs.BROKER_ID_CONFIG, brokerId);
        rlmmProps.put(LOG_DIR_CONFIG, logDir);
        rlmmProps.put("cluster.id", clusterId);

        // 使用配置属性初始化远程日志元数据管理器
        remoteLogMetadataManager.configure(rlmmProps);
    }

    /**
     * 启动远程日志管理器
     * 初始化并配置远程存储管理器(RSM)和远程日志元数据管理器(RLMM)
     * 这些管理器可能需要启动资源来连接broker或远程存储
     */
    public void startup() {
        // 配置远程存储管理器
        configureRSM();
        // 配置远程日志元数据管理器
        configureRLMM();
        // 标记远程日志管理器已配置完成
        remoteLogManagerConfigured = true;
    }

    /**
     * 检查远程日志管理器是否已配置完成
     *
     * @return 如果远程日志管理器已配置完成则返回true，否则返回false
     */
    private boolean isRemoteLogManagerConfigured() {
        return this.remoteLogManagerConfigured;
    }

    /**
     * 获取远程存储管理器实例
     *
     * @return 远程存储管理器实例
     */
    public RemoteStorageManager storageManager() {
        return remoteLogStorageManager;
    }

    /**
     * 过滤启用了远程日志功能的分区
     * 不需要特别检查内部主题，因为log.remoteLogEnabled()已经处理了这种情况
     *
     * @param partitions 需要过滤的分区集合
     * @return 启用了远程日志功能的分区流
     */
    private Stream<Partition> filterPartitions(Set<Partition> partitions) {
        // 过滤出启用了远程日志功能的分区
        return partitions.stream().filter(partition -> partition.log().exists(UnifiedLog::remoteLogEnabled));
    }

    /**
     * 缓存主题分区ID信息
     * 如果主题ID发生变化，会记录日志
     *
     * @param topicIdPartition 包含主题分区和主题ID的信息
     */
    private void cacheTopicPartitionIds(TopicIdPartition topicIdPartition) {
        // 将主题分区和主题ID的映射关系存入缓存
        Uuid previousTopicId = topicIdByPartitionMap.put(topicIdPartition.topicPartition(), topicIdPartition.topicId());
        // 如果主题ID发生变化，记录日志
        if (previousTopicId != null && !previousTopicId.equals(topicIdPartition.topicId())) {
            LOGGER.info("Previous cached topic id {} for {} does not match updated topic id {}",
                    previousTopicId, topicIdPartition.topicPartition(), topicIdPartition.topicId());
        }
    }

    /**
     * 处理主题分区的领导者变更回调
     * 当broker上的分区领导者角色发生变化时调用此方法
     * 如果分区没有现有任务，将分配新的领导者或追随者任务
     * 如果已有任务，则将任务转换为相应的目标状态(领导者或追随者)
     *
     * @param partitionsBecomeLeader   在此broker上成为领导者的分区集合
     * @param partitionsBecomeFollower 在此broker上成为追随者的分区集合
     * @param topicIds                 主题名称到主题ID的映射关系
     */
    public void onLeadershipChange(Set<Partition> partitionsBecomeLeader,
                                   Set<Partition> partitionsBecomeFollower,
                                   Map<String, Uuid> topicIds) {
        // 记录领导者变更信息
        LOGGER.debug("Received leadership changes for leaders: {} and followers: {}", partitionsBecomeLeader, partitionsBecomeFollower);

        // 检查远程存储系统是否已正确配置
        if (rlmConfig.isRemoteStorageSystemEnabled() && !isRemoteLogManagerConfigured()) {
            throw new KafkaException("RemoteLogManager is not configured when remote storage system is enabled");
        }

        // 过滤并收集成为领导者的分区信息
        Map<TopicIdPartition, Boolean> leaderPartitions = filterPartitions(partitionsBecomeLeader)
                .collect(Collectors.toMap(p -> new TopicIdPartition(topicIds.get(p.topic()), p.topicPartition()),
                        p -> p.log().exists(log -> log.config().remoteLogCopyDisable())));

        // 过滤并收集成为追随者的分区信息
        Map<TopicIdPartition, Boolean> followerPartitions = filterPartitions(partitionsBecomeFollower)
                .collect(Collectors.toMap(p -> new TopicIdPartition(topicIds.get(p.topic()), p.topicPartition()),
                        p -> p.log().exists(log -> log.config().remoteLogCopyDisable())));

        // 如果有分区角色发生变化
        if (!leaderPartitions.isEmpty() || !followerPartitions.isEmpty()) {
            // 记录过滤后的有效分区信息
            LOGGER.debug("Effective topic partitions after filtering compact and internal topics, leaders: {} and followers: {}",
                    leaderPartitions, followerPartitions);

            // 缓存主题分区ID信息
            leaderPartitions.forEach((tp, __) -> cacheTopicPartitionIds(tp));
            followerPartitions.forEach((tp, __) -> cacheTopicPartitionIds(tp));

            // 通知远程日志元数据管理器分区领导者角色变更
            remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions.keySet(), followerPartitions.keySet());
            // 处理成为追随者的分区
            followerPartitions.forEach((tp, __) -> doHandleFollowerPartition(tp));

            // 如果此节点之前是分区的领导者，RLMTask可能在后台线程运行并发出指标
            // 因此在将此节点标记为追随者后，需要移除相关指标
            followerPartitions.forEach((tp, __) -> removeRemoteTopicPartitionMetrics(tp));

            // 处理成为领导者的分区
            leaderPartitions.forEach(this::doHandleLeaderPartition);
        }
    }

    /**
     * 停止指定分区的领导者日志复制任务
     * 当分区不再需要复制日志到远程存储时调用此方法
     *
     * @param partitions 需要停止复制任务的分区集合
     */
    public void stopLeaderCopyRLMTasks(Set<Partition> partitions) {
        // 遍历需要停止复制任务的分区
        for (Partition partition : partitions) {
            // 获取主题分区信息
            TopicPartition tp = partition.topicPartition();
            // 检查是否存在对应的主题ID
            if (topicIdByPartitionMap.containsKey(tp)) {
                // 创建主题ID分区对象
                TopicIdPartition tpId = new TopicIdPartition(topicIdByPartitionMap.get(tp), tp);
                // 如果存在复制任务，则取消任务并重置指标
                leaderCopyRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                    // 记录取消任务的日志
                    LOGGER.info("Cancelling the copy RLM task for partition: {}", tpId);
                    // 取消复制任务
                    task.cancel();
                    // 记录重置指标的日志
                    LOGGER.info("Resetting remote copy lag metrics for partition: {}", tpId);
                    // 重置复制延迟统计信息
                    ((RLMCopyTask) task.rlmTask).resetLagStats();
                    // 返回null以从映射中移除任务
                    return null;
                });
            }
        }
    }

    /**
     * 停止指定分区的远程日志管理器任务。
     * 当 {@link StopPartition#deleteLocalLog} 为true时，调用 {@link RemoteLogMetadataManager#onStopPartitions(Set)}。
     * 当 {@link StopPartition#deleteRemoteLog} 为true时，从远程存储中删除这些分区。
     *
     * 应用场景:
     * 1. 分区被删除时
     * 2. 副本状态变更为离线时
     * 3. 副本删除开始时
     *
     * @param stopPartitions 需要停止的主题分区集合
     * @param errorHandler   处理停止分区过程中错误的回调函数
     */
    public void stopPartitions(Set<StopPartition> stopPartitions,
                               BiConsumer<TopicPartition, Throwable> errorHandler) {
        // 记录调试日志
        LOGGER.debug("Stop partitions: {}", stopPartitions);
        
        // 遍历需要停止的分区
        for (StopPartition stopPartition: stopPartitions) {
            TopicPartition tp = stopPartition.topicPartition;
            try {
                // 检查分区是否在主题ID映射表中
                if (topicIdByPartitionMap.containsKey(tp)) {
                    // 创建主题ID分区对象
                    TopicIdPartition tpId = new TopicIdPartition(topicIdByPartitionMap.get(tp), tp);
                    
                    // 取消领导者副本的日志段复制任务
                    leaderCopyRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                        LOGGER.info("Cancelling the copy RLM task for partition: {}", tpId);
                        task.cancel();
                        return null;
                    });
                    
                    // 取消领导者副本的日志段过期清理任务
                    leaderExpirationRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                        LOGGER.info("Cancelling the expiration RLM task for partition: {}", tpId);
                        task.cancel();
                        return null;
                    });
                    
                    // 取消追随者副本的远程日志管理任务
                    followerRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                        LOGGER.info("Cancelling the follower RLM task for partition: {}", tpId);
                        task.cancel();
                        return null;
                    });

                    // 移除远程主题分区的度量指标
                    removeRemoteTopicPartitionMetrics(tpId);

                    // 如果需要删除远程日志，则执行删除操作
                    if (stopPartition.deleteRemoteLog) {
                        LOGGER.info("Deleting the remote log segments task for partition: {}", tpId);
                        deleteRemoteLogPartition(tpId);
                    }
                } else {
                    // 记录警告日志，表示不应该收到此分区的停止请求
                    LOGGER.warn("StopPartition call is not expected for partition: {}", tp);
                }
            } catch (Exception ex) {
                // 调用错误处理回调函数
                errorHandler.accept(tp, ex);
                LOGGER.error("Error while stopping the partition: {}", stopPartition, ex);
            }
        }

        // 对于需要删除本地日志或停止远程日志元数据管理器的分区，从主题ID映射表中移除并通知远程日志元数据管理器
        // 注意：在ZK模式下，当副本状态变更为离线和副本删除开始时会调用此方法
        Set<TopicIdPartition> pendingActionsPartitions = stopPartitions.stream()
                .filter(sp -> (sp.stopRemoteLogMetadataManager || sp.deleteLocalLog) && topicIdByPartitionMap.containsKey(sp.topicPartition))
                .map(sp -> new TopicIdPartition(topicIdByPartitionMap.get(sp.topicPartition), sp.topicPartition))
                .collect(Collectors.toSet());

        // 如果有待处理的分区，则执行清理操作
        if (!pendingActionsPartitions.isEmpty()) {
            // 从主题ID映射表中移除这些分区
            pendingActionsPartitions.forEach(tpId -> topicIdByPartitionMap.remove(tpId.topicPartition()));
            // 通知远程日志元数据管理器停止这些分区
            remoteLogMetadataManager.onStopPartitions(pendingActionsPartitions);
        }
    }

    /**
     * 删除远程日志分区的所有日志段
     * 
     * 实现步骤：
     * 1. 获取分区的所有远程日志段元数据
     * 2. 发布删除开始事件
     * 3. 删除远程存储中的日志段数据
     * 4. 清理索引缓存
     * 5. 发布删除完成事件
     *
     * @param partition 要删除的主题分区ID
     * @throws RemoteStorageException 远程存储操作异常
     * @throws ExecutionException 事件发布执行异常
     * @throws InterruptedException 线程中断异常
     */
    private void deleteRemoteLogPartition(TopicIdPartition partition) throws RemoteStorageException, ExecutionException, InterruptedException {
        // 获取分区的所有远程日志段元数据
        List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        remoteLogMetadataManager.listRemoteLogSegments(partition).forEachRemaining(metadataList::add);

        // 为每个日志段创建删除开始事件
        List<RemoteLogSegmentMetadataUpdate> deleteSegmentStartedEvents = metadataList.stream()
                .map(metadata ->
                        new RemoteLogSegmentMetadataUpdate(metadata.remoteLogSegmentId(), time.milliseconds(),
                                metadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId))
                .collect(Collectors.toList());
        // 发布删除开始事件并等待完成
        publishEvents(deleteSegmentStartedEvents).get();

        // KAFKA-15313: 当分区被删除时，异步删除远程日志段
        Collection<Uuid> deletedSegmentIds = new ArrayList<>();
        for (RemoteLogSegmentMetadata metadata: metadataList) {
            // 收集已删除的日志段ID
            deletedSegmentIds.add(metadata.remoteLogSegmentId().id());
            // 从远程存储中删除日志段数据
            remoteLogStorageManager.deleteLogSegmentData(metadata);
        }
        // 从索引缓存中移除已删除的日志段
        indexCache.removeAll(deletedSegmentIds);

        // 为每个日志段创建删除完成事件
        List<RemoteLogSegmentMetadataUpdate> deleteSegmentFinishedEvents = metadataList.stream()
                .map(metadata ->
                        new RemoteLogSegmentMetadataUpdate(metadata.remoteLogSegmentId(), time.milliseconds(),
                                metadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId))
                .collect(Collectors.toList());
        // 发布删除完成事件并等待完成
        publishEvents(deleteSegmentFinishedEvents).get();
    }

    /**
     * 发布远程日志段元数据更新事件
     * 
     * 实现细节：
     * 1. 将每个事件转换为异步更新操作
     * 2. 等待所有更新操作完成
     * 
     * @param events 要发布的元数据更新事件列表
     * @return 表示所有事件发布完成的Future
     * @throws RemoteStorageException 远程存储操作异常
     */
    private CompletableFuture<Void> publishEvents(List<RemoteLogSegmentMetadataUpdate> events) throws RemoteStorageException {
        // 创建存储每个事件更新操作Future的列表
        List<CompletableFuture<Void>> result = new ArrayList<>();
        // 遍历事件列表，将每个事件转换为异步更新操作
        for (RemoteLogSegmentMetadataUpdate event : events) {
            result.add(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(event));
        }
        // 等待所有更新操作完成
        return CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));
    }

    /**
     * 获取指定主题分区、纪元和偏移量对应的远程日志段元数据
     * 
     * 应用场景：
     * 1. 消费者请求特定偏移量的消息时
     * 2. 副本同步时需要获取特定位置的日志段
     * 
     * @param topicPartition 主题分区
     * @param epochForOffset 领导者纪元
     * @param offset 日志偏移量
     * @return 远程日志段元数据，如果不存在则返回空
     * @throws RemoteStorageException 远程存储操作异常
     * @throws KafkaException 主题分区未注册异常
     */
    public Optional<RemoteLogSegmentMetadata> fetchRemoteLogSegmentMetadata(TopicPartition topicPartition,
                                                                            int epochForOffset,
                                                                            long offset) throws RemoteStorageException {
        // 获取主题ID，如果不存在则抛出异常
        Uuid topicId = topicIdByPartitionMap.get(topicPartition);
        if (topicId == null) {
            throw new KafkaException("No topic id registered for topic partition: " + topicPartition);
        }
        // 从远程日志元数据管理器获取日志段元数据
        return remoteLogMetadataManager.remoteLogSegmentMetadata(new TopicIdPartition(topicId, topicPartition), epochForOffset, offset);
    }

    /**
     * Returns the next segment that may contain the aborted transaction entries. The search ensures that the returned
     * segment offsets are greater than or equal to the given offset and in the same epoch.
     * @param topicPartition topic partition to search
     * @param epochForOffset the epoch
     * @param offset the offset
     * @return The next segment that contains the transaction index in the same epoch.
     * @throws RemoteStorageException If an error occurs while fetching the remote log segment metadata.
     */
    public Optional<RemoteLogSegmentMetadata> fetchNextSegmentWithTxnIndex(TopicPartition topicPartition,
                                                                           int epochForOffset,
                                                                           long offset) throws RemoteStorageException {
        Uuid topicId = topicIdByPartitionMap.get(topicPartition);
        if (topicId == null) {
            throw new KafkaException("No topic id registered for topic partition: " + topicPartition);
        }
        TopicIdPartition tpId = new TopicIdPartition(topicId, topicPartition);
        return remoteLogMetadataManager.nextSegmentWithTxnIndex(tpId, epochForOffset, offset);
    }

    /**
     * 在远程日志段中查找指定时间戳的消息
     * 
     * 应用场景：
     * 1. 基于时间戳的消息查找
     * 2. 消费者按时间戳消费消息
     * 
     * 实现步骤：
     * 1. 使用索引缓存查找时间戳对应的起始位置
     * 2. 从远程存储获取日志段数据
     * 3. 扫描消息批次查找匹配的消息
     * 
     * @param rlsMetadata 远程日志段元数据
     * @param timestamp 目标时间戳
     * @param startingOffset 起始偏移量
     * @return 找到的消息的时间戳和偏移量信息
     * @throws RemoteStorageException 远程存储操作异常
     * @throws IOException IO异常
     */
    Optional<FileRecords.TimestampAndOffset> lookupTimestamp(RemoteLogSegmentMetadata rlsMetadata, long timestamp, long startingOffset)
            throws RemoteStorageException, IOException {
        // 使用索引缓存查找时间戳对应的起始位置
        int startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset);

        InputStream remoteSegInputStream = null;
        try {
            // 从远程存储获取日志段数据，从startPos位置开始读取
            remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata, startPos);
            RemoteLogInputStream remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream);

            // 遍历消息批次
            while (true) {
                RecordBatch batch = remoteLogInputStream.nextBatch();
                if (batch == null) break;
                // 如果批次的最大时间戳和最后偏移量满足条件
                if (batch.maxTimestamp() >= timestamp && batch.lastOffset() >= startingOffset) {
                    // 遍历批次中的消息
                    try (CloseableIterator<Record> recordStreamingIterator = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
                        while (recordStreamingIterator.hasNext()) {
                            Record record = recordStreamingIterator.next();
                            // 找到第一个满足时间戳和偏移量条件的消息
                            if (record.timestamp() >= timestamp && record.offset() >= startingOffset)
                                return Optional.of(new FileRecords.TimestampAndOffset(record.timestamp(), record.offset(), maybeLeaderEpoch(batch.partitionLeaderEpoch())));
                        }
                    }
                }
            }

            // 未找到满足条件的消息
            return Optional.empty();
        } finally {
            // 确保关闭输入流
            Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
        }
    }

    /**
     * 获取给定leader epoch的Optional包装值
     * 
     * @param leaderEpoch leader epoch值
     * @return 如果leaderEpoch为NO_PARTITION_LEADER_EPOCH则返回空Optional，否则返回包含leaderEpoch的Optional
     */
    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        // 如果leaderEpoch为NO_PARTITION_LEADER_EPOCH则返回空Optional，否则返回包含leaderEpoch的Optional
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * 异步读取远程存储中指定时间戳和起始偏移量的消息
     * 
     * @param topicPartition 主题分区
     * @param timestamp 目标时间戳
     * @param startingOffset 起始偏移量
     * @param leaderEpochCache leader epoch缓存
     * @param searchLocalLog 搜索本地日志的函数
     * @return 包含异步任务和结果Future的holder对象
     */
    public AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> asyncOffsetRead(
            TopicPartition topicPartition,
            Long timestamp,
            Long startingOffset,
            LeaderEpochFileCache leaderEpochCache,
            Supplier<Option<FileRecords.TimestampAndOffset>> searchLocalLog) {
        // 创建用于存储异步任务结果的CompletableFuture
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> taskFuture = new CompletableFuture<>();
        
        // 提交RemoteLogOffsetReader任务到远程存储读取线程池
        Future<Void> jobFuture = remoteStorageReaderThreadPool.submit(
                new RemoteLogOffsetReader(this, topicPartition, timestamp, startingOffset, leaderEpochCache, searchLocalLog, result -> {
                    // 创建主题分区操作键
                    TopicPartitionOperationKey key = new TopicPartitionOperationKey(topicPartition.topic(), topicPartition.partition());
                    // 完成异步任务并设置结果
                    taskFuture.complete(result);
                    // 检查并完成延迟的远程列表偏移量操作
                    delayedRemoteListOffsetsPurgatory.checkAndComplete(key);
                })
        );
        
        // 返回包含任务Future和结果Future的holder对象
        return new AsyncOffsetReadFutureHolder<>(jobFuture, taskFuture);
    }

    /**
     * Search the message offset in the remote storage for the given timestamp and starting-offset.
     * Once the target segment where the search to be performed is found:
     * 1. If the target segment lies in the local storage (common segments that lies in both remote and local storage),
     *    then the search will be performed in the local storage.
     * 2. If the target segment is found only in the remote storage, then the search will be performed in the remote storage.
     *
     *  <p>
     * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
     * <p>
     * - If there are no messages in the remote storage, return Empty
     * - If all the messages in the remote storage have smaller offsets, return Empty
     * - If all the messages in the remote storage have smaller timestamps, return Empty
     * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
     * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
     *
     * @param tp               topic partition in which the offset to be found.
     * @param timestamp        The timestamp to search for.
     * @param startingOffset   The starting offset to search.
     * @param leaderEpochCache LeaderEpochFileCache of the topic partition.
     * @return the timestamp and offset of the first message that meets the requirements. Empty will be returned if there
     * is no such message.
     */
    /**
     * 在远程存储中根据时间戳和起始偏移量查找消息偏移量
     * 查找策略:
     * 1. 如果目标日志段同时存在于本地和远程存储，则在本地存储中搜索
     * 2. 如果目标日志段只存在于远程存储，则在远程存储中搜索
     *
     * 返回值规则:
     * - 如果远程存储中没有消息，返回Empty
     * - 如果所有远程消息的偏移量都小于目标偏移量，返回Empty
     * - 如果所有远程消息的时间戳都小于目标时间戳，返回Empty
     * - 否则返回第一个时间戳大于等于目标时间戳且偏移量大于等于起始偏移量的消息的时间戳和偏移量
     *
     * @param tp 要查找的主题分区
     * @param timestamp 目标时间戳
     * @param startingOffset 起始偏移量
     * @param leaderEpochCache 主题分区的leader epoch缓存
     * @return 满足条件的消息的时间戳和偏移量，如果没有找到则返回Empty
     */
    public Optional<FileRecords.TimestampAndOffset> findOffsetByTimestamp(TopicPartition tp,
                                                                          long timestamp,
                                                                          long startingOffset,
                                                                          LeaderEpochFileCache leaderEpochCache) throws RemoteStorageException, IOException {
        // 获取主题ID，如果不存在则抛出异常
        Uuid topicId = topicIdByPartitionMap.get(tp);
        if (topicId == null) {
            throw new KafkaException("Topic id does not exist for topic partition: " + tp);
        }
        
        // 获取UnifiedLog实例，如果不存在则抛出异常
        Optional<UnifiedLog> unifiedLogOptional = fetchLog.apply(tp);
        if (unifiedLogOptional.isEmpty()) {
            throw new KafkaException("UnifiedLog does not exist for topic partition: " + tp);
        }
        UnifiedLog unifiedLog = unifiedLogOptional.get();

        // 获取起始偏移量所在的epoch
        OptionalInt maybeEpoch = leaderEpochCache.epochForOffset(startingOffset);
        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, tp);
        NavigableMap<Integer, Long> epochWithOffsets = buildFilteredLeaderEpochMap(leaderEpochCache.epochWithOffsets());
        
        // 遍历每个epoch，直到找到目标消息或遍历完所有epoch
        while (maybeEpoch.isPresent()) {
            int epoch = maybeEpoch.getAsInt();
            // 获取当前epoch的所有远程日志段
            Iterator<RemoteLogSegmentMetadata> iterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
            
            // 遍历每个远程日志段
            while (iterator.hasNext()) {
                RemoteLogSegmentMetadata rlsMetadata = iterator.next();
                // 检查日志段是否满足查找条件:
                // 1. 最大时间戳大于等于目标时间戳
                // 2. 结束偏移量大于等于起始偏移量
                // 3. 日志段在有效的leader epoch范围内
                // 4. 日志段已完成复制
                if (rlsMetadata.maxTimestampMs() >= timestamp
                    && rlsMetadata.endOffset() >= startingOffset
                    && isRemoteSegmentWithinLeaderEpochs(rlsMetadata, unifiedLog.logEndOffset(), epochWithOffsets)
                    && rlsMetadata.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED)) {
                    
                    // 创建本地日志段的副本以避免并发修改
                    List<LogSegment> segmentsCopy = new ArrayList<>(unifiedLog.logSegments());
                    
                    // 如果本地没有日志段，或者远程日志段的起始偏移量小于本地第一个日志段的基础偏移量
                    // 则在远程日志段中查找
                    if (segmentsCopy.isEmpty() || rlsMetadata.startOffset() < segmentsCopy.get(0).baseOffset()) {
                        return lookupTimestamp(rlsMetadata, timestamp, startingOffset);
                    } else {
                        // 否则在本地日志段中查找
                        for (LogSegment segment : segmentsCopy) {
                            if (segment.largestTimestamp() >= timestamp) {
                                return segment.findOffsetByTimestamp(timestamp, startingOffset);
                            }
                        }
                    }
                }
            }
            // 如果在当前epoch中没有找到，则移动到下一个epoch
            maybeEpoch = leaderEpochCache.nextEpoch(epoch);
        }
        // 如果所有epoch都没有找到满足条件的消息，则返回Empty
        return Optional.empty();
    }

    private abstract static class CancellableRunnable implements Runnable {
        private volatile boolean cancelled = false;

        public void cancel() {
            cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    /**
     * Returns the leader epoch entries within the range of the given start[exclusive] and end[inclusive] offset.
     * <p>
     * Visible for testing.
     *
     * @param log         The actual log from where to take the leader-epoch checkpoint
     * @param startOffset The start offset of the epoch entries (inclusive).
     *                    If start offset is 6, then it will retain an entry at offset 6.
     * @param endOffset   The end offset of the epoch entries (exclusive)
     *                    If end offset is 100, then it will remove the entries greater than or equal to 100.
     * @return the leader epoch entries
     */
    List<EpochEntry> getLeaderEpochEntries(UnifiedLog log, long startOffset, long endOffset) {
        return log.leaderEpochCache().epochEntriesInRange(startOffset, endOffset);
    }

    // VisibleForTesting
    RLMTask rlmCopyTask(TopicIdPartition topicIdPartition) {
        RLMTaskWithFuture task = leaderCopyRLMTasks.get(topicIdPartition);
        if (task != null) {
            return task.rlmTask;
        }
        return null;
    }

    /**
     * 远程日志管理任务的抽象基类，提供了任务执行的基本框架
     * 继承自CancellableRunnable，支持任务取消功能
     */
    abstract class RLMTask extends CancellableRunnable {

        // 任务关联的主题分区ID
        protected final TopicIdPartition topicIdPartition;
        // 日志记录器
        private final Logger logger;

        /**
         * 构造远程日志管理任务
         * 
         * @param topicIdPartition 主题分区ID
         */
        public RLMTask(TopicIdPartition topicIdPartition) {
            this.topicIdPartition = topicIdPartition;
            this.logger = getLogContext().logger(RLMTask.class);
        }

        /**
         * 获取日志上下文，包含broker ID和分区信息
         */
        protected LogContext getLogContext() {
            return new LogContext("[RemoteLogManager=" + brokerId + " partition=" + topicIdPartition + "] ");
        }

        /**
         * 执行远程日志管理任务
         * 1. 检查任务是否已取消
         * 2. 检查远程日志元数据管理器是否就绪
         * 3. 获取并执行UnifiedLog实例的具体操作
         */
        public void run() {
            // 如果任务已被取消，跳过执行
            if (isCancelled()) {
                logger.debug("Skipping the current run for partition {} as it is cancelled", topicIdPartition);
                return;
            }
            // 如果远程日志元数据未就绪，跳过执行
            if (!remoteLogMetadataManager.isReady(topicIdPartition)) {
                logger.debug("Skipping the current run for partition {} as the remote-log metadata is not ready", topicIdPartition);
                return;
            }

            try {
                // 获取UnifiedLog实例
                Optional<UnifiedLog> unifiedLogOptional = fetchLog.apply(topicIdPartition.topicPartition());

                if (unifiedLogOptional.isEmpty()) {
                    return;
                }

                // 执行具体的日志操作
                execute(unifiedLogOptional.get());
            } catch (InterruptedException ex) {
                // 处理线程中断异常
                if (!isCancelled()) {
                    logger.warn("Current thread for partition {} is interrupted", topicIdPartition, ex);
                }
            } catch (RetriableException ex) {
                // 处理可重试异常
                logger.debug("Encountered a retryable error while executing current task for partition {}", topicIdPartition, ex);
            } catch (Exception ex) {
                // 处理其他异常
                if (!isCancelled()) {
                    logger.warn("Current task for partition {} received error but it will be scheduled", topicIdPartition, ex);
                }
            }
        }

        /**
         * 执行具体的日志操作，由子类实现
         * 
         * @param log UnifiedLog实例
         * @throws InterruptedException 线程中断异常
         * @throws RemoteStorageException 远程存储异常
         * @throws ExecutionException 执行异常
         */
        protected abstract void execute(UnifiedLog log) throws InterruptedException, RemoteStorageException, ExecutionException;

        public String toString() {
            return this.getClass() + "[" + topicIdPartition + "]";
        }
    }

    /**
     * 远程日志复制任务，负责将日志段从本地复制到远程存储
     * 继承自RLMTask，实现了具体的日志复制逻辑
     */
    class RLMCopyTask extends RLMTask {
        // 自定义元数据大小限制
        private final int customMetadataSizeLimit;
        // 日志记录器
        private final Logger logger;

        // 已复制的偏移量和epoch信息，初始为空，需要在任务运行时获取
        private volatile Optional<OffsetAndEpoch> copiedOffsetOption = Optional.empty();
        // 日志起始偏移量是否已更新的标志
        private volatile boolean isLogStartOffsetUpdated = false;
        // 日志目录路径，用于检测日志目录是否发生变化
        private volatile Optional<String> logDirectory = Optional.empty();

        /**
         * 构造远程日志复制任务
         *
         * @param topicIdPartition 主题分区ID
         * @param customMetadataSizeLimit 自定义元数据大小限制
         */
        public RLMCopyTask(TopicIdPartition topicIdPartition, int customMetadataSizeLimit) {
            super(topicIdPartition);
            this.customMetadataSizeLimit = customMetadataSizeLimit;
            this.logger = getLogContext().logger(RLMCopyTask.class);
        }

        /**
         * 执行日志复制操作
         * 1. 检查日志目录是否变化，如果变化则重置状态
         * 2. 调用copyLogSegmentsToRemote执行实际的复制操作
         *
         * @param log 要复制的UnifiedLog实例
         * @throws InterruptedException 如果线程被中断
         */
        @Override
        protected void execute(UnifiedLog log) throws InterruptedException {
            // 检查日志目录是否发生变化，如果变化则重置状态(KAFKA-16711)
            if (!log.parentDir().equals(logDirectory.orElse(null))) {
                // 重置已复制的偏移量
                copiedOffsetOption = Optional.empty();
                // 重置日志起始偏移量更新标志
                isLogStartOffsetUpdated = false;
                // 更新日志目录路径
                logDirectory = Optional.of(log.parentDir());
            }

            // 执行日志段复制操作
            copyLogSegmentsToRemote(log);
        }

        /**
         * 在成为领导者后更新日志起始偏移量
         * 
         * @param log 需要更新的UnifiedLog实例
         * @throws RemoteStorageException 如果在查找或更新过程中发生远程存储相关错误
         */
        private void maybeUpdateLogStartOffsetOnBecomingLeader(UnifiedLog log) throws RemoteStorageException {
            // 只有在尚未更新日志起始偏移量时才执行更新操作
            if (!isLogStartOffsetUpdated) {
                // 查找该分区的日志起始偏移量
                long logStartOffset = findLogStartOffset(topicIdPartition, log);
                // 更新远程日志的起始偏移量
                updateRemoteLogStartOffset.accept(topicIdPartition.topicPartition(), logStartOffset);
                // 标记已完成更新
                isLogStartOffsetUpdated = true;
                // 记录日志
                logger.info("Found the logStartOffset: {} for partition: {} after becoming leader",
                        logStartOffset, topicIdPartition);
            }
        }

        /**
         * 更新已复制到远程存储的最高偏移量
         * 
         * @param log 需要更新的UnifiedLog实例
         * @throws RemoteStorageException 如果在查找或更新过程中发生远程存储相关错误
         */
        private void maybeUpdateCopiedOffset(UnifiedLog log) throws RemoteStorageException {
            // 只有在尚未设置已复制偏移量时才执行更新操作
            if (copiedOffsetOption.isEmpty()) {
                // 通过以下步骤查找最高的远程偏移量：
                // 1. 从领导者纪元历史中获取最新的领导者纪元
                // 2. 查找该纪元中已复制到远程存储的段的最高偏移量
                // 3. 如果找不到，则检查前一个领导者纪元，直到找到一个条目
                // 4. 如果直到最早的领导者纪元都没有找到条目，则从最早纪元条目的偏移量开始复制
                copiedOffsetOption = Optional.of(findHighestRemoteOffset(topicIdPartition, log));
                // 记录找到的最高远程偏移量
                logger.info("Found the highest copiedRemoteOffset: {} for partition: {} after becoming leader", copiedOffsetOption, topicIdPartition);
                // 更新日志中的最高远程存储偏移量
                copiedOffsetOption.ifPresent(offsetAndEpoch ->  log.updateHighestOffsetInRemoteStorage(offsetAndEpoch.offset()));
            }
        }

        /**
         * 查找符合复制条件的日志段
         * 符合以下条件的日志段可以复制到远程存储：
         * 1) 不是活动日志段（即不是最后一个正在写入的日志段）
         * 2) 日志段的结束偏移量小于最后稳定偏移量（LSO），因为远程存储只应包含已提交/确认的消息
         *
         * @param log 需要复制日志段的UnifiedLog实例
         * @param fromOffset 开始复制的起始偏移量
         * @param lastStableOffset 日志的最后稳定偏移量
         * @return 可以复制到远程存储的候选日志段列表
         */
        List<EnrichedLogSegment> candidateLogSegments(UnifiedLog log, Long fromOffset, Long lastStableOffset) {
            // 创建候选日志段列表
            List<EnrichedLogSegment> candidateLogSegments = new ArrayList<>();
            // 获取从fromOffset开始到最大偏移量的所有日志段
            List<LogSegment> segments = CollectionConverters.asJava(log.logSegments(fromOffset, Long.MAX_VALUE).toSeq());
            
            if (!segments.isEmpty()) {
                // 遍历除最后一个活动段外的所有日志段
                for (int idx = 1; idx < segments.size(); idx++) {
                    LogSegment previousSeg = segments.get(idx - 1);
                    LogSegment currentSeg = segments.get(idx);
                    // 检查当前段的基础偏移量是否小于等于最后稳定偏移量
                    if (currentSeg.baseOffset() <= lastStableOffset) {
                        // 将符合条件的日志段添加到候选列表中
                        candidateLogSegments.add(new EnrichedLogSegment(previousSeg, currentSeg.baseOffset()));
                    }
                }
                // 丢弃最后一个活动段，因为它可能还在写入中
            }
            return candidateLogSegments;
        }

        /**
         * 将日志段复制到远程存储
         * 该方法负责将符合条件的日志段复制到远程存储，同时处理配额限制和异常情况
         *
         * @param log 需要复制日志段的UnifiedLog实例
         * @throws InterruptedException 如果复制过程被中断
         */
        public void copyLogSegmentsToRemote(UnifiedLog log) throws InterruptedException {
            // 如果任务已被取消，直接返回
            if (isCancelled())
                return;

            try {
                // 更新日志起始偏移量和已复制的最高偏移量
                maybeUpdateLogStartOffsetOnBecomingLeader(log);
                maybeUpdateCopiedOffset(log);
                long copiedOffset = copiedOffsetOption.get().offset();

                // LSO（最后稳定偏移量）表示该偏移量之前的消息已经可以被消费（已达到高水位或已提交）
                long lso = log.lastStableOffset();
                if (lso < 0) {
                    logger.warn("lastStableOffset for partition {} is {}, which should not be negative.", topicIdPartition, lso);
                } else if (lso > 0 && copiedOffset < lso) {
                    // 日志起始偏移量可能会超过已复制偏移量，这种情况发生在：
                    // 1) 通过delete-records API增加了日志起始偏移量
                    // 2) 首次启用远程日志
                    long fromOffset = Math.max(copiedOffset + 1, log.logStartOffset());
                    // 获取符合复制条件的日志段
                    List<EnrichedLogSegment> candidateLogSegments = candidateLogSegments(log, fromOffset, lso);
                    logger.debug("Candidate log segments, logStartOffset: {}, copiedOffset: {}, fromOffset: {}, lso: {} " +
                            "and candidateLogSegments: {}", log.logStartOffset(), copiedOffset, fromOffset, lso, candidateLogSegments);
                    
                    if (candidateLogSegments.isEmpty()) {
                        logger.debug("No segments found to be copied for partition {} with copiedOffset: {} and active segment's base-offset: {}",
                                topicIdPartition, copiedOffset, log.activeSegment().baseOffset());
                    } else {
                        // 遍历所有候选日志段进行复制
                        for (EnrichedLogSegment candidateLogSegment : candidateLogSegments) {
                            // 检查任务是否被取消
                            if (isCancelled()) {
                                logger.info("Skipping copying log segments as the current task state is changed, cancelled: {}",
                                        isCancelled());
                                return;
                            }

                            // 获取复制配额锁，确保不超过复制速率限制
                            copyQuotaManagerLock.lock();
                            try {
                                // 获取需要等待的限流时间
                                long throttleTimeMs = rlmCopyQuotaManager.getThrottleTimeMs();
                                while (throttleTimeMs > 0) {
                                    // 记录限流时间
                                    copyThrottleTimeSensor.record(throttleTimeMs, time.milliseconds());
                                    logger.debug("Quota exceeded for copying log segments, waiting for the quota to be available.");
                                    // 如果线程在等待时被中断，InterruptedException会被抛出给调用者
                                    // 需要注意的是，在执行线程被中断之前，任务已经被取消
                                    // 调用者负责通过检查任务是否已取消来优雅地处理异常
                                    boolean ignored = copyQuotaManagerLockCondition.await(quotaTimeout().toMillis(), TimeUnit.MILLISECONDS);
                                    throttleTimeMs = rlmCopyQuotaManager.getThrottleTimeMs();
                                }
                                // 记录本次复制的字节数
                                rlmCopyQuotaManager.record(candidateLogSegment.logSegment.log().sizeInBytes());
                                // 通知等待的线程重新检查配额
                                copyQuotaManagerLockCondition.signalAll();
                            } finally {
                                copyQuotaManagerLock.unlock();
                            }

                            // 生成新的远程日志段ID并添加到正在复制的集合中
                            RemoteLogSegmentId segmentId = RemoteLogSegmentId.generateNew(topicIdPartition);
                            segmentIdsBeingCopied.add(segmentId);
                            try {
                                // 复制日志段到远程存储
                                copyLogSegment(log, candidateLogSegment.logSegment, segmentId, candidateLogSegment.nextSegmentOffset);
                            } finally {
                                // 完成后从正在复制的集合中移除
                                segmentIdsBeingCopied.remove(segmentId);
                            }
                        }
                    }
                } else {
                    logger.debug("Skipping copying segments, current read-offset:{}, and LSO:{}", copiedOffset, lso);
                }
            } catch (CustomMetadataSizeLimitExceededException e) {
                // 仅停止此任务，异常日志记录在抛出异常的地方
                brokerTopicStats.topicStats(log.topicPartition().topic()).failedRemoteCopyRequestRate().mark();
                brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().mark();
                this.cancel();
            } catch (InterruptedException | RetriableException ex) {
                throw ex;
            } catch (Exception ex) {
                if (!isCancelled()) {
                    // 记录复制失败的统计信息
                    brokerTopicStats.topicStats(log.topicPartition().topic()).failedRemoteCopyRequestRate().mark();
                    brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().mark();
                    logger.error("Error occurred while copying log segments of partition: {}", topicIdPartition, ex);
                }
            }
        }

        /**
         * 将单个日志段复制到远程存储
         * 该方法负责复制日志段的具体实现，包括元数据管理、数据复制和异常处理
         *
         * @param log 日志实例
         * @param segment 要复制的日志段
         * @param segmentId 远程日志段ID
         * @param nextSegmentBaseOffset 下一个日志段的基础偏移量
         * @throws InterruptedException 如果复制过程被中断
         * @throws ExecutionException 如果在执行过程中发生异常
         * @throws RemoteStorageException 如果远程存储操作失败
         * @throws IOException 如果发生I/O错误
         * @throws CustomMetadataSizeLimitExceededException 如果自定义元数据大小超过限制
         */
        private void copyLogSegment(UnifiedLog log, LogSegment segment, RemoteLogSegmentId segmentId, long nextSegmentBaseOffset)
                throws InterruptedException, ExecutionException, RemoteStorageException, IOException,
                CustomMetadataSizeLimitExceededException {
            // 获取日志文件和文件名
            File logFile = segment.log().file();
            String logFileName = logFile.getName();

            logger.info("Copying {} to remote storage.", logFileName);

            // 计算日志段的结束偏移量并获取生产者状态快照
            long endOffset = nextSegmentBaseOffset - 1;
            File producerStateSnapshotFile = log.producerStateManager().fetchSnapshot(nextSegmentBaseOffset).orElse(null);

            // 获取领导者纪元条目并构建纪元映射
            List<EpochEntry> epochEntries = getLeaderEpochEntries(log, segment.baseOffset(), nextSegmentBaseOffset);
            Map<Integer, Long> segmentLeaderEpochs = new HashMap<>(epochEntries.size());
            epochEntries.forEach(entry -> segmentLeaderEpochs.put(entry.epoch, entry.startOffset));

            // 检查事务索引是否为空
            boolean isTxnIdxEmpty = segment.txnIndex().isEmpty();
            // 创建远程日志段元数据（复制开始状态）
            RemoteLogSegmentMetadata copySegmentStartedRlsm = new RemoteLogSegmentMetadata(segmentId, segment.baseOffset(), endOffset,
                    segment.largestTimestamp(), brokerId, time.milliseconds(), segment.log().sizeInBytes(),
                    segmentLeaderEpochs, isTxnIdxEmpty);

            // 添加远程日志段元数据
            remoteLogMetadataManager.addRemoteLogSegmentMetadata(copySegmentStartedRlsm).get();

            // 准备日志段数据，包括索引文件和领导者纪元索引
            ByteBuffer leaderEpochsIndex = epochEntriesAsByteBuffer(getLeaderEpochEntries(log, -1, nextSegmentBaseOffset));
            LogSegmentData segmentData = new LogSegmentData(logFile.toPath(), toPathIfExists(segment.offsetIndex().file()),
                    toPathIfExists(segment.timeIndex().file()), Optional.ofNullable(toPathIfExists(segment.txnIndex().file())),
                    producerStateSnapshotFile.toPath(), leaderEpochsIndex);
            
            // 更新复制请求统计信息
            brokerTopicStats.topicStats(log.topicPartition().topic()).remoteCopyRequestRate().mark();
            brokerTopicStats.allTopicsStats().remoteCopyRequestRate().mark();
            Optional<CustomMetadata> customMetadata;
            
            try {
                // 执行实际的日志段数据复制
                customMetadata = remoteLogStorageManager.copyLogSegmentData(copySegmentStartedRlsm, segmentData);
            } catch (RemoteStorageException e) {
                // 复制失败时，清理已复制的段
                logger.info("Copy failed, cleaning segment {}", copySegmentStartedRlsm.remoteLogSegmentId());
                try {
                    deleteRemoteLogSegment(copySegmentStartedRlsm, ignored -> !isCancelled());
                    LOGGER.info("Cleanup completed for segment {}", copySegmentStartedRlsm.remoteLogSegmentId());
                } catch (RemoteStorageException e1) {
                    LOGGER.info("Cleanup failed, will retry later with segment {}: {}", copySegmentStartedRlsm.remoteLogSegmentId(), e1.getMessage());
                }
                throw e;
            }

            // 创建远程日志段元数据更新（复制完成状态）
            RemoteLogSegmentMetadataUpdate copySegmentFinishedRlsm = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                    customMetadata, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);

            // 检查自定义元数据大小是否超过限制
            if (customMetadata.isPresent()) {
                long customMetadataSize = customMetadata.get().value().length;
                if (customMetadataSize > this.customMetadataSizeLimit) {
                    CustomMetadataSizeLimitExceededException e = new CustomMetadataSizeLimitExceededException();
                    logger.info("Custom metadata size {} exceeds configured limit {}." +
                                    " Copying will be stopped and copied segment will be attempted to clean." +
                                    " Original metadata: {}",
                            customMetadataSize, this.customMetadataSizeLimit, copySegmentStartedRlsm, e);
                    // 创建包含自定义元数据的新元数据对象用于删除操作
                    // 注意：此时不会存储更新本身
                    RemoteLogSegmentMetadata newMetadata = copySegmentStartedRlsm.createWithUpdates(copySegmentFinishedRlsm);
                    try {
                        deleteRemoteLogSegment(newMetadata, ignored -> !isCancelled());
                        LOGGER.info("Cleanup completed for segment {}", newMetadata.remoteLogSegmentId());
                    } catch (RemoteStorageException e1) {
                        LOGGER.info("Cleanup failed, will retry later with segment {}: {}", newMetadata.remoteLogSegmentId(), e1.getMessage());
                    }
                    throw e;
                }
            }

            // 更新远程日志段元数据状态为已完成
            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(copySegmentFinishedRlsm).get();
            // 更新复制字节统计信息
            brokerTopicStats.topicStats(log.topicPartition().topic())
                .remoteCopyBytesRate().mark(copySegmentStartedRlsm.segmentSizeInBytes());
            brokerTopicStats.allTopicsStats().remoteCopyBytesRate().mark(copySegmentStartedRlsm.segmentSizeInBytes());

            // epochEntries不能为空，这是RemoteLogSegmentMetadata构造函数中的前置条件
            int lastEpochInSegment = epochEntries.get(epochEntries.size() - 1).epoch;
            copiedOffsetOption = Optional.of(new OffsetAndEpoch(endOffset, lastEpochInSegment));
            // 更新该分区日志在远程存储中的最高偏移量
            // 这样可以确保本地日志段在复制到远程存储之前不会被删除
            log.updateHighestOffsetInRemoteStorage(endOffset);
            logger.info("Copied {} to remote storage with segment-id: {}",
                    logFileName, copySegmentFinishedRlsm.remoteLogSegmentId());

            // 计算并记录延迟统计信息
            long bytesLag = log.onlyLocalLogSegmentsSize() - log.activeSegment().size();
            long segmentsLag = log.onlyLocalLogSegmentsCount() - 1;
            recordLagStats(bytesLag, segmentsLag);
        }

        /**
         * 记录远程日志复制的延迟统计信息
         * 用于测试目的，记录字节和日志段数量的延迟指标
         *
         * @param bytesLag 待复制的字节数延迟
         * @param segmentsLag 待复制的日志段数延迟
         */
        // VisibleForTesting
        void recordLagStats(long bytesLag, long segmentsLag) {
            // 只有在任务未被取消的情况下才记录统计信息
            if (!isCancelled()) {
                String topic = topicIdPartition.topic();
                int partition = topicIdPartition.partition();
                // 记录字节延迟指标
                brokerTopicStats.recordRemoteCopyLagBytes(topic, partition, bytesLag);
                // 记录日志段数量延迟指标
                brokerTopicStats.recordRemoteCopyLagSegments(topic, partition, segmentsLag);
            }
        }

        /**
         * 重置远程日志复制的延迟统计信息
         * 将字节延迟和日志段数量延迟都重置为0
         */
        void resetLagStats() {
            String topic = topicIdPartition.topic();
            int partition = topicIdPartition.partition();
            // 重置字节延迟为0
            brokerTopicStats.recordRemoteCopyLagBytes(topic, partition, 0);
            // 重置日志段数量延迟为0
            brokerTopicStats.recordRemoteCopyLagSegments(topic, partition, 0);
        }

        /**
         * 如果文件存在则返回其Path对象，否则返回null
         * 
         * @param file 要检查的文件
         * @return 如果文件存在返回Path对象，否则返回null
         */
        private Path toPathIfExists(File file) {
            return file.exists() ? file.toPath() : null;
        }
    }

    /**
     * 远程日志过期清理任务类
     * 负责清理过期的远程日志段，包括：
     * 1. 超过保留大小限制的日志段
     * 2. 超过保留时间限制的日志段
     * 3. 小于日志起始偏移量的日志段
     * 4. 不在当前leader epoch范围内的日志段
     */
    class RLMExpirationTask extends RLMTask {
        // 日志记录器
        private final Logger logger;

        /**
         * 创建远程日志过期清理任务
         * 
         * @param topicIdPartition 主题分区标识符
         */
        public RLMExpirationTask(TopicIdPartition topicIdPartition) {
            super(topicIdPartition);
            this.logger = getLogContext().logger(RLMExpirationTask.class);
        }

        /**
         * 执行远程日志过期清理任务
         * 
         * @param log 统一日志实例
         */
        @Override
        protected void execute(UnifiedLog log) throws InterruptedException, RemoteStorageException, ExecutionException {
            cleanupExpiredRemoteLogSegments();
        }

        /**
         * 处理日志起始偏移量更新
         * 当日志起始偏移量发生变化时，更新相关元数据
         * 
         * @param topicPartition 主题分区
         * @param remoteLogStartOffset 新的远程日志起始偏移量
         */
        public void handleLogStartOffsetUpdate(TopicPartition topicPartition, long remoteLogStartOffset) {
            logger.debug("Updating {} with remoteLogStartOffset: {}", topicPartition, remoteLogStartOffset);
            updateRemoteLogStartOffset.accept(topicPartition, remoteLogStartOffset);
        }

        /**
         * 远程日志保留策略处理器
         * 负责根据配置的保留策略(大小和时间)判断日志段是否需要被删除
         */
        class RemoteLogRetentionHandler {
            // 保留大小相关数据，包含保留大小限制和已超出的大小
            private final Optional<RetentionSizeData> retentionSizeData;
            // 保留时间相关数据，包含保留时间限制和清理截止时间
            private final Optional<RetentionTimeData> retentionTimeData;
            // 剩余需要清理的字节数
            private long remainingBreachedSize;
            // 日志起始偏移量
            private OptionalLong logStartOffset = OptionalLong.empty();

            /**
             * 创建远程日志保留策略处理器
             * 
             * @param retentionSizeData 保留大小相关数据
             * @param retentionTimeData 保留时间相关数据
             */
            public RemoteLogRetentionHandler(Optional<RetentionSizeData> retentionSizeData, Optional<RetentionTimeData> retentionTimeData) {
                this.retentionSizeData = retentionSizeData;
                this.retentionTimeData = retentionTimeData;
                // 初始化剩余需要清理的字节数
                remainingBreachedSize = retentionSizeData.map(sizeData -> sizeData.remainingBreachedSize).orElse(0L);
            }

            /**
             * 判断日志段是否超过保留大小限制
             * 
             * @param metadata 远程日志段元数据
             * @return 如果日志段需要被删除返回true，否则返回false
             */
            private boolean isSegmentBreachedByRetentionSize(RemoteLogSegmentMetadata metadata) {
                boolean shouldDeleteSegment = false;
                // 如果没有配置保留大小限制，则不需要删除
                if (retentionSizeData.isEmpty()) {
                    return shouldDeleteSegment;
                }
                // 假设日志段大小总是大于等于0
                // 如果还有需要清理的字节数
                if (remainingBreachedSize > 0) {
                    // 计算清理当前日志段后剩余需要清理的字节数
                    long remainingBytes = remainingBreachedSize - metadata.segmentSizeInBytes();
                    // 如果剩余需要清理的字节数大于等于0，说明当前日志段需要被删除
                    if (remainingBytes >= 0) {
                        remainingBreachedSize = remainingBytes;
                        shouldDeleteSegment = true;
                    }
                }
                if (shouldDeleteSegment) {
                    // 更新日志起始偏移量为当前日志段的结束偏移量+1
                    if (logStartOffset.isEmpty() || logStartOffset.getAsLong() < metadata.endOffset() + 1) {
                        logStartOffset = OptionalLong.of(metadata.endOffset() + 1);
                    }
                    logger.info("About to delete remote log segment {} due to retention size {} breach. Log size after deletion will be {}.",
                            metadata.remoteLogSegmentId(), retentionSizeData.get().retentionSize, remainingBreachedSize + retentionSizeData.get().retentionSize);
                }
                return shouldDeleteSegment;
            }

            /**
             * 判断日志段是否超过保留时间限制
             * 
             * @param metadata 远程日志段元数据
             * @return 如果日志段需要被删除返回true，否则返回false
             */
            public boolean isSegmentBreachedByRetentionTime(RemoteLogSegmentMetadata metadata) {
                boolean shouldDeleteSegment = false;
                // 如果没有配置保留时间限制，则不需要删除
                if (retentionTimeData.isEmpty()) {
                    return shouldDeleteSegment;
                }
                // 如果日志段的最大时间戳小于等于清理截止时间，说明日志段已过期
                shouldDeleteSegment = metadata.maxTimestampMs() <= retentionTimeData.get().cleanupUntilMs;
                if (shouldDeleteSegment) {
                    // 更新剩余需要清理的字节数
                    remainingBreachedSize = Math.max(0, remainingBreachedSize - metadata.segmentSizeInBytes());
                    // 由于在同一个epoch内日志段的偏移量是递增的，
                    // 所以可以安全地将日志起始偏移量设置为当前日志段的结束偏移量+1
                    if (logStartOffset.isEmpty() || logStartOffset.getAsLong() < metadata.endOffset() + 1) {
                        logStartOffset = OptionalLong.of(metadata.endOffset() + 1);
                    }
                    logger.info("About to delete remote log segment {} due to retention time {}ms breach based on the largest record timestamp in the segment",
                            metadata.remoteLogSegmentId(), retentionTimeData.get().retentionMs);
                }
                return shouldDeleteSegment;
            }

            /**
             * 判断日志段是否小于日志起始偏移量
             * 
             * @param metadata 远程日志段元数据
             * @param logStartOffset 日志起始偏移量
             * @param leaderEpochEntries leader epoch条目映射表
             * @return 如果日志段需要被删除返回true，否则返回false
             */
            private boolean isSegmentBreachByLogStartOffset(RemoteLogSegmentMetadata metadata,
                                                            long logStartOffset,
                                                            NavigableMap<Integer, Long> leaderEpochEntries) {
                boolean shouldDeleteSegment = false;
                if (!leaderEpochEntries.isEmpty()) {
                    // 注意：logStartOffset和leaderEpochEntries.firstEntry().getValue()应该相等
                    Integer firstEpoch = leaderEpochEntries.firstKey();
                    // 如果日志段的所有epoch都小于等于第一个epoch，且日志段的结束偏移量小于日志起始偏移量
                    // 说明该日志段已经不再需要，可以被删除
                    shouldDeleteSegment = metadata.segmentLeaderEpochs().keySet().stream().allMatch(epoch -> epoch <= firstEpoch)
                            && metadata.endOffset() < logStartOffset;
                }
                if (shouldDeleteSegment) {
                    logger.info("About to delete remote log segment {} due to log-start-offset {} breach. " +
                            "Current earliest-epoch-entry: {}, segment-end-offset: {} and segment-epochs: {}",
                            metadata.remoteLogSegmentId(), logStartOffset, leaderEpochEntries.firstEntry(),
                            metadata.endOffset(), metadata.segmentLeaderEpochs());
                }
                return shouldDeleteSegment;
            }

            /**
             * 删除当前领导者最早纪元之前的日志段。这些日志段被认为是未引用的，因为它们不属于当前领导者纪元的谱系。
             * 这个方法主要用于处理非正常领导者选举场景，因为远程存储中可能存在早于当前领导者最早纪元的日志段。
             *
             * @param earliestEpochEntry 当前领导者的最早纪元条目
             * @param metadata 要删除的远程日志段元数据
             * @return 如果日志段被成功删除则返回true，否则返回false
             */
            private boolean deleteLogSegmentsDueToLeaderEpochCacheTruncation(EpochEntry earliestEpochEntry,
                                                                             RemoteLogSegmentMetadata metadata)
                    throws RemoteStorageException, ExecutionException, InterruptedException {
                // 检查日志段中的所有纪元是否都小于当前领导者的最早纪元
                boolean isSegmentDeleted = deleteRemoteLogSegment(metadata, 
                    ignored -> metadata.segmentLeaderEpochs().keySet().stream().allMatch(epoch -> epoch < earliestEpochEntry.epoch));
                
                // 如果日志段被成功删除，记录相关信息
                if (isSegmentDeleted) {
                    logger.info("Deleted remote log segment {} due to leader-epoch-cache truncation. " +
                                    "Current earliest-epoch-entry: {}, segment-end-offset: {} and segment-epochs: {}",
                            metadata.remoteLogSegmentId(), earliestEpochEntry, metadata.endOffset(), metadata.segmentLeaderEpochs().keySet());
                }
                // 无需更新日志起始偏移量，因为这些纪元/偏移量早于该值
                return isSegmentDeleted;
            }
        }

        /**
         * 更新远程日志元数据统计信息
         * 
         * @param metadataCount 远程日志段元数据的数量
         * @param remoteLogSizeBytes 远程日志段的总大小(字节)
         */
        private void updateMetadataCountAndLogSizeWith(int metadataCount, long remoteLogSizeBytes) {
            // 获取主题分区信息
            int partition = topicIdPartition.partition();
            String topic = topicIdPartition.topic();
            
            // 记录远程日志元数据数量和总大小
            brokerTopicStats.recordRemoteLogMetadataCount(topic, partition, metadataCount);
            brokerTopicStats.recordRemoteLogSizeBytes(topic, partition, remoteLogSizeBytes);
        }

        /**
         * 更新远程日志删除延迟统计信息
         * 
         * @param segmentsLeftToDelete 待删除的日志段数量
         * @param sizeOfDeletableSegmentsBytes 待删除日志段的总大小(字节)
         */
        private void updateRemoteDeleteLagWith(int segmentsLeftToDelete, long sizeOfDeletableSegmentsBytes) {
            // 获取主题分区信息
            String topic = topicIdPartition.topic();
            int partition = topicIdPartition.partition();
            
            // 记录待删除的日志段数量和总大小
            brokerTopicStats.recordRemoteDeleteLagSegments(topic, partition, segmentsLeftToDelete);
            brokerTopicStats.recordRemoteDeleteLagBytes(topic, partition, sizeOfDeletableSegmentsBytes);
        }

        /**
         * 清理过期和悬挂的远程日志段。
         * 该方法主要负责以下功能:
         * 1. 清理超过保留时间或保留大小的远程日志段
         * 2. 清理由于日志起始偏移量变更导致的过期日志段
         * 3. 处理非正常领导者选举场景下的日志段清理
         * 4. 重试清理之前删除失败的悬挂日志段
         * 
         * 设计考虑:
         * - 使用RemoteLogRetentionHandler处理不同的保留策略
         * - 通过领导者纪元缓存处理日志截断和非正常领导者选举场景
         * - 实现幂等的删除操作，确保在副本切换场景下的正确性
         * - 维护准确的指标统计，包括删除延迟和大小统计
         */
        void cleanupExpiredRemoteLogSegments() throws RemoteStorageException, ExecutionException, InterruptedException {
            // 如果任务已被取消，则直接返回
            if (isCancelled()) {
                logger.info("Returning from remote log segments cleanup as the task state is changed");
                return;
            }

            // 获取指定主题分区的UnifiedLog实例
            final Optional<UnifiedLog> logOptional = fetchLog.apply(topicIdPartition.topicPartition());
            if (logOptional.isEmpty()) {
                logger.debug("No UnifiedLog instance available for partition: {}", topicIdPartition);
                return;
            }

            final UnifiedLog log = logOptional.get();

            // 获取远程日志段的元数据迭代器
            final Iterator<RemoteLogSegmentMetadata> segmentMetadataIter = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition);
            if (!segmentMetadataIter.hasNext()) {
                // 如果没有远程日志段，更新元数据计数和日志大小为0
                updateMetadataCountAndLogSizeWith(0, 0);
                logger.debug("No remote log segments available on remote storage for partition: {}", topicIdPartition);
                return;
            }

            // 收集所有远程日志段的领导者纪元信息
            final Set<Integer> epochsSet = new HashSet<>();
            int metadataCount = 0;
            long remoteLogSizeBytes = 0;
            // 遍历所有远程日志段，统计元数据数量和总大小
            // TODO: 考虑添加RLMM API直接获取分区的所有远程领导者纪元，避免遍历所有日志段
            while (segmentMetadataIter.hasNext()) {
                RemoteLogSegmentMetadata segmentMetadata = segmentMetadataIter.next();
                // 收集日志段包含的所有领导者纪元
                epochsSet.addAll(segmentMetadata.segmentLeaderEpochs().keySet());
                metadataCount++;
                remoteLogSizeBytes += segmentMetadata.segmentSizeInBytes();
            }

            // 更新远程日志的元数据计数和总大小
            updateMetadataCountAndLogSizeWith(metadataCount, remoteLogSizeBytes);

            // 将远程存储中的领导者纪元按顺序排序
            final List<Integer> remoteLeaderEpochs = new ArrayList<>(epochsSet);
            Collections.sort(remoteLeaderEpochs);

            // 获取领导者纪元缓存并构建过滤后的纪元-偏移量映射
            LeaderEpochFileCache leaderEpochCache = log.leaderEpochCache();
            NavigableMap<Integer, Long> epochWithOffsets = buildFilteredLeaderEpochMap(leaderEpochCache.epochWithOffsets());

            // 获取日志的起始和结束偏移量
            long logStartOffset = log.logStartOffset();
            long logEndOffset = log.logEndOffset();
            // 构建基于大小和时间的保留策略数据
            Optional<RetentionSizeData> retentionSizeData = buildRetentionSizeData(log.config().retentionSize,
                    log.onlyLocalLogSegmentsSize(), logEndOffset, epochWithOffsets);
            Optional<RetentionTimeData> retentionTimeData = buildRetentionTimeData(log.config().retentionMs);

            // 创建远程日志保留处理器，用于处理基于大小和时间的保留策略
            RemoteLogRetentionHandler remoteLogRetentionHandler = new RemoteLogRetentionHandler(retentionSizeData, retentionTimeData);
            
            // 遍历所有领导者纪元，检查每个纪元下的日志段
            Iterator<Integer> epochIterator = epochWithOffsets.navigableKeySet().iterator();
            boolean canProcess = true; // 控制是否继续处理后续日志段
            List<RemoteLogSegmentMetadata> segmentsToDelete = new ArrayList<>(); // 待删除的日志段列表
            long sizeOfDeletableSegmentsBytes = 0L; // 待删除日志段的总大小
            
            while (canProcess && epochIterator.hasNext()) {
                Integer epoch = epochIterator.next();
                // 获取当前纪元下的所有远程日志段
                Iterator<RemoteLogSegmentMetadata> segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
                
                while (canProcess && segmentsIterator.hasNext()) {
                    // 检查任务是否被取消
                    if (isCancelled()) {
                        logger.info("Returning from remote log segments cleanup for the remaining segments as the task state is changed.");
                        return;
                    }
                    RemoteLogSegmentMetadata metadata = segmentsIterator.next();

                    // 如果日志段正在被复制，跳过当前及后续日志段的处理
                    if (segmentIdsBeingCopied.contains(metadata.remoteLogSegmentId())) {
                        logger.debug("Copy for the segment {} is currently in process. Skipping cleanup for it and the remaining segments",
                                metadata.remoteLogSegmentId());
                        canProcess = false;
                        continue;
                    }
                    
                    // 处理之前删除失败的悬挂日志段
                    // 这里作为重试机制，不等待保留策略触发，提前清理以避免缓存污染和浪费远程存储空间
                    if (RemoteLogSegmentState.DELETE_SEGMENT_STARTED.equals(metadata.state())) {
                        segmentsToDelete.add(metadata);
                        continue;
                    }
                    
                    // 跳过已完成删除的日志段
                    if (RemoteLogSegmentState.DELETE_SEGMENT_FINISHED.equals(metadata.state())) {
                        continue;
                    }
                    
                    // 跳过已添加到待删除列表的日志段
                    if (segmentsToDelete.contains(metadata)) {
                        continue;
                    }
                    
                    // 检查日志段是否因日志起始偏移量变更而需要删除
                    // 当用户移动日志起始偏移量时，领导者纪元检查点文件会被截断
                    // 在RLM清理线程的下一次迭代之前，这些远程日志段不会被删除
                    // isRemoteSegmentWithinLeaderEpoch验证日志段的纪元是否在检查点文件中
                    // 由于检查点文件已被截断，该方法将始终返回false
                    boolean shouldDeleteSegment = remoteLogRetentionHandler.isSegmentBreachByLogStartOffset(
                            metadata, logStartOffset, epochWithOffsets);
                    boolean isValidSegment = false;
                    
                    if (!shouldDeleteSegment) {
                        // 检查日志段是否在当前领导者纪元序列的有效范围内
                        isValidSegment = isRemoteSegmentWithinLeaderEpochs(metadata, logEndOffset, epochWithOffsets);
                        if (isValidSegment) {
                            // 检查日志段是否超过保留时间或保留大小限制
                            shouldDeleteSegment =
                                    remoteLogRetentionHandler.isSegmentBreachedByRetentionTime(metadata) ||
                                            remoteLogRetentionHandler.isSegmentBreachedByRetentionSize(metadata);
                        }
                    }
                    
                    // 如果需要删除日志段，添加到待删除列表并更新总大小
                    if (shouldDeleteSegment) {
                        segmentsToDelete.add(metadata);
                        sizeOfDeletableSegmentsBytes += metadata.segmentSizeInBytes();
                    }
                    
                    // 只有当日志段需要删除或不在有效范围内时才继续处理后续日志段
                    canProcess = shouldDeleteSegment || !isValidSegment;
                }
            }

            // 使用计算得到的值更新日志起始偏移量
            remoteLogRetentionHandler.logStartOffset.ifPresent(offset -> handleLogStartOffsetUpdate(topicIdPartition.topicPartition(), offset));

            // 此时我们已更新了日志起始偏移量，但尚未开始删除操作
            // 追随者副本可能已经获取了新的日志起始偏移量，也可能尚未获取
            // 场景1: 如果追随者已获取变更并成为新的领导者，当前副本将无法完成删除操作
            //       但新的领导者会将所有违反起始偏移量的日志段标记为需要删除，并相应地删除它们
            // 场景2: 如果追随者尚未获取变更并成为新的领导者，它将重新执行此过程
            //       并基于原始原因(大小、时间或起始偏移量违规)删除这些日志段
            int segmentsLeftToDelete = segmentsToDelete.size();
            updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
            List<String> undeletedSegments = new ArrayList<>();
            
            // 执行日志段删除操作
            for (RemoteLogSegmentMetadata segmentMetadata : segmentsToDelete) {
                // 尝试删除日志段，如果删除失败则记录到未删除列表
                if (!deleteRemoteLogSegment(segmentMetadata, ignored -> !isCancelled())) {
                    undeletedSegments.add(segmentMetadata.remoteLogSegmentId().toString());
                } else {
                    // 删除成功后更新待删除日志段的大小和数量统计
                    sizeOfDeletableSegmentsBytes -= segmentMetadata.segmentSizeInBytes();
                    segmentsLeftToDelete--;
                    updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                }
            }
            
            // 记录未能成功删除的日志段
            if (!undeletedSegments.isEmpty()) {
                logger.info("The following remote segments could not be deleted: {}", String.join(",", undeletedSegments));
            }

            // 处理非正常领导者选举场景
            // 删除远程存储中早于当前领导者最早纪元的日志段
            // 这些日志段在远程存储中已不再被引用，通常出现在非正常领导者选举场景中
            // 因为远程存储可能包含早于当前领导者最早纪元的日志段
            Optional<EpochEntry> earliestEpochEntryOptional = leaderEpochCache.earliestEntry();
            if (earliestEpochEntryOptional.isPresent()) {
                EpochEntry earliestEpochEntry = earliestEpochEntryOptional.get();
                // 筛选出早于当前领导者最早纪元的所有远程纪元
                Iterator<Integer> epochsToClean = remoteLeaderEpochs.stream()
                        .filter(remoteEpoch -> remoteEpoch < earliestEpochEntry.epoch)
                        .iterator();

                // 收集需要清理的日志段
                List<RemoteLogSegmentMetadata> listOfSegmentsToBeCleaned = new ArrayList<>();

                while (epochsToClean.hasNext()) {
                    int epoch = epochsToClean.next();
                    // 获取当前纪元下的所有日志段
                    Iterator<RemoteLogSegmentMetadata> segmentsToBeCleaned = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
                    while (segmentsToBeCleaned.hasNext()) {
                        if (!isCancelled()) {
                            RemoteLogSegmentMetadata nextSegmentMetadata = segmentsToBeCleaned.next();
                            sizeOfDeletableSegmentsBytes += nextSegmentMetadata.segmentSizeInBytes();
                            listOfSegmentsToBeCleaned.add(nextSegmentMetadata);
                        }
                    }
                }

                // 更新待删除日志段的统计信息
                segmentsLeftToDelete += listOfSegmentsToBeCleaned.size();
                updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                
                // 执行日志段删除
                for (RemoteLogSegmentMetadata segmentMetadata : listOfSegmentsToBeCleaned) {
                    if (!isCancelled()) {
                        // 由于这些纪元/偏移量早于当前的日志起始偏移量，删除日志段时无需更新起始偏移量
                        if (remoteLogRetentionHandler.deleteLogSegmentsDueToLeaderEpochCacheTruncation(earliestEpochEntry, segmentMetadata)) {
                            // 更新删除进度统计
                            sizeOfDeletableSegmentsBytes -= segmentMetadata.segmentSizeInBytes();
                            segmentsLeftToDelete--;
                            updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                        }
                    }
                }
            }
        }

        /**
         * 构建基于时间的保留策略数据
         * 
         * @param retentionMs 日志保留时间(毫秒)，-1表示不启用基于时间的保留策略
         * @return 如果启用了基于时间的保留策略且计算的清理时间点有效，则返回包含保留时间和清理截止时间的RetentionTimeData对象
         */
        private Optional<RetentionTimeData> buildRetentionTimeData(long retentionMs) {
            // 计算清理截止时间点：当前时间减去保留时间
            long cleanupUntilMs = time.milliseconds() - retentionMs;
            // 只有当保留时间大于-1(启用策略)且清理时间点大于等于0时才返回保留数据
            return retentionMs > -1 && cleanupUntilMs >= 0
                    ? Optional.of(new RetentionTimeData(retentionMs, cleanupUntilMs))
                    : Optional.empty();
        }

        /**
         * 构建基于大小的保留策略数据
         * 
         * @param retentionSize 日志保留大小(字节)，-1表示不启用基于大小的保留策略
         * @param onlyLocalLogSegmentsSize 本地日志段的总大小(字节)
         * @param logEndOffset 日志的结束偏移量
         * @param epochEntries 领导者纪元与对应偏移量的映射
         * @return 如果启用了基于大小的保留策略且总大小超过保留大小，则返回包含保留大小和超出大小的RetentionSizeData对象
         * @throws RemoteStorageException 当访问远程存储出错时抛出
         */
        private Optional<RetentionSizeData> buildRetentionSizeData(long retentionSize,
                                                                   long onlyLocalLogSegmentsSize,
                                                                   long logEndOffset,
                                                                   NavigableMap<Integer, Long> epochEntries) throws RemoteStorageException {
            // 如果设置了保留大小限制，则计算远程日志段的总大小并与限制进行比较
            if (retentionSize > -1) {
                // 记录开始时间，用于统计计算耗时
                long startTimeMs = time.milliseconds();
                // 远程日志段总大小
                long remoteLogSizeBytes = 0L;
                // 用于记录已访问的日志段ID，避免重复计算
                Set<RemoteLogSegmentId> visitedSegmentIds = new HashSet<>();
                // 遍历每个领导者纪元
                for (Integer epoch : epochEntries.navigableKeySet()) {
                    // 注意：remoteLogSize(topicIdPartition, epochEntry.epoch)可能不完全准确
                    // 因为远程日志大小可能是针对所有段计算的，而不是仅针对当前分区的领导者纪元序列中的段
                    // 这个API可能需要重新设计
                    // remoteLogSizeBytes += remoteLogMetadataManager.remoteLogSize(topicIdPartition, epochEntry.epoch);
                    
                    // 获取该纪元下的所有远程日志段元数据
                    Iterator<RemoteLogSegmentMetadata> segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
                    while (segmentsIterator.hasNext()) {
                        RemoteLogSegmentMetadata segmentMetadata = segmentsIterator.next();
                        // 只计算状态为"COPY_SEGMENT_FINISHED"的段的大小，因为：
                        // - "COPY_SEGMENT_STARTED"表示复制未完成，稍后会计算
                        // - "DELETE_SEGMENT_STARTED"表示之前的删除尝试失败，稍后会重试
                        // - "DELETE_SEGMENT_FINISHED"表示删除已完成，无需计算
                        if (segmentMetadata.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED)) {
                            RemoteLogSegmentId segmentId = segmentMetadata.remoteLogSegmentId();
                            // 检查日志段是否未被访问过且在领导者纪元范围内
                            if (!visitedSegmentIds.contains(segmentId) && isRemoteSegmentWithinLeaderEpochs(segmentMetadata, logEndOffset, epochEntries)) {
                                // 累加日志段大小
                                remoteLogSizeBytes += segmentMetadata.segmentSizeInBytes();
                                // 将日志段ID添加到已访问集合
                                visitedSegmentIds.add(segmentId);
                            }
                        }
                    }
                }

                // 记录远程日志大小计算耗时
                brokerTopicStats.recordRemoteLogSizeComputationTime(topicIdPartition.topic(), topicIdPartition.partition(), time.milliseconds() - startTimeMs);

                // 计算总大小：本地日志段(基准偏移量>本地日志起始偏移量)的大小 + 
                // 远程存储中的日志段(结束偏移量<本地日志起始偏移量)的大小
                long totalSize = onlyLocalLogSegmentsSize + remoteLogSizeBytes;
                // 如果总大小超过保留大小限制
                if (totalSize > retentionSize) {
                    // 计算超出的大小
                    long remainingBreachedSize = totalSize - retentionSize;
                    // 创建保留大小数据对象
                    RetentionSizeData retentionSizeData = new RetentionSizeData(retentionSize, remainingBreachedSize);
                    return Optional.of(retentionSizeData);
                }
            }

            // 如果未设置保留大小限制或总大小未超过限制，则返回空
            return Optional.empty();
        }
    }

    /**
     * 追随者任务类，负责处理追随者副本的远程日志管理任务
     */
    class RLMFollowerTask extends RLMTask {

        /**
         * 构造函数
         * @param topicIdPartition 主题分区标识符
         */
        public RLMFollowerTask(TopicIdPartition topicIdPartition) {
            super(topicIdPartition);
        }

        /**
         * 执行追随者任务
         * 主要职责是找到远程存储中最高的偏移量，并更新本地日志的远程存储最高偏移量
         * 这样可以确保本地日志段在复制到远程存储之前不会被删除
         */
        @Override
        protected void execute(UnifiedLog log) throws InterruptedException, RemoteStorageException, ExecutionException {
            // 查找远程存储中的最高偏移量
            OffsetAndEpoch offsetAndEpoch = findHighestRemoteOffset(topicIdPartition, log);
            // 更新分区日志中的远程存储最高偏移量，确保本地日志段在复制到远程存储之前不会被删除
            log.updateHighestOffsetInRemoteStorage(offsetAndEpoch.offset());
        }
    }
    
    /**
     * 删除远程日志段
     * 
     * @param segmentMetadata 要删除的远程日志段元数据
     * @param predicate 用于判断是否应该删除该日志段的谓词函数
     * @return 如果日志段被成功删除则返回true，否则返回false
     */
    private boolean deleteRemoteLogSegment(
        RemoteLogSegmentMetadata segmentMetadata,
        Predicate<RemoteLogSegmentMetadata> predicate
    ) throws RemoteStorageException, ExecutionException, InterruptedException {
        // 使用谓词函数判断是否应该删除该日志段
        if (predicate.test(segmentMetadata)) {
            LOGGER.debug("Deleting remote log segment {}", segmentMetadata.remoteLogSegmentId());
            String topic = segmentMetadata.topicIdPartition().topic();

            // 发布删除段开始事件，更新远程日志段状态为DELETE_SEGMENT_STARTED
            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
                new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
                    segmentMetadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId)).get();

            // 更新删除请求的统计指标
            brokerTopicStats.topicStats(topic).remoteDeleteRequestRate().mark();
            brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().mark();

            // 在远程存储中删除日志段数据
            try {
                remoteLogStorageManager.deleteLogSegmentData(segmentMetadata);
            } catch (RemoteStorageException e) {
                // 如果删除失败，更新失败统计指标
                brokerTopicStats.topicStats(topic).failedRemoteDeleteRequestRate().mark();
                brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().mark();
                throw e;
            }

            // 发布删除段完成事件，更新远程日志段状态为DELETE_SEGMENT_FINISHED
            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
                new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
                    segmentMetadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId)).get();
            LOGGER.debug("Deleted remote log segment {}", segmentMetadata.remoteLogSegmentId());
            return true;
        }
        return false;
    }

    /**
     * 检查远程日志段的纪元/偏移量是否在分区的领导者纪元序列范围内
     * 
     * 约束条件如下：
     * 1. 日志段的第一个纪元的偏移量应大于或等于分区领导者纪元序列中相应领导者纪元的偏移量
     * 2. 日志段的结束偏移量应小于或等于分区领导者纪元序列中相应领导者纪元的偏移量
     * 3. 除了日志段中的第一个和最后一个纪元外，日志段的纪元序列(纪元和偏移量)应与领导者纪元序列(纪元和偏移量)相同
     *
     * @param segmentMetadata 需要验证的远程日志段元数据
     * @param logEndOffset 分区的日志结束偏移量
     * @param leaderEpochs 通过过滤不包含数据的纪元得到的分区领导者纪元序列
     * @return 如果远程日志段的纪元/偏移量在分区的领导者纪元序列范围内则返回true
     */
    // 用于测试的可见性
    static boolean isRemoteSegmentWithinLeaderEpochs(RemoteLogSegmentMetadata segmentMetadata,
                                                     long logEndOffset,
                                                     NavigableMap<Integer, Long> leaderEpochs) {
        // 获取日志段的结束偏移量
        long segmentEndOffset = segmentMetadata.endOffset();
        // 过滤掉没有关联消息/记录的纪元
        NavigableMap<Integer, Long> segmentLeaderEpochs = buildFilteredLeaderEpochMap(segmentMetadata.segmentLeaderEpochs());
        
        // 检查日志段纪元是否在当前领导者纪元范围内
        Integer segmentLastEpoch = segmentLeaderEpochs.lastKey();
        if (segmentLastEpoch < leaderEpochs.firstKey() || segmentLastEpoch > leaderEpochs.lastKey()) {
            LOGGER.debug("Segment {} is not within the partition leader epoch lineage. " +
                            "Remote segment epochs: {} and partition leader epochs: {}",
                    segmentMetadata.remoteLogSegmentId(), segmentLeaderEpochs, leaderEpochs);
            return false;
        }

        // 远程存储中可能存在重叠的日志段，例如：
        // leader-epoch-file-cache: {(5, 10), (7, 15), (9, 100)}
        // segment1: offset-range = 5-50, Broker = 0, epochs = {(5, 10), (7, 15)}
        // segment2: offset-range = 14-150, Broker = 1, epochs = {(5, 14), (7, 15), (9, 100)}, 在领导者选举之后
        // 当segment1被删除时，log-start-offset = 51，leader-epoch-file-cache更新为：{(7, 51), (9, 100)}
        // 在验证segment2时，我们需要确保处理重叠的远程日志段情况
        Integer segmentFirstEpoch = segmentLeaderEpochs.ceilingKey(leaderEpochs.firstKey());
        if (segmentFirstEpoch == null) {
            LOGGER.debug("Segment {} is not within the partition leader epoch lineage. " +
                            "Remote segment epochs: {} and partition leader epochs: {}",
                    segmentMetadata.remoteLogSegmentId(), segmentLeaderEpochs, leaderEpochs);
            return false;
        }

        // 遍历日志段的每个纪元条目
        for (Map.Entry<Integer, Long> entry : segmentLeaderEpochs.entrySet()) {
            int epoch = entry.getKey();
            long offset = entry.getValue();
            
            // 跳过早于第一个有效纪元的纪元
            if (epoch < segmentFirstEpoch) {
                continue;
            }

            // 如果日志段的纪元不存在于领导者纪元序列中，则该段无效
            if (!leaderEpochs.containsKey(epoch)) {
                LOGGER.debug("Segment {} epoch {} is not within the leader epoch lineage. " +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, segmentLeaderEpochs, leaderEpochs);
                return false;
            }

            // 处理两种情况：
            // 情况1：当段的第一个纪元等于领导者纪元序列中的第一个纪元时，
            // 偏移量可以在0到(下一个纪元起始偏移量 - 1)之间的任何位置
            // 情况2：当段的第一个纪元不等于领导者纪元序列中的第一个纪元时，
            // 偏移量应该在(当前纪元起始偏移量)到(下一个纪元起始偏移量 - 1)之间
            if (epoch == segmentFirstEpoch && leaderEpochs.lowerKey(epoch) != null && offset < leaderEpochs.get(epoch)) {
                LOGGER.debug("Segment {} first-valid epoch {} offset is less than first leader epoch offset {}." +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, leaderEpochs.get(epoch),
                        segmentLeaderEpochs, leaderEpochs);
                return false;
            }

            // 日志段的结束偏移量应小于或等于相应领导者纪元的偏移量
            if (epoch == segmentLastEpoch) {
                Map.Entry<Integer, Long> nextEntry = leaderEpochs.higherEntry(epoch);
                if (nextEntry != null && segmentEndOffset > nextEntry.getValue() - 1) {
                    LOGGER.debug("Segment {} end offset {} is more than leader epoch offset {}." +
                                    "Remote segment epochs: {} and partition leader epochs: {}",
                            segmentMetadata.remoteLogSegmentId(), segmentEndOffset, nextEntry.getValue() - 1,
                            segmentLeaderEpochs, leaderEpochs);
                    return false;
                }
            }

            // 下一个段纪元条目和下一个领导者纪元条目应该相同，以确保段的纪元在领导者纪元序列内
            if (epoch != segmentLastEpoch && !leaderEpochs.higherEntry(epoch).equals(segmentLeaderEpochs.higherEntry(epoch))) {
                LOGGER.debug("Segment {} epoch {} is not within the leader epoch lineage. " +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, segmentLeaderEpochs, leaderEpochs);
                return false;
            }
        }

        // 日志段的结束偏移量应该在日志结束偏移量之内
        if (segmentEndOffset >= logEndOffset) {
            LOGGER.debug("Segment {} end offset {} is more than log end offset {}.",
                    segmentMetadata.remoteLogSegmentId(), segmentEndOffset, logEndOffset);
            return false;
        }
        return true;
    }

    /**
     * 返回一个经过过滤的领导者纪元映射表，该映射表包含纪元号与起始偏移量的对应关系，
     * 会过滤掉那些没有关联消息/记录的纪元。
     * 
     * 例如，输入以下映射表:
     * <pre>
     * {@code
     *  <纪元号 - 起始偏移量>
     *  0 - 0
     *  1 - 10
     *  2 - 20
     *  3 - 30
     *  4 - 40
     *  5 - 60  // 纪元5没有关联的消息或记录
     *  6 - 60
     *  7 - 70
     * }
     * </pre>
     * 
     * 当上述leaderEpochMap传入此方法时，将返回以下映射表:
     * <pre>
     * {@code
     *  <纪元号 - 起始偏移量>
     *  0 - 0
     *  1 - 10
     *  2 - 20
     *  3 - 30
     *  4 - 40
     *  6 - 60
     *  7 - 70
     * }
     * </pre>
     * 
     * @param leaderEpochs 需要进行过滤的领导者纪元映射表
     * @return 过滤后的领导者纪元映射表
     */
    // 用于测试
    static NavigableMap<Integer, Long> buildFilteredLeaderEpochMap(NavigableMap<Integer, Long> leaderEpochs) {
        // 存储没有消息的纪元号列表
        List<Integer> epochsWithNoMessages = new ArrayList<>();
        // 用于比较的前一个纪元和偏移量对
        Map.Entry<Integer, Long> previousEpochAndOffset = null;
        
        // 遍历所有的纪元和偏移量对
        for (Map.Entry<Integer, Long> currentEpochAndOffset : leaderEpochs.entrySet()) {
            // 如果前一个纪元的偏移量等于当前纪元的偏移量，说明前一个纪元没有消息
            if (previousEpochAndOffset != null && previousEpochAndOffset.getValue().equals(currentEpochAndOffset.getValue())) {
                epochsWithNoMessages.add(previousEpochAndOffset.getKey());
            }
            previousEpochAndOffset = currentEpochAndOffset;
        }
        
        // 如果没有需要过滤的纪元，直接返回原始映射表
        if (epochsWithNoMessages.isEmpty()) {
            return leaderEpochs;
        }
        
        // 创建新的映射表并移除没有消息的纪元
        TreeMap<Integer, Long> filteredLeaderEpochs = new TreeMap<>(leaderEpochs);
        for (Integer epochWithNoMessage : epochsWithNoMessages) {
            filteredLeaderEpochs.remove(epochWithNoMessage);
        }
        return filteredLeaderEpochs;
    }

    /**
     * 从远程存储中读取指定偏移量的消息数据
     * 
     * @param remoteStorageFetchInfo 远程存储获取信息，包含获取请求的相关参数
     * @return 获取的数据信息，包含消息记录和元数据
     * @throws RemoteStorageException 远程存储操作异常
     * @throws IOException IO操作异常
     */
    public FetchDataInfo read(RemoteStorageFetchInfo remoteStorageFetchInfo) throws RemoteStorageException, IOException {
        // 获取请求参数
        int fetchMaxBytes = remoteStorageFetchInfo.fetchMaxBytes;
        TopicPartition tp = remoteStorageFetchInfo.topicPartition;
        FetchRequest.PartitionData fetchInfo = remoteStorageFetchInfo.fetchInfo;

        // 判断是否需要包含已中止的事务信息
        boolean includeAbortedTxns = remoteStorageFetchInfo.fetchIsolation == FetchIsolation.TXN_COMMITTED;

        // 获取目标偏移量和最大字节数
        long offset = fetchInfo.fetchOffset;
        int maxBytes = Math.min(fetchMaxBytes, fetchInfo.maxBytes);

        // 获取日志实例和对应的纪元信息
        Optional<UnifiedLog> logOptional = fetchLog.apply(tp);
        OptionalInt epoch = OptionalInt.empty();

        if (logOptional.isPresent()) {
            LeaderEpochFileCache leaderEpochCache = logOptional.get().leaderEpochCache();
            epoch = leaderEpochCache.epochForOffset(offset);
        }

        // 获取远程日志段元数据
        Optional<RemoteLogSegmentMetadata> rlsMetadataOptional = epoch.isPresent()
                ? fetchRemoteLogSegmentMetadata(tp, epoch.getAsInt(), offset)
                : Optional.empty();

        // 如果找不到对应的远程日志段，抛出异常
        if (rlsMetadataOptional.isEmpty()) {
            String epochStr = (epoch.isPresent()) ? Integer.toString(epoch.getAsInt()) : "NOT AVAILABLE";
            throw new OffsetOutOfRangeException("Received request for offset " + offset + " for leader epoch "
                    + epochStr + " and partition " + tp + " which does not exist in remote tier.");
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = rlsMetadataOptional.get();
        EnrichedRecordBatch enrichedRecordBatch = new EnrichedRecordBatch(null, 0);
        InputStream remoteSegInputStream = null;
        try {
            int startPos = 0;
            // 由于日志压缩的存在，可能需要遍历多个远程日志段元数据
            // 当前日志段中的偏移量可能已被压缩，需要查找下一个日志段以获取大于给定偏移量的消息
            while (enrichedRecordBatch.batch == null && rlsMetadataOptional.isPresent()) {
                remoteLogSegmentMetadata = rlsMetadataOptional.get();
                // 查找大于等于目标偏移量的最后一个位置
                startPos = lookupPositionForOffset(remoteLogSegmentMetadata, offset);
                remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(remoteLogSegmentMetadata, startPos);
                RemoteLogInputStream remoteLogInputStream = getRemoteLogInputStream(remoteSegInputStream);
                enrichedRecordBatch = findFirstBatch(remoteLogInputStream, offset);
                if (enrichedRecordBatch.batch == null) {
                    Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
                    rlsMetadataOptional = findNextSegmentMetadata(rlsMetadataOptional.get(), logOptional.get().leaderEpochCache());
                }
            }
            
            // 获取第一个消息批次
            RecordBatch firstBatch = enrichedRecordBatch.batch;
            if (firstBatch == null)
                return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY, false,
                        includeAbortedTxns ? Optional.of(Collections.emptyList()) : Optional.empty());

            int firstBatchSize = firstBatch.sizeInBytes();
            // 在以下情况下返回空记录而不是不完整的批次：
            // - 没有最小一条消息的约束
            // - 第一个批次大小超过可发送的最大字节数
            // - FetchRequest版本3或以上
            if (!remoteStorageFetchInfo.minOneMessage &&
                    !remoteStorageFetchInfo.hardMaxBytesLimit &&
                    firstBatchSize > maxBytes) {
                return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY);
            }

            // 计算实际的获取大小
            int updatedFetchSize =
                    remoteStorageFetchInfo.minOneMessage && firstBatchSize > maxBytes ? firstBatchSize : maxBytes;

            // 分配缓冲区并写入数据
            ByteBuffer buffer = ByteBuffer.allocate(updatedFetchSize);
            int remainingBytes = updatedFetchSize;

            firstBatch.writeTo(buffer);
            remainingBytes -= firstBatchSize;

            // 如果还有剩余空间，继续读取数据
            if (remainingBytes > 0) {
                // 读取输入流直到遇到EOF或缓冲区已满
                Utils.readFully(remoteSegInputStream, buffer);
            }
            buffer.flip();

            // 更新起始位置并创建获取数据信息
            startPos = startPos + enrichedRecordBatch.skippedBytes;
            FetchDataInfo fetchDataInfo = new FetchDataInfo(
                    new LogOffsetMetadata(firstBatch.baseOffset(), remoteLogSegmentMetadata.startOffset(), startPos),
                    MemoryRecords.readableRecords(buffer));
                    
            // 如果需要，添加已中止的事务信息
            if (includeAbortedTxns) {
                fetchDataInfo = addAbortedTransactions(firstBatch.baseOffset(), remoteLogSegmentMetadata, fetchDataInfo, logOptional.get());
            }

            return fetchDataInfo;
        } finally {
            // 清理资源
            if (enrichedRecordBatch.batch != null) {
                Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
            }
        }
    }
    // for testing
    RemoteLogInputStream getRemoteLogInputStream(InputStream in) {
        return new RemoteLogInputStream(in);
    }

    // Visible for testing
    int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
        return indexCache.lookupOffset(remoteLogSegmentMetadata, offset);
    }

    /**
     * 为获取的数据添加已中止的事务信息
     * 
     * @param startOffset 起始偏移量
     * @param segmentMetadata 远程日志段元数据
     * @param fetchInfo 获取的数据信息
     * @param log 统一日志实例
     * @return 包含已中止事务信息的获取数据信息
     * @throws RemoteStorageException 远程存储操作异常
     */
    private FetchDataInfo addAbortedTransactions(long startOffset,
                                                 RemoteLogSegmentMetadata segmentMetadata,
                                                 FetchDataInfo fetchInfo,
                                                 UnifiedLog log) throws RemoteStorageException {
        // 获取已获取记录的大小
        int fetchSize = fetchInfo.records.sizeInBytes();
        // 创建起始偏移量位置信息
        OffsetPosition startOffsetPosition = new OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
                fetchInfo.fetchOffsetMetadata.relativePositionInSegment);

        // 从索引缓存中获取偏移量索引
        OffsetIndex offsetIndex = indexCache.getIndexEntry(segmentMetadata).offsetIndex();
        // 计算上界偏移量：如果找不到对应位置，则使用日志段结束偏移量加1
        long upperBoundOffset = offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize)
                .map(position -> position.offset).orElse(segmentMetadata.endOffset() + 1);

        // 创建已中止事务集合
        final Set<FetchResponseData.AbortedTransaction> abortedTransactions = new HashSet<>();

        // 创建事务累加器，用于收集已中止的事务
        Consumer<List<AbortedTxn>> accumulator =
                abortedTxns -> abortedTransactions.addAll(abortedTxns.stream()
                        .map(AbortedTxn::asAbortedTransaction).collect(Collectors.toList()));

        // 记录开始时间并收集已中止的事务
        long startTimeNs = time.nanoseconds();
        collectAbortedTransactions(startOffset, upperBoundOffset, segmentMetadata, accumulator, log);
        LOGGER.debug("Time taken to collect: {} aborted transactions for {} in {} ns", abortedTransactions.size(),
                segmentMetadata, time.nanoseconds() - startTimeNs);

        // 返回包含已中止事务信息的获取数据信息
        return new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
                fetchInfo.records,
                fetchInfo.firstEntryIncomplete,
                Optional.of(abortedTransactions.isEmpty() ? Collections.emptyList() : new ArrayList<>(abortedTransactions)));
    }

    /**
     * Collects the aborted transaction entries from the current and subsequent segments until the upper bound offset.
     * Note that the accumulated aborted transaction entries might contain duplicates as it collects the entries across
     * segments. We are relying on the client to discard the duplicates.
     * @param startOffset The start offset of the fetch request.
     * @param upperBoundOffset The upper bound offset of the fetch request.
     * @param segmentMetadata The current segment metadata.
     * @param accumulator The accumulator to collect the aborted transactions.
     * @param log The unified log instance.
     * @throws RemoteStorageException If an error occurs while fetching the remote log segment metadata.
     */
    /**
     * 收集当前段和后续段中的已中止事务条目，直到达到上限偏移量
     * 注意：由于跨段收集条目，累积的已中止事务条目可能包含重复项
     * 我们依赖客户端来丢弃重复项
     * 
     * 应用场景：
     * 1. 事务处理：收集已中止的事务信息
     * 2. 数据一致性：确保事务状态的正确性
     * 3. 日志清理：识别和处理已中止的事务
     * 
     * @param startOffset 获取请求的起始偏移量
     * @param upperBoundOffset 获取请求的上限偏移量
     * @param segmentMetadata 当前段的元数据
     * @param accumulator 收集已中止事务的累加器
     * @param log 统一日志实例
     * @throws RemoteStorageException 如果在获取远程日志段元数据时发生错误
     */
    private void collectAbortedTransactions(long startOffset,
                                            long upperBoundOffset,
                                            RemoteLogSegmentMetadata segmentMetadata,
                                            Consumer<List<AbortedTxn>> accumulator,
                                            UnifiedLog log) throws RemoteStorageException {
        // 从段元数据中获取主题分区信息
        TopicPartition tp = segmentMetadata.topicIdPartition().topicPartition();
        // 初始化搜索完成标志
        boolean isSearchComplete = false;
        // 获取领导者纪元缓存
        LeaderEpochFileCache leaderEpochCache = log.leaderEpochCache();
        // 将当前段元数据包装为Optional
        Optional<RemoteLogSegmentMetadata> currentMetadataOpt = Optional.of(segmentMetadata);
        
        // 循环处理直到搜索完成或没有更多段
        while (!isSearchComplete && currentMetadataOpt.isPresent()) {
            RemoteLogSegmentMetadata currentMetadata = currentMetadataOpt.get();
            // 获取事务索引
            Optional<TransactionIndex> txnIndexOpt = getTransactionIndex(currentMetadata);
            if (txnIndexOpt.isPresent()) {
                TransactionIndex txnIndex = txnIndexOpt.get();
                // 收集指定范围内的已中止事务
                TxnIndexSearchResult searchResult = txnIndex.collectAbortedTxns(startOffset, upperBoundOffset);
                // 将收集到的事务添加到累加器
                accumulator.accept(searchResult.abortedTransactions);
                isSearchComplete = searchResult.isComplete;
            }
            // 如果搜索未完成，查找下一个带有事务索引的段
            if (!isSearchComplete) {
                currentMetadataOpt = findNextSegmentWithTxnIndex(tp, currentMetadata.endOffset() + 1, leaderEpochCache);
            }
        }
        
        // 如果在远程段中未完成搜索，继续在本地段中搜索
        if (!isSearchComplete) {
            collectAbortedTransactionInLocalSegments(startOffset, upperBoundOffset, accumulator, log.logSegments().iterator());
        }
    }

    /**
     * 从远程段元数据中获取事务索引
     * 
     * @param currentMetadata 当前段的元数据
     * @return 事务索引的Optional包装
     */
    private Optional<TransactionIndex> getTransactionIndex(RemoteLogSegmentMetadata currentMetadata) {
        // 检查段是否包含事务索引
        return !currentMetadata.isTxnIdxEmpty() ?
                // 使用ofNullable以支持向后兼容性
                // 旧事件可能返回txnIdxEmpty为false，但事务索引可能在远程存储中不存在
                Optional.ofNullable(indexCache.getIndexEntry(currentMetadata).txnIndex()) : Optional.empty();
    }

    /**
     * 在本地日志段中收集已中止的事务
     * 
     * @param startOffset 起始偏移量
     * @param upperBoundOffset 上限偏移量
     * @param accumulator 事务累加器
     * @param localLogSegments 本地日志段迭代器
     */
    private void collectAbortedTransactionInLocalSegments(long startOffset,
                                                          long upperBoundOffset,
                                                          Consumer<List<AbortedTxn>> accumulator,
                                                          Iterator<LogSegment> localLogSegments) {
        // 遍历所有本地日志段
        while (localLogSegments.hasNext()) {
            // 获取当前段的事务索引
            TransactionIndex txnIndex = localLogSegments.next().txnIndex();
            if (txnIndex != null) {
                // 收集指定范围内的已中止事务
                TxnIndexSearchResult searchResult = txnIndex.collectAbortedTxns(startOffset, upperBoundOffset);
                // 将收集到的事务添加到累加器
                accumulator.accept(searchResult.abortedTransactions);
                // 如果搜索完成，直接返回
                if (searchResult.isComplete) {
                    return;
                }
            }
        }
    }

    // visible for testing.
    /**
     * 查找下一个段的元数据（用于测试）
     * 
     * @param segmentMetadata 当前段的元数据
     * @param leaderEpochFileCacheOption 领导者纪元文件缓存
     * @return 下一个段元数据的Optional包装
     * @throws RemoteStorageException 如果在获取远程日志段元数据时发生错误
     */
    Optional<RemoteLogSegmentMetadata> findNextSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata,
                                                               LeaderEpochFileCache leaderEpochFileCacheOption) throws RemoteStorageException {
        // 计算下一个段的基础偏移量
        long nextSegmentBaseOffset = segmentMetadata.endOffset() + 1;
        // 获取该偏移量对应的纪元
        OptionalInt epoch = leaderEpochFileCacheOption.epochForOffset(nextSegmentBaseOffset);
        // 如果找到对应的纪元，则获取远程日志段元数据
        return epoch.isPresent()
                ? fetchRemoteLogSegmentMetadata(segmentMetadata.topicIdPartition().topicPartition(), epoch.getAsInt(), nextSegmentBaseOffset)
                : Optional.empty();
    }

    /**
     * Returns the next segment metadata that contains the aborted transaction entries from the given offset.
     * Note that the search starts from the given (offset-for-epoch, offset) pair, when there are no segments contains
     * the transaction index in that epoch, then it proceeds to the next epoch (next-epoch, epoch-start-offset)
     * and the search ends when the segment metadata is found or the leader epoch cache is exhausted.
     * Note that the returned segment metadata may or may not contain the transaction index.
     * Visible for testing
     * @param tp The topic partition.
     * @param offset The offset to start the search.
     * @param leaderEpochCache The leader epoch file cache.
     * @return The next segment metadata that contains the transaction index. The transaction index may or may not exist
     * in that segment metadata which depends on the RLMM plugin implementation. The caller of this method should handle
     * for both the cases.
     * @throws RemoteStorageException If an error occurs while fetching the remote log segment metadata.
     */
    /**
     * 查找包含已中止事务条目的下一个段元数据
     * 
     * 实现说明：
     * 1. 从给定的(offset-for-epoch, offset)对开始搜索
     * 2. 如果当前纪元没有包含事务索引的段，则继续搜索下一个纪元
     * 3. 搜索在找到段元数据或领导者纪元缓存耗尽时结束
     * 
     * 应用场景：
     * - 事务处理：查找包含已中止事务的日志段
     * - 数据清理：识别需要处理的事务记录
     * - 一致性维护：确保事务状态的正确性
     *
     * @param tp 主题分区
     * @param offset 开始搜索的偏移量
     * @param leaderEpochCache 领导者纪元文件缓存
     * @return 包含事务索引的下一个段元数据
     */
    Optional<RemoteLogSegmentMetadata> findNextSegmentWithTxnIndex(TopicPartition tp,
                                                                   long offset,
                                                                   LeaderEpochFileCache leaderEpochCache) throws RemoteStorageException {
        OptionalInt initialEpochOpt = leaderEpochCache.epochForOffset(offset);
        if (initialEpochOpt.isEmpty()) {
            return Optional.empty();
        }
        int initialEpoch = initialEpochOpt.getAsInt();
        for (EpochEntry epochEntry : leaderEpochCache.epochEntries()) {
            if (epochEntry.epoch >= initialEpoch) {
                long startOffset = Math.max(epochEntry.startOffset, offset);
                Optional<RemoteLogSegmentMetadata> metadataOpt = fetchNextSegmentWithTxnIndex(tp, epochEntry.epoch, startOffset);
                if (metadataOpt.isPresent()) {
                    return metadataOpt;
                }
            }
        }
        return Optional.empty();
    }

    // Visible for testing
    /**
     * 在远程日志输入流中查找第一个批次
     * 
     * 实现说明：
     * - 跳过所有lastOffset小于目标偏移量的批次
     * - 记录跳过的字节数
     * 
     * 应用场景：
     * - 数据读取：定位特定偏移量的记录批次
     * - 性能优化：跟踪跳过的数据量
     */
    EnrichedRecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) throws IOException {
        int skippedBytes = 0;
        RecordBatch nextBatch = null;
        // Look for the batch which has the desired offset
        // We will always have a batch in that segment as it is a non-compacted topic.
        do {
            if (nextBatch != null) {
                skippedBytes += nextBatch.sizeInBytes();
            }
            nextBatch = remoteLogInputStream.nextBatch();
        } while (nextBatch != null && nextBatch.lastOffset() < offset);
        return new EnrichedRecordBatch(nextBatch, skippedBytes);
    }

    /**
     * 查找最高的远程偏移量
     * 
     * 实现说明：
     * - 从最新的纪元开始向前搜索
     * - 比较远程偏移量和本地结束偏移量
     * 
     * 应用场景：
     * - 日志同步：确定需要同步的数据范围
     * - 一致性检查：验证远程存储的数据完整性
     */
    OffsetAndEpoch findHighestRemoteOffset(TopicIdPartition topicIdPartition, UnifiedLog log) throws RemoteStorageException {
        OffsetAndEpoch offsetAndEpoch = null;
        LeaderEpochFileCache leaderEpochCache = log.leaderEpochCache();
        Optional<EpochEntry> maybeEpochEntry = leaderEpochCache.latestEntry();
        while (offsetAndEpoch == null && maybeEpochEntry.isPresent()) {
            int epoch = maybeEpochEntry.get().epoch;
            Optional<Long> highestRemoteOffsetOpt =
                    remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch);
            if (highestRemoteOffsetOpt.isPresent()) {
                Map.Entry<Integer, Long> entry = leaderEpochCache.endOffsetFor(epoch, log.logEndOffset());
                int requestedEpoch = entry.getKey();
                long endOffset = entry.getValue();
                long highestRemoteOffset = highestRemoteOffsetOpt.get();
                if (endOffset <= highestRemoteOffset) {
                    LOGGER.info("The end-offset for epoch {}: ({}, {}) is less than or equal to the " +
                            "highest-remote-offset: {} for partition: {}", epoch, requestedEpoch, endOffset,
                            highestRemoteOffset, topicIdPartition);
                    offsetAndEpoch = new OffsetAndEpoch(endOffset - 1, requestedEpoch);
                } else {
                    offsetAndEpoch = new OffsetAndEpoch(highestRemoteOffset, epoch);
                }
            }
            maybeEpochEntry = leaderEpochCache.previousEntry(epoch);
        }
        if (offsetAndEpoch == null) {
            offsetAndEpoch = new OffsetAndEpoch(-1L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
        }
        return offsetAndEpoch;
    }

    /**
     * 查找日志的起始偏移量
     * 
     * 实现说明：
     * - 从最早的纪元开始搜索
     * - 遍历远程日志段查找最小偏移量
     * 
     * 应用场景：
     * - 日志管理：确定可用数据的起始位置
     * - 数据清理：识别可以清理的数据范围
     */
    long findLogStartOffset(TopicIdPartition topicIdPartition, UnifiedLog log) throws RemoteStorageException {
        Optional<Long> logStartOffset = Optional.empty();
        LeaderEpochFileCache leaderEpochCache = log.leaderEpochCache();
        OptionalInt earliestEpochOpt = leaderEpochCache.earliestEntry()
                .map(epochEntry -> OptionalInt.of(epochEntry.epoch))
                .orElseGet(OptionalInt::empty);
        while (logStartOffset.isEmpty() && earliestEpochOpt.isPresent()) {
            Iterator<RemoteLogSegmentMetadata> iterator =
                    remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, earliestEpochOpt.getAsInt());
            if (iterator.hasNext()) {
                logStartOffset = Optional.of(iterator.next().startOffset());
            }
            earliestEpochOpt = leaderEpochCache.nextEpoch(earliestEpochOpt.getAsInt());
        }
        return logStartOffset.orElseGet(log::localLogStartOffset);
    }

    /**
     * Submit a remote log read task.
     * This method returns immediately. The read operation is executed in a thread pool.
     * The callback will be called when the task is done.
     *
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be accepted for execution (task queue is full)
     */
    /**
     * 提交异步远程日志读取任务
     * 
     * 实现说明：
     * - 立即返回Future对象
     * - 在线程池中执行实际的读取操作
     * 
     * 应用场景：
     * - 异步读取：提高系统吞吐量
     * - 资源管理：控制并发读取请求
     */
    public Future<Void> asyncRead(RemoteStorageFetchInfo fetchInfo, Consumer<RemoteLogReadResult> callback) {
        return remoteStorageReaderThreadPool.submit(
                new RemoteLogReader(fetchInfo, this, callback, brokerTopicStats, rlmFetchQuotaManager, remoteReadTimer));
    }

    /**
     * 处理领导者分区
     * 
     * 实现说明：
     * - 取消现有的追随者任务
     * - 创建新的复制和过期任务
     * 
     * 应用场景：
     * - 领导者切换：处理分区角色变更
     * - 任务管理：维护分区相关的后台任务
     */
    void doHandleLeaderPartition(TopicIdPartition topicPartition, Boolean remoteLogCopyDisable) {
        RLMTaskWithFuture followerRLMTaskWithFuture = followerRLMTasks.remove(topicPartition);
        if (followerRLMTaskWithFuture != null) {
            LOGGER.info("Cancelling the follower task: {}", followerRLMTaskWithFuture.rlmTask);
            followerRLMTaskWithFuture.cancel();
        }

        // Only create copy task when remoteLogCopyDisable is disabled
        if (!remoteLogCopyDisable) {
            leaderCopyRLMTasks.computeIfAbsent(topicPartition, topicIdPartition -> {
                RLMCopyTask task = new RLMCopyTask(topicIdPartition, this.rlmConfig.remoteLogMetadataCustomMetadataMaxBytes());
                // set this upfront when it is getting initialized instead of doing it after scheduling.
                LOGGER.info("Created a new copy task: {} and getting scheduled", task);
                ScheduledFuture<?> future = rlmCopyThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
                return new RLMTaskWithFuture(task, future);
            });
        }

        leaderExpirationRLMTasks.computeIfAbsent(topicPartition, topicIdPartition -> {
            RLMExpirationTask task = new RLMExpirationTask(topicIdPartition);
            LOGGER.info("Created a new expiration task: {} and getting scheduled", task);
            ScheduledFuture<?> future = rlmExpirationThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
            return new RLMTaskWithFuture(task, future);
        });
    }

    /**
     * 处理追随者分区
     * 
     * 实现说明：
     * - 取消现有的领导者任务
     * - 创建新的追随者任务
     * 
     * 应用场景：
     * - 角色切换：处理分区变为追随者
     * - 任务管理：维护分区相关的后台任务
     */
    void doHandleFollowerPartition(TopicIdPartition topicPartition) {
        RLMTaskWithFuture copyRLMTaskWithFuture = leaderCopyRLMTasks.remove(topicPartition);
        if (copyRLMTaskWithFuture != null) {
            LOGGER.info("Cancelling the copy task: {}", copyRLMTaskWithFuture.rlmTask);
            copyRLMTaskWithFuture.cancel();
        }

        RLMTaskWithFuture expirationRLMTaskWithFuture = leaderExpirationRLMTasks.remove(topicPartition);
        if (expirationRLMTaskWithFuture != null) {
            LOGGER.info("Cancelling the expiration task: {}", expirationRLMTaskWithFuture.rlmTask);
            expirationRLMTaskWithFuture.cancel();
        }

        followerRLMTasks.computeIfAbsent(topicPartition, topicIdPartition -> {
            RLMFollowerTask task = new RLMFollowerTask(topicIdPartition);
            LOGGER.info("Created a new follower task: {} and getting scheduled", task);
            ScheduledFuture<?> future = followerThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
            return new RLMTaskWithFuture(task, future);
        });
    }

    /**
     * RLM任务与Future的包装类
     * 用于管理远程日志管理任务及其对应的Future对象
     * 
     * 应用场景：
     * 1. 任务管理：包装RLM任务和对应的Future
     * 2. 任务取消：提供统一的任务取消机制
     * 3. 资源清理：确保任务正确取消和资源释放
     */
    static class RLMTaskWithFuture {
        // RLM任务实例
        private final RLMTask rlmTask;
        // 任务对应的Future对象
        private final Future<?> future;

        /**
         * 创建RLMTaskWithFuture实例
         *
         * @param rlmTask RLM任务实例
         * @param future 任务对应的Future对象
         */
        RLMTaskWithFuture(RLMTask rlmTask, Future<?> future) {
            this.rlmTask = rlmTask;
            this.future = future;
        }

        /**
         * 取消任务
         * 同时取消RLM任务和Future对象
         */
        public void cancel() {
            // 首先取消RLM任务
            rlmTask.cancel();
            try {
                // 尝试取消Future，参数true表示即使任务正在运行也尝试中断
                future.cancel(true);
            } catch (Exception ex) {
                // 记录取消任务时发生的错误
                LOGGER.error("Error occurred while canceling the task: {}", rlmTask, ex);
            }
        }
    }

    /**
     * Closes and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
     */
    /**
     * 关闭RemoteLogManager并释放所有资源
     * 包括RemoteStorageManager和RemoteLogMetadataManager等
     * 
     * 实现说明：
     * 1. 使用同步块确保线程安全
     * 2. 按顺序取消所有任务和关闭资源
     * 3. 清理所有任务集合
     * 
     * 应用场景：
     * - 系统关闭：正常关闭时的资源清理
     * - 异常处理：发生错误时的资源释放
     * - 资源回收：确保所有资源被正确释放
     */
    public void close() {
        synchronized (this) {
            if (!closed) {
                // 取消所有领导者和追随者任务
                leaderCopyRLMTasks.values().forEach(RLMTaskWithFuture::cancel);
                leaderExpirationRLMTasks.values().forEach(RLMTaskWithFuture::cancel);
                followerRLMTasks.values().forEach(RLMTaskWithFuture::cancel);
                
                // 安静地关闭各种管理器
                Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager");
                Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager");
                Utils.closeQuietly(indexCache, "RemoteIndexCache");

                // 关闭所有线程池
                rlmCopyThreadPool.close();
                rlmExpirationThreadPool.close();
                followerThreadPool.close();
                try {
                    // 关闭远程存储读取线程池并等待终止
                    shutdownAndAwaitTermination(remoteStorageReaderThreadPool, "RemoteStorageReaderThreadPool", 10, TimeUnit.SECONDS);
                } finally {
                    // 移除所有度量指标
                    removeMetrics();
                }

                // 清空所有任务集合
                leaderCopyRLMTasks.clear();
                leaderExpirationRLMTasks.clear();
                followerRLMTasks.clear();
                // 设置关闭标志
                closed = true;
            }
        }
    }

    /**
     * 关闭执行器服务并等待其终止
     * 
     * @param executor 要关闭的执行器服务
     * @param poolName 线程池名称（用于日志记录）
     * @param timeout 等待终止的超时时间
     * @param timeUnit 超时时间单位
     */
    private static void shutdownAndAwaitTermination(ExecutorService executor, String poolName, long timeout, TimeUnit timeUnit) {
        // 记录开始关闭的日志
        LOGGER.info("Shutting down {} executor", poolName);
        // 安静地关闭执行器服务
        ThreadUtils.shutdownExecutorServiceQuietly(executor, timeout, timeUnit);
        // 记录关闭完成的日志
        LOGGER.info("{} executor shutdown completed", poolName);
    }

    //Visible for testing
    /**
     * 将纪元条目列表转换为ByteBuffer
     * 主要用于测试目的
     * 
     * 实现说明：
     * 1. 创建字节数组输出流
     * 2. 使用CheckpointWriteBuffer写入纪元条目
     * 3. 将结果转换为ByteBuffer
     * 
     * @param epochEntries 要转换的纪元条目列表
     * @return 包含序列化纪元条目的ByteBuffer
     * @throws IOException 如果写入过程中发生IO错误
     */
    static ByteBuffer epochEntriesAsByteBuffer(List<EpochEntry> epochEntries) throws IOException {
        // 创建字节数组输出流
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        // 使用带缓冲的UTF-8编码的写入器
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))) {
            // 创建检查点写入缓冲区
            CheckpointFile.CheckpointWriteBuffer<EpochEntry> writeBuffer =
                    new CheckpointFile.CheckpointWriteBuffer<>(writer, 0, LeaderEpochCheckpointFile.FORMATTER);
            // 写入纪元条目
            writeBuffer.write(epochEntries);
            // 刷新写入器
            writer.flush();
        }

        // 将输出流的内容包装为ByteBuffer并返回
        return ByteBuffer.wrap(stream.toByteArray());
    }

    /**
     * 移除远程主题分区的度量指标
     * 
     * 实现说明：
     * 1. 如果主题统计信息不存在，移除broker级别的度量指标
     * 2. 如果主题统计信息存在，移除分区级别的度量指标
     * 
     * 应用场景：
     * - 主题删除：清理相关的度量指标
     * - 资源回收：释放不再需要的度量指标
     * - 监控管理：维护准确的监控数据
     *
     * @param topicIdPartition 要移除度量指标的主题分区
     */
    private void removeRemoteTopicPartitionMetrics(TopicIdPartition topicIdPartition) {
        // 获取主题名称
        String topic = topicIdPartition.topic();
        // 检查主题统计信息是否存在
        if (!brokerTopicStats.isTopicStatsExisted(topicIdPartition.topic())) {
            // 主题统计信息已被移除，清理broker级别的度量指标
            brokerTopicStats.removeBrokerLevelRemoteCopyLagBytes(topic);        // 移除复制延迟字节数
            brokerTopicStats.removeBrokerLevelRemoteCopyLagSegments(topic);     // 移除复制延迟段数
            brokerTopicStats.removeBrokerLevelRemoteDeleteLagBytes(topic);      // 移除删除延迟字节数
            brokerTopicStats.removeBrokerLevelRemoteDeleteLagSegments(topic);   // 移除删除延迟段数
            brokerTopicStats.removeBrokerLevelRemoteLogMetadataCount(topic);    // 移除日志元数据计数
            brokerTopicStats.removeBrokerLevelRemoteLogSizeComputationTime(topic); // 移除日志大小计算时间
            brokerTopicStats.removeBrokerLevelRemoteLogSizeBytes(topic);        // 移除日志大小字节数
        } else {
            // 获取分区号
            int partition = topicIdPartition.partition();
            // 移除分区级别的度量指标并更新broker级别的度量指标
            brokerTopicStats.removeRemoteCopyLagBytes(topic, partition);        // 移除分区复制延迟字节数
            brokerTopicStats.removeRemoteCopyLagSegments(topic, partition);     // 移除分区复制延迟段数
            brokerTopicStats.removeRemoteDeleteLagBytes(topic, partition);      // 移除分区删除延迟字节数
            brokerTopicStats.removeRemoteDeleteLagSegments(topic, partition);   // 移除分区删除延迟段数
            brokerTopicStats.removeRemoteLogMetadataCount(topic, partition);    // 移除分区日志元数据计数
            brokerTopicStats.removeRemoteLogSizeComputationTime(topic, partition); // 移除分区日志大小计算时间
            brokerTopicStats.removeRemoteLogSizeBytes(topic, partition);        // 移除分区日志大小字节数
        }
    }

    /**
     * 获取领导者复制任务（用于测试）
     * @param partition 主题分区
     * @return 对应的任务和Future包装对象
     */
    RLMTaskWithFuture leaderCopyTask(TopicIdPartition partition) {
        return leaderCopyRLMTasks.get(partition);
    }

    /**
     * 获取领导者过期任务（用于测试）
     * @param partition 主题分区
     * @return 对应的任务和Future包装对象
     */
    RLMTaskWithFuture leaderExpirationTask(TopicIdPartition partition) {
        return leaderExpirationRLMTasks.get(partition);
    }

    /**
     * 获取追随者任务（用于测试）
     * @param partition 主题分区
     * @return 对应的任务和Future包装对象
     */
    RLMTaskWithFuture followerTask(TopicIdPartition partition) {
        return followerRLMTasks.get(partition);
    }

    /**
     * 远程日志管理器调度线程池
     * 用于管理定时执行的远程日志管理任务
     * 
     * 实现说明：
     * 1. 支持动态调整线程池大小
     * 2. 提供任务调度和取消功能
     * 3. 维护线程池状态和度量指标
     * 
     * 应用场景：
     * - 任务调度：定期执行远程日志管理任务
     * - 资源管理：控制并发任务数量
     * - 性能监控：跟踪线程池使用情况
     */
    static class RLMScheduledThreadPool {
        // 日志记录器
        private static final Logger LOGGER = LoggerFactory.getLogger(RLMScheduledThreadPool.class);
        // 线程池名称
        private final String threadPoolName;
        // 线程名称模式
        private final String threadNamePattern;
        // 调度线程池执行器
        private final ScheduledThreadPoolExecutor scheduledThreadPool;

        /**
         * 创建线程池实例
         * @param poolSize 线程池大小
         * @param threadPoolName 线程池名称
         * @param threadNamePattern 线程名称模式
         */
        public RLMScheduledThreadPool(int poolSize, String threadPoolName, String threadNamePattern) {
            this.threadPoolName = threadPoolName;
            this.threadNamePattern = threadNamePattern;
            // 创建线程池
            scheduledThreadPool = createPool(poolSize);
        }

        /**
         * 设置核心线程池大小
         * @param newSize 新的线程池大小
         */
        public void setCorePoolSize(int newSize) {
            scheduledThreadPool.setCorePoolSize(newSize);
        }

        /**
         * 获取核心线程池大小
         * @return 当前线程池大小
         */
        public int getCorePoolSize() {
            return scheduledThreadPool.getCorePoolSize();
        }

        /**
         * 创建调度线程池
         * 
         * 实现说明：
         * 1. 创建自定义线程工厂
         * 2. 配置线程池策略
         * 3. 设置异常处理
         * 
         * @param poolSize 线程池大小
         * @return 配置好的线程池执行器
         */
        private ScheduledThreadPoolExecutor createPool(int poolSize) {
            // 创建线程工厂，支持异常处理
            ThreadFactory threadFactory = ThreadUtils.createThreadFactory(threadNamePattern, true,
                    (t, e) -> LOGGER.error("Uncaught exception in thread '{}':", t.getName(), e));
            
            // 创建线程池执行器
            ScheduledThreadPoolExecutor threadPool = new ScheduledThreadPoolExecutor(poolSize);
            // 设置取消任务时移除策略
            threadPool.setRemoveOnCancelPolicy(true);
            // 关闭时不执行已存在的延迟任务
            threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            // 关闭时不继续执行周期性任务
            threadPool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            // 设置线程工厂
            threadPool.setThreadFactory(threadFactory);
            return threadPool;
        }

        /**
         * 获取线程池空闲百分比
         * @return 空闲线程的百分比（0-1之间的小数）
         */
        public Double getIdlePercent() {
            return 1 - (double) scheduledThreadPool.getActiveCount() / (double) scheduledThreadPool.getCorePoolSize();
        }

        /**
         * 调度定期执行的任务
         * 
         * @param runnable 要执行的任务
         * @param initialDelay 初始延迟时间
         * @param delay 固定延迟时间
         * @param timeUnit 时间单位
         * @return 调度任务的Future对象
         */
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit) {
            // 记录任务调度信息
            LOGGER.info("Scheduling runnable {} with initial delay: {}, fixed delay: {}", runnable, initialDelay, delay);
            // 调度任务
            return scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit);
        }

        /**
         * 关闭线程池
         * 停止接收新任务并等待现有任务完成
         */
        public void close() {
            shutdownAndAwaitTermination(scheduledThreadPool, threadPoolName, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * 保留大小数据类
     * 用于存储和验证日志段保留大小相关的数据
     * 
     * 应用场景：
     * 1. 日志清理：基于大小的日志段清理策略
     * 2. 存储管理：跟踪需要清理的数据大小
     * 3. 容量规划：监控存储使用情况
     */
    public static class RetentionSizeData {
        // 保留的总大小限制（字节）
        private final long retentionSize;
        // 超出保留大小的剩余字节数
        private final long remainingBreachedSize;

        /**
         * 创建保留大小数据实例
         * 
         * @param retentionSize 保留的总大小限制（字节）
         * @param remainingBreachedSize 超出保留大小的剩余字节数
         * @throws IllegalArgumentException 如果参数值无效
         */
        public RetentionSizeData(long retentionSize, long remainingBreachedSize) {
            // 验证保留大小不能为负数
            if (retentionSize < 0)
                throw new IllegalArgumentException("retentionSize should be non negative, but it is " + retentionSize);

            // 验证超出的大小必须大于0
            if (remainingBreachedSize <= 0) {
                throw new IllegalArgumentException("remainingBreachedSize should be more than zero, but it is " + remainingBreachedSize);
            }

            // 初始化字段
            this.retentionSize = retentionSize;
            this.remainingBreachedSize = remainingBreachedSize;
        }
    }

    /**
     * 保留时间数据类
     * 用于存储和验证日志段保留时间相关的数据
     * 
     * 应用场景：
     * 1. 日志清理：基于时间的日志段清理策略
     * 2. 数据生命周期：管理数据的保留期限
     * 3. 合规要求：确保数据在规定时间内被清理
     */
    public static class RetentionTimeData {
        // 保留时间（毫秒）
        private final long retentionMs;
        // 清理截止时间（毫秒）
        private final long cleanupUntilMs;

        /**
         * 创建保留时间数据实例
         * 
         * @param retentionMs 保留时间（毫秒）
         * @param cleanupUntilMs 清理截止时间（毫秒）
         * @throws IllegalArgumentException 如果参数值无效
         */
        public RetentionTimeData(long retentionMs, long cleanupUntilMs) {
            // 验证保留时间不能为负数
            if (retentionMs < 0)
                throw new IllegalArgumentException("retentionMs should be non negative, but it is " + retentionMs);

            // 验证清理截止时间不能为负数
            if (cleanupUntilMs < 0)
                throw new IllegalArgumentException("cleanupUntilMs should be non negative, but it is " + cleanupUntilMs);

            // 初始化字段
            this.retentionMs = retentionMs;
            this.cleanupUntilMs = cleanupUntilMs;
        }
    }

    /**
     * 增强的日志段类
     * 包装LogSegment并提供额外的元数据信息
     * 
     * 应用场景：
     * 1. 日志管理：跟踪日志段的边界信息
     * 2. 日志清理：确定日志段的范围
     * 3. 日志迭代：支持日志段的顺序访问
     */
    static class EnrichedLogSegment {
        // 原始日志段
        private final LogSegment logSegment;
        // 下一个日志段的起始偏移量
        private final long nextSegmentOffset;

        /**
         * 创建增强的日志段实例
         * 
         * @param logSegment 原始日志段
         * @param nextSegmentOffset 下一个日志段的起始偏移量
         */
        public EnrichedLogSegment(LogSegment logSegment,
                                  long nextSegmentOffset) {
            this.logSegment = logSegment;
            this.nextSegmentOffset = nextSegmentOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EnrichedLogSegment that = (EnrichedLogSegment) o;
            return nextSegmentOffset == that.nextSegmentOffset && Objects.equals(logSegment, that.logSegment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logSegment, nextSegmentOffset);
        }

        @Override
        public String toString() {
            return "EnrichedLogSegment{" +
                    "logSegment=" + logSegment +
                    ", nextSegmentOffset=" + nextSegmentOffset +
                    '}';
        }
    }

    /**
     * 增强的记录批次类
     * 包装RecordBatch并提供跳过的字节数信息
     * 
     * 应用场景：
     * 1. 日志读取：跟踪读取过程中跳过的数据量
     * 2. 性能优化：支持高效的日志扫描
     * 3. 资源监控：统计数据读取的开销
     */
    static class EnrichedRecordBatch {
        // 原始记录批次
        private final RecordBatch batch;
        // 在到达此批次之前跳过的字节数
        private final int skippedBytes;

        /**
         * 创建增强的记录批次实例
         * 
         * @param batch 原始记录批次
         * @param skippedBytes 跳过的字节数
         */
        public EnrichedRecordBatch(RecordBatch batch, int skippedBytes) {
            this.batch = batch;
            this.skippedBytes = skippedBytes;
        }
    }
}
