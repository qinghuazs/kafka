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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * 一个封装元数据管理逻辑的类。
 * <p>
 * 此类由客户端线程（用于分区）和后台发送线程共享。
 *
 * 元数据仅维护一部分主题的信息，这些主题可以随时间增加。当我们请求一个没有元数据的主题时，
 * 会触发元数据更新。
 * <p>
 * 如果启用了主题过期功能，任何在过期时间内未被使用的主题都会在更新后从元数据刷新集合中移除。
 * 消费者会禁用主题过期，因为它们显式管理主题，而生产者依赖主题过期来限制刷新集合的大小。
 *
 * 主要功能：
 * 1. 管理集群元数据的获取和更新
 * 2. 维护主题分区的领导者信息
 * 3. 实现指数退避的重试机制
 * 4. 处理元数据的过期和刷新
 */
public class Metadata implements Closeable {
    // 日志记录器
    private final Logger log;
    // 指数退避算法，用于控制元数据刷新的重试间隔
    private final ExponentialBackoff refreshBackoff;
    // 元数据的过期时间（毫秒）
    private final long metadataExpireMs;
    // 元数据更新版本号，每次收到元数据响应时递增
    private int updateVersion;
    // 请求版本号，每次添加新主题时递增
    private int requestVersion;
    // 上次刷新元数据的时间戳
    private long lastRefreshMs;
    // 上次成功刷新元数据的时间戳
    private long lastSuccessfulRefreshMs;
    // 当前尝试次数
    private long attempts;
    // 致命异常，如果发生则停止更新
    private KafkaException fatalException;
    // 无效的主题集合
    private Set<String> invalidTopics;
    // 未授权的主题集合
    private Set<String> unauthorizedTopics;
    // 当前的元数据快照，使用volatile保证可见性
    private volatile MetadataSnapshot metadataSnapshot = MetadataSnapshot.empty();
    // 是否需要完整的元数据更新
    private boolean needFullUpdate;
    // 是否需要部分元数据更新
    private boolean needPartialUpdate;
    // 收到相同响应的次数，用于实现退避机制
    private long equivalentResponseCount;
    // 集群资源监听器集合
    private final ClusterResourceListeners clusterResourceListeners;
    // 元数据管理器是否已关闭
    private boolean isClosed;
    // 记录每个主题分区最后看到的领导者纪元
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;
    /** 初始化元数据时使用的引导地址列表 */
    private List<InetSocketAddress> bootstrapAddresses;

    /**
     * 创建一个新的Metadata实例
     *
     * @param refreshBackoffMs         元数据刷新之间必须经过的最小时间间隔，用于避免频繁轮询
     * @param refreshBackoffMaxMs      元数据刷新之间的最大等待时间
     * @param metadataExpireMs         元数据在不刷新的情况下可以保留的最长时间
     * @param logContext               对应客户端的日志上下文
     * @param clusterResourceListeners 将接收元数据更新的ClusterResourceListener列表
     */
    public Metadata(long refreshBackoffMs,
                    long refreshBackoffMaxMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.log = logContext.logger(Metadata.class);
        this.refreshBackoff = new ExponentialBackoff(
            refreshBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            refreshBackoffMaxMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.attempts = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needFullUpdate = false;
        this.needPartialUpdate = false;
        this.equivalentResponseCount = 0;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
        this.invalidTopics = Collections.emptySet();
        this.unauthorizedTopics = Collections.emptySet();
    }

    /**
     * 获取当前集群信息，非阻塞调用
     * 返回当前缓存的集群元数据快照
     */
    public Cluster fetch() {
        return metadataSnapshot.cluster();
    }

    /**
     * 获取当前元数据缓存
     * 返回完整的元数据快照对象
     */
    public MetadataSnapshot fetchMetadataSnapshot() {
        return metadataSnapshot;
    }

    /**
     * 返回当前集群信息可以更新的下一个时间点（即退避时间已过）
     * 退避时间的计算基于两个因素：
     * 1. 自上次成功响应以来尝试获取元数据的次数
     * 2. 收到的相同元数据响应的次数
     * 第二个因素允许在出现过期元数据错误时进行退避，即使元数据响应本身是正常的
     * <p>
     * 可以用来检查是否值得请求更新，如果此方法返回0，则表示可以立即更新
     *
     * @param nowMs 当前时间（毫秒）
     * @return 到下次可以更新集群信息的剩余时间（毫秒）
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        // Calculate the backoff for attempts which acts when metadata responses fail
        long backoffForAttempts = Math.max(this.lastRefreshMs +
                this.refreshBackoff.backoff(this.attempts > 0 ? this.attempts - 1 : 0) - nowMs, 0);

        // Periodic updates based on expiration resets the equivalent response count so exponential backoff is not used
        if (Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0) == 0) {
            this.equivalentResponseCount = 0;
        }

        // Calculate the backoff for equivalent responses which acts when metadata responses are not making progress
        long backoffForEquivalentResponseCount = Math.max(this.lastRefreshMs +
                (this.equivalentResponseCount > 0 ? this.refreshBackoff.backoff(this.equivalentResponseCount - 1) : 0) - nowMs, 0);

        return Math.max(backoffForAttempts, backoffForEquivalentResponseCount);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed). If an update has been requested, the metadata
     * expiry time is now.
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = updateRequested() ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    public long metadataExpireMs() {
        return this.metadataExpireMs;
    }

    /**
     * 请求更新当前集群的元数据信息，基于相同响应的数量实现退避机制
     * 相同响应的数量增加表明响应没有新的进展，可能是过期的
     * 
     * @param resetEquivalentResponseBackoff 是否重置基于连续相同响应的退避计数
     *                                       在以下情况下应设置为<i>false</i>：
     *                                       - 当请求更新是为了重试操作，例如领导者发生变更时
     *                                       在以下情况下应设置为<i>true</i>：
     *                                       - 当请求新的元数据时，例如向订阅中添加主题时
     *                                       如果不确定，最好使用<i>true</i>
     * 
     * @return 更新前的当前updateVersion值
     */
    public synchronized int requestUpdate(final boolean resetEquivalentResponseBackoff) {
        this.needFullUpdate = true;
        if (resetEquivalentResponseBackoff) {
            this.equivalentResponseCount = 0;
        }
        return this.updateVersion;
    }

    /**
     * 请求立即更新当前集群的元数据信息
     * 当调用者需要获取新请求的元数据时使用此方法
     * 此方法会重置上次刷新时间，强制立即更新
     * 
     * @return 更新前的当前updateVersion值
     */
    public synchronized int requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        this.needPartialUpdate = true;
        this.equivalentResponseCount = 0;
        this.requestVersion++;
        return this.updateVersion;
    }

    /**
     * 仅当发现更新的领导者纪元时，才请求更新分区元数据
     * 当客户端处理包含领导者纪元的broker响应时调用此方法
     * 注意：通过Metadata RPC更新时走不同的代码路径 ({@link #update})
     *
     * 领导者纪元(Leader Epoch)是Kafka用来确保一致性的机制：
     * 1. 每当分区领导者变更时，纪元号会增加
     * 2. 通过比较纪元号可以检测过期的领导者信息
     * 3. 帮助防止脑裂情况的发生
     *
     * @param topicPartition 主题分区
     * @param leaderEpoch 新的领导者纪元号
     * @return 如果更新了最后看到的纪元则返回true，否则返回false
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (leaderEpoch < 0)
            throw new IllegalArgumentException("Invalid leader epoch " + leaderEpoch + " (must be non-negative)");

        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        log.trace("Determining if we should replace existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);

        final boolean updated;
        if (oldEpoch == null) {
            log.debug("Not replacing null epoch with new epoch {} for partition {}", leaderEpoch, topicPartition);
            updated = false;
        } else if (leaderEpoch > oldEpoch) {
            log.debug("Updating last seen epoch from {} to {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, leaderEpoch);
            updated = true;
        } else {
            log.debug("Not replacing existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            updated = false;
        }

        this.needFullUpdate = this.needFullUpdate || updated;
        return updated;
    }

    /**
     * 获取指定主题分区最后一次看到的领导者纪元
     * 
     * @param topicPartition 主题分区
     * @return 该分区最后一次看到的领导者纪元，如果没有则返回空
     */
    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * 检查是否已经显式请求了元数据更新
     * 当needFullUpdate或needPartialUpdate为true时表示需要更新
     * 
     * @return 如果请求了更新返回true，否则返回false
     */
    public synchronized boolean updateRequested() {
        return this.needFullUpdate || this.needPartialUpdate;
    }

    /**
     * 添加集群更新监听器
     * 当集群元数据发生变化时会通知这些监听器
     * 
     * @param listener 要添加的集群资源监听器
     */
    public synchronized void addClusterUpdateListener(ClusterResourceListener listener) {
        this.clusterResourceListeners.maybeAdd(listener);
    }

    /**
     * 返回缓存的分区元数据，但仅当该元数据存在且没有更新的领导者纪元时才返回
     * 
     * @param topicPartition 要查询的主题分区
     * @return 如果存在且是最新的则返回分区元数据，否则返回空
     */
    public synchronized Optional<MetadataResponse.PartitionMetadata> partitionMetadataIfCurrent(TopicPartition topicPartition) {
        // 获取最后一次看到的领导者纪元
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        // 从元数据快照中获取分区元数据
        Optional<MetadataResponse.PartitionMetadata> partitionMetadata = metadataSnapshot.partitionMetadata(topicPartition);
        if (epoch == null) {
            // 如果没有纪元信息(旧集群格式)，直接返回元数据
            return partitionMetadata;
        } else {
            // 只返回领导者纪元匹配的元数据
            return partitionMetadata.filter(metadata ->
                    metadata.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH).equals(epoch));
        }
    }

    /**
     * 获取所有具有有效ID的主题的映射关系
     * 
     * @return 从主题名称到主题ID的映射，只包含有效ID的主题
     */
    public Map<String, Uuid> topicIds() {
        return metadataSnapshot.topicIds();
    }

    /**
     * 获取指定主题分区的当前领导者和纪元信息
     * 
     * @param topicPartition 要查询的主题分区
     * @return 包含领导者节点和纪元信息的LeaderAndEpoch对象
     */
    public synchronized LeaderAndEpoch currentLeader(TopicPartition topicPartition) {
        // 获取当前有效的分区元数据
        Optional<MetadataResponse.PartitionMetadata> maybeMetadata = partitionMetadataIfCurrent(topicPartition);
        if (maybeMetadata.isEmpty())
            // 如果没有有效的元数据，返回空的领导者和最后看到的纪元
            return new LeaderAndEpoch(Optional.empty(), Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition)));

        // 从元数据中提取领导者和纪元信息
        MetadataResponse.PartitionMetadata partitionMetadata = maybeMetadata.get();
        Optional<Integer> leaderEpochOpt = partitionMetadata.leaderEpoch;
        Optional<Node> leaderNodeOpt = partitionMetadata.leaderId.flatMap(metadataSnapshot::nodeById);
        return new LeaderAndEpoch(leaderNodeOpt, leaderEpochOpt);
    }

    /**
     * 使用给定的broker地址列表初始化元数据
     * 这通常是客户端启动时的第一步操作
     * 
     * @param addresses 初始的broker地址列表
     */
    public synchronized void bootstrap(List<InetSocketAddress> addresses) {
        this.needFullUpdate = true;  // 标记需要完整的元数据更新
        this.updateVersion += 1;     // 增加更新版本号
        this.metadataSnapshot = MetadataSnapshot.bootstrap(addresses);  // 创建初始元数据快照
        this.bootstrapAddresses = addresses;  // 保存引导地址列表以便后续重新引导
    }

    /**
     * 使用之前的引导地址重新初始化元数据
     * 当需要重新建立与集群的连接时使用
     */
    public synchronized void rebootstrap() {
        log.info("Rebootstrapping with {}", this.bootstrapAddresses);
        this.bootstrap(this.bootstrapAddresses);
    }

    /**
     * 使用当前请求版本更新元数据
     * 这个方法主要用于测试目的
     * 
     * @param response 元数据响应
     * @param isPartialUpdate 是否是部分更新
     * @param nowMs 当前时间戳
     */
    public synchronized void updateWithCurrentRequestVersion(MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        this.update(this.requestVersion, response, isPartialUpdate, nowMs);
    }

    /**
     * 更新集群元数据
     * 如果启用了主题过期功能，会设置主题的过期时间并移除已过期的主题
     *
     * @param requestVersion 对应更新响应的请求版本，由{@link #newMetadataRequestAndVersion(long)}提供
     * @param response 从broker收到的元数据响应
     * @param isPartialUpdate 是否是针对活动主题子集的部分更新
     * @param nowMs 当前时间戳(毫秒)
     */
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        // 参数校验
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        // 更新状态标志和计数器
        this.needPartialUpdate = requestVersion < this.requestVersion;  // 如果请求版本落后，标记需要部分更新
        this.lastRefreshMs = nowMs;  // 更新最后刷新时间
        this.attempts = 0;  // 重置尝试次数
        this.updateVersion += 1;  // 增加更新版本号
        
        // 如果是完整更新，更新相关标志
        if (!isPartialUpdate) {
            this.needFullUpdate = false;
            this.lastSuccessfulRefreshMs = nowMs;
        }
        
        // 增加相同响应计数，如果发现新元数据不同，这个计数会在updateLatestMetadata()中重置为0
        this.equivalentResponseCount++;

        // 记录更新前的集群ID
        String previousClusterId = metadataSnapshot.clusterResource().clusterId();

        // 处理元数据响应，生成新的元数据快照
        this.metadataSnapshot = handleMetadataResponse(response, isPartialUpdate, nowMs);

        // 检查并设置可能的元数据错误
        Cluster cluster = metadataSnapshot.cluster();
        maybeSetMetadataError(cluster);

        // 移除已过期主题的领导者纪元记录
        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, nowMs));

        // 检查集群ID是否发生变化
        String newClusterId = metadataSnapshot.clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {
            log.info("Cluster ID: {}", newClusterId);
        }
        
        // 通知监听器元数据已更新
        clusterResourceListeners.onUpdate(metadataSnapshot.clusterResource());

        // 记录调试日志
        log.debug("Updated cluster metadata updateVersion {} to {}", this.updateVersion, this.metadataSnapshot);
    }

    /**
     * 更新元数据中的分区领导者信息
     * 通过合并现有元数据与输入的领导者信息和节点信息来完成更新
     * 这个方法在收到broker的响应(如ProduceResponse和FetchResponse)中包含分区领导者更新时被调用
     * 注意：通过Metadata RPC的更新在{@link #update}中单独处理
     * partitionLeader和leaderNodes会覆盖现有的元数据，而不重叠的元数据保持不变
     *
     * @param partitionLeaders 分区的新领导者信息映射
     * @param leaderNodes 上述映射中领导者对应的节点列表
     * @return 已更新领导者的分区集合
     */
    public synchronized Set<TopicPartition> updatePartitionLeadership(Map<TopicPartition, LeaderIdAndEpoch> partitionLeaders, List<Node> leaderNodes) {
        // 将新的领导者节点转换为id到节点的映射
        Map<Integer, Node> newNodes = leaderNodes.stream().collect(Collectors.toMap(Node::id, node -> node));
        // 将现有节点中不重叠的部分添加到新节点映射中
        this.metadataSnapshot.cluster().nodes().stream().forEach(node -> newNodes.putIfAbsent(node.id(), node));

        // 为所有需要更新的分区创建新的元数据
        // 以下情况的分区会被排除：
        // 1. 现有元数据中的领导者比新的更新
        // 2. 新节点集合中缺少对应领导者的节点信息
        // 3. 现有元数据中没有该分区的信息
        List<PartitionMetadata> updatePartitionMetadata = new ArrayList<>();
        for (Entry<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeader: partitionLeaders.entrySet()) {
            TopicPartition partition = partitionLeader.getKey();
            Metadata.LeaderAndEpoch currentLeader = currentLeader(partition);
            Metadata.LeaderIdAndEpoch newLeader = partitionLeader.getValue();
            
            // 检查新领导者信息是否完整
            if (newLeader.epoch.isEmpty() || newLeader.leaderId.isEmpty()) {
                log.debug("For {}, incoming leader information is incomplete {}", partition, newLeader);
                continue;
            }
            
            // 检查新领导者的纪元是否更新
            if (currentLeader.epoch.isPresent() && newLeader.epoch.get() <= currentLeader.epoch.get()) {
                log.debug("For {}, incoming leader({}) is not-newer than the one in the existing metadata {}, so ignoring.", partition, newLeader, currentLeader);
                continue;
            }
            
            // 检查新领导者节点是否存在
            if (!newNodes.containsKey(newLeader.leaderId.get())) {
                log.debug("For {}, incoming leader({}), the corresponding node information for node-id {} is missing, so ignoring.", partition, newLeader, newLeader.leaderId.get());
                continue;
            }
            
            // 检查分区元数据是否仍在缓存中
            if (this.metadataSnapshot.partitionMetadata(partition).isEmpty()) {
                log.debug("For {}, incoming leader({}), partition metadata is no longer cached, ignoring.", partition, newLeader);
                continue;
            }

            // 创建更新后的分区元数据
            MetadataResponse.PartitionMetadata existingMetadata = this.metadataSnapshot.partitionMetadata(partition).get();
            MetadataResponse.PartitionMetadata updatedMetadata = new MetadataResponse.PartitionMetadata(
                existingMetadata.error,
                partition,
                newLeader.leaderId,
                newLeader.epoch,
                existingMetadata.replicaIds,
                existingMetadata.inSyncReplicaIds,
                existingMetadata.offlineReplicaIds
            );
            updatePartitionMetadata.add(updatedMetadata);

            // 更新最后看到的领导者纪元
            lastSeenLeaderEpochs.put(partition, newLeader.epoch.get());
        }

        // 如果没有需要更新的分区元数据，直接返回空集合
        if (updatePartitionMetadata.isEmpty()) {
            log.debug("No relevant metadata updates.");
            return new HashSet<>();
        }

        // 从更新的分区元数据中提取所有涉及的主题名称
        Set<String> updatedTopics = updatePartitionMetadata.stream().map(MetadataResponse.PartitionMetadata::topic).collect(Collectors.toSet());

        // 从现有的主题ID映射中获取更新主题的ID
        // 这确保了我们保持现有主题ID的一致性
        Map<String, Uuid> existingTopicIds = this.metadataSnapshot.topicIds();
        Map<String, Uuid> topicIdsForUpdatedTopics = updatedTopics.stream()
            .filter(existingTopicIds::containsKey)
            .collect(Collectors.toMap(e -> e, existingTopicIds::get));

        // 如果启用了调试日志，记录每个分区的元数据更新信息
        if (log.isDebugEnabled()) {
            updatePartitionMetadata.forEach(
                partMetadata -> log.debug("For {} updating leader information, updated metadata is {}.", partMetadata.topicPartition, partMetadata)
            );
        }

        // Fetch响应可能包含分区级别的领导者变更
        // 当这种情况发生时，我们执行部分元数据更新：
        // 1. 保持未变更分区的信息不变
        // 2. 仅更新发生变更的分区信息
        this.metadataSnapshot = metadataSnapshot.mergeWith(
            metadataSnapshot.clusterResource().clusterId(),  // 集群ID
            newNodes,                                        // 新的节点列表
            updatePartitionMetadata,                         // 需要更新的分区元数据
            Collections.emptySet(),                          // 无效主题集合（空）
            Collections.emptySet(),                          // 未授权主题集合（空）
            Collections.emptySet(),                          // 内部主题集合（空）
            metadataSnapshot.cluster().controller(),         // 控制器节点信息
            topicIdsForUpdatedTopics,                       // 更新主题的ID映射
            (topic, isInternal) -> true);                   // 主题过滤器（接受所有主题）
        
        // 通知所有监听器元数据已更新
        clusterResourceListeners.onUpdate(metadataSnapshot.clusterResource());

        // 返回所有更新的主题分区集合
        return updatePartitionMetadata.stream()
            .map(metadata -> metadata.topicPartition)
            .collect(Collectors.toSet());
    }

    /**
     * 检查并设置元数据错误，包括无效主题和未授权主题
     */
    private void maybeSetMetadataError(Cluster cluster) {
        // 清除之前的可恢复错误
        clearRecoverableErrors();
        // 检查无效主题
        checkInvalidTopics(cluster);
        // 检查未授权主题
        checkUnauthorizedTopics(cluster);
    }

    /**
     * 检查并记录无效主题
     */
    private void checkInvalidTopics(Cluster cluster) {
        // 如果存在无效主题，记录错误日志并更新无效主题集合
        if (!cluster.invalidTopics().isEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics());
            invalidTopics = new HashSet<>(cluster.invalidTopics());
        }
    }

    /**
     * 检查并记录未授权主题
     */
    private void checkUnauthorizedTopics(Cluster cluster) {
        // 如果存在未授权主题，记录错误日志并更新未授权主题集合
        if (!cluster.unauthorizedTopics().isEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics());
            unauthorizedTopics = new HashSet<>(cluster.unauthorizedTopics());
        }
    }

    /**
     * 将MetadataResponse转换为新的MetadataCache实例
     */
    private MetadataSnapshot handleMetadataResponse(MetadataResponse metadataResponse, boolean isPartialUpdate, long nowMs) {
        // 所有遇到的主题集合
        Set<String> topics = new HashSet<>();

        // 要传递给元数据缓存的保留主题集合
        Set<String> internalTopics = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();
        Set<String> invalidTopics = new HashSet<>();

        // 分区元数据列表和主题ID映射
        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<String, Uuid> oldTopicIds = metadataSnapshot.topicIds();

        // 处理每个主题的元数据
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            String topicName = metadata.topic();
            Uuid topicId = metadata.topicId();
            topics.add(topicName);

            // 只有当新元数据包含主题ID时才能判断主题ID变化
            Uuid oldTopicId = null;
            if (!Uuid.ZERO_UUID.equals(topicId)) {
                topicIds.put(topicName, topicId);
                oldTopicId = oldTopicIds.get(topicName);
            } else {
                topicId = null;
            }

            // 如果主题不需要保留，跳过后续处理
            if (!retainTopic(topicName, metadata.isInternal(), nowMs))
                continue;

            // 如果是内部主题，添加到内部主题集合
            if (metadata.isInternal())
                internalTopics.add(topicName);

            // 处理主题级别的元数据
            if (metadata.error() == Errors.NONE) {
                // 处理每个分区的元数据
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    // 即使分区元数据包含错误，也需要处理更新以捕获新的纪元
                    updateLatestMetadata(partitionMetadata, metadataResponse.hasReliableLeaderEpochs(), topicId, oldTopicId)
                        .ifPresent(partitions::add);

                    // 如果分区元数据包含无效元数据异常，请求更新
                    if (partitionMetadata.error.exception() instanceof InvalidMetadataException) {
                        log.debug("Requesting metadata update for partition {} due to error {}",
                                partitionMetadata.topicPartition, partitionMetadata.error);
                        requestUpdate(false);
                    }
                }
            } else {
                // 处理主题级别的错误
                if (metadata.error().exception() instanceof InvalidMetadataException) {
                    log.debug("Requesting metadata update for topic {} due to error {}", topicName, metadata.error());
                    requestUpdate(false);
                }

                // 根据错误类型更新相应的主题集合
                if (metadata.error() == Errors.INVALID_TOPIC_EXCEPTION)
                    invalidTopics.add(topicName);
                else if (metadata.error() == Errors.TOPIC_AUTHORIZATION_FAILED)
                    unauthorizedTopics.add(topicName);
            }
        }

        // 获取broker节点信息
        Map<Integer, Node> nodes = metadataResponse.brokersById();
        
        // 根据是否是部分更新，选择合并或创建新的元数据快照
        if (isPartialUpdate)
            return this.metadataSnapshot.mergeWith(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(), topicIds,
                (topic, isInternal) -> !topics.contains(topic) && retainTopic(topic, isInternal, nowMs));
        else
            return new MetadataSnapshot(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(), topicIds);
    }

    /**
     * 根据领导者纪元的排序（如果可用且可靠）和主题ID是否变化来计算最新的分区元数据
     */
    private Optional<MetadataResponse.PartitionMetadata> updateLatestMetadata(
            MetadataResponse.PartitionMetadata partitionMetadata,
            boolean hasReliableLeaderEpoch,
            Uuid topicId,
            Uuid oldTopicId) {
        TopicPartition tp = partitionMetadata.topicPartition;
        
        // 如果有可靠的领导者纪元且当前元数据包含纪元信息
        if (hasReliableLeaderEpoch && partitionMetadata.leaderEpoch.isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch.get();
            Integer currentEpoch = lastSeenLeaderEpochs.get(tp);
            
            if (currentEpoch == null) {
                // 如果没有之前的纪元信息，直接插入新的纪元信息
                log.debug("Setting the last seen epoch of partition {} to {} since the last known epoch was undefined.",
                        tp, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                this.equivalentResponseCount = 0;
                return Optional.of(partitionMetadata);
            } else if (topicId != null && !topicId.equals(oldTopicId)) {
                // 如果主题ID有效且发生变化，更新元数据
                // 在主题被删除和重新创建之间，客户端可能会丢失相应的topicId（即oldTopicId为null）
                // 在这种情况下，当我们发现新的topicId时，允许相应的领导者纪元覆盖最后看到的值
                log.info("Resetting the last seen epoch of partition {} to {} since the associated topicId changed from {} to {}",
                        tp, newEpoch, oldTopicId, topicId);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                this.equivalentResponseCount = 0;
                return Optional.of(partitionMetadata);
            } else if (newEpoch >= currentEpoch) {
                // 如果收到的领导者纪元至少与之前的一样新，更新元数据
                log.debug("Updating last seen epoch for partition {} from {} to epoch {} from new metadata", tp, currentEpoch, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                if (newEpoch > currentEpoch) {
                    this.equivalentResponseCount = 0;
                }
                return Optional.of(partitionMetadata);
            } else {
                // 否则忽略新的元数据，使用之前缓存的信息
                log.debug("Got metadata for an older epoch {} (current is {}) for partition {}, not updating", newEpoch, currentEpoch, tp);
                return metadataSnapshot.partitionMetadata(tp);
            }
        } else {
            // 处理旧集群格式以及缺少领导者和纪元的错误响应
            lastSeenLeaderEpochs.remove(tp);
            this.equivalentResponseCount = 0;
            return Optional.of(partitionMetadata.withoutLeaderEpoch());
        }
    }

    /**
     * 如果在元数据更新期间遇到任何不可重试的异常，清除并抛出该异常。
     * 消费者使用此方法来传播其元数据中任何主题的致命异常或主题异常。
     */
    public synchronized void maybeThrowAnyException() {
        clearErrorsAndMaybeThrowException(this::recoverableException);
    }

    /**
     * 如果在元数据更新期间遇到任何致命异常，抛出该异常。
     * 如果在上次元数据更新中出现致命异常（如身份验证失败），生产者使用此方法中止等待元数据。
     */
    protected synchronized void maybeThrowFatalException() {
        KafkaException metadataException = this.fatalException;
        if (metadataException != null) {
            fatalException = null;
            throw metadataException;
        }
    }

    /**
     * 如果在元数据更新期间遇到任何不可重试的异常，且该异常是致命的或与指定主题相关，则抛出异常。
     * 清除上次元数据更新的所有异常。生产者使用此方法来传播发送请求的主题元数据错误。
     */
    public synchronized void maybeThrowExceptionForTopic(String topic) {
        clearErrorsAndMaybeThrowException(() -> recoverableExceptionForTopic(topic));
    }

    /**
     * 清除错误并可能抛出异常
     * @param recoverableExceptionSupplier 可恢复异常的提供者
     */
    private void clearErrorsAndMaybeThrowException(Supplier<KafkaException> recoverableExceptionSupplier) {
        // 获取致命异常，如果没有则获取可恢复异常
        KafkaException metadataException = Optional.ofNullable(fatalException).orElseGet(recoverableExceptionSupplier);
        fatalException = null;  // 清除致命异常
        clearRecoverableErrors();  // 清除可恢复错误
        if (metadataException != null)  // 如果存在异常则抛出
            throw metadataException;
    }

    // 如果不再需要此主题的元数据，我们可能可以从此异常中恢复
    private KafkaException recoverableException() {
        if (!unauthorizedTopics.isEmpty())  // 如果存在未授权的主题
            return new TopicAuthorizationException(unauthorizedTopics);
        else if (!invalidTopics.isEmpty())  // 如果存在无效的主题
            return new InvalidTopicException(invalidTopics);
        else
            return null;
    }

    /**
     * 获取指定主题的可恢复异常
     * @param topic 主题名称
     * @return 如果主题存在问题返回对应的异常，否则返回null
     */
    private KafkaException recoverableExceptionForTopic(String topic) {
        if (unauthorizedTopics.contains(topic))  // 如果主题未授权
            return new TopicAuthorizationException(Collections.singleton(topic));
        else if (invalidTopics.contains(topic))  // 如果主题无效
            return new InvalidTopicException(Collections.singleton(topic));
        else
            return null;
    }

    /**
     * 清除所有可恢复的错误
     */
    private void clearRecoverableErrors() {
        invalidTopics = Collections.emptySet();  // 清除无效主题集合
        unauthorizedTopics = Collections.emptySet();  // 清除未授权主题集合
    }

    /**
     * 记录一次失败的元数据更新尝试。
     * 我们需要跟踪这个以避免立即重试。
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;  // 更新最后刷新时间
        this.attempts++;  // 增加尝试次数
        this.equivalentResponseCount = 0;  // 重置相同响应计数
    }

    /**
     * 传播影响获取集群元数据能力的致命错误。
     * 两个例子是身份验证和不支持的版本异常。
     *
     * @param exception 致命异常
     */
    public synchronized void fatalError(KafkaException exception) {
        this.fatalException = exception;  // 设置致命异常
    }

    /**
     * @return 当前的元数据更新版本
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * 返回最后一次成功更新元数据的时间
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * 关闭此元数据实例，表示不再可能进行元数据更新
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * 检查此元数据实例是否已关闭。更多信息请参见{@link #close()}。
     *
     * @return 如果此实例已关闭则返回true，否则返回false
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    /**
     * 创建新的元数据请求和版本
     * @param nowMs 当前时间戳（毫秒）
     * @return 包含请求和版本信息的对象
     */
    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion(long nowMs) {
        MetadataRequest.Builder request = null;
        boolean isPartialUpdate = false;

        // 仅当未请求完整更新且上次成功刷新未超过元数据过期时间时执行部分更新
        if (!this.needFullUpdate && this.lastSuccessfulRefreshMs + this.metadataExpireMs > nowMs) {
            request = newMetadataRequestBuilderForNewTopics();
            isPartialUpdate = true;
        }
        if (request == null) {
            request = newMetadataRequestBuilder();
            isPartialUpdate = false;
        }
        return new MetadataRequestAndVersion(request, requestVersion, isPartialUpdate);
    }

    /**
     * 构造并返回用于获取集群数据和所有活动主题的元数据请求构建器
     *
     * @return 构造的非空元数据构建器
     */
    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    /**
     * 构造并返回用于获取集群数据和任何未缓存主题的元数据请求构建器
     * 如果不支持该功能则返回null
     *
     * @return 构造的元数据构建器，如果不支持则返回null
     */
    protected MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return null;
    }

    /**
     * @return Mapping from topic IDs to topic names for all topics in the cache.
     */
    /**
     * 获取缓存中所有主题的ID到名称的映射关系
     * 
     * @return 从主题ID到主题名称的映射
     */
    public Map<Uuid, String> topicNames() {
        return metadataSnapshot.topicNames();
    }

    /**
     * 判断是否保留指定的主题
     * 这是一个受保护的方法，子类可以重写此方法来实现自定义的主题保留逻辑
     * 
     * @param topic 要判断的主题名称
     * @param isInternal 是否是内部主题
     * @param nowMs 当前时间戳
     * @return 如果应该保留该主题则返回true，否则返回false
     */
    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    /**
     * 元数据请求和版本信息的封装类
     * 用于跟踪元数据请求的构建器、版本号和更新类型
     */
    public static class MetadataRequestAndVersion {
        // 元数据请求的构建器
        public final MetadataRequest.Builder requestBuilder;
        // 请求的版本号
        public final int requestVersion;
        // 是否是部分更新
        public final boolean isPartialUpdate;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion,
                                          boolean isPartialUpdate) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
            this.isPartialUpdate = isPartialUpdate;
        }
    }

    /**
     * 表示元数据中已知的当前leader状态
     * 可能出现以下情况：
     * 1. 我们知道leader但不知道epoch，这种情况发生在broker不支持足够新的元数据API版本时
     * 2. 我们知道epoch但不知道leader，这种情况发生在信息来自外部源时（如已提交的偏移量）
     */
    public static class LeaderAndEpoch {
        // 表示没有leader和epoch信息的常量实例
        private static final LeaderAndEpoch NO_LEADER_OR_EPOCH = new LeaderAndEpoch(Optional.empty(), Optional.empty());

        // leader节点信息
        public final Optional<Node> leader;
        // leader的epoch值
        public final Optional<Integer> epoch;

        public LeaderAndEpoch(Optional<Node> leader, Optional<Integer> epoch) {
            this.leader = Objects.requireNonNull(leader);
            this.epoch = Objects.requireNonNull(epoch);
        }

        /**
         * 获取一个表示没有leader和epoch信息的实例
         */
        public static LeaderAndEpoch noLeaderOrEpoch() {
            return NO_LEADER_OR_EPOCH;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeaderAndEpoch that = (LeaderAndEpoch) o;

            if (!leader.equals(that.leader)) return false;
            return epoch.equals(that.epoch);
        }

        @Override
        public int hashCode() {
            int result = leader.hashCode();
            result = 31 * result + epoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                    "}";
        }
    }

    /**
     * 表示leader ID和epoch信息的封装类
     * 用于在不需要完整Node对象的场景下传递leader信息
     */
    public static class LeaderIdAndEpoch {
        // leader的ID
        public final Optional<Integer> leaderId;
        // leader的epoch值
        public final Optional<Integer> epoch;

        public LeaderIdAndEpoch(Optional<Integer> leaderId, Optional<Integer> epoch) {
            this.leaderId = Objects.requireNonNull(leaderId);
            this.epoch = Objects.requireNonNull(epoch);
        }

        @Override
        public String toString() {
            return "LeaderIdAndEpoch{" +
                "leaderId=" + leaderId.map(Number::toString).orElse("absent") +
                ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                "}";
        }
    }
}
