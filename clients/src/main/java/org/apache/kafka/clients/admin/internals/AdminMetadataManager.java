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

package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Manages the metadata for KafkaAdminClient.
 * 管理KafkaAdminClient的元数据。
 *
 * This class is not thread-safe.  It is only accessed from the AdminClient
 * service thread (which also uses the NetworkClient).
 * 此类不是线程安全的。它只能从AdminClient服务线程（该线程同时使用NetworkClient）访问。
 */
public class AdminMetadataManager {
    // 日志记录器
    private final Logger log;

    /**
     * The minimum amount of time that we should wait between subsequent
     * retries, when fetching metadata.
     * 在获取元数据时，两次重试之间需要等待的最小时间间隔。
     * 这个退避时间可以防止在短时间内发送过多的元数据请求，避免对服务器造成过大压力。
     */
    private final long refreshBackoffMs;

    /**
     * The minimum amount of time that we should wait before triggering an
     * automatic metadata refresh.
     * 触发自动元数据刷新前需要等待的最小时间间隔。
     * 这个过期时间确保元数据不会过于陈旧，同时避免过于频繁的刷新。
     */
    private final long metadataExpireMs;

    /**
     * True if we are communicating directly with the controller quorum as specified by KIP-919.
     * 如果我们按照KIP-919的规范直接与控制器仲裁组通信，则为true。
     * 这个标志决定了元数据请求的目标：是发送到普通的broker还是直接发送到控制器。
     */
    private final boolean usingBootstrapControllers;

    /**
     * Used to update the NetworkClient metadata.
     * 用于更新NetworkClient的元数据。
     * 这个更新器实现了MetadataUpdater接口，处理元数据的更新、重试和错误处理。
     */
    private final AdminMetadataUpdater updater;

    /**
     * The current metadata state.
     * 当前元数据的状态。
     * 状态机用于跟踪元数据更新的进度，包括静默、请求更新和更新待处理三种状态。
     */
    private State state = State.QUIESCENT;

    /**
     * The time in wall-clock milliseconds when we last updated the metadata.
     * 最后一次更新元数据的时间戳（以毫秒为单位）。
     * 用于判断元数据是否过期，决定是否需要刷新。
     */
    private long lastMetadataUpdateMs = 0;

    /**
     * The time in wall-clock milliseconds when we last attempted to fetch new
     * metadata.
     * 最后一次尝试获取新元数据的时间戳（以毫秒为单位）。
     * 用于实现重试退避机制，避免频繁请求。
     */
    private long lastMetadataFetchAttemptMs = 0;

    /**
     * The time in wall-clock milliseconds when we started attempts to fetch metadata. If empty,
     * metadata has not been requested. This is the start time based on which rebootstrap is
     * triggered if metadata is not obtained for the configured rebootstrap trigger interval.
     * Set to Optional.of(0L) to force rebootstrap immediately.
     * 
     * 开始尝试获取元数据的时间戳（以毫秒为单位）。如果为空，表示尚未请求元数据。
     * 这个时间戳用于判断是否需要重新引导：如果在配置的触发间隔内未能获取元数据，则触发重新引导。
     * 设置为Optional.of(0L)可以立即强制进行重新引导。
     */
    private Optional<Long> metadataAttemptStartMs = Optional.empty();

    /**
     * The current cluster information.
     * 当前的集群信息。
     * 包含了集群的节点列表、主题分区分配等信息。初始为空集群。
     */
    private Cluster cluster = Cluster.empty();

    /**
     * If this is non-null, it is a fatal exception that will terminate all attempts at communication.
     * 如果非空，表示发生了致命异常，将终止所有通信尝试。
     * 用于处理不可恢复的错误，如认证失败或不支持的API版本。
     */
    private ApiException fatalException = null;

    /**
     * The cluster with which the metadata was bootstrapped.
     * 用于引导的集群信息。
     * 保存初始引导时使用的集群信息，在需要重新引导时使用。
     */
    private Cluster bootstrapCluster;

    /**
     * AdminMetadataUpdater类实现了MetadataUpdater接口，负责处理元数据的更新操作。
     * 这个内部类作为NetworkClient和AdminMetadataManager之间的桥梁，
     * 处理服务器断开连接、请求失败等情况，并在必要时触发元数据的更新或重新引导。
     */
    public class AdminMetadataUpdater implements MetadataUpdater {
        /**
         * 获取当前集群中的所有节点列表
         * @return 当前已知的所有Kafka节点列表
         */
        @Override
        public List<Node> fetchNodes() {
            return cluster.nodes();
        }

        /**
         * 检查是否需要更新元数据
         * 在AdminClient中，元数据更新由外部触发，因此始终返回false
         */
        @Override
        public boolean isUpdateDue(long now) {
            return false;
        }

        /**
         * 尝试更新元数据
         * 在AdminClient中不使用此方法进行更新，返回最大值表示永不自动更新
         */
        @Override
        public long maybeUpdate(long now) {
            return Long.MAX_VALUE;
        }

        /**
         * 处理服务器断开连接的情况
         * 如果断开连接伴随着认证异常，则将其标记为更新失败
         * 同时请求更新元数据以尝试重新建立连接
         */
        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            maybeFatalException.ifPresent(AdminMetadataManager.this::updateFailed);
            AdminMetadataManager.this.requestUpdate();
        }

        /**
         * 处理请求失败的情况
         * 在AdminClient中，失败的处理由外层逻辑完成
         */
        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            // Do nothing
        }

        /**
         * 处理成功的元数据响应
         * 在AdminClient中，响应的处理由外层逻辑完成
         */
        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse) {
            // Do nothing
        }

        /**
         * 检查是否需要重新引导
         * 当元数据获取尝试时间超过触发阈值时，返回true
         */
        @Override
        public boolean needsRebootstrap(long now, long rebootstrapTriggerMs) {
            return AdminMetadataManager.this.needsRebootstrap(now, rebootstrapTriggerMs);
        }

        /**
         * 执行重新引导操作
         * 使用初始的引导集群信息重新开始元数据同步
         */
        @Override
        public void rebootstrap(long now) {
            AdminMetadataManager.this.rebootstrap(now);
        }

        /**
         * 关闭更新器
         * 在AdminClient中不需要特殊的清理操作
         */
        @Override
        public void close() {
        }
    }

    /**
     * AdminMetadataManager的当前状态
     * 使用状态机模式管理元数据的更新过程
     */
    enum State {
        /** 
         * 静默状态：没有待处理的更新请求
         * 在这个状态下，元数据管理器可能触发定期刷新
         */
        QUIESCENT,
        
        /** 
         * 更新已请求：已收到更新请求，但尚未开始获取新的元数据
         * 在这个状态下，元数据管理器会在遵守退避时间的前提下尝试更新
         */
        UPDATE_REQUESTED,
        
        /** 
         * 更新待处理：正在进行元数据的获取
         * 在这个状态下，元数据管理器等待更新操作完成或失败
         */
        UPDATE_PENDING
    }

    /**
     * 创建AdminMetadataManager实例
     * 用于管理KafkaAdminClient的元数据，包括元数据的更新、缓存和状态管理
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param refreshBackoffMs 元数据刷新的退避时间（毫秒），用于控制重试间隔
     * @param metadataExpireMs 元数据的过期时间（毫秒），超过此时间将触发自动刷新
     * @param usingBootstrapControllers 是否使用KIP-919规范直接与控制器通信
     */
    public AdminMetadataManager(
        LogContext logContext,
        long refreshBackoffMs,
        long metadataExpireMs,
        boolean usingBootstrapControllers
    ) {
        // 初始化日志记录器
        this.log = logContext.logger(AdminMetadataManager.class);
        // 设置元数据刷新的退避时间
        this.refreshBackoffMs = refreshBackoffMs;
        // 设置元数据的过期时间
        this.metadataExpireMs = metadataExpireMs;
        // 设置是否使用引导控制器
        this.usingBootstrapControllers = usingBootstrapControllers;
        // 创建元数据更新器实例
        this.updater = new AdminMetadataUpdater();
    }

    /**
     * 检查是否使用引导控制器模式
     * 在KIP-919规范中，AdminClient可以直接与控制器通信，而不是通过普通的broker
     *
     * @return 如果使用引导控制器模式返回true，否则返回false
     */
    public boolean usingBootstrapControllers() {
        return usingBootstrapControllers;
    }

    /**
     * 获取元数据更新器实例
     * 元数据更新器负责处理与NetworkClient的交互，包括节点连接和断开、请求失败等情况
     *
     * @return AdminMetadataUpdater实例
     */
    public AdminMetadataUpdater updater() {
        return updater;
    }

    /**
     * 检查元数据是否准备就绪可用
     * 元数据在以下情况下被认为是不可用的：
     * 1. 存在致命异常
     * 2. 集群节点列表为空
     * 3. 仅配置了引导节点但尚未获取完整元数据
     *
     * @return 如果元数据可用返回true，否则返回false
     * @throws ApiException 如果存在致命异常则抛出
     */
    public boolean isReady() {
        // 检查是否存在致命异常
        if (fatalException != null) {
            log.debug("Metadata is not usable: failed to get metadata.", fatalException);
            throw fatalException;
        }
        // 检查集群节点列表是否为空
        if (cluster.nodes().isEmpty()) {
            log.trace("Metadata is not ready: bootstrap nodes have not been " +
                "initialized yet.");
            return false;
        }
        // 检查是否只配置了引导节点但尚未获取完整元数据
        if (cluster.isBootstrapConfigured()) {
            log.trace("Metadata is not ready: we have not fetched metadata from " +
                "the bootstrap nodes yet.");
            return false;
        }
        log.trace("Metadata is ready to use.");
        return true;
    }

    /**
     * 获取当前集群的控制器节点
     * 控制器节点负责处理主题创建、分区分配等管理操作
     *
     * @return 控制器节点信息，如果未知则返回null
     */
    public Node controller() {
        return cluster.controller();
    }

    /**
     * 根据节点ID获取节点信息
     * 用于在需要与特定节点通信时获取其连接信息
     *
     * @param nodeId 节点ID
     * @return 对应的节点信息，如果不存在则返回null
     */
    public Node nodeById(int nodeId) {
        return cluster.nodeById(nodeId);
    }

    /**
     * 请求更新元数据
     * 只有在当前状态为QUIESCENT（静默）时才会触发新的更新请求
     * 这个方法通常在需要刷新元数据或发现当前元数据可能过期时调用
     */
    public void requestUpdate() {
        // 只有在静默状态下才接受新的更新请求
        if (state == State.QUIESCENT) {
            // 将状态转换为UPDATE_REQUESTED
            state = State.UPDATE_REQUESTED;
            log.debug("Requesting metadata update.");
        }
    }

    /**
     * 清除当前缓存的控制器节点信息
     * 通常在控制器发生变更或需要重新发现控制器时调用
     * 创建新的集群对象，保留原有节点信息但清除控制器引用
     */
    public void clearController() {
        if (cluster.controller() != null) {
            log.trace("Clearing cached controller node {}.", cluster.controller());
            // 创建新的集群对象，保持原有配置但清除控制器引用
            this.cluster = new Cluster(cluster.clusterResource().clusterId(),
                cluster.nodes(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null);
        }
    }

    /**
     * 计算获取新元数据之前需要等待的时间
     * 根据当前状态和时间戳计算合适的延迟时间，以实现退避和限流机制
     *
     * @param now 当前时间戳（毫秒）
     * @return 需要等待的毫秒数，如果不需要获取则返回Long.MAX_VALUE
     */
    public long metadataFetchDelayMs(long now) {
        switch (state) {
            case QUIESCENT:
                // 在静默状态下，需要同时考虑退避时间和过期时间
                // 取两者的最大值，确保既不会过于频繁地请求，也不会使用过期的元数据
                return Math.max(delayBeforeNextAttemptMs(now), delayBeforeNextExpireMs(now));
            case UPDATE_REQUESTED:
                // 即使已经请求了更新，也要遵守退避时间
                // 这可以防止在短时间内发送过多的请求
                return delayBeforeNextAttemptMs(now);
            default:
                // 已经有一个更新在进行中，不需要发起新的更新
                return Long.MAX_VALUE;
        }
    }

    /**
     * 计算距离元数据过期还需要等待的时间
     * 基于上次更新时间和配置的过期时间计算
     *
     * @param now 当前时间戳（毫秒）
     * @return 距离过期的剩余毫秒数，如果已过期则返回0
     */
    private long delayBeforeNextExpireMs(long now) {
        // 计算自上次更新以来经过的时间
        long timeSinceUpdate = now - lastMetadataUpdateMs;
        // 如果经过的时间超过过期时间，返回0；否则返回剩余时间
        return Math.max(0, metadataExpireMs - timeSinceUpdate);
    }

    /**
     * 计算距离下一次允许尝试更新的等待时间
     * 基于上次尝试时间和配置的退避时间计算
     *
     * @param now 当前时间戳（毫秒）
     * @return 需要等待的毫秒数，如果退避时间已过则返回0
     */
    private long delayBeforeNextAttemptMs(long now) {
        // 计算自上次尝试以来经过的时间
        long timeSinceAttempt = now - lastMetadataFetchAttemptMs;
        // 如果经过的时间超过退避时间，返回0；否则返回剩余时间
        return Math.max(0, refreshBackoffMs - timeSinceAttempt);
    }

    /**
     * 检查是否需要重新引导
     * 当元数据获取尝试持续时间超过触发阈值时，需要进行重新引导
     *
     * @param now 当前时间戳（毫秒）
     * @param rebootstrapTriggerMs 触发重新引导的时间阈值（毫秒）
     * @return 如果需要重新引导返回true，否则返回false
     */
    public boolean needsRebootstrap(long now, long rebootstrapTriggerMs) {
        // 检查是否存在元数据获取开始时间，且是否超过了触发阈值
        return metadataAttemptStartMs.filter(startMs -> now - startMs > rebootstrapTriggerMs).isPresent();
    }

    /**
     * 将状态转换为UPDATE_PENDING（更新待处理）
     * 更新最后尝试时间，并在必要时记录首次尝试时间
     *
     * @param now 当前时间戳（毫秒）
     */
    public void transitionToUpdatePending(long now) {
        // 转换状态为更新待处理
        this.state = State.UPDATE_PENDING;
        // 更新最后尝试时间
        this.lastMetadataFetchAttemptMs = now;
        // 如果是首次尝试，记录开始时间
        if (metadataAttemptStartMs.isEmpty())
            metadataAttemptStartMs = Optional.of(now);
    }

    /**
     * 处理元数据更新失败的情况
     * 根据异常类型进行不同的处理：
     * 1. 对于致命异常（如认证失败），记录并保存异常信息
     * 2. 对于不支持的API版本异常，根据是否使用引导控制器输出相应的警告
     * 3. 对于非致命异常，仅记录信息级别的日志
     *
     * @param exception 更新失败时抛出的异常
     */
    public void updateFailed(Throwable exception) {
        // 我们依赖待处理的调用来请求另一次元数据更新
        // 将状态重置为静默状态，等待下一次更新请求
        this.state = State.QUIESCENT;

        if (RequestUtils.isFatalException(exception)) {
            // 记录致命错误的警告日志
            log.warn("Fatal error during metadata update", exception);
            // 避免对ApiException进行未经检查/未确认的类型转换
            if (exception instanceof  ApiException) {
                // 保存致命异常，这将阻止后续的元数据更新尝试
                this.fatalException = (ApiException) exception;
            }

            if (exception instanceof UnsupportedVersionException) {
                if (usingBootstrapControllers) {
                    // 如果使用引导控制器模式，但远程节点不支持KIP-919的DESCRIBE_CLUSTER API
                    log.warn("The remote node is not a CONTROLLER that supports the KIP-919 " +
                        "DESCRIBE_CLUSTER api.", exception);
                } else {
                    // 如果使用普通模式，但远程节点不支持METADATA API
                    log.warn("The remote node is not a BROKER that supports the METADATA api.", exception);
                }
            }
        } else {
            // 对于非致命异常，记录信息级别的日志
            log.info("Metadata update failed", exception);
        }
    }

    /**
     * 接收新的元数据，并转换到静默状态
     * 更新lastMetadataUpdateMs、cluster和authException
     * 
     * @param cluster 新的集群元数据
     * @param now 当前时间戳（毫秒）
     */
    public void update(Cluster cluster, long now) {
        if (cluster.isBootstrapConfigured()) {
            // 如果是引导配置的集群，保存为引导集群元数据
            log.debug("Setting bootstrap cluster metadata {}.", cluster);
            bootstrapCluster = cluster;
        } else {
            // 更新普通集群元数据和最后更新时间
            log.debug("Updating cluster metadata to {}", cluster);
            this.lastMetadataUpdateMs = now;
        }

        // 重置状态和错误信息
        this.state = State.QUIESCENT;
        this.fatalException = null;
        this.metadataAttemptStartMs = Optional.empty();

        // 只有当新集群包含节点时才更新当前集群
        if (!cluster.nodes().isEmpty()) {
            this.cluster = cluster;
        }
    }

    /**
     * 初始化重新引导过程
     * 通过将元数据尝试开始时间设置为0，强制触发立即重新引导
     */
    public void initiateRebootstrap() {
        this.metadataAttemptStartMs = Optional.of(0L);
    }

    /**
     * 使用之前用于引导的集群重新进行引导
     * 这个方法在需要重新建立与集群的连接时使用
     * 
     * @param now 当前时间戳（毫秒）
     */
    public void rebootstrap(long now) {
        // 使用保存的引导集群信息进行重新引导
        log.info("Rebootstrapping with {}", this.bootstrapCluster);
        // 更新元数据到引导集群状态
        update(bootstrapCluster, now);
        // 记录新的尝试开始时间
        this.metadataAttemptStartMs = Optional.of(now);
    }
}
