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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 生产者的元数据管理类，继承自基础的Metadata类
 * 主要功能：
 * 1. 管理主题的元数据缓存和过期机制
 * 2. 处理新主题的元数据请求
 * 3. 维护主题级别的错误信息
 * 4. 实现元数据的异步更新和等待机制
 */
public class ProducerMetadata extends Metadata {
    // 主题的空闲超时时间（毫秒），如果一个主题在这段时间内没有被访问，将从缓存中移除
    private final long metadataIdleMs;

    // 维护主题及其过期时间的映射，key是主题名，value是过期时间戳
    private final Map<String, Long> topics = new HashMap<>();
    // 新添加的主题集合，这些主题需要立即获取元数据
    private final Set<String> newTopics = new HashSet<>();
    // 日志记录器
    private final Logger log;
    // 时间服务，用于获取当前时间和实现等待机制
    private final Time time;
    // 主题级别的错误信息映射，key是主题名，value是对应的错误类型
    private Map<String, Errors> errors = null;

    /**
     * 创建ProducerMetadata实例
     * 
     * @param refreshBackoffMs 元数据刷新的初始退避时间（毫秒）
     * @param refreshBackoffMaxMs 元数据刷新的最大退避时间（毫秒）
     * @param metadataExpireMs 元数据的过期时间（毫秒）
     * @param metadataIdleMs 主题的空闲超时时间（毫秒）
     * @param logContext 日志上下文
     * @param clusterResourceListeners 集群资源监听器列表
     * @param time 时间服务实例
     */
    public ProducerMetadata(long refreshBackoffMs,
                            long refreshBackoffMaxMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    /**
     * 创建元数据请求构建器
     * 重写父类方法，为所有已知主题创建元数据请求
     * 
     * @return 包含所有已缓存主题的元数据请求构建器
     */
    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    /**
     * 创建新主题的元数据请求构建器
     * 重写父类方法，仅为新添加的主题创建元数据请求
     * 
     * @return 仅包含新主题的元数据请求构建器
     */
    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    /**
     * 添加新主题并设置其过期时间
     * 如果是首次添加该主题，会将其加入新主题集合并请求更新元数据
     * 
     * @param topic 要添加的主题名称
     * @param nowMs 当前时间戳（毫秒）
     */
    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        // 设置主题的过期时间，如果主题不存在（返回null），则将其添加到新主题集合
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic);
            requestUpdateForNewTopics();
        }
    }

    /**
     * 请求更新指定主题的元数据
     * 如果是新主题，则触发新主题的更新请求
     * 否则触发常规的元数据更新请求
     * 
     * @param topic 需要更新元数据的主题
     * @return 当前的更新版本号
     */
    public synchronized int requestUpdateForTopic(String topic) {
        if (newTopics.contains(topic)) {
            return requestUpdateForNewTopics();
        } else {
            return requestUpdate(false);
        }
    }

    /**
     * 获取所有已知主题的集合
     * 该方法主要用于测试
     * 
     * @return 所有已缓存主题的集合
     */
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    /**
     * 获取所有新主题的集合
     * 该方法主要用于测试
     * 
     * @return 所有新添加但尚未获取元数据的主题集合
     */
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    /**
     * 检查指定主题是否在缓存中
     * 
     * @param topic 要检查的主题名称
     * @return 如果主题在缓存中返回true，否则返回false
     */
    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    /**
     * 判断是否保留指定主题的元数据
     * 重写父类方法，实现基于空闲时间的主题过期机制
     * 
     * @param topic 要检查的主题
     * @param isInternal 是否是内部主题（在此实现中未使用）
     * @param nowMs 当前时间戳（毫秒）
     * @return 如果主题应该被保留返回true，否则返回false
     */
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        Long expireMs = topics.get(topic);
        if (expireMs == null) {  // 主题不在缓存中
            return false;
        } else if (newTopics.contains(topic)) {  // 新主题始终保留
            return true;
        } else if (expireMs <= nowMs) {  // 主题已过期
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            topics.remove(topic);
            return false;
        } else {  // 主题未过期
            return true;
        }
    }

    /**
     * 等待元数据更新完成
     * 当前版本号大于指定的版本号时，表示更新已完成
     * 
     * @param lastVersion 上一个已知的元数据版本号
     * @param timeoutMs 最大等待时间（毫秒）
     * @throws InterruptedException 如果等待过程中线程被中断
     * @throws KafkaException 如果在等待过程中元数据管理器被关闭
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        // 计算等待截止时间，处理可能的溢出
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        
        // 等待直到元数据版本号增加或管理器关闭
        time.waitObject(this, () -> {
            // 检查是否有致命异常，如果有则抛出。可恢复的主题错误由调用者处理
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        // 如果管理器已关闭，抛出异常
        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    /**
     * 更新元数据信息
     * 重写父类方法，增加了错误处理和新主题的处理逻辑
     * 
     * @param requestVersion 请求版本号
     * @param response 服务器返回的元数据响应
     * @param isPartialUpdate 是否是部分更新
     * @param nowMs 当前时间戳（毫秒）
     */
    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        // 首先调用父类的更新方法
        super.update(requestVersion, response, isPartialUpdate, nowMs);
        // 保存主题级别的错误信息
        errors = response.errors();

        // 从新主题集合中移除已收到元数据的主题
        // 注意：如果获取新主题的元数据时遇到错误，解决该错误的过程会在完整的元数据更新中包含该主题
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }

        // 通知所有等待的线程
        notifyAll();
    }

    /**
     * 获取指定主题的错误信息
     * 
     * @param topic 要查询的主题名称
     * @return 主题相关的错误信息，如果没有错误或主题不存在则返回null
     */
    public Errors getError(final String topic) {
        if (errors != null) {
            return errors.get(topic);
        }
        return null;
    }

    /**
     * 处理致命错误
     * 重写父类方法，在设置致命错误后通知所有等待的线程
     * 
     * @param fatalException 发生的致命异常
     */
    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * 关闭元数据管理器实例
     * 重写父类方法，在关闭后通知所有等待的线程
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
