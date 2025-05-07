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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * 共享会话处理器
 * 维护与broker连接的共享会话状态。
 *
 * <p>基于KIP-932协议，客户端可以创建共享会话。这些会话允许客户端
 * 重复从一组共享分区获取数据，而无需在每个请求和响应中显式枚举所有分区。
 *
 * <p>ShareSessionHandler跟踪会话中的分区，并确定需要包含在每个
 * ShareFetch/ShareAcknowledge请求中的分区。
 */
public class ShareSessionHandler {
    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * broker节点ID
     */
    private final int node;

    /**
     * 会话成员ID
     */
    private final Uuid memberId;

    /**
     * 下一个ShareFetch/ShareAcknowledge请求的元数据
     */
    private ShareRequestMetadata nextMetadata;

    /**
     * 共享会话中的所有分区
     * 键为主题分区，值为带ID的主题分区
     */
    private final LinkedHashMap<TopicPartition, TopicIdPartition> sessionPartitions;

    /**
     * 将包含在下一个ShareFetch请求中的分区
     * 键为主题分区，值为带ID的主题分区
     */
    private LinkedHashMap<TopicPartition, TopicIdPartition> nextPartitions;

    /**
     * 将包含在下一个ShareFetch/ShareAcknowledge请求中的确认信息
     * 键为带ID的主题分区，值为确认信息
     */
    private LinkedHashMap<TopicIdPartition, Acknowledgements> nextAcknowledgements;

    /**
     * 构造函数
     * 创建一个新的共享会话处理器实例
     *
     * @param logContext 日志上下文
     * @param node broker节点ID
     * @param memberId 会话成员ID
     */
    public ShareSessionHandler(LogContext logContext, int node, Uuid memberId) {
        // 初始化日志记录器
        this.log = logContext.logger(ShareSessionHandler.class);
        // 设置节点ID
        this.node = node;
        // 设置成员ID
        this.memberId = memberId;
        // 初始化元数据为初始世代
        this.nextMetadata = ShareRequestMetadata.initialEpoch(memberId);
        // 初始化会话分区映射
        this.sessionPartitions = new LinkedHashMap<>();
        // 初始化下一批次分区映射
        this.nextPartitions = new LinkedHashMap<>();
        // 初始化下一批次确认映射
        this.nextAcknowledgements = new LinkedHashMap<>();
    }

    /**
     * 获取会话分区映射
     *
     * @return 当前会话中的分区映射
     */
    Map<TopicPartition, TopicIdPartition> sessionPartitionMap() {
        return sessionPartitions;
    }

    /**
     * 获取会话分区集合
     *
     * @return 当前会话中分区的不可修改集合
     */
    public Collection<TopicIdPartition> sessionPartitions() {
        return Collections.unmodifiableCollection(sessionPartitions.values());
    }

    /**
     * 添加要获取的分区
     *
     * @param topicIdPartition 要添加的带ID的主题分区
     * @param partitionAcknowledgements 分区的确认信息
     */
    public void addPartitionToFetch(TopicIdPartition topicIdPartition, Acknowledgements partitionAcknowledgements) {
        // 将分区添加到下一批次分区映射
        nextPartitions.put(topicIdPartition.topicPartition(), topicIdPartition);
        // 如果有确认信息，添加到下一批次确认映射
        if (partitionAcknowledgements != null) {
            nextAcknowledgements.put(topicIdPartition, partitionAcknowledgements);
        }
    }

    /**
     * 创建新的ShareFetch请求构建器
     *
     * @param groupId 消费者组ID
     * @param fetchConfig 获取配置
     * @return ShareFetch请求构建器
     */
    public ShareFetchRequest.Builder newShareFetchBuilder(String groupId, FetchConfig fetchConfig) {
        // 初始化添加、移除和替换的分区列表
        List<TopicIdPartition> added = new ArrayList<>();
        List<TopicIdPartition> removed = new ArrayList<>();
        List<TopicIdPartition> replaced = new ArrayList<>();

        // 如果是新会话
        if (nextMetadata.isNewSession()) {
            // 将所有新分区添加到会话中
            for (Entry<TopicPartition, TopicIdPartition> entry : nextPartitions.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition topicIdPartition = entry.getValue();
                sessionPartitions.put(topicPartition, topicIdPartition);
            }

            // 新会话需要将所有分区添加到请求中
            added.addAll(sessionPartitions.values());
        } else {
            // 遍历会话分区，统计变更
            Iterator<Entry<TopicPartition, TopicIdPartition>> partitionIterator = sessionPartitions.entrySet().iterator();
            while (partitionIterator.hasNext()) {
                Entry<TopicPartition, TopicIdPartition> entry = partitionIterator.next();
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition prevData = entry.getValue();
                TopicIdPartition nextData = nextPartitions.remove(topicPartition);
                if (nextData != null) {
                    // 如果主题ID不匹配，说明主题被重新创建
                    if (!prevData.equals(nextData)) {
                        nextPartitions.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        replaced.add(prevData);
                    }
                } else {
                    // 分区不在构建器中，需要从会话中移除
                    partitionIterator.remove();
                    removed.add(prevData);
                }
            }

            // 添加新分区到会话
            for (Entry<TopicPartition, TopicIdPartition> entry : nextPartitions.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                TopicIdPartition topicIdPartition = entry.getValue();
                sessionPartitions.put(topicPartition, topicIdPartition);
                added.add(topicIdPartition);
            }
        }

        // 记录调试日志
        if (log.isDebugEnabled()) {
            log.debug("Build ShareFetch {} for node {}. Added {}, removed {}, replaced {} out of {}",
                    nextMetadata, node,
                    topicIdPartitionsToLogString(added),
                    topicIdPartitionsToLogString(removed),
                    topicIdPartitionsToLogString(replaced),
                    topicIdPartitionsToLogString(sessionPartitions.values()));
        }

        // 将替换的分区添加到移除列表中
        removed.addAll(replaced);

        // 构建确认批次映射
        Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgementBatches = new HashMap<>();
        nextAcknowledgements.forEach((partition, acknowledgements) -> acknowledgementBatches.put(partition, acknowledgements.getAcknowledgementBatches()
                .stream().map(AcknowledgementBatch::toShareFetchRequest)
                .collect(Collectors.toList())));

        // 清空下一批次的分区和确认映射
        nextPartitions = new LinkedHashMap<>();
        nextAcknowledgements = new LinkedHashMap<>();

        // 创建并返回请求构建器
        return ShareFetchRequest.Builder.forConsumer(
                groupId, nextMetadata, fetchConfig.maxWaitMs,
                fetchConfig.minBytes, fetchConfig.maxBytes, fetchConfig.fetchSize, fetchConfig.maxPollRecords,
                added, removed, acknowledgementBatches);
    }

    /**
     * 创建新的ShareAcknowledge请求构建器
     *
     * @param groupId 消费者组ID
     * @param fetchConfig 获取配置
     * @return ShareAcknowledge请求构建器，如果是新会话则返回null
     */
    public ShareAcknowledgeRequest.Builder newShareAcknowledgeBuilder(String groupId, FetchConfig fetchConfig) {
        // 新会话不能以ShareAcknowledge请求开始
        if (nextMetadata.isNewSession()) {
            nextPartitions.clear();
            nextAcknowledgements.clear();
            return null;
        }

        // 构建确认批次映射
        Map<TopicIdPartition, List<ShareAcknowledgeRequestData.AcknowledgementBatch>> acknowledgementBatches = new HashMap<>();
        nextAcknowledgements.forEach((partition, acknowledgements) ->
                acknowledgementBatches.put(partition, acknowledgements.getAcknowledgementBatches()
                        .stream().map(AcknowledgementBatch::toShareAcknowledgeRequest)
                        .collect(Collectors.toList())));

        // 清空下一批次的确认映射
        nextAcknowledgements = new LinkedHashMap<>();

        // 创建并返回请求构建器
        return ShareAcknowledgeRequest.Builder.forConsumer(groupId, nextMetadata, acknowledgementBatches);
    }

    /**
     * 将分区集合转换为日志字符串
     *
     * @param partitions 分区集合
     * @return 格式化的日志字符串
     */
    private String topicIdPartitionsToLogString(Collection<TopicIdPartition> partitions) {
        // 如果不是跟踪级别，只返回分区数量
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        // 否则返回详细的分区信息
        return "(" + partitions.stream().map(TopicIdPartition::toString).collect(Collectors.joining(", ")) + ")";
    }

    /**
     * 处理ShareFetch响应
     *
     * @param response 响应对象
     * @param version 请求版本
     * @return 如果响应格式正确返回true，如果由于缺失或意外的分区而无法处理则返回false
     */
    public boolean handleResponse(ShareFetchResponse response, short version) {
        // 检查会话错误
        if ((response.error() == Errors.SHARE_SESSION_NOT_FOUND) ||
                (response.error() == Errors.INVALID_SHARE_SESSION_EPOCH)) {
            log.info("Node {} was unable to process the ShareFetch request with {}: {}.",
                    node, nextMetadata, response.error());
            // 关闭现有会话并尝试创建新会话
            nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
            return false;
        }

        // 检查其他错误
        if (response.error() != Errors.NONE) {
            log.info("Node {} was unable to process the ShareFetch request with {}: {}.",
                    node, nextMetadata, response.error());
            // 进入下一个世代
            nextMetadata = nextMetadata.nextEpoch();
            return false;
        }

        // 服务器继续会话
        if (log.isDebugEnabled())
            log.debug("Node {} sent a ShareFetch response with throttleTimeMs = {} " +
                    "for session {}", node, response.throttleTimeMs(), memberId);
        // 进入下一个世代
        nextMetadata = nextMetadata.nextEpoch();
        return true;
    }

    /**
     * 处理ShareAcknowledge响应
     *
     * @param response 响应对象
     * @param version 请求版本
     * @return 如果响应格式正确返回true，如果由于缺失或意外的分区而无法处理则返回false
     */
    public boolean handleResponse(ShareAcknowledgeResponse response, short version) {
        // 检查会话错误
        if ((response.error() == Errors.SHARE_SESSION_NOT_FOUND) ||
                (response.error() == Errors.INVALID_SHARE_SESSION_EPOCH)) {
            log.info("Node {} was unable to process the ShareAcknowledge request with {}: {}.",
                    node, nextMetadata, response.error());
            // 关闭现有会话并尝试创建新会话
            nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
            return false;
        }

        // 检查其他错误
        if (response.error() != Errors.NONE) {
            log.info("Node {} was unable to process the ShareAcknowledge request with {}: {}.",
                    node, nextMetadata, response.error());
            // 进入下一个世代
            nextMetadata = nextMetadata.nextEpoch();
            return false;
        }

        // 服务器继续会话
        if (log.isDebugEnabled())
            log.debug("Node {} sent a ShareAcknowledge response with throttleTimeMs = {} " +
                    "for session {}", node, response.throttleTimeMs(), memberId);
        // 进入下一个世代
        nextMetadata = nextMetadata.nextEpoch();
        return true;
    }

    /**
     * 通知客户端将在下一个ShareFetch请求中关闭会话
     */
    public void notifyClose() {
        log.debug("Set the metadata for next ShareFetch request to close the share session memberId={}",
                nextMetadata.memberId());
        // 设置为最终世代
        nextMetadata = nextMetadata.finalEpoch();
    }

    /**
     * 处理发送准备好的请求时的错误
     * 当发生网络错误时，我们在下一个请求中关闭任何现有的共享会话，
     * 并尝试创建新会话
     *
     * @param t 异常对象
     */
    public void handleError(Throwable t) {
        log.info("Error sending fetch request {} to node {}:", nextMetadata, node, t);
        // 关闭现有会话并尝试创建新会话
        nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
    }
}
