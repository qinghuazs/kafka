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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 领导者纪元偏移量工具类
 * 提供用于准备OffsetsForLeaderEpoch API请求和处理响应的工具方法
 * 
 * 应用场景：
 * 1. 获取特定领导者纪元的偏移量信息
 * 2. 处理分区领导者变更
 * 3. 确保消费者位置的一致性
 * 4. 支持日志截断检测
 */
public final class OffsetsForLeaderEpochUtils {
    /**
     * 日志记录器
     * 用于记录工具类的操作和错误信息
     */
    private static final Logger LOG = LoggerFactory.getLogger(OffsetsForLeaderEpochUtils.class);

    /**
     * 私有构造函数
     * 防止实例化工具类
     */
    private OffsetsForLeaderEpochUtils() {}

    /**
     * 准备领导者纪元偏移量请求
     * 将请求数据转换为请求构建器
     *
     * @param requestData 主题分区到获取位置的映射
     * @return 领导者纪元偏移量请求的构建器
     */
    static AbstractRequest.Builder<OffsetsForLeaderEpochRequest> prepareRequest(
            Map<TopicPartition, SubscriptionState.FetchPosition> requestData) {
        // 创建主题集合，初始容量为请求数据的大小
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection(requestData.size());
        
        // 遍历请求数据，为每个主题分区构建请求
        requestData.forEach((topicPartition, fetchPosition) ->
                // 只处理有偏移量纪元的情况
                fetchPosition.offsetEpoch.ifPresent(fetchEpoch -> {
                    // 查找或创建主题
                    OffsetForLeaderTopic topic = topics.find(topicPartition.topic());
                    if (topic == null) {
                        // 如果主题不存在，创建新主题并添加到集合
                        topic = new OffsetForLeaderTopic().setTopic(topicPartition.topic());
                        topics.add(topic);
                    }
                    // 添加分区信息到主题
                    topic.partitions().add(new OffsetForLeaderPartition()
                            .setPartition(topicPartition.partition())
                            .setLeaderEpoch(fetchEpoch)
                            // 设置当前领导者纪元，如果不存在则使用NO_PARTITION_LEADER_EPOCH
                            .setCurrentLeaderEpoch(fetchPosition.currentLeader.epoch
                                    .orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    );
                })
        );
        // 创建消费者请求构建器
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(topics);
    }

    /**
     * 处理领导者纪元偏移量响应
     * 解析响应并处理各种错误情况
     *
     * @param requestData 原始请求数据
     * @param response 服务器的响应
     * @return 处理后的偏移量结果
     * @throws TopicAuthorizationException 如果存在未授权的主题
     */
    public static OffsetForEpochResult handleResponse(
            Map<TopicPartition, SubscriptionState.FetchPosition> requestData,
            OffsetsForLeaderEpochResponse response) {
        // 初始化需要重试的分区集合（初始包含所有请求的分区）
        Set<TopicPartition> partitionsToRetry = new HashSet<>(requestData.keySet());
        // 初始化未授权主题集合
        Set<String> unauthorizedTopics = new HashSet<>();
        // 初始化成功获取的偏移量映射
        Map<TopicPartition, EpochEndOffset> endOffsets = new HashMap<>();

        // 遍历响应中的每个主题
        for (OffsetForLeaderTopicResult topic : response.data().topics()) {
            // 遍历主题中的每个分区
            for (EpochEndOffset partition : topic.partitions()) {
                // 创建主题分区对象
                TopicPartition topicPartition = new TopicPartition(topic.topic(), partition.partition());

                // 验证是否是请求的分区
                if (!requestData.containsKey(topicPartition)) {
                    LOG.warn("Received unrequested topic or partition {} from response, ignoring.", topicPartition);
                    continue;
                }

                // 获取错误码并处理
                Errors error = Errors.forCode(partition.errorCode());
                switch (error) {
                    case NONE:
                        // 成功获取偏移量
                        LOG.debug("Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}.",
                                topicPartition, partition.endOffset(), partition.leaderEpoch());
                        endOffsets.put(topicPartition, partition);
                        partitionsToRetry.remove(topicPartition);
                        break;
                    case NOT_LEADER_OR_FOLLOWER:
                    case REPLICA_NOT_AVAILABLE:
                    case KAFKA_STORAGE_ERROR:
                    case OFFSET_NOT_AVAILABLE:
                    case LEADER_NOT_AVAILABLE:
                    case FENCED_LEADER_EPOCH:
                    case UNKNOWN_LEADER_EPOCH:
                        // 可重试的错误
                        LOG.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                                topicPartition, error);
                        break;
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        // 未知主题或分区错误
                        LOG.warn("Received unknown topic or partition error in OffsetsForLeaderEpoch request for partition {}.",
                                topicPartition);
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                        // 授权失败错误
                        unauthorizedTopics.add(topicPartition.topic());
                        partitionsToRetry.remove(topicPartition);
                        break;
                    default:
                        // 其他未知错误
                        LOG.warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.",
                                topicPartition, error.message());
                }
            }
        }

        // 如果存在未授权的主题，抛出异常
        if (!unauthorizedTopics.isEmpty())
            throw new TopicAuthorizationException(unauthorizedTopics);

        // 返回处理结果
        return new OffsetForEpochResult(endOffsets, partitionsToRetry);
    }

    /**
     * 领导者纪元偏移量结果类
     * 封装了处理响应后的结果信息
     */
    static class OffsetForEpochResult {
        /**
         * 成功获取的分区偏移量映射
         */
        private final Map<TopicPartition, EpochEndOffset> endOffsets;
        
        /**
         * 需要重试的分区集合
         */
        private final Set<TopicPartition> partitionsToRetry;

        /**
         * 构造函数
         *
         * @param endOffsets 成功获取的分区偏移量映射
         * @param partitionsNeedingRetry 需要重试的分区集合
         */
        OffsetForEpochResult(Map<TopicPartition, EpochEndOffset> endOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.endOffsets = endOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        /**
         * 获取成功获取的分区偏移量映射
         */
        public Map<TopicPartition, EpochEndOffset> endOffsets() {
            return endOffsets;
        }

        /**
         * 获取需要重试的分区集合
         */
        public Set<TopicPartition> partitionsToRetry() {
            return partitionsToRetry;
        }
    }
}
