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

package kafka.server.handlers;

import kafka.network.RequestChannel;
import kafka.server.AuthHelper;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.resource.Resource;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

/**
 * 主题分区描述请求处理器，用于处理客户端获取主题分区元数据的请求
 * 主要功能包括：
 * 1. 处理主题分区的描述请求，支持获取所有主题或指定主题的分区信息
 * 2. 实现基于游标的分页查询机制
 * 3. 进行授权检查，确保请求方有权限访问相应的主题
 * 4. 返回包含分区信息的响应数据
 */
public class DescribeTopicPartitionsRequestHandler {
    /** 元数据缓存，用于存储和获取主题分区的元数据信息 */
    MetadataCache metadataCache;
    /** 授权帮助类，用于处理主题访问的权限验证 */
    AuthHelper authHelper;
    /** Kafka配置类，包含服务器的配置参数 */
    KafkaConfig config;

    /**
     * 构造函数，初始化处理器所需的组件
     *
     * @param metadataCache 元数据缓存实例，用于访问主题分区信息
     * @param authHelper 授权帮助类实例，用于权限验证
     * @param config Kafka配置实例，包含服务器配置
     */
    public DescribeTopicPartitionsRequestHandler(
        MetadataCache metadataCache,
        AuthHelper authHelper,
        KafkaConfig config
    ) {
        this.metadataCache = metadataCache;
        this.authHelper = authHelper;
        this.config = config;
    }

    /**
     * 处理描述主题分区的请求
     * 该方法实现了主题分区信息的查询、授权验证和分页处理
     *
     * @param abstractRequest 包含请求信息的抽象请求对象
     * @return 返回包含主题分区信息的响应数据
     */
    public DescribeTopicPartitionsResponseData handleDescribeTopicPartitionsRequest(RequestChannel.Request abstractRequest) {
        // 从请求中提取数据
        DescribeTopicPartitionsRequestData request = ((DescribeTopicPartitionsRequest) abstractRequest.loggableRequest()).data();
        Set<String> topics = new HashSet<>();
        // 判断是否需要获取所有主题
        boolean fetchAllTopics = request.topics().isEmpty();
        // 获取分页游标信息
        DescribeTopicPartitionsRequestData.Cursor cursor = request.cursor();
        String cursorTopicName = cursor != null ? cursor.topicName() : "";
        
        if (fetchAllTopics) {
            // 获取所有主题，并根据游标过滤
            CollectionConverters.asJavaCollection(metadataCache.getAllTopics()).forEach(topicName -> {
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });
        } else {
            // 获取指定的主题列表，并根据游标过滤
            request.topics().forEach(topic -> {
                String topicName = topic.name();
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });

            // 验证游标主题是否在请求的主题列表中
            if (cursor != null && !topics.contains(cursor.topicName())) {
                throw new InvalidRequestException("DescribeTopicPartitionsRequest topic list should contain the cursor topic: " + cursor.topicName());
            }
        }

        // 验证分区索引的有效性
        if (cursor != null && cursor.partitionIndex() < 0) {
            throw new InvalidRequestException("DescribeTopicPartitionsRequest cursor partition must be valid: " + cursor);
        }

        // 存储未授权主题的元数据
        Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata = new HashSet<>();

        // 过滤出已授权的主题
        Stream<String> authorizedTopicsStream = topics.stream().sorted().filter(topicName -> {
            // 检查主题的访问权限
            boolean isAuthorized = authHelper.authorize(
                abstractRequest.context(), DESCRIBE, TOPIC, topicName, true, true, 1);
            if (!fetchAllTopics && !isAuthorized) {
                // 对于未授权的主题，返回零UUID
                unauthorizedForDescribeTopicMetadata.add(describeTopicPartitionsResponseTopic(
                    Errors.TOPIC_AUTHORIZATION_FAILED, topicName, Uuid.ZERO_UUID, false, Collections.emptyList())
                );
            }
            return isAuthorized;
        });

        // 获取主题分区的详细信息
        DescribeTopicPartitionsResponseData response = metadataCache.describeTopicResponse(
            CollectionConverters.asScala(authorizedTopicsStream.iterator()),
            abstractRequest.context().listenerName,
            (String topicName) -> topicName.equals(cursorTopicName) ? cursor.partitionIndex() : 0,
            Math.max(Math.min(config.maxRequestPartitionSizeLimit(), request.responsePartitionLimit()), 1),
            fetchAllTopics
        );

        // 设置主题的授权操作
        response.topics().forEach(topicData ->
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(abstractRequest, new Resource(TOPIC, topicData.name()))));

        // 添加未授权主题的信息到响应中
        response.topics().addAll(unauthorizedForDescribeTopicMetadata);
        return response;
    }

    /**
     * 创建主题分区响应的辅助方法
     * 用于构建包含主题元数据的响应对象
     *
     * @param error 错误信息
     * @param topic 主题名称
     * @param topicId 主题ID
     * @param isInternal 是否为内部主题
     * @param partitionData 分区数据列表
     * @return 返回主题分区响应对象
     */
    private DescribeTopicPartitionsResponseTopic describeTopicPartitionsResponseTopic(
        Errors error,
        String topic,
        Uuid topicId,
        Boolean isInternal,
        List<DescribeTopicPartitionsResponsePartition> partitionData
    ) {
        return new DescribeTopicPartitionsResponseTopic()
            .setErrorCode(error.code())    // 设置错误码
            .setName(topic)                // 设置主题名称
            .setTopicId(topicId)          // 设置主题ID
            .setIsInternal(isInternal)     // 设置是否为内部主题
            .setPartitions(partitionData); // 设置分区数据
    }
}
