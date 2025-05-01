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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link Admin#createTopics(Collection)}方法的执行结果。
 * 该类用于处理主题创建操作的异步结果，包括主题配置、主题ID、分区数和副本因子等信息的获取。
 * 通过异步Future机制，允许客户端在不阻塞的情况下创建多个主题，并能够分别获取每个主题的创建状态。
 *
 * 该类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class CreateTopicsResult {
    // 表示未知或无效的值，用于分区数和副本因子的默认值
    static final int UNKNOWN = -1;

    // 存储每个主题的创建结果Future，key为主题名称，value为包含主题元数据和配置的Future对象
    private final Map<String, KafkaFuture<TopicMetadataAndConfig>> futures;

    /**
     * 构造函数
     * @param futures 主题创建结果的Future映射，每个主题对应一个Future
     */
    protected CreateTopicsResult(Map<String, KafkaFuture<TopicMetadataAndConfig>> futures) {
        this.futures = futures;
    }

    /**
     * 返回主题名称到对应Future的映射，用于检查每个主题的创建状态
     * 
     * @return 返回Map，其中key为主题名称，value为Future对象
     *         Future完成时返回null，表示主题创建成功；如果创建失败，Future会抛出异常
     */
    public Map<String, KafkaFuture<Void>> values() {
        // 将每个主题的Future转换为只返回null的Future，主要用于检查完成状态
        return futures.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().thenApply(v -> null)));
    }

    /**
     * 返回一个Future，只有当所有主题都创建成功时，该Future才会成功完成
     * 
     * @return 聚合了所有主题创建结果的Future
     *         如果所有主题都创建成功，Future完成并返回null
     *         如果任何主题创建失败，Future将抛出第一个遇到的异常
     */
    public KafkaFuture<Void> all() {
        // 使用KafkaFuture.allOf组合所有Future，任一Future失败都会导致结果Future失败
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }

    /**
     * 返回一个Future，该Future在请求完成时提供主题的配置信息
     * 
     * @param topic 主题名称
     * @return 返回Future<Config>对象，完成时提供主题的配置信息
     * 
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *         如果broker版本不支持在响应中返回副本因子信息
     * @throws org.apache.kafka.common.errors.TopicAuthorizationException
     *         如果用户没有查看主题配置的权限
     * 
     * 注意：配置中的type和documentation字段将为null
     */
    public KafkaFuture<Config> config(String topic) {
        // 从Future中获取主题元数据，并提取配置信息
        return futures.get(topic).thenApply(TopicMetadataAndConfig::config);
    }

    /**
     * 返回一个Future，该Future在请求完成时提供主题的ID
     * 
     * @param topic 主题名称
     * @return 返回Future<Uuid>对象，完成时提供主题的唯一标识符
     * 
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *         如果broker版本不支持在响应中返回副本因子信息
     * @throws org.apache.kafka.common.errors.TopicAuthorizationException
     *         如果用户没有查看主题配置的权限
     */
    public KafkaFuture<Uuid> topicId(String topic) {
        // 从Future中获取主题元数据，并提取主题ID
        return futures.get(topic).thenApply(TopicMetadataAndConfig::topicId);
    }
    
    /**
     * 返回一个Future，该Future在请求完成时提供主题的分区数
     * 
     * @param topic 主题名称
     * @return 返回Future<Integer>对象，完成时提供主题的分区数
     * 
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *         如果broker版本不支持在响应中返回副本因子信息
     * @throws org.apache.kafka.common.errors.TopicAuthorizationException
     *         如果用户没有查看主题配置的权限
     */
    public KafkaFuture<Integer> numPartitions(String topic) {
        // 从Future中获取主题元数据，并提取分区数
        return futures.get(topic).thenApply(TopicMetadataAndConfig::numPartitions);
    }

    /**
     * 返回一个Future，该Future在请求完成时提供主题的副本因子
     * 
     * @param topic 主题名称
     * @return 返回Future<Integer>对象，完成时提供主题的副本因子
     * 
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *         如果broker版本不支持在响应中返回副本因子信息
     * @throws org.apache.kafka.common.errors.TopicAuthorizationException
     *         如果用户没有查看主题配置的权限
     */
    public KafkaFuture<Integer> replicationFactor(String topic) {
        // 从Future中获取主题元数据，并提取副本因子
        return futures.get(topic).thenApply(TopicMetadataAndConfig::replicationFactor);
    }

    /**
     * 内部静态类，用于存储主题的元数据和配置信息
     * 包含主题ID、分区数、副本因子等基本信息，以及可能发生的异常
     */
    public static class TopicMetadataAndConfig {
        // 如果主题创建失败，存储相关异常
        private final ApiException exception;
        // 主题的唯一标识符
        private final Uuid topicId;
        // 主题的分区数
        private final int numPartitions;
        // 主题的副本因子
        private final int replicationFactor;
        // 主题的配置信息
        private final Config config;

        /**
         * 成功场景的构造函数
         * @param topicId 主题ID
         * @param numPartitions 分区数
         * @param replicationFactor 副本因子
         * @param config 主题配置
         */
        public TopicMetadataAndConfig(Uuid topicId, int numPartitions, int replicationFactor, Config config) {
            this.exception = null;
            this.topicId = topicId;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.config = config;
        }

        /**
         * 失败场景的构造函数
         * @param exception 创建主题时发生的异常
         */
        public TopicMetadataAndConfig(ApiException exception) {
            this.exception = exception;
            this.topicId = Uuid.ZERO_UUID; // 使用零UUID表示无效的主题ID
            this.numPartitions = UNKNOWN;  // 使用UNKNOWN表示未知的分区数
            this.replicationFactor = UNKNOWN; // 使用UNKNOWN表示未知的副本因子
            this.config = null; // 配置为null表示没有可用的配置信息
        }
        
        /**
         * 获取主题ID
         * @return 主题的唯一标识符
         * @throws ApiException 如果主题创建失败
         */
        public Uuid topicId() {
            ensureSuccess(); // 检查是否存在异常
            return topicId;
        }

        /**
         * 获取主题的分区数
         * @return 主题的分区数
         * @throws ApiException 如果主题创建失败
         */
        public int numPartitions() {
            ensureSuccess(); // 检查是否存在异常
            return numPartitions;
        }

        /**
         * 获取主题的副本因子
         * @return 主题的副本因子
         * @throws ApiException 如果主题创建失败
         */
        public int replicationFactor() {
            ensureSuccess(); // 检查是否存在异常
            return replicationFactor;
        }

        /**
         * 获取主题的配置信息
         * @return 主题的配置对象
         * @throws ApiException 如果主题创建失败
         */
        public Config config() {
            ensureSuccess(); // 检查是否存在异常
            return config;
        }

        /**
         * 确保操作成功完成
         * @throws ApiException 如果存在异常则抛出
         */
        private void ensureSuccess() {
            if (exception != null)
                throw exception; // 如果存在异常，抛出该异常
        }
    }
}
