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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 主题创建策略接口
 * 用于对创建主题的请求实施策略验证
 *
 * 应用场景：
 * 1. 验证复制因子：确保主题的复制因子在允许范围内，防止设置过大或过小的复制因子
 * 2. 验证同步副本：验证min.insync.replicas配置是否合理，确保数据可靠性
 * 3. 验证数据保留：检查retention相关设置是否在合理范围内，避免存储资源浪费
 * 4. 自定义验证：实现自定义的主题创建规则，如命名规范、特定配置要求等
 *
 * 实现说明：
 * 当create.topic.policy.class.name被定义时，Kafka会：
 * 1. 使用默认构造函数创建指定类的实例
 * 2. 将broker配置通过configure()方法传递给实例
 * 3. 在broker关闭时调用close()方法释放资源
 *
 * 设计考虑：
 * 1. 可扩展性：通过接口设计支持自定义策略实现
 * 2. 解耦性：策略验证与主题创建逻辑分离
 * 3. 灵活性：支持多种验证规则组合
 * 4. 资源管理：提供资源释放机制
 */
public interface CreateTopicPolicy extends Configurable, AutoCloseable {

    /**
     * 请求元数据类
     * 包含创建主题请求的参数信息
     * 
     * 应用场景：
     * 1. 参数验证：验证主题创建请求的各项参数
     * 2. 策略执行：根据参数执行相应的验证策略
     * 3. 参数传递：在验证过程中传递必要的请求信息
     */
    class RequestMetadata {
        /**
         * 主题名称
         * 要创建的主题的名称
         */
        private final String topic;

        /**
         * 分区数量
         * 要创建的分区数量，如果指定了replicasAssignments则为null
         */
        private final Integer numPartitions;

        /**
         * 复制因子
         * 主题的复制因子，如果指定了replicasAssignments则为null
         */
        private final Short replicationFactor;

        /**
         * 副本分配方案
         * 从分区ID到副本（broker）ID的映射，如果指定了numPartitions和replicationFactor则为null
         */
        private final Map<Integer, List<Integer>> replicasAssignments;

        /**
         * 主题配置
         * 要创建的主题的配置参数，不包括broker默认配置
         */
        private final Map<String, String> configs;

        /**
         * Create an instance of this class with the provided parameters.
         *
         * This constructor is public to make testing of <code>CreateTopicPolicy</code> implementations easier.
         *
         * @param topic the name of the topic to create.
         * @param numPartitions the number of partitions to create or null if replicasAssignments is set.
         * @param replicationFactor the replication factor for the topic or null if replicaAssignments is set.
         * @param replicasAssignments replica assignments or null if numPartitions and replicationFactor is set. The
         *                            assignment is a map from partition id to replica (broker) ids.
         * @param configs topic configs for the topic to be created, not including broker defaults. Broker configs are
         *                passed via the {@code configure()} method of the policy implementation.
         */
        public RequestMetadata(String topic, Integer numPartitions, Short replicationFactor,
                        Map<Integer, List<Integer>> replicasAssignments, Map<String, String> configs) {
            this.topic = topic;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = replicasAssignments == null ? null : Collections.unmodifiableMap(replicasAssignments);
            this.configs = Collections.unmodifiableMap(configs);
        }

        /**
         * Return the name of the topic to create.
         */
        public String topic() {
            return topic;
        }

        /**
         * Return the number of partitions to create or null if replicaAssignments is not null.
         */
        public Integer numPartitions() {
            return numPartitions;
        }

        /**
         * Return the number of replicas to create or null if replicaAssignments is not null.
         */
        public Short replicationFactor() {
            return replicationFactor;
        }

        /**
         * Return a map from partition id to replica (broker) ids or null if numPartitions and replicationFactor are
         * set instead.
         */
        public Map<Integer, List<Integer>> replicasAssignments() {
            return replicasAssignments;
        }

        /**
         * Return topic configs in the request, not including broker defaults. Broker configs are passed via
         * the {@code configure()} method of the policy implementation.
         */
        public Map<String, String> configs() {
            return configs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, numPartitions, replicationFactor,
                replicasAssignments, configs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RequestMetadata other = (RequestMetadata) o;
            return topic.equals(other.topic) &&
                Objects.equals(numPartitions, other.numPartitions) &&
                Objects.equals(replicationFactor, other.replicationFactor) &&
                Objects.equals(replicasAssignments, other.replicasAssignments) &&
                configs.equals(other.configs);
        }

        @Override
        public String toString() {
            return "CreateTopicPolicy.RequestMetadata(topic=" + topic +
                    ", numPartitions=" + numPartitions +
                    ", replicationFactor=" + replicationFactor +
                    ", replicasAssignments=" + replicasAssignments +
                    ", configs=" + configs + ")";
        }
    }

    /**
     * 验证请求参数
     * 如果提供的主题的创建请求参数不满足策略要求，则抛出带有适当错误消息的PolicyViolationException
     *
     * 实现要求：
     * 1. 验证逻辑：根据具体策略实现验证逻辑
     * 2. 错误处理：提供清晰的错误消息说明违反的策略
     * 3. 部分验证：验证失败只影响相关主题，不影响请求中的其他主题
     *
     * 使用场景：
     * 1. 配置验证：验证主题配置是否符合要求
     * 2. 资源控制：控制主题的资源使用（如分区数、复制因子）
     * 3. 命名规范：验证主题名称是否符合规范
     * 4. 安全控制：实施主题创建的安全策略
     *
     * @param requestMetadata 要验证的主题创建请求参数
     * @throws PolicyViolationException 当请求参数不满足策略要求时抛出
     */
    void validate(RequestMetadata requestMetadata) throws PolicyViolationException;
}
