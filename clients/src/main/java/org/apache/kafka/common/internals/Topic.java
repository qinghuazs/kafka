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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Kafka主题管理工具类，提供主题名称的验证和管理功能。
 * 主要用于以下场景：
 * 1. 验证主题名称的合法性
 * 2. 管理Kafka内部主题
 * 3. 处理主题名称冲突
 */
public class Topic {

    /**
     * 消费者偏移量主题名称，用于存储消费者组的偏移量信息
     */
    public static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";

    /**
     * 事务状态主题名称，用于存储事务相关的元数据
     */
    public static final String TRANSACTION_STATE_TOPIC_NAME = "__transaction_state";

    /**
     * 共享组状态主题名称，用于存储共享消费者组的状态信息
     */
    public static final String SHARE_GROUP_STATE_TOPIC_NAME = "__share_group_state";

    /**
     * 集群元数据主题名称，用于存储集群级别的元数据信息
     */
    public static final String CLUSTER_METADATA_TOPIC_NAME = "__cluster_metadata";

    /**
     * 集群元数据主题分区，固定使用0号分区
     */
    public static final TopicPartition CLUSTER_METADATA_TOPIC_PARTITION = new TopicPartition(
        CLUSTER_METADATA_TOPIC_NAME,
        0
    );

    /**
     * 主题名称允许使用的字符正则表达式
     */
    public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";

    /**
     * Kafka内部主题集合，包含所有系统使用的特殊主题
     */
    private static final Set<String> INTERNAL_TOPICS = Set.of(GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME);

    /**
     * 主题名称最大长度限制
     */
    private static final int MAX_NAME_LENGTH = 249;

    /**
     * 验证主题名称的合法性，如果不合法则抛出异常
     * 
     * @param topic 要验证的主题名称
     * @throws InvalidTopicException 如果主题名称不合法
     */
    public static void validate(String topic) {
        validate(topic, "Topic name", message -> {
            throw new InvalidTopicException(message);
        });
    }

    /**
     * 检测主题名称是否合法，返回不合法的原因
     * 
     * @param name 要检测的主题名称
     * @return 如果主题名称不合法，返回具体原因；如果合法，返回null
     */
    private static String detectInvalidTopic(String name) {
        // 不允许空字符串
        if (name.isEmpty())
            return "the empty string is not allowed";
        // 不允许单个点号
        if (".".equals(name))
            return "'.' is not allowed";
        // 不允许双点号
        if ("..".equals(name))
            return "'..' is not allowed";
        // 检查名称长度
        if (name.length() > MAX_NAME_LENGTH)
            return "the length of '" + name + "' is longer than the max allowed length " + MAX_NAME_LENGTH;
        // 检查字符合法性
        if (!containsValidPattern(name))
            return "'" + name + "' contains one or more characters other than " +
                "ASCII alphanumerics, '.', '_' and '-'";
        return null;
    }

    /**
     * 检查主题名称是否合法
     * 
     * @param name 要检查的主题名称
     * @return 如果主题名称合法返回true，否则返回false
     */
    public static boolean isValid(String name) {
        String reasonInvalid = detectInvalidTopic(name);
        return reasonInvalid == null;
    }

    /**
     * 验证主题名称的合法性，如果不合法则通过指定的消费者处理错误信息
     * 
     * @param name 要验证的主题名称
     * @param logPrefix 错误信息前缀
     * @param throwableConsumer 处理错误信息的消费者
     */
    public static void validate(String name, String logPrefix, Consumer<String> throwableConsumer) {
        String reasonInvalid = detectInvalidTopic(name);
        if (reasonInvalid != null) {
            throwableConsumer.accept(logPrefix + " is invalid: " +  reasonInvalid);
        }
    }

    /**
     * 检查主题是否为Kafka内部主题
     * 
     * @param topic 要检查的主题名称
     * @return 如果是内部主题返回true，否则返回false
     */
    public static boolean isInternal(String topic) {
        return INTERNAL_TOPICS.contains(topic);
    }

    /**
     * 检查主题名称是否包含可能导致度量指标名称冲突的字符
     * 由于度量指标名称的限制，包含点号('.')或下划线('_')的主题可能会发生冲突
     *
     * @param topic 要检查的主题名称
     * @return 如果主题包含冲突字符返回true，否则返回false
     */
    public static boolean hasCollisionChars(String topic) {
        return topic.contains("_") || topic.contains(".");
    }

    /**
     * 统一主题名称中的点号('.')和下划线('_')字符
     * 这个方法仅用于检查冲突，不会实际改变主题名称
     * 统一规则：将所有的点号('.')替换为下划线('_')
     *
     * @param topic 要统一处理的主题名称
     * @return 统一处理后的主题名称
     */
    public static String unifyCollisionChars(String topic) {
        return topic.replace('.', '_');
    }

    /**
     * 检查两个主题名称是否因为在相同位置使用点号('.')或下划线('_')而发生冲突
     * 例如：'my.topic'和'my_topic'会被认为是冲突的
     *
     * @param topicA 要检查的第一个主题名称
     * @param topicB 要检查的第二个主题名称
     * @return 如果两个主题名称冲突返回true，否则返回false
     */
    public static boolean hasCollision(String topicA, String topicB) {
        return unifyCollisionChars(topicA).equals(unifyCollisionChars(topicB));
    }

    /**
     * 检查主题名称是否只包含合法字符
     * Kafka主题名称只允许使用ASCII字母数字、点号('.')、下划线('_')和连字符('-')
     * 注：不使用Character.isLetterOrDigit(c)是为了提高性能
     * 
     * @param topic 要检查的主题名称
     * @return 如果主题名称只包含合法字符返回true，否则返回false
     */
    static boolean containsValidPattern(String topic) {
        for (int i = 0; i < topic.length(); ++i) {
            char c = topic.charAt(i);

            // 直接比较字符范围，比使用Character.isLetterOrDigit(c)更快
            boolean validChar = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || c == '.' ||
                    c == '_' || c == '-';
            if (!validChar)
                return false;
        }
        return true;
    }
}
