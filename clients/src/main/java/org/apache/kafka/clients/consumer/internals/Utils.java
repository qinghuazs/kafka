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

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Kafka消费者内部工具类，提供了主题分区比较器的实现。
 * 这些比较器用于在消费者端对主题分区进行排序和组织。
 */
public final class Utils {

    /**
     * 基于分区大小的主题分区比较器。
     * 该比较器首先根据主题中的分区数量进行比较，分区数量少的主题排在前面。
     * 当分区数量相同时，按主题名称的字典序排序，最后按分区号排序。
     */
    static final class PartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;
        
        /**
         * 存储主题及其关联信息的映射，key为主题名称，value为该主题相关的信息列表
         */
        private final Map<String, List<String>> map;

        /**
         * 构造函数
         * @param map 主题信息映射，用于获取主题的分区数量信息
         */
        PartitionComparator(Map<String, List<String>> map) {
            this.map = map;
        }

        /**
         * 比较两个主题分区的顺序
         * @param o1 第一个主题分区
         * @param o2 第二个主题分区
         * @return 比较结果：
         *         负数表示o1应排在o2前面
         *         正数表示o1应排在o2后面
         *         0表示顺序无关紧要
         */
        @Override
        public int compare(TopicPartition o1, TopicPartition o2) {
            // 首先比较主题的分区数量
            int ret = map.get(o1.topic()).size() - map.get(o2.topic()).size();
            if (ret == 0) {
                // 如果分区数量相同，则按主题名称的字典序比较
                ret = o1.topic().compareTo(o2.topic());
                if (ret == 0)
                    // 如果主题名称也相同，则按分区号比较
                    ret = o1.partition() - o2.partition();
            }
            return ret;
        }
    }

    /**
     * 标准的主题分区比较器实现。
     * 该比较器按照主题名称的字典序进行排序，当主题名称相同时，按分区号排序。
     * 这个比较器常用于需要对主题分区进行一致性排序的场景，如分区分配、消费进度跟踪等。
     */
    public static final class TopicPartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * 比较两个主题分区的顺序
         * @param topicPartition1 第一个主题分区
         * @param topicPartition2 第二个主题分区
         * @return 比较结果：
         *         负数表示topicPartition1应排在topicPartition2前面
         *         正数表示topicPartition1应排在topicPartition2后面
         *         0表示两者相等
         */
        @Override
        public int compare(TopicPartition topicPartition1, TopicPartition topicPartition2) {
            // 获取两个主题分区的主题名称
            String topic1 = topicPartition1.topic();
            String topic2 = topicPartition2.topic();

            if (topic1.equals(topic2)) {
                // 如果主题名称相同，则按分区号排序
                return topicPartition1.partition() - topicPartition2.partition();
            } else {
                // 如果主题名称不同，则按主题名称的字典序排序
                return topic1.compareTo(topic2);
            }
        }
    }

    /**
     * 带主题ID的主题分区比较器实现。
     * 该比较器的行为与TopicPartitionComparator类似，但是用于处理带有主题ID的分区对象。
     * 主题ID是Kafka 2.8版本引入的特性，用于在主题重命名场景下保持一致性。
     */
    public static final class TopicIdPartitionComparator implements Comparator<TopicIdPartition>, Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * 比较两个带ID的主题分区的顺序
         * @param topicPartition1 第一个主题分区
         * @param topicPartition2 第二个主题分区
         * @return 比较结果：
         *         负数表示topicPartition1应排在topicPartition2前面
         *         正数表示topicPartition1应排在topicPartition2后面
         *         0表示两者相等
         */
        @Override
        public int compare(TopicIdPartition topicPartition1, TopicIdPartition topicPartition2) {
            // 获取两个主题分区的主题名称
            String topic1 = topicPartition1.topic();
            String topic2 = topicPartition2.topic();

            if (topic1.equals(topic2)) {
                // 如果主题名称相同，则按分区号排序
                return topicPartition1.partition() - topicPartition2.partition();
            } else {
                // 如果主题名称不同，则按主题名称的字典序排序
                return topic1.compareTo(topic2);
            }
        }
    }
}
