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

import org.apache.kafka.common.Uuid;

/**
 * Kafka集群中主题的基本信息列表项。
 * 该类用于表示Kafka集群中单个主题的基本元数据信息，包括主题名称、ID和类型。
 * 它通常在AdminClient的listTopics()方法中使用，用于获取集群中所有主题的概览信息。
 * 与TopicDescription相比，TopicListing包含的信息更少，主要用于快速列举主题。
 */
public class TopicListing {
    /**
     * 主题名称
     * 在Kafka集群中唯一标识一个主题的字符串
     */
    private final String name;

    /**
     * 主题的唯一标识符
     * 在Kafka 2.8.0及以后版本中引入，用于在集群范围内唯一标识主题
     * 与主题名称不同，topicId在主题重命名后仍保持不变
     */
    private final Uuid topicId;

    /**
     * 是否为Kafka内部主题的标志
     * 用于标识该主题是否为Kafka内部使用的系统主题
     */
    private final boolean internal;

    /**
     * 创建TopicListing实例
     * 用于构造一个新的主题列表项，包含主题的基本元数据信息
     *
     * @param name 主题名称，用于在Kafka集群中唯一标识该主题
     * @param topicId 主题的唯一标识符，在Kafka 2.8.0及以后版本中使用
     * @param internal 是否为Kafka内部主题的标志，true表示这是一个内部主题
     */
    public TopicListing(String name, Uuid topicId, boolean internal) {
        this.topicId = topicId;
        this.name = name;
        this.internal = internal;
    }

    /**
     * 获取主题的唯一标识符
     * 返回在Kafka 2.8.0及以后版本中用于唯一标识主题的UUID
     * 这个ID在主题的整个生命周期内保持不变，即使主题被重命名
     */
    public Uuid topicId() {
        return topicId;
    }

    /**
     * 获取主题名称
     * 返回在Kafka集群中唯一标识该主题的名称字符串
     * 主题名称可以通过管理操作进行修改
     */
    public String name() {
        return name;
    }

    /**
     * 判断是否为Kafka内部主题
     * 返回一个布尔值，指示该主题是否为Kafka内部使用的系统主题
     * 内部主题（如消费者偏移量主题__consumer_offsets）用于存储Kafka自身的元数据信息
     * 这些主题对于Kafka的正常运行至关重要，通常由Kafka自动管理
     */
    public boolean isInternal() {
        return internal;
    }

    @Override
    public String toString() {
        return "(name=" + name + ", topicId=" + topicId +  ", internal=" + internal + ")";
    }
}
